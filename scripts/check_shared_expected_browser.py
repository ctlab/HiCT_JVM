#!/usr/bin/env python3
"""Qualify exact-cut expected fitting and display histogram semantics on disposable data."""
import argparse
from collections import Counter
import functools
import hashlib
import http.server
import json
import math
from pathlib import Path
import threading

from playwright.sync_api import sync_playwright
from sequence_browser_harness import HictTestServer, BrowserCheck


def oracle(raw, row_ids, col_ids, row_start=0, col_start=0):
    """Enumerate unordered physical pairs, then divide their support among visible aliases."""
    occurrences = Counter((min(a,b),max(a,b)) for a in row_ids for b in col_ids)
    sums, support = Counter(), Counter()
    for r, a in enumerate(row_ids):
        for c, b in enumerate(col_ids):
            value = raw[r*len(col_ids)+c]
            if value > 0 and math.isfinite(value):
                weight = 1/occurrences[min(a,b),max(a,b)]
                distance = abs(row_start+r-col_start-c)
                sums[distance] += value*weight
                support[distance] += weight
    return [sums[abs(row_start+r-col_start-c)]/support[abs(row_start+r-col_start-c)]
            if support[abs(row_start+r-col_start-c)] else 0
            for r in range(len(row_ids)) for c in range(len(col_ids))]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ('jar','webui','data-dir','output'):
        parser.add_argument('--'+key,required=True)
    args = parser.parse_args()
    data, output = Path(args.data_dir).absolute(), Path(args.output).absolute()
    if data != data.resolve() or not data.is_relative_to('/tmp') or not output.is_relative_to('/tmp'):
        parser.error('Use isolated nonsymlinked disposable paths under /tmp')
    source = data/'tiny-primary.hict.hdf5'
    if source.is_symlink() or source.stat().st_nlink != 1:
        parser.error('Use a physical 17 bp reinforcement fixture copy')
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    output.mkdir(parents=True,exist_ok=False)

    class Quiet(http.server.SimpleHTTPRequestHandler):
        def log_message(self,*_): pass

    static = http.server.ThreadingHTTPServer(('127.0.0.1',0),
        functools.partial(Quiet,directory=str(Path(args.webui).resolve())))
    threading.Thread(target=static.serve_forever,daemon=True).start()
    try:
        with HictTestServer(args.jar,data,output/'server.log') as app, sync_playwright() as pw:
            app.http('/open',{'filename':source.name})
            browser = pw.chromium.launch(headless=True)
            page = browser.new_page(viewport={'width':1440,'height':1000})
            page.set_default_timeout(30000)
            page.add_init_script('window.__HICT_BENCHMARK_ENABLED__=true')
            page.route('http://localhost:5000/**',lambda route:route.continue_(
                url=app.url+route.request.url.removeprefix('http://localhost:5000')))
            check = BrowserCheck(page,output)
            page.goto(f'http://127.0.0.1:{static.server_port}/')
            check.menu('File','Attach'); check.settle()
            for offset in (5,1):
                page.evaluate('''async offset => {
                    const m=window.__hictBenchmark.manager();
                    m.eventManager.applyAssemblyInfo(await m.networkManager.requestManager.splitContigAtBp(
                        m.contigDimensionHolder.contigDescriptors[0].contigId,offset));
                    await m.reloadVisuals();
                }''',offset)
                check.settle()
            app.http('/reverse_selection_range',{'startBP':0,'endBP':17})
            app.http('/group_contigs_into_scaffold',{'startBP':0,'endBP':17})
            page.reload(); check.menu('File','Attach'); check.settle()
            mode = page.locator('select').filter(has=page.locator('option[value="EXPECTED"]'))
            mode.select_option('EXPECTED'); check.settle()
            assert 'repeated bin pairs share' in mode.get_attribute('title')
            options = json.loads(app.http('/get_visualization_options',{}))
            options.update(preLogBase=-1,postLogBase=-1,applyCoolerWeights=False,
                           resolutionScaling=False,resolutionLinearScaling=False)
            ids = [4,3,2,1,1,0,0]
            results = []
            for unit, row_start, row_end, col_start, col_end in (
                    ('PIXELS',0,7,0,7),('PIXELS',2,7,0,5),('PIXELS',3,7,4,7),('BINS',0,7,0,7)):
                # Clear the viewport cache, then fit the numeric fallback independently.
                app.http('/set_visualization_options',dict(options,signalDisplayMode='OBSERVED'))
                app.http('/set_visualization_options',dict(options,signalDisplayMode='EXPECTED'))
                query = {'bpResolution':4,'unit':unit,'startRow':row_start,'endRow':row_end,
                         'startCol':col_start,'endCol':col_end,'format':'JSON'}
                raw = json.loads(app.http('/matrix/query',dict(query,signalMode='RAW_COUNTS',includeTileStatistics=True)))
                expected = oracle(raw['values'],ids[row_start:row_end],ids[col_start:col_end],row_start,col_start)
                actual = json.loads(app.http('/matrix/query',dict(query,signalMode='TRADITIONAL_NORMALIZED')))
                assert len(actual['values']) == len(expected)
                error = max(abs(a-b) for a,b in zip(actual['values'],expected))
                assert error < 1e-12,(unit,error,actual,expected)
                fitted = json.loads(app.http('/visualization/expected_profile',{'bpResolution':4,'unit':unit,
                    'startRowPx':row_start,'endRowPx':row_end,'startColPx':col_start,'endColPx':col_end}))
                assert fitted['unit']==unit
                assert fitted['sampleSemantics']=='UNIQUE_UNORDERED_SOURCE_PAIR_MASS'
                assert fitted['sharedReferenceWeighting'] is True
                cached = json.loads(app.http('/matrix/query',dict(query,signalMode='TRADITIONAL_NORMALIZED')))
                assert cached['values']==actual['values']
                results.append({'unit':unit,'rows':[row_start,row_end],'cols':[col_start,col_end],'maxError':error})
            # Both cached windows cover the probe, but their fitted means differ.
            # A BINS fit must not replace the visible PIXELS fit (or conversely).
            profiles={}
            for unit,start in [('PIXELS',0),('BINS',2)]:
                query={'bpResolution':4,'unit':unit,'startRow':start,'endRow':7,
                       'startCol':start,'endCol':7,'format':'JSON'}
                raw=json.loads(app.http('/matrix/query',dict(query,signalMode='RAW_COUNTS')))
                profiles[unit]=(start,oracle(raw['values'],ids[start:7],ids[start:7],start,start))
                fitted=json.loads(app.http('/visualization/expected_profile',{'bpResolution':4,'unit':unit,
                    'startRowPx':start,'endRowPx':7,'startColPx':start,'endColPx':7}))
                assert fitted['unit']==unit
            for unit,(start,values) in profiles.items():
                actual=json.loads(app.http('/matrix/query',{'bpResolution':4,'unit':unit,'startRow':3,'endRow':7,
                    'startCol':3,'endCol':7,'format':'JSON','signalMode':'TRADITIONAL_NORMALIZED'}))['values']
                expected=[values[(r-start)*(7-start)+c-start] for r in range(3,7) for c in range(3,7)]
                error=max(abs(a-b) for a,b in zip(actual,expected))
                assert error<1e-12,(unit,'profile cache isolation',actual,expected)
                results.append({'unit':unit,'cacheIsolationMaxError':error})
            stats = page.request.post(app.url+'/matrix/query',data={'bpResolution':4,'rows':7,'cols':7,
                'format':'JSON','signalMode':'RAW_COUNTS','includeTileStatistics':True})
            assert stats.ok,stats.text()
            header = json.loads(stats.headers['x-hict-tile-statistics'])
            assert header['countSemantics']=='DISPLAY_CELLS'
            assert header['raw']['count']==49
            # Exercise the visible O/E control and the real selection histogram.
            mode.select_option('OBSERVED'); check.settle()
            mode.select_option('OBSERVED_OVER_EXPECTED'); check.settle()
            page.screenshot(path=str(output/'shared-oe.png'))
            check.select_range(); check.settle()
            point = page.evaluate('''() => {
                const m=window.__hictBenchmark.manager(),extent=m.viewAndLayersManager.selectionInteractions.contigSelectExtent.getExtent();
                const p=m.getMap().getPixelFromCoordinate([(extent[0]+extent[2])/2,(extent[1]+extent[3])/2]);
                const rect=m.getMap().getViewport().getBoundingClientRect();return [rect.left+p[0],rect.top+p[1]];
            }''')
            page.mouse.click(*point,button='right')
            page.get_by_role('menuitem',name='Signal histogram...',exact=True).click()
            dialog = page.get_by_role('dialog',name='Signal histogram',exact=True)
            dialog.get_by_text('counts displayed samples.',exact=False).wait_for()
            sequence_scale=page.evaluate('window.__hictBenchmark.manager().viewAndLayersManager.isSequenceScale')
            dialog.get_by_text('native matrix references' if sequence_scale else 'displayed matrix cells',exact=False).wait_for()
            page.screenshot(path=str(output/'shared-histogram.png'))
            assert not check.errors,check.errors
            assert hashlib.sha256(source.read_bytes()).hexdigest()==digest
            (output/'results.json').write_text(json.dumps({'status':'ok','cases':results,
                'checks':['exact cuts and reversal','independent unordered-pair oracle','cached and fallback profiles',
                          'BINS axis','O/E control','histogram display-sample labels','native source unchanged'],
                'pageErrors':check.errors,'sourceSha256':digest},indent=2))
            browser.close()
    finally:
        static.shutdown(); static.server_close()


if __name__=='__main__': main()
