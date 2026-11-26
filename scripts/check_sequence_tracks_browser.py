#!/usr/bin/env python3
"""Qualify real base-grid tracks on binless, hidden and visible source sequences."""
import argparse
import functools
import hashlib
import http.server
import json
from pathlib import Path
import subprocess
import threading
from playwright.sync_api import sync_playwright
from sequence_browser_harness import HictTestServer, BrowserCheck, configure_test_api


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    for key in ('jar','webui','data-dir','output'):parser.add_argument('--'+key,required=True)
    args=parser.parse_args()
    data,out=Path(args.data_dir).absolute(),Path(args.output).absolute()
    if any(p!=p.resolve() or not p.is_relative_to('/tmp') for p in (data,out)):
        parser.error('Use fresh nonsymlinked disposable directories under /tmp')
    data.mkdir(parents=True,exist_ok=False);out.mkdir(parents=True,exist_ok=False)
    with (out/'fixture.log').open('w') as log:
        subprocess.run(['java','--class-path',str(Path(args.jar).resolve()),str(Path(__file__).with_name('SequenceTrackBrowserFixture.java')),str(data)],
                       check=True,stdout=log,stderr=subprocess.STDOUT)
    source=data/'tracks.hict.hdf5';digest=hashlib.sha256(source.read_bytes()).hexdigest()
    class Quiet(http.server.SimpleHTTPRequestHandler):
        def log_message(self,*_):pass
    static=http.server.ThreadingHTTPServer(('127.0.0.1',0),functools.partial(Quiet,directory=str(Path(args.webui).resolve())))
    threading.Thread(target=static.serve_forever,daemon=True).start()
    try:
        with HictTestServer(args.jar,data,out/'server.log') as app,sync_playwright() as pw:
            opened=json.loads(app.http('/open',{'filename':source.name}))
            app.http('/link_fasta',{'fastaFilename':'tracks.fa'})
            browser=pw.chromium.launch(headless=True)
            page=browser.new_page(viewport={'width':1600,'height':1100},accept_downloads=True)
            page.set_default_timeout(30000);page.add_init_script('window.__HICT_BENCHMARK_ENABLED__=true')
            configure_test_api(page, app.url)
            check=BrowserCheck(page,out);requests=[]
            page.on('request',lambda r:requests.append(r.post_data_json) if r.url.endswith('/tracks/query_1d') else None)
            page.goto(f'http://127.0.0.1:{static.server_port}/');check.menu('File','Join running session');check.settle()
            page.evaluate('''async()=>{const t=window.__hictBenchmark.manager().linearTrackManager;
                await t.openTrack('genes.gtf','Genes');await t.openTrack('signal.bed','Signal');await t.openCoolerWeightsTrack('Weights');}''');check.settle()
            def focus(bp):
                page.evaluate('''bp=>window.__hictBenchmark.manager().viewAndLayersManager.restoreViewPosition({centerBp:[bp,bp],resolution:1/2000,rotation:0})''',bp);check.settle()
            def snapshot():
                return page.evaluate('''()=>{const m=window.__hictBenchmark.manager();return {tracks:m.linearTrackManager.lastRenderedSnapshot.horizontal.tracks,
                    hits:m.linearTrackManager.lastRenderedSnapshot.horizontal.featureHits,view:m.viewAndLayersManager.captureViewPosition()};}''')
            def query(start,end,res=100,sequence=True):
                return json.loads(app.http('/tracks/query_1d',{'unit':'BP','startBP':start,'endBP':end,'widthPx':max(1,end-start),
                    'bpResolution':res,'sequenceScale':sequence}))
            def value(track,bp):
                hits=[b for b in track['bins'] if b['startBp']<=bp<b['endBp']]
                return hits[0]['value'] if hits else None
            legacy=query(0,12,sequence=False)
            assert not any(b['label'] in ('binless','hidden') for b in legacy['tracks'][0]['bins']),legacy
            focus(8)
            page.wait_for_function('''()=>window.__hictBenchmark.manager().linearTrackManager.lastRenderedSnapshot.horizontal?.featureHits.some(h=>h.bin.label==='binless')''')
            first=snapshot();assert first['view']['displayGrid']=='SEQUENCE_BP',first['view']
            byname={t['name']:t for t in first['tracks']}
            assert any(b['label']=='binless' and (b['startBp'],b['endBp'])==(0,1) for b in byname['Genes']['bins'])
            assert any(b['label']=='hidden' and (b['startBp'],b['endBp'])==(1,2) for b in byname['Genes']['bins'])
            assert [value(byname['Signal'],p) for p in range(5)]==[7,9,9,4,4],byname['Signal']
            assert [value(byname['Weights'],p) for p in range(5)]==[None,20,20,100,100],byname['Weights']
            assert byname['Weights']['samplingBpResolution']==100
            assert all(t['error'] is None and t['statusMessage'] is None for t in first['tracks'])
            assert page.locator('#normalization-mode option[value="file"]').is_enabled()
            assert not page.locator('#imported-weights-unavailable').count()
            # Hover a native-weight value on hidden sequence and inspect the scientific resolution label.
            track_host = page.locator('#horizontal-igv-track-div')
            weights_lane = track_host.get_by_role('region', name='Weights feature rows', exact=True)
            lane_bounds = weights_lane.bounding_box()
            assert lane_bounds and lane_bounds['height'] > 0
            weight_x = track_host.evaluate('''host => {
                const tracks = window.__hictBenchmark.manager().linearTrackManager;
                return host.getBoundingClientRect().left +
                    tracks.lastRenderedSnapshot.horizontal.viewport.pxToScreen(1.5);
            }''')
            page.mouse.move(weight_x, lane_bounds['y'] + lane_bounds['height'] / 2)
            page.get_by_text('Native weights: 100 bp bins',exact=False).last.wait_for()
            page.get_by_text('Assembly 2-2 bp (1-based, inclusive)',exact=True).last.wait_for()
            page.screenshot(path=str(out/'hidden-binless-tracks.png'))
            page.mouse.move(800,80)
            for res in (100,200):
                api=query(0,12,res)
                assert api['displayGrid']=='SEQUENCE_BP' and api['bpResolution']==1 and api['sourceBpResolution']==res,api
                assert [value(api['tracks'][1],p) for p in range(5)]==[7,9,9,4,4],api
                assert [value(api['tracks'][2],p) for p in range(5)]==[None,20,20,100,100],api
                assert api['tracks'][2]['samplingBpResolution']==res
            context=json.loads(app.http('/tracks/feature_context',{'unit':'BP','startBP':0,'endBP':1,'widthPx':64,'bpResolution':100,'sequenceScale':True,'marginScreens':0}))
            assert context['query']['displayGrid']=='SEQUENCE_BP'
            assert any(b['label']=='binless' for b in context['query']['tracks'][0]['bins'])
            coarse=json.loads(app.http('/tracks/query_1d',{'unit':'BP','startBP':0,'endBP':12,'widthPx':1,'bpResolution':100,'sequenceScale':True}))
            assert value(coarse['tracks'][2],0) is None,coarse
            assert value(coarse['tracks'][2],1)==100,coarse
            # Cut within the hidden two-base source using its one-base feature's right edge.
            point=page.evaluate('''()=>{const t=window.__hictBenchmark.manager().linearTrackManager,s=t.lastRenderedSnapshot.horizontal;
                const h=s.featureHits.find(h=>h.bin.label==='hidden'),r=document.querySelector('#horizontal-igv-track-div').getBoundingClientRect();
                return [r.left+(h.axisStart+h.axisEnd)/2,r.top+(h.crossStart+h.crossEnd)/2];}''')
            page.mouse.click(*point,button='right')
            with page.expect_response('**/split_contig_at_bp') as response:
                page.get_by_role('menu',name='Genes',exact=True).get_by_role('menuitem',name='Cut at feature end',exact=True).click()
            assert response.value.ok,response.value.text()
            assert response.value.request.post_data_json['offsetBp']==1
            check.settle()
            assert [value(query(0,5)['tracks'][2],p) for p in range(5)]==[None,20,20,100,100]
            page.evaluate('''()=>{const m=window.__hictBenchmark.manager(),h=m.contigDimensionHolder;
                m.viewAndLayersManager.selectEntityRangeByBp(0,h.prefix_sum_bp[h.contig_count]);}''')
            with page.expect_response('**/reverse_selection_range') as response:
                page.get_by_role('button',name='Reverse Selection',exact=True).click()
            assert response.value.ok;check.settle();focus(3998)
            reversed_query=query(3998,4003)
            assert [value(reversed_query['tracks'][1],p) for p in range(3998,4003)]==[4,4,9,9,7],reversed_query
            assert [value(reversed_query['tracks'][2],p) for p in range(3998,4003)]==[100,100,20,20,None],reversed_query
            assert any(b['label']=='binless' and b['startBp']==4002 and b['strand']=='-' for b in reversed_query['tracks'][0]['bins'])
            with page.expect_download() as saved:check.menu('File','Save session…')
            session=out/'sequence-tracks-session.json';saved.value.save_as(session)
            check.menu('File','Close map')
            with page.expect_file_chooser() as chooser:check.menu('File','Open session…')
            chooser.value.set_files(session)
            page.get_by_text('Session restored. Undo history starts from the restored assembly.',exact=True).last.wait_for();check.settle()
            assert query(3998,4003)['tracks']==reversed_query['tracks']
            page.screenshot(path=str(out/'restored-sequence-tracks.png'))
            for extra in ({'sequenceScale':'true'},{'sequenceScale':True,'unit':'PIXELS'},
                          {'sequenceScale':True,'widthPx':8193},{'sequenceScale':True,'endBP':1_048_577}):
                bad={'sequenceScale':True,'unit':'BP','startBP':0,'endBP':10,'widthPx':10,'bpResolution':100,**extra}
                rejected=page.request.post(app.url+'/tracks/query_1d',data=bad)
                assert 400<=rejected.status<500,(bad,rejected.status,rejected.text())
            assert any(r.get('sequenceScale') is True and r['unit']=='BP' and r['bpResolution']==100 for r in requests),requests
            assert hashlib.sha256(source.read_bytes()).hexdigest()==digest
            assert not check.errors,check.errors
            (out/'results.json').write_text(json.dumps({'status':'ok','first':first,'reversed':reversed_query,'context':context,'requests':requests,
                'coarse':coarse,'sourceSha256':digest,'pageErrors':check.errors,'checks':['real hidden and binless components','visible-native behavior retained',
                    'base-aligned GTF and quantitative BED','no fabricated binless weights','native weight metadata and tooltip','both stored levels',
                    'base feature context','real feature cut within hidden sequence','reversal','session restoration','invalid query rejection',
                    'coarse weight support excludes missing bases','native bytes unchanged']},indent=2))
            browser.close()
    finally:static.shutdown();static.server_close()


if __name__=='__main__':main()
