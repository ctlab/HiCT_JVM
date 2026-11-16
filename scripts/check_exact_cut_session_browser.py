#!/usr/bin/env python3
"""Restore exact shared-bin cuts through real session-v4 UI, then save/reopen them."""
import argparse
import functools
import http.server
import json
from pathlib import Path
import subprocess
import threading

from playwright.sync_api import sync_playwright
from sequence_browser_harness import HictTestServer, BrowserCheck


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ('jar', 'webui', 'data-dir', 'output'):
        parser.add_argument('--' + key, required=True)
    args = parser.parse_args()
    data, output = Path(args.data_dir).absolute(), Path(args.output).absolute()
    if data != data.resolve() or not data.is_relative_to('/tmp') or not output.is_relative_to('/tmp'):
        parser.error('Use nonsymlinked disposable fixture paths under /tmp')
    for filename in ('tiny-primary.hict.hdf5', 'tiny.fa'):
        path = data / filename
        if path.is_symlink() or path.stat().st_nlink != 1:
            parser.error('Use physical fixture copies')
    output.mkdir(parents=True, exist_ok=False)
    source = output / 'exact-source.json'
    with (output / 'fixture.log').open('w') as log:
        subprocess.run(['java', '--class-path', str(Path(args.jar).resolve()),
                        str(Path(__file__).with_name('ExactCutBrowserFixture.java')), str(data), str(source)],
                       check=True, stdout=log, stderr=subprocess.STDOUT)

    class Quiet(http.server.SimpleHTTPRequestHandler):
        def log_message(self, *_):
            pass

    static = http.server.ThreadingHTTPServer(('127.0.0.1', 0),
        functools.partial(Quiet, directory=str(Path(args.webui).resolve())))
    threading.Thread(target=static.serve_forever, daemon=True).start()
    try:
        with HictTestServer(args.jar, data, output / 'server.log') as app, sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            page = browser.new_page(viewport={'width': 1440, 'height': 1000}, accept_downloads=True)
            page.set_default_timeout(30000)
            page.add_init_script('window.__HICT_BENCHMARK_ENABLED__=true')
            page.route('http://localhost:5000/**', lambda route: route.continue_(
                url=app.url + route.request.url.removeprefix('http://localhost:5000')))
            check = BrowserCheck(page, output)
            app.http('/open', {'filename': 'tiny-primary.hict.hdf5'})
            app.http('/link_fasta', {'fastaFilename': 'tiny.fa'})
            original_contacts = {}
            for resolution, count in ((4, 5), (10, 2)):
                original_contacts[resolution] = json.loads(app.http('/matrix/query', {
                    'bpResolution': resolution, 'rows': count, 'cols': count,
                    'signalMode': 'RAW_COUNTS', 'format': 'JSON'}))
            page.goto(f'http://127.0.0.1:{static.server_port}/')
            check.menu('File', 'Attach')
            check.settle()
            with page.expect_download() as download:
                check.menu('File', 'Save session')
            baseline = output / 'baseline.json'
            download.value.save_as(baseline)
            envelope = json.loads(baseline.read_text())
            assert envelope['version'] == 4 and envelope['snapshot']['snapshotVersion'] == 2
            envelope['snapshot']['primary'].update(json.loads(source.read_text()))
            exact = output / 'exact-session.json'
            exact.write_text(json.dumps(envelope))

            def restore(path):
                with page.expect_file_chooser() as chooser:
                    check.menu('File', 'Open session')
                chooser.value.set_files(path)
                page.get_by_text('Session restored. Undo history starts from the restored assembly.', exact=True).last.wait_for()
                check.settle()

            restore(exact)
            expected = 'ACGTRYMKSWBDHVNAC'.translate(str.maketrans('ACGTRYMKSWBDHVN', 'TGCAYRKMSWVHDBN'))[::-1]
            sequence = json.loads(app.http('/sequence/region', {'startBp': 0, 'endBp': 17}))['sequence']
            assert sequence == expected, (sequence, expected)
            state = page.evaluate("""() => {
                const h=window.__hictBenchmark.manager().contigDimensionHolder,failures=[];
                for(const r of [4,10]) {
                    let pixel=0,bp=0;
                    for(const d of h.contigDescriptors) {
                        const a=d.contigOffsetInSource,b=a+d.contigLengthBp,intervals=[];
                        for(let s=Math.floor(a/r)*r;s<b;s+=r)intervals.push([Math.max(a,s),Math.min(b,s+r)]);
                        if(d.direction===0)intervals.reverse();
                        if(intervals.length!==d.contigLengthBins.get(r))failures.push('bin count');
                        intervals.forEach(([start,end],bin)=>{
                            const first=d.direction===0?b-end:start-a,last=d.direction===0?b-start:end-a;
                            for(let local=first;local<last;local++) {
                                if(h.getPxContainingBp(bp+local,r)!==pixel+bin)failures.push('bin position');
                                const p=h.getFractionalPxOfBp(bp+local+0.5,r);
                                if(Math.abs(h.getFractionalBpOfPx(p,r)-bp-local-0.5)>1e-8)failures.push('base position');
                            }
                        });
                        pixel+=intervals.length;bp+=d.contigLengthBp;
                    }
                    if(bp!==17)failures.push('sequence loss');
                }
                return {failures, lengths:h.contigDescriptors.map(d=>d.contigLengthBp)};
            }""")
            assert not state['failures'], state
            assert state['lengths'] == [12, 2, 3], state
            with page.expect_download() as download:
                check.menu('File', 'Save session')
            saved = output / 'resaved.json'
            download.value.save_as(saved)
            resaved = json.loads(saved.read_text())
            assert resaved['version'] == 4
            assert resaved['snapshot']['primary']['assemblyInfo'] == envelope['snapshot']['primary']['assemblyInfo']
            assert all(edit['exact'] for edit in resaved['snapshot']['primary']['splitEdits'])
            check.menu('File', 'Close')
            restore(saved)
            assert json.loads(app.http('/sequence/region', {'startBp': 0, 'endBp': 17}))['sequence'] == expected
            assert not check.errors, check.errors
            page.screenshot(path=str(output / 'restored-exact-cuts.png'))
            # Exercise the real request adapter and strict displayed-revision
            # endpoint. The scissors continue to use the compatibility path
            # until export/statistics handling is ready for the new geometry.
            with page.expect_request('**/split_contig_at_bp') as cut_request:
                page.evaluate("""async () => {
                    const m=window.__hictBenchmark.manager();
                    const id=m.contigDimensionHolder.contigDescriptors[0].contigId;
                    const assembly=await m.networkManager.requestManager.splitContigAtBp(id,5);
                    m.eventManager.applyAssemblyInfo(assembly);
                    await m.reloadVisuals();
                }""")
            check.settle()
            cut_headers = {k:v for k,v in cut_request.value.headers.items() if k.startswith('x-hict-history-')}
            conflict = page.request.post(app.url + '/split_contig_at_bp',
                data=cut_request.value.post_data_json, headers=cut_headers)
            assert conflict.status == 409, conflict.text()
            lengths = lambda: page.evaluate("window.__hictBenchmark.manager().contigDimensionHolder.contigDescriptors.map(d=>d.contigLengthBp)")
            assert lengths() == [5,7,2,3], lengths()
            assert page.evaluate("window.__hictBenchmark.manager().contigDimensionHolder.contigDescriptors.every(d=>d.contigSharedBinSlice===true)")
            provenance_checks = []
            for resolution in (4, 10):
                count = page.evaluate("""r => window.__hictBenchmark.manager().contigDimensionHolder
                    .contigDescriptors.reduce((n,d)=>n+d.contigLengthBins.get(r),0)""", resolution)
                request = {'bpResolution': resolution,
                    'rows': count, 'cols': count, 'signalMode': 'RAW_COUNTS', 'format': 'JSON',
                    'includeBinProvenance': True}
                response = page.evaluate("""async request => {
                    const response=await fetch('http://localhost:5000/matrix/query', {
                        method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(request)});
                    return {status:response.status, semantics:response.headers.get('x-hict-cell-semantics'),
                        tensor:await response.json()};
                }""", request)
                assert response['status'] == 200 and response['semantics'] == 'SOURCE_BIN_REFERENCES'
                numeric = response['tensor']
                assert numeric['cellSemantics'] == 'SOURCE_BIN_REFERENCES'
                metadata = numeric['binProvenance']
                assert metadata['source'] == 'PRIMARY' and metadata['bpResolution'] == resolution
                axis = metadata['rowAxis']
                assert axis == metadata['colAxis'] and axis['axisLength'] == count
                bins = [None] * count
                for run in axis['runs']:
                    assert run['stripeId'] == 0
                    for i in range(run['length']):
                        bins[run['offset']+i] = run['firstBinInStripe'] + i*run['step']
                original = original_contacts[resolution]
                assert axis['uniqueSourceBins'] == len(set(bins)) == original['rows']
                assert axis['referencedBins'] == count > axis['uniqueSourceBins']
                unique_contacts = {}
                for row, source_row in enumerate(bins):
                    for col, source_col in enumerate(bins):
                        value = numeric['values'][row*count+col]
                        assert value == original['values'][source_row*original['cols']+source_col]
                        unique_contacts[tuple(sorted((source_row,source_col)))] = value
                original_total = sum(original['values'][r*original['cols']+c]
                    for r in range(original['rows']) for c in range(r,original['cols']))
                assert sum(unique_contacts.values()) == original_total
                provenance_checks.append({'resolution': resolution, 'references': count,
                    'uniqueBins': len(set(bins)), 'uniqueContactSum': original_total})
            page.locator('#hic-contact-map').click(position={'x':100,'y':100})
            with page.expect_response('**/assembly/history/restore') as undo:
                page.keyboard.press('Control+z')
            assert undo.value.ok, undo.value.text()
            check.settle()
            assert lengths() == [12,2,3], lengths()
            with page.expect_response('**/assembly/history/restore') as redo:
                page.keyboard.press('Control+y')
            assert redo.value.ok, redo.value.text()
            check.settle()
            assert lengths() == [5,7,2,3], lengths()
            assert json.loads(app.http('/sequence/region', {'startBp':0,'endBp':17}))['sequence'] == expected
            with page.expect_download() as download:
                check.menu('File','Save session')
            api_saved = output / 'api-cut-session.json'
            download.value.save_as(api_saved)
            check.menu('File','Close')
            restore(api_saved)
            assert lengths() == [5,7,2,3], lengths()
            assert not check.errors, check.errors
            page.screenshot(path=str(output/'api-cut-restored.png'))
            (output / 'results.json').write_text(json.dumps({'status': 'ok', 'state': state, 'sequence': sequence,
                'provenance': provenance_checks,
                'checks': ['v4 save', 'exact restore', 'source-bin coordinate parity', 'gap-free sequence', 'save/close/reopen', 'guarded API cut', 'stale cut rejected', 'keyboard undo/redo', 'API cut save/restore', 'physical-bin query parity and unique-contact conservation']}, indent=2))
            browser.close()
    finally:
        static.shutdown()
        static.server_close()


if __name__ == '__main__':
    main()
