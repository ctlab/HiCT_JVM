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
                page.get_by_text('Session restored. Undo history starts from the restored assembly.', exact=True).wait_for()
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
            (output / 'results.json').write_text(json.dumps({'status': 'ok', 'state': state, 'sequence': sequence,
                'checks': ['v4 save', 'exact restore', 'source-bin coordinate parity', 'gap-free sequence', 'save/close/reopen']}, indent=2))
            browser.close()
    finally:
        static.shutdown()
        static.server_close()


if __name__ == '__main__':
    main()
