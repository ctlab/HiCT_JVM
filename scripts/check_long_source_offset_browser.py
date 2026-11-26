#!/usr/bin/env python3
"""Verify exact cuts and portable sessions beyond signed 32-bit source coordinates."""
import argparse
import functools
import http.server
import json
from pathlib import Path
import threading

from playwright.sync_api import sync_playwright
from sequence_browser_harness import HictTestServer, BrowserCheck, configure_test_api


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ('jar', 'webui', 'data-dir', 'output'):
        parser.add_argument('--' + key, required=True)
    args = parser.parse_args()
    data, output = Path(args.data_dir).absolute(), Path(args.output).absolute()
    if data != data.resolve() or not data.is_relative_to('/tmp') or not output.is_relative_to('/tmp'):
        parser.error('Use nonsymlinked disposable fixture paths under /tmp')
    source = data / 'long-source.hict.hdf5'
    if source.is_symlink() or source.stat().st_nlink != 1:
        parser.error('Use a physical long-offset reinforcement fixture copy')
    output.mkdir(parents=True, exist_ok=False)

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
            configure_test_api(page, app.url)
            check = BrowserCheck(page, output)
            app.http('/open', {'filename': 'long-source.hict.hdf5'})
            page.goto(f'http://127.0.0.1:{static.server_port}/')
            check.menu('File', 'Join running session')
            check.settle()

            for position, offset in ((0, 2_500_000_001), (-1, 1)):
                page.evaluate("""async ({position,offset}) => {
                    const m=window.__hictBenchmark.manager();
                    const id=m.contigDimensionHolder.contigDescriptors.at(position).contigId;
                    const assembly=await m.networkManager.requestManager.splitContigAtBp(id,offset);
                    m.eventManager.applyAssemblyInfo(assembly); await m.reloadVisuals();
                }""", {'position': position, 'offset': offset})
                check.settle()

            def state():
                return page.evaluate("""() => window.__hictBenchmark.manager().contigDimensionHolder
                    .contigDescriptors.map(d=>({offset:d.contigOffsetInSource,length:d.contigLengthBp}))""")

            expected = [{'offset': 0, 'length': 2_500_000_001},
                        {'offset': 2_500_000_001, 'length': 1},
                        {'offset': 2_500_000_002, 'length': 500_000_015}]
            assert state() == expected, state()
            with page.expect_download() as download:
                check.menu('File', 'Save session…')
            saved = output / 'long-offset-session.json'
            download.value.save_as(saved)
            envelope = json.loads(saved.read_text())
            snapshot = envelope['snapshot']['primary']
            assert snapshot['splitEdits'][-1]['sourceOffsetBp'] == 2_500_000_001
            assert snapshot['assemblyInfo']['contigDescriptors'][-1]['contigOffsetInSource'] == 2_500_000_002
            check.menu('File', 'Close map')
            with page.expect_file_chooser() as chooser:
                check.menu('File', 'Open session…')
            chooser.value.set_files(saved)
            page.get_by_text('Session restored. Undo history starts from the restored assembly.', exact=True).last.wait_for()
            check.settle()
            assert state() == expected, state()
            agp = json.loads(app.http('/session/snapshot', {'verifyAssemblyRoundTrip': True}))['primary']['agp']
            assert '\ttiny\t2500000002\t2500000002\t+' in agp
            assert '\ttiny\t2500000003\t3000000017\t+' in agp
            assert not check.errors, check.errors
            page.screenshot(path=str(output / 'long-offset-restored.png'))
            (output / 'results.json').write_text(json.dumps({'status': 'ok', 'state': state(),
                'checks': ['nested exact cuts beyond 32-bit coordinates', 'compact response offsets',
                           'saved lineage offsets', 'session save/close/reopen', 'AGP coordinates'],
                'pageErrors': check.errors}, indent=2))
            browser.close()
    finally:
        static.shutdown()
        static.server_close()


if __name__ == '__main__':
    main()
