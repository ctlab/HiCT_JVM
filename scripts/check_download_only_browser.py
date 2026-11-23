#!/usr/bin/env python3
"""Exercise real download-only browser exports and unsaved guards without a writable-picker shim."""
import argparse
import functools
import hashlib
import http.server
import json
from pathlib import Path
import subprocess
import threading

from playwright.sync_api import sync_playwright
from sequence_browser_harness import HictTestServer, BrowserCheck


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('jar', 'webui', 'data-dir', 'output'):
        parser.add_argument('--' + name, required=True)
    parser.add_argument('--browser', choices=('firefox', 'webkit'), required=True)
    args = parser.parse_args()
    data, out = Path(args.data_dir).absolute(), Path(args.output).absolute()
    for path in (data, out):
        if path != path.resolve() or not path.is_relative_to('/tmp'):
            parser.error('Use fresh physical /tmp paths')
        path.mkdir(parents=True, exist_ok=False)
    repo = Path(__file__).resolve().parents[1]
    with (out / 'fixture.log').open('w') as log:
        subprocess.run(['java', '--class-path', str(Path(args.jar).resolve()),
            str(repo / 'src/test/java/ru/itmo/ctlab/hict/fixtures/ReinforcementHdf5Fixture.java'), str(data)],
            check=True, stdout=log, stderr=subprocess.STDOUT)
    hashes = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in data.iterdir() if p.is_file()}
    source = ''.join(line for line in (data / 'tiny.fa').read_text().splitlines() if not line.startswith('>'))
    expected = source.translate(str.maketrans('ACGTRYMKSWBDHVN', 'TGCAYRKMSWVHDBN'))[::-1]

    class Quiet(http.server.SimpleHTTPRequestHandler):
        def log_message(self, *_):
            pass

    static = http.server.ThreadingHTTPServer(('127.0.0.1', 0),
        functools.partial(Quiet, directory=str(Path(args.webui).resolve())))
    threading.Thread(target=static.serve_forever, daemon=True).start()
    try:
        with HictTestServer(args.jar, data, out / 'server.log') as app, sync_playwright() as pw:
            app.http('/open', {'filename': 'tiny-fine.hict.hdf5'})
            app.http('/link_fasta', {'fastaFilename': 'tiny.fa'})
            browser = getattr(pw, args.browser).launch(headless=True)
            page = browser.new_page(viewport={'width': 1440, 'height': 1000}, accept_downloads=True)
            page.set_default_timeout(30000)
            page.add_init_script('window.__HICT_BENCHMARK_ENABLED__=true')
            page.route('http://localhost:5000/**', lambda route: route.continue_(
                url=app.url + route.request.url.removeprefix('http://localhost:5000')))
            check = BrowserCheck(page, out, writable_picker=False)
            page.goto(f'http://127.0.0.1:{static.server_port}/')
            check.menu('File', 'Join running session')
            check.settle()
            capabilities = page.evaluate('({savePicker:typeof showSaveFilePicker,openPicker:typeof showOpenFilePicker,userAgent:navigator.userAgent})')
            assert capabilities['savePicker'] == 'undefined' and capabilities['openPicker'] == 'undefined', capabilities

            def dirty():
                return page.evaluate('window.__hictBenchmark.manager().networkManager.requestManager.hasUnsavedAssemblyEdits()')

            def reverse():
                page.evaluate('window.__hictBenchmark.manager().viewAndLayersManager.selectEntityRangeByBp(0,17)')
                with page.expect_response('**/reverse_selection_range') as response:
                    page.get_by_role('button', name='Reverse Selection', exact=True).click()
                assert response.value.ok
                check.settle()
                assert dirty()
            check.step('reverse-through-real-button', reverse)

            def session_download():
                with page.expect_download() as download:
                    check.menu('File', 'Save session…')
                download.value.save_as(out / 'downloaded-session.json')
                assert json.loads((out / 'downloaded-session.json').read_text())['version'] == 4
                page.get_by_text('Session download started.', exact=False).last.wait_for()
                assert dirty(), 'An unconfirmed browser download must not clear the dirty revision'
            check.step('session-download-keeps-unconfirmed-revision-dirty', session_download)

            def guarded_close():
                check.menu('File', 'Close map')
                dialog = page.get_by_role('alertdialog', name='Unsaved assembly edits', exact=True)
                with page.expect_download() as download:
                    dialog.get_by_role('button', name='Save session', exact=True).click()
                download.value.save_as(out / 'guarded-session.json')
                assert dialog.is_visible() and dirty()
                # The modal's keyboard focus must stay among its own controls.
                dialog.get_by_role('button', name='Cancel', exact=True).focus()
                for _ in range(5):
                    page.keyboard.press('Tab')
                    assert dialog.evaluate('e=>e.contains(document.activeElement)')
                dialog.get_by_role('button', name='Cancel', exact=True).click()
                dialog.wait_for(state='hidden')
            check.step('download-does-not-dismiss-unsaved-guard-and-focus-is-trapped', guarded_close)

            def fasta():
                with page.expect_download() as download:
                    check.menu('Assembly', 'Export FASTA…')
                download.value.save_as(out / 'assembly.fasta')
                actual = ''.join(line for line in (out / 'assembly.fasta').read_text().splitlines() if not line.startswith('>'))
                assert actual == expected, (actual, expected)
            check.step('whole-fasta-download-matches-independent-reverse-complement', fasta)

            def agp():
                with page.expect_download() as download:
                    check.menu('Assembly', 'Export AGP…')
                download.value.save_as(out / 'assembly.agp')
                rows = [line.split('\t') for line in (out / 'assembly.agp').read_text().splitlines() if line and not line.startswith('#')]
                assert len(rows) == 1 and rows[0][1:3] == ['1', '17'], rows
                assert rows[0][4:] == ['W', 'tiny', '1', '17', '-'], rows
            check.step('agp-download-retains-complete-reversed-reference', agp)

            def selected():
                check.menu('Selection', 'Select region by coordinates…')
                dialog = page.get_by_role('dialog', name='Select region by coordinates', exact=True)
                dialog.locator('#region-convention').select_option('zero-based')
                for key, value in {'row-start': 2, 'row-end': 11, 'column-start': 4, 'column-end': 9}.items():
                    dialog.locator('#region-' + key).fill(str(value))
                dialog.get_by_role('button', name='Select region', exact=True).click()
                dialog.wait_for(state='hidden')
                with page.expect_download() as download:
                    page.get_by_role('button', name='Export FASTA for selection', exact=True).click()
                download.value.save_as(out / 'selection.fasta')
                records = []
                for line in (out / 'selection.fasta').read_text().splitlines():
                    if line.startswith('>'):
                        records.append('')
                    else:
                        records[-1] += line
                assert sorted(records) == sorted([expected[4:9], expected[2:11]]), records
                page.keyboard.press('Escape')
            check.step('typed-selection-download-retains-exact-both-axis-sequences', selected)

            for extension in ('png', 'svg', 'pdf'):
                def figure(extension=extension):
                    page.get_by_role('button', name='Export', exact=True).click()
                    with page.expect_download(timeout=90000) as download:
                        page.locator('#export-' + extension + '-button').click()
                    path = out / ('figure.' + extension)
                    download.value.save_as(path)
                    content = path.read_bytes()
                    assert len(content) > 1000
                    if extension == 'png':
                        assert content.startswith(b'\x89PNG\r\n\x1a\n')
                    elif extension == 'pdf':
                        assert content.startswith(b'%PDF-')
                    else:
                        assert b'<svg' in content
                    page.get_by_role('dialog', name='Export figure', exact=True).get_by_role('button', name='Close', exact=True).last.click()
                check.step(extension + '-figure-download-through-real-export-control', figure)

            def reopen_figure():
                check.menu('File', 'Open exported figure…')
                viewer = page.get_by_role('dialog', name='Exported figure', exact=True)
                with page.expect_file_chooser() as chooser:
                    viewer.get_by_role('button', name='Open PNG or SVG…', exact=True).click()
                chooser.value.set_files(out / 'figure.png')
                image = viewer.get_by_role('img')
                image.wait_for()
                assert image.evaluate('e=>e.complete&&e.naturalWidth>10&&e.naturalHeight>10')
                viewer.get_by_role('button', name='Zoom in figure', exact=True).click()
                viewer.get_by_role('button', name='Fit', exact=True).click()
                viewer.get_by_role('button', name='Close', exact=True).last.click()
            check.step('downloaded-png-opens-in-real-figure-viewer', reopen_figure)

            def restore_session():
                check.menu('File', 'Close map')
                page.get_by_role('alertdialog', name='Unsaved assembly edits', exact=True).get_by_role('button', name='Discard edits', exact=True).click()
                with page.expect_file_chooser() as chooser:
                    check.menu('File', 'Open session…')
                chooser.value.set_files(out / 'downloaded-session.json')
                page.get_by_text('Session restored. Undo history starts from the restored assembly.', exact=True).last.wait_for()
                check.settle()
                assert not dirty()
                assert json.loads(app.http('/sequence/region', {'startBp': 0, 'endBp': 17}))['sequence'] == expected
            check.step('downloaded-session-restores-through-html-file-input', restore_session)
            assert not check.errors, check.errors
            for name, digest in hashes.items():
                assert hashlib.sha256((data / name).read_bytes()).hexdigest() == digest, name
            (out / 'results.json').write_text(json.dumps({'status': 'passed', 'browser': args.browser,
                'browserVersion': browser.version, 'capabilities': capabilities, 'checks': check.rows,
                'sourceHashesUnchanged': hashes, 'pageErrors': check.errors}, indent=2) + '\n')
            browser.close()
    finally:
        static.shutdown()
        static.server_close()


if __name__ == '__main__':
    main()
