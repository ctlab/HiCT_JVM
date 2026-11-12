#!/usr/bin/env python3
"""Verify that failed actions report one message and expose copyable diagnostics."""
import argparse
import json
from pathlib import Path

from playwright.sync_api import sync_playwright


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('url', 'api', 'output'):
        parser.add_argument('--' + name, required=True)
    parser.add_argument('--file', default='tiny-primary.hict.hdf5')
    parser.add_argument('--fasta', default='tiny.fa')
    parser.add_argument('--track', default='session-track.bed')
    parser.add_argument('--coverage-file', help='A map large enough for the coverage job to overlap a second request')
    args = parser.parse_args()
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    checks = []
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(
            viewport={'width': 1440, 'height': 900},
            permissions=['clipboard-read', 'clipboard-write'],
        )
        context.add_init_script('window.__HICT_BENCHMARK_ENABLED__=true')
        context.route('http://localhost:5000/**', lambda route: route.continue_(
            url=args.api + route.request.url.removeprefix('http://localhost:5000')))
        page = context.new_page()
        page.set_default_timeout(15000)

        def post(path, body=None):
            response = context.request.post(args.api + path, data=body or {})
            assert response.ok, response.text()
            return response.json()

        def menu(group, action):
            page.get_by_role('link', name=group, exact=True).click()
            page.get_by_role('link', name=action, exact=True).click()

        def choose(dialog, filename):
            dialog.locator('.file-name-cell').get_by_text(filename, exact=True).click()
            dialog.get_by_role('button', name='Load', exact=True).click()

        def force_failure(path, reason):
            page.route('**' + path, lambda route: route.fulfill(status=400, json={'error': reason}))

        def check_toast(reason, operation, *, copy=False):
            toast = page.locator('[data-sonner-toast]').filter(has_text=reason)
            toast.wait_for()
            page.wait_for_timeout(300)
            assert toast.count() == 1, toast.all_text_contents()
            assert 'AxiosError' not in toast.inner_text(), toast.inner_text()
            if copy:
                toast.get_by_role('button', name='Details', exact=True).click()
                modal = page.get_by_role('dialog').filter(has=page.get_by_role('heading', name='Notifications', exact=True))
                modal.get_by_role('button', name='Copy diagnostics', exact=True).first.click()
                details = page.evaluate('() => navigator.clipboard.readText()')
                assert operation in details and reason in details and 'HTTP 400' in details, details
                modal.get_by_role('button', name='Close', exact=True).click()
            checks.append(operation)
            print('PASS:', operation)

        try:
            post('/close')
            page.goto(args.url)
            page.get_by_role('button', name='Open map…', exact=True).click()
            reason = 'The map header could not be read.'
            force_failure('/open', reason)
            choose(page.get_by_role('dialog'), args.file)
            check_toast(reason, 'Open map', copy=True)
            page.unroute('**/open')

            page.evaluate('([file,api])=>window.__hictBenchmark.open(file,api)', [args.file, args.api])
            menu('Assembly', 'Link FASTA')
            reason = 'The FASTA index could not be read.'
            force_failure('/link_fasta', reason)
            choose(page.get_by_role('dialog'), args.fasta)
            check_toast(reason, 'Link FASTA')
            page.unroute('**/link_fasta')
            page.keyboard.press('Escape')

            menu('View', 'Tracks and layers…')
            tracks = page.get_by_role('dialog').filter(has=page.get_by_role('heading', name='Tracks and layers', exact=True))
            tracks.get_by_role('button', name='Browse…', exact=True).first.click()
            choose(page.get_by_role('dialog').last, args.track)
            reason = 'The track contains a malformed interval.'
            force_failure('/tracks/open', reason)
            tracks.get_by_role('button', name='Add file', exact=True).click()
            check_toast(reason, 'Add track')
            page.unroute('**/tracks/open')
            tracks.get_by_role('button', name='Close', exact=True).click()

            reason = 'Session verification was interrupted.'
            force_failure('/session/snapshot', reason)
            menu('File', 'Save session')
            dialog = page.get_by_role('alertdialog', name='Session could not be saved')
            dialog.get_by_text(reason, exact=True).wait_for()
            assert page.get_by_text(reason, exact=True).count() == 1
            dialog.get_by_role('button', name='Copy diagnostics').click()
            assert reason in page.evaluate('() => navigator.clipboard.readText()')
            dialog.get_by_role('button', name='OK', exact=True).click()
            page.unroute('**/session/snapshot')
            checks.append('Save session')
            print('PASS: Save session')

            reason = 'The export renderer could not produce an image.'
            page.evaluate('message => { window.__hictBenchmark.manager().exportCurrentMapPng = async () => { throw new Error(message); }; }', reason)
            page.locator('#export-dropdown-toggle').click()
            page.get_by_title('Export selected area as PNG').click()
            check_toast(reason, 'Export PNG')

            page.evaluate('''() => {
              console.error("Console-only diagnostic");
              void Promise.reject(new Error("Rejected background operation"));
            }''')
            page.wait_for_timeout(300)
            assert page.locator('[data-sonner-toast]').filter(has_text='Console-only diagnostic').count() == 0
            assert page.locator('[data-sonner-toast]').filter(has_text='Rejected background operation').count() == 0
            checks.append('Console and unhandled rejection stay out of toasts')

            if args.coverage_file:
                post('/open', {'filename': args.coverage_file})
                states = page.evaluate('''async api => {
                    const prepare = () => fetch(api + '/normalization/coverage/prepare', {
                        method: 'POST', headers: {'content-type': 'application/json'}, body: '{}'
                    }).then(response => response.status);
                    return Promise.all([prepare(), prepare()]);
                }''', args.api)
                assert sorted(states) == [200, 409], states
                post('/normalization/coverage/cancel')
                checks.append('Second coverage preparation returns HTTP 409')
        finally:
            (output / 'results.json').write_text(json.dumps(checks, indent=2) + '\n')
            page.screenshot(path=str(output / 'failure-reporting.png'))
            browser.close()


if __name__ == '__main__':
    main()
