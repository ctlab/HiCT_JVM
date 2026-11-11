#!/usr/bin/env python3
"""Verify failed manual replacement retains the live map and its linked sources."""
import argparse
import json
import time
from pathlib import Path
from playwright.sync_api import sync_playwright
from browser_review_helpers import link_fasta


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url', required=True)
    parser.add_argument('--api', required=True)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    result = {'setup': 'benchmark tiny primary, UI FASTA link, API secondary attachment/reversal, UI Attach',
              'pageErrors': [], 'unexpectedHttpErrors': [], 'checks': []}
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(viewport={'width': 1440, 'height': 1000})
        context.add_init_script('window.__HICT_BENCHMARK_ENABLED__=true')
        context.route('http://localhost:5000/**', lambda route: route.continue_(
            url=args.api + route.request.url.removeprefix('http://localhost:5000')))
        page = context.new_page()
        page.set_default_timeout(15000)
        page.on('pageerror', lambda error: result['pageErrors'].append(str(error)))
        page.on('response', lambda response: result['unexpectedHttpErrors'].append(
            {'url': response.url, 'status': response.status})
            if response.status >= 400 and not response.url.endswith('/open') else None)
        try:
            page.goto(args.url)
            page.wait_for_function('window.__hictBenchmark')
            page.evaluate("api=>window.__hictBenchmark.open('tiny-primary.hict.hdf5',api)", args.api)
            link_fasta(page, 'tiny.fa')
            page.evaluate("window.__hictBenchmark.manager().networkManager.requestManager.openSecondarySource('tiny-disjoint.hict.hdf5',true)")
            # Include the real initial autosave in the baseline rather than
            # mistaking its first publication for a replacement side effect.
            for _ in range(50):
                state = context.request.post(args.api + '/attach', data={})
                assert state.ok, state.text()
                if state.json().get('workspaceState') is not None:
                    break
                time.sleep(.1)
            else:
                raise AssertionError('Initial workspace autosave did not complete')

            edited = context.request.post(args.api + '/reverse_selection_range', data={'startBP': 0, 'endBP': 17})
            assert edited.ok, edited.text()
            page.reload()
            page.get_by_role('link', name='File', exact=True).click()
            page.get_by_role('link', name='Attach', exact=True).click()
            page.get_by_text('Attached to session tiny-primary.hict.hdf5', exact=True).wait_for(state='visible')
            page.evaluate('() => {window.__retainedManager=window.__hictBenchmark.manager();}')

            def snapshot():
                attached = context.request.post(args.api + '/attach', data={})
                assert attached.ok, attached.text()
                secondary = context.request.post(args.api + '/secondary/status', data={})
                assert secondary.ok, secondary.text()
                history = context.request.post(args.api + '/assembly/history', data={})
                assert history.ok, history.text()
                values = page.evaluate("""async()=>Array.from((await window.__hictBenchmark.manager().networkManager.requestManager.queryMatrixFloat32({
                    bpResolution:4,startRowPx:0,startColPx:0,endRowPx:5,endColPx:5,source:'PRIMARY',signalMode:'RAW_COUNTS'})).values)""")
                return {'attached': attached.json(), 'secondary': secondary.json(), 'history': history.json(), 'values': values}

            before = snapshot()
            result['before'] = before
            assert before['values'] == list(reversed([1,3,0,0,0,3,0,7,0,0,0,7,0,0,0,0,0,0,0,0,0,0,0,0,11]))
            assert before['history']['summary']['position'] > 0, before['history']
            for attempt in range(2):
                page.get_by_role('link', name='File', exact=True).click()
                page.get_by_role('link', name='Open…', exact=True).click()
                dialog = page.locator('.modal.show').filter(has_text='Open Hi-C dataset')
                dialog.locator('span[title]').filter(has_text='broken.hict.hdf5').click()
                with page.expect_response(lambda response: response.url.endswith('/open')) as failed:
                    dialog.get_by_role('button', name='Load', exact=True).click()
                    page.get_by_role('alertdialog', name='Unsaved assembly edits').get_by_role('button', name='Discard edits', exact=True).click()
                assert failed.value.status >= 400, failed.value.text()
                assert snapshot() == before, 'Failed replacement changed the active server session or pixels'
                assert page.evaluate('window.__hictBenchmark.manager()===window.__retainedManager')
                assert page.locator('#hic-contact-map canvas').count() > 0
                page.screenshot(path=str(output / f'failed-open-{attempt}.png'))
                result['checks'].append('failed manual open retained manager, source metadata and 25 exact cells')
            # Real navigation remains usable after the error; remount through Attach
            # also verifies that another client can recover the same live session.
            page.reload()
            page.get_by_role('link', name='File', exact=True).click()
            page.get_by_role('link', name='Attach', exact=True).click()
            page.wait_for_function('window.__hictBenchmark.manager()')
            page.get_by_text('Attached to session tiny-primary.hict.hdf5', exact=True).wait_for(state='visible')
            after_attach = snapshot()
            result['afterAttach'] = after_attach
            assert after_attach == before
            result['checks'].append('fresh client Attach retained primary, secondary, FASTA and pixels')
            assert not result['pageErrors'], result['pageErrors']
            assert not result['unexpectedHttpErrors'], result['unexpectedHttpErrors']
        finally:
            (output / 'results.json').write_text(json.dumps(result, indent=2) + '\n')
            browser.close()


if __name__ == '__main__':
    main()
