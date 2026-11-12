#!/usr/bin/env python3
"""Exercise secondary replacement errors, cancellation and successful edited alignment."""
import argparse
import json
import re
from pathlib import Path
from playwright.sync_api import sync_playwright


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for arg in ('url', 'api', 'output'):
        parser.add_argument('--' + arg, required=True)
    parser.add_argument('--secondary-drives-assembly', action='store_true')
    args = parser.parse_args()
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    result = {'setup': 'benchmark primary, API secondary and reversal, UI Attach',
              'checks': [], 'pageErrors': [], 'unexpectedHttpErrors': []}
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
            if response.status >= 400 and not response.url.endswith('/secondary/open') else None)

        def post(path, data=None):
            response = context.request.post(args.api + path, data=data or {})
            assert response.ok, response.text()
            return response.json()

        def snapshot():
            pixels = page.evaluate("""async()=>{
                const api=window.__hictBenchmark.manager().networkManager.requestManager;
                const query=source=>api.queryMatrixFloat32({bpResolution:4,startRowPx:0,startColPx:0,
                    endRowPx:5,endColPx:5,source,signalMode:'RAW_COUNTS'});
                return {primary:Array.from((await query('PRIMARY')).values),secondary:Array.from((await query('SECONDARY')).values)};
            }""")
            contigs = page.evaluate('''()=>window.__hictBenchmark.manager().contigDimensionHolder.contigDescriptors
                .map(c=>({id:c.contigId,name:c.contigName,direction:c.direction}))''')
            return {'status': post('/secondary/status'), 'history': post('/assembly/history'),
                    'pixels': pixels, 'clientContigs': contigs}

        def choose(filename):
            dialog = page.locator('.modal.show').filter(has=page.get_by_role('heading', name='Tracks and layers', exact=True))
            dialog.get_by_role('button', name='Browse…', exact=True).last.click()
            picker = page.locator('.modal.show').filter(has=page.get_by_role('heading', name='Select secondary data source', exact=True))
            picker.locator('span[title]').filter(has_text=re.compile('^' + re.escape(filename) + '$')).click()
            picker.get_by_role('button', name='Load', exact=True).click()
            picker.wait_for(state='hidden')
            with page.expect_response(lambda response: response.url.endswith('/secondary/open')) as opened:
                dialog.get_by_role('button', name='Attach', exact=True).click()
            return opened.value

        try:
            page.goto(args.url)
            page.wait_for_function('window.__hictBenchmark')
            page.evaluate("api=>window.__hictBenchmark.open('tiny-primary.hict.hdf5',api)", args.api)
            post('/secondary/open', {'filename': 'old-secondary.hict.hdf5'})
            post('/reverse_selection_range', {'startBP': 0, 'endBP': 17})
            page.reload()
            page.get_by_role('link', name='File', exact=True).click()
            page.get_by_role('link', name='Attach', exact=True).click()
            page.get_by_text('Attached to session tiny-primary.hict.hdf5', exact=True).wait_for(state='visible')
            before = snapshot()
            expected = list(reversed([1,3,0,0,0,3,0,7,0,0,0,7,0,0,0,0,0,0,0,0,0,0,0,0,11]))
            assert before['pixels'] == {'primary': expected, 'secondary': expected}
            page.get_by_role('link', name='View', exact=True).click()
            page.get_by_role('link', name='Tracks and layers…', exact=True).click()
            if args.secondary_drives_assembly:
                dialog = page.locator('.modal.show').filter(has=page.get_by_role('heading', name='Tracks and layers', exact=True))
                with page.expect_response(lambda response: response.url.endswith('/secondary/set_assembly_source')):
                    dialog.locator('select').select_option('SECONDARY')
                page.wait_for_function("()=>window.__hictBenchmark.manager().contigDimensionHolder.contigDescriptors[0].direction===0")
                before = snapshot()
            failed = choose('broken.hict.hdf5')
            assert failed.status >= 400, failed.text()
            assert snapshot() == before
            result['checks'].append('invalid secondary retains both exact edited matrices and journal')
            pending = choose('tiny-unrelated.hict.hdf5')
            assert pending.ok and pending.json()['requiresConfirmation']
            page.get_by_role('button', name='Cancel', exact=True).click()
            assert snapshot() == before
            result['checks'].append('cancelled mismatch leaves both sources unchanged')
            accepted = choose('candidate.hict.hdf5')
            assert accepted.ok and not accepted.json()['requiresConfirmation']
            page.get_by_text('Secondary source attached', exact=True).wait_for(state='attached')
            after = snapshot()
            assert after['status']['filename'] == 'candidate.hict.hdf5'
            if args.secondary_drives_assembly:
                forward = list(reversed(expected))
                assert after['pixels'] == {'primary': forward, 'secondary': forward}
                page.wait_for_function("()=>window.__hictBenchmark.manager().contigDimensionHolder.contigDescriptors[0].direction===1")
                after = snapshot()
                assert before['clientContigs'][0]['direction'] == 0
                assert after['clientContigs'][0]['direction'] == 1
                result['checks'].append('secondary-driven replacement refreshes client strand and both exact matrices')
            else:
                assert after['pixels'] == before['pixels']
                assert after['clientContigs'] == before['clientContigs']
                result['checks'].append('accepted candidate follows reversed primary assembly with exact pixels')
            page.screenshot(path=str(output / 'replacement.png'))
            result['before'], result['after'] = before, after
            pending = choose('tiny-unrelated.hict.hdf5')
            assert pending.ok and pending.json()['requiresConfirmation']
            with page.expect_response(lambda response: response.url.endswith('/secondary/open')) as confirmed:
                page.get_by_role('button', name='Continue and Attach', exact=True).click()
            assert confirmed.value.ok, confirmed.value.text()
            page.get_by_text('Secondary source attached with compatibility warning', exact=True).wait_for(state='attached')
            assembly = confirmed.value.json()['assemblyInfo']['contigDescriptors']
            expected_contigs = [{'id': c['contigId'], 'name': c['contigName'], 'direction': c['contigDirection']} for c in assembly]
            client_contigs = page.evaluate('''()=>window.__hictBenchmark.manager().contigDimensionHolder.contigDescriptors
                .map(c=>({id:c.contigId,name:c.contigName,direction:c.direction}))''')
            assert client_contigs == expected_contigs
            assert post('/secondary/status')['filename'] == 'tiny-unrelated.hict.hdf5'
            result['checks'].append('confirmed mismatched replacement applies the returned assembly to the client')
            dialog = page.locator('.modal.show').filter(has=page.get_by_role('heading', name='Tracks and layers', exact=True))
            for source in ('PRIMARY', 'SECONDARY'):
                with page.expect_response(lambda response: response.url.endswith('/secondary/set_assembly_source')) as switched:
                    dialog.locator('select').select_option(source)
                assert switched.value.ok, switched.value.text()
                assert set(switched.value.json()['assemblyInfo']['contigDescriptors'][0]['contigLengthBins']) == {'4', '10'}
            page.wait_for_function('''()=>window.__hictBenchmark.manager().contigDimensionHolder.contigDescriptors[0]
                .contigLengthBins.has(4)''')
            page.screenshot(path=str(output / 'mismatched-replacement.png'))
            result['checks'].append('assembly-source switching keeps the primary coordinate grid for disjoint pyramids')
            assert not result['pageErrors'], result['pageErrors']
            assert not result['unexpectedHttpErrors'], result['unexpectedHttpErrors']
        finally:
            (output / 'results.json').write_text(json.dumps(result, indent=2) + '\n')
            browser.close()


if __name__ == '__main__':
    main()
