#!/usr/bin/env python3
"""Reject malformed session files through the actual chooser without replacing an edited map."""
import argparse
import json
from pathlib import Path
from playwright.sync_api import sync_playwright


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ('url', 'api', 'output'):
        parser.add_argument('--' + key, required=True)
    parser.add_argument('--legacy-file', help='Optional legacy fixture to restore instead of the downloaded file')
    args = parser.parse_args()
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    result = {'checks': [], 'pageErrors': [], 'mutationRequests': []}
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(viewport={'width': 1440, 'height': 1000}, accept_downloads=True)
        context.add_init_script('window.__HICT_BENCHMARK_ENABLED__=true')
        context.route('http://localhost:5000/**', lambda route: route.continue_(url=args.api + route.request.url.removeprefix('http://localhost:5000')))
        page = context.new_page()
        page.set_default_timeout(15000)
        page.on('pageerror', lambda error: result['pageErrors'].append(str(error)))
        try:
            page.goto(args.url)
            page.wait_for_function('window.__hictBenchmark')
            page.evaluate("api=>window.__hictBenchmark.open('tiny-primary.hict.hdf5',api)", args.api)
            response = context.request.post(args.api + '/reverse_selection_range', data={'startBP': 0, 'endBP': 17})
            assert response.ok, response.text()
            page.reload()
            page.get_by_role('link', name='File', exact=True).click()
            page.get_by_role('link', name='Attach', exact=True).click()
            page.get_by_text('Attached to session tiny-primary.hict.hdf5', exact=True).wait_for(state='visible')
            page.evaluate('window.__sessionValidationManager=window.__hictBenchmark.manager()')

            def snapshot():
                return page.evaluate('''async()=>{
                    const m=window.__hictBenchmark.manager();
                    const pixels=await m.networkManager.requestManager.queryMatrixFloat32({bpResolution:4,startRowPx:0,startColPx:0,endRowPx:5,endColPx:5,source:'PRIMARY',signalMode:'RAW_COUNTS'});
                    return {sameManager:m===window.__sessionValidationManager,pixels:Array.from(pixels.values),
                        contigs:m.contigDimensionHolder.contigDescriptors.map(c=>({id:c.contigId,direction:c.direction})),
                        center:m.getView().getCenter(),resolution:m.getView().getResolution()};
                }''')

            before = snapshot()
            assert before['pixels'][0] == 11
            history = context.request.post(args.api + '/assembly/history', data={}).json()
            mutations = ('/open', '/close', '/link_fasta', '/load_agp', '/set_visualization_options', '/secondary/open')
            def record_mutation(request):
                if request.url.endswith(mutations):
                    result['mutationRequests'].append(request.url)
            page.on('request', record_mutation)
            cases = [
                ('future', {'version': 999, 'filename': 'tiny-primary.hict.hdf5'}, 'version'),
                ('null', None, 'document'),
                ('missing-version', {}, 'version'),
                ('short-center', {'version': 2, 'view': {'center': [1]}}, 'view.center'),
                ('zero-resolution', {'version': 2, 'view': {'resolution': 0}}, 'view.resolution'),
                ('wrong-filename', {'version': 2, 'filename': 42}, 'filename'),
                ('bad-bookmark', {'version': 2, 'savedLocations': [{'location_id': 1}]}, 'center_point'),
                ('bad-visibility', {'version': 2, 'axisTrackVisibility': {'horizontalHidden': [2]}}, 'horizontalHidden'),
                ('bad-weight-flag', {'version': 2, 'visualizationOptions': {'applyCoolerWeights': 'false'}}, 'applyCoolerWeights'),
                ('bad-log-base', {'version': 2, 'visualizationOptions': {'postLogBase': 1}}, 'postLogBase'),
                ('bad-color', {'version': 2, 'backgroundColor': 'invalid color'}, 'backgroundColor'),
                ('partial-styles', {'version': 2, 'trackStyles': {'contigs': {}}}, 'trackStyles.contigs'),
                ('reversed-range', {'version': 2, 'visualizationOptions': {'colormap': {'minSignal': 4, 'maxSignal': 1}}}, 'maxSignal'),
                ('bad-action', {'version': 2, 'savedVisualizationPresets': [{'option_id': 1, 'name': 'bad', 'action': {'type': 'sequence-overlay', 'mode': 'overlay', 'kind': 'other'}}]}, 'action.kind'),
            ]
            for name, document, field in cases:
                page.get_by_role('link', name='File', exact=True).click()
                with page.expect_file_chooser() as chooser:
                    page.get_by_role('link', name='Open session', exact=True).click()
                chooser.value.set_files({'name': name + '.json', 'mimeType': 'application/json', 'buffer': json.dumps(document).encode()})
                page.get_by_text('Invalid session', exact=False).filter(has_text=field).last.wait_for(state='visible')
                assert snapshot() == before, name
                assert context.request.post(args.api + '/assembly/history', data={}).json() == history, name
                result['checks'].append(name + ': rejected before changing edited map, view or journal')
            assert not result['mutationRequests'], result['mutationRequests']
            page.remove_listener('request', record_mutation)
            page.get_by_role('link', name='File', exact=True).click()
            with page.expect_download() as download:
                page.get_by_role('link', name='Save session', exact=True).click()
            download.value.save_as(str(output / 'saved-session.json'))
            page.screenshot(path=str(output / 'rejected-sessions.png'))
            page.get_by_role('link', name='File', exact=True).click()
            with page.expect_file_chooser() as chooser:
                page.get_by_role('link', name='Open session', exact=True).click()
            chooser.value.set_files(args.legacy_file or str(output / 'saved-session.json'))
            page.get_by_text('Legacy session v2 restores settings', exact=False).wait_for(state='visible')
            page.get_by_text('Session restored', exact=True).wait_for(state='visible')
            result['checks'].append('valid v2 settings restore with an explicit legacy limitation notice')
            result['restoredFile'] = args.legacy_file or str(output / 'saved-session.json')
            assert not result['pageErrors'], result['pageErrors']
        finally:
            (output / 'results.json').write_text(json.dumps(result, indent=2) + '\n')
            browser.close()


if __name__ == '__main__':
    main()
