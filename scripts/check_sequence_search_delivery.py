#!/usr/bin/env python3
"""Exercise actual search completion and visible track delivery on disposable DATA."""
import argparse
import json
import random
import re
from pathlib import Path
from playwright.sync_api import sync_playwright


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--url', default='http://127.0.0.1:8197')
    parser.add_argument('--api', default='http://127.0.0.1:5117')
    parser.add_argument('--data-dir', default='/tmp/hict-search-review/data')
    parser.add_argument('--output', default='/tmp/hict-search-review/search')
    args = parser.parse_args()
    data = Path(args.data_dir).absolute()
    assert data == data.resolve() and data.is_relative_to('/tmp')
    for name in ('combined_ind2_4DN.hict.hdf5', 'quad_combined_ind2.fasta'):
        path = data / name
        assert path.is_file() and not path.is_symlink() and path.stat().st_nlink == 1
    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=True)
    errors, results = [], []
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_page(viewport={'width': 1440, 'height': 1000})
        page.set_default_timeout(60000)
        page.on('pageerror', lambda e: errors.append(str(e)))
        page.add_init_script('window.__HICT_BENCHMARK_ENABLED__ = true')
        page.route('http://localhost:5000/**', lambda route: route.continue_(
            url=args.api + route.request.url.removeprefix('http://localhost:5000')))
        try:
            page.goto(args.url)
            page.wait_for_function('window.__hictBenchmark')
            page.evaluate('api => window.__hictBenchmark.open("combined_ind2_4DN.hict.hdf5", api)', args.api)
            page.wait_for_timeout(1000)
            nan = page.get_by_role('button', name='Do not use Cooler weights', exact=True)
            if nan.is_visible():
                nan.click()
            response = page.request.post(args.api + '/link_fasta', data={'fastaFilename': 'quad_combined_ind2.fasta'})
            assert response.ok, response.text()
            response = page.request.post(args.api + '/sequence/region', data={'startBp': 0, 'endBp': 4096})
            assert response.ok, response.text()
            match = re.search('[ACGT]{80}', response.json()['sequence'].upper())
            assert match
            absent_query = ''.join(random.Random(20261031).choices('ACGT', k=200))
            for scenario in ('sidecar', 'reusable', 'retry', 'whole', 'no-match', 'slow-track-query'):
                reusable = scenario == 'reusable'
                page.evaluate('''() => {
                    const b=window.__hictBenchmark.manager().viewAndLayersManager.currentViewState.selectionBorders;
                    b.leftBP=[0,0]; b.rightBP=[40000,40000];
                }''')
                page.get_by_role('link', name='Tools', exact=True).click()
                page.get_by_role('button', name='Find sequences in linked FASTA…', exact=True).click()
                dialog = page.get_by_role('dialog', name='Find sequences in the linked FASTA', exact=True)
                dialog.get_by_label('FASTA records, or one DNA sequence per line').fill(
                    '>acceptance query\n' + (absent_query if scenario == 'no-match' else match.group()))
                dialog.get_by_label('Search scope', exact=True).select_option('assembly' if scenario == 'whole' else 'selection')
                if reusable:
                    dialog.get_by_label('Save reusable BED under DATA_DIR').check()
                    dialog.get_by_label('BED output path').fill('acceptance/exact.bed')
                    dialog.get_by_label('Overwrite', exact=True).check()
                if scenario == 'retry':
                    def lost_reply(route):
                        response = route.fetch()
                        assert response.ok, response.text()
                        page.unroute('**/tracks/open', lost_reply)
                        route.fulfill(status=503, content_type='application/json', body='{"error":"Simulated lost track-open reply"}')
                    page.route('**/tracks/open', lost_reply)
                before = page.evaluate('window.__hictBenchmark.manager().linearTrackManager.getTracksSnapshot().length')
                pending_queries = []
                def hold_query(route):
                    pending_queries.append(route)
                if scenario == 'slow-track-query':
                    page.route('**/tracks/query_1d', hold_query)
                with page.expect_response(lambda r: r.url.endswith('/sequence/search/jobs')) as submitted:
                    dialog.get_by_role('button', name='Find sequences', exact=True).click()
                job_id = submitted.value.json()['jobId']
                if scenario == 'no-match':
                    dialog.get_by_text('No matching sequence found. No track was added.', exact=True).wait_for()
                    assert page.evaluate('window.__hictBenchmark.manager().linearTrackManager.getTracksSnapshot().length') == before
                    jobs = page.request.post(args.api + '/sequence/jobs/list', data={}).json()
                    job = next(j for j in jobs if j['jobId'] == job_id)
                    assert job['status'] == 'finished' and job['hits'] == 0, job
                    assert not dialog.get_by_role('button', name='Open result track', exact=True).count()
                    results.append({'scenario': scenario, 'job': job, 'tracksAdded': 0})
                    dialog.locator('footer').get_by_role('button', name='Close', exact=True).click()
                    continue
                if scenario == 'retry':
                    dialog.get_by_text('Use Open result track to retry', exact=False).wait_for()
                    dialog.get_by_role('button', name='Open result track', exact=True).click()
                try:
                    dialog.wait_for(state='hidden', timeout=15000 if scenario == 'slow-track-query' else 180000)
                finally:
                    if scenario == 'slow-track-query':
                        page.unroute('**/tracks/query_1d', hold_query)
                        for route in pending_queries:
                            route.continue_()
                tracks = page.evaluate('window.__hictBenchmark.manager().linearTrackManager.getTracksSnapshot()')
                assert len(tracks) == before + 1, 'Result retry duplicated a track'
                track = tracks[-1]
                assert track['visible'] and track['name'].startswith('Sequence search ('), tracks
                page.get_by_role('button', name=f"Hide {track['name']} horizontally", exact=True).last.wait_for()
                page.get_by_role('button', name=f"Hide {track['name']} vertically", exact=True).last.wait_for()
                jobs = page.request.post(args.api + '/sequence/jobs/list', data={}).json()
                job = next(j for j in jobs if j['jobId'] == job_id)
                assert job['outputFile'] == track['sourceFile']
                assert job['status'] == 'finished' and job['hits'] > 0, job
                results.append({'scenario': scenario, 'job': job, 'track': track})
            assert not errors, errors
        finally:
            page.screenshot(path=str(out / 'last-state.png'))
            (out / 'results.json').write_text(json.dumps({'results': results, 'errors': errors}, indent=2))
            print(json.dumps({'results': results, 'errors': errors}, indent=2))
            browser.close()


if __name__ == '__main__':
    main()
