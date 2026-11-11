#!/usr/bin/env python3
"""Run real local BLAST through WebUI on disposable physical quad input copies."""
import argparse
import base64
import json
import re
from pathlib import Path

from playwright.sync_api import sync_playwright


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--url', default='http://127.0.0.1:8187')
    parser.add_argument('--api', default='http://127.0.0.1:5107')
    parser.add_argument('--data-dir', default='/tmp/hict-release-physical/data')
    parser.add_argument('--output', default='/tmp/hict-release-physical/blast-browser')
    args = parser.parse_args()
    data = Path(args.data_dir).absolute()
    if data != data.resolve() or not data.is_relative_to('/tmp'):
        raise ValueError('Use an isolated server with DATA_DIR under /tmp')
    for name in ('combined_ind2_4DN.hict.hdf5', 'quad_combined_ind2.fasta'):
        path = data / name
        if not path.is_file() or path.is_symlink() or path.stat().st_nlink != 1:
            raise ValueError(f'Input must be a physical copy: {path}')
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    errors = []
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_page(viewport={'width': 1440, 'height': 1000})
        page.set_default_timeout(60000)
        page.on('pageerror', lambda error: errors.append(str(error)))
        page.add_init_script('window.__HICT_BENCHMARK_ENABLED__ = true')
        page.route('http://localhost:5000/**', lambda route: route.continue_(
            url=args.api + route.request.url.removeprefix('http://localhost:5000')))
        try:
            page.goto(args.url)
            page.wait_for_function('window.__hictBenchmark')
            page.evaluate('api => window.__hictBenchmark.open("combined_ind2_4DN.hict.hdf5", api)', args.api)
            page.wait_for_timeout(1500)
            nan = page.get_by_role('button', name='Do not use Cooler weights', exact=True)
            if nan.is_visible():
                nan.click()
            response = page.request.post(args.api + '/link_fasta', data={'fastaFilename': 'quad_combined_ind2.fasta'})
            assert response.ok, response.text()
            response = page.request.post(args.api + '/sequence/region', data={'startBp': 0, 'endBp': 4096})
            assert response.ok, response.text()
            sequence = response.json()['sequence'].upper()
            match = re.search('[ACGT]{120}', sequence)
            assert match, 'Need an unambiguous source query'
            query = match.group()
            reverse = query.translate(str.maketrans('ACGT', 'TGCA'))[::-1]
            assert query != reverse
            page.evaluate('''() => {
                const borders=window.__hictBenchmark.manager().viewAndLayersManager.currentViewState.selectionBorders;
                borders.leftBP=[0,0];borders.rightBP=[40000,40000];
            }''')
            page.get_by_role('link', name='Tools', exact=True).click()
            page.get_by_role('button', name='Find sequences in linked FASTA…', exact=True).click()
            dialog = page.get_by_role('dialog', name='Find sequences in the linked FASTA', exact=True)
            page.wait_for_function("!document.querySelector('#search-mode option[value=blast]').disabled")
            dialog.get_by_label('FASTA records, or one DNA sequence per line').fill(
                f'>acceptance_forward Known source interval\n{query}\n>acceptance_reverse Reverse complement\n{reverse}')
            dialog.get_by_label('Search scope', exact=True).select_option('selection')
            dialog.get_by_label('Matching', exact=True).select_option('blast')
            dialog.get_by_label('Save reusable BED under DATA_DIR').check()
            dialog.get_by_label('BED output path').fill('acceptance/blast.bed')
            dialog.get_by_label('Overwrite', exact=True).check()
            dialog.get_by_role('button', name='Find sequences', exact=True).click()
            dialog.wait_for(state='hidden', timeout=180000)
            bed = data / 'acceptance/blast.bed'
            assert bed.is_file()
            records = [line.split('\t') for line in bed.read_text().splitlines() if line and not line.startswith('#')]
            assert records, 'BLAST did not recover the exact known source query'
            for row in records:
                assert row[3] in {'query_1', 'query_2'}, row
                encoded = row[12].removeprefix('hict-search-v1:')
                metadata = json.loads(base64.urlsafe_b64decode(encoded + '=' * (-len(encoded) % 4)))
                assert metadata['description'].startswith(('acceptance_forward ', 'acceptance_reverse ')), metadata
                assert 'BLAST identity ' in metadata['description'] and 'gap openings ' in metadata['description'], metadata
                assert not re.search(r'query \d+-\d+/\d+', metadata['description'])
                assert 0 <= int(row[1]) < int(row[2]) <= 40000
            exact = [row for row in records if (int(row[1]), int(row[2])) == (match.start(), match.end())]
            assert {'+', '-'} <= {row[5] for row in exact}, exact
            assert all(row[8] == ('220,38,38' if row[5] == '+' else '37,99,235') for row in exact), exact
            tracks = page.evaluate('window.__hictBenchmark.manager().linearTrackManager.getTracksSnapshot()')
            track = next(track for track in tracks if track['sourceFile'] == 'acceptance/blast.bed')
            features = page.evaluate('''async trackId => {
                const response=await window.__hictBenchmark.manager().networkManager.requestManager
                    .queryTracks1DByUnits({unit:'BP',start:0,end:40000,widthPx:1200,bpResolution:5000});
                return response.tracks.find(track=>track.trackId===trackId);
            }''', track['trackId'])
            assert features and features['bins'], features
            for feature in features['bins']:
                assert feature['label'].startswith(('acceptance_forward ', 'acceptance_reverse ')), feature
                assert not re.search(r'query \d+-\d+/\d+', feature['label']), feature
            assert not errors, errors
            result = {'hits': len(records), 'exactStrands': [row[5] for row in exact],
                      'track': track, 'displayedFeatureCount': len(features['bins']),
                      'exampleDisplayedFeature': features['bins'][0], 'browserErrors': errors}
            (output / 'results.json').write_text(json.dumps(result, indent=2) + '\n')
            print(json.dumps(result, indent=2))
        finally:
            page.screenshot(path=str(output / 'last-state.png'))
            browser.close()


if __name__ == '__main__':
    main()
