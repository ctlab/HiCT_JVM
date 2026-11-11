#!/usr/bin/env python3
"""Rename a real scaffold through the sidebar and verify model, viewport and AGP."""
import argparse
import json
from pathlib import Path
from playwright.sync_api import sync_playwright


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--url', default='http://127.0.0.1:8197')
    parser.add_argument('--api', default='http://127.0.0.1:5117')
    parser.add_argument('--data-dir', default='/tmp/hict-search-review/data')
    parser.add_argument('--output', default='/tmp/hict-search-review/rename')
    args = parser.parse_args()
    data = Path(args.data_dir).absolute()
    assert data == data.resolve() and data.is_relative_to('/tmp')
    source = data / 'combined_ind2_4DN.hict.hdf5'
    assert source.is_file() and not source.is_symlink() and source.stat().st_nlink == 1
    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=True)
    errors = []
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_page(viewport={'width': 1440, 'height': 1000})
        page.set_default_timeout(60000)
        page.on('pageerror', lambda e: errors.append(str(e)))
        page.on('console', lambda message: errors.append(message.text) if message.type == 'error' else None)
        page.add_init_script('window.__HICT_BENCHMARK_ENABLED__ = true')
        page.route('http://localhost:5000/**', lambda route: route.continue_(
            url=args.api + route.request.url.removeprefix('http://localhost:5000')))
        try:
            page.goto(args.url)
            page.wait_for_function('window.__hictBenchmark')
            page.evaluate('api=>window.__hictBenchmark.open("combined_ind2_4DN.hict.hdf5",api)', args.api)
            page.wait_for_timeout(1000)
            nan = page.get_by_role('button', name='Do not use Cooler weights', exact=True)
            if nan.is_visible(): nan.click()
            page.wait_for_function("window.__hictBenchmark.manager().scaffoldHolder.scaffoldTable.size>0")
            before = page.evaluate('''() => {const v=window.__hictBenchmark.manager().getView();return [v.getCenter(),v.getResolution()]}''')
            page.get_by_role('button', name='Entities', exact=True).click()
            page.locator('label[for=scaffolds-btn]').click()
            row = page.locator('.name-row').filter(has=page.locator('input')).first
            row.locator('input').fill('Renamed_chrX')
            with page.expect_response(lambda r: r.url.endswith('/names/scaffold')) as rename:
                row.get_by_role('button', name='Rename scaffold', exact=True).click()
            assert rename.value.ok, rename.value.text()
            page.wait_for_function("[...window.__hictBenchmark.manager().scaffoldHolder.scaffoldTable.values()].some(s=>s.scaffoldName==='Renamed_chrX')")
            page.wait_for_timeout(500)
            after = page.evaluate('''() => {const v=window.__hictBenchmark.manager().getView();return [v.getCenter(),v.getResolution()]}''')
            assert before == after, (before, after)
            agp = page.request.post(args.api + '/get_agp_for_assembly', data={})
            assert agp.ok, agp.text()
            assert any(line.startswith('Renamed_chrX\t') for line in agp.text().splitlines()), agp.text()[:500]
            (out / 'renamed.agp').write_text(agp.text())
            with page.expect_response(lambda r:r.url.endswith('/names/export')) as names_response, page.expect_download() as names_download:
                page.get_by_role('button', name='Export names', exact=True).click()
            assert names_response.value.ok and names_response.value.request.method == 'GET'
            names_path = out / 'exported-names.json'
            names_download.value.save_as(names_path)
            mapping = json.loads(names_path.read_text())
            renamed = [entry for entry in mapping['scaffolds'] if entry['name'] == 'Renamed_chrX']
            assert len(renamed) == 1
            renamed_id = renamed[0]['scaffoldId']
            assert len({entry['contigId'] for entry in mapping['contigs']}) == 1765
            assert mapping == page.request.get(args.api + '/names/export').json()
            with page.expect_response(lambda r:r.url.endswith('/names/scaffold')) as reset:
                row.get_by_role('button', name='Reset scaffold name', exact=True).click()
            assert reset.value.ok, reset.value.text()
            page.wait_for_function("id=>window.__hictBenchmark.manager().scaffoldHolder.scaffoldTable.get(id)?.scaffoldName!=='Renamed_chrX'", arg=renamed_id)
            reset_mapping = page.request.get(args.api + '/names/export').json()
            reset_entry = next(entry for entry in reset_mapping['scaffolds'] if entry['scaffoldId']==renamed_id)
            assert reset_entry['name'] == reset_entry['originalName']
            assert [entry for entry in reset_mapping['scaffolds'] if entry['scaffoldId']!=renamed_id] == [entry for entry in mapping['scaffolds'] if entry['scaffoldId']!=renamed_id]
            with page.expect_response(lambda r:r.url.endswith('/names/import')) as imported:
                page.locator('.names-actions input[type=file]').set_input_files(names_path)
            assert imported.value.ok, imported.value.text()
            page.wait_for_function("id=>window.__hictBenchmark.manager().scaffoldHolder.scaffoldTable.get(id)?.scaffoldName==='Renamed_chrX'", arg=renamed_id)
            assert page.request.get(args.api + '/names/export').json() == mapping
            assert page.request.post(args.api + '/get_agp_for_assembly', data={}).text() == agp.text()
            page.locator('label[for=contigs-btn]').click()
            expected_names = page.evaluate('window.__hictBenchmark.manager().contigDimensionHolder.contigDescriptors.map(c=>c.contigName)')
            assert len(expected_names) == 1765
            sizes = []
            for expected_size in (300, 800, 1300, 1765):
                page.wait_for_function('n=>document.querySelectorAll(".name-row").length===n', arg=expected_size)
                names = page.locator('.name-row input').evaluate_all('nodes=>nodes.map(n=>n.value)')
                assert names == expected_names[:expected_size]
                assert len(set(names)) == expected_size
                sizes.append(expected_size)
                more = page.get_by_role('button', name='Show 500 more', exact=True)
                if expected_size == 1765:
                    assert more.count() == 0
                else:
                    more.click()
            assert page.request.get(args.api + '/names/export').json() == mapping
            result = {'renamed': 'Renamed_chrX', 'viewportPreserved': True, 'agpNameVerified': True,
                      'nameMappingRoundTrip':True, 'resetOnlySelectedScaffold':True,
                      'contigPaginationSizes':sizes, 'errors': errors}
            assert not errors, errors
            (out / 'results.json').write_text(json.dumps(result, indent=2))
            print(json.dumps(result))
        finally:
            page.screenshot(path=str(out / 'last-state.png'))
            browser.close()


if __name__ == '__main__':
    main()
