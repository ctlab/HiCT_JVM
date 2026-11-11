#!/usr/bin/env python3
"""Every way of opening a map ends on a readable map whose pipeline, options and sidebar agree.

Opens each fixture through File > Manual Open, the Open Wizard and Attach, with each normalization choice, and
measures how much of the main diagonal is drawn in a color that differs from the background. A blank map, or a
pipeline that still multiplies by weights the options switched off, fails the check.
"""
import argparse
import io
import json
from pathlib import Path

import numpy as np
from PIL import Image
from playwright.sync_api import sync_playwright

WEIGHTS_TRACK = '__builtin_cooler_weights__'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('url', 'api', 'output'):
        parser.add_argument('--' + name, required=True)
    parser.add_argument('--files', nargs='+', default=['mat18_100k.hict.hdf5', 'combined_ind2_4DN.hict.hdf5', 'arabiensis.0.hict.hdf5'],
                        help='Fixtures under the server data folder')
    parser.add_argument('--nan-files', nargs='*', default=['combined_ind2_4DN.hict.hdf5', 'arabiensis.0.hict.hdf5'],
                        help='Fixtures whose weights have NaNs: the note about masked bins must appear for them')
    parser.add_argument('--floor', type=float, default=0.5, help='Minimum share of diagonal samples that must be drawn')
    args = parser.parse_args()
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    result = {'checks': [], 'pageErrors': [], 'consoleErrors': []}

    def check(name, ok, detail=''):
        result['checks'].append({'name': name, 'ok': bool(ok), 'detail': detail})
        print(('PASS' if ok else 'FAIL') + ': ' + name + (' ' + str(detail) if detail else ''))

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(viewport={'width': 1440, 'height': 900})
        context.add_init_script('window.__HICT_BENCHMARK_ENABLED__=true')
        context.route('http://localhost:5000/**', lambda route: route.continue_(url=args.api + route.request.url.removeprefix('http://localhost:5000')))
        page = context.new_page()
        page.set_default_timeout(60000)
        page.on('pageerror', lambda error: result['pageErrors'].append(str(error)))
        page.on('console', lambda message: result['consoleErrors'].append(message.text[:300]) if message.type == 'error' else None)

        def post(path, data=None):
            response = context.request.post(args.api + path, data=data or {})
            return response.json() if response.ok else {'status': response.status}

        def fresh():
            post('/close')
            page.goto(args.url)
            page.wait_for_function('window.__hictBenchmark')

        def file_menu(item):
            page.get_by_role('link', name='File', exact=True).click()
            page.get_by_role('link', name=item, exact=True).click()

        def pick(dialog, fixture):
            dialog.get_by_text(fixture, exact=False).first.click()
            dialog.get_by_role('button', name='Load', exact=True).click()

        def wait_for_map():
            page.wait_for_function('()=>!!window.__hictBenchmark.manager?.()', timeout=240000)

        def settle():
            """Waits out the opening work and any rendering that follows."""
            page.wait_for_timeout(2500)
            page.wait_for_function("()=>!document.querySelector('.open-progress, [aria-label=\"Opening progress\"]')", timeout=120000)
            page.wait_for_timeout(3500)

        def open_manual(fixture):
            file_menu('Open…')
            dialog = page.get_by_role('dialog')
            dialog.wait_for(state='visible')
            pick(dialog, fixture)
            wait_for_map()

        def open_wizard(fixture):
            file_menu('Open with options…')
            dialog = page.get_by_role('dialog')
            dialog.wait_for(state='visible')
            dialog.get_by_role('button', name='Single map').click()
            dialog.get_by_role('button', name='Next', exact=True).click()
            dialog.get_by_role('button', name='Browse…').first.click()
            picker = page.get_by_role('dialog').last
            pick(picker, fixture)
            dialog.get_by_text('Map files conversion', exact=True).wait_for(state='hidden')
            check(f'{fixture}: no track precompute step without tracks',
                  dialog.get_by_text('Track precomputing', exact=True).count() == 0)
            page.wait_for_timeout(800)
            for _ in range(14):
                finish = dialog.get_by_role('button', name='Finish', exact=True)
                if finish.count() and finish.is_enabled():
                    finish.click()
                    break
                nxt = dialog.get_by_role('button', name='Next', exact=True)
                if not nxt.is_enabled():
                    raise AssertionError('wizard cannot advance')
                nxt.click()
                page.wait_for_timeout(500)
            wait_for_map()

        def open_attach(fixture):
            post('/open', {'filename': fixture})
            page.reload()
            page.wait_for_function('window.__hictBenchmark')
            file_menu('Attach')
            wait_for_map()

        def server_state():
            return page.evaluate('''async (api) => {
                const post = (p) => fetch(api + p, {method: 'POST', headers: {'content-type': 'application/json'}, body: '{}'}).then(r => r.json());
                const pipeline = await post('/render_pipeline/get');
                const options = await post('/get_visualization_options');
                return {pipeline, options};
            }''', args.api)

        def uses_weights(node):
            return WEIGHTS_TRACK in json.dumps(node)

        def map_screenshot(tag):
            geometry = page.evaluate('''() => {
                const m = window.__hictBenchmark.manager();
                const map = m.getMap();
                const extent = map.getView().getProjection().getExtent();
                const a = map.getPixelFromCoordinate([extent[0], extent[3]]);
                const b = map.getPixelFromCoordinate([extent[2], extent[1]]);
                const r = map.getViewport().getBoundingClientRect();
                return {a, b, x: r.x, y: r.y, w: r.width, h: r.height};
            }''')
            shot = Image.open(io.BytesIO(page.screenshot(clip={'x': geometry['x'], 'y': geometry['y'], 'width': geometry['w'], 'height': geometry['h']}))).convert('RGB')
            shot.save(output / f'{tag}.png')
            return geometry, np.asarray(shot).astype(int)

        def diagonal_share(tag):
            """Share of samples along the main diagonal that are not the background color."""
            geometry, pixels = map_screenshot(tag)
            drawn = total = 0
            start = np.asarray(geometry['a'])
            delta = np.asarray(geometry['b']) - start
            low, high = 0.0, 1.0
            for origin, span, size in zip(start, delta, reversed(pixels.shape[:2])):
                if span == 0:
                    if not 6 <= origin <= size - 7:
                        return 0.0, 0
                    continue
                entry, exit = sorted(((6 - origin) / span, (size - 7 - origin) / span))
                low, high = max(low, entry), min(high, exit)
            if high <= low:
                return 0.0, 0
            # Sample distinct screen pixels, even when only a tiny part of the map is visible.
            length = (high - low) * np.abs(delta).max()
            for t in np.linspace(low, high, min(240, int(length) + 1)):
                x, y = start + t * delta
                window = pixels[int(y) - 5:int(y) + 6, int(x) - 5:int(x) + 6].reshape(-1, 3)
                total += 1
                if (np.abs(window - 255).max(axis=1) >= 40).any():
                    drawn += 1
            return drawn / total if total else 0.0, total

        def assert_agreement(tag):
            state = server_state()
            pipeline, options = state['pipeline'], state['options']
            if pipeline.get('enabled'):
                check(f'{tag}: pipeline weights match the weights option',
                      uses_weights(pipeline) == bool(options.get('applyCoolerWeights')),
                      f"pipeline={uses_weights(pipeline)} option={options.get('applyCoolerWeights')}")
            upper = page.get_by_label('Upper signal threshold').input_value()
            expected = (pipeline['upperExpression'].get('maxSignal') if pipeline.get('enabled') else options['colormap']['maxSignal'])
            check(f'{tag}: sidebar upper threshold equals the rendered one', abs(float(upper) - float(expected)) <= 1e-6 * max(1.0, abs(float(expected))),
                  f'sidebar={upper} server={expected}')
            if pipeline.get('enabled'):
                stored = options['colormap']['maxSignal']
                check(f'{tag}: stored upper threshold equals the rendered one',
                      abs(float(stored) - float(expected)) <= 1e-6 * max(1.0, abs(float(expected))),
                      f'options={stored} pipeline={expected}')

        def measure(tag, minimum_samples=51):
            settle()
            share, samples = diagonal_share(tag)
            detail = f'share={share:.2f} samples={samples}'
            if share < args.floor:
                state = server_state()
                detail += ' ' + json.dumps({'enabled': state['pipeline'].get('enabled'), 'weights': uses_weights(state['pipeline']),
                                            'options': {k: state['options'].get(k) for k in ('applyCoolerWeights', 'coolerWeightsNaNPolicy')},
                                            'range': state['options']['colormap'].get('maxSignal'), 'pipelineMax': state['pipeline'].get('upperExpression', {}).get('maxSignal')})
            check(f'{tag}: diagonal is drawn', share >= args.floor and samples >= minimum_samples, detail)
            assert_agreement(tag)

        try:
            for fixture in args.files:
                stem = fixture.split('.')[0]
                for way, opener in (('manual', open_manual), ('wizard', open_wizard), ('attach', open_attach)):
                    fresh()
                    opener(fixture)
                    check(f'{stem} {way}: the weights dialog is not shown', page.get_by_role('button', name='Do not use Cooler weights').count() == 0)
                    measure(f'{stem}-{way}')
                # zoomed in on the diagonal the map must stay readable (the range is not tied to the first view)
                fresh()
                open_manual(fixture)
                settle()
                initial_resolution = page.evaluate(
                    '() => window.__hictBenchmark.manager().getMap().getView().getResolution()'
                )
                for steps in (4, 8):
                    page.evaluate('''(resolution) => {
                        const view = window.__hictBenchmark.manager().getMap().getView();
                        const extent = view.getProjection().getExtent();
                        view.setCenter([(extent[0] + extent[2]) / 2, (extent[1] + extent[3]) / 2]);
                        view.setResolution(resolution);
                    }''', initial_resolution / 2**steps)
                    measure(f'{stem}-zoom-{steps}', minimum_samples=20)
                # a file with excluded bins is masked silently, and Change reaches every other choice
                fresh()
                page.evaluate('()=>window.localStorage.clear()')
                open_manual(fixture)
                settle()
                note = page.get_by_role('button', name='Change')
                if note.count():
                    check(f'{stem}: a one-line note offers Change', note.first.is_visible())
                    note.first.click()
                    page.get_by_role('button', name='Do not use Cooler weights').click()
                    measure(f'{stem}-excluded-off')
                    page.get_by_text('Cooler weights', exact=True).first.click()
                    for value in ('REPLACE_NANS_WITH_ONE', 'REPLACE_NANS_WITH_ZERO'):
                        page.locator('#cooler-weights-nan-policy').select_option(value)
                        measure(f'{stem}-excluded-{value.split("_")[-1].lower()}')
                else:
                    check(f'{stem}: no excluded bins, nothing to mask', fixture not in args.nan_files)
                # every normalization choice, on a map opened the plain way
                fresh()
                open_manual(fixture)
                settle()
                select = page.locator('#normalization-mode')
                for choice in ('file', 'sqrt', 'none'):
                    if choice == 'file' and select.locator('option[value=file]').is_disabled():
                        continue
                    select.select_option(choice)
                    if choice == 'sqrt':
                        page.wait_for_function("()=>!document.querySelector('#normalization-progress')", timeout=240000)
                        page.get_by_role('button', name='Apply', exact=True).click()
                    page.wait_for_timeout(1500)
                    page.get_by_role('button', name='Apply', exact=True).click()
                    measure(f'{stem}-norm-{choice}')
                fresh()
                open_manual(fixture)
                settle()
                display_mode = page.locator('select').filter(has=page.locator('option[value="OBSERVED_OVER_EXPECTED"]'))
                observed_range = server_state()['options']['colormap']
                display_mode.select_option('OBSERVED_OVER_EXPECTED')
                settle()
                oe_range = server_state()['options']['colormap']
                check(f'{stem}: O/E uses reciprocal ratio bounds',
                      (oe_range['minSignal'], oe_range['maxSignal']) == (0.5, 2))
                _, oe_pixels = map_screenshot(f'{stem}-oe')
                blue, red = ([33, 102, 172], [178, 24, 43])
                blue_count = np.all(oe_pixels == blue, axis=2).sum()
                red_count = np.all(oe_pixels == red, axis=2).sum()
                check(f'{stem}: O/E renders both palette endpoints',
                      blue_count > 0 and red_count > 0, f'blue={blue_count} red={red_count}')
                display_mode.select_option('OBSERVED')
                settle()
                check(f'{stem}: Observed restores its color range',
                      server_state()['options']['colormap'] == observed_range)
        finally:
            browser.close()
    check('no page errors', not result['pageErrors'], result['pageErrors'][:3])
    (output / 'result.json').write_text(json.dumps(result, indent=2))
    failed = [c for c in result['checks'] if not c['ok']]
    print(f"{len(result['checks']) - len(failed)} passed, {len(failed)} failed")
    raise SystemExit(1 if failed else 0)


if __name__ == '__main__':
    main()
