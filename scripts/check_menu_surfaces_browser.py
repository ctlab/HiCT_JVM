#!/usr/bin/env python3
"""Exercise declared menu destinations and record controls without claiming functional coverage."""
import argparse
import hashlib
import json
from pathlib import Path

from playwright.sync_api import sync_playwright


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url', required=True)
    parser.add_argument('--api', required=True)
    parser.add_argument('--file', required=True, help='Disposable server map filename')
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    cases = json.loads((root / 'src/test/resources/reinforcement/menu-surfaces.json').read_text())
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    results, signatures = [], {}
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        for width, height, keyboard in ((1280, 720, False), (1440, 900, True)):
            context = browser.new_context(viewport={'width': width, 'height': height})
            context.add_init_script('window.__HICT_BENCHMARK_ENABLED__=true')
            context.route('http://localhost:5000/**', lambda route: route.continue_(
                url=args.api + route.request.url.removeprefix('http://localhost:5000')))
            page = context.new_page()
            page.set_default_timeout(15000)
            errors = []
            page.on('pageerror', lambda error: errors.append(str(error)))
            try:
                assert context.request.post(args.api + '/close', data={}).ok
                page.goto(args.url)
                page.get_by_role('heading', name='No contact map open').wait_for()
                assert page.locator('#global-search-input').is_disabled()
                assert page.locator('#export-dropdown-toggle').is_disabled()
                page.get_by_role('button', name='Open map…', exact=True).click()
                dialog = page.get_by_role('dialog')
                dialog.get_by_role('heading', name='Open Hi-C dataset').wait_for()
                filename = Path(args.file).name
                file_name = dialog.locator('.file-name-cell').get_by_text(filename, exact=True)
                file_name.wait_for()
                assert dialog.get_by_role('button', name='Load', exact=True).is_disabled()
                for _ in range(4):
                    page.keyboard.press('Shift+Tab')
                    assert dialog.evaluate('(element) => element.contains(document.activeElement)')
                if keyboard:
                    file_name.press('Enter')
                else:
                    file_name.dblclick()
                page.wait_for_function('''() => {
                    const manager = window.__hictBenchmark?.manager();
                    if (!manager) return false;
                    const map = manager.getMap();
                    const view = map.getView();
                    const [left, bottom, right, top] = view.getProjection().getExtent();
                    const layers = manager.viewAndLayersManager.layersHolder.primaryHiCDataLayers;
                    return Array.from({length: 31}, (_, i) => (i + 1) / 32).some(fraction => {
                        const pixel = map.getPixelFromCoordinate([
                            left + (right - left) * fraction, top + (bottom - top) * fraction
                        ]);
                        return layers.some(layer => {
                            if (!layer.getVisible()) return false;
                            const rgba = layer.getData(pixel);
                            return rgba?.length >= 4 && rgba[3] > 0 && Math.min(...rgba.slice(0, 3)) < 220;
                        });
                    });
                }''', timeout=60000)
                page.locator('.open-progress-modal').wait_for(state='hidden', timeout=60000)
                assert not errors, errors
                page.screenshot(path=str(output / f'direct-open-{width}x{height}.png'))
                point = page.evaluate('''() => {
                    const map = window.__hictBenchmark.manager().getMap();
                    const extent = map.getView().getProjection().getExtent();
                    const rect = map.getViewport().getBoundingClientRect();
                    const pixel = map.getPixelFromCoordinate([
                        extent[0] + 0.8 * (extent[2] - extent[0]),
                        extent[3] + 0.75 * (extent[1] - extent[3])
                    ]);
                    return {x: rect.left + pixel[0], y: rect.top + pixel[1]};
                }''')
                page.mouse.click(point['x'], point['y'], button='right')
                menu = page.get_by_role('menu', name='Assembly actions')
                menu.wait_for()
                box = menu.bounding_box()
                assert 0 <= box['x'] and box['x'] + box['width'] <= width, box
                assert 0 <= box['y'] and box['y'] + box['height'] <= height, box
                page.screenshot(path=str(output / f'context-menu-{width}x{height}.png'))
                page.keyboard.press('Escape')
                menu.wait_for(state='hidden')
                print(f'PASS: direct open at {width}x{height} with {"Enter" if keyboard else "double-click"}')
            except Exception:
                page.screenshot(path=str(output / f'direct-open-{width}x{height}-failure.png'))
                raise
            finally:
                context.close()
        for case in cases:
            context = browser.new_context(viewport={'width': 1280, 'height': 720})
            context.add_init_script('window.__HICT_BENCHMARK_ENABLED__=true')
            context.route('http://localhost:5000/**', lambda route: route.continue_(
                url=args.api + route.request.url.removeprefix('http://localhost:5000')))
            errors, http_errors = [], []
            context.on('page', lambda page: page.on('pageerror', lambda error: errors.append(str(error))))
            context.on('response', lambda response: http_errors.append({'url': response.url, 'status': response.status})
                       if response.status >= 400 else None)
            page = context.new_page()
            page.set_default_timeout(10000)
            result = {'id': case['id'], 'setup': 'benchmark map open, actual menu clicks',
                      'surfaceStatus': 'pending', 'functionalStatus': 'pending', 'errors': errors,
                      'httpErrors': http_errors}
            try:
                page.goto(args.url)
                page.wait_for_function('window.__hictBenchmark')
                page.evaluate('args=>window.__hictBenchmark.open(args.file,args.api)', vars(args))
                prompt = page.get_by_role('button', name='Do not use Cooler weights', exact=True)
                if prompt.is_visible():
                    prompt.click()
                if case['id'] == 'menu-08':
                    response = context.request.post(args.api + '/render_pipeline/get', data={})
                    assert response.ok
                    pipeline = response.json()
                    pipeline['enabled'] = True
                    assert context.request.post(args.api + '/render_pipeline/set', data=pipeline).ok
                page.get_by_role('link', name=case['menu'], exact=True).click()
                action = page.get_by_role('link', name=case['action'], exact=True)
                if not action.count():
                    action = page.get_by_role('button', name=case['action'], exact=True)
                if case['heading'] is None:
                    with context.expect_page() as popup:
                        action.click()
                    target = popup.value
                    target.wait_for_load_state()
                    # The API docs open on the configured API origin, not the WebUI.
                    assert target.url.rstrip('/') == args.api.rstrip('/') + '/api/v1', target.url
                    target.wait_for_function("document.body.innerText.includes('HiCT')")
                    result['popupUrl'] = target.url
                else:
                    action.click()
                    target = page
                    target.get_by_role('heading', name=case['heading'], exact=True).wait_for(state='visible')
                    if case['id'] == 'menu-08':
                        target.wait_for_function("document.querySelector('#pipeline-enabled')?.checked")
                        result['pipelineEnabled'] = 'matches server'
                    if case['id'] in ('menu-12', 'menu-13', 'menu-14'):
                        link = target.get_by_role('button', name='Link FASTA…', exact=True)
                        link.wait_for(state='visible', timeout=30000)
                        submit_name = 'Find sequences' if case['id'] == 'menu-12' else case['heading']
                        assert target.get_by_role('button', name=submit_name, exact=True).is_disabled()
                        result['missingFastaGuidance'] = 'visible with computation disabled'
                controls = target.locator('button,a,input,select,summary,textarea,[role=menuitem]').evaluate_all('''es=>es.filter(e=>e.getClientRects().length).map(e=>({
                    tag:e.tagName,id:e.id,aria:e.getAttribute('aria-label'),text:(e.innerText||'').trim().slice(0,200),
                    title:e.title,type:e.type??null,disabled:!!e.disabled||e.classList.contains('disabled'),
                    elementHtml:e.outerHTML.slice(0,5000),
                    owner:(()=>{const p=e.closest('[role=dialog],.modal,[data-sonner-toast]');return p?{
                      tag:p.tagName,id:p.id,role:p.getAttribute('role'),classes:p.className,
                      heading:p.querySelector('h1,h2,h3,h4,h5,h6')?.textContent?.trim()??null}:null})()}))''')
                for control in controls:
                    signature = {key: control[key] for key in ('tag', 'id', 'aria', 'text', 'title', 'type')}
                    key = hashlib.sha256(json.dumps(signature, sort_keys=True).encode()).hexdigest()[:16]
                    entry = signatures.setdefault(key, {'id': 'control-' + key, 'signature': signature,
                        'surfaces': [], 'coverage': 'pending'})
                    if case['id'] not in entry['surfaces']:
                        entry['surfaces'].append(case['id'])
                result['controls'] = controls
                target.screenshot(path=str(output / (case['id'] + '.png')))
                if case['id'] in ('menu-12', 'menu-13', 'menu-14'):
                    target.get_by_role('button', name='Link FASTA…', exact=True).click()
                    target.get_by_role('heading', name='Select FASTA file', exact=True).wait_for(state='visible')
                    result['missingFastaRecovery'] = 'link action opens FASTA selector'
                assert not errors, errors
                assert not http_errors, http_errors
                result['surfaceStatus'] = 'outcome verified'
            except Exception as error:
                result['surfaceStatus'] = 'failed'
                result['failure'] = str(error)
                page.screenshot(path=str(output / (case['id'] + '-failure.png')))
            finally:
                context.close()
                results.append(result)
                (output / 'results.json').write_text(json.dumps(results, indent=2) + '\n')
                (output / 'control-inventory.json').write_text(json.dumps(list(signatures.values()), indent=2) + '\n')
                print(case['id'], result['surfaceStatus'], flush=True)
        browser.close()
    assert all(result['surfaceStatus'] == 'outcome verified' for result in results), 'See per-surface failures'


if __name__ == '__main__':
    main()
