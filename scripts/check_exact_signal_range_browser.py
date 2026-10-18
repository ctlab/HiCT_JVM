#!/usr/bin/env python3
"""Read-only browser checks for whole-resolution contrast bounds; no generation jobs."""
import argparse
import json
from pathlib import Path
from playwright.sync_api import sync_playwright


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:8186")
    parser.add_argument("--api", default="http://127.0.0.1:5106")
    parser.add_argument("--output", default="/tmp/hict-review-20261018/exact-browser")
    args = parser.parse_args()
    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=True)
    errors, blocks, results = [], [], []
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1440, "height": 1000})
        page.set_default_timeout(60000)
        page.on("pageerror", lambda error: errors.append(str(error)))
        page.on("request", lambda request: blocks.append(request.post_data_json)
                if request.url.endswith("/matrix/statistics") and not request.post_data_json.get("metadataOnly") else None)
        page.add_init_script("window.__HICT_BENCHMARK_ENABLED__ = true")
        page.route("http://localhost:5000/**", lambda route: route.continue_(
            url=args.api + route.request.url.removeprefix("http://localhost:5000")))
        page.goto(args.url)
        page.wait_for_function("window.__hictBenchmark")
        page.evaluate("api => window.__hictBenchmark.open('combined_ind2_4DN.hict.hdf5', api)", args.api)
        page.wait_for_timeout(1500)
        button = page.get_by_role("button", name="Do not use Cooler weights", exact=True)
        if button.is_visible():
            button.click()
        page.evaluate("""async () => {
            const m=window.__hictBenchmark.manager(),api=m.networkManager.requestManager;
            const status=await api.openSecondarySource('ind2.self.hict.hdf5',true);
            m.viewAndLayersManager.setSecondaryResolutionModel(status.compatibility);
            const color=s=>({type:'colormap',mode:'LINEAR',input:{type:'source',source:s},
                minSignal:0,maxSignal:100,startColor:'#ffffff00',endColor:s==='PRIMARY'?'#ff0000ff':'#000000ff'});
            await api.setRenderPipelineConfig({enabled:true,swapUpperLower:false,
                upperExpression:color('PRIMARY'),lowerExpression:color('SECONDARY')});
            await m.reloadTilesFromBackend();m.viewAndLayersManager.fitViewToActiveExtent();
        }""")
        page.wait_for_function("""() => {
            const t=document.querySelector('[data-testid="exact-range-status"]')?.textContent ?? '';
            return t.includes('Exact limits:') && !t.includes('blocks');
        }""")
        page.wait_for_timeout(1000)
        before = page.get_by_test_id("exact-range-status").inner_text()
        block_count = len(blocks)
        page.evaluate("""() => {const m=window.__hictBenchmark.manager(),v=m.getView(),c=v.getCenter();
            v.setCenter([c[0]+v.getResolution()*10,c[1]]);m.getMap().renderSync();}""")
        page.wait_for_timeout(1500)
        assert page.get_by_test_id("exact-range-status").inner_text() == before
        assert len(blocks) == block_count, "Panning restarted a completed whole-resolution scan"
        results.append({"test": "pan retains exact bounds and cache", "status": before, "blocks": block_count})
        page.get_by_role('group', name='Visualization source', exact=True).get_by_role('button', name='SECONDARY', exact=True).click()
        page.wait_for_function("""previous => {
            const t=document.querySelector('[data-testid="exact-range-status"]')?.textContent ?? '';
            return t.includes('Exact limits:') && !t.includes('blocks') && t!==previous;
        }""", arg=before)
        secondary = page.get_by_test_id("exact-range-status").inner_text()
        results.append({"test": "independent secondary bounds", "status": secondary})
        page.screenshot(path=str(out / "exact-range-desktop.png"))
        before_return = len(blocks)
        page.get_by_role('group', name='Visualization source', exact=True).get_by_role('button', name='PRIMARY', exact=True).click()
        page.wait_for_function("""expected => document.querySelector('[data-testid="exact-range-status"]')?.textContent.trim()===expected""", arg=before)
        assert len(blocks) == before_return, "Switching back did not reuse the completed source range"
        results.append({"test": "returning to PRIMARY uses cached exact extrema"})
        page.set_viewport_size({"width": 390, "height": 844})
        page.wait_for_timeout(500)
        page.get_by_test_id("exact-range-status").scroll_into_view_if_needed()
        page.get_by_test_id("exact-range-status").screenshot(path=str(out / "exact-range-mobile-label.png"))
        assert not errors, errors
        (out / "results.json").write_text(json.dumps(results, indent=2) + "\n")
        browser.close()
        print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
