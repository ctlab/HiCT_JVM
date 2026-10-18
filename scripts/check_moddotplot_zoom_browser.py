#!/usr/bin/env python3
"""Exercise real generated identity overlays, source contrast and zoom rendering."""
import argparse
import io
import json
import time
from pathlib import Path

import numpy as np
from PIL import Image
from playwright.sync_api import sync_playwright


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:8187")
    parser.add_argument("--api", default="http://127.0.0.1:5107")
    parser.add_argument("--data-dir", required=True)
    parser.add_argument("--output", default="/tmp/hict-release-physical/moddotplot-zoom")
    args = parser.parse_args()
    data = Path(args.data_dir).absolute()
    if data != data.resolve() or not data.is_relative_to("/tmp"):
        raise ValueError("Use a nonsymlinked disposable DATA directory under /tmp")
    for name in ("combined_ind2_4DN.hict.hdf5", "quad_combined_ind2.fasta"):
        path = data / name
        if not path.is_file() or path.is_symlink() or path.stat().st_nlink != 1:
            raise ValueError(f"Use physical input copies: {path}")
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    errors, results, failures = [], [], []
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1440, "height": 1000})
        page.set_default_timeout(60000)
        page.on("pageerror", lambda error: errors.append(str(error)))
        page.on("response", lambda response: failures.append({"url": response.url, "status": response.status})
                if response.status >= 500 else None)
        page.add_init_script("window.__HICT_BENCHMARK_ENABLED__=true")
        page.route("http://localhost:5000/**", lambda route: route.continue_(
            url=args.api + route.request.url.removeprefix("http://localhost:5000")))

        def settled():
            page.wait_for_function("""() => {const map=window.__hictBenchmark.manager().getMap();
                return map.tileQueue_.getTilesLoading()===0 && map.tileQueue_.getCount()===0 && map.renderComplete_;}""")
            page.wait_for_timeout(200)

        def snapshot(name):
            settled()
            raw = page.locator("#hic-contact-map").screenshot()
            (output / f"{name}.png").write_bytes(raw)
            return np.array(Image.open(io.BytesIO(raw)).convert("RGB"))

        def graph():
            return page.evaluate("window.__hictBenchmark.manager().networkManager.requestManager.getRenderPipelineConfig()")

        def set_view(resolution, magnification=1):
            page.evaluate("""({resolution,magnification}) => {
                const m=window.__hictBenchmark.manager(),l=m.viewAndLayersManager;
                const p=l.getPixelResolutionForBpResolution(resolution);
                const x=m.contigDimensionHolder.getFractionalPxOfBp(1020000,resolution)*p;
                m.getView().setResolution(p/magnification);m.getView().setCenter([x,-x]);
                m.getMap().renderSync();
            }""", {"resolution": resolution, "magnification": magnification})
            page.wait_for_function("bp=>window.__hictBenchmark.manager().viewAndLayersManager.currentViewState.resolutionDesciptor.bpResolution===bp", arg=resolution)
            settled()

        try:
            page.goto(args.url)
            page.wait_for_function("window.__hictBenchmark")
            page.evaluate("api=>window.__hictBenchmark.open('combined_ind2_4DN.hict.hdf5',api)", args.api)
            page.wait_for_timeout(1500)
            button = page.get_by_role("button", name="Do not use Cooler weights", exact=True)
            if button.is_visible():
                button.click()
            response = page.request.post(args.api + "/link_fasta", data={"fastaFilename": "quad_combined_ind2.fasta"})
            assert response.ok, response.text()
            page.evaluate("""async () => {const m=window.__hictBenchmark.manager();
                const primary={type:'colormap',mode:'LINEAR',input:{type:'source',source:'PRIMARY'},
                    startColor:'#ffffff00',endColor:'#ff0000ff',minSignal:0,maxSignal:100};
                await m.networkManager.requestManager.setRenderPipelineConfig({enabled:true,
                    upperExpression:primary,lowerExpression:primary,swapUpperLower:false});
                m.viewAndLayersManager.setEnabledBpResolutions([5000,10000]);
                await m.reloadTilesFromBackend();
            }""")
            set_view(5000, 16)
            baseline = snapshot("primary-before")
            original_primary = graph()["upperExpression"]
            transform = page.evaluate("""() => {const m=window.__hictBenchmark.manager();
                return {origin:m.getMap().getCoordinateFromPixel([0,0]),scale:m.getView().getResolution()};}""")
            y, x = np.indices(baseline.shape[:2])
            column = transform["origin"][0] + x * transform["scale"]
            row = -transform["origin"][1] + y * transform["scale"]
            # Exclude controls and the edge shadow of transient workspace toasts.
            interior = (x > 60) & (x < baseline.shape[1] - 30) & (y > 10) & (y < baseline.shape[0] - 10)
            upper = (column > row + 10 * transform["scale"]) & interior
            assert np.count_nonzero(baseline[upper, 0] > baseline[upper, 2]) > 100, "Baseline has no red Hi-C signal"
            page.evaluate("""() => {window.frameGaps=[];let last=performance.now();
                window.frameTimer=setInterval(()=>{const now=performance.now();window.frameGaps.push(now-last);last=now;},50);}""")
            for iteration in range(2):
                page.evaluate("""i => {const s=window.__hictBenchmark.manager().viewAndLayersManager.currentViewState.selectionBorders;
                    s.leftBP=[1000000+i*40000,1000000+i*40000];s.rightBP=[1040000+i*40000,1040000+i*40000];}""", iteration)
                page.get_by_role("link", name="Tools", exact=True).click()
                page.get_by_role("button", name="Generate ModDotPlot…", exact=True).click()
                dialog = page.get_by_role("dialog", name="Generate ModDotPlot", exact=True)
                page.get_by_label("Analysis scope").select_option("selection")
                for checkbox in dialog.locator(".resolution-options input").all():
                    checkbox.uncheck()
                dialog.locator('.resolution-options input[value="5000"]').check()
                page.get_by_label("Worker threads").fill("4")
                start = time.monotonic()
                dialog.get_by_role("button", name="Generate ModDotPlot", exact=True).click()
                dialog.wait_for(state="hidden", timeout=180000)
                set_view(5000, 16)
                current = snapshot(f"generated-{iteration + 1}")
                assert graph()["upperExpression"] == original_primary
                assert np.array_equal(baseline[upper], current[upper]), "Generated overlay changed primary upper-triangle pixels"
                results.append({"test": "real 5K generation preserves primary pixels", "iteration": iteration + 1,
                                "seconds": time.monotonic() - start, "comparedPixels": int(upper.sum())})
            page.get_by_role("group", name="Visualization source", exact=True).get_by_role("button", name="SECONDARY", exact=True).click()
            page.wait_for_function("document.querySelector('[data-testid=exact-range-status]')?.textContent.includes('Percentage scale')")
            assert float(page.get_by_label("Slider minimum signal", exact=True).input_value()) == 0
            assert float(page.get_by_label("Slider maximum signal", exact=True).input_value()) == 100
            page.locator(".threshold-number-input").first.fill("90")
            page.locator(".threshold-number-input").first.press("Enter")
            page.wait_for_timeout(500)
            changed = snapshot("secondary-threshold-90")
            config = graph()
            assert config["upperExpression"] == original_primary
            assert config["lowerExpression"]["minSignal"] == 90, config
            assert config["lowerExpression"]["input"]["op"] == "DIV", config
            assert np.array_equal(baseline[upper], changed[upper]), "Secondary contrast changed primary pixels"
            results.append({"test": "secondary percentage threshold preserves primary pixels and numeric scale"})
            for iteration in range(3):
                set_view(10000)
                missing = snapshot(f"missing-10K-{iteration}")
                visible = page.evaluate("""() => window.__hictBenchmark.manager().viewAndLayersManager.layersHolder
                    .secondaryHiCDataLayers.filter(l=>l.getVisible()).map(l=>l.get('bpResolution'))""")
                assert not visible, visible
                assert graph() == config
                set_view(5000, 16)
                returned = snapshot(f"returned-5K-{iteration}")
                assert np.array_equal(changed[interior], returned[interior]), "Zoom return changed map pixels"
                assert graph() == config
                results.append({"test": "5K/10K zoom roundtrip", "iteration": iteration + 1,
                                "missingResolutionVisibleSecondaryLayers": visible})
            identity = page.evaluate("""async () => {const m=window.__hictBenchmark.manager();
                const result=await m.networkManager.requestManager.queryMatrixFloat32({bpResolution:5000,
                    startRowPx:200,endRowPx:216,startColPx:200,endColPx:216,source:'SECONDARY',signalMode:'RAW_COUNTS'});
                return Array.from(result.values);}""")
            assert max(identity) == 1000000 and sum(value > 0 for value in identity) >= 8, identity
            for preset, mode in (("ModDotPlot: multicolor overlay", "overlay"),
                                 ("ModDotPlot: Hi-C above, identity below", "below")):
                for item in page.locator(".saved-locations").all():
                    if item.locator("input").input_value() == preset:
                        item.get_by_title("Load visualization preset", exact=True).click()
                        break
                else:
                    raise AssertionError(f"Missing normal preset row: {preset}")
                page.wait_for_function("""mode=>window.__hictBenchmark.manager().networkManager.requestManager
                    .getRenderPipelineConfig().then(p=>mode==='overlay'?p.upperExpression.type==='pixel_blend':p.upperExpression.type==='colormap')""", arg=mode)
                rendered = snapshot(f"preset-{mode}")
                current_graph = graph()
                if mode == "overlay":
                    assert current_graph["upperExpression"]["bottom"] == original_primary
                    changed_pixels = int(np.any(rendered != changed, axis=2)[interior].sum())
                    assert changed_pixels > 100, "Generated identities did not appear when applying overlay preset"
                else:
                    assert current_graph["upperExpression"] == original_primary
                    assert np.array_equal(baseline[upper], rendered[upper])
                results.append({"test": "ordinary preset row applies generated identities", "mode": mode})
            gaps = page.evaluate("clearInterval(window.frameTimer);window.frameGaps")
            assert max(gaps, default=0) < 2000, max(gaps)
            assert not errors, errors
            assert not failures, failures
            results.append({"test": "browser responsiveness", "maximumHeartbeatGapMs": max(gaps)})
        finally:
            (output / "results.json").write_text(json.dumps({"results": results, "errors": errors, "serverErrors": failures}, indent=2))
            page.screenshot(path=str(output / "final.png"))
            browser.close()
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
