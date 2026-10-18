#!/usr/bin/env python3
"""Exercise cancelled tile responses and recovery on a real OpenLayers map."""
import argparse
import base64
import io
import json
from pathlib import Path
from urllib.parse import urlparse

import numpy as np
from PIL import Image
from playwright.sync_api import sync_playwright


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:8187")
    parser.add_argument("--api", default="http://127.0.0.1:5107")
    parser.add_argument("--output", default="/tmp/hict-release-physical/cancelled-tiles")
    args = parser.parse_args()
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    png = io.BytesIO()
    Image.new("RGBA", (1, 1)).save(png, format="PNG")
    placeholder = "data:image/png;base64," + base64.b64encode(png.getvalue()).decode()
    errors, warnings, results = [], [], []
    cancellation = {"enabled": False, "png": 0, "binary": 0}

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1440, "height": 1000})
        page.set_default_timeout(60000)
        page.on("pageerror", lambda error: errors.append(str(error)))
        page.on("console", lambda message: warnings.append(message.text)
                if "corrupt" in message.text.lower() or "truncated" in message.text.lower() else None)
        page.add_init_script("window.__HICT_BENCHMARK_ENABLED__=true")

        def route_request(route):
            request = route.request
            path = urlparse(request.url).path
            if cancellation["enabled"] and (path == "/get_tile" or
                    (path == "/matrix/query" and request.method == "POST")):
                binary = path == "/matrix/query"
                cancellation["binary" if binary else "png"] += 1
                # Match the server contract tested independently by ApiHttpIntegrationTest.
                headers = {"Access-Control-Allow-Origin": "*", "Cache-Control": "no-store",
                           "Access-Control-Expose-Headers": "x-hict-cancelled,x-hict-rows,x-hict-cols",
                           "x-hict-cancelled": "true", "x-hict-rows": "0", "x-hict-cols": "0"}
                route.fulfill(status=200, headers=headers,
                              content_type="application/octet-stream" if binary else "application/json",
                              body=b"" if binary else json.dumps({"image": placeholder, "cancelled": True}))
            elif request.url.startswith("http://localhost:5000/"):
                route.continue_(url=args.api + request.url.removeprefix("http://localhost:5000"))
            else:
                route.continue_()

        page.route("**/*", route_request)

        def settled():
            page.wait_for_function("""() => {const map=window.__hictBenchmark.manager().getMap();
                return map.tileQueue_.getTilesLoading()===0 && map.tileQueue_.getCount()===0 && map.renderComplete_;}""")
            page.wait_for_timeout(200)

        def snapshot(name):
            settled()
            raw = page.locator("#hic-contact-map").screenshot()
            (output / f"{name}.png").write_bytes(raw)
            return np.array(Image.open(io.BytesIO(raw)).convert("RGB"))[50:-30, 60:-30]

        try:
            page.goto(args.url)
            page.wait_for_function("window.__hictBenchmark")
            page.evaluate("api=>window.__hictBenchmark.open('combined_ind2_4DN.hict.hdf5',api)", args.api)
            page.wait_for_timeout(1500)
            button = page.get_by_role("button", name="Do not use Cooler weights", exact=True)
            if button.is_visible():
                button.click()
            page.evaluate("""async () => {const m=window.__hictBenchmark.manager();
                const color={type:'colormap',mode:'LINEAR',input:{type:'source',source:'PRIMARY'},
                    startColor:'#ffffff00',endColor:'#ff0000ff',minSignal:0,maxSignal:100};
                await m.networkManager.requestManager.setRenderPipelineConfig({enabled:true,
                    upperExpression:color,lowerExpression:color,swapUpperLower:false});
                m.viewAndLayersManager.setEnabledBpResolutions([1000000]);
                await m.reloadTilesFromBackend();m.viewAndLayersManager.fitViewToActiveExtent();
            }""")
            for transport in ("png", "binary"):
                if transport == "binary":
                    page.get_by_role("link", name="Dev", exact=True).click()
                    page.get_by_label("Render tiles from binary signal (experimental)").check()
                    page.get_by_role("link", name="Dev", exact=True).click()
                baseline = snapshot(f"{transport}-before")
                assert np.count_nonzero(baseline[:, :, 0] > baseline[:, :, 2]) > 100
                cancellation["enabled"] = True
                page.evaluate("window.__hictBenchmark.manager().reloadTiles()")
                settled()
                assert cancellation[transport] > 0, cancellation
                states = page.evaluate("""() => window.__hictBenchmark.manager().viewAndLayersManager
                    .layersHolder.hicDataLayers.filter(l=>l.getVisible()).flatMap(l=>
                        l.getSource().tileCache.getValues().map(t=>t.getState()))""")
                assert states and all(state == 4 for state in states), states  # OpenLayers EMPTY
                cancellation["enabled"] = False
                page.evaluate("window.__hictBenchmark.manager().reloadTiles()")
                recovered = snapshot(f"{transport}-recovered")
                assert np.array_equal(baseline, recovered), f"{transport}: recovered map pixels differ"
                results.append({"transport": transport, "cancelledRequests": cancellation[transport],
                                "emptyTiles": len(states), "recoveredPixels": int(baseline.size // 3)})
            assert not errors, errors
            assert not warnings, warnings
        finally:
            (output / "results.json").write_text(json.dumps({"results": results, "errors": errors,
                                                           "imageWarnings": warnings}, indent=2) + "\n")
            page.screenshot(path=str(output / "final.png"))
            browser.close()
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
