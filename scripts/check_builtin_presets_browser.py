#!/usr/bin/env python3
"""Apply every built-in sidebar preset and verify independent signals and colors."""

import argparse
import json
import math
import re
from pathlib import Path

import numpy as np
from playwright.sync_api import sync_playwright
from sequence_browser_harness import BrowserCheck


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("url", "api", "output"):
        parser.add_argument("--" + name, required=True)
    args = parser.parse_args()
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    webui = Path(__file__).resolve().parents[2] / "HiCT_WebUI"
    catalog = json.loads(
        (webui / "src/app/core/visualization/colormap/default_options.json").read_text()
    )["data"]["savedVisualizationPresets"]
    results = []
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1440, "height": 1000})
        page.set_default_timeout(30000)
        page.add_init_script("window.__HICT_BENCHMARK_ENABLED__=true")
        page.route(
            "http://localhost:5000/**",
            lambda route: route.continue_(
                url=args.api + route.request.url.removeprefix("http://localhost:5000")
            ),
        )
        check = BrowserCheck(page, output)

        def api(path, payload=None):
            response = page.request.post(args.api + path, data=payload or {})
            assert response.ok, response.text()
            return response.json()

        query = {
            "bpResolution": 4,
            "startRowPx": 0,
            "endRowPx": 5,
            "startColPx": 0,
            "endColPx": 5,
            "source": "PRIMARY",
            "format": "JSON",
        }

        def colors(value):
            channels = [float(part) for part in re.findall(r"[\d.]+", value)]
            return channels[:3] + [channels[3] * 255]

        def apply_preset(preset):
            action = preset.get("action", {})
            if action.get("type") == "active-source-palette":
                # Manual bounds give this palette-only assertion a fixed range.
                page.get_by_role(
                    "combobox", name="Visualization preset", exact=True
                ).select_option(label="Dotplot black")
                page.wait_for_timeout(400)
                check.settle()
            before = api("/render_pipeline/get")
            previous_options = api("/get_visualization_options")
            notification_count = page.evaluate(
                """() => document.querySelector('#app').__vue_app__
                .config.globalProperties.$pinia._s.get('notificationCenter').entries.length"""
            )
            page.get_by_role(
                "combobox", name="Visualization preset", exact=True
            ).select_option(label=preset["name"])
            if action.get("type") == "sequence-overlay":
                page.wait_for_function(
                    """count=>document.querySelector('#app').__vue_app__
                    .config.globalProperties.$pinia._s.get('notificationCenter').entries.length>count""",
                    arg=notification_count,
                )
                assert api("/render_pipeline/get") == before
                assert api("/get_visualization_options") == previous_options
                message = page.evaluate(
                    """() => document.querySelector('#app').__vue_app__
                    .config.globalProperties.$pinia._s.get('notificationCenter').entries.at(-1).message"""
                )
                assert "Attach a generated overlay first" in message, message
                results.append({"preset": preset["name"], "missingOverlay": message})
                return

            page.wait_for_timeout(400)
            check.settle()
            options = api("/get_visualization_options")
            raw = api(
                "/matrix/query",
                {**query, "signalMode": "RAW_COUNTS", "includeWeights": True},
            )
            assert raw["values"] == known, raw
            signal = np.array(known, dtype=float).reshape(5, 5)
            expected_options = preset.get("options", previous_options)
            for key, default in (
                ("preLogBase", -1),
                ("postLogBase", 10),
                ("applyCoolerWeights", False),
            ):
                assert options[key] == expected_options.get(key, default), (
                    preset["name"],
                    key,
                    options,
                )
            for key in ("preLogBase", "postLogBase"):
                if key == "postLogBase" and options["applyCoolerWeights"]:
                    weights = [
                        1 if value is None or not math.isfinite(value) else value
                        for value in raw["rowWeights"]
                    ]
                    signal *= np.outer(weights, weights)
                base = options[key]
                if base > 0 and base != 1:
                    signal = np.log1p(np.maximum(0, signal)) / math.log(base)
            graph = api("/render_pipeline/get")
            colormap = graph["upperExpression"]
            assert colormap["type"] == "colormap", colormap
            actual = api(
                "/matrix/query",
                {
                    **query,
                    "signalMode": "PIPELINE_SIGNAL",
                    "expression": colormap["input"],
                },
            )["values"]
            assert np.allclose(actual, signal.ravel(), rtol=2e-6, atol=1e-8), (
                preset["name"],
                actual,
                signal.ravel().tolist(),
            )
            if action.get("type") == "active-source-palette":
                assert colormap["input"] == before["upperExpression"]["input"]
                assert colormap["minSignal"] == before["upperExpression"]["minSignal"]
                assert colormap["maxSignal"] == before["upperExpression"]["maxSignal"]
                start, end = [49, 54, 149, 0], [165, 0, 38, 255]
            else:
                start = colors(preset["options"]["colormap"]["startColorRGBAString"])
                end = colors(preset["options"]["colormap"]["endColorRGBAString"])
            minimum, maximum = colormap["minSignal"], colormap["maxSignal"]
            rendered = api("/matrix/query", {**query, "signalMode": "PIPELINE_SIGNAL"})[
                "values"
            ]
            assert np.allclose(
                rendered,
                np.clip(signal, minimum, maximum).ravel(),
                rtol=2e-6,
                atol=1e-8,
            )
            fraction = min(
                1, max(0, (float(signal[4, 4]) - minimum) / (maximum - minimum))
            )
            expected_rgba = [a + (b - a) * fraction for a, b in zip(start, end)]
            page.wait_for_function(
                """expected=>{
                const m=window.__hictBenchmark.manager(),l=m.viewAndLayersManager;
                const p=l.getPixelResolutionForBpResolution(4);
                const pixel=m.getMap().getPixelFromCoordinate([4.5*p,-4.5*p]);
                return l.layersHolder.primaryHiCDataLayers.filter(layer=>layer.getVisible()).some(layer=>{
                    const rgba=layer.getData(pixel);
                    window.__presetSample=rgba?Array.from(rgba):null;
                    if(!rgba)return false;
                    if(expected[3]<1)return rgba[3]===0;
                    return expected.every((value,index)=>Math.abs(value-rgba[index])<=2);
                });
            }""",
                arg=expected_rgba,
            )
            results.append(
                {
                    "preset": preset["name"],
                    "signals": actual,
                    "renderedSignals": rendered,
                    "expectedRgba": expected_rgba,
                    "actualRgba": page.evaluate("window.__presetSample"),
                }
            )

        try:
            page.goto(args.url)
            page.wait_for_function("window.__hictBenchmark")
            page.evaluate(
                "api=>window.__hictBenchmark.open('tiny-weighted.hict.hdf5',api)",
                args.api,
            )
            page.evaluate("""()=>{
                const m=window.__hictBenchmark.manager(),l=m.viewAndLayersManager;
                const p=l.getPixelResolutionForBpResolution(4);
                m.getView().setResolution(p/8);m.getView().setCenter([2.5*p,-2.5*p]);
            }""")
            check.settle()
            known = [
                1,
                3,
                0,
                0,
                0,
                3,
                0,
                7,
                0,
                0,
                0,
                7,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                11,
            ]
            for preset in catalog:
                check.step(
                    "preset-" + str(preset["option_id"]),
                    lambda preset=preset: apply_preset(preset),
                )
        finally:
            (output / "results.json").write_text(
                json.dumps({"checks": results, "errors": check.errors}, indent=2) + "\n"
            )
            browser.close()


if __name__ == "__main__":
    main()
