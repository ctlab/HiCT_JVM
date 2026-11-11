#!/usr/bin/env python3
"""Verify contrast, slider domains, logarithms and scaling against known contacts."""

import argparse
import io
import json
import math
from pathlib import Path

import numpy as np
from PIL import Image
from playwright.sync_api import expect, sync_playwright
from sequence_browser_harness import BrowserCheck


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("url", "api", "output"):
        parser.add_argument("--" + name, required=True)
    args = parser.parse_args()
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
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

        def pipeline():
            return page.evaluate(
                "async()=>await window.__hictBenchmark.manager().networkManager.requestManager.getRenderPipelineConfig()"
            )

        def query(bp=4, size=5, processed=False):
            expression = None
            if processed:
                root = pipeline()["upperExpression"]
                assert root["type"] == "colormap", root
                expression = root["input"]
            return page.evaluate(
                """async a=>Array.from((await window.__hictBenchmark.manager().networkManager.requestManager.queryMatrixFloat32({
                bpResolution:a.bp,startRowPx:0,endRowPx:a.size,startColPx:0,endColPx:a.size,
                source:'PRIMARY',signalMode:a.expression?'PIPELINE_SIGNAL':'RAW_COUNTS',
                ...(a.expression?{expression:a.expression}:{})})).values)""",
                {"bp": bp, "size": size, "expression": expression},
            )

        def apply():
            page.get_by_role("button", name="Apply", exact=True).click()
            page.wait_for_timeout(450)
            check.settle()

        def sample(label):
            point = page.evaluate(
                """()=>{const m=window.__hictBenchmark.manager(),p=m.getMap().getPixelFromCoordinate([1.5,-.5]),b=m.getMap().getViewport().getBoundingClientRect();return [Math.round(b.x+p[0]),Math.round(b.y+p[1])];}"""
            )
            content = page.screenshot(path=str(output / (label + ".png")))
            image = Image.open(io.BytesIO(content)).convert("RGB")
            assert 0 <= point[0] < image.width and 0 <= point[1] < image.height, point
            return list(image.getpixel(tuple(point)))

        try:
            page.goto(args.url)
            page.wait_for_function("window.__hictBenchmark")
            page.evaluate(
                'async api=>{await window.__hictBenchmark.open("tiny-primary.hict.hdf5",api)}',
                args.api,
            )
            check.settle()
            page.locator("#normalization-mode").select_option("none")
            page.locator("#signal-transform-linear").click()
            advanced = page.locator("#signal-advanced")
            advanced.locator("summary").click()
            page.locator("#scaling-across-zoom").select_option("none")
            lower = page.get_by_role(
                "spinbutton", name="Lower signal threshold", exact=True
            )
            upper = page.get_by_role(
                "spinbutton", name="Upper signal threshold", exact=True
            )
            lower.fill("0")
            upper.fill("11")
            apply()
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
            assert query() == known
            np.testing.assert_allclose(
                query(processed=True), known, rtol=1e-6, atol=1e-6
            )
            before_color = sample("linear-range-0-11")
            lower_handle = page.get_by_role(
                "slider", name="Lower threshold", exact=True
            )
            lower_handle.press("ArrowRight")
            assert float(lower.input_value()) > 0
            rail = page.get_by_role(
                "group", name="Signal thresholds", exact=True
            ).bounding_box()
            handle = page.get_by_role(
                "slider", name="Upper threshold", exact=True
            ).bounding_box()
            page.mouse.move(
                handle["x"] + handle["width"] / 2, handle["y"] + handle["height"] / 2
            )
            page.mouse.down()
            page.mouse.move(
                rail["x"] + rail["width"] * 0.7, rail["y"] + rail["height"] / 2, steps=8
            )
            page.mouse.up()
            apply()
            assert 7.5 < float(upper.input_value()) < 7.9, upper.input_value()
            after_color = sample("keyboard-and-drag-contrast")
            assert before_color != after_color, (before_color, after_color)
            assert query() == known
            results.append(
                {
                    "test": "Keyboard lower and dragged upper handles change actual colors and preserve raw contacts",
                    "beforeRgb": before_color,
                    "afterRgb": after_color,
                }
            )
            graph = pipeline()
            thresholds = [lower.input_value(), upper.input_value()]
            scale = page.get_by_role("group", name="Slider scale", exact=True)
            for mode in ("Log", "Linear"):
                scale.get_by_role("button", name=mode, exact=True).click()
                assert [lower.input_value(), upper.input_value()] == thresholds
                assert pipeline() == graph
            page.get_by_role("button", name="Focus range", exact=True).click()
            details = page.locator("details.range-details")
            details.locator("summary").click()
            minimum = page.get_by_role(
                "spinbutton", name="Slider minimum signal", exact=True
            )
            maximum = page.get_by_role(
                "spinbutton", name="Slider maximum signal", exact=True
            )
            assert float(minimum.input_value()) == float(thresholds[0])
            assert float(maximum.input_value()) == float(thresholds[1])
            minimum.fill("-5")
            minimum.press("Tab")
            maximum.fill("20")
            maximum.press("Tab")
            assert pipeline() == graph
            maximum.fill("-10")
            maximum.press("Tab")
            assert float(maximum.input_value()) == float(minimum.input_value()) + 1
            assert pipeline() == graph
            page.get_by_role("button", name="Use full loaded range", exact=True).click()
            assert (
                float(minimum.input_value()) == 0 and float(maximum.input_value()) == 11
            )
            page.get_by_role("button", name="Scan whole map", exact=True).click()
            page.get_by_test_id("exact-range-status").filter(
                has_text="Exact"
            ).wait_for()
            assert (
                float(minimum.input_value()) == 0 and float(maximum.input_value()) == 11
            )
            assert pipeline() == graph and query() == known
            results.append(
                {
                    "test": "Slider Linear/Log, Focus, domain repair, loaded range and exact scan preserve graph and exact contacts"
                }
            )
            page.locator("#signal-transform-log").click()
            base = page.locator("#normalization-post-log-base")
            base.fill("2")
            base.press("Enter")
            apply()
            expected = np.log1p(np.array(known)) / math.log(2)
            np.testing.assert_allclose(
                query(processed=True), expected, rtol=1e-6, atol=1e-6
            )
            log_graph = pipeline()
            base.fill("1")
            base.press("Enter")
            expect(base).to_have_attribute("aria-invalid", "true")
            assert pipeline() == log_graph
            page.get_by_role("button", name="Entities", exact=True).click()
            page.get_by_role("button", name="Visualization", exact=True).click()
            expect(page.locator("#signal-transform-log")).to_have_attribute(
                "aria-pressed", "true"
            )
            np.testing.assert_allclose(
                query(processed=True), expected, rtol=1e-6, atol=1e-6
            )
            results.append(
                {
                    "test": "Log2 matches log1p(x)/log(2), invalid base preserves graph and panel remount retains processing"
                }
            )
            advanced = page.locator("#signal-advanced")
            if advanced.get_attribute("open") is None:
                advanced.locator("summary").click()
            page.get_by_role("button", name="Reset", exact=True).click()
            apply()
            np.testing.assert_allclose(
                query(processed=True),
                np.log1p(np.array(known)) / math.log(10),
                rtol=1e-6,
                atol=1e-6,
            )
            page.locator("#signal-transform-linear").click()
            apply()
            page.evaluate(
                """()=>{const m=window.__hictBenchmark.manager();m.viewAndLayersManager.setEnabledBpResolutions([10]);m.getView().setResolution(m.viewAndLayersManager.getPixelResolutionForBpResolution(10));}"""
            )
            check.settle()
            raw = np.array(query(10, 2))
            assert raw.tolist() == [11, 0, 0, 11]
            for mode, power in (("none", 0), ("linear", 1), ("quadratic", 2)):
                page.locator("#scaling-across-zoom").select_option(mode)
                apply()
                actual = query(10, 2, True)
                np.testing.assert_allclose(
                    actual, raw / (2.5**power), rtol=1e-6, atol=1e-6
                )
            results.append(
                {
                    "test": "Reset uses declared Log10 and native 10/4 resolution scaling matches independent linear/quadratic formula"
                }
            )
            assert not check.errors, check.errors
        finally:
            page.screenshot(path=str(output / "last-state.png"))
            (output / "results.json").write_text(
                json.dumps({"checks": results, "errors": check.errors}, indent=2) + "\n"
            )
            browser.close()
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
