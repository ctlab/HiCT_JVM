#!/usr/bin/env python3
"""Verify contrast, slider domains, logarithms and scaling against known contacts."""

import argparse
import copy
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

        def sample(label, coordinate=(1.5, -0.5)):
            point = page.evaluate(
                """coordinate=>{const m=window.__hictBenchmark.manager(),p=m.getMap().getPixelFromCoordinate(coordinate),b=m.getMap().getViewport().getBoundingClientRect();return [Math.round(b.x+p[0]),Math.round(b.y+p[1])];}""",
                coordinate,
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
                np.log1p(np.array(known)) / math.log(2),
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
                    "test": "Reset uses declared Log2 and native 10/4 resolution scaling matches independent linear/quadratic formula"
                }
            )
            page.evaluate(
                """async api => {
                await window.__hictBenchmark.open('tiny-primary.hict.hdf5', api);
                const m=window.__hictBenchmark.manager(), requests=m.networkManager.requestManager;
                const status=await requests.openSecondarySource('old-secondary.hict.hdf5', false);
                m.viewAndLayersManager.setSecondaryResolutionModel(status.compatibility);
                const color=(source,endColor,maxSignal,startColor='#ffffffff')=>({type:'colormap',mode:'LINEAR',
                    input:{type:'source',source},minSignal:0,maxSignal,startColor,endColor});
                const blend=(top,bottom)=>({type:'pixel_blend',mode:'OVER',top,bottom,topOpacity:0.5,bottomOpacity:1});
                await requests.setRenderPipelineConfig({enabled:true,swapUpperLower:false,
                    upperExpression:blend(color('PRIMARY','#0000ffff',22,'#ffffff7f'),color('PRIMARY','#ff0000ff',11)),
                    lowerExpression:blend(color('SECONDARY','#0000ffff',11),color('PRIMARY','#00ff00ff',11))});
                await m.reloadTilesFromBackend();m.viewAndLayersManager.fitViewToActiveExtent();
            }""",
                args.api,
            )
            check.settle()
            sources = page.get_by_role("group", name="Visualization source", exact=True)
            sources.get_by_role("button", name="Primary", exact=True).click()
            page.get_by_role("button", name="Upper triangle", exact=True).click()
            targets = page.get_by_label(
                "Rendering pipeline colormap target", exact=True
            )
            expect(targets.locator("option")).to_have_count(2)
            targets.select_option("upper.top")
            upper = page.get_by_role(
                "spinbutton", name="Upper signal threshold", exact=True
            )
            page.wait_for_function(
                "document.querySelector('[aria-label=\"Upper signal threshold\"]').value === '22'"
            )
            original = pipeline()
            before_upper = sample("target-before-upper")
            before_lower = sample("target-before-lower", (0.5, -1.5))
            upper.fill("55")
            apply()
            expected = copy.deepcopy(original)

            def set_expected_range(node, maximum):
                node["maxSignal"] = maximum
                for key, value in (("min", node["minSignal"]), ("max", maximum)):
                    node[key] = {
                        "type": "constant",
                        "value": value,
                        "rangeSource": node["input"]["source"],
                        "rangeOutput": "selectedMin" if key == "min" else "selectedMax",
                    }

            set_expected_range(expected["upperExpression"]["top"], 55)
            assert pipeline() == expected, (pipeline(), expected)
            assert sample("target-after-upper") != before_upper
            assert sample("target-after-lower", (0.5, -1.5)) == before_lower
            link = page.get_by_role(
                "button", name="Link upper and lower triangles", exact=True
            )
            assert link.get_attribute("aria-pressed") == "false"
            link.click()
            check.settle()
            upper.fill("66")
            apply()
            for node in (
                expected["upperExpression"]["top"],
                expected["upperExpression"]["bottom"],
                expected["lowerExpression"]["bottom"],
            ):
                set_expected_range(node, 66)
                node["startColor"] = "#ffffff7f"
                node["endColor"] = "#0000ffff"
            assert pipeline() == expected, (pipeline(), expected)
            sources.get_by_role("button", name="Secondary", exact=True).click()
            page.wait_for_function(
                "document.querySelector('[aria-label=\"Upper signal threshold\"]').value === '11'"
            )
            upper.fill("77")
            apply()
            set_expected_range(expected["lowerExpression"]["top"], 77)
            assert pipeline() == expected, (pipeline(), expected)
            assert query() == known
            for label in ("Swap sources", "Swap upper and lower triangles"):
                before = pipeline()
                before_pixels = sample(label.replace(" ", "-") + "-before")
                with page.expect_response(
                    lambda response: response.url.endswith("/render_pipeline/set")
                ):
                    page.get_by_role("button", name=label, exact=True).click()
                check.settle()
                changed = pipeline()
                assert changed != before
                if label == "Swap upper and lower triangles":
                    assert changed["upperExpression"] == before["lowerExpression"]
                    assert changed["lowerExpression"] == before["upperExpression"]
                with page.expect_response(
                    lambda response: response.url.endswith("/render_pipeline/set")
                ):
                    page.get_by_role("button", name=label, exact=True).click()
                check.settle()
                assert pipeline() == before
                assert sample(label.replace(" ", "-") + "-restored") == before_pixels
                assert query() == known
            results.append(
                {
                    "test": "Source, triangle, individual colormap, linked ranges and both swap controls preserve exact signals and isolate displayed colors"
                }
            )
            sources.get_by_role("button", name="Primary", exact=True).click()
            check.settle()
            if link.get_attribute("aria-pressed") != "true":
                link.click()
                check.settle()
            before_graph = pipeline()
            options_response = page.request.post(
                args.api + "/get_visualization_options", data={}
            )
            assert options_response.ok, options_response.text()
            before_options = options_response.json()
            before_pixels = sample("atomic-publication-before")
            errors_before = page.evaluate(
                "document.querySelector('#app').__vue_app__.config.globalProperties.$pinia._s.get('notificationCenter').entries.filter(e=>e.level==='error').length"
            )
            failed_requests = []

            def fail_publication(route):
                failed_requests.append(route.request.post_data_json)
                route.fulfill(
                    status=503,
                    json={"error": "Forced visualization publication failure"},
                )

            page.route("**/set_visualization_options", fail_publication)
            try:
                upper.fill("85")
                with page.expect_response(
                    lambda response: response.url.endswith("/set_visualization_options")
                ) as failed:
                    apply()
                assert failed.value.status == 503
                page.wait_for_timeout(750)
                page.wait_for_function(
                    "count=>document.querySelector('#app').__vue_app__.config.globalProperties.$pinia._s.get('notificationCenter').entries.filter(e=>e.level==='error').length===count+1",
                    arg=errors_before,
                )
                assert len(failed_requests) == 1, failed_requests
                assert "renderPipeline" in failed_requests[0], failed_requests
                assert pipeline() == before_graph
                after_options = page.request.post(
                    args.api + "/get_visualization_options", data={}
                )
                assert after_options.ok and after_options.json() == before_options
                assert sample("atomic-publication-rejected") == before_pixels
                expected_error = "Failed to load resource: the server responded with a status of 503 (Service Unavailable)"
                assert len(check.errors) <= 1 and all(
                    error == expected_error for error in check.errors
                ), check.errors
                check.errors.clear()
            finally:
                page.unroute("**/set_visualization_options", fail_publication)
            apply()
            expected = copy.deepcopy(before_graph)
            for node in (
                expected["upperExpression"]["top"],
                expected["upperExpression"]["bottom"],
                expected["lowerExpression"]["bottom"],
            ):
                set_expected_range(node, 85)
            assert pipeline() == expected, (pipeline(), expected)
            options_response = page.request.post(
                args.api + "/get_visualization_options", data={}
            )
            assert (
                options_response.ok
                and options_response.json()["colormap"]["maxSignal"] == 85
            )
            assert query() == known
            results.append(
                {
                    "test": "Rejected atomic primary publication preserves graph, options and pixels, reports one error and succeeds on retry"
                }
            )

            for index, key, color in (
                (0, "startColor", "#00ff00b3"),
                (1, "endColor", "#ff0000e5"),
            ):
                before_pixels = sample("color-picker-" + key + "-before")
                page.locator(".threshold-compact-card").get_by_role(
                    "button", name="Choose color", exact=True
                ).nth(index).click()
                picker = page.locator(".hict-color-picker-overlay:visible")
                field = picker.get_by_role(
                    "textbox", name="Type a color name or hex value", exact=True
                )
                field.fill(color)
                field.press("Tab")
                picker.locator(".picker_done button").click()
                apply()
                for node in (
                    expected["upperExpression"]["top"],
                    expected["upperExpression"]["bottom"],
                    expected["lowerExpression"]["bottom"],
                ):
                    node[key] = color
                assert pipeline() == expected, (pipeline(), expected)
                assert sample("color-picker-" + key + "-after") != before_pixels
                assert query() == known
            results.append(
                {
                    "test": "Both actual colormap color pickers preserve explicit alpha and change rendered colors in the linked primary target set"
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
