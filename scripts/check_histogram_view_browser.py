#!/usr/bin/env python3
"""Check that the visible histogram domain can change independently of map colours."""

import argparse
import hashlib
import json
import math
from pathlib import Path

from playwright.sync_api import expect, sync_playwright

from sequence_browser_harness import BrowserCheck, configure_test_api


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("url", "api", "output"):
        parser.add_argument("--" + name, required=True)
    args = parser.parse_args()
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1440, "height": 900})
        page.add_init_script("window.__HICT_BENCHMARK_ENABLED__=true")
        configure_test_api(page, args.api)
        check = BrowserCheck(page, output)
        page.goto(args.url)
        page.wait_for_function("window.__hictBenchmark")
        page.evaluate(
            'api => window.__hictBenchmark.open("tiny-primary.hict.hdf5", api)', args.api
        )
        check.settle()
        page.locator("#normalization-mode").select_option("none")
        page.evaluate("""async () => {
          const manager = window.__hictBenchmark.manager();
          await manager.visualizationManager.waitForPendingOptionRequests();
          const signal = {type: 'binary', op: 'DIV',
            left: {type: 'source', source: 'PRIMARY'}, right: {type: 'constant', value: 500}};
          const colour = {type: 'colormap', mode: 'LINEAR', input: signal,
            minSignal: 0.001, maxSignal: 0.024, startColor: '#ffffffff', endColor: '#ff0000ff'};
          await manager.networkManager.requestManager.setRenderPipelineConfig({
            enabled: true, upperExpression: colour, lowerExpression: colour});
          await manager.reloadTilesFromBackend();
          window.dispatchEvent(new CustomEvent('hict:render-pipeline-changed'));
          manager.viewAndLayersManager.restoreViewPosition({centerBp:[8.5,8.5],
            resolution:2 / manager.viewAndLayersManager.coordinateBaseBp, rotation:0});
        }""")
        check.settle()

        histogram = page.get_by_role("group", name="Viewport signal histogram")
        view = histogram.get_by_role("group", name="Histogram view", exact=True)
        start = view.get_by_label("Histogram view from", exact=True)
        end = view.get_by_label("Histogram view to", exact=True)
        fit_levels = histogram.get_by_role("button", name="Fit levels", exact=True)
        fit_view = view.get_by_role("button", name="Fit view to colour range", exact=True)
        whole_data = view.get_by_role("button", name="Whole data", exact=True)
        editor = histogram.locator(".curve-editor")
        axis = histogram.locator(".histogram-axis-strip")
        lower = page.get_by_label("Lower signal threshold", exact=True)
        upper = page.get_by_label("Upper signal threshold", exact=True)
        lower_handle = page.get_by_role("slider", name="Lower threshold", exact=True)
        upper_handle = page.get_by_role("slider", name="Upper threshold", exact=True)

        def field_range():
            return float(start.input_value()), float(end.input_value())

        def set_field(field, value):
            field.fill(str(value))
            field.press("Enter")

        def tile_digest():
            response = page.request.get(args.api + "/get_tile", params={
                "source": "PRIMARY", "bpResolution": 4, "format": "PNG_BY_PIXELS",
                "row": 0, "col": 0, "rows": 5, "cols": 5,
            })
            assert response.ok
            return hashlib.sha256(response.body()).hexdigest()

        try:
            expect(fit_levels).to_be_enabled()
            before_tile = tile_digest()
            before_thresholds = lower.input_value(), upper.input_value()
            assert list(map(float, before_thresholds)) == [0.001, 0.024], before_thresholds
            set_field(start, 0.002)
            set_field(end, 0.02)
            expect(view).to_have_attribute("data-mode", "manual")
            expect(lower_handle).to_have_class("range-handle lower pinned-below")
            expect(upper_handle).to_have_class("range-handle upper pinned-above")
            expect(histogram.locator(".outside-count.below")).to_be_visible()
            expect(histogram.locator(".outside-count.above")).to_be_visible()
            assert field_range() == (0.002, 0.02)
            assert (lower.input_value(), upper.input_value()) == before_thresholds
            assert tile_digest() == before_tile
            view.scroll_into_view_if_needed()
            page.screenshot(path=output / "manual-histogram-view.png")

            box = editor.bounding_box()
            fraction = 0.65
            page.mouse.move(box["x"] + box["width"] * fraction, box["y"] + box["height"] / 2)
            page.mouse.wheel(0, -100)
            expect(start).not_to_have_value("0.002")
            wheel_from, wheel_to = field_range()
            assert math.isclose(wheel_to - wheel_from, 0.018 / 1.25)
            assert math.isclose(wheel_from + (wheel_to - wheel_from) * fraction,
                                0.002 + 0.018 * fraction, abs_tol=0.0001)

            box = axis.bounding_box()
            y = box["y"] + box["height"] / 2
            page.mouse.move(box["x"] + box["width"] * 0.45, y)
            page.mouse.down()
            page.mouse.move(box["x"] + box["width"] * 0.65, y, steps=8)
            page.mouse.up()
            panned_from, panned_to = field_range()
            assert panned_from < wheel_from
            assert math.isclose(panned_to - panned_from, wheel_to - wheel_from)
            assert tile_digest() == before_tile

            fit_levels.click()
            check.settle()
            assert field_range() == (panned_from, panned_to)
            whole_data.click()
            expect(view).to_have_attribute("data-mode", "auto")
            lo, hi = map(float, (lower.input_value(), upper.input_value()))
            fit_view.click()
            margin = (hi - lo) * 0.05
            assert math.isclose(field_range()[0], lo - margin, abs_tol=1e-6)
            assert math.isclose(field_range()[1], hi + margin, abs_tol=1e-6)
            axis.dblclick()
            expect(view).to_have_attribute("data-mode", "auto")

            set_field(start, 0.002)
            set_field(end, 0.02)
            expect(lower_handle).to_have_class("range-handle lower pinned-below")
            box = lower_handle.bounding_box()
            rail = page.get_by_role("group", name="Signal thresholds", exact=True).bounding_box()
            page.mouse.move(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2)
            page.mouse.down()
            page.mouse.move(rail["x"] + rail["width"] * 0.25, box["y"] + box["height"] / 2, steps=8)
            page.mouse.up()
            assert 0.002 < float(lower.input_value()) < 0.02
            assert field_range() == (0.002, 0.02)

            histogram.get_by_role("button", name="Log", exact=True).click()
            set_field(start, 0)
            expect(start).to_have_attribute("aria-invalid", "true")
            expect(start).to_have_value("0.002")
            expect(start).not_to_have_attribute("aria-invalid", "true")

            report = {"status": "passed", "tile_sha256": before_tile,
                      "typed_view": [0.002, 0.02], "wheel_view": [wheel_from, wheel_to],
                      "panned_view": [panned_from, panned_to]}
            (output / "result.json").write_text(json.dumps(report, indent=2) + "\n")
            print(json.dumps(report))
        except Exception:
            page.screenshot(path=output / "failure.png")
            raise
        finally:
            browser.close()


if __name__ == "__main__":
    main()
