#!/usr/bin/env python3
"""Require fresh, finer map tiles after Ctrl, Shift and ribbon isolation."""

import argparse
import json
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from playwright.sync_api import TimeoutError as PlaywrightTimeout, expect, sync_playwright

from sequence_browser_harness import BrowserCheck


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("api", "url", "file", "output"):
        parser.add_argument("--" + name, required=True)
    args = parser.parse_args()
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    manager = "window.__hictBenchmark.manager()"
    visible_resolution = (
        f"{manager}.viewAndLayersManager.getVisibleSourceResolutionDescriptors().primary?.bpResolution"
    )

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1280, "height": 720})
        page.add_init_script("window.__HICT_BENCHMARK_ENABLED__ = true")
        page.route("http://localhost:5000/**", lambda route: route.continue_(
            url=args.api + route.request.url.removeprefix("http://localhost:5000")))
        check = BrowserCheck(page, output)
        traces = []

        def map_point(bp):
            return page.evaluate("""bp => {
                const manager = window.__hictBenchmark.manager();
                const map = manager.getMap();
                const grid = manager.viewAndLayersManager.currentViewState.resolutionDesciptor;
                const coord = manager.contigDimensionHolder.getFractionalPxOfBp(bp, grid.bpResolution)
                    * grid.pixelResolution;
                const position = map.getPixelFromCoordinate([coord, -coord]);
                const bounds = map.getViewport().getBoundingClientRect();
                return [position[0] + bounds.left, position[1] + bounds.top];
            }""", bp)

        def response_resolution(response):
            url = urlsplit(response.url)
            if url.path == "/get_tile" and response.ok:
                return parse_qs(url.query).get("bpResolution", [None])[0]
            return None

        def verify_fine_tiles(scope, zoom_in):
            check.settle()
            initial = page.evaluate(visible_resolution)
            assert initial == 200, (scope, initial)
            with page.expect_response(lambda response: response_resolution(response) == "100") as fine_tile:
                for _ in range(4):
                    zoom_in()
                    try:
                        page.wait_for_function(f"{visible_resolution} === 100", timeout=2000)
                        break
                    except PlaywrightTimeout:
                        continue
                assert page.evaluate(visible_resolution) == 100, scope
            traces.append({"scope": scope, "initialBp": initial, "visibleBp": 100,
                           "tileBp": int(response_resolution(fine_tile.value))})

        def reset_scope():
            page.evaluate(f"""() => {{
                const layers = {manager}.viewAndLayersManager;
                layers.setAxisScopes({{kind: 'all'}}, {{kind: 'all'}});
                layers.fitViewToActiveExtent();
            }}""")
            page.keyboard.press("Escape")
            check.settle()

        def menu_isolation(region, name):
            check.select_range(region=region)
            page.mouse.click(*map_point(180000), button="right")
            menu = page.get_by_role("menu", name="Assembly actions")
            menu.get_by_role("menuitem", name=name, exact=True).click()

        try:
            page.goto(args.url)
            page.wait_for_function("window.__hictBenchmark")
            page.evaluate("a => window.__hictBenchmark.open(a.file, a.api)", vars(args))
            check.settle()

            def region_isolation():
                menu_isolation(True, "Isolate view")
                page.mouse.move(*map_point(180000))
                verify_fine_tiles("Ctrl region", lambda: page.mouse.wheel(0, -180))

            check.step("region-wheel-loads-finer-tiles", region_isolation)
            reset_scope()

            def shift_isolation():
                menu_isolation(False, "Isolate view to selection")
                verify_fine_tiles("Shift contig", lambda: page.locator(".ol-zoom-in").first.click())

            check.step("shift-selection-zoom-button-loads-finer-tiles", shift_isolation)
            reset_scope()

            def ribbon_isolation():
                field = page.get_by_role("textbox", name="Rows scope")
                field.fill("three")
                option = field.locator("..").locator(".scope-result").filter(
                    has=page.get_by_text("Contig", exact=True),
                ).filter(has=page.get_by_text("three", exact=True))
                expect(option).to_have_count(1)
                option.click()
                page.get_by_role("button", name="Copy row scope to columns").click()
                page.locator("#hic-contact-map .ol-viewport").click()
                verify_fine_tiles("Ribbon contig", lambda: page.keyboard.press("+"))

            check.step("ribbon-scope-keyboard-loads-finer-tiles", ribbon_isolation)
        finally:
            (output / "results.json").write_text(json.dumps(
                {"checks": check.rows, "transitions": traces, "errors": check.errors}, indent=2))
            browser.close()


if __name__ == "__main__":
    main()
