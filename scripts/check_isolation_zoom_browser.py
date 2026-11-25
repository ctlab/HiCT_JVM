#!/usr/bin/env python3
"""Verify every zoom control after Ctrl, Shift and ribbon isolation."""

import argparse
from io import BytesIO
from itertools import groupby
import json
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from PIL import Image
from playwright.sync_api import expect, sync_playwright

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

        def center_on_landmark(bp):
            page.evaluate("""bp => {
                const manager = window.__hictBenchmark.manager();
                const layers = manager.viewAndLayersManager;
                const grid = layers.currentViewState.resolutionDesciptor;
                const coordinate = manager.contigDimensionHolder
                    .getFractionalPxOfBp(bp, grid.bpResolution) * grid.pixelResolution;
                manager.getMap().getView().setCenter([coordinate, -coordinate]);
            }""", bp)
            check.settle()

        def response_resolution(response):
            url = urlsplit(response.url)
            if url.path == "/get_tile" and response.ok:
                return parse_qs(url.query).get("bpResolution", [None])[0]
            return None

        def verify_fine_raster(name):
            viewport = page.locator("#hic-contact-map .ol-viewport").bounding_box()
            side = 64
            center_x = viewport["x"] + viewport["width"] / 2
            center_y = viewport["y"] + viewport["height"] / 2
            image = Image.open(BytesIO(page.screenshot(clip={
                "x": round(center_x) - side // 2, "y": round(center_y) - side // 2,
                "width": side, "height": side,
            }, scale="css"))).convert("RGB")
            image.save(output / f"{name}-fine-raster.png")
            # The fixture paints a varying one-bin-wide contact diagonal. The
            # blank off-diagonal cells are deliberately excluded.
            diagonals = [
                [image.getpixel((x, x + offset)) for x in range(side)
                 if 0 <= x + offset < side]
                for offset in range(1 - side, side)
            ]
            background = image.getpixel((side - 1, 0))
            signal = max(diagonals, key=lambda pixels: len(set(pixels) - {background}))
            lengths = [len(list(run)) for color, run in groupby(signal) if color != background]
            assert len(set(signal) - {background}) >= 6, (name, "insufficient raster signal")
            assert max(lengths) <= 2, (name, "finest raster run in CSS pixels", max(lengths))
            return max(lengths)

        def verify_fine_tiles(name, zoom_in, measure_raster):
            check.settle()
            initial = page.evaluate(visible_resolution)
            assert initial == 200, (name, initial)
            trace = {"test": name, "initialBp": initial, "steps": []}
            traces.append(trace)
            previous = initial
            for _ in range(8):
                zoom_in()
                check.settle()
                current = page.evaluate(visible_resolution)
                trace["steps"].append({"visibleBp": current, "tileLoaded": current in tiles})
                assert 100 <= current <= previous, (name, previous, current)
                assert current in tiles, (name, "missing successful Hi-C tile", current)
                if current == 100:
                    break
                previous = current
            assert current == 100, (name, "did not reach the finest level")
            if measure_raster:
                trace["maxFineRunCssPx"] = verify_fine_raster(name)

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

        def remember_tile(response):
            resolution = response_resolution(response)
            if resolution is not None:
                tiles.add(int(resolution))

        def slider_zoom():
            slider = page.locator("#hic-contact-map .ol-zoomslider")
            thumb = slider.locator(".ol-zoomslider-thumb")
            track = slider.bounding_box()
            bounds = thumb.bounding_box()
            assert track is not None and bounds is not None
            x = bounds["x"] + bounds["width"] / 2
            y = bounds["y"] + bounds["height"] / 2
            top = track["y"] + bounds["height"] / 2
            page.mouse.move(x, y)
            page.mouse.down()
            page.mouse.move(x, max(top, y - 12), steps=6)
            page.mouse.up()

        def map_zoom(center_bp, action):
            point = map_point(center_bp)
            if action == "wheel":
                page.mouse.move(*point)
                page.mouse.wheel(0, -140)
            else:
                page.mouse.dblclick(*point)

        try:
            tiles = set()
            page.on("response", remember_tile)
            page.goto(args.url)
            page.wait_for_function("window.__hictBenchmark")
            page.evaluate("a => window.__hictBenchmark.open(a.file, a.api)", vars(args))
            check.settle()
            # Give the fixture's integer counts a distinct, stable color per
            # bin regardless of the application's default Hi-C colormap.
            page.evaluate(f"""async () => {{
                const manager = {manager};
                const color = {{
                    type: 'colormap', mode: 'LINEAR',
                    input: {{type: 'source', source: 'PRIMARY'}},
                    minSignal: 0, maxSignal: 100,
                    startColor: '#ffffffff', endColor: '#bd1818ff',
                }};
                await manager.networkManager.requestManager.setRenderPipelineConfig({{
                    enabled: true, upperExpression: color, lowerExpression: color,
                }});
                await manager.reloadTilesFromBackend();
            }}""")
            check.settle()

            def ribbon_isolation():
                field = page.get_by_role("textbox", name="Rows scope")
                field.fill("three")
                option = field.locator("..").locator(".scope-result").filter(
                    has=page.get_by_text("Contig", exact=True),
                ).filter(has=page.get_by_text("three", exact=True))
                expect(option).to_have_count(1)
                option.click()
                page.get_by_role("button", name="Copy row scope to columns").click()

            scopes = {
                "region": (lambda: menu_isolation(True, "Isolate view"), 180000),
                "shift-contig": (lambda: menu_isolation(False, "Isolate view to selection"), 200000),
                "ribbon-contig": (ribbon_isolation, 1000000),
            }
            failures = []
            lock = page.locator("#map-anchor-lock")
            expect(lock).to_have_attribute("aria-pressed", "true")
            for anchored in (True, False):
                if not anchored:
                    lock.click()
                    expect(lock).to_have_attribute("aria-pressed", "false")
                for scope, (isolate, center_bp) in scopes.items():
                    controls = {
                        "wheel": lambda: map_zoom(center_bp, "wheel"),
                        "map-plus": lambda: page.locator(".ol-zoom-in").first.click(),
                        "double-click": lambda: map_zoom(center_bp, "double-click"),
                        "toolbar-plus": lambda: page.get_by_role("button", name="Zoom in", exact=True).click(),
                        "slider": slider_zoom,
                    }
                    for control, zoom_in in controls.items():
                        name = f"{'anchored' if anchored else 'centered'}-{scope}-{control}"

                        def exercise():
                            reset_scope()
                            isolate()
                            check.settle()
                            # Use free positioning for the central raster.
                            # When locked, the map deliberately fits against the rulers.
                            if not anchored:
                                center_on_landmark(center_bp)
                            verify_fine_tiles(name, zoom_in, measure_raster=not anchored)

                        try:
                            check.step(name, exercise)
                        except Exception as failure:
                            failures.append(name)
                            print(f"Failed {name}: {failure}", flush=True)
            assert not failures, failures
        finally:
            (output / "results.json").write_text(json.dumps(
                {"checks": check.rows, "transitions": traces, "errors": check.errors}, indent=2))
            browser.close()


if __name__ == "__main__":
    main()
