#!/usr/bin/env python3
"""Check bookmark reveal and keyboard panning on an isolated Borrelia map."""

import argparse
from functools import partial
from http.server import ThreadingHTTPServer
import json
from math import isclose
from pathlib import Path
import shutil
import tempfile
import threading

from playwright.sync_api import expect, sync_playwright

from check_shift_selection_browser import Quiet
from sequence_browser_harness import BrowserCheck, HictTestServer, configure_test_api


VIEW = """() => {
    const map = window.__hictBenchmark.manager().getMap();
    const view = map.getView();
    return {
        center: view.getCenter(),
        resolution: view.getResolution(),
        size: map.getSize(),
        animating: view.getAnimating(),
    };
}"""


def check_map(page, check, url, api_url, output):
    page.goto(url)
    page.wait_for_function("window.__hictBenchmark")
    page.evaluate("api => window.__hictBenchmark.open('borrelia.hict.hdf5', api)", api_url)
    check.settle()

    map_region = page.get_by_role("region", name="Contact map", exact=True)
    anchor = page.locator("#map-anchor-lock")
    if anchor.get_attribute("aria-pressed") == "true":
        anchor.click()
    page.evaluate("""() => {
        const layers = window.__hictBenchmark.manager().viewAndLayersManager;
        layers.restoreViewPosition({centerBp:[710000,710000],
            resolution: layers.getPixelResolutionForBpResolution(1000) / 3});
    }""")
    check.settle()
    bookmark = page.get_by_role("button", name="Bookmark current location", exact=True)
    page.get_by_role("button", name="Entities", exact=True).click()
    page.locator("label[for=scaffolds-btn]").click()
    page.get_by_role("button", name="Hide side panel", exact=True).click()
    check.settle()

    def position():
        state = page.evaluate(VIEW)
        return {"center": state["center"], "resolution": state["resolution"]}

    saved = position()
    bookmark.click()
    expect(page.get_by_role("button", name="Hide side panel", exact=True)).to_be_visible()
    expect(page.locator("#bookmarks-btn")).to_be_checked()
    rows = page.locator(".bookmark-row")
    expect(rows).to_have_count(1)
    expect(rows.first.get_by_role("textbox")).to_have_value("Location 1")
    expect(rows.first).to_have_class("bookmark-row bookmark-row-new")
    expect(rows.first).to_be_in_viewport()

    def pan(key, axis, fraction):
        map_region.focus()
        before = page.evaluate(VIEW)
        page.keyboard.press(key)
        page.wait_for_function(f"""before => {{
            const view = ({VIEW})();
            return !view.animating && view.center[{axis}] !== before;
        }}""", arg=before["center"][axis])
        after = page.evaluate(VIEW)
        distance = after["center"][axis] - before["center"][axis]
        expected = before["size"][axis] * before["resolution"] * fraction
        assert isclose(distance, expected, abs_tol=1e-7), (before, after, distance, expected)
        assert after["center"][1 - axis] == before["center"][1 - axis]
        assert after["resolution"] == before["resolution"]
        return distance

    right = pan("ArrowRight", 0, 1 / 8)
    far_right = pan("Shift+ArrowRight", 0, 1 / 2)
    assert isclose(far_right, 4 * right, abs_tol=1e-7)
    down = pan("ArrowDown", 1, -1 / 8)

    search = page.get_by_role("textbox", name="Find a contig, scaffold or feature", exact=True)
    search.fill("Ch1")
    search.press("Home")
    before = position()
    search.press("ArrowRight")
    assert search.evaluate("input => input.selectionStart") == 1
    assert position() == before
    search.press("Escape")

    bookmark.click()
    expect(rows).to_have_count(2)
    expect(rows.first.get_by_role("textbox")).to_have_value("Location 2")
    expect(rows.last.get_by_role("textbox")).to_have_value("Location 1")
    expect(rows.first).to_have_class("bookmark-row bookmark-row-new")
    expect(rows.first).to_be_in_viewport()
    page.screenshot(path=str(output / "bookmark-reveal.png"))
    expect(rows.first).to_have_class("bookmark-row", timeout=3500)

    map_region.focus()
    page.keyboard.press("+")
    page.wait_for_function(f"""before => {{
        const view = ({VIEW})();
        return !view.animating && view.resolution < before;
    }}""", arg=saved["resolution"])
    rows.last.get_by_role("button", name="Go", exact=True).click()
    check.settle()
    assert position() == saved, (position(), saved)
    map_region.focus()
    page.screenshot(path=str(output / "focused-map.png"))

    anchor.click()
    expect(anchor).to_have_attribute("aria-pressed", "true")
    check.settle()
    before = position()
    map_region.focus()
    page.keyboard.press("ArrowRight")
    check.settle()
    assert position() == before
    assert not check.errors, check.errors
    (output / "results.json").write_text(json.dumps({
        "bookmarkRestored": saved, "right": right, "shiftRight": far_right,
        "down": down, "searchArrowStayedInInput": True, "lockedMapStayedStill": True,
    }, indent=2))
    print("Bookmark reveal, exact Go restoration and keyboard pan passed.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("jar", "webui", "borrelia", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    with (
        tempfile.TemporaryDirectory(prefix="hict-small-fixes-") as temporary,
        ThreadingHTTPServer(("127.0.0.1", 0), partial(Quiet, directory=args.webui.resolve())) as web,
        sync_playwright() as playwright,
    ):
        data = Path(temporary)
        shutil.copyfile(args.borrelia, data / "borrelia.hict.hdf5")
        threading.Thread(target=web.serve_forever, daemon=True).start()
        try:
            with HictTestServer(args.jar, data, args.output / "server.log") as api:
                browser = playwright.chromium.launch()
                page = browser.new_page(viewport={"width": 1440, "height": 900})
                page.set_default_timeout(20000)
                page.add_init_script("window.__HICT_BENCHMARK_ENABLED__=true")
                configure_test_api(page, api.url)
                check_map(page, BrowserCheck(page, args.output), f"http://127.0.0.1:{web.server_port}/", api.url, args.output)
                browser.close()
        finally:
            web.shutdown()


if __name__ == "__main__":
    main()
