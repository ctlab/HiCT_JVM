#!/usr/bin/env python3
"""Check that the workspace stays white in both operating-system themes."""

import argparse
from functools import partial
from http.server import ThreadingHTTPServer
from pathlib import Path
import shutil
import tempfile
import threading

from playwright.sync_api import expect, sync_playwright

from check_shift_selection_browser import Quiet
from sequence_browser_harness import BrowserCheck, HictTestServer, configure_test_api


WHITE = "rgb(255, 255, 255)"
MATRIX_INSIDE_VIEWPORT = """() => {
    const map = window.__hictBenchmark.manager().getMap();
    const view = map.getView();
    const [left, bottom, right, top] = view.getProjection().getExtent();
    const [leftPixel, topPixel] = map.getPixelFromCoordinate([left, top]);
    const [rightPixel, bottomPixel] = map.getPixelFromCoordinate([right, bottom]);
    const [width, height] = map.getSize();
    return Math.max(leftPixel, topPixel, width - rightPixel, height - bottomPixel) > 5;
}"""


def check_theme(browser, scheme, web_url, api_url, output):
    page = browser.new_page(viewport={"width": 1440, "height": 900}, color_scheme=scheme)
    try:
        page.add_init_script("window.__HICT_BENCHMARK_ENABLED__ = true")
        configure_test_api(page, api_url)
        check = BrowserCheck(page, output)
        page.goto(web_url)
        expect(page.get_by_role("heading", name="No contact map open")).to_be_visible()
        assert page.evaluate("(scheme) => matchMedia('(prefers-color-scheme: ' + scheme + ')').matches", scheme)
        body = page.locator("body")
        viewport = page.locator("#hic-contact-map")
        expect(body).to_have_css("background-color", WHITE)
        expect(viewport).to_have_css("background-color", WHITE)
        expect(page.locator(".workspace-component--empty")).to_have_css("background-color", WHITE)
        expect(page.locator("html")).to_have_css("color-scheme", "light")
        if scheme == "dark":
            page.screenshot(path=str(output / "dark-empty.png"))

        page.evaluate("api => window.__hictBenchmark.open('borrelia.hict.hdf5', api)", api_url)
        check.settle()
        viewport.focus()
        page.keyboard.press("-")
        check.settle()
        page.wait_for_function(MATRIX_INSIDE_VIEWPORT)
        expect(body).to_have_css("background-color", WHITE)
        expect(viewport).to_have_css("background-color", WHITE)
        if scheme == "dark":
            page.screenshot(path=str(output / "dark-map.png"))
        assert not check.errors, check.errors
        print(f"{scheme}: white body and viewport with no map and beyond the open matrix.")
    finally:
        page.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("jar", "webui", "borrelia", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    with (
        tempfile.TemporaryDirectory(prefix="hict-light-workspace-") as temporary,
        ThreadingHTTPServer(("127.0.0.1", 0), partial(Quiet, directory=args.webui.resolve())) as web,
        sync_playwright() as playwright,
    ):
        data = Path(temporary)
        shutil.copyfile(args.borrelia, data / "borrelia.hict.hdf5")
        threading.Thread(target=web.serve_forever, daemon=True).start()
        try:
            with HictTestServer(args.jar, data, args.output / "server.log") as api:
                browser = playwright.chromium.launch()
                for scheme in ("dark", "light"):
                    check_theme(browser, scheme, f"http://127.0.0.1:{web.server_port}/", api.url, args.output)
                browser.close()
        finally:
            web.shutdown()


if __name__ == "__main__":
    main()
