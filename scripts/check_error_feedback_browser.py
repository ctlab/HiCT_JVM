#!/usr/bin/env python3
"""Exercise failed requests and a failed dotplot job in a disposable browser session."""
import argparse
from pathlib import Path

from playwright.sync_api import expect, sync_playwright
from sequence_browser_harness import BrowserCheck, configure_test_api


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("api", "url", "file", "output"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--invalid-agp", default="invalid.agp")
    args = parser.parse_args()
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1440, "height": 900})
        page.set_default_timeout(30_000)
        page.add_init_script("window.__HICT_BENCHMARK_ENABLED__ = true")
        configure_test_api(page, args.api)
        check = BrowserCheck(page, output)
        page.goto(args.url)
        page.wait_for_function("window.__hictBenchmark")
        page.evaluate("([file, api]) => window.__hictBenchmark.open(file, api)", [args.file, args.api])
        check.settle()
        snapshot = lambda: page.request.post(args.api + "/get_agp_for_assembly", data={}).text()
        initial = snapshot()

        def error_toast(text):
            locator = page.locator('[data-sonner-toast][data-type="error"]').filter(has_text=text)
            expect(locator).to_have_count(1)
            expect(locator).to_be_visible()
            return locator

        check.select_range(region=False)
        page.route("**/reverse_selection_range", lambda route: route.abort("connectionclosed"))
        page.get_by_role("button", name="Reverse Selection", exact=True).click()
        error_toast("The server didn’t answer")
        page.wait_for_function("window.__hictBenchmark.manager().networkManager.requestManager.history.busy === 0")
        assert snapshot() == initial
        page.unroute("**/reverse_selection_range")
        page.screenshot(path=str(output / "network-failure.png"))

        check.menu("Assembly", "Load AGP…")
        picker = page.get_by_role("dialog", name="Load AGP")
        picker.locator(".file-name-cell").get_by_text(args.invalid_agp, exact=True).dblclick()
        error_toast("line 2")
        assert snapshot() == initial
        page.screenshot(path=str(output / "invalid-agp.png"))
        page.keyboard.press("Escape")

        defaults = page.request.post(args.api + "/sequence/settings", data={}).json()
        settings = {**defaults, "mapOpen": True, "fastaLinked": True,
                    "tools": {"hict-moddotplot": {"available": True}}}
        job = {
            "jobId": "browser-test-failed-job", "kind": "moddotplot", "status": "failed",
            "progress": 0, "logs": ["test adapter stopped after loading the region"],
            "error": "The test adapter exited with code 1",
        }
        page.route("**/sequence/settings", lambda route:
            route.fulfill(json=settings) if route.request.post_data_json.get("checkTools") else route.fallback())
        page.route("**/sequence/moddotplot/estimate", lambda route: route.fulfill(json={
            "requestedLevels": route.request.post_data_json["resolutions"],
            "overviewLevels": [], "rowBp": 3_000, "columnBp": 3_000,
            "estimatedBytes": 1_000_000,
        }))
        page.route("**/sequence/moddotplot/jobs", lambda route:
            route.fulfill(json={**job, "status": "submitted"}))
        page.route("**/sequence/jobs/list", lambda route: route.fulfill(json=[job]))
        page.evaluate("""() => {
            const layers = window.__hictBenchmark.manager().viewAndLayersManager;
            layers.setRegionBounds({rowStart:1000,rowEnd:4000,columnStart:1000,columnEnd:4000});
        }""")
        check.menu("Tools", "ModDotPlot…")
        dialog = page.get_by_role("dialog", name="ModDotPlot", exact=True)
        dialog.get_by_label("Analysis scope").select_option("selection")
        dialog.get_by_role("button", name="Generate", exact=True).click()
        error_toast("ModDotPlot could not finish")
        expect(dialog.get_by_role("alert")).to_have_text("ModDotPlot could not finish. The bundled tool reported an error.")
        assert dialog.evaluate("""modal => {
            const toaster = document.querySelector('[data-sonner-toaster]');
            return Number(getComputedStyle(toaster).zIndex) <
                Number(getComputedStyle(modal).zIndex);
        }""")
        expect(dialog.get_by_role("button", name="Generate", exact=True)).to_be_enabled()
        dialog.get_by_text("Show log", exact=True).click()
        expect(dialog.get_by_label("ModDotPlot log")).to_contain_text("test adapter stopped")
        assert snapshot() == initial
        page.screenshot(path=str(output / "dotplot-failure.png"))
        unexpected = [entry for entry in check.errors
                      if "net::ERR_CONNECTION_CLOSED" not in entry and "responded with a status of 400" not in entry]
        assert not unexpected, unexpected
        browser.close()


if __name__ == "__main__":
    main()
