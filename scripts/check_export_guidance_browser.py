#!/usr/bin/env python3
"""Verify save destinations and FASTA guidance without altering the disposable source."""
import argparse
from pathlib import Path

from playwright.sync_api import expect, sync_playwright
from sequence_browser_harness import BrowserCheck


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ("api", "url", "file", "output"):
        parser.add_argument("--" + key, required=True)
    args = parser.parse_args()
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1440, "height": 900}, accept_downloads=True)
        page.set_default_timeout(30_000)
        page.add_init_script("window.__HICT_BENCHMARK_ENABLED__ = true")
        page.route("http://localhost:5000/**", lambda route: route.continue_(
            url=args.api + route.request.url.removeprefix("http://localhost:5000")))
        check = BrowserCheck(page, output)
        page.add_init_script("""(() => {
            window.savePickerCalls = [];
            const original = window.showSaveFilePicker;
            Object.defineProperty(window, "showSaveFilePicker", {
              get: () => async options => {
                window.savePickerCalls.push(options.suggestedName);
                return window.allowSave
                  ? original(options)
                  : Promise.reject(new DOMException("Cancelled", "AbortError"));
              }
            });
        })()""")
        page.goto(args.url)
        page.wait_for_function("window.__hictBenchmark")
        page.evaluate("([file, api]) => window.__hictBenchmark.open(file, api)", [args.file, args.api])
        check.settle()

        check.menu("Assembly", "Export FASTA…")
        prompt = page.get_by_role("alertdialog", name="Link a FASTA file first")
        expect(prompt).to_be_visible()
        assert page.evaluate("window.savePickerCalls") == []
        page.screenshot(path=str(output / "fasta-required.png"))
        prompt.get_by_role("button", name="Link FASTA…").click()
        expect(page.get_by_role("heading", name="Select FASTA file")).to_be_visible()
        page.keyboard.press("Escape")

        check.menu("Assembly", "Export AGP…")
        page.wait_for_function("window.savePickerCalls.length === 1")
        assert page.evaluate("window.savePickerCalls[0].endsWith('.agp')")
        page.evaluate("window.allowSave = true")
        with page.expect_download() as download:
            check.menu("Assembly", "Export AGP…")
        path = output / download.value.suggested_filename
        download.value.save_as(path)
        assert path.suffix == ".agp" and path.stat().st_size > 0
        assert not check.errors, check.errors
        browser.close()


if __name__ == "__main__":
    main()
