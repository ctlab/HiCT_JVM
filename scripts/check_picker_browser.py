#!/usr/bin/env python3
"""Verify picker navigation, filtering, modes, selection and exact loaded data."""

import argparse
import json
from pathlib import Path
import shutil

from playwright.sync_api import expect, sync_playwright
from sequence_browser_harness import BrowserCheck


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("url", "api", "output", "data-dir"):
        parser.add_argument("--" + name, required=True)
    args = parser.parse_args()
    data = Path(args.data_dir).absolute()
    if data != data.resolve() or not data.is_relative_to("/tmp"):
        raise ValueError("Use a nonsymlinked disposable DATA directory under /tmp")
    source = data / "tiny-primary.hict.hdf5"
    assert source.is_file() and not source.is_symlink() and source.stat().st_nlink == 1
    nested = data / "picker-folder/nested"
    nested.mkdir(parents=True, exist_ok=False)
    shutil.copyfile(source, nested / source.name)
    (data / "picker-other.txt").write_text("unsupported matrix contents\n")
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
        try:
            page.goto(args.url)
            page.wait_for_function("window.__hictBenchmark")
            check.menu("File", "Open map…")
            dialog = page.get_by_role("dialog").last
            path = dialog.get_by_placeholder(
                "Relative folder or file path under DATA_DIR"
            )
            filter_input = dialog.get_by_placeholder(
                "Filter current folder by file name or path"
            )
            load = dialog.get_by_role("button", name="Load", exact=True)
            assert load.is_disabled()
            dialog.locator(".file-name-cell").get_by_text(
                "tiny-primary.hict.hdf5", exact=True
            ).wait_for()
            all_rows = dialog.locator(".file-name-cell").count()
            filter_input.fill("tiny-primary.hict.hdf5")
            page.wait_for_function(
                'document.querySelectorAll(".file-name-cell").length===1'
            )
            filter_input.clear()
            page.wait_for_function(
                'count => document.querySelectorAll(".file-name-cell").length===count',
                arg=all_rows,
            )
            assert (
                dialog.locator(".file-name-cell")
                .get_by_text("picker-other.txt", exact=True)
                .count()
                == 0
            )
            dialog.get_by_label("Show all files", exact=True).check()
            dialog.locator(".file-name-cell").get_by_text(
                "picker-other.txt", exact=True
            ).wait_for()
            dialog.get_by_label("Show all files", exact=True).uncheck()
            dialog.locator(".file-name-cell").get_by_text(
                "picker-other.txt", exact=True
            ).wait_for(state="detached")
            results.append(
                {
                    "test": "Filter and supported-file visibility preserve the listed directory"
                }
            )
            path.fill("picker-folder")
            dialog.get_by_role("button", name="Go", exact=True).click()
            expect(path).to_have_value("picker-folder")
            dialog.get_by_role("button", name="Up", exact=True).click()
            expect(path).to_have_value("")
            dialog.get_by_role("button", name="Up", exact=True).click()
            assert path.input_value() == ""
            path.fill("picker-folder")
            path.press("Enter")
            expect(path).to_have_value("picker-folder")
            dialog.get_by_role("button", name="Tree", exact=True).click()
            dialog.locator(".tree-node-content").filter(
                has=page.get_by_text("nested", exact=True)
            ).dblclick()
            expect(path).to_have_value("picker-folder/nested")
            dialog.get_by_role("button", name="Root", exact=True).click()
            expect(path).to_have_value("")
            dialog.get_by_role("button", name="Explorer", exact=True).click()
            response = page.request.post(
                args.api + "/list_directory", data={"directory": "../"}
            )
            assert response.status == 400, response.text()
            listed_before = dialog.locator(".file-name-cell").all_text_contents()
            errors_before = len(check.errors)
            path.fill("../")
            with page.expect_response(
                lambda response: response.url.endswith("/list_directory") and response.status == 400
            ):
                dialog.get_by_role("button", name="Go", exact=True).click()
            alert = dialog.locator(".alert-danger")
            expect(alert).to_be_visible()
            assert alert.inner_text().strip()
            expect(dialog.locator(".file-name-cell")).to_have_text(listed_before)
            assert load.is_disabled()
            # Chromium reports the deliberately rejected directory request once.
            expected_errors = check.errors[errors_before:]
            assert len(expected_errors) <= 1 and all(
                message == "Failed to load resource: the server responded with a status of 400 (Bad Request)"
                for message in expected_errors
            ), expected_errors
            del check.errors[errors_before:]
            dialog.get_by_role("button", name="Root", exact=True).click()
            expect(alert).to_be_hidden()
            expect(path).to_have_value("")
            expect(dialog.locator(".file-name-cell")).to_have_text(listed_before)
            results.append(
                {"test": "Go, Enter, tree, Up and Root stay inside DATA_DIR and recover after a rejected path"}
            )
            path.fill("picker-folder/nested")
            dialog.get_by_role("button", name="Go", exact=True).click()
            dialog.locator(".file-name-cell").get_by_text(
                "tiny-primary.hict.hdf5", exact=True
            ).click()
            assert load.is_enabled()
            dialog.get_by_role("button", name="Tree", exact=True).click()
            assert load.is_enabled()
            dialog.get_by_role("button", name="Explorer", exact=True).click()
            assert load.is_enabled()
            with page.expect_response("**/open") as opened:
                load.click()
            assert opened.value.ok, opened.value.text()
            page.wait_for_function(
                'window.__hictBenchmark.manager()?.getOptions().filename==="picker-folder/nested/tiny-primary.hict.hdf5"'
            )
            check.settle()
            actual = page.evaluate(
                """async () => Array.from((await window.__hictBenchmark.manager().networkManager.requestManager.queryMatrixFloat32({
                bpResolution:4,startRowPx:0,endRowPx:2,startColPx:0,endColPx:2,source:'PRIMARY',signalMode:'RAW_COUNTS'})).values)"""
            )
            assert actual == [1, 3, 3, 0], actual
            results.append(
                {
                    "test": "Explorer/Tree preserve selection and Load opens the exact nested file",
                    "values": actual,
                }
            )
            before = page.evaluate(
                "async () => (await window.__hictBenchmark.manager().networkManager.requestManager.getSessionSnapshot(true)).primary.agpSha256"
            )
            check.menu("File", "Open…")
            page.get_by_role("dialog").last.get_by_role(
                "button", name="Dismiss", exact=True
            ).click()
            after = page.evaluate(
                "async () => (await window.__hictBenchmark.manager().networkManager.requestManager.getSessionSnapshot(true)).primary.agpSha256"
            )
            assert before == after
            results.append({"test": "Dismiss preserves the actual open assembly"})
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
