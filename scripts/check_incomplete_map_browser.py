#!/usr/bin/env python3
"""Reject an incomplete HiCT map through the picker while preserving edited pixels."""

import argparse
import json
from pathlib import Path
from playwright.sync_api import expect, sync_playwright
from sequence_browser_harness import BrowserCheck


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ("url", "api", "output"):
        parser.add_argument("--" + key, required=True)
    args = parser.parse_args()
    out = Path(args.output)
    out.mkdir(exist_ok=False)
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1440, "height": 1000})
        page.set_default_timeout(30000)
        page.add_init_script("window.__HICT_BENCHMARK_ENABLED__=true")
        page.route(
            "http://localhost:5000/**",
            lambda route: route.continue_(
                url=args.api + route.request.url.removeprefix("http://localhost:5000")
            ),
        )
        check = BrowserCheck(page, out)
        checks = []
        try:
            page.goto(args.url)
            page.evaluate(
                "async api=>await window.__hictBenchmark.open('structural.hict.hdf5',api)",
                args.api,
            )
            check.settle()
            check.reverse()
            page.wait_for_function(
                "window.__hictBenchmark.manager().networkManager.requestManager.hasUnsavedAssemblyEdits()"
            )
            page.evaluate("window.__retainedManager=window.__hictBenchmark.manager()")

            def state():
                return page.evaluate("""async()=>{
                    const m=window.__hictBenchmark.manager(),r=m.networkManager.requestManager;
                    const snapshot=await r.getSessionSnapshot(true);
                    const matrix=await r.queryMatrixFloat32({bpResolution:100,startRowPx:0,endRowPx:5,startColPx:0,endColPx:5,signalMode:'RAW_COUNTS'});
                    return {primary:snapshot.primary,pixels:Array.from(matrix.values),dirty:r.hasUnsavedAssemblyEdits()};
                }""")

            before = state()
            assert before["dirty"]
            for attempt in range(2):
                check.menu("File", "Open…")
                check.choose_file("incomplete.hict.hdf5")
                with page.expect_response(
                    lambda response: response.url.endswith("/open")
                ) as response:
                    page.get_by_role("button", name="Load", exact=True).click()
                    page.get_by_role(
                        "alertdialog", name="Unsaved assembly edits"
                    ).get_by_role("button", name="Discard edits", exact=True).click()
                assert response.value.status == 400, response.value.text()
                assert (
                    "Recreate it from the original matrix"
                    in response.value.json()["error"]
                )
                expect(
                    page.get_by_text(
                        "Recreate it from the original matrix", exact=False
                    ).first
                ).to_be_visible()
                assert state() == before
                assert page.evaluate(
                    "window.__hictBenchmark.manager()===window.__retainedManager"
                )
                page.screenshot(
                    path=str(out / f"incomplete-map-rejected-{attempt}.png")
                )
                page.keyboard.press("Escape")
                checks.append(
                    {
                        "test": f"incomplete-open-{attempt}-400-recovery-and-retained-edited-map",
                        "status": "passed",
                    }
                )
            assert not [
                e for e in check.errors if "400 (Bad Request)" not in e
            ], check.errors
            (out / "results.json").write_text(
                json.dumps(
                    {
                        "checks": checks,
                        "errors": [
                            e for e in check.errors if "400 (Bad Request)" not in e
                        ],
                    },
                    indent=2,
                )
            )
        finally:
            page.screenshot(path=str(out / "final.png"))
            browser.close()


if __name__ == "__main__":
    main()
