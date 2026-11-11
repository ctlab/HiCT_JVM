#!/usr/bin/env python3
"""Verify real journal paging, immutable restore IDs, branch invalidation and text Undo."""

import argparse
import hashlib
import json
from pathlib import Path

from playwright.sync_api import sync_playwright
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
        page.set_default_timeout(60000)
        page.add_init_script("window.__HICT_BENCHMARK_ENABLED__=true")
        page.route(
            "http://localhost:5000/**",
            lambda route: route.continue_(
                url=args.api + route.request.url.removeprefix("http://localhost:5000")
            ),
        )
        check = BrowserCheck(page, output)

        def post(path, body):
            response = page.request.post(args.api + path, data=body)
            assert response.ok, response.text()
            return response

        def agp_hash():
            return hashlib.sha256(post("/get_agp_for_assembly", {}).body()).hexdigest()

        def journal():
            return post("/assembly/history", {"offset": 0, "limit": 200}).json()

        def restore_button(name, expected):
            with page.expect_response("**/assembly/history/restore") as response:
                page.get_by_role("button", name=name, exact=True).click()
            assert response.value.ok, response.value.text()
            check.settle()
            assert agp_hash() == expected

        try:
            # Seed a journal larger than one page before attaching a fresh UI client.
            post("/open", {"filename": "structural.hict.hdf5"})
            original = agp_hash()
            reversed_hash = None
            for index in range(120):
                post("/reverse_selection_range", {"startBP": 0, "endBP": 400000})
                if index == 0:
                    reversed_hash = agp_hash()
            assert original != reversed_hash and agp_hash() == original
            seeded = journal()
            assert (
                seeded["summary"]["size"] == 121
                and seeded["summary"]["position"] == 120
            )
            page.goto(args.url)
            page.wait_for_function("window.__hictBenchmark")
            check.menu("File", "Attach")
            page.wait_for_function(
                "window.__hictBenchmark.manager()?.contigDimensionHolder.contig_count===4"
            )
            check.settle()
            graph = post("/render_pipeline/get", {}).json()
            page.get_by_role("button", name="Journal", exact=True).click()
            section = page.get_by_role("region", name="Edit journal", exact=True)
            page.wait_for_function(
                'document.querySelector(".journal-entries")?.getAttribute("aria-busy")==="false"'
            )
            with page.expect_response("**/assembly/history") as earlier:
                section.get_by_role("button", name="Earlier", exact=True).click()
            earlier_page = earlier.value.json()
            assert earlier_page["offset"] == 0 and len(earlier_page["entries"]) == 100
            assert agp_hash() == original and journal()["summary"] == seeded["summary"]
            with page.expect_response("**/assembly/history") as later:
                section.get_by_role("button", name="Later", exact=True).click()
            later_page = later.value.json()
            assert later_page["offset"] == 100 and len(later_page["entries"]) == 21
            page.wait_for_function(
                'document.querySelectorAll(".journal-entry[aria-current=step]").length===1'
            )
            assert agp_hash() == original and journal()["summary"] == seeded["summary"]
            results.append(
                {
                    "test": "Earlier/Later changes pages without restoring assembly",
                    "size": 121,
                    "offsets": [0, 100],
                }
            )

            target = later_page["entries"][3]
            assert target["id"] % 2 == 1
            with page.expect_response("**/assembly/history/restore") as restored:
                section.locator(".journal-entry").nth(3).click()
            assert restored.value.ok, restored.value.text()
            request = restored.value.request.post_data_json
            assert (
                request["action"] == "restore" and request["id"] == target["id"]
            ), request
            check.settle()
            assert agp_hash() == reversed_hash
            assert journal()["summary"]["currentId"] == target["id"]
            assert (
                page.evaluate(
                    "window.__hictBenchmark.manager().contigDimensionHolder.contigDescriptors[0].direction"
                )
                == 0
            )
            assert post("/render_pipeline/get", {}).json() == graph
            results.append(
                {
                    "test": "Repeated label restores the exact immutable ID and native state",
                    "id": target["id"],
                    "agpSha256": reversed_hash,
                }
            )
            restore_button("Redo assembly edit", original)
            restore_button("Undo assembly edit", reversed_hash)
            check.reverse()
            check.settle()
            branched = journal()["summary"]
            assert agp_hash() == original and not branched["canRedo"]
            assert branched["currentId"] > seeded["summary"]["currentId"]
            assert branched["size"] == target["id"] + 2
            results.append(
                {
                    "test": "New edit invalidates the redo branch and keeps IDs monotonic",
                    "summary": branched,
                }
            )
            field = page.locator('input[placeholder^="Find contig"]').first
            field.fill("typing")
            field.press("End")
            page.keyboard.type("x")
            before_text = field.input_value()
            before = agp_hash()
            field.press("Control+z")
            assert field.input_value() != before_text
            assert agp_hash() == before and journal()["summary"] == branched
            results.append(
                {"test": "Focused text Undo leaves assembly and journal unchanged"}
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
