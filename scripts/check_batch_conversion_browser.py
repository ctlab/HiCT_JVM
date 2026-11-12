#!/usr/bin/env python3
"""Verify batch selection controls and independently query a real converted map."""

import argparse
import json
import hashlib
from pathlib import Path
import cooler
import h5py
import numpy as np
import pandas as pd
import time

from playwright.sync_api import expect, sync_playwright
from sequence_browser_harness import BrowserCheck


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("url", "api", "output", "data-dir"):
        parser.add_argument("--" + name, required=True)
    args = parser.parse_args()
    data = Path(args.data_dir).absolute()
    if data != data.resolve() or not data.is_relative_to("/tmp"):
        raise ValueError("Use a nonsymlinked disposable directory under /tmp")
    source = data / "tiny-primary.mcool"
    assert source.is_file() and not source.is_symlink() and source.stat().st_nlink == 1
    matrix = data / "batch-new.mcool"
    for resolution, pixels in (
        (4, [(0, 0, 1), (0, 1, 3), (1, 2, 7), (4, 4, 11)]),
        (10, [(0, 0, 1), (1, 1, 11)]),
    ):
        starts = np.arange(0, 17, resolution)
        bins = pd.DataFrame(
            {
                "chrom": ["assembly"] * len(starts),
                "start": starts,
                "end": np.minimum(starts + resolution, 17),
            }
        )
        contacts = pd.DataFrame(pixels, columns=["bin1_id", "bin2_id", "count"])
        cooler.create_cooler(
            str(matrix) + f"::resolutions/{resolution}",
            bins,
            contacts,
            ordered=True,
            mode="a",
        )
    with h5py.File(matrix, "r+") as handle:
        handle.attrs["format"] = "HDF5::MCOOL"
        handle.attrs["format-version"] = 2
    sources = list(data.rglob("*.mcool"))
    expected = {p.name for p in sources}
    converted = {
        p.name
        for p in sources
        if p.with_name(p.name.removesuffix(".mcool") + ".hict.hdf5").exists()
    }
    assert converted and expected - converted == {"batch-new.mcool"}
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

        def selected(dialog):
            return set(
                dialog.locator(".file-row-checkbox:checked").evaluate_all(
                    "nodes=>nodes.map(n=>n.getAttribute('aria-label').replace(/^Select /,''))"
                )
            )

        def converter():
            check.menu("File", "Convert matrices")
            dialog = page.locator(".converter-dialog")
            dialog.get_by_role(
                "checkbox", name="Select batch-new.mcool", exact=True
            ).wait_for()
            return dialog

        try:
            page.goto(args.url)
            page.wait_for_function("window.__hictBenchmark")
            dialog = converter()
            assert not selected(dialog)
            expect(
                dialog.get_by_role("button", name="Continue", exact=True)
            ).to_be_disabled()
            for action, wanted in [
                ("Select All", expected),
                ("Select None", set()),
                ("Invert Selection", expected),
                ("Invert Selection", set()),
                ("Select All Converted", converted),
                ("Select All Unconverted", expected - converted),
            ]:
                dialog.get_by_role("button", name=action, exact=True).click()
                assert selected(dialog) == wanted, (action, selected(dialog), wanted)
            dialog.get_by_role("button", name="Continue", exact=True).click()
            dialog.get_by_role("button", name="Back", exact=True).click()
            assert selected(dialog) == {"batch-new.mcool"}
            results.append(
                {
                    "test": "All, None, Invert, Converted, Unconverted and settings Back preserve exact file sets",
                    "files": sorted(expected),
                }
            )
            notice = dialog.locator("details.toolchain-details")
            notice.locator("summary").click()
            assert notice.inner_text().strip()
            assert (
                dialog.get_by_role("link", name="hictk", exact=True).get_attribute(
                    "href"
                )
                == "https://github.com/paulsengroup/hictk"
            )
            dialog.get_by_role("button", name="Continue", exact=True).click()
            with page.expect_response(
                lambda r: r.request.method == "POST" and "/convert/" in r.url
            ) as started:
                dialog.get_by_role("button", name="Start", exact=True).click()
            assert started.value.ok, started.value.text()
            request = started.value.request.post_data_json
            assert (
                request["files"] == ["batch-new.mcool"] and not request["overwrite"]
            ), request
            jobs = started.value.json()["jobIds"]
            assert len(jobs) == 1
            dialog.get_by_role("button", name="Dismiss", exact=True).last.click()
            deadline = time.monotonic() + 90
            while time.monotonic() < deadline:
                response = page.request.get(args.api + "/convert/jobs/" + jobs[0])
                assert response.ok, response.text()
                state = response.json()
                assert state["status"] not in ("failed", "cancelled"), state
                if state["status"] == "finished":
                    break
                page.wait_for_timeout(100)
            else:
                raise AssertionError(state)
            assert (data / "batch-new.hict.hdf5").is_file()
            page.evaluate(
                'async a=>{await window.__hictBenchmark.open("batch-new.hict.hdf5",a)}',
                args.api,
            )
            check.settle()
            actual = page.evaluate(
                """async()=>Array.from((await window.__hictBenchmark.manager().networkManager.requestManager.queryMatrixFloat32({bpResolution:4,startRowPx:0,endRowPx:2,startColPx:0,endColPx:2,source:'PRIMARY',signalMode:'RAW_COUNTS'})).values)"""
            )
            assert actual == [1, 3, 3, 0], actual
            results.append(
                {
                    "test": "UI batch conversion survives dialog close and produces exact independent contacts",
                    "values": actual,
                    "jobId": jobs[0],
                }
            )
            dialog = converter()
            dialog.get_by_role("button", name="Select None", exact=True).click()
            dialog.get_by_role(
                "checkbox", name="Select batch-new.mcool", exact=True
            ).check()
            dialog.get_by_role("button", name="Continue", exact=True).click()
            output_path = data / "batch-new.hict.hdf5"
            before_hash = hashlib.sha256(output_path.read_bytes()).hexdigest()
            before_response = page.request.post(
                args.api + "/convert/jobs/list", data={}
            )
            assert before_response.ok, before_response.text()
            before_jobs = before_response.json()
            assert len(before_jobs) == 1 and before_jobs[0]["status"] == "finished"
            dialog.get_by_role("button", name="Start", exact=True).click()
            dialog.get_by_role("button", name="Cancel", exact=True).click()
            assert hashlib.sha256(output_path.read_bytes()).hexdigest() == before_hash
            assert (
                page.request.post(args.api + "/convert/jobs/list", data={}).json()
                == before_jobs
            )
            assert (data / "batch-new.hict.hdf5").is_file()
            dialog.get_by_role("button", name="Dismiss", exact=True).last.click()
            results.append(
                {
                    "test": "Existing converted output is protected by overwrite cancellation"
                }
            )
            dialog = converter()
            with page.expect_response(
                lambda r: r.url.endswith("/convert/jobs/list")
            ) as listed:
                dialog.get_by_role(
                    "button", name="View current conversion jobs", exact=True
                ).click()
            assert listed.value.ok
            assert all(
                job["status"] not in ("queued", "running")
                for job in listed.value.json()
            )
            expect(
                dialog.get_by_text("No active conversion jobs.", exact=True)
            ).to_be_visible()
            dialog.get_by_role("button", name="Dismiss", exact=True).last.click()
            results.append(
                {
                    "test": "Current conversion jobs agrees with the empty active server set"
                }
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
