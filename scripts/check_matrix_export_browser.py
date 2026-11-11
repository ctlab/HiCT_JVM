#!/usr/bin/env python3
"""Verify real Cooler exports, source/edit isolation and truthful export controls."""

import argparse
import json
from pathlib import Path
import time

import h5py
import numpy as np
from playwright.sync_api import sync_playwright
from sequence_browser_harness import BrowserCheck


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("url", "api", "output", "data-dir"):
        parser.add_argument("--" + name, required=True)
    args = parser.parse_args()
    data = Path(args.data_dir).absolute()
    if data != data.resolve() or not data.is_relative_to("/tmp"):
        raise ValueError("Use a nonsymlinked disposable DATA directory under /tmp")
    source = data / "export-fixture.hict.hdf5"
    if not source.is_file() or source.is_symlink() or source.stat().st_nlink != 1:
        raise ValueError("The server fixture must be an independent physical copy")
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
        try:
            page.goto(args.url)
            page.wait_for_function("window.__hictBenchmark")
            page.evaluate(
                'async api => {await window.__hictBenchmark.open("export-fixture.hict.hdf5",api);}',
                args.api,
            )
            check.settle()
            check.reverse()
            check.settle()
            for mode, all_levels, current, balance, level in (
                ("internal", True, True, False, 4),
                ("hictk", False, False, False, 4),
                ("auto", False, True, True, 4),
                ("internal", False, True, False, 0),
                ("auto", False, True, True, 0),
            ):
                check.menu("File", "Export matrix")
                dialog = page.get_by_role("dialog").filter(
                    has=page.get_by_role(
                        "heading", name="Export matrix to Cooler", exact=True
                    )
                )
                selects = dialog.locator("select")
                selects.nth(0).select_option(mode)
                compression = selects.nth(1)
                for algorithm in ("zstd", "lzf"):
                    assert compression.locator(
                        "option[value=" + algorithm + "]"
                    ).is_disabled()
                assert "Cooler export supports Deflate" in dialog.inner_text()
                compression.select_option("deflate")
                balancing = dialog.locator("#export-balance-coolers")
                if mode == "internal":
                    page.wait_for_function(
                        'document.querySelector("#export-balance-coolers").disabled'
                    )
                    assert (
                        "Balancing requires auto or hictk-assisted export."
                        in dialog.inner_text()
                    )
                else:
                    assert balancing.is_enabled()
                    balancing.set_checked(balance)
                dialog.locator("#export-current-assembly").set_checked(current)
                dialog.locator("#export-all-resolutions").set_checked(all_levels)
                target = f"export-{mode}-{level}." + ("mcool" if all_levels else "cool")
                dialog.get_by_placeholder("matrix.mcool", exact=True).fill(target)
                numeric = dialog.locator("input[type=number]")
                numeric.nth(0).fill(str(level))
                numeric.nth(1).fill("1")
                with page.expect_response(
                    lambda response: response.url.endswith("/convert/jobs")
                ) as response:
                    dialog.get_by_role(
                        "button", name="Export Cooler", exact=True
                    ).click()
                assert response.value.status in (200, 202), response.value.text()
                request = response.value.request.post_data_json
                assert (
                    request["compressionAlgorithm"] == "deflate"
                    and request["compression"] == level
                )
                assert (
                    request["parallelism"] == 1
                    and request["useCurrentAssembly"] == current
                )
                assert request["balanceExportedCoolers"] == balance, request
                job = response.value.json()
                job_id = job["jobId"]
                deadline = time.monotonic() + 180
                while time.monotonic() < deadline:
                    status_response = page.request.get(
                        args.api + "/convert/jobs/" + job_id
                    )
                    assert status_response.ok, status_response.text()
                    job = status_response.json()
                    if job["status"] in ("finished", "failed", "cancelled"):
                        break
                    page.wait_for_timeout(100)
                assert job["status"] == "finished", job
                levels = (100, 200) if all_levels else (100,)
                with h5py.File(data / target) as exported:
                    if all_levels:
                        assert sorted(map(int, exported["resolutions"].keys())) == [
                            100,
                            200,
                        ]
                    for resolution in levels:
                        group = (
                            exported[f"resolutions/{resolution}"]
                            if all_levels
                            else exported
                        )
                        n = 4000 // resolution
                        # Independent deterministic source oracle used by the sequence fixture.
                        expected = {}
                        for row in range(n):
                            for column in range(row, n):
                                key = (
                                    (n - 1 - column, n - 1 - row)
                                    if current
                                    else (row, column)
                                )
                                expected[key] = (row + 1) * 43 + column + 1
                        actual = {
                            (int(row), int(column)): int(count)
                            for row, column, count in zip(
                                group["pixels/bin1_id"][:],
                                group["pixels/bin2_id"][:],
                                group["pixels/count"][:],
                            )
                        }
                        assert actual == expected, (mode, resolution, actual, expected)
                        interactions = group["pixels/count"]
                        assert interactions.compression == ("gzip" if level else None)
                        assert interactions.compression_opts == (
                            level if level else None
                        )
                        assert group["bins/start"].shape == (n,)
                        assert int(group["chroms/length"][:].sum()) == 4000
                        if balance:
                            weights = group["bins/weight"][:]
                            assert (
                                len(weights) == n
                                and np.isfinite(weights).any()
                                and np.nanmin(weights) > 0
                                and np.nanmax(weights) / np.nanmin(weights) > 1.01
                            )
                results.append(
                    {
                        "mode": mode,
                        "compressionLevel": level,
                        "resolutions": levels,
                        "usesEditedAssembly": current,
                        "balanced": balance,
                        "allContactsExact": True,
                        "jobId": job_id,
                    }
                )
                dialog.locator(".modal-header .btn-close").click()
                dialog.wait_for(state="hidden")
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
