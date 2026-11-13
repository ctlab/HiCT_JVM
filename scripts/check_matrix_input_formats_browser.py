#!/usr/bin/env python3
"""Verify public .hic v8/v9 conversion, exact source counts and cached reopening."""

import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import time

import numpy as np
from playwright.sync_api import expect, sync_playwright
from sequence_browser_harness import BrowserCheck


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("url", "api", "output", "data-dir"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument(
        "--files", nargs="+", default=["public-hic8.hic", "public-hic9.hic"]
    )
    parser.add_argument("--resolution", type=int, default=10000000)
    parser.add_argument("--job-timeout", type=float, default=120,
                        help="Seconds allowed for conversion including balancing; increase for large real inputs")
    args = parser.parse_args()
    data = Path(args.data_dir).absolute()
    if data != data.resolve() or not data.is_relative_to("/tmp"):
        parser.error("Use a physical disposable DATA_DIR under /tmp")
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    results = []
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        try:
            for filename in args.files:
                source = data / filename
                assert (
                    source.is_file()
                    and not source.is_symlink()
                    and source.stat().st_nlink == 1
                )
                original = hashlib.sha256(source.read_bytes()).hexdigest()
                version = int.from_bytes(source.read_bytes()[4:8], "little")
                assert version in (8, 9)
                context = browser.new_context(viewport={"width": 1440, "height": 1000})
                context.add_init_script("window.__HICT_BENCHMARK_ENABLED__=true")
                context.route(
                    "http://localhost:5000/**",
                    lambda route: route.continue_(
                        url=args.api
                        + route.request.url.removeprefix("http://localhost:5000")
                    ),
                )
                page = context.new_page()
                page.set_default_timeout(60000)
                check = BrowserCheck(page, output)

                def post(path, payload=None):
                    response = context.request.post(args.api + path, data=payload or {})
                    assert response.ok, response.text()
                    return response.json() if response.body() else None

                def pick(name):
                    check.menu("File", "Open…")
                    picker = page.get_by_role("dialog").filter(
                        has=page.get_by_role(
                            "heading", name="Open Hi-C dataset", exact=True
                        )
                    )
                    picker.wait_for(state="visible")
                    check.choose_file(name)
                    picker.get_by_role("button", name="Load", exact=True).click()

                try:
                    post("/close")
                    page.goto(args.url)
                    page.wait_for_function("window.__hictBenchmark")
                    pick(filename)
                    converter = page.locator(".converter-dialog")
                    converter.wait_for(state="visible")
                    expect(
                        converter.get_by_role(
                            "checkbox", name="Select " + filename, exact=True
                        )
                    ).to_be_checked()
                    converter.get_by_role("button", name="Continue", exact=True).click()
                    converter.locator(
                        'input[title="-1: automatic server CPU allocation"]'
                    ).fill("1")
                    with page.expect_response("**/convert/jobs/batch") as started:
                        converter.get_by_role(
                            "button", name="Start", exact=True
                        ).click()
                    assert started.value.ok, started.value.text()
                    job_ids = started.value.json()["jobIds"]
                    assert len(job_ids) == 1
                    deadline = time.monotonic() + args.job_timeout
                    while time.monotonic() < deadline:
                        response = context.request.get(
                            args.api + "/convert/jobs/" + job_ids[0]
                        )
                        assert response.ok, response.text()
                        job = response.json()
                        if job["status"] in ("finished", "failed", "cancelled"):
                            break
                        page.wait_for_timeout(100)
                    assert job["status"] == "finished", job
                    converted = filename.removesuffix(".hic") + ".hict.hdf5"
                    assert (data / converted).is_file()
                    expect(
                        converter.locator(".overall-status .status-pill")
                    ).to_have_text("Finished")
                    converter.get_by_role("button", name="Dismiss", exact=True).click()
                    pick(converted)
                    page.wait_for_function(
                        "name=>window.__hictBenchmark.manager()?.getOptions().filename===name",
                        arg=converted,
                    )
                    check.settle()
                    chromosome = page.evaluate(
                        "window.__hictBenchmark.manager().contigDimensionHolder.contigDescriptors[0].contigName"
                    )
                    native_bins = page.evaluate(
                        "resolution=>window.__hictBenchmark.manager().contigDimensionHolder.contigDescriptors[0].contigLengthBins.get(resolution)",
                        args.resolution,
                    )
                    toolchain = post("/convert/toolchain")
                    hictk = Path(toolchain["hictkCommand"])
                    assert hictk.is_file(), toolchain
                    commands = {
                        "bins": [
                            str(hictk),
                            "dump",
                            str(source),
                            "--resolution",
                            str(args.resolution),
                            "--table",
                            "bins",
                        ],
                        "pixels": [
                            str(hictk),
                            "dump",
                            str(source),
                            "--resolution",
                            str(args.resolution),
                        ],
                    }
                    dumped = {}
                    for table, command in commands.items():
                        result = subprocess.run(
                            command,
                            check=True,
                            capture_output=True,
                            text=True,
                            timeout=60,
                        )
                        dumped[table] = result.stdout.splitlines()
                        (output / (filename + "." + table + ".tsv")).write_text(
                            result.stdout
                        )
                    indices = [
                        index
                        for index, line in enumerate(dumped["bins"])
                        if line.split()[0] == chromosome
                    ]
                    assert len(indices) == native_bins and native_bins > 1, (
                        chromosome,
                        indices,
                        native_bins,
                    )
                    local = {index: position for position, index in enumerate(indices)}
                    expected = np.zeros((native_bins, native_bins), dtype=np.float32)
                    for line in dumped["pixels"]:
                        left, right, count = line.split()
                        left, right = int(left), int(right)
                        if left in local and right in local:
                            row, column = local[left], local[right]
                            value = np.float32(float(count))
                            expected[row, column] += value
                            if row != column:
                                expected[column, row] += value
                    assert np.count_nonzero(expected) > 0
                    query = {
                        "bpResolution": args.resolution,
                        "startRowPx": 0,
                        "startColPx": 0,
                        "endRowPx": native_bins,
                        "endColPx": native_bins,
                        "source": "PRIMARY",
                        "signalMode": "RAW_COUNTS",
                    }
                    actual = page.evaluate(
                        "async query=>Array.from((await window.__hictBenchmark.manager().networkManager.requestManager.queryMatrixFloat32(query)).values)",
                        query,
                    )
                    np.testing.assert_array_equal(
                        np.array(actual, dtype=np.float32), expected.ravel()
                    )
                    page.locator("#normalization-mode").select_option("none")
                    page.locator("#signal-transform-linear").click()
                    advanced = page.locator("#signal-advanced")
                    if advanced.get_attribute("open") is None:
                        advanced.locator(":scope > summary").click()
                    page.locator("#scaling-across-zoom").select_option("none")
                    page.get_by_role(
                        "spinbutton", name="Lower signal threshold", exact=True
                    ).fill("0")
                    page.get_by_role(
                        "spinbutton", name="Upper signal threshold", exact=True
                    ).fill(str(float(expected.max())))
                    page.get_by_role("button", name="Apply", exact=True).click()
                    check.settle()
                    # Compare rendered pixels at the same native level as the
                    # independent numerical oracle, even for multi-level real inputs.
                    page.evaluate("""resolution => {
                        const m=window.__hictBenchmark.manager(),l=m.getLayersManager();
                        l.setEnabledBpResolutions([resolution]);
                        l.fitViewToActiveExtent();
                    }""", args.resolution)
                    check.settle()
                    strongest = np.unravel_index(int(expected.argmax()), expected.shape)
                    point = {
                        "row": int(strongest[0]),
                        "column": int(strongest[1]),
                        "resolution": args.resolution,
                    }
                    page.wait_for_function(
                        """a=>{
                        const m=window.__hictBenchmark.manager(),l=m.getLayersManager();
                        const scale=l.getPixelResolutionForBpResolution(a.resolution);
                        const pixel=m.getMap().getPixelFromCoordinate([(a.column+.5)*scale,-(a.row+.5)*scale]);
                        return l.layersHolder.primaryHiCDataLayers.some(layer=>{
                            if(!layer.getVisible())return false;const rgba=layer.getData(pixel);
                            return rgba?.[3]>100 && rgba[0]>rgba[1]+20;
                        });
                    }""",
                        arg=point,
                    )
                    page.screenshot(path=str(output / (filename + ".png")))
                    jobs_before = {item["jobId"] for item in post("/convert/jobs/list")}
                    check.menu("File", "Close")
                    page.get_by_role(
                        "heading", name="No contact map open", exact=True
                    ).wait_for()
                    with page.expect_response("**/resolve_matrix_source") as resolved:
                        pick(filename)
                    assert (
                        resolved.value.ok
                        and resolved.value.json()["action"] == "REUSE_CONVERTED"
                    ), resolved.value.text()
                    page.wait_for_function(
                        "name=>window.__hictBenchmark.manager()?.getOptions().filename===name",
                        arg=converted,
                    )
                    check.settle()
                    reopened = page.evaluate(
                        "async query=>Array.from((await window.__hictBenchmark.manager().networkManager.requestManager.queryMatrixFloat32(query)).values)",
                        query,
                    )
                    np.testing.assert_array_equal(
                        np.array(reopened, dtype=np.float32), expected.ravel()
                    )
                    assert {
                        item["jobId"] for item in post("/convert/jobs/list")
                    } == jobs_before
                    assert hashlib.sha256(source.read_bytes()).hexdigest() == original
                    assert not check.errors, check.errors
                    results.append(
                        {
                            "input": filename,
                            "hicVersion": version,
                            "sha256": original,
                            "converted": converted,
                            "chromosome": chromosome,
                            "binsCompared": native_bins,
                            "valuesCompared": expected.size,
                            "nonzeroValues": int(np.count_nonzero(expected)),
                            "cachedReopen": resolved.value.json(),
                            "errors": check.errors,
                        }
                    )
                    print(
                        "Passed",
                        filename,
                        "with",
                        expected.size,
                        "exact source contact values",
                        flush=True,
                    )
                finally:
                    (output / "results.json").write_text(
                        json.dumps(results, indent=2) + "\n"
                    )
                    context.close()
        finally:
            browser.close()


if __name__ == "__main__":
    main()
