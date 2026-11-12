#!/usr/bin/env python3
"""Exercise real job services and result delivery through the navbar panel."""

import argparse
import json
import time
from pathlib import Path
from playwright.sync_api import sync_playwright

parser = argparse.ArgumentParser()
for name in ("url", "api", "output", "data-dir"):
    parser.add_argument("--" + name, required=True)
args = parser.parse_args()
data = Path(args.data_dir).resolve()
assert data.is_relative_to("/tmp")
(data / "jobs.bed").write_text("tiny\t0\t12017\t2\n")
out = Path(args.output)
out.mkdir(parents=True, exist_ok=True)
results = []
with sync_playwright() as pw:
    browser = pw.chromium.launch(headless=True)
    page = browser.new_page(viewport={"width": 1440, "height": 1000})
    page.set_default_timeout(60000)
    errors = []
    page.on("pageerror", lambda error: errors.append(str(error)))
    page.add_init_script("window.__HICT_BENCHMARK_ENABLED__=true")
    page.route(
        "http://localhost:5000/**",
        lambda route: route.continue_(
            url=args.api + route.request.url.removeprefix("http://localhost:5000")
        ),
    )

    def post(path, body=None):
        response = page.request.post(args.api + path, data=body or {})
        assert response.ok, (path, response.status, response.text())
        return response.json()

    def jobs():
        response = page.request.get(args.api + "/jobs")
        assert response.ok, response.text()
        return response.json()

    def await_kind(kind):
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            matches = [job for job in jobs() if job["kind"] == kind]
            if matches and all(not job["canCancel"] for job in matches):
                assert matches[0]["state"] == "finished", matches
                print("Finished", kind, flush=True)
                return matches[0]
            page.wait_for_timeout(200)
        raise AssertionError((kind, jobs()))

    def panel():
        page.get_by_role("button", name="Jobs", exact=False).click()
        dialog = page.get_by_role("dialog", name="Background jobs", exact=True)
        dialog.wait_for()
        return dialog

    def tools(label, title):
        page.get_by_role("link", name="Tools", exact=True).click()
        page.get_by_role("button", name=label, exact=True).click()
        dialog = page.get_by_role("dialog", name=title, exact=True)
        dialog.wait_for()
        return dialog

    page.goto(args.url)
    page.wait_for_function("window.__hictBenchmark")
    assert jobs() == []
    page.evaluate(
        "api => window.__hictBenchmark.open('mixed-primary.hict.hdf5',api,'mixed.fa')",
        args.api,
    )
    # Keep the matrix scale below automatic letter-level alignments during this gate.
    page.evaluate("""() => {const m=window.__hictBenchmark.manager();
      const s=m.viewAndLayersManager.currentViewState.selectionBorders;
      s.leftBP=[0,0]; s.rightBP=[256,256];}""")
    dialog = tools(
        "Find sequences in linked FASTA…", "Find sequences in the linked FASTA"
    )
    dialog.get_by_label("FASTA records, or one DNA sequence per line").fill(
        "".join((data / "mixed.fa").read_text().splitlines()[1:])[:80]
    )
    dialog.get_by_label("Search scope", exact=True).select_option("assembly")
    dialog.get_by_role("button", name="Find sequences", exact=True).click()
    dialog.get_by_role("button", name="Close", exact=True).first.click()
    dialog.wait_for(state="hidden")
    search = await_kind("search")
    assert search["outputs"][0]["action"] == "open_tracks", search
    dialog = panel()
    row = dialog.locator('[data-job-id="' + search["id"] + '"]')
    row.get_by_role("button", name="Open result", exact=True).click()
    page.wait_for_function(
        "window.__hictBenchmark.manager().linearTrackManager.getTracksSnapshot().length > 0"
    )
    results.append(
        {
            "test": "search survives dialog close and opens its result track",
            "job": search,
        }
    )

    dialog = tools("Generate ModDotPlot…", "Generate ModDotPlot")
    dialog.get_by_label("Analysis scope").select_option("selection")
    for checkbox in dialog.locator(".resolution-options input").all():
        checkbox.uncheck()
    dialog.locator('.resolution-options input[value="10"]').check()
    dialog.locator("#dotplot-display").select_option("none")
    dialog.get_by_role("button", name="Generate ModDotPlot", exact=True).click()
    dialog.get_by_role("button", name="Close", exact=True).first.click()
    identity = await_kind("moddotplot")
    dialog = panel()
    dialog.locator('[data-job-id="' + identity["id"] + '"]').get_by_role(
        "button", name="Open result", exact=True
    ).click()
    page.wait_for_function(
        """async () => (await window.__hictBenchmark.manager().networkManager.requestManager.getSecondarySourceStatus()).attached"""
    )
    results.append(
        {
            "test": "ModDotPlot survives dialog close and attaches from panel",
            "job": identity,
        }
    )

    post(
        "/convert/jobs",
        {
            "filename": "mixed-primary.mcool",
            "direction": "mcool-to-hict",
            "outputFilename": "jobs-converted.hict.hdf5",
            "chunkSize": 64,
            "buildResolutionPyramid": False,
            "balanceInputCoolers": False,
        },
    )
    results.append(
        {
            "test": "conversion adapter publishes completed matrix",
            "job": await_kind("conversion"),
        }
    )
    post(
        "/dotplot/jobs",
        {
            "fastaFiles": ["mixed.fa"],
            "outputDirectory": "jobs-dotplot",
            "binSize": 1000,
            "resolutions": "1000",
            "alignmentThreads": 1,
            "conversionThreads": 1,
        },
    )
    results.append(
        {
            "test": "legacy native dotplot adapter completes",
            "job": await_kind("legacy_dotplot"),
        }
    )
    page.evaluate(
        "() => window.__hictBenchmark.manager().networkManager.requestManager.closeSecondarySource()"
    )
    post("/normalization/coverage/prepare")
    results.append(
        {"test": "coverage adapter completes", "job": await_kind("coverage")}
    )
    track = post("/tracks/open", {"filename": "jobs.bed", "name": "Jobs coverage"})
    post("/tracks/precompute/start", {"force": True})
    results.append(
        {
            "test": "track precompute adapter completes",
            "job": await_kind("track_precompute"),
        }
    )
    post(
        "/sequence/dotplot/jobs",
        {
            "scope": "selection",
            "mergeCompatible": False,
            "region": {
                "rowStart": 0,
                "rowEnd": 256,
                "columnStart": 0,
                "columnEnd": 256,
            },
            "resolutions": [10],
            "threads": 1,
        },
    )
    results.append(
        {"test": "sequence dotplot adapter completes", "job": await_kind("dotplot")}
    )
    post(
        "/sequence/alignment_detail",
        {"rowStart": 0, "rowEnd": 256, "columnStart": 0, "columnEnd": 256},
    )
    results.append(
        {
            "test": "base alignment adapter completes",
            "job": await_kind("alignment_detail"),
        }
    )
    post("/examples/download/start", {"exampleId": "dnazoo-cheetah"})
    download = next(job for job in jobs() if job["kind"] == "example_download")
    dialog = panel()
    row = dialog.locator('[data-job-id="' + download["id"] + '"]')
    row.get_by_role("button", name="Cancel", exact=True).click()
    page.wait_for_function(
        """async api => (await (await fetch(api+'/jobs')).json()).find(j=>j.kind==='example_download')?.state==='cancelled'""",
        arg=args.api,
    )
    page.keyboard.press("Escape")
    results.append(
        {
            "test": "download cancels through panel",
            "job": next(job for job in jobs() if job["kind"] == "example_download"),
        }
    )
    page.get_by_role("link", name="File", exact=True).click()
    page.get_by_role("link", name="Open FASTA only…", exact=True).click()
    dialog = page.get_by_role("dialog", name="Open FASTA only", exact=True)
    dialog.get_by_label("FASTA file", exact=True).select_option("mixed.fa")
    dialog.get_by_role("button", name="Create and open", exact=True).click()
    dialog.get_by_role("button", name="Close", exact=True).first.click()
    fasta = await_kind("fasta_only")
    dialog = panel()
    dialog.locator('[data-job-id="' + fasta["id"] + '"]').get_by_role(
        "button", name="Open result", exact=True
    ).click()
    page.wait_for_function(
        """() => window.__hictBenchmark.manager().networkManager.requestManager.sourceCapabilities.PRIMARY?.contactDataState==='ABSENT'"""
    )
    results.append(
        {"test": "FASTA-only survives dialog close and opens from panel", "job": fasta}
    )
    assert not errors, errors
    panel()
    page.screenshot(path=str(out / "background-jobs.png"))
    (out / "results.json").write_text(json.dumps(results, indent=2))
    print(
        json.dumps(
            {
                "checks": len(results),
                "kinds": sorted({job["kind"] for job in jobs()}),
                "errors": errors,
            }
        )
    )
    browser.close()
