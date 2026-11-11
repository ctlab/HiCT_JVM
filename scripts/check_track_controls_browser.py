#!/usr/bin/env python3
"""Verify track naming, imported weights, background pixels and precompute controls."""

import argparse
import json
import shutil
from pathlib import Path

from playwright.sync_api import expect, sync_playwright
from sequence_browser_harness import BrowserCheck


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("url", "api", "output", "data-dir"):
        parser.add_argument("--" + name, required=True)
    args = parser.parse_args()
    data = Path(args.data_dir).absolute()
    if data != data.resolve() or not data.is_relative_to("/tmp"):
        parser.error("Use a nonsymlinked disposable DATA_DIR under /tmp")
    bed = data / "qa-release-track.bed"
    secondary = data / "qa-secondary-weighted.hict.hdf5"
    assert not bed.exists() and not secondary.exists()
    bed.write_text("tiny\t0\t3\tRelease feature\t42\t+\n")
    shutil.copy2(data / "tiny-weighted.hict.hdf5", secondary)
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
        dialog = page.get_by_role("dialog").filter(
            has=page.get_by_role("heading", name="Tracks and layers", exact=True)
        )

        def api(path, payload=None):
            response = page.request.post(args.api + path, data=payload or {})
            assert response.ok, response.text()
            if not response.body():
                return None
            if "json" in response.headers.get("content-type", ""):
                return response.json()
            return response.text()

        def open_manager():
            check.menu("View", "Tracks and layers...")
            dialog.wait_for(state="visible")

        def close_manager():
            dialog.get_by_role("button", name="Close", exact=True).click()
            dialog.wait_for(state="hidden")
            check.settle()

        def browse(index, heading, filename):
            dialog.get_by_role("button", name="Browse…", exact=True).nth(index).click()
            picker = page.get_by_role("dialog").filter(
                has=page.get_by_role("heading", name=heading, exact=True)
            )
            picker.locator(".file-name-cell").get_by_text(
                filename, exact=True
            ).dblclick()
            picker.wait_for(state="hidden")

        def query():
            return api(
                "/tracks/query_1d",
                {
                    "unit": "BP",
                    "startBP": 0,
                    "endBP": 20,
                    "widthPx": 5,
                    "bpResolution": 4,
                },
            )

        def weight_values(source):
            tracks = api("/tracks/list")
            selected = [
                track
                for track in tracks
                if track["sourceFile"] == "__internal__/cooler_weights/" + source
            ]
            assert len(selected) == 1, tracks
            rendered = next(
                track
                for track in query()["tracks"]
                if track["trackId"] == selected[0]["trackId"]
            )
            assert [item["value"] for item in rendered["bins"]] == [
                2,
                3,
                4,
                5,
                6,
            ], rendered
            assert [(item["startBp"], item["endBp"]) for item in rendered["bins"]] == [
                (0, 4),
                (4, 8),
                (8, 12),
                (12, 16),
                (16, 20),
            ], rendered
            return rendered

        def assert_background(expected):
            page.wait_for_function(
                """expected=>{
                const selectors=['#horizontal-igv-track-div canvas','#vertical-igv-track-div canvas'];
                return selectors.every(selector=>{
                    const canvas=document.querySelector(selector);
                    if(!canvas || canvas.width<3 || canvas.height<3)return false;
                    const rgba=Array.from(canvas.getContext('2d').getImageData(2,2,1,1).data);
                    return expected.every((value,index)=>rgba[index]===value);
                });
            }""",
                arg=expected,
            )
            exported = page.evaluate("""async()=>{
                const manager=window.__hictBenchmark.manager().linearTrackManager;
                return await Promise.all(['horizontal','vertical'].map(async orientation=>{
                    const canvas=await manager.renderFullExtentCanvasForExport(orientation,{
                        bpResolution:4,startPx:0,endPx:5,trackPanelSizePx:80});
                    return {orientation,rgba:Array.from(canvas.getContext('2d').getImageData(2,2,1,1).data)};
                }));
            }""")
            assert all(item["rgba"] == expected for item in exported), exported
            results.append({"background": expected, "exportedAxes": exported})

        def add_track():
            open_manager()
            expect(
                dialog.get_by_role("button", name="Add file", exact=True)
            ).to_be_disabled()
            browse(0, "Select 1D track file", bed.name)
            dialog.get_by_placeholder("Optional", exact=True).fill(
                "Release annotations"
            )
            with page.expect_response("**/tracks/open") as response:
                dialog.get_by_role("button", name="Add file", exact=True).click()
            assert response.value.ok, response.value.text()
            close_manager()
            tracks = api("/tracks/list")
            assert (
                len(tracks) == 1 and tracks[0]["name"] == "Release annotations"
            ), tracks
            rendered = query()["tracks"][0]
            assert len(rendered["bins"]) == 1, rendered
            feature = rendered["bins"][0]
            assert (
                feature["startBp"],
                feature["endBp"],
                feature["value"],
                feature["label"],
                feature["strand"],
            ) == (0, 3, 1, "Release feature", "+"), feature
            assert feature["count"] == 1 and feature["attributes"]["score"] == "42", feature
            for orientation in ("horizontal", "vertical"):
                assert (
                    page.evaluate(
                        """orientation=>window.__hictBenchmark.manager().linearTrackManager
                    .getVisibleTracksForAxis(orientation).map(track=>track.name)""",
                        orientation,
                    )
                    == ["Release annotations"]
                )
            open_manager()
            expect(
                dialog.get_by_text("Release annotations", exact=True).first
            ).to_be_visible()
            results.append({"track": tracks[0], "feature": feature})

        def primary_weights():
            expect(dialog.locator("#cooler-weights-secondary")).to_be_disabled()
            dialog.locator("#cooler-weights-primary").check()
            page.wait_for_function(
                """()=>window.__hictBenchmark.manager().linearTrackManager
                .getTracksSnapshot().some(track=>track.sourceFile==='__internal__/cooler_weights/PRIMARY')"""
            )
            results.append({"primaryWeights": weight_values("PRIMARY")})
            dialog.locator("#cooler-weights-primary").uncheck()
            page.wait_for_function(
                """()=>!window.__hictBenchmark.manager().linearTrackManager
                .getTracksSnapshot().some(track=>track.sourceFile==='__internal__/cooler_weights/PRIMARY')"""
            )
            assert len(api("/tracks/list")) == 1

        def backgrounds():
            dialog.locator("#track-bg-inherit").uncheck()
            panel = dialog.locator(".alert").filter(
                has=page.get_by_text("Track panel background", exact=True)
            )
            color = panel.locator('input[type="color"]')
            expect(color).to_be_enabled()
            color.evaluate(
                """element=>{element.value='#123456';element.dispatchEvent(new Event('change',{bubbles:true}));}"""
            )
            assert_background([18, 52, 86, 255])
            close_manager()
            page.get_by_role(
                "combobox", name="Visualization preset", exact=True
            ).select_option(label="Red on black")
            check.settle()
            assert_background([18, 52, 86, 255])
            open_manager()
            expect(color).to_have_value("#123456")
            dialog.locator("#track-bg-inherit").check()
            expect(color).to_be_disabled()
            assert_background([0, 0, 0, 255])

        def secondary_weights():
            browse(1, "Select secondary data source", secondary.name)
            with page.expect_response("**/secondary/open") as response:
                dialog.get_by_role("button", name="Attach", exact=True).click()
            assert response.value.ok and not response.value.json().get(
                "requiresConfirmation"
            ), response.value.text()
            expect(dialog.locator("#cooler-weights-secondary")).to_be_enabled()
            dialog.locator("#cooler-weights-secondary").check()
            page.wait_for_function(
                """()=>window.__hictBenchmark.manager().linearTrackManager
                .getTracksSnapshot().some(track=>track.sourceFile==='__internal__/cooler_weights/SECONDARY')"""
            )
            results.append({"secondaryWeights": weight_values("SECONDARY")})
            dialog.locator("#cooler-weights-secondary").uncheck()
            page.wait_for_function(
                """()=>!window.__hictBenchmark.manager().linearTrackManager
                .getTracksSnapshot().some(track=>track.sourceFile==='__internal__/cooler_weights/SECONDARY')"""
            )
            dialog.get_by_role("button", name="Detach", exact=True).click()
            expect(dialog.locator("#cooler-weights-secondary")).to_be_disabled()
            assert not api("/secondary/status")["attached"]
            assert len(api("/tracks/list")) == 1

        def precompute():
            with page.expect_response("**/tracks/precompute/start") as response:
                dialog.get_by_role("button", name="Precompute all", exact=True).click()
            assert response.value.ok
            close_manager()
            page.get_by_role("button", name="Jobs", exact=False).click()
            jobs_dialog = page.get_by_role("dialog", name="Background jobs", exact=True)
            jobs_dialog.wait_for()
            page.wait_for_function(
                """async api=>{
                const jobs=await (await fetch(api+'/jobs')).json();
                return jobs.some(job=>job.kind==='track_precompute' && job.state==='finished');
            }""",
                arg=args.api,
            )
            result = api("/tracks/precompute/status")
            assert not result["runningJobs"] and result["tracks"], result
            assert all(
                item["status"] == "finished" for item in result["tracks"]
            ), result
            assert query()["tracks"][0]["bins"][0]["value"] == 1
            results.append({"precompute": result})

        try:
            page.goto(args.url)
            page.wait_for_function("window.__hictBenchmark")
            page.evaluate(
                "api=>window.__hictBenchmark.open('tiny-weighted.hict.hdf5',api)",
                args.api,
            )
            check.settle()
            before = api("/get_agp_for_assembly")
            check.step("named-track-browse-add-refresh-and-real-feature", add_track)
            check.step("primary-weight-track-toggle-and-values", primary_weights)
            check.step(
                "manual-inherited-track-backgrounds-and-export-pixels", backgrounds
            )
            check.step("secondary-browse-attach-weights-detach", secondary_weights)
            check.step(
                "precompute-survives-dialog-close-and-retains-values", precompute
            )
            assert api("/get_agp_for_assembly") == before
            assert not check.errors, check.errors
        finally:
            (output / "results.json").write_text(
                json.dumps(
                    {"checks": results, "cases": check.rows, "errors": check.errors},
                    indent=2,
                )
                + "\n"
            )
            browser.close()


if __name__ == "__main__":
    main()
