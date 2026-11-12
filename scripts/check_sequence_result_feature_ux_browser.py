#!/usr/bin/env python3
"""Real detached sequence jobs, overlay presentation and packed BED feature actions.

Run only on an isolated server with physical copies of combined_ind2_4DN.hict.hdf5
and quad_combined_ind2.fasta in an explicitly supplied disposable /tmp data directory.
"""

import argparse
import json
import time
from pathlib import Path

from playwright.sync_api import sync_playwright
from sequence_browser_harness import BrowserCheck


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("url", "api", "output", "data-dir"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--browser-executable")
    args = parser.parse_args()
    data = Path(args.data_dir).resolve()
    if not data.is_relative_to("/tmp"):
        parser.error("Use a disposable /tmp fixture directory")
    with (data / "ux-overlapping.bed").open("x") as stream:
        for i in range(6):
            start = 1000000 + 2 * i
            stream.write(f"chrX\t{start}\t{start+24}\tUX-lane-{i}\t0\t+\n")
    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=True)
    result = {"checks": [], "visualizationRequests": []}
    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=True, executable_path=args.browser_executable
        )
        page = browser.new_page(viewport={"width": 1600, "height": 1100})
        page.set_default_timeout(30000)
        page.add_init_script("window.__HICT_BENCHMARK_ENABLED__=true")
        page.route(
            "http://localhost:5000/**",
            lambda route: route.continue_(
                url=args.api + route.request.url.removeprefix("http://localhost:5000")
            ),
        )
        check = BrowserCheck(page, out)
        page.on(
            "request",
            lambda request: (
                result["visualizationRequests"].append(
                    {"url": request.url, "body": request.post_data_json}
                )
                if request.url.endswith(
                    ("/set_visualization_options", "/render_pipeline/set")
                )
                else None
            ),
        )

        def post(path, payload=None):
            response = page.request.post(args.api + path, data=payload or {})
            assert response.ok, response.text()
            return response.json()

        def wait_job(job_id):
            deadline = time.monotonic() + 180
            while time.monotonic() < deadline:
                job = next(
                    j for j in post("/sequence/jobs/list") if j["jobId"] == job_id
                )
                if job["status"] in ("finished", "failed", "cancelled"):
                    assert job["status"] == "finished", job
                    return job
                page.wait_for_timeout(250)
            raise TimeoutError(job_id)

        def wait_async(expression, argument=None, timeout=30):
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                if page.evaluate(expression, argument):
                    return
                page.wait_for_timeout(100)
            raise TimeoutError(expression)

        def graph():
            return post("/render_pipeline/get")

        def feature_point(orientation, index):
            return page.evaluate(
                """({orientation,index}) => {
                const t=window.__hictBenchmark.manager().linearTrackManager;
                const snapshot=t.lastRenderedSnapshot[orientation];
                const hits=snapshot.featureHits.filter(h=>h.bin.label?.startsWith('UX-lane-'));
                const hit=hits[index];
                const commonStart=Math.max(...hits.map(h=>h.axisStart));
                const commonEnd=Math.min(...hits.map(h=>h.axisEnd));
                if(commonEnd<=commonStart)throw Error('Fixture features need a common painted interval');
                const axis=(commonStart+commonEnd)/2,cross=(hit.crossStart+hit.crossEnd)/2;
                const r=document.querySelector('#'+orientation+'-igv-track-div').getBoundingClientRect();
                return {x:r.x+(orientation==='horizontal'?axis:cross),
                        y:r.y+(orientation==='horizontal'?cross:axis),
                        start:hit.bin.startBp,end:hit.bin.endBp,label:hit.bin.label};
            }""",
                {"orientation": orientation, "index": index},
            )

        try:
            page.goto(args.url)
            page.wait_for_function("window.__hictBenchmark")
            page.evaluate(
                """api => window.__hictBenchmark.open('combined_ind2_4DN.hict.hdf5',api,'quad_combined_ind2.fasta')""",
                args.api,
            )
            check.settle()
            page.evaluate("""()=>{
                window.uxOptionCalls=[];
                const v=window.__hictBenchmark.manager().visualizationManager;
                const send=v.sendVisualizationOptionsToServer.bind(v);
                v.sendVisualizationOptionsToServer=(options)=>{
                    window.uxOptionCalls.push({options,stack:new Error().stack});
                    return send(options);
                };
            }""")
            nan = page.get_by_role(
                "button", name="Do not use Cooler weights", exact=True
            )
            if nan.is_visible():
                nan.click()
                check.settle()
            page.evaluate("""() => {
                const m=window.__hictBenchmark.manager(),v=m.viewAndLayersManager;
                m.getView().setResolution(v.getPixelResolutionForBpResolution(10000));
                v.currentViewState.selectionBorders.leftBP=[1000000,1000000];
                v.currentViewState.selectionBorders.rightBP=[1040000,1040000];
            }""")
            check.settle()
            check.menu("Tools", "Generate ModDotPlot…")
            dialog = page.get_by_role("dialog", name="Generate ModDotPlot", exact=True)
            selected = dialog.locator(".resolution-options input:checked").evaluate_all(
                "(items)=>items.map(i=>Number(i.value))"
            )
            assert selected == [1000, 5000, 10000], selected
            assert dialog.locator("#dotplot-display").input_value() == "below"
            dialog.get_by_label("Analysis scope", exact=True).select_option("selection")
            assert dialog.locator("#dotplot-display").input_value() == "overlay"
            dialog.get_by_label("Worker threads", exact=True).fill("2")
            with page.expect_response(
                lambda response: response.url.endswith("/sequence/moddotplot/jobs")
            ) as submitted:
                dialog.get_by_role(
                    "button", name="Generate ModDotPlot", exact=True
                ).click()
            identity_id = submitted.value.json()["jobId"]
            dialog.locator("footer").get_by_role(
                "button", name="Close", exact=True
            ).click()
            dialog.wait_for(state="hidden")
            # Start a separate search before opening either job from the Jobs list.
            check.menu("Tools", "Find sequences in linked FASTA…")
            search = page.get_by_role(
                "dialog", name="Find sequences in the linked FASTA", exact=True
            )
            search.get_by_label("FASTA records, or one DNA sequence per line").fill(
                "GAGAGAGAGAGA"
            )
            search.get_by_label("Search scope", exact=True).select_option("selection")
            with page.expect_response(
                lambda response: response.url.endswith("/sequence/search/jobs")
            ) as submitted:
                search.get_by_role("button", name="Find sequences", exact=True).click()
            search_id = submitted.value.json()["jobId"]
            search.locator("footer").get_by_role(
                "button", name="Close", exact=True
            ).click()
            search.wait_for(state="hidden")
            identity = wait_job(identity_id)
            search_job = wait_job(search_id)
            wait_async(
                """async output => {
                const m=window.__hictBenchmark.manager();
                const s=await m.networkManager.requestManager.getSecondarySourceStatus();
                return s.attached && s.filename===output;
            }""",
                identity["outputFile"],
            )
            check.settle()
            wait_async(
                """async()=>{
                const g=await window.__hictBenchmark.manager().networkManager.requestManager.getRenderPipelineConfig();
                return g.upperExpression?.type==='pixel_blend';
            }""",
            )
            assert graph()["upperExpression"]["type"] == "pixel_blend", graph()
            background = page.request.get(args.api + "/jobs").json()
            own = next(j for j in background if j["id"] == "sequence/" + identity_id)
            assert own["outputs"][0]["displayMode"] == "overlay", own
            if search_job["hits"]:
                page.wait_for_function(
                    """file=>window.__hictBenchmark.manager().linearTrackManager.getTracksSnapshot().some(t=>t.sourceFile===file)""",
                    arg=search_job["outputFile"],
                )
            result["checks"].append(
                {
                    "closedDialogsDeliverWithoutOpeningJobs": True,
                    "identityJob": identity_id,
                    "searchJob": search_id,
                    "searchHits": search_job["hits"],
                    "resolutions": selected,
                }
            )
            layout = page.get_by_label("Sequence overlay view", exact=True)
            original = graph()
            primary = original["upperExpression"]["bottom"]
            secondary = original["upperExpression"]["top"]
            for mode in ("above", "below", "overlay"):
                layout.select_option(mode)
                page.wait_for_function(
                    """mode=>{
                    const c=document.querySelector('[aria-label="Sequence overlay view"]');
                    return c&&!c.disabled&&c.value===mode;
                }""",
                    arg=mode,
                )
                check.settle()
                current = graph()
                if mode == "above":
                    assert (
                        current["upperExpression"] == secondary
                        and current["lowerExpression"] == primary
                    )
                elif mode == "below":
                    assert (
                        current["upperExpression"] == primary
                        and current["lowerExpression"] == secondary
                    )
                else:
                    assert current["upperExpression"]["top"] == secondary
                    assert current["upperExpression"]["bottom"] == primary
            result["checks"].append(
                {"allPresentationModesPreserveSourceBranches": True}
            )
            # Manual Jobs opening must use the submitted preference too.
            page.get_by_role("button", name="Jobs", exact=False).click()
            row = page.locator(".job-row").filter(has_text=identity["outputFile"])
            row.get_by_role("button", name="Open result", exact=True).click()
            check.settle()
            assert graph()["upperExpression"]["type"] == "pixel_blend"
            result["checks"].append({"manualJobOpeningRetainsOverlayPreference": True})
            # Exercise the same default-resolution behavior in alignment dotplot generation.
            check.menu("Tools", "Generate dotplot from active assembly…")
            alignment = page.get_by_role("dialog").filter(
                has=page.locator("#dotplot-display")
            )
            expected = page.evaluate("""()=>{
                const v=window.__hictBenchmark.manager().viewAndLayersManager;
                return [...v.resolutions].sort((a,b)=>a-b).filter(r=>r<=v.currentViewState.resolutionDesciptor.bpResolution);
            }""")
            actual = alignment.locator(
                ".resolution-options input:checked"
            ).evaluate_all("(items)=>items.map(i=>Number(i.value))")
            assert actual == expected, (actual, expected)
            alignment.locator("footer").get_by_role(
                "button", name="Close", exact=True
            ).click()
            result["checks"].append({"alignmentDefaultResolutions": actual})
            check.menu("Tools", "Generate dotplot from FASTA files…")
            legacy = page.get_by_role("dialog").filter(
                has=page.get_by_role(
                    "heading", name="Generate self-alignment dotplots", exact=True
                )
            )
            legacy.get_by_text("quad_combined_ind2.fasta", exact=True).first.wait_for(
                state="visible"
            )
            legacy.get_by_role("button", name="Select All", exact=True).click()
            legacy.get_by_role("button", name="Continue", exact=True).click()
            assert (
                float(
                    legacy.get_by_label(
                        "Base dotplot resolution, bp/bin", exact=True
                    ).input_value()
                )
                == expected[0]
            )
            legacy_levels = [
                int(value.strip())
                for value in legacy.get_by_label("Zoom resolutions", exact=True)
                .input_value()
                .split(",")
            ]
            assert legacy_levels == expected, (legacy_levels, expected)
            legacy.locator(".modal-header .btn-close").click()
            legacy.wait_for(state="hidden")
            result["checks"].append(
                {"FASTAFileDotplotDefaultResolutions": legacy_levels}
            )

            page.evaluate("""async()=>{
                const m=window.__hictBenchmark.manager(),t=m.linearTrackManager;
                for(const track of [...t.getTracksSnapshot()])await t.updateTrack(track.trackId,{visible:false});
                await t.openTrack('ux-overlapping.bed','UX overlapping features',false);
                const entries=await t.searchFeatureSuggestionsRemote('UX-lane-',20);
                if(entries.length!==6)throw Error('Expected six overlapping fixture features');
                window.uxFeature=entries[3];
                t.zoomToFeature(entries[3]);
            }""")
            check.settle()
            for orientation in ("horizontal", "vertical"):
                page.wait_for_function(
                    """o=>window.__hictBenchmark.manager().linearTrackManager.lastRenderedSnapshot[o]?.featureHits.filter(h=>h.bin.label?.startsWith('UX-lane-')).length===6""",
                    arg=orientation,
                )
                for index in range(6):
                    point = feature_point(orientation, index)
                    page.mouse.click(point["x"], point["y"])
                    page.wait_for_function(
                        """p=>{
                        const s=window.__hictBenchmark.manager().linearTrackManager.selectedFeature;
                        return s&&s.startBp===p.start&&s.endBp===p.end&&s.label===p.label;
                    }""",
                        arg=point,
                    )
                result["checks"].append(
                    {"axis": orientation, "allSixPackedLanesSelectExactFeature": True}
                )
            page.evaluate(
                """()=>{const m=window.__hictBenchmark.manager();m.getView().setResolution(m.getView().getResolution()*20);}"""
            )
            check.settle()
            point = feature_point("horizontal", 3)
            before_resolution = page.evaluate(
                "window.__hictBenchmark.manager().getView().getResolution()"
            )
            page.mouse.click(point["x"], point["y"], button="right")
            page.get_by_role("menuitem", name="Zoom to feature", exact=True).click()
            check.settle()
            state = page.evaluate(
                """p=>{
                const m=window.__hictBenchmark.manager(),v=m.viewAndLayersManager;
                const finest=m.linearTrackManager.getFinestBpResolution();
                const pixel=v.getPixelResolutionForBpResolution(finest);
                const expected=m.getContigDimensionHolder().getFractionalPxOfBp((p.start+p.end)/2,finest)*pixel;
                return {center:m.getView().getCenter(),expected,resolution:m.getView().getResolution(),
                    pixelsPerBase:pixel/finest/m.getView().getResolution()};
            }""",
                point,
            )
            assert state["resolution"] < before_resolution / 10, state
            assert abs(state["center"][0] - state["expected"]) < 1e-7, state
            assert abs(state["center"][1] + state["expected"]) < 1e-7, state
            assert state["pixelsPerBase"] > 12, state
            assert page.locator("canvas.sequence-axis.horizontal").is_visible()
            result["checks"].append({"zoomToFeatureAtReadableBaseScale": state})
            assert not check.errors, check.errors
            result["status"] = "passed"
            page.screenshot(path=str(out / "feature-zoom.png"))
        except BaseException:
            result["browserErrors"] = check.errors
            result["optionCallStacks"] = page.evaluate("window.uxOptionCalls")
            page.screenshot(path=str(out / "failure.png"))
            raise
        finally:
            (out / "results.json").write_text(json.dumps(result, indent=2) + "\n")
            browser.close()


if __name__ == "__main__":
    main()
