#!/usr/bin/env python3
"""Exercise user edits while normalization and visualization replies are delayed."""

import argparse
import functools
import hashlib
import http.server
import io
import json
from pathlib import Path
import subprocess
import sys
import math
import threading
from PIL import Image
from playwright.sync_api import expect, sync_playwright
from sequence_browser_harness import BrowserCheck, HictTestServer


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ("jar", "webui", "data-dir", "output"):
        parser.add_argument("--" + key, required=True)
    parser.add_argument("--hdf-python", default=sys.executable)
    args = parser.parse_args()
    data, out = Path(args.data_dir).absolute(), Path(args.output).absolute()
    if any(p != p.resolve() or not p.is_relative_to("/tmp") for p in (data, out)):
        parser.error("Use fresh nonsymlinked /tmp paths")
    data.mkdir(exist_ok=False)
    out.mkdir(exist_ok=False)
    jar = Path(args.jar).resolve()
    with (out / "fixture.log").open("w") as log:
        subprocess.run(
            [
                "java",
                "--class-path",
                str(jar),
                str(
                    Path(__file__).resolve().parents[1]
                    / "src/test/java/ru/itmo/ctlab/hict/fixtures/ReinforcementHdf5Fixture.java"
                ),
                str(data),
            ],
            check=True,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
    with (out / "sources.log").open("w") as log:
        subprocess.run(
            [
                args.hdf_python,
                str(
                    Path(__file__).resolve().parent / "prepare_reinforcement_sources.py"
                ),
                "--data-dir",
                str(data),
            ],
            check=True,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
    hashes = {
        p.name: hashlib.sha256(p.read_bytes()).hexdigest()
        for p in data.glob("*.hict.hdf5")
    }

    class Quiet(http.server.SimpleHTTPRequestHandler):
        def log_message(self, *_):
            pass

    web = http.server.ThreadingHTTPServer(
        ("127.0.0.1", 0),
        functools.partial(Quiet, directory=str(Path(args.webui).resolve())),
    )
    threading.Thread(target=web.serve_forever, daemon=True).start()
    results = []
    try:
        with HictTestServer(
            jar, data, out / "server.log"
        ) as api, sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            page = browser.new_page(viewport={"width": 1440, "height": 1000})
            page.set_default_timeout(30000)
            page.add_init_script("window.__HICT_BENCHMARK_ENABLED__=true")
            page.route(
                "http://localhost:5000/**",
                lambda r: r.continue_(
                    url=api.url + r.request.url.removeprefix("http://localhost:5000")
                ),
            )
            check = BrowserCheck(page, out)
            publications = []
            page.on(
                "requestfinished",
                lambda r: (
                    publications.append({"url": r.url, "body": r.post_data})
                    if r.url.endswith("/set_visualization_options")
                    or r.url.endswith("/render_pipeline/set")
                    else None
                ),
            )

            def post(path, payload=None):
                return json.loads(api.http(path, payload or {}))

            def record(name):
                page.screenshot(path=str(out / (name + ".png")))
                results.append({"test": name, "status": "passed"})
                print("Passed", name, flush=True)

            def options():
                return post("/get_visualization_options")

            def tile(source="PRIMARY"):
                response = page.request.get(
                    api.url + "/get_tile",
                    params={
                        "source": source,
                        "bpResolution": 4,
                        "format": "PNG_BY_PIXELS",
                        "row": 0,
                        "col": 0,
                        "rows": 5,
                        "cols": 5,
                    },
                )
                assert response.ok
                return Image.open(io.BytesIO(response.body())).convert("RGBA")

            def wait():
                page.evaluate(
                    "async()=>await window.__hictBenchmark.manager().visualizationManager.waitForPendingOptionRequests()"
                )
                check.settle()

            def wait_held(held):
                for _ in range(3000):
                    if held:
                        return
                    page.wait_for_timeout(10)
                raise AssertionError("Expected visualization request did not start")

            def set_upper(value, apply=True):
                page.get_by_role(
                    "spinbutton", name="Upper signal threshold", exact=True
                ).fill(str(value))
                if apply:
                    page.locator("#gradient-apply-button").click()

            def choose_source(source):
                page.get_by_role(
                    "group", name="Visualization source", exact=True
                ).get_by_role("button", name=source, exact=True).click()
                check.settle()

            def hold(path, predicate=lambda body: True):
                held = []

                def callback(route):
                    if (
                        route.request.method == "POST"
                        and not held
                        and predicate(route.request.post_data_json)
                    ):
                        held.append(route)
                    else:
                        route.fallback()

                page.route("**" + path, callback)
                return held, callback

            def release(path, held, callback):
                held[0].fallback()
                page.unroute("**" + path, callback)

            try:
                page.goto(f"http://127.0.0.1:{web.server_port}")
                page.evaluate(
                    'api=>window.__hictBenchmark.open("tiny-primary.hict.hdf5",api)',
                    api.url,
                )
                check.settle()
                raw = post(
                    "/matrix/query",
                    {
                        "bpResolution": 4,
                        "startRowPx": 0,
                        "endRowPx": 5,
                        "startColPx": 0,
                        "endColPx": 5,
                        "signalMode": "RAW_COUNTS",
                        "format": "JSON",
                    },
                )
                # Observe the real normalization operation, without changing it.
                # Opening can finish with an unweighted default preset. Establish
                # the opposite policy so selecting None always performs a change.
                page.evaluate(
                    "async()=>{const v=window.__hictBenchmark.manager().visualizationManager;await v.waitForPendingOptionRequests();await v.applyCoolerWeightsPolicy('REPLACE_NANS_WITH_ONE',true);await v.waitForPendingOptionRequests();}"
                )
                expect(page.locator("#normalization-mode")).to_have_value("file")
                page.evaluate(
                    """()=>{const v=window.__hictBenchmark.manager().visualizationManager,f=v.applyCoolerWeightsPolicy.bind(v);v.applyCoolerWeightsPolicy=(...args)=>(window.__normalizationOperation=f(...args));}"""
                )
                held = []

                def delay_policy(route):
                    if (
                        route.request.method == "POST"
                        and not held
                        and route.request.post_data_json.get("applyCoolerWeights")
                        is False
                    ):
                        held.append(route)
                    else:
                        route.fallback()

                page.route("**/set_visualization_options", delay_policy)
                page.locator("#normalization-mode").select_option("none")
                page.wait_for_function("window.__normalizationOperation!==undefined")
                while not held:
                    page.wait_for_timeout(10)
                page.locator("#signal-transform-log").click()
                page.get_by_role(
                    "spinbutton", name="Upper signal threshold", exact=True
                ).fill("100")
                page.locator("#gradient-apply-button").click()
                # Give the legacy overlapping Apply time to start; the fixed queue may keep it waiting.
                page.wait_for_timeout(100)
                held[0].fallback()
                page.unroute("**/set_visualization_options", delay_policy)
                page.evaluate(
                    "async()=>{await window.__normalizationOperation;await window.__hictBenchmark.manager().visualizationManager.waitForPendingOptionRequests();}"
                )
                check.settle()
                actual = options()
                assert (
                    actual["postLogBase"] == 2
                    and actual["colormap"]["maxSignal"] == 100
                    and not actual["applyCoolerWeights"]
                ), actual
                expect(
                    page.get_by_role(
                        "spinbutton", name="Upper signal threshold", exact=True
                    )
                ).to_have_value("100")
                expect(page.locator("#signal-transform-log")).to_have_attribute(
                    "aria-pressed", "true"
                )
                assert (
                    post(
                        "/matrix/query",
                        {
                            "bpResolution": 4,
                            "startRowPx": 0,
                            "endRowPx": 5,
                            "startColPx": 0,
                            "endColPx": 5,
                            "signalMode": "RAW_COUNTS",
                            "format": "JSON",
                        },
                    )
                    == raw
                )
                pixel = tile().getpixel((0, 0))
                assert pixel[1] in (252, 253) and pixel[2] in (252, 253), pixel
                record(
                    "delayed-normalization-keeps-newer-log-transform-and-threshold-in-options-ui-and-pixels"
                )

                # A processing-only edit defers the normalization refit to the final signal.
                page.locator("#signal-transform-linear").click()
                wait()
                page.evaluate(
                    "async()=>await window.__hictBenchmark.manager().visualizationManager.applyCoolerWeightsPolicy('REPLACE_NANS_WITH_ONE',true)"
                )
                wait()
                held, callback = hold(
                    "/set_visualization_options",
                    lambda body: body.get("applyCoolerWeights") is False,
                )
                page.locator("#normalization-mode").select_option("none")
                wait_held(held)
                page.locator("#signal-transform-log").click()
                page.wait_for_timeout(400)
                release("/set_visualization_options", held, callback)
                wait()
                actual = options()
                assert (
                    actual["postLogBase"] == 2
                    and abs(actual["colormap"]["maxSignal"] - math.log2(12)) < 1e-5
                ), actual
                record(
                    "processing-only-draft-applies-without-apply-and-fits-the-final-normalized-signal"
                )

                # Real weighted Secondary input has weights 2..6; both branches are canonical.
                page.evaluate(
                    """async()=>{const m=window.__hictBenchmark.manager(),r=m.networkManager.requestManager;
                  const status=await r.openSecondarySource('tiny-weighted.hict.hdf5',true);
                  m.viewAndLayersManager.setSecondaryResolutionModel(status.compatibility);
                  const weights={type:'binary',op:'MUL',left:{type:'track1d',trackId:'__builtin_cooler_weights__',axis:'ROW'},right:{type:'track1d',trackId:'__builtin_cooler_weights__',axis:'COL'}};
                  const color=(input,max)=>({type:'colormap',mode:'LINEAR',input,startColor:'#ffffffff',endColor:'#e80000ff',minSignal:0,maxSignal:max});
                  await r.setRenderPipelineConfig({enabled:true,swapUpperLower:false,upperExpression:color({type:'source',source:'PRIMARY'},11),lowerExpression:color({type:'binary',op:'MUL',left:{type:'source',source:'SECONDARY'},right:weights},1000)});
                  await m.reloadTilesFromBackend();window.dispatchEvent(new CustomEvent('hict:render-pipeline-changed'));}"""
                )
                check.settle()
                wait()
                original = post("/render_pipeline/get")
                original_secondary_input = original["lowerExpression"]["input"]
                held, callback = hold(
                    "/set_visualization_options",
                    lambda body: body.get("colormap", {}).get("maxSignal") == 150,
                )
                set_upper(150)
                wait_held(held)
                choose_source("Secondary")
                set_upper(750)
                release("/set_visualization_options", held, callback)
                wait()
                actual_graph = post("/render_pipeline/get")
                assert actual_graph["upperExpression"]["maxSignal"] == 150, actual_graph
                assert actual_graph["lowerExpression"]["maxSignal"] == 750, actual_graph
                assert (
                    actual_graph["lowerExpression"]["input"] == original_secondary_input
                )
                expect(
                    page.get_by_role(
                        "spinbutton", name="Upper signal threshold", exact=True
                    )
                ).to_have_value("750")
                assert tile("SECONDARY").getpixel((4, 4))[1] < 130, tile(
                    "SECONDARY"
                ).getpixel((4, 4))
                record(
                    "late-primary-acknowledgement-preserves-secondary-draft-input-and-weighted-pixels"
                )

                # Changing Secondary normalization must not redirect a later Primary edit.
                before_secondary = tile("SECONDARY").tobytes()
                held, callback = hold("/render_pipeline/set")
                page.locator("#normalization-mode").select_option("none")
                wait_held(held)
                choose_source("Primary")
                page.locator("#signal-transform-log").click()
                set_upper(175)
                release("/render_pipeline/set", held, callback)
                wait()
                graph = post("/render_pipeline/get")
                assert graph["upperExpression"]["maxSignal"] == 175, graph
                assert graph["upperExpression"]["input"]["type"] == "log", graph
                assert graph["lowerExpression"]["input"] == {
                    "type": "source",
                    "source": "SECONDARY",
                }, graph
                assert graph["lowerExpression"]["maxSignal"] == 11, graph
                assert (
                    options()["postLogBase"] == 2
                    and options()["colormap"]["maxSignal"] == 175
                )
                assert tile("SECONDARY").tobytes() != before_secondary
                expect(
                    page.get_by_role(
                        "spinbutton", name="Upper signal threshold", exact=True
                    )
                ).to_have_value("175")
                record(
                    "secondary-normalization-and-refit-keep-the-new-primary-draft-and-isolate-source-pixels"
                )

                # Superseded queued ranges must not issue another graph/options write.
                index = len(publications)
                held, callback = hold(
                    "/set_visualization_options",
                    lambda body: body.get("colormap", {}).get("maxSignal") == 200,
                )
                set_upper(200)
                wait_held(held)
                set_upper(220)
                set_upper(240)
                release("/set_visualization_options", held, callback)
                wait()
                writes = [
                    json.loads(r["body"])["colormap"]["maxSignal"]
                    for r in publications[index:]
                    if r["url"].endswith("/set_visualization_options")
                ]
                assert writes == [200, 240], writes
                assert options()["colormap"]["maxSignal"] == 240
                record("superseded-queued-thresholds-coalesce-without-late-overwrite")

                # Timers retain their original source even after the sidebar is switched.
                set_upper(260, False)
                choose_source("Secondary")
                set_upper(18, False)
                wait()
                graph = post("/render_pipeline/get")
                assert (
                    graph["upperExpression"]["maxSignal"] == 260
                    and graph["lowerExpression"]["maxSignal"] == 18
                ), graph
                expect(
                    page.get_by_role(
                        "spinbutton", name="Upper signal threshold", exact=True
                    )
                ).to_have_value("18")
                record(
                    "debounced-drafts-stay-with-their-source-through-sidebar-switches"
                )
                choose_source("Primary")

                # A stale configuration read cannot rehydrate an old range after a newer commit.
                for kind, path in [
                    ("source", "/render_pipeline/get"),
                    ("primary", "/get_visualization_options"),
                ]:
                    reads = []

                    def stale_read(route):
                        if route.request.method == "POST" and not reads:
                            response = route.fetch()
                            reads.append((route, response))
                        else:
                            route.fallback()

                    page.route("**" + path, stale_read)
                    page.evaluate(
                        """kind=>{const v=window.__hictBenchmark.manager().visualizationManager;window.__staleRead=kind==='source'?v.loadVisualizationOptionsForSource('PRIMARY',null):v.fetchVisualizationOptions();}""",
                        kind,
                    )
                    wait_held(reads)
                    value = 270 if kind == "source" else 280
                    with page.expect_response(
                        lambda r: r.url.endswith("/set_visualization_options")
                        and r.request.post_data_json.get("colormap", {}).get(
                            "maxSignal"
                        )
                        == value
                        and r.status == 200
                    ):
                        set_upper(value)
                    reads[0][0].fulfill(response=reads[0][1])
                    page.unroute("**" + path, stale_read)
                    page.evaluate("async()=>await window.__staleRead")
                    wait()
                    assert options()["colormap"]["maxSignal"] == value
                    expect(
                        page.get_by_role(
                            "spinbutton", name="Upper signal threshold", exact=True
                        )
                    ).to_have_value(str(value))
                    record(
                        "late-" + kind + "-read-cannot-replace-a-newer-committed-range"
                    )

                # A failed graph read must not be mistaken for a disabled graph.
                before_graph = post("/render_pipeline/get")
                before_options = options()
                before_pixels = tile().tobytes()
                failed = [False]

                def fail_read(route):
                    if route.request.method == "POST" and not failed[0]:
                        failed[0] = True
                        route.fulfill(
                            status=503,
                            content_type="application/json",
                            body='{"error":"Injected graph read failure"}',
                            headers={"Access-Control-Allow-Origin": "*"},
                        )
                    else:
                        route.fallback()

                page.route("**/render_pipeline/get", fail_read)
                set_upper(20)
                wait()
                page.unroute("**/render_pipeline/get", fail_read)
                assert failed[0]
                assert (
                    post("/render_pipeline/get") == before_graph
                    and options() == before_options
                    and tile().tobytes() == before_pixels
                )
                expect(
                    page.get_by_role(
                        "spinbutton", name="Upper signal threshold", exact=True
                    )
                ).to_have_value("20")
                expect(
                    page.get_by_text("Changes pending. Apply to retry.", exact=True)
                ).to_be_visible()
                assert all("503" in e for e in check.errors), check.errors
                check.errors.clear()
                page.locator("#gradient-apply-button").click()
                wait()
                assert (
                    options()["colormap"]["maxSignal"] == 20
                    and tile().tobytes() != before_pixels
                )
                record(
                    "failed-configuration-read-preserves-graph-options-pixels-and-draft-for-retry"
                )

                before_graph = post("/render_pipeline/get")
                before_options = options()
                before_pixels = tile().tobytes()
                failed = [False]

                def fail_write(route):
                    if route.request.method == "POST" and not failed[0]:
                        failed[0] = True
                        route.fulfill(
                            status=503,
                            content_type="application/json",
                            body='{"error":"Injected palette write failure"}',
                            headers={"Access-Control-Allow-Origin": "*"},
                        )
                    else:
                        route.fallback()

                page.route("**/set_visualization_options", fail_write)
                set_upper(30)
                wait()
                page.unroute("**/set_visualization_options", fail_write)
                assert failed[0]
                assert (
                    post("/render_pipeline/get") == before_graph
                    and options() == before_options
                    and tile().tobytes() == before_pixels
                )
                expect(
                    page.get_by_role(
                        "spinbutton", name="Upper signal threshold", exact=True
                    )
                ).to_have_value("30")
                expect(
                    page.get_by_text("Changes pending. Apply to retry.", exact=True)
                ).to_be_visible()
                assert all("503" in e for e in check.errors), check.errors
                check.errors.clear()
                page.locator("#gradient-apply-button").click()
                wait()
                assert (
                    options()["colormap"]["maxSignal"] == 30
                    and tile().tobytes() != before_pixels
                )
                record(
                    "rejected-atomic-write-keeps-the-draft-and-recovers-on-explicit-retry"
                )

                # Save flushes the pending debounce before capturing options and the graph.
                set_upper(45, False)
                with page.expect_download() as download:
                    check.menu("File", "Save session…")
                saved = out / "pending-draft-session.json"
                download.value.save_as(saved)
                session = json.loads(saved.read_text())
                assert (
                    session["snapshot"]["visualizationOptions"]["colormap"]["maxSignal"]
                    == 45
                )
                assert (
                    session["snapshot"]["renderPipeline"]["upperExpression"][
                        "maxSignal"
                    ]
                    == 45
                )
                assert options()["colormap"]["maxSignal"] == 45
                record(
                    "session-save-flushes-debounced-drafts-and-captures-the-committed-pipeline"
                )

                # A mode change follows the pending Observed edit and retains its palette on return.
                secondary_before = post("/render_pipeline/get")["lowerExpression"]
                set_upper(55, False)
                mode = page.locator('select[title^="Expected and O/E"]')
                mode.select_option("OBSERVED_OVER_EXPECTED")
                expect(mode).to_be_enabled()
                wait()
                assert options()["signalDisplayMode"] == "OBSERVED_OVER_EXPECTED"
                assert (
                    options()["colormap"]["minSignal"] > 0
                    and options()["colormap"]["maxSignal"] > 1
                )
                mode.select_option("OBSERVED")
                expect(mode).to_be_enabled()
                wait()
                assert (
                    options()["colormap"]["maxSignal"] == 55
                    and options()["postLogBase"] == 2
                )
                assert (
                    post("/render_pipeline/get")["lowerExpression"] == secondary_before
                )
                expect(
                    page.get_by_role(
                        "spinbutton", name="Upper signal threshold", exact=True
                    )
                ).to_have_value("55")
                record(
                    "display-mode-changes-follow-pending-edits-and-retain-observed-and-secondary-state"
                )

                # A source preset is a new complete intent, not an older pending range.
                layout = page.get_by_role(
                    "combobox", name="Sequence overlay view", exact=True
                )
                layout.select_option("below")
                expect(layout).to_be_enabled()
                check.settle()
                wait()
                choose_source("Secondary")
                primary_before = post("/render_pipeline/get")["upperExpression"]
                set_upper(80, False)
                page.get_by_role(
                    "combobox", name="Visualization preset", exact=True
                ).select_option(label="Cold-Hot")
                wait()
                graph = post("/render_pipeline/get")
                assert graph["upperExpression"] == primary_before, graph
                assert graph["lowerExpression"]["maxSignal"] == 0.75, graph
                expect(
                    page.get_by_role(
                        "spinbutton", name="Upper signal threshold", exact=True
                    )
                ).to_have_value("0.75")
                record(
                    "source-preset-supersedes-an-older-range-without-changing-the-other-source"
                )

                # Import a portable whole-graph preset through the actual preset chooser.
                preset_options = json.loads(
                    json.dumps(
                        session["client"]["savedVisualizationPresets"][0]["options"]
                    )
                )
                preset_options.update(
                    {
                        "postLogBase": -1,
                        "preLogBase": -1,
                        "applyCoolerWeights": False,
                        "resolutionScaling": False,
                        "resolutionLinearScaling": False,
                        "autoThresholdEnabled": False,
                        "signalDisplayMode": "OBSERVED",
                    }
                )
                preset_options["colormap"].update({"minSignal": 0, "maxSignal": 70})

                def color(source, maximum):
                    return {
                        "type": "colormap",
                        "mode": "LINEAR",
                        "input": {"type": "source", "source": source},
                        "startColor": "#ffffffff",
                        "endColor": "#e80000ff",
                        "minSignal": 0,
                        "maxSignal": maximum,
                    }

                preset_graph = {
                    "enabled": True,
                    "swapUpperLower": False,
                    "upperExpression": color("PRIMARY", 70),
                    "lowerExpression": color("SECONDARY", 700),
                }
                imported = {
                    "exportType": "visualizationOptions",
                    "data": {
                        "savedVisualizationPresets": [
                            {
                                "name": "Transaction graph",
                                "options": preset_options,
                                "backgroundColor": "#ffffffff",
                                "renderPipeline": preset_graph,
                            }
                        ]
                    },
                }
                page.get_by_role("button", name="Manage…", exact=True).click()
                group = page.get_by_role(
                    "group", name="Visualization presets", exact=True
                )
                before_import = post("/render_pipeline/get")
                group.locator("input[type=file]").set_input_files(
                    {
                        "name": "transaction-preset.json",
                        "mimeType": "application/json",
                        "buffer": json.dumps(imported).encode(),
                    }
                )
                expect(
                    page.get_by_role("option", name="Transaction graph", exact=True)
                ).to_have_count(1)
                assert post("/render_pipeline/get") == before_import
                choose_source("Primary")
                set_upper(95, False)
                page.get_by_role(
                    "combobox", name="Visualization preset", exact=True
                ).select_option(label="Transaction graph")
                wait()
                assert post("/render_pipeline/get") == preset_graph
                assert (
                    options()["colormap"]["maxSignal"] == 70
                    and options()["postLogBase"] == -1
                )
                record(
                    "whole-graph-preset-follows-older-pending-drafts-and-applies-the-complete-snapshot"
                )

                held, callback = hold(
                    "/set_visualization_options",
                    lambda body: body.get("renderPipeline", {})
                    .get("upperExpression", {})
                    .get("maxSignal")
                    == 70,
                )
                page.get_by_role(
                    "combobox", name="Visualization preset", exact=True
                ).select_option(label="Transaction graph")
                wait_held(held)
                set_upper(85, False)
                choose_source("Secondary")
                set_upper(900, False)
                release("/set_visualization_options", held, callback)
                wait()
                graph = post("/render_pipeline/get")
                assert (
                    graph["upperExpression"]["maxSignal"] == 85
                    and graph["lowerExpression"]["maxSignal"] == 900
                ), graph
                expect(
                    page.get_by_role(
                        "spinbutton", name="Upper signal threshold", exact=True
                    )
                ).to_have_value("900")
                record(
                    "newer-edits-to-both-sources-outlive-a-delayed-whole-graph-preset"
                )

                # Opening an editor waits for existing commits and prevents editing its loading canvas.
                choose_source("Primary")
                held, callback = hold(
                    "/set_visualization_options",
                    lambda body: body.get("colormap", {}).get("maxSignal") == 60,
                )
                set_upper(60)
                wait_held(held)
                check.menu("View", "Rendering pipeline…")
                dialog = page.get_by_role(
                    "dialog", name="Rendering Pipeline", exact=True
                )
                expect(
                    dialog.get_by_role("button", name="Apply", exact=True)
                ).to_be_disabled()
                assert dialog.locator(".graph-host").evaluate("node=>node.inert")
                release("/set_visualization_options", held, callback)
                wait()
                expect(
                    dialog.get_by_role("button", name="Apply", exact=True)
                ).to_be_enabled()
                assert not dialog.locator(".graph-host").evaluate("node=>node.inert")
                with page.expect_download() as download:
                    dialog.get_by_role(
                        "button", name="Export graph", exact=True
                    ).click()
                exported = out / "editor-after-pending-write.json"
                download.value.save_as(exported)
                expr = json.loads(exported.read_text())["upperExpression"]
                assert (
                    expr.get("maxSignal", expr.get("max", {}).get("value")) == 60
                ), expr
                before_close = post("/render_pipeline/get")
                dialog.get_by_role("button", name="Close", exact=True).click()
                check.settle()
                wait()
                assert post("/render_pipeline/get") == before_close
                record(
                    "pipeline-editor-waits-for-pending-commits-and-opens-with-the-latest-unchanged-graph"
                )
                assert hashes == {
                    p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                    for p in data.glob("*.hict.hdf5")
                }
                assert not check.errors, check.errors
                (out / "results.json").write_text(
                    json.dumps(
                        {
                            "status": "ok",
                            "checks": results,
                            "sourceHashes": hashes,
                            "errors": check.errors,
                        },
                        indent=2,
                    )
                )
            except BaseException:
                page.screenshot(path=str(out / "failure.png"))
                (out / "publications.json").write_text(
                    json.dumps(publications, indent=2)
                )
                (out / "errors.json").write_text(json.dumps(check.errors, indent=2))
                raise
            finally:
                browser.close()
    finally:
        web.shutdown()
        web.server_close()


if __name__ == "__main__":
    main()
