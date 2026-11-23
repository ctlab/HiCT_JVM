#!/usr/bin/env python3
"""Qualify viewport histogram values, levels, updates and missing O/E support."""

import argparse
import functools
import hashlib
import http.server
import io
import json
from pathlib import Path
import subprocess
import sys
import threading
from PIL import Image
from playwright.sync_api import expect, sync_playwright
from sequence_browser_harness import BrowserCheck, HictTestServer


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ("jar", "webui", "data-dir", "output"):
        parser.add_argument("--" + key, required=True)
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
    with (out / "large-fixture.log").open("w") as log:
        subprocess.run(
            [
                "java",
                "--class-path",
                str(jar),
                str(
                    Path(__file__).resolve().parents[1]
                    / "src/test/java/ru/itmo/ctlab/hict/fixtures/ReinforcementHdf5Fixture.java"
                ),
                "mixed-pyramids",
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
        def log_message(self, *args):
            pass

    web = http.server.ThreadingHTTPServer(
        ("127.0.0.1", 0),
        functools.partial(Quiet, directory=str(Path(args.webui).resolve())),
    )
    threading.Thread(target=web.serve_forever, daemon=True).start()
    rows = []
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
            request_timings = []
            tiles = {}
            cdp = page.context.new_cdp_session(page)
            cdp.send("Network.enable")

            def started(event):
                url = event["request"]["url"]
                if "/get_tile?" in url:
                    tiles[event["requestId"]] = {"url": url, "start": event["timestamp"]}

            def ended(event):
                transfer = tiles.get(event["requestId"])
                if transfer is not None:
                    transfer["end"] = event["timestamp"]
                    transfer["error"] = event.get("errorText")

            cdp.on("Network.requestWillBeSent", started)
            cdp.on("Network.loadingFinished", ended)
            cdp.on("Network.loadingFailed", ended)
            page.on(
                "requestfinished",
                lambda r: (
                    request_timings.append({"url": r.url, "timing": r.timing})
                    if any(path in r.url for path in ["/matrix/query", "/get_tile"])
                    else None
                ),
            )
            page.add_init_script(
                "window.__histLongTasks=[];new PerformanceObserver(l=>{for(const e of l.getEntries())window.__histLongTasks.push({start:e.startTime,duration:e.duration});}).observe({type:'longtask',buffered:true});"
            )
            matrix_queries = []
            page.on(
                "request",
                lambda r: (
                    matrix_queries.append(r.post_data_json)
                    if r.url.endswith("/matrix/query") and r.method == "POST"
                    else None
                ),
            )
            page.on(
                "requestfinished",
                lambda r: (
                    publications.append({"path": r.url, "body": r.post_data})
                    if any(
                        x in r.url
                        for x in ["set_visualization_options", "render_pipeline/set"]
                    )
                    else None
                ),
            )

            def record(name):
                page.screenshot(path=str(out / (name + ".png")))
                rows.append({"test": name, "status": "passed"})
                print("Passed", name, flush=True)

            def post(path, data=None):
                return json.loads(api.http(path, data or {}))

            def ready():
                hist = page.get_by_label("Viewport signal histogram", exact=True)
                expect(hist).to_have_attribute("aria-busy", "false")
                expect(hist.locator("path.histogram-curve")).to_be_visible()
                return hist

            def tile():
                response = page.request.get(
                    api.url + "/get_tile",
                    params={
                        "source": "PRIMARY",
                        "bpResolution": 4,
                        "format": "PNG_BY_PIXELS",
                        "row": 0,
                        "col": 0,
                        "rows": 5,
                        "cols": 5,
                    },
                )
                assert response.ok
                return Image.open(io.BytesIO(response.body())).convert("RGBA").tobytes()

            try:
                page.goto(f"http://127.0.0.1:{web.server_port}")
                page.evaluate(
                    'api=>window.__hictBenchmark.open("tiny-primary.hict.hdf5",api)',
                    api.url,
                )
                check.settle()
                # Await the actual normalization/refit operation before configuring this fixture.
                # Rapid edits during a legacy normalization refit are a separate release finding.
                page.evaluate(
                    """()=>{const v=window.__hictBenchmark.manager().visualizationManager,apply=v.applyCoolerWeightsPolicy.bind(v);v.applyCoolerWeightsPolicy=(...args)=>(window.__histNormalizationDone=apply(...args));}"""
                )
                page.locator("#normalization-mode").select_option("none")
                page.evaluate("async()=>await window.__histNormalizationDone")
                check.settle()
                page.locator("#signal-transform-linear").click()
                advanced = page.locator("#signal-advanced")
                if advanced.get_attribute("open") is None:
                    advanced.locator("summary").click()
                page.locator("#scaling-across-zoom").select_option("none")
                with page.expect_response(
                    lambda r: r.url.endswith("/set_visualization_options")
                    and r.request.post_data
                    and json.loads(r.request.post_data)
                    .get("colormap", {})
                    .get("maxSignal")
                    == 100
                    and r.status == 200
                ):
                    page.get_by_label("Lower signal threshold", exact=True
                    ).fill("0")
                    page.get_by_label("Upper signal threshold", exact=True
                    ).fill("100")
                    page.locator("#gradient-apply-button").click()
                check.settle()
                assert (
                    post("/get_visualization_options")["colormap"]["maxSignal"] == 100
                )
                hist = ready()
                label = hist.locator("svg").get_attribute("aria-label")
                assert (
                    "25 finite" in label
                    and "19 zeros" in label
                    and "0 missing" in label
                ), label
                graph = post("/render_pipeline/get")
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
                before = tile()
                record("full-viewport-counts-measured-zeros-and-native-base-grid-label")
                with page.expect_response(
                    lambda r: r.url.endswith("/set_visualization_options")
                    and r.request.post_data
                    and json.loads(r.request.post_data)
                    .get("colormap", {})
                    .get("maxSignal")
                    == 11
                    and r.status == 200
                ):
                    hist.get_by_role("button", name="Fit levels", exact=True).click()
                check.settle()
                ready()
                cmap = post("/get_visualization_options")["colormap"]
                assert (cmap["minSignal"], cmap["maxSignal"]) == (0, 11), cmap
                assert tile() != before
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
                record(
                    "fit-sample-percentiles-changes-colors-without-changing-contacts"
                )
                graph = post("/render_pipeline/get")
                # Repeated pans at a fixed letter zoom select different native windows.
                page.evaluate(
                    """()=>{window.__histTimings=[];const h=document.querySelector('.viewport-histogram');new MutationObserver(()=>{if(h.getAttribute('aria-busy')==='false'&&window.__histPanStart){window.__histTimings.push(performance.now()-window.__histPanStart);window.__histPanStart=0;}}).observe(h,{attributes:true,attributeFilter:['aria-busy']});}"""
                )
                small_windows = []
                for center in [2, 14, 6, 10]:
                    page.evaluate(
                        """center=>{const m=window.__hictBenchmark.manager(),l=m.viewAndLayersManager;m.getMap().once('moveend',()=>window.__histPanStart=performance.now());l.restoreViewPosition({centerBp:[center,center],resolution:0.02/l.coordinateBaseBp,rotation:0});}""",
                        center,
                    )
                    check.settle()
                    histogram = ready()
                    q = matrix_queries[-1]
                    prefix = "Bin" if q.get("unit") == "BINS" else "Px"
                    window = tuple(
                        q[k + prefix]
                        for k in ["startRow", "endRow", "startCol", "endCol"]
                    )
                    small_windows.append(window)
                    expected_count = (window[1] - window[0]) * (window[3] - window[2])
                    assert f"{expected_count} finite" in histogram.locator(
                        "svg"
                    ).get_attribute("aria-label")
                # This 17 bp map fits entirely at the allowed zoom; larger windows below prove cropping.
                timings = page.evaluate("window.__histTimings")
                assert len(timings) >= 3 and max(timings) <= 150, timings
                (out / "pan-latency.json").write_text(
                    json.dumps({"milliseconds": timings, "targetMs": 150}, indent=2)
                )
                record("pan-updates-visible-native-windows-and-clears-obsolete-data")
                page.evaluate(
                    "()=>window.__hictBenchmark.manager().viewAndLayersManager.fitViewToActiveExtent()"
                )
                check.settle()
                control = page.locator('select[title^="Expected and O/E"]')
                control.select_option("OBSERVED_OVER_EXPECTED")
                expect(control).to_be_enabled()
                check.settle()
                hist = ready()
                label = hist.locator("svg").get_attribute("aria-label")
                assert (
                    "13 finite" in label
                    and "7 zeros" in label
                    and "12 missing" in label
                ), label
                values = post(
                    "/matrix/query",
                    {
                        "bpResolution": 4,
                        "startRowPx": 0,
                        "endRowPx": 5,
                        "startColPx": 0,
                        "endColPx": 5,
                        "signalMode": "TRADITIONAL_NORMALIZED",
                        "format": "JSON",
                    },
                )["values"]
                assert sum(v is None for v in values) == 12, values
                assert sum(v == 0 for v in values) == 7, values
                assert (
                    abs(values[0] - 1 / 6) < 1e-10 and abs(values[1] - 0.6) < 1e-10
                ), values
                assert post("/render_pipeline/get") == graph
                record("oe-missing-support-is-excluded-while-measured-zero-is-counted")
                with page.expect_response(
                    lambda r: r.url.endswith("/set_visualization_options")
                    and r.request.post_data
                    and abs(
                        json.loads(r.request.post_data)
                        .get("colormap", {})
                        .get("maxSignal", 0)
                        - 11 / 6
                    )
                    < 1e-5
                    and r.status == 200
                ):
                    hist.get_by_role("button", name="Fit levels", exact=True).click()
                check.settle()
                ready()
                cmap = post("/get_visualization_options")["colormap"]
                assert (
                    abs(cmap["minSignal"] - 1 / 6) < 1e-5 and cmap["maxSignal"] > 1
                ), cmap
                record("oe-fit-keeps-neutral-one-and-valid-ratio-limits")
                for width, height in [(800, 600), (640, 800)]:
                    page.set_viewport_size({"width": width, "height": height})
                    check.settle()
                    hist = ready()
                    hist.scroll_into_view_if_needed()
                    box = hist.bounding_box()
                    assert box["x"] >= 0 and box["x"] + box["width"] <= width + 1, box
                    assert hist.get_by_role("button", name="Fit levels").is_enabled()
                    record(f"inline-histogram-fits-{width}")
                page.set_viewport_size({"width": 1440, "height": 1000})
                control.select_option("OBSERVED")
                expect(control).to_be_enabled()
                check.settle()
                page.evaluate(
                    """async()=>{const m=window.__hictBenchmark.manager(),r=m.networkManager.requestManager;
                  const status=await r.openSecondarySource('tiny-primary.hict.hdf5',true);
                  m.viewAndLayersManager.setSecondaryResolutionModel(status.compatibility);
                  const color=(source,input,max)=>({type:'colormap',mode:'LINEAR',input,startColor:'#ffffffff',endColor:'#a50026ff',minSignal:0,maxSignal:max});
                  const p=color('PRIMARY',{type:'source',source:'PRIMARY'},11);
                  const q=color('SECONDARY',{type:'binary',op:'MUL',left:{type:'source',source:'SECONDARY'},right:{type:'constant',value:3}},100);
                  await r.setRenderPipelineConfig({enabled:true,swapUpperLower:false,upperExpression:p,lowerExpression:q});
                  await m.reloadTilesFromBackend();window.dispatchEvent(new CustomEvent('hict:render-pipeline-changed'));}"""
                )
                check.settle()
                page.get_by_role(
                    "group", name="Visualization source", exact=True
                ).get_by_role("button", name="Secondary", exact=True).click()
                check.settle()
                hist = ready()
                label = hist.locator("svg").get_attribute("aria-label")
                assert "25 finite" in label and "19 zeros" in label, label
                before_primary = tile()
                before_graph = post("/render_pipeline/get")
                with page.expect_response(
                    lambda r: r.url.endswith("/render_pipeline/set") and r.status == 200
                ):
                    hist.get_by_role("button", name="Fit levels", exact=True).click()
                check.settle()
                ready()
                after_graph = post("/render_pipeline/get")
                assert after_graph["upperExpression"] == before_graph["upperExpression"]
                assert after_graph["lowerExpression"]["maxSignal"] == 33, after_graph
                assert tile() == before_primary
                record(
                    "secondary-processed-viewport-levels-preserve-primary-graph-and-pixels"
                )
                # A separate larger map forces bounded 2-D stratified sampling.
                page.evaluate(
                    'api=>window.__hictBenchmark.open("mixed-primary.hict.hdf5",api)',
                    api.url,
                )
                check.settle()
                page.evaluate(
                    """()=>{const m=window.__hictBenchmark.manager(),l=m.viewAndLayersManager;l.restoreViewPosition({centerBp:[6008,6008],resolution:4/l.coordinateBaseBp,rotation:0});}"""
                )
                check.settle()
                hist = ready()
                label = hist.locator("svg").get_attribute("aria-label")
                assert label.startswith("Sampled viewport"), label
                assert "65,536 finite" in label, label
                record("large-viewport-is-explicitly-sampled-with-bounded-cell-count")
                page.evaluate(
                    """()=>{window.__histTimings=[];const h=document.querySelector('.viewport-histogram');new MutationObserver(()=>{if(h.getAttribute('aria-busy')==='false'&&window.__histPanStart){window.__histTimings.push(performance.now()-window.__histPanStart);window.__histPanStart=0;}}).observe(h,{attributes:true,attributeFilter:['aria-busy']});}"""
                )
                sampled_windows = []
                for center in [5000, 7000, 6000]:
                    page.evaluate(
                        """center=>{const m=window.__hictBenchmark.manager(),l=m.viewAndLayersManager;m.getMap().once('moveend',()=>window.__histPanStart=performance.now());l.restoreViewPosition({centerBp:[center,center],resolution:4/l.coordinateBaseBp,rotation:0});}""",
                        center,
                    )
                    check.settle()
                    ready()
                    q = matrix_queries[-1]
                    prefix = "Bin" if q.get("unit") == "BINS" else "Px"
                    sampled_windows.append(
                        tuple(
                            q[k + prefix]
                            for k in ["startRow", "endRow", "startCol", "endCol"]
                        )
                    )
                assert len(set(sampled_windows)) >= 2, sampled_windows
                timings = page.evaluate("window.__histTimings")
                (out / "sampled-pan-latency.json").write_text(
                    json.dumps({"milliseconds": timings, "targetMs": 150}, indent=2)
                )
                assert len(timings) >= 3 and max(timings) <= 150, timings
                record("sampled-viewport-refreshes-after-repeated-pan")
                hist = ready()
                assert "65536 zeros" in hist.locator("svg").get_attribute("aria-label")
                empty_pixels = tile()
                with page.expect_response(
                    lambda r: r.url.endswith("/set_visualization_options")
                    and r.request.post_data
                    and json.loads(r.request.post_data)
                    .get("colormap", {})
                    .get("maxSignal")
                    == 1
                    and r.status == 200
                ):
                    hist.get_by_role("button", name="Fit levels", exact=True).click()
                check.settle()
                ready()
                empty_palette = post("/get_visualization_options")["colormap"]
                assert (empty_palette["minSignal"], empty_palette["maxSignal"]) == (
                    0,
                    1,
                )
                assert tile() == empty_pixels
                record(
                    "empty-viewport-fitting-keeps-zero-at-the-lower-color-and-does-not-invent-contrast"
                )
                stable_graph = post("/render_pipeline/get")
                stable_options = post("/get_visualization_options")

                def unavailable(route):
                    if route.request.method == "POST":
                        route.fulfill(
                            status=503,
                            content_type="application/json",
                            body='{"error":"Injected histogram failure"}',
                            headers={"Access-Control-Allow-Origin": "*"},
                        )
                    else:
                        route.fallback()

                page.route("**/matrix/query", unavailable)
                page.evaluate(
                    """()=>{const m=window.__hictBenchmark.manager(),l=m.viewAndLayersManager;l.restoreViewPosition({centerBp:[5500,5500],resolution:4/l.coordinateBaseBp,rotation:0});}"""
                )
                hist = page.get_by_label("Viewport signal histogram", exact=True)
                expect(
                    hist.get_by_role("button", name="Retry", exact=True)
                ).to_be_visible()
                expect(
                    hist.get_by_role("button", name="Fit levels", exact=True)
                ).to_be_disabled()
                assert hist.locator("path.histogram-curve").count() == 0
                assert post("/render_pipeline/get") == stable_graph
                assert post("/get_visualization_options") == stable_options
                page.unroute("**/matrix/query", unavailable)
                hist.get_by_role("button", name="Retry", exact=True).click()
                check.settle()
                ready()
                assert all("503" in e for e in check.errors), check.errors
                check.errors.clear()
                record(
                    "failed-histogram-clears-stale-curve-disables-fit-and-retries-without-mutation"
                )
                # Playwright callback delivery can lag behind a replacement
                # request after completion or abort. Use Chromium timestamps.
                events = []
                for transfer in tiles.values():
                    start, end = transfer["start"], transfer.get("end")
                    if end is not None:
                        assert end >= start, transfer
                        if end == start:
                            continue
                        events.append((end, -1))
                    events.append((start, 1))
                active = peak_tiles = 0
                for _, change in sorted(events):
                    active += change
                    peak_tiles = max(peak_tiles, active)
                (out / "tile-transfer-timings.json").write_text(
                    json.dumps(list(tiles.values()), indent=2)
                )
                assert 0 < peak_tiles <= 4, peak_tiles
                record("tile-network-budget-leaves-room-for-interactive-requests")
                assert hashes == {
                    p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                    for p in data.glob("*.hict.hdf5")
                }
                (out / "request-timings.json").write_text(
                    json.dumps(request_timings, indent=2)
                )
                (out / "long-tasks.json").write_text(
                    json.dumps(page.evaluate("window.__histLongTasks"), indent=2)
                )
                assert not check.errors, check.errors
                (out / "results.json").write_text(
                    json.dumps(
                        {
                            "status": "ok",
                            "checks": rows,
                            "sourceHashes": hashes,
                            "errors": check.errors,
                            "peakTileTransfers": peak_tiles,
                        },
                        indent=2,
                    )
                )
            except BaseException:
                page.screenshot(path=str(out / "failure.png"))
                (out / "publications.json").write_text(
                    json.dumps(publications, indent=2)
                )
                (out / "request-timings.json").write_text(
                    json.dumps(request_timings, indent=2)
                )
                (out / "long-tasks.json").write_text(
                    json.dumps(page.evaluate("window.__histLongTasks"), indent=2)
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
