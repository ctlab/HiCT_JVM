#!/usr/bin/env python3
"""Check pinned Expected/O/E captures through the real figure controls on disposable data."""

import argparse
import functools
import hashlib
import http.server
import json
import re
from pathlib import Path
import subprocess
import threading
import xml.etree.ElementTree as ET
from urllib.parse import parse_qs, urlparse
from playwright.sync_api import expect, sync_playwright
from sequence_browser_harness import BrowserCheck, HictTestServer


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ("jar", "webui", "output"):
        parser.add_argument("--" + key, required=True)
    args = parser.parse_args()
    out = Path(args.output).absolute()
    assert out == out.resolve() and out.is_relative_to("/tmp")
    out.mkdir(exist_ok=False)
    data = out / "data"
    data.mkdir()
    jar = Path(args.jar).resolve()
    with (out / "fixture.log").open("w") as log:
        subprocess.run(
            [
                "java",
                "--class-path",
                str(jar),
                "src/test/java/ru/itmo/ctlab/hict/fixtures/ReinforcementHdf5Fixture.java",
                "mixed-pyramids",
                str(data),
            ],
            check=True,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
    original = hashlib.sha256(
        (data / "mixed-primary.hict.hdf5").read_bytes()
    ).hexdigest()

    class Quiet(http.server.SimpleHTTPRequestHandler):
        def log_message(self, *_):
            pass

    web = http.server.ThreadingHTTPServer(
        ("127.0.0.1", 0),
        functools.partial(Quiet, directory=str(Path(args.webui).resolve())),
    )
    threading.Thread(target=web.serve_forever, daemon=True).start()
    try:
        with HictTestServer(
            jar, data, out / "server.log"
        ) as api, sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            page = browser.new_page(viewport={"width": 1440, "height": 1000})
            page.set_default_timeout(90000)
            page.add_init_script("window.__HICT_BENCHMARK_ENABLED__=true")
            page.route(
                "http://localhost:5000/**",
                lambda route: route.continue_(
                    url=api.url
                    + route.request.url.removeprefix("http://localhost:5000")
                ),
            )
            check = BrowserCheck(page, out)
            requests, released, checks = [], [], []
            failures, mutations = [], []
            page.on(
                "response",
                lambda response: (
                    failures.append(
                        {
                            "url": response.url,
                            "status": response.status,
                            "body": response.text(),
                        }
                    )
                    if response.status >= 400
                    else None
                ),
            )
            page.on(
                "request",
                lambda request: (
                    mutations.append({"url": request.url, "body": request.post_data})
                    if any(
                        part in request.url
                        for part in (
                            "/tiles/reload",
                            "/set_visualization_options",
                            "/render_pipeline/set",
                            "/visualization/captures",
                        )
                    )
                    else None
                ),
            )
            page.on(
                "request",
                lambda r: (
                    requests.append(r.post_data_json)
                    if r.url.endswith("/visualization/expected_profile")
                    else None
                ),
            )
            page.on(
                "response",
                lambda r: (
                    released.append(r.url.split("/")[-2])
                    if "/visualization/captures/" in r.url
                    and r.url.endswith("/release")
                    and r.ok
                    else None
                ),
            )
            page.add_init_script("""(()=>{
                window.__figureDiagnostics=[];
                setInterval(()=>{
                    const m=window.__hictBenchmark?.manager();
                    if(!m?.isExporting)return;
                    const layers=m.viewAndLayersManager;
                    const entry={time:Date.now(),id:layers.figureRenderCaptureId,
                        size:m.map.getSize(),center:m.getView().getCenter(),resolution:m.getView().getResolution(),
                        loading:m.map.tileQueue_.getTilesLoading(),queued:m.map.tileQueue_.getCount(),
                        complete:m.map.renderComplete_,overlayLoading:layers.overlayStatus.loading.value,
                        layers:layers.layersHolder.hicDataLayers.filter(l=>l.getVisible()).map(l=>{
                            const s=l.getSource(),states={};s?.tileCache?.forEach(t=>states[t.getState()]=(states[t.getState()]??0)+1);
                            return {source:l.get('sourceName'),resolution:l.get('bpResolution'),grid:l.get('projectionKey'),
                                opacity:l.getOpacity(),paused:s?.loadsPaused,requests:s?.activeTileRequests?.size,states};
                        })};
                    window.__figureDiagnostics.push(entry);
                    if(window.__figureDiagnostics.length>100)window.__figureDiagnostics.shift();
                },1000);
            })();""")
            dialog = page.get_by_role("dialog", name="Export figure", exact=True)

            def record(name, **details):
                page.screenshot(path=str(out / (name + ".png")))
                checks.append({"test": name, "status": "passed", **details})
                print("Passed", name, flush=True)

            def close():
                dialog.get_by_role("button", name="Close", exact=True).last.click()
                expect(dialog).to_have_count(0)

            def camera():
                return page.evaluate(
                    "()=>{const v=window.__hictBenchmark.manager().getView();return [v.getCenter(),v.getResolution(),v.getRotation()]}"
                )

            try:
                page.goto(f"http://127.0.0.1:{web.server_port}/")
                page.evaluate(
                    "async api=>await window.__hictBenchmark.open('mixed-primary.hict.hdf5',api)",
                    api.url,
                )
                page.evaluate(
                    "()=>{const v=window.__hictBenchmark.manager().getView();v.setResolution(v.getResolution()/16)}"
                )
                check.settle()
                for mode in ("EXPECTED", "OBSERVED_OVER_EXPECTED"):
                    page.locator('select:has(option[value="EXPECTED"])').select_option(
                        mode
                    )
                    page.evaluate(
                        "async()=>await window.__hictBenchmark.manager().visualizationManager.waitForPendingOptionRequests()"
                    )
                    check.settle()
                    for scope in ("viewport", "whole-map", "selection"):
                        if scope == "selection":
                            page.evaluate(
                                "()=>window.__hictBenchmark.manager().viewAndLayersManager.setRegionBounds({columnStart:5000,columnEnd:7000,rowStart:5000,rowEnd:7000})"
                            )
                        before = camera()
                        mark = len(requests)
                        page.locator("#export-dropdown-toggle").click()
                        expect(dialog.locator("#export-svg-button")).to_be_enabled(
                            timeout=90000
                        )
                        if scope != "viewport":
                            dialog.get_by_label(
                                "Export area", exact=True
                            ).select_option(scope)
                            dialog.get_by_role(
                                "button", name="Refresh capture", exact=True
                            ).click()
                            expect(dialog.locator("#export-svg-button")).to_be_enabled(
                                timeout=90000
                            )
                        name = mode.lower() + "-" + scope
                        with page.expect_download() as download:
                            dialog.locator("#export-svg-button").click()
                        artifact = out / (name + ".svg")
                        download.value.save_as(artifact)
                        root = ET.parse(artifact).getroot()
                        metadata = json.loads(
                            root.find("{http://www.w3.org/2000/svg}metadata").text
                        )
                        capture = metadata["renderCapture"]
                        profile = capture["expectedProfile"]
                        assert capture["signalDisplayMode"] == mode
                        assert len(profile["sha256"]) == 64
                        assert (
                            profile["sampleSemantics"]
                            == "UNIQUE_UNORDERED_SOURCE_PAIR_MASS"
                        )
                        assert capture["primaryNormalization"]["mode"] == "FILE"
                        assert metadata["scope"] == scope
                        assert camera() == before
                        assert page.evaluate(
                            "()=>window.__hictBenchmark.manager().viewAndLayersManager.figureRenderCaptureId===undefined"
                        )
                        # Released contexts can no longer be used, even though saved figure metadata keeps their provenance.
                        page.wait_for_timeout(100)
                        result = page.evaluate(
                            'async ([url,id])=>(await fetch(`${url}/visualization/captures/${id}/validate`,{method:"POST",headers:{"content-type":"application/json"},body:"{}"})).status',
                            [api.url, capture["id"]],
                        )
                        assert result == 409
                        # Export layout must not refit the shared live profile to its temporary whole-map camera.
                        assert (
                            all(r == requests[mark - 1] for r in requests[mark:])
                            if mark
                            else not requests[mark:]
                        )
                        record(
                            name + "-pinned-profile-restored-camera-released-context",
                            profile=profile,
                        )
                        close()
                        check.settle()
                # Shared-reference sources may have different native resolutions.
                attached = page.evaluate("""async()=>{
                    const m=window.__hictBenchmark.manager();
                    const status=await m.networkManager.requestManager.openSecondarySource('mixed-fine.hict.hdf5');
                    m.viewAndLayersManager.setSecondaryResolutionModel(status.compatibility);
                    await m.reloadTilesFromBackend();
                    return status;
                }""")
                assert (
                    attached["attached"]
                    and attached["compatibility"]["sharedReference"]
                )
                check.settle()
                # Force the native sequence -> projected grid transition that
                # previously left a second fine-grid raster visible underneath.
                native_camera = camera()
                page.evaluate(
                    "()=>window.__hictBenchmark.manager().getView().setResolution(0.001)"
                )
                check.settle()
                page.evaluate(
                    "resolution=>window.__hictBenchmark.manager().getView().setResolution(resolution)",
                    native_camera[1],
                )
                check.settle()
                for mode in ("EXPECTED", "OBSERVED_OVER_EXPECTED"):
                    page.locator('select:has(option[value="EXPECTED"])').select_option(
                        mode
                    )
                    page.evaluate(
                        "async()=>await window.__hictBenchmark.manager().visualizationManager.waitForPendingOptionRequests()"
                    )
                    check.settle()
                    for scope in ("viewport", "whole-map"):
                        before = camera()
                        page.locator("#export-dropdown-toggle").click()
                        expect(dialog.locator("#export-svg-button")).to_be_enabled(
                            timeout=90000
                        )
                        if scope == "whole-map":
                            dialog.get_by_label(
                                "Export area", exact=True
                            ).select_option(scope)
                            dialog.get_by_role(
                                "button", name="Refresh capture", exact=True
                            ).click()
                            expect(dialog.locator("#export-svg-button")).to_be_enabled(
                                timeout=90000
                            )
                        with page.expect_download() as download:
                            dialog.locator("#export-svg-button").click()
                        artifact = out / (mode.lower() + "-mixed-" + scope + ".svg")
                        download.value.save_as(artifact)
                        metadata = json.loads(
                            ET.parse(artifact)
                            .getroot()
                            .find("{http://www.w3.org/2000/svg}metadata")
                            .text
                        )
                        assert metadata["provenance"]["secondary"]
                        assert (
                            metadata["renderCapture"]["secondaryNormalization"]["mode"]
                            == "FILE"
                        )
                        assert metadata["renderCapture"]["expectedProfile"][
                            "bpResolution"
                        ] in (4, 10)
                        assert camera() == before
                        visible_secondary = page.evaluate("""()=>{
                            const l=window.__hictBenchmark.manager().viewAndLayersManager;
                            return l.layersHolder.secondaryHiCDataLayers
                                .filter(x=>x.getVisible())
                                .map(x=>({projection:x.get('projectionKey')??null}));
                        }""")
                        assert len(visible_secondary) == 1, visible_secondary
                        assert visible_secondary[0]["projection"], visible_secondary
                        record(mode.lower() + "-mixed-native-grids-" + scope)
                        close()
                        check.settle()
                # At base-level zoom the fitted axis is native BINS, not the synthetic one-base display grid.
                page.evaluate(
                    "()=>window.__hictBenchmark.manager().getView().setResolution(0.001)"
                )
                check.settle()
                assert page.evaluate(
                    "()=>window.__hictBenchmark.manager().viewAndLayersManager.isSequenceScale"
                )
                for mode in ("EXPECTED", "OBSERVED_OVER_EXPECTED"):
                    page.locator('select:has(option[value="EXPECTED"])').select_option(
                        mode
                    )
                    page.evaluate(
                        "async()=>await window.__hictBenchmark.manager().visualizationManager.waitForPendingOptionRequests()"
                    )
                    check.settle()
                    before = camera()
                    page.locator("#export-dropdown-toggle").click()
                    expect(dialog.locator("#export-svg-button")).to_be_enabled(
                        timeout=90000
                    )
                    with page.expect_download() as download:
                        dialog.locator("#export-svg-button").click()
                    artifact = out / (mode.lower() + "-sequence.svg")
                    download.value.save_as(artifact)
                    metadata = json.loads(
                        ET.parse(artifact)
                        .getroot()
                        .find("{http://www.w3.org/2000/svg}metadata")
                        .text
                    )
                    assert metadata["renderedGrid"]["displayGrid"] == "SEQUENCE_BP"
                    assert (
                        metadata["renderCapture"]["expectedProfile"]["unit"] == "BINS"
                    )
                    assert (
                        metadata["renderCapture"]["expectedProfile"]["bpResolution"]
                        == metadata["renderedGrid"]["sourceBpResolution"]
                    )
                    assert camera() == before
                    record(mode.lower() + "-sequence-grid-uses-native-bin-profile")
                    close()
                    check.settle()
                # Record actual prepared coverage weights, not just applyWeights=true.
                mode_control = page.locator('select:has(option[value="EXPECTED"])')
                mode_control.select_option("OBSERVED")
                page.evaluate(
                    "async()=>await window.__hictBenchmark.manager().visualizationManager.waitForPendingOptionRequests()"
                )
                page.locator("#normalization-mode").select_option("sqrt")
                page.wait_for_function(
                    """async url=>{
                    const r=await fetch(url+'/normalization/coverage/status',{method:'POST',headers:{'content-type':'application/json'},body:'{}'});
                    return (await r.json()).mode==='SQRT_COVERAGE';
                }""",
                    arg=api.url,
                    timeout=90000,
                )
                page.evaluate(
                    "async()=>await window.__hictBenchmark.manager().visualizationManager.waitForPendingOptionRequests()"
                )
                check.settle()
                mode_control.select_option("OBSERVED_OVER_EXPECTED")
                page.evaluate(
                    "async()=>await window.__hictBenchmark.manager().visualizationManager.waitForPendingOptionRequests()"
                )
                check.settle()
                page.locator("#export-dropdown-toggle").click()
                expect(dialog.locator("#export-svg-button")).to_be_enabled(
                    timeout=90000
                )
                with page.expect_download() as download:
                    dialog.locator("#export-svg-button").click()
                artifact = out / "coverage-oe.svg"
                download.value.save_as(artifact)
                metadata = json.loads(
                    ET.parse(artifact)
                    .getroot()
                    .find("{http://www.w3.org/2000/svg}metadata")
                    .text
                )
                normalization = metadata["renderCapture"]["primaryNormalization"]
                assert normalization["mode"] == "SQRT_COVERAGE"
                assert (
                    normalization["applyCoolerWeightsOption"]
                    and normalization["algorithmVersion"]
                )
                record("oe-figure-records-active-coverage-algorithm-and-weights")
                close()
                # Actual rendering and weights edits after drawing must fail validation.
                for change in ("rendering", "normalization"):
                    validations = []

                    def hold_validation(route):
                        validations.append(route)

                    page.route("**/visualization/captures/*/validate", hold_validation)
                    before = camera()
                    page.locator("#export-dropdown-toggle").click()
                    expect(dialog).to_contain_text("Capturing figure")
                    for _ in range(200):
                        if validations:
                            break
                        page.wait_for_timeout(50)
                    assert validations
                    status = page.evaluate(
                        """async ([url,change])=>{
                        if(change==='normalization') return (await fetch(url+'/normalization/weights',{method:'POST',headers:{'content-type':'application/json'},body:'{"mode":"FILE"}'})).status;
                        const options=await (await fetch(url+'/get_visualization_options',{method:'POST',headers:{'content-type':'application/json'},body:'{}'})).json();
                        options.postLogBase=options.postLogBase===2?10:2;
                        return (await fetch(url+'/set_visualization_options',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(options)})).status;
                    }""",
                        [api.url, change],
                    )
                    assert status == 200
                    validations[0].continue_()
                    expect(dialog.get_by_role("alert")).to_contain_text(
                        "changed during figure capture"
                    )
                    expect(dialog.locator("#export-svg-button")).to_be_disabled()
                    assert camera() == before
                    assert page.evaluate(
                        "()=>!window.__hictBenchmark.manager().isExporting && window.__hictBenchmark.manager().viewAndLayersManager.figureRenderCaptureId===undefined"
                    )
                    record(
                        "real-"
                        + change
                        + "-change-rejects-capture-and-restores-interactions"
                    )
                    close()
                    page.unroute(
                        "**/visualization/captures/*/validate", hold_validation
                    )
                # A single failed map tile must reject promptly, rather than freeze an incomplete scene.
                failed_tiles = []

                def fail_tile(route):
                    if not failed_tiles:
                        failed_tiles.append(route.request.url)
                        route.fulfill(
                            status=503,
                            headers={
                                "access-control-allow-origin": "*",
                                "access-control-expose-headers": "x-hict-cancelled",
                            },
                            content_type="application/json",
                            body='{"error":"controlled figure tile failure"}',
                        )
                    else:
                        route.continue_()

                tile_pattern = re.compile(r".*/get_tile\?.*renderCapture=.*")
                page.route(tile_pattern, fail_tile)
                page.locator("#export-dropdown-toggle").click()
                expect(dialog.get_by_role("alert")).to_contain_text(
                    "A map tile failed during figure capture", timeout=15000
                )
                expect(dialog.locator("#export-svg-button")).to_be_disabled()
                assert failed_tiles
                assert page.evaluate(
                    "()=>!window.__hictBenchmark.manager().isExporting"
                )
                record("failed-tile-rejects-incomplete-figure-promptly")
                close()
                page.unroute(tile_pattern, fail_tile)
                # Closing while a create response is in flight must release the late context.
                held = []

                def hold(route):
                    response = route.fetch()
                    held.append((route, response, response.json()["id"]))

                page.route("**/visualization/captures", hold)
                page.locator("#export-dropdown-toggle").click()
                expect(dialog).to_contain_text("Capturing figure")
                page.wait_for_timeout(200)
                assert held
                close()
                held[0][0].fulfill(response=held[0][1])
                page.wait_for_timeout(300)
                assert held[0][2] in released
                assert page.evaluate(
                    "()=>!window.__hictBenchmark.manager().isExporting && window.__hictBenchmark.manager().viewAndLayersManager.figureRenderCaptureId===undefined"
                )
                page.unroute("**/visualization/captures", hold)
                record("close-during-context-creation-releases-late-result")
                assert (
                    original
                    == hashlib.sha256(
                        (data / "mixed-primary.hict.hdf5").read_bytes()
                    ).hexdigest()
                )
                failed_capture_id = parse_qs(urlparse(failed_tiles[0]).query)[
                    "renderCapture"
                ][0]
                # The injected failure releases its lease while other requests can
                # already be in flight on the server. Only that released context
                # may return an expired-lease tile response during cleanup.
                cleanup_tile_conflicts = [
                    failure
                    for failure in failures
                    if failure["status"] == 409
                    and urlparse(failure["url"]).path == "/get_tile"
                    and parse_qs(urlparse(failure["url"]).query).get("renderCapture")
                    == [failed_capture_id]
                    and failed_capture_id in released
                    and json.loads(failure["body"])
                    == {
                        "error": "The figure rendering context expired. Capture the figure again."
                    }
                ]
                assert all(
                    failure in cleanup_tile_conflicts
                    or (
                        failure["status"] == 409
                        and "/visualization/captures/" in failure["url"]
                        and failure["url"].endswith("/validate")
                    )
                    or (failure["status"] == 503 and failure["url"] in failed_tiles)
                    for failure in failures
                ), failures
                # Expected 409 probes are console errors; all application errors remain unexpected.
                errors = [
                    e
                    for e in check.errors
                    if "409 (Conflict)" not in e
                    and "503 (Service Unavailable)" not in e
                ]
                assert not errors, errors
                (out / "results.json").write_text(
                    json.dumps(
                        {
                            "checks": checks,
                            "errors": errors,
                            "sourceSha256": original,
                            "profileRequests": requests,
                            "controlledFailureCleanupConflicts": cleanup_tile_conflicts,
                        },
                        indent=2,
                    )
                )
            finally:
                page.screenshot(path=str(out / "final.png"))
                (out / "render-diagnostics.json").write_text(
                    json.dumps(page.evaluate("window.__figureDiagnostics"), indent=2)
                )
                (out / "partial.json").write_text(
                    json.dumps(
                        {
                            "checks": checks,
                            "errors": check.errors,
                            "failures": failures,
                            "mutations": mutations,
                        },
                        indent=2,
                    )
                )
                browser.close()
    finally:
        web.shutdown()
        web.server_close()


if __name__ == "__main__":
    main()
