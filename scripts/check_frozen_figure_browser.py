#!/usr/bin/env python3
"""Verify frozen previews, format provenance, composition and exact-edit captures."""

import argparse
import functools
import hashlib
import http.server
import json
from pathlib import Path
import subprocess
import threading
import time
import xml.etree.ElementTree as ET

from PIL import Image
from playwright.sync_api import expect, sync_playwright
from sequence_browser_harness import BrowserCheck, HictTestServer


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("jar", "webui", "output"):
        parser.add_argument("--" + name, required=True)
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
                str(data),
            ],
            check=True,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
    (data / "features.bed").write_text(
        "tiny\t0\t5\tFirst feature\ntiny\t8\t15\tSecond feature\n"
    )
    hashes = {
        p.name: hashlib.sha256(p.read_bytes()).hexdigest()
        for p in data.iterdir()
        if p.is_file()
    }

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
            page.add_init_script("window.__HICT_BENCHMARK_ENABLED__=true")
            page.route(
                "http://localhost:5000/**",
                lambda route: route.continue_(
                    url=api.url
                    + route.request.url.removeprefix("http://localhost:5000")
                ),
            )
            check = BrowserCheck(page, out)
            page.set_default_timeout(20000)
            dialog = page.get_by_role("dialog", name="Export figure", exact=True)
            checks = []

            def record(name):
                page.screenshot(path=str(out / (name + ".png")))
                checks.append({"test": name, "status": "passed"})
                print("Passed", name, flush=True)

            def open_export():
                page.locator("#export-dropdown-toggle").click()
                dialog.wait_for()
                check.advanced()
                expect(dialog).to_be_visible()
                expect(dialog.locator("#export-png-button")).to_be_enabled(
                    timeout=90000
                )

            def close_export():
                dialog.get_by_role("button", name="Close", exact=True).last.click()
                expect(dialog).to_have_count(0)

            def save(stem, extension):
                with page.expect_download(timeout=90000) as download:
                    dialog.locator("#export-" + extension + "-button").click()
                target = out / (stem + "." + extension)
                download.value.save_as(target)
                return target

            def metadata(path):
                if path.suffix == ".png":
                    with Image.open(path) as image:
                        image.verify()
                    with Image.open(path) as image:
                        return json.loads(image.text["HiCT provenance"])
                if path.suffix == ".svg":
                    root = ET.parse(path).getroot()
                    return json.loads(
                        root.find("{http://www.w3.org/2000/svg}metadata").text
                    )
                document = ET.fromstring(
                    subprocess.check_output(["pdfinfo", "-meta", str(path)])
                )
                return json.loads(document.find(".//{urn:hict:figure#}metadata").text)

            try:
                page.goto(f"http://127.0.0.1:{web.server_port}/")
                page.evaluate(
                    "async api=>await window.__hictBenchmark.open('tiny-primary.hict.hdf5',api)",
                    api.url,
                )
                page.evaluate(
                    "async()=>await window.__hictBenchmark.manager().linearTrackManager.openTrack('features.bed')"
                )
                check.settle()
                open_export()
                expect(
                    dialog.get_by_role("img", name="Captured figure", exact=True)
                ).to_be_visible()
                assert dialog.locator('input[type="range"]').count() == 0
                before = {}
                for extension in ("png", "svg", "pdf"):
                    before[extension] = save("before", extension)
                first = metadata(before["svg"])
                assert first == metadata(before["png"]) == metadata(before["pdf"])
                assert (
                    first["format"] == "hict-figure"
                    and first["provenance"]["filesFingerprinted"]
                )
                assert first["provenance"]["primary"]["agpSha256"]
                assert first["provenance"]["tracks"]
                assert first["renderedGrid"]["bpResolution"] > 0
                record(
                    "preview-and-three-formats-share-one-capture-with-identical-scientific-provenance"
                )
                page.set_viewport_size({"width": 640, "height": 600})
                expect(dialog.locator("#export-png-button")).to_be_in_viewport()
                expect(
                    dialog.get_by_role("button", name="Close", exact=True).last
                ).to_be_in_viewport()
                assert dialog.evaluate("e=>e.scrollWidth<=e.clientWidth+1")
                record(
                    "export-dialog-controls-remain-reachable-at-640-pixels-without-a-zoom-slider"
                )
                page.set_viewport_size({"width": 1440, "height": 1000})
                dialog.get_by_label("Include 1D tracks", exact=True).uncheck()
                expect(dialog.locator("#export-png-button")).to_be_disabled()
                expect(dialog).to_contain_text("Update the preview to apply the changed options.")
                dialog.get_by_role("button", name="Refresh capture", exact=True).click()
                expect(dialog.locator("#export-png-button")).to_be_enabled(
                    timeout=90000
                )
                no_tracks = metadata(save("without-tracks", "svg"))
                assert no_tracks["composition"] == {
                    "map": True,
                    "rulers": True,
                    "tracks": False,
                }
                record(
                    "composition-changes-require-a-new-capture-and-can-remove-annotation-panels"
                )
                dialog.get_by_label("Include rulers and FASTA", exact=True).uncheck()
                dialog.get_by_role("button", name="Refresh capture", exact=True).click()
                expect(dialog.locator("#export-png-button")).to_be_enabled(
                    timeout=90000
                )
                bare = save("map-only", "svg")
                assert metadata(bare)["composition"] == {
                    "map": True,
                    "rulers": False,
                    "tracks": False,
                }
                record("map-only-composition-removes-ruler-and-track-margins")
                view_before = page.evaluate(
                    "()=>{const v=window.__hictBenchmark.manager().getView();return [v.getCenter(),v.getResolution(),v.getRotation()]}"
                )
                dialog.get_by_label("Export area", exact=True).select_option(
                    "whole-map"
                )
                dialog.get_by_role("button", name="Refresh capture", exact=True).click()
                expect(dialog.locator("#export-png-button")).to_be_enabled(
                    timeout=90000
                )
                whole = metadata(save("whole-map", "png"))
                assert (
                    whole["scope"] == "whole-map"
                    and whole["renderedView"]["mapViewportSize"]
                )
                assert (
                    page.evaluate(
                        "()=>{const v=window.__hictBenchmark.manager().getView();return [v.getCenter(),v.getResolution(),v.getRotation()]}"
                    )
                    == view_before
                ), (
                    view_before,
                    page.evaluate(
                        "()=>{const v=window.__hictBenchmark.manager().getView();return [v.getCenter(),v.getResolution(),v.getRotation()]}"
                    ),
                )
                with Image.open(out / "whole-map.png") as image:
                    assert min(image.size) >= 300, image.size
                record(
                    "whole-map-capture-records-rendered-geometry-and-restores-the-live-view"
                )
                assert whole["wholeMapSizing"] == "FIT_WINDOW"
                dialog.get_by_label("Expand small whole maps to fit window", exact=True).uncheck()
                expect(dialog.locator("#export-png-button")).to_be_disabled()
                dialog.get_by_role("button", name="Refresh capture", exact=True).click()
                expect(dialog.locator("#export-png-button")).to_be_enabled(timeout=90000)
                compact = metadata(save("whole-map-native-grid", "png"))
                assert compact["wholeMapSizing"] == "NATIVE_GRID"
                assert compact["renderedView"]["resolution"] == compact["renderedGrid"]["pixelResolution"]
                assert compact["provenance"] == whole["provenance"]
                with Image.open(out / "whole-map-native-grid.png") as small, Image.open(out / "whole-map.png") as large:
                    assert small.width < large.width and small.height < large.height, (small.size, large.size)
                assert page.evaluate("()=>{const v=window.__hictBenchmark.manager().getView();return [v.getCenter(),v.getResolution(),v.getRotation()]}") == view_before
                record("compact-whole-map-preserves-native-grid-provenance-and-restores-live-camera")
                region = {"columnStart": 1, "columnEnd": 8, "rowStart": 4, "rowEnd": 12}
                page.evaluate(
                    "region=>window.__hictBenchmark.manager().viewAndLayersManager.setRegionBounds(region)",
                    region,
                )
                dialog.get_by_label("Export area", exact=True).select_option(
                    "selection"
                )
                dialog.get_by_role("button", name="Refresh capture", exact=True).click()
                expect(dialog.locator("#export-png-button")).to_be_enabled(
                    timeout=90000
                )
                selected = metadata(save("selected-rectangle", "svg"))
                assert selected["scope"] == "selection"
                assert selected["selectedRegion"] == {
                    "coordinateSystem": "ZERO_BASED_HALF_OPEN",
                    "bounds": region,
                }
                assert selected["imageCrop"]["units"] == "CSS_PIXELS"
                record(
                    "selected-rectangle-capture-preserves-exact-base-bounds-and-crop-geometry"
                )
                close_export()
                # Capture through the same public API, then edit the assembly and save the old scene.
                page.evaluate(
                    "async()=>{window.__savedFigure=await window.__hictBenchmark.manager().captureCurrentFigure();}"
                )
                with page.expect_download() as download:
                    page.evaluate(
                        "()=>window.__hictBenchmark.manager().saveCapturedFigure(window.__savedFigure,'png','immutable-before.png')"
                    )
                immutable = out / "immutable-before.png"
                download.value.save_as(immutable)
                original_bytes = immutable.read_bytes()
                page.evaluate(
                    """async()=>{const m=window.__hictBenchmark.manager();const id=m.contigDimensionHolder.contigDescriptors[0].contigId;const assembly=await m.networkManager.requestManager.splitContigAtBp(id,5);m.eventManager.applyAssemblyInfo(assembly);await m.reloadVisuals();}"""
                )
                check.settle()
                assert page.evaluate(
                    "window.__hictBenchmark.manager().contigDimensionHolder.contigDescriptors.map(d=>d.contigLengthBp)"
                ) == [5, 12]
                with page.expect_download() as download:
                    page.evaluate(
                        "()=>window.__hictBenchmark.manager().saveCapturedFigure(window.__savedFigure,'png','immutable-after.png')"
                    )
                replay = out / "immutable-after.png"
                download.value.save_as(replay)
                assert replay.read_bytes() == original_bytes
                record("a-frozen-figure-remains-byte-identical-after-an-exact-cut")
                open_export()
                after = metadata(save("after-cut", "png"))
                assert (
                    after["provenance"]["primary"]["agpSha256"]
                    != first["provenance"]["primary"]["agpSha256"]
                )
                assert after["history"] != first["history"]
                record("new-capture-records-the-exact-cut-assembly-and-history")
                close_export()
                held = []
                snapshot_calls = 0

                def hold_final_snapshot(route):
                    nonlocal snapshot_calls
                    snapshot_calls += 1
                    if snapshot_calls == 2:
                        held.append(route)
                    else:
                        route.fulfill(
                            response=route.fetch(url=api.url + "/session/snapshot")
                        )

                page.route("**/session/snapshot", hold_final_snapshot)
                page.locator("#export-dropdown-toggle").click()
                dialog.wait_for()
                check.advanced()
                deadline = time.monotonic() + 60
                while not held:
                    assert time.monotonic() < deadline
                    page.wait_for_timeout(25)
                settings = json.loads(api.http("/get_visualization_options", {}))
                changed = json.loads(json.dumps(settings))
                changed["colormap"]["maxSignal"] += 1
                api.http(
                    "/set_visualization_options",
                    {**changed, "preserveRenderPipeline": True},
                )
                held[0].fulfill(
                    response=held[0].fetch(url=api.url + "/session/snapshot")
                )
                expect(dialog.get_by_role("alert")).to_contain_text(
                    "visualization changed"
                )
                expect(dialog.locator("#export-png-button")).to_be_disabled()
                expect(dialog.get_by_role("img")).to_have_count(0)
                record(
                    "concurrent-rendering-changes-reject-an-inconsistent-capture-before-saving"
                )
                close_export()
                page.unroute("**/session/snapshot", hold_final_snapshot)
                api.http(
                    "/set_visualization_options",
                    {**settings, "preserveRenderPipeline": True},
                )
                page.evaluate(
                    "async()=>await window.__hictBenchmark.manager().reloadTilesFromBackend()"
                )
                held = []

                def hold_first_snapshot(route):
                    held.append(route)

                page.route("**/session/snapshot", hold_first_snapshot)
                page.locator("#export-dropdown-toggle").click()
                dialog.wait_for()
                check.advanced()
                deadline = time.monotonic() + 30
                while not held:
                    assert time.monotonic() < deadline
                    page.wait_for_timeout(25)
                close_export()
                page.unroute("**/session/snapshot", hold_first_snapshot)
                for route in held:
                    route.fulfill(
                        response=route.fetch(url=api.url + "/session/snapshot")
                    )
                page.evaluate(
                    "()=>new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve)))"
                )
                expect(dialog).to_have_count(0)
                assert not page.evaluate("window.__hictBenchmark.manager().isExporting")
                record(
                    "closing-capture-ignores-late-provenance-and-keeps-map-interactions-available"
                )
                assert hashes == {
                    name: hashlib.sha256((data / name).read_bytes()).hexdigest()
                    for name in hashes
                }
                assert not check.errors, check.errors
                (out / "results.json").write_text(
                    json.dumps(
                        {
                            "checks": checks,
                            "errors": check.errors,
                            "sourceHashes": hashes,
                            "beforeMetadata": first,
                            "afterMetadata": after,
                        },
                        indent=2,
                    )
                )
            except BaseException:
                page.screenshot(path=str(out / "failure.png"))
                (out / "errors.json").write_text(json.dumps(check.errors))
                raise
            finally:
                browser.close()
    finally:
        web.shutdown()
        web.server_close()


if __name__ == "__main__":
    main()
