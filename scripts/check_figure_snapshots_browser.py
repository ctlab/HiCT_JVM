#!/usr/bin/env python3
"""Qualify named cut/scaffold snapshots, comparisons and portable sessions through real controls."""

import argparse
import functools
import hashlib
import http.server
import json
from pathlib import Path
import subprocess
import threading
import xml.etree.ElementTree as ET
from PIL import Image
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
                str(data),
            ],
            check=True,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
    source = data / "tiny-primary.hict.hdf5"
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    (data / "features.bed").write_text(
        "tiny\t0\t5\tFirst feature\ntiny\t8\t15\tSecond feature\n"
    )

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
            page.set_default_timeout(30000)
            page.add_init_script("window.__HICT_BENCHMARK_ENABLED__=true")
            page.route(
                "http://localhost:5000/**",
                lambda route: route.continue_(
                    url=api.url
                    + route.request.url.removeprefix("http://localhost:5000")
                ),
            )
            check = BrowserCheck(page, out)
            checks = []
            restore_requests = []
            page.on(
                "request",
                lambda request: (
                    restore_requests.append(request.url)
                    if request.url.endswith("/session/restore")
                    else None
                ),
            )
            dialog = page.get_by_role("dialog", name="Export figure", exact=True)

            def record(name):
                page.screenshot(path=str(out / (name + ".png")))
                checks.append({"test": name, "status": "passed"})
                print("Passed", name, flush=True)

            def open_export():
                print("Opening export", flush=True)
                page.locator("#export-dropdown-toggle").click()
                expect(dialog.locator("#export-svg-button")).to_be_enabled(
                    timeout=90000
                )

            def close_export():
                dialog.get_by_role("button", name="Close", exact=True).last.click()
                expect(dialog).to_have_count(0)

            def save(stem, extension):
                print("Saving", stem, extension, flush=True)
                with page.expect_download(timeout=90000) as download:
                    dialog.locator("#export-" + extension + "-button").click()
                target = out / (stem + "." + extension)
                download.value.save_as(target)
                return target

            def metadata(path):
                if path.suffix == ".png":
                    with Image.open(path) as image:
                        return json.loads(image.text["HiCT provenance"])
                if path.suffix == ".svg":
                    return json.loads(
                        ET.parse(path)
                        .getroot()
                        .find("{http://www.w3.org/2000/svg}metadata")
                        .text
                    )
                root = ET.fromstring(
                    subprocess.check_output(["pdfinfo", "-meta", str(path)])
                )
                return json.loads(root.find(".//{urn:hict:figure#}metadata").text)

            def snapshot(name):
                dialog.get_by_label("Snapshot name", exact=True).fill(name)
                dialog.get_by_role("button", name="Save snapshot", exact=True).click()
                expect(dialog).to_contain_text("Snapshot “" + name + "” added.")

            def library():
                return page.evaluate(
                    "()=>window.__hictBenchmark.manager().figureSnapshots.documents()"
                )

            def show_library():
                if not dialog.get_by_role(
                    "region", name="Saved figure snapshots"
                ).count():
                    dialog.get_by_role(
                        "button", name="Snapshots (", exact=False
                    ).click()

            def compare(name):
                show_library()
                row = dialog.locator("li").filter(
                    has=page.get_by_text(name, exact=True)
                )
                row.get_by_role(
                    "button", name="Compare to snapshot", exact=True
                ).click()
                expect(dialog.locator("#export-svg-button")).to_be_enabled(
                    timeout=90000
                )

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
                before_png = save("before-cut", "png")
                before_svg = save("before-cut", "svg")
                snapshot("Before cut")
                saved_before = library()[0]
                assert ET.fromstring(saved_before["svg"]).findall(
                    ".//{http://www.w3.org/2000/svg}text"
                )
                assert "features.bed" in saved_before["svg"]
                assert saved_before["name"] == "Before cut"
                record("named-snapshot-keeps-original-capture-and-vector-labels")
                page.set_viewport_size({"width": 640, "height": 600})
                show_library()
                assert dialog.evaluate("e=>e.scrollWidth<=e.clientWidth+1")
                expect(dialog.locator("#export-svg-button")).to_be_in_viewport()
                record("snapshot-controls-fit-a-640-pixel-window")
                page.set_viewport_size({"width": 1440, "height": 1000})
                close_export()
                check.menu("File", "Close map")
                prompt = page.get_by_role(
                    "alertdialog", name="Unsaved figure snapshots", exact=True
                )
                expect(prompt).to_be_visible()
                prompt.get_by_role("button", name="Cancel", exact=True).click()
                assert library()[0] == saved_before
                record("snapshot-only-changes-are-protected-before-close")
                # Exact cut keeps the original snapshot and its scientific identity frozen.
                page.evaluate("""async()=>{
                    const m=window.__hictBenchmark.manager(),api=m.networkManager.requestManager;
                    m.eventManager.applyAssemblyInfo(await api.splitContigAtBp(m.contigDimensionHolder.contigDescriptors[0].contigId,5));
                    await m.reloadVisuals();
                }""")
                check.settle()
                open_export()
                snapshot("After cut")
                after_cut = save("after-cut", "png")
                assert library()[0] == saved_before
                assert (
                    metadata(after_cut)["provenance"]["primary"]["agpSha256"]
                    != metadata(before_png)["provenance"]["primary"]["agpSha256"]
                )
                compare("Before cut")
                dialog.get_by_label("Current panel label", exact=True).fill("After cut")
                dialog.get_by_role(
                    "button", name="Update comparison", exact=True
                ).click()
                expect(dialog.locator("#export-svg-button")).to_be_enabled(
                    timeout=90000
                )
                artifacts = {
                    ext: save("cut-comparison", ext) for ext in ("png", "svg", "pdf")
                }
                comparison = metadata(artifacts["svg"])
                assert (
                    comparison
                    == metadata(artifacts["png"])
                    == metadata(artifacts["pdf"])
                )
                assert [p["label"] for p in comparison["comparison"]["panels"]] == [
                    "Before cut",
                    "After cut",
                ]
                root = ET.parse(artifacts["svg"]).getroot()
                assert len(root.findall(".//{http://www.w3.org/2000/svg}metadata")) == 1
                ids = [e.attrib["id"] for e in root.iter() if "id" in e.attrib]
                assert len(ids) == len(set(ids))
                with Image.open(artifacts["png"]).convert("RGBA") as image:
                    for panel, path in zip(
                        comparison["comparison"]["panels"], (before_png, after_cut)
                    ):
                        x, y = panel["offset"]["x"], panel["offset"]["y"]
                        with Image.open(path).convert("RGBA") as original:
                            assert (
                                image.crop(
                                    (x, y, x + panel["width"], y + panel["height"])
                                ).tobytes()
                                == original.tobytes()
                            )
                subprocess.run(
                    [
                        "rsvg-convert",
                        str(artifacts["svg"]),
                        "-o",
                        str(out / "comparison-svg.png"),
                    ],
                    check=True,
                )
                subprocess.run(
                    [
                        "pdftoppm",
                        "-png",
                        "-singlefile",
                        "-r",
                        "96",
                        str(artifacts["pdf"]),
                        str(out / "comparison-pdf"),
                    ],
                    check=True,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                reference = Image.open(artifacts["png"]).convert("RGBA")
                rendered = [
                    Image.open(out / "comparison-svg.png").convert("RGBA"),
                    Image.open(out / "comparison-pdf.png").convert("RGBA"),
                ]
                assert all(image.size == reference.size for image in rendered)
                samples = []
                for y in range(70, reference.height - 10, 13):
                    for x in range(10, reference.width - 10, 13):
                        color = reference.getpixel((x, y))
                        if color[0] <= color[1] + 20 or not all(
                            reference.getpixel((x + dx, y + dy)) == color
                            for dx, dy in [(-2, 0), (2, 0), (0, -2), (0, 2)]
                        ):
                            continue
                        assert all(
                            max(
                                abs(a - b)
                                for a, b in zip(color, image.getpixel((x, y)))
                            )
                            <= 4
                            for image in rendered
                        ), (x, y, color, [image.getpixel((x, y)) for image in rendered])
                        samples.append([x, y, list(color)])
                assert len(samples) > 100, len(samples)
                text = subprocess.check_output(
                    ["pdftotext", str(artifacts["pdf"]), "-"]
                ).decode()
                assert all(
                    label in text
                    for label in ["Before cut", "After cut", "features.bed"]
                )
                (out / "comparison-parity.json").write_text(
                    json.dumps(
                        {
                            "matchingInteriorMapSamples": samples,
                            "editablePdfText": True,
                        },
                        indent=2,
                    )
                )
                record(
                    "cut-comparison-preserves-both-panel-pixels-and-three-format-provenance"
                )
                dialog.get_by_label("Comparison layout", exact=True).select_option(
                    "vertical"
                )
                expect(dialog.locator("#export-svg-button")).to_be_disabled()
                dialog.get_by_role(
                    "button", name="Update comparison", exact=True
                ).click()
                expect(dialog.locator("#export-svg-button")).to_be_enabled(
                    timeout=90000
                )
                vertical = save("vertical-comparison", "svg")
                assert metadata(vertical)["comparison"]["layout"] == "vertical"
                record("comparison-layout-change-requires-an-explicit-update")
                close_export()
                # Real scaffold action uses its typed request and normal selection handler.
                page.evaluate(
                    "()=>window.__hictBenchmark.manager().viewAndLayersManager.selectEntityRangeByBp(0,17)"
                )
                with page.expect_response(
                    lambda response: response.url.endswith(
                        "/group_contigs_into_scaffold"
                    )
                ) as grouped:
                    page.evaluate(
                        "()=>window.__hictBenchmark.manager().eventManager.onAddScaffoldClicked()"
                    )
                assert grouped.value.ok, grouped.value.text()
                check.settle()
                open_export()
                snapshot("After scaffold")
                compare("After cut")
                save("scaffold-comparison", "svg")
                record("scaffold-snapshot-and-comparison-preserve-earlier-cut-captures")
                close_export()
                originals = library()
                # Confirmed writable handle rather than download-started marks the library saved.
                page.evaluate("""()=>{
                    window.__snapshotWrite={text:'',pending:false,hold:false};
                    window.showSaveFilePicker=async()=>({name:'snapshots-session.json',createWritable:async()=>({
                        write:async blob=>{window.__snapshotWrite.text=await blob.text()},
                        close:async()=>{if(window.__snapshotWrite.hold)await new Promise(resolve=>{window.__snapshotWrite.pending=true;window.__snapshotWrite.release=resolve})},abort:async()=>{}
                    }),getFile:async()=>new File([window.__snapshotWrite.text],'snapshots-session.json',{type:'application/json'})});
                }""")
                check.menu("File", "Save session…")
                page.wait_for_function(
                    "()=>!window.__hictBenchmark.manager().figureSnapshots.dirty"
                )
                session_text = page.evaluate("window.__snapshotWrite.text")
                session = json.loads(session_text)
                assert (
                    session["version"] == 5
                    and session["client"]["figureSnapshots"] == originals
                )
                session_path = out / "snapshots-session.json"
                session_path.write_text(session_text)
                page.evaluate("window.showSaveFilePicker=undefined")
                check.menu("File", "Close map")
                expect(
                    page.get_by_role("heading", name="No contact map open", exact=True)
                ).to_be_visible()
                with page.expect_file_chooser() as chooser:
                    check.menu("File", "Open session…")
                chooser.value.set_files(session_path)
                page.get_by_text(
                    "Session restored. Undo history starts from the restored assembly.",
                    exact=True,
                ).wait_for()
                check.settle()
                assert library() == originals
                assert not page.evaluate(
                    "window.__hictBenchmark.manager().figureSnapshots.dirty"
                )
                record(
                    "session-v5-restores-snapshot-bytes-names-provenance-and-clean-baseline"
                )
                open_export()
                compare("Before cut")
                restored = save("restored-comparison", "png")
                assert (
                    metadata(restored)["comparison"]["panels"][0]["snapshotId"]
                    == saved_before["id"]
                )
                record("restored-snapshot-can-be-compared-and-exported")
                close_export()
                # Open the original stored panel without querying or rebuilding the old assembly.
                open_export()
                show_library()
                dialog.get_by_role(
                    "button", name="View snapshot Before cut", exact=True
                ).click()
                expect(dialog).to_contain_text("Saved snapshot: Before cut")
                original_again = save("restored-original-before-cut", "png")
                assert original_again.read_bytes() == before_png.read_bytes()
                solo_svg = save("restored-original-before-cut", "svg")
                assert metadata(solo_svg) == metadata(before_svg)
                assert "features.bed" in solo_svg.read_text()
                record(
                    "stored-snapshot-opens-and-exports-its-original-png-bytes-and-vector-labels"
                )
                close_export()
                # Removal can be undone, and the displayed comparison can itself be stored.
                open_export()
                compare("Before cut")
                snapshot("Stored comparison")
                show_library()
                last = library()[-1]
                assert json.loads(
                    ET.fromstring(last["svg"])
                    .find("{http://www.w3.org/2000/svg}metadata")
                    .text
                )["comparison"]
                dialog.get_by_role(
                    "button", name="Remove snapshot Stored comparison", exact=True
                ).click()
                assert len(library()) == 3
                dialog.get_by_role("button", name="Undo remove", exact=True).click()
                assert library()[-1] == last
                record("displayed-comparison-can-be-stored-and-removal-can-be-undone")
                close_export()
                current_documents = library()
                # A scene with active markup and an updated checksum still fails preflight.
                bad = json.loads(session_text)
                bad["client"]["figureSnapshots"][0]["svg"] = bad["client"][
                    "figureSnapshots"
                ][0]["svg"].replace("</svg>", "<script>alert(1)</script></svg>")
                import zlib

                bad["client"]["figureSnapshots"][0][
                    "sceneCrc32"
                ] = f"{zlib.crc32(bad['client']['figureSnapshots'][0]['svg'].encode()):08x}"
                bad_path = out / "unsafe-session.json"
                bad_path.write_text(json.dumps(bad))
                applied = page.evaluate(
                    "window.__hictBenchmark.manager().networkManager.requestManager.history.applied"
                )
                count_before_bad = len(restore_requests)
                with page.expect_file_chooser() as chooser:
                    check.menu("File", "Open session…")
                chooser.value.set_files(bad_path)
                page.get_by_text(
                    "Invalid figure snapshot: unsupported scene element script",
                    exact=False,
                ).last.wait_for()
                assert library() == current_documents
                assert len(restore_requests) == count_before_bad
                assert (
                    page.evaluate(
                        "window.__hictBenchmark.manager().networkManager.requestManager.history.applied"
                    )
                    == applied
                )
                record("unsafe-session-scene-is-rejected-before-live-state-replacement")
                # The confirmed write may finish after the user adds another snapshot.
                page.evaluate("""()=>{
                    window.__snapshotWrite.hold=true;window.__snapshotWrite.pending=false;
                    window.showSaveFilePicker=async()=>({name:'snapshots-session.json',createWritable:async()=>({
                        write:async blob=>{window.__snapshotWrite.text=await blob.text()},
                        close:async()=>{if(window.__snapshotWrite.hold)await new Promise(resolve=>{window.__snapshotWrite.pending=true;window.__snapshotWrite.release=resolve})},abort:async()=>{}
                    }),getFile:async()=>new File([window.__snapshotWrite.text],'snapshots-session.json',{type:'application/json'})});
                }""")
                check.menu("File", "Save session…")
                page.wait_for_function("window.__snapshotWrite.pending")
                open_export()
                snapshot("Newer while saving")
                close_export()
                page.evaluate(
                    "()=>{window.__snapshotWrite.hold=false;window.__snapshotWrite.release()}"
                )
                page.get_by_text(
                    "The session was saved, but newer edits remain unsaved. Save again before continuing.",
                    exact=True,
                ).last.wait_for()
                assert page.evaluate(
                    "window.__hictBenchmark.manager().figureSnapshots.dirty"
                )
                assert (
                    len(
                        json.loads(page.evaluate("window.__snapshotWrite.text"))[
                            "client"
                        ]["figureSnapshots"]
                    )
                    == 4
                )
                record("late-session-write-does-not-mark-newer-snapshots-saved")
                check.menu("File", "Save session…")
                page.wait_for_function(
                    "()=>!window.__hictBenchmark.manager().figureSnapshots.dirty"
                )
                assert (
                    len(
                        json.loads(page.evaluate("window.__snapshotWrite.text"))[
                            "client"
                        ]["figureSnapshots"]
                    )
                    == 5
                )
                page.evaluate("window.showSaveFilePicker=undefined")
                # Ignore an original PNG read that finishes after the export dialog closes.
                page.evaluate("""()=>{
                    const m=window.__hictBenchmark.manager(),capture=m.captureCurrentFigure;
                    window.__captureBeforeTest=capture;
                    m.captureCurrentFigure=async(...args)=>{const figure=await capture.apply(m,args);window.__testFigure=figure;return figure;};
                }""")
                before_late = library()
                open_export()
                page.evaluate("""()=>{
                    const original=Blob.prototype.arrayBuffer;window.__blobReadBeforeTest=original;
                    Blob.prototype.arrayBuffer=function(...args){
                        if(this===window.__testFigure.png){const blob=this;return new Promise(resolve=>{window.__releaseSnapshotRead=async()=>resolve(await original.apply(blob,args));});}
                        return original.apply(this,args);
                    };
                }""")
                dialog.get_by_label("Snapshot name", exact=True).fill("Closed snapshot")
                dialog.get_by_role("button", name="Save snapshot", exact=True).click()
                page.wait_for_function("!!window.__releaseSnapshotRead")
                close_export()
                page.evaluate(
                    "async()=>{await window.__releaseSnapshotRead();Blob.prototype.arrayBuffer=window.__blobReadBeforeTest;}"
                )
                page.wait_for_timeout(150)
                assert library() == before_late
                record("closing-pending-snapshot-storage-ignores-the-late-png-read")
                # A pending comparison releases its image URL after close and cannot revive the dialog.
                open_export()
                page.evaluate("""()=>{
                    const decode=HTMLImageElement.prototype.decode;window.__decodeBeforeTest=decode;
                    const create=URL.createObjectURL.bind(URL),revoke=URL.revokeObjectURL.bind(URL);
                    window.__createBeforeTest=create;window.__revokeBeforeTest=revoke;window.__activeComparisonUrls=new Set();
                    URL.createObjectURL=blob=>{const url=create(blob);window.__activeComparisonUrls.add(url);return url;};
                    URL.revokeObjectURL=url=>{window.__activeComparisonUrls.delete(url);revoke(url);};
                    HTMLImageElement.prototype.decode=function(...args){const image=this;return new Promise(resolve=>{window.__resumeComparison=async()=>{await decode.apply(image,args);resolve();};});};
                }""")
                show_library()
                dialog.locator("li").filter(
                    has=page.get_by_text("Before cut", exact=True)
                ).get_by_role("button", name="Compare to snapshot", exact=True).click()
                page.wait_for_function("!!window.__resumeComparison")
                close_export()
                page.evaluate("async()=>await window.__resumeComparison()")
                page.wait_for_function("window.__activeComparisonUrls.size===0")
                assert library() == before_late
                expect(dialog).to_have_count(0)
                page.evaluate("""()=>{
                    HTMLImageElement.prototype.decode=window.__decodeBeforeTest;
                    URL.createObjectURL=window.__createBeforeTest;URL.revokeObjectURL=window.__revokeBeforeTest;
                    window.__hictBenchmark.manager().captureCurrentFigure=window.__captureBeforeTest;
                    delete window.__testFigure;
                }""")
                record(
                    "closing-pending-comparison-releases-raster-url-and-keeps-the-map-usable"
                )
                assert digest == hashlib.sha256(source.read_bytes()).hexdigest()
                assert not check.errors, check.errors
                (out / "results.json").write_text(
                    json.dumps(
                        {
                            "checks": checks,
                            "errors": check.errors,
                            "nativeSha256": digest,
                        },
                        indent=2,
                    )
                )
            finally:
                (out / "partial.json").write_text(
                    json.dumps({"checks": checks, "errors": check.errors}, indent=2)
                )
                try:
                    page.screenshot(path=str(out / "final.png"), timeout=5000)
                except Exception as error:
                    print("Final screenshot:", str(error), flush=True)
                browser.close()
    finally:
        web.shutdown()
        web.server_close()


if __name__ == "__main__":
    main()
