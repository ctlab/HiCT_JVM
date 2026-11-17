#!/usr/bin/env python3
"""Open actual PNG/SVG exports and inspect the read-only viewer on disposable inputs."""

import argparse
import base64
import functools
import hashlib
import http.server
import json
from pathlib import Path
import struct
import subprocess
import threading

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
            page.set_default_timeout(15000)
            checks = []
            viewer = page.get_by_role("dialog", name="Exported figure", exact=True)
            preview = viewer.get_by_role("img")

            def record(name):
                page.screenshot(path=str(out / (name + ".png")))
                checks.append({"test": name, "status": "passed"})
                print("Passed", name, flush=True)

            def open_viewer():
                check.menu("File", "Open exported figure…")
                expect(viewer).to_be_visible()

            def choose(path):
                with page.expect_file_chooser() as chooser:
                    viewer.get_by_role(
                        "button", name="Open PNG or SVG…", exact=True
                    ).click()
                chooser.value.set_files(str(path))

            def close():
                viewer.get_by_role("button", name="Close", exact=True).last.click()
                expect(viewer).to_have_count(0)

            try:
                page.goto(f"http://127.0.0.1:{web.server_port}/")
                open_viewer()
                expect(viewer).to_contain_text("Choose an exported PNG or SVG")
                record("figure-viewer-is-available-without-an-open-map")
                close()
                page.evaluate(
                    "async api=>await window.__hictBenchmark.open('tiny-primary.hict.hdf5',api)",
                    api.url,
                )
                check.settle()
                original = page.evaluate(
                    "async()=>await window.__hictBenchmark.manager().networkManager.requestManager.getSessionSnapshot(true,true,true,true)"
                )
                files = {}
                for extension in ("png", "svg"):
                    page.get_by_role("button", name="Export", exact=True).click()
                    with page.expect_download(timeout=90000) as download:
                        page.locator(f"#export-{extension}-button").click()
                    path = out / ("actual-map." + extension)
                    download.value.save_as(path)
                    files[extension] = path
                    # Close the persistent export menu without changing the map.
                    page.get_by_role("button", name="Export", exact=True).click()
                    open_viewer()
                    choose(path)
                    expect(preview).to_be_visible()
                    expect(viewer).to_contain_text(path.name)
                    width, height = preview.evaluate(
                        "e=>[e.naturalWidth,e.naturalHeight]"
                    )
                    assert width > 10 and height > 10
                    assert not page.locator(".figure-viewport svg").count()
                    if extension == "png":
                        expected = Image.open(path).convert("RGBA")
                        assert expected.size == (width, height)
                        actual_sha = preview.evaluate(
                            """async e=>{const c=document.createElement('canvas');c.width=e.naturalWidth;c.height=e.naturalHeight;const ctx=c.getContext('2d');ctx.drawImage(e,0,0);return Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',ctx.getImageData(0,0,c.width,c.height).data))).map(x=>x.toString(16).padStart(2,'0')).join('')}"""
                        )
                        assert (
                            actual_sha == hashlib.sha256(expected.tobytes()).hexdigest()
                        )
                    record(
                        "reopen-actual-"
                        + extension
                        + "-export-with-correct-image-content"
                    )
                    viewer.get_by_role("button", name="Actual size", exact=True).click()
                    assert (
                        preview.evaluate("e=>e.getBoundingClientRect().width") == width
                    )
                    viewer.get_by_role(
                        "button", name="Zoom in figure", exact=True
                    ).click()
                    assert (
                        preview.evaluate("e=>e.getBoundingClientRect().width")
                        == width * 1.5
                    )
                    viewer.get_by_role("button", name="Fit", exact=True).click()
                    assert (
                        preview.evaluate("e=>e.getBoundingClientRect().width") <= width
                    )
                    record("figure-" + extension + "-fit-actual-size-and-zoom-controls")
                    close()
                open_viewer()
                choose(files["png"])
                expect(preview).to_be_visible()
                page.set_viewport_size({"width": 640, "height": 600})
                expect(
                    viewer.get_by_role("button", name="Open PNG or SVG…", exact=True)
                ).to_be_in_viewport()
                expect(
                    viewer.get_by_role("button", name="Close", exact=True).last
                ).to_be_in_viewport()
                assert viewer.evaluate("e=>e.scrollWidth<=e.clientWidth+1")
                record("figure-controls-and-fit-preview-remain-usable-at-640-pixels")
                page.set_viewport_size({"width": 1440, "height": 1000})
                with page.expect_file_chooser() as chooser:
                    viewer.get_by_role(
                        "button", name="Open PNG or SVG…", exact=True
                    ).click()
                chooser.value.set_files([])
                expect(preview).to_be_visible()
                expect(viewer).to_contain_text("actual-map.png")
                record("cancelled-file-choice-keeps-the-current-preview")
                invalid = out / "invalid.png"
                invalid.write_text("not an image")
                choose(invalid)
                expect(viewer.get_by_role("alert")).to_contain_text("not a valid PNG")
                expect(viewer).to_contain_text("actual-map.png")
                record("invalid-file-is-explained-without-losing-the-previous-preview")
                huge = out / "oversized-pixels.png"
                huge.write_bytes(
                    b"\x89PNG\r\n\x1a\n"
                    + struct.pack(">I", 13)
                    + b"IHDR"
                    + struct.pack(">II", 100000, 100000)
                )
                choose(huge)
                expect(viewer.get_by_role("alert")).to_contain_text("128-megapixel")
                record("oversized-png-dimensions-are-rejected-before-decoding")
                external = []
                page.on(
                    "request",
                    lambda request: (
                        external.append(request.url)
                        if "/must-not-load-figure" in request.url
                        else None
                    ),
                )
                inert = out / "inert.svg"
                inert.write_text(
                    f"""<svg xmlns="http://www.w3.org/2000/svg" width="320" height="160"><script>window.__figureScriptExecuted=true</script><image href="http://127.0.0.1:{web.server_port}/must-not-load-figure.png" width="100" height="100"/><rect width="320" height="160" fill="green"/><text x="10" y="40">Read-only figure</text></svg>"""
                )
                choose(inert)
                expect(preview).to_have_attribute("alt", "inert.svg")
                assert preview.evaluate("e=>[e.naturalWidth,e.naturalHeight]") == [
                    320,
                    160,
                ]
                assert not page.evaluate("Boolean(window.__figureScriptExecuted)")
                assert not external, external
                expect(viewer.get_by_role("alert")).to_have_count(0)
                record(
                    "svg-is-an-inert-image-without-script-or-external-resource-loading"
                )
                page.keyboard.press("Escape")
                expect(viewer).to_have_count(0)
                after = page.evaluate(
                    "async()=>await window.__hictBenchmark.manager().networkManager.requestManager.getSessionSnapshot(true,true,true,true)"
                )
                assert original["primary"] == after["primary"]
                record("escape-closes-viewer-and-keeps-the-current-assembly")
                open_viewer()
                page.evaluate(
                    """payload=>{
                  window.__figurePickerCalls=[];
                  window.__figurePickerHandle={getFile:async()=>new File([Uint8Array.from(atob(payload),c=>c.charCodeAt(0))],'picker-figure.png',{type:'image/png'})};
                  window.showOpenFilePicker=async options=>{window.__figurePickerCalls.push({id:options.id,remembered:options.startIn===window.__figurePickerHandle});return [window.__figurePickerHandle]};
                }""",
                    base64.b64encode(files["png"].read_bytes()).decode(),
                )
                for _ in range(2):
                    viewer.get_by_role(
                        "button", name="Open PNG or SVG…", exact=True
                    ).click()
                    expect(preview).to_have_attribute("alt", "picker-figure.png")
                picker_calls = page.evaluate("window.__figurePickerCalls")
                assert len(picker_calls) == 2 and all(
                    c["id"] == "hict-figure" for c in picker_calls
                )
                assert picker_calls[1]["remembered"]
                record(
                    "browser-picker-opens-real-file-bytes-and-remembers-the-figure-handle"
                )
                page.evaluate(
                    "()=>{window.showOpenFilePicker=async()=>{throw Object.assign(Error('cancel'),{name:'AbortError'})};}"
                )
                viewer.get_by_role(
                    "button", name="Open PNG or SVG…", exact=True
                ).click()
                expect(preview).to_have_attribute("alt", "picker-figure.png")
                expect(viewer.get_by_role("alert")).to_have_count(0)
                record(
                    "browser-picker-cancellation-retains-the-preview-without-an-error"
                )
                page.evaluate("""()=>{
                  window.showOpenFilePicker=undefined;
                  const create=URL.createObjectURL.bind(URL),revoke=URL.revokeObjectURL.bind(URL);
                  window.__figurePendingUrls=[];window.__figureRevokedUrls=[];
                  URL.createObjectURL=blob=>{const url=create(blob);window.__figurePendingUrls.push(url);return url};
                  URL.revokeObjectURL=url=>{window.__figureRevokedUrls.push(url);revoke(url)};
                  const decode=Image.prototype.decode;
                  Image.prototype.decode=function(){return decode.call(this).then(()=>new Promise(resolve=>{window.__releaseFigureDecode=resolve}))};
                }""")
                choose(files["svg"])
                page.wait_for_function(
                    "typeof window.__releaseFigureDecode==='function'"
                )
                close()
                page.evaluate("window.__releaseFigureDecode()")
                page.wait_for_function(
                    "window.__figurePendingUrls.every(url=>window.__figureRevokedUrls.includes(url))"
                )
                expect(viewer).to_have_count(0)
                record(
                    "late-image-decoding-cannot-reopen-the-viewer-and-releases-its-object-url"
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
