#!/usr/bin/env python3
"""Exercise real O/E pickers, ranges, persistence, sessions and ratio graph colors."""

import argparse
import functools
import hashlib
import http.server
import io
import json
import math
from pathlib import Path
import subprocess
import shutil
import sys
import threading
import time
from PIL import Image
from playwright.sync_api import expect, sync_playwright
from sequence_browser_harness import BrowserCheck, HictTestServer


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ("jar", "webui", "data-dir", "output"):
        parser.add_argument("--" + key, required=True)
    parser.add_argument("--hdf-python", default=sys.executable)
    parser.add_argument(
        "--focused-only",
        action="store_true",
        help="Skip baseline suites already qualified on the same artifact pair",
    )
    args = parser.parse_args()
    data, out = Path(args.data_dir).absolute(), Path(args.output).absolute()
    jar = Path(args.jar).resolve()
    if any(p != p.resolve() or not p.is_relative_to("/tmp") for p in (data, out)):
        parser.error("Use fresh nonsymlinked disposable /tmp paths")
    data.mkdir(parents=True, exist_ok=False)
    out.mkdir(parents=True, exist_ok=False)
    scripts = Path(__file__).resolve().parent
    with (out / "fixture.log").open("w") as log:
        subprocess.run(
            [
                "java",
                "--class-path",
                str(jar),
                str(
                    scripts.parent
                    / "src/test/java/ru/itmo/ctlab/hict/fixtures/ReinforcementHdf5Fixture.java"
                ),
                str(data),
            ],
            check=True,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        subprocess.run(
            [
                args.hdf_python,
                str(scripts / "prepare_reinforcement_sources.py"),
                "--data-dir",
                str(data),
            ],
            check=True,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
    shutil.copyfile(data / "tiny-primary.hict.hdf5", data / "old-secondary.hict.hdf5")
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
    url = f"http://127.0.0.1:{web.server_port}/"
    window_errors = []
    try:
        for name in (
            [] if args.focused_only else ["display_mode_roundtrip", "signal_controls"]
        ):
            with HictTestServer(
                jar, data, out / (name + "-server.log")
            ) as regression_api:
                with (out / (name + ".log")).open("w") as log:
                    subprocess.run(
                        [
                            sys.executable,
                            str(scripts / ("check_" + name + "_browser.py")),
                            "--url",
                            url,
                            "--api",
                            regression_api.url,
                            "--output",
                            str(out / name),
                        ],
                        check=True,
                        stdout=log,
                        stderr=subprocess.STDOUT,
                    )
        with HictTestServer(jar, data, out / "server.log") as api:
            with sync_playwright() as pw:
                browser = pw.chromium.launch(headless=True)
                context = browser.new_context(viewport={"width": 1440, "height": 1000})
                context.expose_binding(
                    "__capturePaletteError",
                    lambda source, message: window_errors.append(message),
                )
                context.add_init_script(
                    "window.__HICT_BENCHMARK_ENABLED__=true;window.addEventListener('error',e=>{if(e instanceof ErrorEvent)window.__capturePaletteError(e.message);},true)"
                )
                context.route(
                    "http://localhost:5000/**",
                    lambda r: r.continue_(
                        url=api.url
                        + r.request.url.removeprefix("http://localhost:5000")
                    ),
                )
                page = context.new_page()
                page.set_default_timeout(30000)
                check = BrowserCheck(page, out)

                def post(path, data=None):
                    return json.loads(api.http(path, data or {}))

                def options():
                    return post("/get_visualization_options")

                def mode(value):
                    control = page.locator('select[title^="Expected and O/E"]')
                    control.select_option(value)
                    expect(control).to_be_enabled()
                    check.settle()
                    assert options()["signalDisplayMode"] == value

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
                    assert response.ok, response.text()
                    return Image.open(io.BytesIO(response.body())).convert("RGBA")

                def apply():
                    page.locator("#gradient-apply-button").click()
                    check.settle()

                def bounds(lo, hi):
                    page.get_by_role(
                        "spinbutton", name="Lower signal threshold", exact=True
                    ).fill(str(lo))
                    page.get_by_role(
                        "spinbutton", name="Upper signal threshold", exact=True
                    ).fill(str(hi))
                    apply()

                def pick(index, color):
                    page.locator(".threshold-compact-card").get_by_role(
                        "button", name="Choose color", exact=True
                    ).nth(index).click()
                    picker = page.locator(".hict-color-picker-overlay:visible")
                    field = picker.get_by_role(
                        "textbox", name="Type a color name or hex value", exact=True
                    )
                    field.fill(color)
                    field.press("Tab")
                    picker.locator(".picker_done button").click()
                    apply()

                def assert_palette():
                    cmap = options()["colormap"]
                    assert (cmap["minSignal"], cmap["maxSignal"]) == (0.25, 4), cmap
                    picture = tile()
                    assert picture.getpixel((1, 1)) == (
                        0,
                        255,
                        0,
                        128,
                    ), picture.getpixel((1, 1))
                    # The active viewport profile determines the ratio; color
                    # interpolation must agree with that numeric ratio, including alpha.
                    ratios = page.evaluate(
                        """async()=>Array.from((await window.__hictBenchmark.manager().networkManager.requestManager.queryMatrixFloat32({bpResolution:4,startRowPx:0,endRowPx:5,startColPx:0,endColPx:5,source:'PRIMARY',signalMode:'TRADITIONAL_NORMALIZED'})).values)"""
                    )
                    ratio = ratios[24]
                    assert ratio > 1, ratio
                    strength = min(1, math.log(ratio) / math.log(4))
                    expected = (
                        255,
                        math.floor(255 * (1 - strength) + 0.5),
                        255,
                        math.floor(255 + (204 - 255) * strength + 0.5),
                    )
                    assert picture.getpixel((4, 4)) == expected, (
                        ratio,
                        picture.getpixel((4, 4)),
                        expected,
                    )
                    return picture.tobytes()

                try:
                    page.goto(url)
                    page.evaluate(
                        'api=>window.__hictBenchmark.open("tiny-primary.hict.hdf5",api)',
                        api.url,
                    )
                    check.settle()
                    page.locator("#normalization-mode").select_option("none")
                    page.locator("#signal-transform-linear").click()
                    details = page.locator("#signal-advanced")
                    if details.get_attribute("open") is None:
                        details.locator("summary").click()
                    page.locator("#scaling-across-zoom").select_option("none")
                    bounds(0, 11)
                    observed = post("/render_pipeline/get")
                    baseline = tile().tobytes()
                    pixels = None

                    def colors():
                        nonlocal pixels
                        mode("OBSERVED_OVER_EXPECTED")
                        bounds(0.25, 4)
                        pick(0, "#00ff0080")
                        pick(1, "#ff00ffcc")
                        pixels = assert_palette()
                        assert post("/render_pipeline/get") == observed
                        expect(
                            page.get_by_text(
                                "O/E: lower color is depleted, white is 1, upper color is enriched.",
                                exact=False,
                            )
                        ).to_be_visible()

                    check.step(
                        "actual-pickers-change-oe-rgba-and-keep-observed-graph", colors
                    )

                    def roundtrip():
                        mode("EXPECTED")
                        bounds(2, 9)
                        mode("OBSERVED_OVER_EXPECTED")
                        assert assert_palette() == pixels
                        mode("OBSERVED")
                        assert tile().tobytes() == baseline
                        mode("EXPECTED")
                        cmap = options()["colormap"]
                        assert (cmap["minSignal"], cmap["maxSignal"]) == (2, 9), cmap
                        mode("OBSERVED_OVER_EXPECTED")
                        assert assert_palette() == pixels

                    check.step(
                        "expected-and-ratio-ranges-have-independent-mode-memory",
                        roundtrip,
                    )

                    def reload():
                        page.reload()
                        check.menu("File", "Attach")
                        check.settle()
                        assert assert_palette() == pixels
                        mode("OBSERVED")
                        mode("OBSERVED_OVER_EXPECTED")
                        assert assert_palette() == pixels

                    check.step(
                        "chosen-ratio-colors-and-range-survive-page-reload", reload
                    )

                    def session():
                        with page.expect_download() as event:
                            check.menu("File", "Save session")
                        saved = out / "oe-session.json"
                        event.value.save_as(saved)
                        document = json.loads(saved.read_text())
                        assert (
                            document["snapshot"]["visualizationOptions"][
                                "signalDisplayMode"
                            ]
                            == "OBSERVED_OVER_EXPECTED"
                        )
                        check.menu("File", "Close")
                        page.get_by_role(
                            "heading", name="No contact map open"
                        ).wait_for()
                        with page.expect_file_chooser() as chooser:
                            check.menu("File", "Open session")
                        chooser.value.set_files(saved)
                        page.get_by_text(
                            "Session restored. Undo history starts from the restored assembly.",
                            exact=True,
                        ).wait_for()
                        check.settle()
                        assert assert_palette() == pixels

                    check.step("session-keeps-ratio-palette-ranges-and-pixels", session)

                    def slider():
                        before_pixels = tile().tobytes()
                        upper = page.get_by_role(
                            "slider", name="Upper threshold", exact=True
                        )
                        upper.focus()
                        upper.press("ArrowLeft")
                        page.wait_for_timeout(500)
                        check.settle()
                        assert 1 < options()["colormap"]["maxSignal"] < 4
                        assert tile().tobytes() != before_pixels
                        assert ".25" in str(options()["colormap"]["minSignal"])
                        upper.press("Home")
                        page.wait_for_timeout(500)
                        check.settle()
                        assert options()["colormap"]["maxSignal"] > 1
                        lower = page.get_by_role(
                            "slider", name="Lower threshold", exact=True
                        )
                        lower.press("End")
                        page.wait_for_timeout(500)
                        check.settle()
                        assert 0 < options()["colormap"]["minSignal"] < 1
                        bounds(0.25, 4)

                    check.step("keyboard-range-handle-publishes-ratio-limits", slider)

                    def pipeline_palette():
                        mode("OBSERVED")
                        color = {
                            "type": "colormap",
                            "mode": "OBSERVED_OVER_EXPECTED",
                            "input": {"type": "source", "source": "PRIMARY"},
                            "startColor": "#00ff0080",
                            "endColor": "#ff00ffcc",
                            "minSignal": 0.25,
                            "maxSignal": 4,
                        }
                        config = {
                            "enabled": True,
                            "swapUpperLower": False,
                            "upperExpression": color,
                            "lowerExpression": color,
                        }
                        page.evaluate(
                            "async config=>{const m=window.__hictBenchmark.manager();await m.networkManager.requestManager.setRenderPipelineConfig(config);await m.reloadTilesFromBackend();}",
                            config,
                        )
                        check.settle()
                        picture = tile()
                        assert picture.getpixel((0, 0)) == (255, 255, 255, 255)
                        assert picture.getpixel((1, 1)) == (0, 255, 0, 128)
                        assert picture.getpixel((4, 4)) == (255, 0, 255, 204)
                        check.menu("View", "Rendering pipeline…")
                        dialog = page.get_by_role("dialog")
                        dialog.get_by_role(
                            "heading", name="Rendering Pipeline", exact=True
                        ).wait_for()
                        page.screenshot(path=str(out / "ratio-pipeline-editor.png"))
                        with page.expect_response(
                            lambda r: r.url.endswith("/render_pipeline/set")
                            and r.request.method == "POST"
                        ) as committed:
                            dialog.get_by_role(
                                "button", name="Apply", exact=True
                            ).click()
                        assert committed.value.ok, committed.value.text()
                        expect(
                            dialog.get_by_role("button", name="Apply", exact=True)
                        ).to_be_enabled()
                        dialog.get_by_role(
                            "button", name="Close rendering editor", exact=True
                        ).click()
                        dialog.wait_for(state="hidden")
                        check.settle()
                        graph = post("/render_pipeline/get")
                        assert (
                            graph["upperExpression"]["mode"] == "OBSERVED_OVER_EXPECTED"
                        ), graph
                        assert tile().tobytes() == picture.tobytes()
                        bounds(0.25, 8)
                        graph = post("/render_pipeline/get")
                        assert (
                            graph["upperExpression"]["mode"] == "OBSERVED_OVER_EXPECTED"
                            and graph["upperExpression"]["maxSignal"] == 8
                        ), graph

                    check.step(
                        "ratio-palette-node-survives-editor-apply-and-sidebar-edit",
                        pipeline_palette,
                    )

                    def await_upper(value):
                        deadline = time.monotonic() + 10
                        while time.monotonic() < deadline:
                            if options()["colormap"]["maxSignal"] == value:
                                return
                            page.wait_for_timeout(30)
                        raise AssertionError(options())

                    # Exercise the active primary palette while the sidebar is on Secondary.
                    page.evaluate(
                        """async observed=>{const m=window.__hictBenchmark.manager(),r=m.networkManager.requestManager;
                        const status=await r.openSecondarySource('tiny-identity.hict.hdf5',true);
                        m.viewAndLayersManager.setSecondaryResolutionModel(status.compatibility);
                        const identity={type:'colormap',mode:'MODDOTPLOT_IDENTITY',input:{type:'binary',op:'DIV',left:{type:'source',source:'SECONDARY'},right:{type:'constant',value:10000}},startColor:'#31369500',endColor:'#a50026ff',minSignal:80,maxSignal:100};
                        const blend={type:'pixel_blend',mode:'OVER',top:identity,bottom:observed.upperExpression,topOpacity:.65,bottomOpacity:1};
                        await r.setRenderPipelineConfig({enabled:true,swapUpperLower:false,upperExpression:blend,lowerExpression:blend});await m.reloadTilesFromBackend();}""",
                        observed,
                    )
                    check.settle()
                    editor_expected = json.loads(json.dumps(observed))
                    editor_expected["enabled"] = True
                    editor_expected["lowerExpression"] = {
                        "type": "colormap",
                        "mode": "MODDOTPLOT_IDENTITY",
                        "input": {
                            "type": "binary",
                            "op": "DIV",
                            "left": {"type": "source", "source": "SECONDARY"},
                            "right": {"type": "constant", "value": 10000},
                        },
                        "startColor": "#31369500",
                        "endColor": "#a50026ff",
                        "minSignal": 80,
                        "maxSignal": 100,
                    }
                    blend = {
                        "type": "pixel_blend",
                        "mode": "OVER",
                        "top": editor_expected["lowerExpression"],
                        "bottom": observed["upperExpression"],
                        "topOpacity": 0.65,
                        "bottomOpacity": 1,
                    }
                    editor_expected = {
                        "enabled": True,
                        "swapUpperLower": False,
                        "upperExpression": blend,
                        "lowerExpression": blend,
                    }
                    assert post("/render_pipeline/get") == editor_expected, post(
                        "/render_pipeline/get"
                    )
                    mode("OBSERVED_OVER_EXPECTED")
                    assert post("/render_pipeline/get") == editor_expected, post(
                        "/render_pipeline/get"
                    )
                    page.get_by_role(
                        "group", name="Visualization source", exact=True
                    ).get_by_role("button", name="Secondary", exact=True).click()
                    check.settle()
                    editor_observed = post("/render_pipeline/get")
                    editor_signal_options={k:v for k,v in options().items() if k!="colormap"}
                    assert editor_observed == editor_expected, editor_observed
                    assert tile("SECONDARY").getchannel("A").getextrema()[1] > 0
                    secondary_pixels = tile("SECONDARY").tobytes()
                    dialog = page.get_by_role("dialog")
                    editor = dialog.locator(".oe-palette-editor")

                    def editor_open():
                        check.menu("View", "Rendering pipeline…")
                        expect(
                            dialog.get_by_role(
                                "tab", name="Primary O/E colormap", exact=True
                            )
                        ).to_have_attribute("aria-selected", "true")
                        expect(
                            dialog.get_by_role("button", name="Apply", exact=True)
                        ).to_be_enabled()

                    def unchanged():
                        assert {k:v for k,v in options().items() if k!="colormap"}==editor_signal_options
                        assert post("/render_pipeline/get") == editor_observed
                        assert tile("SECONDARY").tobytes() == secondary_pixels
                        expect(
                            page.get_by_role(
                                "group", name="Visualization source", exact=True
                            ).get_by_role("button", name="Secondary", exact=True)
                        ).to_have_attribute("aria-pressed", "true")

                    def active_editor():
                        editor_open()
                        expect(
                            dialog.get_by_label("Enable custom pipeline", exact=True)
                        ).not_to_be_visible()
                        tab = dialog.get_by_role(
                            "tab", name="Primary O/E colormap", exact=True
                        )
                        tab.focus()
                        tab.press("Home")
                        expect(
                            dialog.get_by_role("tab", name="Observed graph", exact=True)
                        ).to_have_attribute("aria-selected", "true")
                        expect(
                            dialog.get_by_role("tab", name="Observed graph", exact=True)
                        ).to_be_focused()
                        unchanged()
                        dialog.get_by_role(
                            "tab", name="Observed graph", exact=True
                        ).press("End")
                        expect(tab).to_have_attribute("aria-selected", "true")
                        expect(tab).to_be_focused()
                        expect(
                            dialog.get_by_role("button", name="Apply", exact=True)
                        ).to_be_enabled()
                        unchanged()
                        editor.get_by_role(
                            "combobox", name="O/E palette", exact=True
                        ).select_option("purple-green")
                        editor.get_by_role(
                            "spinbutton", name="O/E upper ratio", exact=True
                        ).fill("7")
                        await_upper(7)
                        assert tile().getpixel((1, 1)) == (118, 42, 131, 255)
                        unchanged()
                        page.screenshot(path=str(out / "active-oe-editor.png"))
                        for width, height in [(800, 600), (640, 800)]:
                            page.set_viewport_size({"width": width, "height": height})
                            check.settle()
                            panel = dialog.locator(".modal-content")
                            rect = panel.bounding_box()
                            assert (
                                rect["x"] >= 0
                                and rect["x"] + rect["width"] <= width + 1
                                and rect["y"] >= 0
                                and rect["y"] + rect["height"] <= height + 1
                            ), rect
                            for button in ["Apply", "Close", "Reset"]:
                                control = dialog.locator(".modal-footer").get_by_role(
                                    "button", name=button, exact=True
                                )
                                assert control.evaluate(
                                    "e=>{const b=e.getBoundingClientRect(),p=document.elementFromPoint(b.x+b.width/2,b.y+b.height/2);return b.bottom<=innerHeight+1&&(p===e||e.contains(p));}"
                                ), (width, button)
                            editor.get_by_role(
                                "spinbutton", name="O/E upper ratio", exact=True
                            ).scroll_into_view_if_needed()
                            page.screenshot(
                                path=str(
                                    out / ("active-oe-editor-" + str(width) + ".png")
                                )
                            )
                        page.set_viewport_size({"width": 1440, "height": 1000})
                        check.settle()
                        dialog.get_by_role(
                            "button", name="Preview", exact=True
                        ).last.click()
                        expect(editor).not_to_be_visible()
                        page.screenshot(path=str(out / "active-oe-preview.png"))
                        sources = page.get_by_role(
                            "group", name="Visualization source", exact=True
                        )
                        sources.get_by_role(
                            "button", name="Primary", exact=True
                        ).click()
                        check.settle()
                        page.locator(".threshold-compact-card").get_by_role(
                            "combobox", name="O/E palette", exact=True
                        ).select_option("blue-red")
                        deadline = time.monotonic() + 10
                        while time.monotonic() < deadline and tile().getpixel(
                            (1, 1)
                        ) != (33, 102, 172, 255):
                            page.wait_for_timeout(30)
                        assert tile().getpixel((1, 1)) == (33, 102, 172, 255)
                        sources.get_by_role(
                            "button", name="Secondary", exact=True
                        ).click()
                        check.settle()
                        dialog.get_by_role(
                            "button", name="Show editor", exact=True
                        ).click()
                        expect(editor).to_be_visible()
                        expect(
                            editor.get_by_role(
                                "combobox", name="O/E palette", exact=True
                            )
                        ).to_have_value("blue-red")
                        editor.get_by_role(
                            "combobox", name="O/E palette", exact=True
                        ).select_option("purple-green")
                        deadline = time.monotonic() + 10
                        while time.monotonic() < deadline and tile().getpixel(
                            (1, 1)
                        ) != (118, 42, 131, 255):
                            page.wait_for_timeout(30)
                        assert tile().getpixel((1, 1)) == (118, 42, 131, 255)
                        unchanged()

                    check.step(
                        "active-oe-editor-default-tab-presets-keyboard-preview-and-secondary-isolation",
                        active_editor,
                    )

                    def editor_files():
                        with page.expect_download() as download:
                            dialog.get_by_role(
                                "button", name="Export palette", exact=True
                            ).click()
                        saved = out / "active-oe-palette.json"
                        download.value.save_as(saved)
                        document = json.loads(saved.read_text())
                        assert (
                            document["target"] == "PRIMARY_OE"
                            and document["palette"]["max"] == 7
                        )
                        dialog.get_by_role("button", name="Reset", exact=True).click()
                        await_upper(2)
                        assert tile().getpixel((1, 1)) == (33, 102, 172, 255)
                        with page.expect_file_chooser() as chooser:
                            dialog.get_by_role(
                                "button", name="Import palette", exact=True
                            ).click()
                        chooser.value.set_files(saved)
                        await_upper(7)
                        assert tile().getpixel((1, 1)) == (118, 42, 131, 255)
                        unchanged()

                    check.step(
                        "active-oe-editor-export-reset-import-and-pixel-parity",
                        editor_files,
                    )

                    def queued_edits():
                        editor.get_by_role(
                            "spinbutton", name="O/E upper ratio", exact=True
                        ).fill("4")
                        await_upper(4)
                        held = []
                        writes = []

                        def hold(route):
                            data = route.request.post_data_json
                            writes.append(
                                data.get("options", data)["colormap"]["maxSignal"]
                            )
                            if len(writes) == 1:
                                held.append(route)
                            else:
                                route.fallback()

                        page.route("**/set_visualization_options", hold)
                        try:
                            field = editor.get_by_role(
                                "spinbutton", name="O/E upper ratio", exact=True
                            )
                            field.fill("6")
                            page.wait_for_timeout(500)
                            assert len(held) == 1
                            field.fill("8")
                            page.wait_for_timeout(500)
                            field.fill("4")
                            page.wait_for_timeout(500)
                            held.pop().fallback()
                            expect(
                                dialog.get_by_role("button", name="Apply", exact=True)
                            ).to_be_enabled()
                            await_upper(4)
                            assert writes == [6, 4], writes
                        finally:
                            for route in held:
                                route.fallback()
                            page.unroute("**/set_visualization_options", hold)
                        unchanged()

                    check.step(
                        "active-oe-editor-serializes-late-writes-and-restores-the-latest-draft",
                        queued_edits,
                    )

                    def rejected_edit():
                        before = len(check.errors)
                        page.route(
                            "**/set_visualization_options",
                            lambda r: r.fulfill(
                                status=503,
                                json={"error": "deliberate palette rejection"},
                            ),
                        )
                        editor.get_by_role(
                            "spinbutton", name="O/E upper ratio", exact=True
                        ).fill("5")
                        expect(editor.get_by_role("alert")).to_be_visible()
                        assert options()["colormap"]["maxSignal"] == 4
                        unchanged()
                        page.unroute("**/set_visualization_options")
                        assert all(
                            "503" in e for e in check.errors[before:]
                        ), check.errors[before:]
                        del check.errors[before:]
                        dialog.get_by_role("button", name="Apply", exact=True).click()
                        await_upper(5)
                        unchanged()
                        editor.get_by_role(
                            "spinbutton", name="O/E lower ratio", exact=True
                        ).fill("1")
                        expect(editor.get_by_role("alert")).to_be_visible()
                        dialog.get_by_role("button", name="Close", exact=True).click()
                        dialog.wait_for(state="hidden")
                        assert (
                            options()["colormap"]["minSignal"] == 0.25
                            and options()["colormap"]["maxSignal"] == 5
                        )
                        unchanged()

                    check.step(
                        "active-oe-editor-failed-write-retry-and-invalid-draft-close",
                        rejected_edit,
                    )

                    def sidebar_presets_and_mode_guard():
                        sources = page.get_by_role(
                            "group", name="Visualization source", exact=True
                        )
                        sources.get_by_role(
                            "button", name="Primary", exact=True
                        ).click()
                        check.settle()
                        page.locator(".threshold-compact-card").get_by_role(
                            "combobox", name="O/E palette", exact=True
                        ).select_option("blue-red")
                        deadline = time.monotonic() + 10
                        while time.monotonic() < deadline and tile().getpixel(
                            (1, 1)
                        ) != (33, 102, 172, 255):
                            page.wait_for_timeout(30)
                        assert tile().getpixel((1, 1)) == (33, 102, 172, 255)
                        assert post("/render_pipeline/get") == editor_observed
                        mode("OBSERVED")
                        before = options()
                        outcome = page.evaluate(
                            """async()=>{try {await window.__hictBenchmark.manager().visualizationManager.applyObservedExpectedPalette({min:.25,max:4,startColor:'#2166acff',endColor:'#b2182bff'});return 'wrongly applied';}catch(e){return e.message;}}"""
                        )
                        assert "Select Show O/E" in outcome, outcome
                        assert (
                            options() == before
                            and post("/render_pipeline/get") == editor_observed
                        )
                        mode("OBSERVED_OVER_EXPECTED")
                        editor_open()
                        dialog.locator(".modal-footer").get_by_role(
                            "button", name="Preview", exact=True
                        ).click()
                        expect(editor).not_to_be_visible()
                        mode("OBSERVED")
                        dialog.get_by_role(
                            "button", name="Show editor", exact=True
                        ).click()
                        expect(dialog.locator(".graph-canvas")).to_be_visible()
                        assert (
                            options()["signalDisplayMode"] == "OBSERVED"
                            and post("/render_pipeline/get") == editor_observed
                        )
                        dialog.get_by_role(
                            "button", name="Close rendering editor", exact=True
                        ).click()
                        dialog.wait_for(state="hidden")

                    check.step(
                        "shared-sidebar-presets-and-inactive-mode-publication-guard",
                        sidebar_presets_and_mode_guard,
                    )
                    assert not check.errors, check.errors
                    assert not window_errors, window_errors
                    assert all(
                        hashlib.sha256((data / name).read_bytes()).hexdigest() == value
                        for name, value in hashes.items()
                    )
                    (out / "results.json").write_text(
                        json.dumps(
                            {
                                "status": "ok",
                                "checks": check.rows,
                                "sourceHashes": hashes,
                                "pageErrors": check.errors,
                                "windowErrors": window_errors,
                            },
                            indent=2,
                        )
                    )
                except Exception:
                    page.screenshot(path=str(out / "failed.png"))
                    raise
                finally:
                    browser.close()
    finally:
        web.shutdown()
        web.server_close()


if __name__ == "__main__":
    main()
