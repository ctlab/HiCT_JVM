#!/usr/bin/env python3
"""Exercise color-only midpoints through rendering, source changes and persistence."""

import argparse, functools, hashlib, http.server, io, json, math, shutil, subprocess, threading
from pathlib import Path
from PIL import Image
from playwright.sync_api import expect, sync_playwright
from sequence_browser_harness import BrowserCheck, HictTestServer
from browser_review_helpers import select_visualization_source


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ("jar", "webui", "data-dir", "output"):
        parser.add_argument("--" + key, required=True)
    args = parser.parse_args()
    data, out = Path(args.data_dir).absolute(), Path(args.output).absolute()
    if any(p != p.resolve() or not p.is_relative_to("/tmp") for p in (data, out)):
        parser.error("Use fresh disposable /tmp paths")
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
    shutil.copyfile(data / "tiny-primary.hict.hdf5", data / "tiny-secondary.hict.hdf5")
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

            def post(path, body=None):
                return json.loads(api.http(path, body or {}))

            def options():
                return post("/get_visualization_options")

            def wait():
                page.evaluate(
                    "()=>window.__hictBenchmark.manager().visualizationManager.waitForPendingOptionRequests()"
                )
                check.settle()

            def graph():
                return post("/render_pipeline/get")

            def choose(source):
                select_visualization_source(page, source)
                check.settle()
                wait()

            def gamma(source="PRIMARY"):
                if source == "PRIMARY":
                    return options()["colormap"].get("gamma", 1)
                return graph()["lowerExpression"].get("gamma", 1)

            def tile(source="PRIMARY"):
                r = page.request.get(
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
                assert r.ok
                return Image.open(io.BytesIO(r.body())).convert("RGBA")

            def query(expression):
                return post(
                    "/matrix/query",
                    {
                        "bpResolution": 4,
                        "unit": "PIXELS",
                        "startRowPx": 0,
                        "endRowPx": 5,
                        "startColPx": 0,
                        "endColPx": 5,
                        "signalMode": "PIPELINE_SIGNAL",
                        "expression": expression,
                        "format": "JSON",
                    },
                )["values"]

            def record(name):
                page.screenshot(path=str(out / (name + ".png")))
                rows.append({"test": name, "status": "passed"})
                print("Passed", name, flush=True)

            midpoint = page.get_by_role("slider", name="Color midpoint", exact=True)

            def mode(value):
                c = page.locator('select[title^="Expected and O/E"]')
                c.select_option(value)
                expect(c).to_be_enabled()
                wait()
                assert options()["signalDisplayMode"] == value

            def key(value):
                midpoint.focus()
                midpoint.press(value)
                wait()

            try:
                page.goto(f"http://127.0.0.1:{web.server_port}/")
                page.wait_for_function("window.__hictBenchmark")
                page.evaluate(
                    'api=>window.__hictBenchmark.open("tiny-primary.hict.hdf5",api)',
                    api.url,
                )
                check.settle()
                page.locator("#normalization-mode").select_option("none")
                wait()
                page.locator("#signal-transform-linear").click()
                wait()
                page.evaluate(
                    """async()=>{const m=window.__hictBenchmark.manager(),r=m.networkManager.requestManager;
                  const status=await r.openSecondarySource('tiny-secondary.hict.hdf5',true);m.viewAndLayersManager.setSecondaryResolutionModel(status.compatibility);
                  const color=s=>({type:'colormap',mode:'LINEAR',input:{type:'source',source:s},minSignal:0,maxSignal:11,startColor:'#ffffffff',endColor:'#ff0000ff'});
                  await r.setRenderPipelineConfig({enabled:true,upperExpression:color('PRIMARY'),lowerExpression:color('SECONDARY')});
                  await m.reloadTilesFromBackend();window.dispatchEvent(new CustomEvent('hict:render-pipeline-changed'));}"""
                )
                check.settle()
                wait()
                choose("PRIMARY")
                initial = graph()
                input_before = initial["upperExpression"]["input"]
                raw = query(input_before)
                secondary_before = initial["lowerExpression"]
                before = tile().getpixel((1, 0))
                expect(midpoint).to_have_count(1)
                assert gamma() == 1
                key("ArrowRight")
                assert gamma() > 1
                after = tile().getpixel((1, 0))
                expected = round(255 * (1 - (3 / 11) ** gamma()))
                assert abs(after[1] - expected) <= 1 and after[1] > before[1], (
                    before,
                    after,
                    expected,
                    gamma(),
                )
                assert (
                    query(input_before) == raw
                    and graph()["upperExpression"]["input"] == input_before
                    and graph()["lowerExpression"] == secondary_before
                )
                assert (
                    options()["colormap"]["minSignal"] == 0
                    and options()["colormap"]["maxSignal"] == 11
                )
                record(
                    "keyboard-midpoint-changes-actual-colors-without-changing-signal-or-the-other-source"
                )
                # Real pointer drag moves the diamond through the frozen slider domain.
                box = midpoint.bounding_box()
                rail = page.locator(".signal-range").bounding_box()
                assert box and rail
                page.mouse.move(
                    box["x"] + box["width"] / 2, box["y"] + box["height"] / 2
                )
                page.mouse.down()
                page.mouse.move(
                    rail["x"] + rail["width"] * 0.75,
                    box["y"] + box["height"] / 2,
                    steps=8,
                )
                page.mouse.up()
                wait()
                assert abs(math.pow(0.5, 1 / gamma()) - 0.75) < 0.03, gamma()
                assert query(input_before) == raw
                record("pointer-drag-places-half-color-at-the-selected-signal")
                saved_gamma = gamma()
                primary_before = graph()["upperExpression"]
                choose("SECONDARY")
                assert gamma("SECONDARY") == 1
                key("ArrowLeft")
                assert (
                    gamma("SECONDARY") < 1
                    and graph()["upperExpression"] == primary_before
                )
                choose("PRIMARY")
                assert gamma() == saved_gamma
                record("source-switch-keeps-independent-midpoints-and-inputs")
                # Typed thresholds and a logarithmic slider axis preserve the color curve.
                page.get_by_label("Upper signal threshold", exact=True
                ).fill("22")
                page.get_by_role("button", name="Apply", exact=True).click()
                wait()
                assert gamma() == saved_gamma
                page.get_by_role("group", name="Slider scale", exact=True).get_by_role(
                    "button", name="Log", exact=True
                ).click()
                key("ArrowLeft")
                assert gamma() != saved_gamma
                assert query(input_before) == raw
                record(
                    "threshold-and-log-axis-adjustments-keep-color-and-signal-transforms-separate"
                )
                page.get_by_role("button", name="Reset curve", exact=True).click()
                wait()
                assert gamma() == 1
                assert (
                    graph()["upperExpression"]["input"] == input_before
                    and options()["colormap"]["maxSignal"] == 22
                )
                record(
                    "reset-restores-linear-colors-without-changing-the-selected-range"
                )
                page.get_by_role("group", name="Slider scale", exact=True).get_by_role(
                    "button", name="Linear", exact=True
                ).click()
                maximum = page.get_by_label("Slider maximum signal", exact=True
                )
                maximum.fill("10000")
                maximum.press("Tab")
                diamond_box = midpoint.bounding_box()
                upper_box = page.get_by_role(
                    "slider", name="Upper threshold", exact=True
                ).bounding_box()
                assert (
                    diamond_box
                    and upper_box
                    and diamond_box["y"] + diamond_box["height"] < upper_box["y"]
                )
                key("ArrowRight")
                assert gamma() > 1
                record(
                    "close-thresholds-leave-the-midpoint-visible-and-keyboard-accessible"
                )
                page.get_by_role("button", name="Focus range", exact=True).click()
                key("ArrowRight")
                observed_gamma = gamma()
                observed_graph = graph()
                mode("EXPECTED")
                expect(midpoint).to_have_count(1)
                key("ArrowLeft")
                expected_gamma = gamma()
                assert expected_gamma != observed_gamma
                mode("OBSERVED_OVER_EXPECTED")
                expect(midpoint).to_have_count(0)
                assert (
                    options()["signalDisplayMode"] == "OBSERVED_OVER_EXPECTED"
                    and graph() == observed_graph
                )
                mode("EXPECTED")
                assert gamma() == expected_gamma
                mode("OBSERVED")
                assert gamma() == observed_gamma and graph() == observed_graph
                record(
                    "derived-mode-memory-preserves-expected-midpoint-and-fixed-oe-neutral"
                )
                # A source preset intentionally replaces the source curve with its own default.
                choose("SECONDARY")
                page.get_by_role(
                    "combobox", name="Visualization preset", exact=True
                ).select_option(label="Cold-Hot")
                wait()
                assert gamma("SECONDARY") == 1
                choose("PRIMARY")
                assert gamma() == observed_gamma
                record("source-preset-replaces-only-its-own-midpoint")
                # Portable full-graph preset importing keeps explicit gamma in both branches.
                preset_options = options()
                preset_options["colormap"]["gamma"] = 2
                preset_graph = graph()
                preset_graph["upperExpression"]["gamma"] = 2
                preset_graph["lowerExpression"]["gamma"] = 0.5
                imported = {
                    "exportType": "visualizationOptions",
                    "data": {
                        "savedVisualizationPresets": [
                            {
                                "name": "Gamma test",
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
                group.locator("input[type=file]").set_input_files(
                    {
                        "name": "gamma.json",
                        "mimeType": "application/json",
                        "buffer": json.dumps(imported).encode(),
                    }
                )
                page.get_by_role(
                    "combobox", name="Visualization preset", exact=True
                ).select_option(label="Gamma test")
                wait()
                assert gamma() == 2 and gamma("SECONDARY") == 0.5
                record("portable-whole-graph-preset-retains-both-source-gammas")
                if not group.is_visible():
                    page.get_by_role("button", name="Manage…", exact=True).click()
                midpoint.press("ArrowRight")
                save_preset = group.get_by_title(
                    "Save applied visualization and complete rendering graph",
                    exact=True,
                )
                save_preset.click()
                expect(save_preset).to_be_enabled()
                wait()
                applied_gamma = gamma()
                assert applied_gamma != 2
                with page.expect_download() as exported_event:
                    group.get_by_role("button", name="Export", exact=True).click()
                exported_file = out / "saved-gamma-presets.json"
                exported_event.value.save_as(exported_file)
                exported_presets = json.loads(exported_file.read_text())["data"][
                    "savedVisualizationPresets"
                ]
                saved_preset = next(
                    p for p in exported_presets if p["name"].startswith("Preset ")
                )
                assert saved_preset["options"]["colormap"]["gamma"] == applied_gamma
                assert (
                    saved_preset["renderPipeline"]["upperExpression"]["gamma"]
                    == applied_gamma
                )
                page.get_by_role(
                    "combobox", name="Visualization preset", exact=True
                ).select_option(label="Gamma test")
                wait()
                assert gamma() == 2
                record(
                    "save-and-export-user-preset-flush-the-new-midpoint-and-preserve-its-gamma"
                )
                # Preview opening and closing must retain the literal gamma and graph.
                before_editor = graph()
                check.menu("View", "Rendering pipeline…")
                dialog = page.locator(".modal.show").filter(
                    has_text="Rendering pipeline"
                )
                expect(
                    dialog.get_by_role("button", name="Apply", exact=True)
                ).to_be_enabled()
                dialog.get_by_role("button", name="Close", exact=True).click()
                wait()
                assert graph() == before_editor
                record(
                    "graph-editor-roundtrip-preserves-gamma-and-does-not-rewrite-an-unchanged-graph"
                )
                pixels = tile().tobytes()
                with page.expect_download() as event:
                    check.menu("File", "Save session…")
                saved = out / "gamma-session.json"
                event.value.save_as(saved)
                doc = json.loads(saved.read_text())
                assert (
                    doc["snapshot"]["visualizationOptions"]["colormap"]["gamma"] == 2
                    and doc["snapshot"]["renderPipeline"]["lowerExpression"]["gamma"]
                    == 0.5
                )
                check.menu("File", "Close map")
                page.get_by_role("heading", name="No contact map open").wait_for()
                with page.expect_file_chooser() as chooser:
                    check.menu("File", "Open session…")
                chooser.value.set_files(saved)
                page.get_by_text(
                    "Session restored. Undo history starts from the restored assembly.",
                    exact=True,
                ).wait_for()
                check.settle()
                wait()
                assert (
                    gamma() == 2
                    and gamma("SECONDARY") == 0.5
                    and tile().tobytes() == pixels
                )
                record(
                    "session-save-and-restore-preserve-midpoints-and-rendered-pixels"
                )
                page.get_by_role("button", name="Reset curve", exact=True).click()
                page.get_by_role("button", name="Focus range", exact=True).click()
                wait()
                editor = page.locator(".curve-editor")
                editor.scroll_into_view_if_needed()
                box = editor.bounding_box()
                points = editor.locator(".curve-point")
                for count, (x, y) in enumerate([(0.22, 0.68), (0.77, 0.18)], 2):
                    editor.dblclick(position={"x": box["width"] * x, "y": box["height"] * y})
                    expect(points).to_have_count(count)
                page.get_by_role("button", name="Apply", exact=True).click()
                wait()
                assert len(options()["colormap"]["curve"]) == 6
                points.nth(1).click(button="right")
                expect(points).to_have_count(2)
                page.get_by_role("slider", name="Curve point 1", exact=True).click(button="right")
                expect(points).to_have_count(1)
                page.get_by_role("button", name="Reset curve", exact=True).click()
                wait()
                assert "curve" not in options()["colormap"]
                record("histogram-points-and-slider-projections-edit-the-rendered-curve")
                page.set_viewport_size({"width": 640, "height": 800})
                check.settle()
                midpoint.scroll_into_view_if_needed()
                key("Home")
                assert 0.05 <= gamma() <= 100
                key("End")
                assert 0.05 <= gamma() <= 100
                record("compact-midpoint-supports-keyboard-endpoints-with-valid-gamma")
                assert not check.errors, check.errors
                assert hashes == {
                    p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                    for p in data.glob("*.hict.hdf5")
                }
                (out / "results.json").write_text(
                    json.dumps(
                        {
                            "status": "ok",
                            "checks": rows,
                            "sourceHashes": hashes,
                            "errors": check.errors,
                        },
                        indent=2,
                    )
                )
            except BaseException:
                page.screenshot(path=str(out / "failure.png"))
                (out / "errors.json").write_text(json.dumps(check.errors))
                (out / "state.json").write_text(
                    json.dumps({"options": options(), "graph": graph()}, indent=2)
                )
                raise
            finally:
                browser.close()
    finally:
        web.shutdown()
        web.server_close()


if __name__ == "__main__":
    main()
