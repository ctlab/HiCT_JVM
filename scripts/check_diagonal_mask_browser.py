#!/usr/bin/env python3
"""Exercise absolute-coordinate diagonal masking in the sidebar, tiles and statistics."""

import argparse
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
import io
import json
from pathlib import Path
import shutil
import threading

from PIL import Image
from playwright.sync_api import expect, sync_playwright

from check_structural_edits_browser import prepare_fixture
from sequence_browser_harness import BrowserCheck, HictTestServer, configure_test_api


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("jar", "webui", "data-dir", "output"):
        parser.add_argument("--" + name, required=True, type=Path)
    args = parser.parse_args()
    data, out = args.data_dir.absolute(), args.output.absolute()
    if any(p != p.resolve() or not p.is_relative_to("/tmp") for p in (data, out)):
        parser.error("Use fresh nonsymlinked /tmp paths")
    data.mkdir()
    out.mkdir()
    prepare_fixture(data, args.jar.resolve())
    shutil.copyfile(data / "structural.hict.hdf5", data / "secondary.hict.hdf5")

    class Quiet(SimpleHTTPRequestHandler):
        def log_message(self, *_):
            pass

    handler = partial(Quiet, directory=args.webui.resolve())
    with (
        HictTestServer(args.jar, data, out / "server.log") as api,
        ThreadingHTTPServer(("127.0.0.1", 0), handler) as web,
        sync_playwright() as pw,
    ):
        threading.Thread(target=web.serve_forever, daemon=True).start()
        browser = pw.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1440, "height": 900})
        page.add_init_script("window.__HICT_BENCHMARK_ENABLED__=true")
        configure_test_api(page, api.url)
        check = BrowserCheck(page, out)
        page.goto(f"http://127.0.0.1:{web.server_port}/")
        page.wait_for_function("window.__hictBenchmark")
        page.evaluate(
            """api => window.__hictBenchmark.open("structural.hict.hdf5", api)""", api.url
        )
        check.settle()

        def tile(row=0, col=0, capture=None, base=False, source="PRIMARY", grid=False):
            params = dict(source=source, bpResolution=100, format="PNG_BY_PIXELS",
                          row=row, col=col, rows=8, cols=8)
            if capture:
                params["renderCapture"] = capture
            if base:
                params["sequenceScale"] = "true"
            if grid:
                params.update(gridBpResolution=200, gridSampleFactor=2)
            reply = page.request.get(api.url + "/get_tile", params=params)
            assert reply.ok, reply.text()
            return Image.open(io.BytesIO(reply.body())).convert("RGBA")

        query = dict(source="PRIMARY", bpResolution=100, signalMode="RAW_COUNTS",
                     startRowPx=0, endRowPx=8, startColPx=0, endColPx=8, format="JSON")

        def post(path, payload=None):
            return json.loads(api.http(path, payload if payload is not None else {}))

        advanced = page.locator("#signal-advanced")
        advanced.locator(":scope > summary").click()
        mask = page.get_by_role("checkbox", name="Ignore values near the diagonal")
        distance = page.get_by_role("textbox", name="Diagonal mask distance")
        expect(mask).not_to_be_checked()
        expect(distance).to_be_disabled()
        before = tile().tobytes()
        outside = tile(0, 1500).tobytes()
        raw = post("/matrix/query", query)["values"]
        before_stats = post("/matrix/statistics", {"bpResolution": 100})
        with page.expect_response("**/set_visualization_options") as changed:
            mask.check()
        assert changed.value.ok, changed.value.text()
        expect(distance).to_be_enabled()
        expect(distance).to_have_value("100,000")
        check.settle()
        assert tile().getchannel("A").getextrema() == (0, 0)
        assert tile(0, 1500).tobytes() == outside
        assert tile(0, 0, base=True).getchannel("A").getextrema() == (0, 0)
        assert post("/matrix/query", query)["values"] == raw
        masked_query = page.request.post(api.url + "/matrix/query", data={
            **query, "respectDiagonalMask": True, "includeTileStatistics": True,
        })
        assert masked_query.ok, masked_query.text()
        assert masked_query.json()["values"] == [None] * 64
        assert json.loads(masked_query.headers["x-hict-tile-statistics"])["raw"]["count"] == 0
        after_stats = post("/matrix/statistics", {"bpResolution": 100})
        assert before_stats["count"] > after_stats["count"]
        capture = post("/visualization/captures")
        assert capture["diagonalMask"] == {"enabled": True, "distanceBp": 100_000}
        assert tile(capture=capture["id"]).getchannel("A").getextrema() == (0, 0)
        post(f'/visualization/captures/{capture["id"]}/release')
        snapshot = post("/session/snapshot")
        assert snapshot["visualizationOptions"]["diagonalMask"] == capture["diagonalMask"]
        page.screenshot(path=str(out / "diagonal-hidden.png"))
        page.get_by_role("button", name="Manage…", exact=True).click()
        preset_editor = page.get_by_role("group", name="Visualization presets", exact=True)
        save = preset_editor.get_by_title("Save applied visualization and complete rendering graph", exact=True)
        save.click()
        expect(save).to_be_enabled()
        with page.expect_download() as download:
            preset_editor.get_by_role("button", name="Export", exact=True).click()
        export_path = out / "presets.json"
        download.value.save_as(export_path)
        saved = json.loads(export_path.read_text())["data"]["savedVisualizationPresets"]
        saved_mask = next(preset for preset in saved if preset.get("options", {}).get("diagonalMask") == capture["diagonalMask"])
        page.get_by_role("button", name="Manage…", exact=True).click()

        distance.fill("0")
        distance.press("Tab")
        expect(page.get_by_text("Enter a distance between 1 bp and 1 Gb.", exact=True)).to_be_visible()
        distance.fill("0.05 Mb")
        with page.expect_response("**/set_visualization_options") as changed:
            distance.press("Tab")
        assert changed.value.ok, changed.value.text()
        expect(distance).to_have_value("50,000")
        with page.expect_response("**/set_visualization_options") as changed:
            mask.uncheck()
        assert changed.value.ok, changed.value.text()
        expect(distance).to_be_disabled()
        assert tile().tobytes() == before
        assert post("/matrix/query", query)["values"] == raw
        page.get_by_role("combobox", name="Visualization preset", exact=True).select_option(label=saved_mask["name"])
        expect(mask).to_be_checked()
        expect(distance).to_have_value("100,000")
        assert tile().getchannel("A").getextrema() == (0, 0)
        post("/secondary/open", {"filename": "secondary.hict.hdf5"})
        assert tile(source="SECONDARY").getchannel("A").getextrema() == (0, 0)
        assert tile(source="SECONDARY", grid=True).getchannel("A").getextrema() == (0, 0)
        post("/reverse_selection_range", {"startBP": 0, "endBP": 400000})
        assert tile().getchannel("A").getextrema() == (0, 0)
        assert not check.errors, check.errors
        (out / "results.json").write_text(json.dumps({
            "beforeStatistics": before_stats, "maskedStatistics": after_stats,
            "capture": capture["diagonalMask"], "rawCellsPreserved": len(raw),
            "savedPresetRestoredMask": True, "secondaryAndProjectedTileMasked": True,
            "restoredIdenticalTile": True, "reversedMaskedTile": True,
        }, indent=2))
        browser.close()
    print("Passed diagonal tiles, absolute off-diagonal cells, statistics, capture, and reverse")


if __name__ == "__main__":
    main()
