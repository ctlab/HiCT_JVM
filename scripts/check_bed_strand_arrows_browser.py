#!/usr/bin/env python3
"""Check strand colors and transcript geometry in live and exported track canvases."""

import argparse
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import threading

from playwright.sync_api import sync_playwright

from check_structural_edits_browser import prepare_fixture
from sequence_browser_harness import BrowserCheck, HictTestServer


BED = """one	0	85000	Clipped	0	-
one	100000	240000	Transcript	0	+	115000	226000	0	2	40000,50000	0,90000
one	255000	315000	Reverse	0	-
track name=custom color="10, 110, 140"
one	325000	350000	Track-color	0	+
one	360000	390000	Item-color	0	+	360000	390000	0,140,0
"""


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
    (data / "strands.bed").write_text(BED)

    class Quiet(SimpleHTTPRequestHandler):
        def log_message(self, *_):
            pass

    handler = partial(Quiet, directory=args.webui.resolve())
    reports = []
    with (
        HictTestServer(args.jar, data, out / "server.log") as api,
        ThreadingHTTPServer(("127.0.0.1", 0), handler) as web,
        sync_playwright() as pw,
    ):
        threading.Thread(target=web.serve_forever, daemon=True).start()
        browser = pw.chromium.launch(headless=True)
        for dpr in (1, 2):
            page = browser.new_page(
                viewport={"width": 1440, "height": 900}, device_scale_factor=dpr
            )
            page.add_init_script("window.__HICT_BENCHMARK_ENABLED__=true")
            page.route(
                "http://localhost:5000/**",
                lambda route: route.continue_(
                    url=api.url + route.request.url.removeprefix("http://localhost:5000")
                ),
            )
            check = BrowserCheck(page, out)
            page.goto(f"http://127.0.0.1:{web.server_port}/")
            page.wait_for_function("window.__hictBenchmark")
            page.evaluate(
                """async api => {
                    await window.__hictBenchmark.open("structural.hict.hdf5", api);
                    const m = window.__hictBenchmark.manager();
                    await m.linearTrackManager.openTrack("strands.bed");
                    m.viewAndLayersManager.restoreViewPosition({
                        centerBp:[200000,200000],
                        resolution:4000/m.getMap().getSize()[1], rotation:0
                    });
                }""",
                api.url,
            )
            check.settle()
            reports.append(page.evaluate(
                """async () => {
                    const manager = window.__hictBenchmark.manager().linearTrackManager;
                    const reports = {};
                    const matches = (a, b) => a.every((value, i) => value === b[i]);
                    for (const orientation of ["horizontal", "vertical"]) {
                        const live = document.querySelector(
                            `#${orientation}-igv-track-div canvas`);
                        const liveCanvas = live.getContext("2d");
                        const livePixels = liveCanvas.getImageData(0,0,live.width,live.height).data;
                        let liveRed = 0, liveBlue = 0;
                        for (let i = 0; i < livePixels.length; i += 4) {
                            const r = livePixels[i], g = livePixels[i+1], b = livePixels[i+2];
                            if (r === 217 && g === 45 && b === 32) liveRed++;
                            if (r === 29 && g === 78 && b === 216) liveBlue++;
                        }
                        const canvas = await manager.renderFullExtentCanvasForExport(orientation,
                            {bpResolution:100, startPx:500, endPx:4000});
                        const context = canvas.getContext("2d");
                        const red = [217,45,32], blue = [29,78,216];
                        const sample = (axis,cross) => Array.from(context.getImageData(
                            orientation === "horizontal" ? axis : cross,
                            orientation === "horizontal" ? cross : axis, 1, 1
                        ).data).slice(0,3);
                        const count = (axis, color) => Array.from({length:24},
                            (_, offset) => matches(sample(axis,18+offset), color)).filter(Boolean).length;
                        const intronRedOutsideCenter = [];
                        for (let axis=901; axis<1400; axis++) {
                            if (Array.from({length:24}, (_,off)=>off+18).some(cross =>
                                cross!==30 && (([r,g,b]) => r>170 && g<170 && r-b>75)(
                                    sample(axis,cross)))) {
                                intronRedOutsideCenter.push(axis);
                            }
                        }
                        const arrowGroups = intronRedOutsideCenter.filter((axis, i, all) =>
                            !i || axis > all[i-1] + 2).length;
                        const clipped = Array.from({length:10}, (_,axis) =>
                            count(axis,blue)).some(painted => painted > 0 && painted < 18);
                        reports[orientation] = {
                            coding:count(800,red), utr:count(560,red), intron:count(925,red),
                            reverse:count(2150,blue), header:count(2820,[10,110,140]),
                            item:count(3160,[0,140,0]), arrowGroups, clipped, liveRed, liveBlue
                        };
                    }
                    return reports;
                }"""
            ))
            assert not check.errors, check.errors
            page.screenshot(path=str(out / f"strand-tracks-{dpr}x.png"))
            page.close()
        browser.close()
    (out / "results.json").write_text(json.dumps(reports, indent=2))
    for report in reports:
        for geometry in report.values():
            assert geometry["coding"] == 18, geometry
            assert geometry["utr"] == 9, geometry
            assert geometry["intron"] == 1, geometry
            assert geometry["reverse"] == 18, geometry
            assert geometry["header"] == 18, geometry
            assert geometry["item"] == 18, geometry
            assert geometry["arrowGroups"] == 3, geometry
            assert geometry["clipped"], geometry
            assert geometry["liveRed"] > 100 and geometry["liveBlue"] > 100, geometry
    print("Passed BED projection and styled track export at both device pixel ratios")


if __name__ == "__main__":
    main()
