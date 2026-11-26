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
from sequence_browser_harness import BrowserCheck, HictTestServer, configure_test_api


BED = """one	0	85000	Clipped	0	-
one	100000	240000	Transcript	0	+	115000	226000	0	2	40000,50000	0,90000
one	255000	315000	Reverse	0	-
track name=custom color="10, 110, 140"
one	325000	350000	Track-color	0	+
one	360000	390000	Item-color	0	+	360000	390000	0,140,0
"""

LABEL_BEDS = {
    "strands-nocolor.bed": "".join(
        f"Ch1\t{start}\t{start + size}\t{label}{i}\t0\t{strand}\n"
        for label, strand, intervals in [
            ("plus", "+", [(20000, 60000), (100000, 6000), (130000, 900)]),
            ("minus", "-", [(160000, 60000), (240000, 6000), (270000, 900)]),
        ]
        for i, (start, size) in enumerate(intervals)
    ),
    "strands-itemrgb.bed": """Ch1	20000	80000	greenPlus	0	+	20000	80000	0,140,0
Ch1	160000	220000	yellowMinus	0	-	160000	220000	240,220,40
Ch1	300000	360000	blackPlus	0	+	300000	360000	10,10,10
""",
    "strands-bed12.bed": """Ch1	20000	120000	genePlus	0	+	25000	115000	0	3	20000,10000,15000,	0,40000,85000,
Ch1	160000	260000	geneMinus	0	-	165000	255000	0	3	20000,10000,15000,	0,40000,85000,
""",
}


def check_label_bounds(page):
    return page.evaluate(
        """async files => {
            const manager = window.__hictBenchmark.manager().linearTrackManager;
            for (const file of files) await manager.openTrack(file);
            await document.fonts.ready;
            const bounds = (ctx, x0, y0, x1, y1) => {
                const transform = ctx.getTransform();
                const corners = [[x0,y0], [x0,y1], [x1,y0], [x1,y1]]
                    .map(([x,y]) => transform.transformPoint({x,y}));
                return {
                    left: Math.min(...corners.map(point => point.x)),
                    right: Math.max(...corners.map(point => point.x)),
                    top: Math.min(...corners.map(point => point.y)),
                    bottom: Math.max(...corners.map(point => point.y)),
                };
            };
            const intersects = (a,b) => a.left < b.right && b.left < a.right
                && a.top < b.bottom && b.top < a.bottom;
            const drawBin = manager.drawFeatureBin;
            const drawLabel = manager.drawFeatureLabel;
            const outlinedText = manager.drawOutlinedText;
            const reports = {};
            try {
                for (const orientation of ["horizontal", "vertical"]) {
                    const blocks = new Map();
                    const labels = [];
                    const isExport = ctx => orientation === "horizontal"
                        ? ctx.canvas.width === 400 && ctx.canvas.height === 240
                        : ctx.canvas.width === 240 && ctx.canvas.height === 400;
                    manager.drawFeatureBin = function(ctx, axis, bin, ...args) {
                        if (!isExport(ctx)) return drawBin.call(this, ctx, axis, bin, ...args);
                        const fillRect = ctx.fillRect;
                        const rectangles = [];
                        ctx.fillRect = function(x, y, width, height) {
                            rectangles.push(bounds(this, x, y, x + width, y + height));
                            return fillRect.call(this, x, y, width, height);
                        };
                        try {
                            return drawBin.call(this, ctx, axis, bin, ...args);
                        } finally {
                            delete ctx.fillRect;
                            blocks.set(bin.label, rectangles);
                        }
                    };
                    manager.drawFeatureLabel = function(ctx, ...args) {
                        if (!isExport(ctx)) return drawLabel.call(this, ctx, ...args);
                        this.drawOutlinedText = function(ctx, label, x, y, palette) {
                            const m = ctx.measureText(label);
                            const outline = 1.5;
                            labels.push({label, box: bounds(ctx,
                                x - m.actualBoundingBoxLeft - outline,
                                y - m.actualBoundingBoxAscent - outline,
                                x + m.actualBoundingBoxRight + outline,
                                y + m.actualBoundingBoxDescent + outline)});
                            return outlinedText.call(this, ctx, label, x, y, palette);
                        };
                        try {
                            return drawLabel.call(this, ctx, ...args);
                        } finally {
                            this.drawOutlinedText = outlinedText;
                        }
                    };
                    await manager.renderFullExtentCanvasForExport(orientation,
                        {bpResolution:1000, startPx:0, endPx:400, trackPanelSizePx:240});
                    reports[orientation] = {
                        features: [...blocks.keys()],
                        labels: labels.map(({label}) => label),
                        overlaps: labels.filter(({label, box}) =>
                            blocks.get(label)?.some(block => intersects(box, block))),
                    };
                }
            } finally {
                manager.drawFeatureBin = drawBin;
                manager.drawFeatureLabel = drawLabel;
            }
            return reports;
        }""",
        list(LABEL_BEDS),
    )


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
    prepare_fixture(data, args.jar.resolve(), first_contig="Ch1", resolutions=(100, 200, 1000))
    (data / "strands.bed").write_text(BED.replace("one\t", "Ch1\t"))
    for filename, contents in LABEL_BEDS.items():
        (data / filename).write_text(contents)

    class Quiet(SimpleHTTPRequestHandler):
        def log_message(self, *_):
            pass

    handler = partial(Quiet, directory=args.webui.resolve())
    reports = []
    label_reports = []
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
            configure_test_api(page, api.url)
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
                    const requests = window.__hictBenchmark.manager().networkManager.requestManager;
                    // Keep the original feature endpoints while clipping the render window.
                    const full = manager.refineTrackResponsePx(
                        await requests.queryTracks1D(0, 4000, 4000, 100));
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
                        const clipCanvas = document.createElement("canvas");
                        clipCanvas.width = canvas.width;
                        clipCanvas.height = canvas.height;
                        manager.drawTrackCanvas(clipCanvas, orientation, full, {
                            startBp:50000, endBp:400000, startPx:500, endPx:4000,
                            bpResolution:100, visibleWidthPx:3500,
                            scopeStartPx:500, scopeEndPx:4000, pxToScreen:px=>px-500
                        }, undefined, false);
                        const clippedContext = clipCanvas.getContext("2d");
                        const clipped = Array.from({length:12}, (_,axis) =>
                            Array.from({length:24}, (_,off) => {
                                const cross = 18 + off;
                                return matches(Array.from(clippedContext.getImageData(
                                    orientation === "horizontal" ? axis : cross,
                                    orientation === "horizontal" ? cross : axis, 1, 1
                                ).data).slice(0,3), blue);
                            }).filter(Boolean).length
                        ).some(painted => painted > 0 && painted < 18);
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
            label_reports.append(check_label_bounds(page))
            page.close()
        browser.close()
    (out / "results.json").write_text(json.dumps(reports, indent=2))
    (out / "label-bounds.json").write_text(json.dumps(label_reports, indent=2))
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
    expected_features = {line.split("\t")[3] for bed in LABEL_BEDS.values() for line in bed.splitlines()}
    for report in label_reports:
        for orientation, result in report.items():
            assert expected_features <= set(result["features"]), (orientation, result)
            assert {"yellowMinus", "blackPlus"} <= set(result["labels"]), (orientation, result)
            assert not result["overlaps"], (orientation, result["overlaps"])
    print("Passed BED projection, styled track export and non-overlapping labels on both axes")


if __name__ == "__main__":
    main()
