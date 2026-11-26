#!/usr/bin/env python3
"""Exercise the single Shift-selection rule with real gestures and assembly edits."""

import argparse
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import re
import shutil
import tempfile
import threading

from playwright.sync_api import sync_playwright

from sequence_browser_harness import BrowserCheck, HictTestServer, configure_test_api


class Quiet(SimpleHTTPRequestHandler):
    def log_message(self, *_):
        pass


MODEL = """() => {
    const m = window.__hictBenchmark.manager(), h = m.contigDimensionHolder;
    const l = m.viewAndLayersManager, s = l.currentViewState;
    const extent = l.entityHighlightLayer.getSource().getExtent().slice();
    return {
        rows: h.contigDescriptors.map((c, i) => ({
            name: c.contigName, id: c.contigId, direction: c.direction,
            start: h.prefix_sum_bp[i], end: h.prefix_sum_bp[i+1],
        })),
        ids: s.selectedContigIds, scaffolds: s.selectedScaffoldIds,
        range: l.getSelectedEntityRange(), highlight: extent, status: s.entitySelectionStatus,
        mode: s.entitySelectionKind
    };
}"""


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("jar", "webui", "borrelia", "orlando", "output"):
        parser.add_argument("--" + name, required=True, type=Path)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    with (
        tempfile.TemporaryDirectory(prefix="hict-shift-selection-") as temp,
        ThreadingHTTPServer(("127.0.0.1", 0), partial(Quiet, directory=args.webui.resolve())) as web,
        sync_playwright() as playwright,
    ):
        data = Path(temp)
        for source in (args.borrelia, args.orlando):
            shutil.copyfile(source, data / source.name)
        worker = threading.Thread(target=web.serve_forever, daemon=True)
        worker.start()
        try:
            with HictTestServer(args.jar, data, args.output / "server.log") as api:
                browser = playwright.chromium.launch(headless=True)
                page = browser.new_page(viewport={"width": 1440, "height": 900})
                page.set_default_timeout(20000)
                page.add_init_script("window.__HICT_BENCHMARK_ENABLED__=true")
                configure_test_api(page, api.url)
                check = BrowserCheck(page, args.output)
                test(page, check, f"http://127.0.0.1:{web.server_port}/", args)
                browser.close()
        finally:
            web.shutdown()


def test(page, check, url, args):
    page.goto(url)
    page.wait_for_function("window.__hictBenchmark")
    records = []

    def open_map(filename, *, discard=False):
        check.menu("File", "Open map…")
        check.choose_file(filename)
        with page.expect_response("**/open") as opening:
            page.get_by_role("button", name="Load", exact=True).click()
            if discard:
                page.get_by_role("alertdialog", name="Unsaved assembly edits").get_by_role(
                    "button", name="Discard edits", exact=True
                ).click()
        assert opening.value.ok, opening.value.text()
        page.wait_for_function("window.__hictBenchmark.manager()")
        check.settle()
        page.evaluate("window.__hictBenchmark.manager().viewAndLayersManager.fitViewToActiveExtent()")
        check.settle()

    def model():
        return page.evaluate(MODEL)

    def middle(row):
        return (row["start"] + row["end"]) / 2

    def point(x, y=None):
        return page.evaluate("""([x,y]) => {
            const m=window.__hictBenchmark.manager(), l=m.viewAndLayersManager;
            const d=l.currentViewState.resolutionDesciptor, h=m.contigDimensionHolder;
            const view=m.getMap().getViewport().getBoundingClientRect();
            const p=m.getMap().getPixelFromCoordinate([
                h.getFractionalPxOfBp(x,d.bpResolution)*d.pixelResolution,
                -h.getFractionalPxOfBp(y,d.bpResolution)*d.pixelResolution]);
            return [p[0]+view.left,p[1]+view.top];
        }""", [x, x if y is None else y])

    def shift_click(row):
        page.keyboard.down("Shift")
        try:
            page.mouse.click(*point(middle(row)))
        finally:
            page.keyboard.up("Shift")
        check.settle()

    def drag(start, end):
        page.keyboard.down("Shift")
        try:
            page.mouse.move(*start)
            page.mouse.down()
            page.mouse.move(*end, steps=14)
            page.mouse.up()
        finally:
            page.keyboard.up("Shift")
        check.settle()

    def selection(rows, scaffolds):
        current = model()
        assert current["mode"] == "scaffold", current
        assert current["ids"] == [row["id"] for row in rows], current
        assert len(current["scaffolds"]) == scaffolds, current
        assert current["range"] == {"startBP": rows[0]["start"], "endBP": rows[-1]["end"]}, current
        assert current["highlight"] == page.evaluate("""([a,b]) => {
            const m=window.__hictBenchmark.manager(), l=m.viewAndLayersManager;
            const d=l.currentViewState.resolutionDesciptor, h=m.contigDimensionHolder;
            const x=h.getFractionalPxOfBp(a,d.bpResolution)*d.pixelResolution;
            const y=h.getFractionalPxOfBp(b,d.bpResolution)*d.pixelResolution;
            return [x,-y,y,-x];
        }""", [rows[0]["start"], rows[-1]["end"]]), current

    def button(name, endpoint):
        with page.expect_response(f"**/{endpoint}") as response:
            page.get_by_role("button", name=name, exact=True).click()
        assert response.value.ok, response.value.text()
        check.settle()

    def history(key):
        with page.expect_response("**/assembly/history/restore") as response:
            page.keyboard.press(key)
        assert response.value.ok, response.value.text()
        check.settle()

    open_map(args.borrelia.name)
    anchor = page.locator("#map-anchor-lock")
    if anchor.get_attribute("aria-pressed") == "true":
        anchor.click()
    page.evaluate("""async () => {
        const m=window.__hictBenchmark.manager(), l=m.viewAndLayersManager;
        const view=m.getView(), extent=l.getActiveMapExtent();
        const resolution=view.getResolution()*1.25;
        l.exportMaxResolution=resolution;
        l.updateZoomLimit();
        view.setResolution(resolution);
        view.setCenter([(extent[0]+extent[2])/2,(extent[1]+extent[3])/2]);
        await l.onViewResolutionChanged();
    }""")
    check.settle()
    rows = model()["rows"]
    assert [row["name"] for row in rows[:4]] == ["Ch1", "cp26", "cp32-1", "cp32-3"]
    shift_click(rows[0])
    selection(rows[:1], 0)
    start = point(rows[0]["start"])
    drag([start[0] - 8, start[1] - 8], point(middle(rows[1])))
    selection(rows[:2], 0)
    end = point(rows[-1]["end"])
    drag(point(middle(rows[-2])), [end[0] + 8, end[1] + 8])
    selection(rows[-2:], 0)
    drag(point(middle(rows[1])), point(middle(rows[3])))
    selection(rows[1:4], 0)
    button("Group selection into scaffold", "group_contigs_into_scaffold")
    selection(rows[1:4], 1)
    shift_click(rows[2])
    selection(rows[1:4], 1)
    page.locator(".selection-label").click()
    page.get_by_text("To change one contig inside a scaffold, right-click it.", exact=True).wait_for()
    page.screenshot(path=str(args.output / "selection-dropdown.png"))
    page.keyboard.press("Escape")
    drag(point(middle(rows[0])), point(middle(rows[2])))
    selection(rows[:4], 1)
    drag(point(middle(rows[2])), point(middle(rows[0])))
    selection(rows[:4], 1)
    border = point(rows[4]["start"])
    drag(point(middle(rows[1])), [border[0] + 2, border[1] + 2])
    selection(rows[1:4], 1)
    drag(point(middle(rows[0]), middle(rows[1])), point(rows[0]["end"] - 10, middle(rows[2])))
    assert model()["ids"] == [], model()
    page.locator(".selection-label").click()
    page.get_by_text("Shift-drag across the diagonal to select contigs", exact=True).wait_for()
    page.keyboard.press("Escape")
    records.append("same-unit click, forward and backward drag, border tolerance and off-diagonal status")

    shift_click(rows[2])
    button("Reverse Selection", "reverse_selection_range")
    reversed_rows = model()["rows"]
    assert [r["id"] for r in reversed_rows[1:4]] == [r["id"] for r in reversed(rows[1:4])]
    selection(reversed_rows[1:4], 1)
    history("Control+z")
    assert model()["rows"] == rows
    selection(rows[1:4], 1)
    history("Control+Shift+z")
    assert model()["rows"] == reversed_rows
    selection(reversed_rows[1:4], 1)
    history("Control+z")
    records.append("group, reverse, Undo and Redo keep the selected contigs")

    shift_click(rows[0])
    with page.expect_response("**/split_contig_at_bp") as cut:
        page.get_by_role("button", name=re.compile("^Split contig")).click()
        page.mouse.dblclick(*point(middle(rows[0])), delay=80)
    assert cut.value.ok, cut.value.text()
    check.settle()
    assert model()["ids"] == []
    history("Control+z")
    shift_click(rows[4])
    button("Move to unplaced (debris)…", "move_selection_to_debris")
    assert model()["ids"] == []
    history("Control+z")
    records.append("scissors and Move to unplaced clear the selection")
    print(records[-1], flush=True)

    page.mouse.click(*point(middle(rows[2])), button="right")
    menu = page.get_by_role("menu", name="Assembly actions")
    menu.wait_for()
    contigs = menu.locator('[data-submenu="contig"]')
    if contigs.count():
        contigs.click()
    menu.get_by_role("menuitem", name="Move…", exact=True).click()
    selection(rows[2:3], 0)
    target = page.evaluate("""() => {
        const m=window.__hictBenchmark.manager(), l=m.viewAndLayersManager;
        const d=l.currentViewState.resolutionDesciptor;
        const arrows=l.layersHolder.bpResolutionToContigTranslocationArrowsLayer.get(d.bpResolution)
          .getSource().getFeatures().filter(f => f.get('leftContigDescriptor') && f.get('rightContigDescriptor'));
        return arrows.map(f => {
          const p=m.getMap().getPixelFromCoordinate(f.getGeometry().getInteriorPoints().getCoordinates()[0]);
          const box=m.getMap().getViewport().getBoundingClientRect();
          return { point:[box.left+p[0],box.top+p[1]], id:f.get('leftContigDescriptor').contigId };
        });
    }""")
    destination = next(item["point"] for item in target if item["id"] == rows[0]["id"])
    with page.expect_response("**/move_selection_range") as move:
        page.mouse.move(*destination)
        page.mouse.click(*destination)
    assert move.value.ok, move.value.text()
    assert move.value.request.post_data_json["startBP"] == rows[2]["start"]
    assert move.value.request.post_data_json["endBP"] == rows[2]["end"]
    assert move.value.request.post_data_json["exactContig"] is True
    check.settle()
    moved_rows = model()["rows"]
    assert [row["id"] for row in moved_rows] == [
        row["id"] for row in [rows[0], rows[2], rows[1], *rows[3:]]
    ]
    selection(moved_rows[1:2], 0)
    history("Control+z")
    assert model()["rows"] == rows
    records.append("per-contig context action moves the exact contig interval")

    open_map(args.orlando.name, discard=True)
    chr_x = next(row for row in model()["rows"] if row["name"] == "chrX")
    assert (chr_x["start"], chr_x["end"]) == (184935303, 202451527)
    shift_click(chr_x)
    selection([chr_x], len(model()["scaffolds"]))
    assert page.evaluate("""() => window.__hictBenchmark.manager().viewAndLayersManager
        .currentViewState.resolutionDesciptor.bpResolution""") == 200000
    page.screenshot(path=str(args.output / "orlando-chrX.png"))
    records.append("Orlando chrX selection and its highlight match the exact M2 boundaries")
    assert not check.errors, check.errors
    (args.output / "results.json").write_text(json.dumps(records, indent=2))
    print("\n".join(records))


if __name__ == "__main__":
    main()
