#!/usr/bin/env python3
"""Check exact visible selection, rendering, and edit boundaries beside hidden contigs."""

import argparse
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import threading

from playwright.sync_api import sync_playwright

from check_structural_edits_browser import prepare_fixture
from sequence_browser_harness import BrowserCheck, HictTestServer, configure_test_api


RESOLUTIONS = (200_000, 100_000, 10_000)
CONTIGS = {
    "left": 1_000_000,
    "middle": 300_000,
    **{f"short{i:02}": 15_000 for i in range(40)},
    "right": 250_000,
}
MODEL = """() => {
    const m = window.__hictBenchmark.manager(), l = m.viewAndLayersManager;
    const h = m.contigDimensionHolder, d = l.currentViewState.resolutionDesciptor;
    const i = h.contigDescriptors.findIndex(c => c.contigName === 'middle');
    const pixels = h.prefix_sum_px.get(d.bpResolution);
    const [a,b] = [pixels[i], pixels[i+1]].map(px => px * d.pixelResolution);
    const center = (a + b) / 2, extent = [a,-b,b,-a];
    const view = m.getMap().getViewport().getBoundingClientRect();
    const point = m.getMap().getPixelFromCoordinate([center, -center]);
    const track = l.track2DHolder.contigBordersTrack;
    const border = track.features.get(d.bpResolution)?.find(f =>
        f.get('contigDescriptor')?.contigId === h.contigDescriptors[i].contigId);
    return {resolution:d.bpResolution, viewResolution:m.getView().getResolution(),
        extent, border:border?.getGeometry().getExtent(),
        bp:[h.prefix_sum_bp[i],h.prefix_sum_bp[i+1]],
        point:[view.left+point[0], view.top+point[1]],
        selected:l.getSelectedEntityRange(),
        highlight:l.entityHighlightLayer?.getSource().getFeatures().map(f=>f.getGeometry().getExtent()),
        region:l.getRegionBounds(),
        ids:l.currentViewState.selectedContigIds,
        overlapping:h.contigDescriptors.filter((_,order)=>{
            const r=l.getRegionBounds();
            return r && h.prefix_sum_bp[order]<r.columnEnd && h.prefix_sum_bp[order+1]>r.columnStart;
        }).map(c=>c.contigName)};
}"""


class Quiet(SimpleHTTPRequestHandler):
    def log_message(self, *_):
        pass


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
    prepare_fixture(data, args.jar, resolutions=RESOLUTIONS, contigs=CONTIGS)
    (data / "scaffolds.agp").write_text("".join(
        f"scf_{name}\t1\t{length}\t1\tW\t{name}\t1\t{length}\t+\n"
        for name, length in CONTIGS.items()
    ))
    rows = []
    with (
        HictTestServer(args.jar, data, out / "server.log") as api,
        ThreadingHTTPServer(("127.0.0.1", 0), partial(Quiet, directory=args.webui.resolve())) as web,
        sync_playwright() as pw,
    ):
        threading.Thread(target=web.serve_forever, daemon=True).start()
        browser = pw.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1440, "height": 950})
        page.set_default_timeout(30000)
        page.add_init_script("window.__HICT_BENCHMARK_ENABLED__=true")
        configure_test_api(page, api.url)
        check = BrowserCheck(page, out)
        page.goto(f"http://127.0.0.1:{web.server_port}/")
        page.wait_for_function("window.__hictBenchmark")
        page.evaluate("api => window.__hictBenchmark.open('structural.hict.hdf5',api)", api.url)
        check.settle()
        check.menu("Assembly", "Load AGP…")
        check.choose_file("scaffolds.agp")
        with page.expect_response("**/load_agp") as loaded:
            page.get_by_role("button", name="Load", exact=True).click()
        assert loaded.value.ok, loaded.value.text()
        check.settle()
        anchor = page.locator("#map-anchor-lock")
        if anchor.get_attribute("aria-pressed") == "true":
            anchor.click()

        def level(resolution):
            page.evaluate("""async bp => {
                const m=window.__hictBenchmark.manager(), l=m.viewAndLayersManager;
                const resolution=l.getPixelResolutionForBpResolution(bp);
                // The synthetic whole map is only nine pixels wide at 200 kb.
                // Permit its native tile scales even on the large desktop viewport.
                l.exportMaxResolution=resolution;
                l.updateZoomLimit();
                await new Promise(resolve => m.getView().animate({
                    resolution,duration:100},resolve));
                await l.onViewResolutionChanged();
                const h=m.contigDimensionHolder, d=l.currentViewState.resolutionDesciptor;
                const i=h.contigDescriptors.findIndex(c=>c.contigName==='middle');
                const px=h.prefix_sum_px.get(bp);
                const c=(px[i]+px[i+1])*d.pixelResolution/2;
                m.getView().setCenter([c,-c]);
                m.getMap().renderSync();
            }""", resolution)
            check.settle()
            state = page.evaluate(MODEL)
            assert state["resolution"] == resolution, state

        def select():
            page.evaluate("window.__hictBenchmark.manager().viewAndLayersManager.clearSelections()")
            current = page.evaluate(MODEL)
            page.keyboard.down("Shift")
            page.mouse.click(*current["point"])
            page.keyboard.up("Shift")
            page.wait_for_function("window.__hictBenchmark.manager().viewAndLayersManager.getSelectedEntityRange()")
            state = page.evaluate(MODEL)
            start, end = state["bp"]
            assert state["selected"] == {"startBP": start, "endBP": end}, state
            assert len(state["ids"]) == 1 and len(state["highlight"]) == 1, state
            assert state["border"] == state["extent"], state
            tolerance = state["viewResolution"]
            assert all(abs(a-b) <= tolerance for a, b in zip(state["highlight"][0], state["border"])), state
            return state

        def draw_exact_region(state):
            # Mouse coordinates are rounded to screen pixels. Send the exact border
            # through OpenLayers' extentchanged event, as a finished gesture does.
            page.evaluate("""extent => window.__hictBenchmark.manager().viewAndLayersManager
                .selectionInteractions.contigSelectExtent.setExtent(extent)""", state["extent"])
            actual = page.evaluate(MODEL)
            start, end = actual["bp"]
            assert actual["region"] == dict(rowStart=start, rowEnd=end, columnStart=start, columnEnd=end), actual
            assert actual["overlapping"] == ["middle"], actual

        def agp():
            return api.http("/get_agp_for_assembly", {}).decode("utf-8").splitlines()

        def undo(expected):
            with page.expect_response("**/assembly/history/restore") as response:
                page.get_by_role("button", name="Undo", exact=True).click()
            assert response.value.ok, response.value.text()
            check.settle()
            assert agp() == expected

        for resolution in RESOLUTIONS:
            level(resolution)
            before = agp()
            state = select()
            page.screenshot(path=str(out / f"{resolution}-selection.png"))
            draw_exact_region(state)
            page.mouse.click(*page.evaluate(MODEL)["point"], button="right")
            menu = page.get_by_role("menu", name="Assembly actions")
            menu.wait_for()
            scaffold = menu.locator('[data-submenu="scaffold"]')
            entity = scaffold if scaffold.count() else menu.locator('[data-submenu="contig"]')
            entity.click()
            with page.expect_response("**/reverse_selection_range") as response:
                page.get_by_role("menuitem", name="Reverse", exact=True).click()
            assert response.value.ok, response.value.text()
            payload = response.value.request.post_data_json
            assert [payload["startBP"], payload["endBP"]] == state["bp"], payload
            check.settle()
            after = agp()
            changed = [(a.split("\t"), b.split("\t")) for a, b in zip(before, after) if a != b]
            assert len(after) == len(before) and len(changed) == 1, changed
            old, new = changed[0]
            assert old[5] == "middle" and old[:-1] == new[:-1] and [old[-1], new[-1]] == ["+", "-"], changed
            # Verify the highlight and region after reloading a reversed contig.
            level(resolution)
            draw_exact_region(select())
            rows.append(dict(resolution=resolution, bounds=state["bp"], reverseExact=True))
            undo(before)

        # Move the last visible contig to the assembly start. The hidden run is
        # now at the end, so a right-edge selection must still end at middle.
        level(RESOLUTIONS[0])
        before = agp()
        page.evaluate("""() => {
            const m=window.__hictBenchmark.manager(),l=m.viewAndLayersManager,h=m.contigDimensionHolder;
            const i=h.contigDescriptors.findIndex(c=>c.contigName==='right');
            l.selectEntityRangeByBp(h.prefix_sum_bp[i],h.prefix_sum_bp[i+1]);
            l.fitViewToActiveExtent();
        }""")
        page.get_by_role("button", name="Enter translocation mode", exact=True).click()
        target = page.evaluate("""() => {
            const m=window.__hictBenchmark.manager(),l=m.viewAndLayersManager;
            const r=l.currentViewState.resolutionDesciptor.bpResolution;
            const arrow=l.layersHolder.bpResolutionToContigTranslocationArrowsLayer.get(r)
                .getSource().getFeatures().find(f=>!f.get('leftContigDescriptor'));
            const p=m.getMap().getPixelFromCoordinate(arrow.getGeometry().getInteriorPoints().getCoordinates()[0]);
            const box=m.getMap().getViewport().getBoundingClientRect();
            return [box.left+p[0],box.top+p[1]];
        }""")
        with page.expect_response("**/move_selection_range") as moved:
            page.mouse.move(*target)
            page.mouse.click(*target)
        assert moved.value.ok, moved.value.text()
        assert moved.value.request.post_data_json["targetStartBP"] == 0
        check.settle()
        assert next(line for line in agp() if not line.startswith("#")).split("\t")[5] == "right"
        level(RESOLUTIONS[0])
        draw_exact_region(select())
        undo(before)
        assert not check.errors, check.errors
        browser.close()
    (out / "results.json").write_text(json.dumps({"checks": rows, "moveToStartAndUndo": True}, indent=2))
    print("Passed hidden-contig highlights, exact region edits and move/undo at native resolutions")


if __name__ == "__main__":
    main()
