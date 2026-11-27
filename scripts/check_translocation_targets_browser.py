#!/usr/bin/env python3
"""Check fitted-view translocation targets and the persistent map zoom option."""

import argparse
from functools import partial
import json
from http.server import ThreadingHTTPServer
from pathlib import Path
import shutil
import tempfile
import threading

from playwright.sync_api import sync_playwright

from check_shift_selection_browser import Quiet
from sequence_browser_harness import BrowserCheck, HictTestServer, configure_test_api


ARROWS = """() => {
  const manager = window.__hictBenchmark.manager(), layers = manager.getLayersManager();
  const map = manager.getMap(), box = map.getViewport().getBoundingClientRect();
  const resolution = layers.currentViewState.resolutionDesciptor.bpResolution;
  const source = layers.layersHolder.bpResolutionToContigTranslocationArrowsLayer.get(resolution).getSource();
  const triangles = [];
  for (const feature of source.getFeatures()) {
    const left = feature.get('leftContigDescriptor')?.contigName ?? null;
    const right = feature.get('rightContigDescriptor')?.contigName ?? null;
    for (const polygon of feature.getGeometry().getPolygons()) {
      const vertices = polygon.getCoordinates()[0].slice(0, 3).map(coordinate => {
        const pixel = map.getPixelFromCoordinate(coordinate);
        return [pixel[0] + box.left, pixel[1] + box.top];
      });
      const x = vertices.map(point => point[0]), y = vertices.map(point => point[1]);
      const centre = [x.reduce((a,b)=>a+b)/3, y.reduce((a,b)=>a+b)/3];
      triangles.push({left, right, centre, width:Math.max(...x)-Math.min(...x),
        height:Math.max(...y)-Math.min(...y), vertices,
        hit:document.elementFromPoint(...centre)?.tagName,
        covering:document.elementFromPoint(...centre)?.outerHTML.slice(0,350)});
    }
  }
  return { triangles, box:[box.left,box.top,box.right,box.bottom] };
}"""


def check_ui(page, check, api_url, output):
    page.wait_for_function("window.__hictBenchmark")
    page.evaluate(
        "api=>window.__hictBenchmark.open('borrelia.hict.hdf5',api)", api_url
    )
    check.settle()
    page.evaluate("window.__hictBenchmark.manager().getLayersManager().fitViewToActiveExtent()")
    check.settle()
    fitted = page.evaluate("""() => {
      const m=window.__hictBenchmark.manager(), l=m.getLayersManager(), h=m.contigDimensionHolder;
      const d=l.currentViewState.resolutionDesciptor, map=m.getMap();
      const c=(h.prefix_sum_bp[2]+h.prefix_sum_bp[3])/2;
      const bp=h.getFractionalPxOfBp(c,d.bpResolution)*d.pixelResolution;
      const [x,y]=map.getPixelFromCoordinate([bp,-bp]);
      const box=map.getViewport().getBoundingClientRect();
      return [x+box.left,y+box.top];
    }""")

    def order():
        return page.evaluate("""() => window.__hictBenchmark.manager()
          .contigDimensionHolder.contigDescriptors.map(c=>c.contigName)""")

    original = order()
    evidence = []
    for edge in ("first", "last"):
        page.keyboard.down("Shift")
        page.mouse.click(*fitted)
        page.keyboard.up("Shift")
        check.settle()
        page.get_by_role("button", name="Enter translocation mode", exact=True).click()
        page.locator(".tl-hint").wait_for()
        check.settle()
        geometry = page.evaluate(ARROWS)
        assert geometry["triangles"]
        x0, y0, x1, y1 = geometry["box"]
        for arrow in geometry["triangles"]:
            assert min(arrow["width"], arrow["height"]) >= 23.99, arrow
            assert max(arrow["width"], arrow["height"]) <= 96.01, arrow
            assert all(x0 - .5 <= x <= x1 + .5 and y0 - .5 <= y <= y1 + .5
                       for x, y in arrow["vertices"]), arrow
        target = next(arrow for arrow in geometry["triangles"]
                      if arrow["left" if edge == "first" else "right"] is None)
        assert target["hit"] == "CANVAS", target
        page.mouse.move(*target["centre"])
        page.locator(".tl-hint").get_by_text("Target:", exact=False).wait_for()
        page.screenshot(path=str(output / f"translocation-{edge}.png"))
        with page.expect_response("**/move_selection_range") as move:
            page.mouse.click(*target["centre"])
        assert move.value.ok, move.value.text()
        check.settle()
        names = order()
        assert names[0 if edge == "first" else -1] == original[2], names
        assert [n for n in names if n != original[2]] == original[:2] + original[3:]
        with page.expect_response("**/assembly/history/restore") as undo:
            page.keyboard.press("Control+z")
        assert undo.value.ok, undo.value.text()
        check.settle()
        assert order() == original
        evidence.append({"edge":edge, "target":target, "triangles":len(geometry["triangles"])})

    page.set_viewport_size({"width":1280, "height":720})
    check.settle()
    for selector in (".ol-zoom", ".ol-zoomslider"):
        control = page.locator(selector).first
        assert control.is_visible()
        box = control.bounding_box()
        viewport = page.locator("#hic-contact-map .ol-viewport").bounding_box()
        assert box["x"] >= viewport["x"] and box["y"] >= viewport["y"]
        assert box["y"] + box["height"] <= viewport["y"] + viewport["height"]
    page.set_viewport_size({"width":1440,"height":900})
    page.get_by_role("link", name="View", exact=True).click()
    toggle = page.get_by_role("menuitemcheckbox", name="Show zoom controls", exact=True)
    assert toggle.get_attribute("aria-checked") == "true"
    page.screenshot(path=str(output / "view-zoom-controls.png"))
    toggle.click()
    page.locator(".ol-zoom").wait_for(state="hidden")
    page.locator(".ol-zoomslider").wait_for(state="hidden")
    page.keyboard.press("Escape")
    page.get_by_role("region", name="Contact map").focus()
    read_resolution = "window.__hictBenchmark.manager().getView().getResolution()"
    initial = page.evaluate(read_resolution)
    page.keyboard.press("+")
    page.wait_for_function(f"before => {read_resolution} < before", arg=initial)
    check.settle()
    zoomed = page.evaluate(read_resolution)
    page.keyboard.press("-")
    page.wait_for_function(f"before => {read_resolution} > before", arg=zoomed)
    check.settle()
    page.reload()
    page.wait_for_function("window.__hictBenchmark")
    check.menu("File","Join running session")
    check.settle()
    assert page.get_by_role("region", name="Contact map").get_attribute("tabindex") == "0"
    assert page.locator(".ol-zoom").count() > 0 and not page.locator(".ol-zoom").first.is_visible()
    assert page.locator(".ol-zoomslider").count() > 0 and not page.locator(".ol-zoomslider").first.is_visible()
    assert not check.errors, check.errors
    (output / "results.json").write_text(json.dumps(evidence, indent=2))
    print("First and last targets clicked and undone. Zoom controls persist and keyboard zoom works.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("jar", "webui", "borrelia", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    with (
        tempfile.TemporaryDirectory(prefix="hict-move-targets-") as temp,
        ThreadingHTTPServer(("127.0.0.1",0), partial(Quiet,directory=args.webui.resolve())) as web,
        sync_playwright() as playwright,
    ):
        data = Path(temp)
        shutil.copyfile(args.borrelia,data / "borrelia.hict.hdf5")
        threading.Thread(target=web.serve_forever,daemon=True).start()
        try:
            with HictTestServer(args.jar,data,args.output / "server.log") as api:
                browser = playwright.chromium.launch()
                page = browser.new_page(viewport={"width":1440,"height":900})
                page.set_default_timeout(20000)
                page.add_init_script("window.__HICT_BENCHMARK_ENABLED__=true")
                configure_test_api(page,api.url)
                page.goto(f"http://127.0.0.1:{web.server_port}/")
                check_ui(page,BrowserCheck(page,args.output),api.url,args.output)
                browser.close()
        finally:
            web.shutdown()


if __name__ == "__main__":
    main()
