#!/usr/bin/env python3
"""Real rotated quad-overlay export regression; run on a dedicated tmp-copy server."""
import argparse
import json
import math
import subprocess
import xml.etree.ElementTree as ET
from pathlib import Path

import numpy as np
from PIL import Image
from playwright.sync_api import sync_playwright


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:8187")
    parser.add_argument("--api", default="http://127.0.0.1:5107")
    parser.add_argument("--data-dir", required=True)
    parser.add_argument("--output", default="/tmp/hict-release-physical/rotated-export")
    args = parser.parse_args()
    data = Path(args.data_dir).absolute()
    if data != data.resolve() or not data.is_relative_to("/tmp"):
        raise ValueError("Use a dedicated nonsymlinked DATA directory under /tmp")
    for name in ("combined_ind2_4DN.hict.hdf5", "ind2.self.hict.hdf5"):
        path = data / name
        if not path.is_file() or path.is_symlink() or path.stat().st_nlink != 1:
            raise ValueError(f"Use physical input copies: {path}")
    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=True)
    errors, results = [], []
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1440, "height": 1000})
        page.set_default_timeout(120000)
        page.on("pageerror", lambda error: errors.append(str(error)))
        page.add_init_script("window.__HICT_BENCHMARK_ENABLED__=true")
        page.route("http://localhost:5000/**", lambda route: route.continue_(
            url=args.api + route.request.url.removeprefix("http://localhost:5000")))
        try:
            page.goto(args.url)
            page.wait_for_function("window.__hictBenchmark")
            page.evaluate("api=>window.__hictBenchmark.open('combined_ind2_4DN.hict.hdf5',api)", args.api)
            page.wait_for_timeout(2000)
            button = page.get_by_role("button", name="Do not use Cooler weights", exact=True)
            if button.is_visible():
                button.click()
            page.evaluate("""async () => {
                const m=window.__hictBenchmark.manager(),api=m.networkManager.requestManager;
                const status=await api.openSecondarySource('ind2.self.hict.hdf5',true);
                m.viewAndLayersManager.setSecondaryResolutionModel(status.compatibility);
                const color=(s,c,max)=>({type:'colormap',mode:'LINEAR',input:{type:'source',source:s},
                    startColor:'#ffffff00',endColor:c,minSignal:0,maxSignal:max});
                const overlay={type:'pixel_blend',mode:'OVER',top:color('SECONDARY','#000000ff',20),
                    bottom:color('PRIMARY','#ff0000ff',100),topOpacity:0.65,bottomOpacity:1};
                await api.setRenderPipelineConfig({enabled:true,swapUpperLower:false,
                    upperExpression:overlay,lowerExpression:overlay});
                await m.reloadTilesFromBackend();m.viewAndLayersManager.fitViewToActiveExtent();
            }""")
            for angle, scope in ((90, "viewport"), (45, "viewport"), (180, "viewport"), (45, "whole-map")):
                page.evaluate("angle=>{const m=window.__hictBenchmark.manager();m.getView().setRotation(angle*Math.PI/180);m.getMap().renderSync()}", angle)
                page.wait_for_timeout(1000)
                before = page.evaluate("""() => {const m=window.__hictBenchmark.manager(),v=m.getView();
                    return [v.getCenter(),v.getResolution(),v.getRotation()];}""")
                geometry = page.evaluate("""() => {
                    const m=window.__hictBenchmark.manager(),l=m.viewAndLayersManager;
                    const d=l.currentViewState.resolutionDesciptor,b=l.getActiveMapPixelBounds(d.bpResolution),s=d.pixelResolution;
                    const corners=[[b.colStartPx,-b.rowStartPx],[b.colEndPx,-b.rowStartPx],
                        [b.colEndPx,-b.rowEndPx],[b.colStartPx,-b.rowEndPx]].map(p=>m.getMap().getPixelFromCoordinate(p.map(n=>n*s)));
                    const size=m.getMap().getSize(),r=m.getMap().getViewport().getBoundingClientRect(),w=document.querySelector('.interactive-workspace').getBoundingClientRect();
                    return {corners,size,margin:[r.left-w.left,r.top-w.top]};
                }""")
                stem = f"{scope}-{angle}"
                page.screenshot(path=str(out / f"{stem}-workspace.png"))
                for fmt in ("Svg", "Png", "Pdf"):
                    with page.expect_download() as download:
                        page.evaluate("""async ({fmt,scope})=>{window.exportProgress=[];
                            await window.__hictBenchmark.manager()['exportCurrentMap'+fmt](
                                value=>window.exportProgress.push(value),{scope,cropToMap:true});}""", {"fmt": fmt, "scope": scope})
                    path = out / f"{stem}.{fmt.lower()}"
                    download.value.save_as(path)
                    after = page.evaluate("""() => {const v=window.__hictBenchmark.manager().getView();
                        return [v.getCenter(),v.getResolution(),v.getRotation()];}""")
                    assert before == after, (before, after)
                    progress = page.evaluate("window.exportProgress")
                    assert progress[0] > 0 and progress[-1] == 1, progress
                    assert path.stat().st_size > 1000, path
                svg = ET.parse(out / f"{stem}.svg").getroot()
                dimensions = [float(svg.get(key)) for key in ("width", "height")]
                if scope == "viewport":
                    # These centered views intersect each viewport extremum of
                    # the map's bounding box; unrelated clipping cases are unit-tested.
                    expected = [geometry["margin"][axis] + min(geometry["size"][axis],
                        max(p[axis] for p in geometry["corners"])) - max(0,
                        min(p[axis] for p in geometry["corners"])) for axis in (0, 1)]
                    assert np.allclose(dimensions, expected), (dimensions, expected)
                image = Image.open(out / f"{stem}.png").convert("RGB")
                assert image.size == tuple(math.ceil(value) for value in dimensions)
                pixels = np.asarray(image)
                red = ((pixels[:, :, 0].astype(int) - pixels[:, :, 1]) > 40).sum()
                assert red > 500, (stem, red)
                subprocess.run(["pdftoppm", "-singlefile", "-scale-to-x", str(image.width),
                    "-scale-to-y", str(image.height), "-png", str(out / f"{stem}.pdf"),
                    str(out / f"{stem}-pdf")], check=True, timeout=60, capture_output=True)
                pdf_pixels = np.asarray(Image.open(out / f"{stem}-pdf.png").convert("RGB"))
                pdf_red = ((pdf_pixels[:, :, 0].astype(int) - pdf_pixels[:, :, 1]) > 40).sum()
                assert pdf_red > 500, (stem, pdf_red)
                subprocess.run(["inkscape", str(out / f"{stem}.svg"),
                    "--export-type=png", f"--export-filename={out / f'{stem}-inkscape.png'}",
                    f"--export-width={image.width}", f"--export-height={image.height}"],
                    check=True, timeout=60, capture_output=True)
                svg_pixels = np.asarray(Image.open(out / f"{stem}-inkscape.png").convert("RGB"))
                svg_red = ((svg_pixels[:, :, 0].astype(int) - svg_pixels[:, :, 1]) > 40).sum()
                assert svg_red > 500, (stem, svg_red)
                results.append({"scope": scope, "angle": angle, "dimensions": dimensions,
                    "red_pixels_png": int(red), "red_pixels_pdf": int(pdf_red),
                    "red_pixels_inkscape": int(svg_red), "viewport_preserved": True})
            assert not errors, errors
        finally:
            (out / "results.json").write_text(json.dumps({"results": results, "errors": errors}, indent=2))
            browser.close()
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
