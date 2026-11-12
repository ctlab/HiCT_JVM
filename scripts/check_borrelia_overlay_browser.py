#!/usr/bin/env python3
"""Borrelia minimap and O/E acceptance against an explicitly isolated local server.

Use physical copies of borrelia.hict.hdf5 and borrelia-matching.fa in a /tmp data
folder. Never point this script at an owner's active server: it opens maps and
starts a real selection ModDotPlot job. See the corresponding release note.
"""

import argparse
import base64
import hashlib
import io
import json
import time
from pathlib import Path
from urllib.parse import urlparse

from PIL import Image
from playwright.sync_api import sync_playwright
from sequence_browser_harness import BrowserCheck


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("url", "api", "output", "data-dir"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--browser-executable")
    args = parser.parse_args()
    if not Path(args.data_dir).resolve().is_relative_to("/tmp"):
        parser.error("Use disposable copies in /tmp")
    for address in (args.url, args.api):
        if urlparse(address).hostname not in ("localhost", "127.0.0.1"):
            parser.error("An isolated local server is required")
    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=True)
    results = {"checks": []}
    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=True, executable_path=args.browser_executable
        )
        page = browser.new_page(viewport={"width": 1600, "height": 1100})
        page.set_default_timeout(30000)
        page.add_init_script("window.__HICT_BENCHMARK_ENABLED__=true")
        page.route(
            "http://localhost:5000/**",
            lambda route: route.continue_(
                url=args.api + route.request.url.removeprefix("http://localhost:5000")
            ),
        )
        check = BrowserCheck(page, out)

        def post(path, payload=None):
            response = page.request.post(args.api + path, data=payload or {})
            assert response.ok, response.text()
            return response.json()

        def set_mode(mode):
            page.evaluate(
                "mode=>window.__hictBenchmark.manager().visualizationManager.setSignalDisplayMode(mode)",
                mode,
            )
            check.settle()

        def mini_pixels():
            # Inspect painted imagery, excluding the blue viewport vector layer.
            return page.evaluate("""()=>{
                const m=window.__hictBenchmark.manager(),l=m.minimap.getLayers().item(0);
                const s=l.getSource(), pixels=[];
                s.tileCache.forEach(tile=>{
                    const image=tile.getImage();
                    if(!image.width || !image.height)return;
                    const c=document.createElement('canvas');c.width=image.width;c.height=image.height;
                    const ctx=c.getContext('2d');ctx.drawImage(image,0,0);
                    const d=ctx.getImageData(0,0,c.width,c.height).data;
                    for(let i=0;i<d.length;i+=4)
                        if(d[i+3]>0 && (d[i]<245 || d[i+1]<245 || d[i+2]<245))pixels.push(1);
                });
                return {colored:pixels.length,disposed:s.disposed,bp:s.bpResolution};
            }""")

        def primary_tile(bp=2000):
            # Entirely between chromosome 1 and the plasmids: cis expected is undefined.
            params = f"source=PRIMARY&bpResolution={bp}&row=0&col={2 if bp == 2000 else 4}&tile_size=256&format=JSON_PNG_WITH_RANGES&version=0"
            response = page.request.get(args.api + "/get_tile?" + params)
            assert response.ok, response.text()
            image = response.json()["image"].split(",", 1)[1]
            raw = base64.b64decode(image)
            tile = Image.open(io.BytesIO(raw)).convert("RGBA")
            return tile, hashlib.sha256(tile.tobytes()).hexdigest()

        try:
            page.goto(args.url)
            page.wait_for_function("window.__hictBenchmark")
            page.evaluate(
                "api=>window.__hictBenchmark.open('borrelia.hict.hdf5',api,'borrelia-matching.fa')",
                args.api,
            )
            check.settle()
            nan = page.get_by_role(
                "button", name="Do not use Cooler weights", exact=True
            )
            if nan.is_visible():
                nan.click()
                check.settle()
            assert mini_pixels()["colored"] > 0, mini_pixels()
            results["checks"].append({"openingMinimap": mini_pixels()})
            page.evaluate("""()=>{
                const v=window.__hictBenchmark.manager().viewAndLayersManager;
                v.currentViewState.selectionBorders.leftBP=[880000,880000];
                v.currentViewState.selectionBorders.rightBP=[1080000,1080000];
            }""")
            check.menu("Tools", "Generate ModDotPlot…")
            dialog = page.get_by_role("dialog", name="Generate ModDotPlot", exact=True)
            dialog.get_by_label("Analysis scope", exact=True).select_option("selection")
            dialog.get_by_label("Worker threads", exact=True).fill("2")
            dialog.get_by_role("button", name="Generate ModDotPlot", exact=True).click()
            dialog.locator("footer").get_by_role(
                "button", name="Close", exact=True
            ).click()
            deadline = time.monotonic() + 180
            while time.monotonic() < deadline:
                if post("/secondary/status").get("attached"):
                    break
                page.wait_for_timeout(250)
            else:
                raise TimeoutError(post("/sequence/jobs/list"))
            page.get_by_label("Sequence overlay view").wait_for()
            page.wait_for_timeout(1500)
            check.settle()
            assert mini_pixels()["colored"] > 0, mini_pixels()
            assert not mini_pixels()["disposed"]
            positions = page.evaluate(
                """()=>({
                layout:document.querySelector('[aria-label="Sequence overlay view"]').getBoundingClientRect().bottom,
                source:document.querySelector('[aria-label="Visualization source"]').getBoundingClientRect().top})"""
            )
            assert positions["layout"] < positions["source"], positions
            results["checks"].append({"overlayMinimapAndLayout": mini_pixels()})
            graph = post("/render_pipeline/get")
            page.screenshot(path=str(out / "observed-overlay.png"))
            set_mode("OBSERVED_OVER_EXPECTED")
            tile, original_hash = primary_tile()
            assert not tile.getchannel(
                "A"
            ).getbbox(), "Undefined trans O/E colored as depletion"
            assert post("/render_pipeline/get") == graph
            # No viewport profile exists for this finer level yet. It must still
            # distinguish undefined inter-replicon ratios from measured depletion.
            uncached, _ = primary_tile(1000)
            assert not uncached.getchannel(
                "A"
            ).getbbox(), "Uncached resolution used a cross-replicon fallback"
            results["checks"].append({"undefinedTransOeTransparent": original_hash})
            page.get_by_role("button", name="Secondary", exact=True).click()
            page.wait_for_timeout(300)
            visited_levels = set()
            for factor in (0.4, 2.5, 0.8):
                page.evaluate(
                    "factor=>{const m=window.__hictBenchmark.manager();m.getView().setResolution(m.getView().getResolution()*factor)}",
                    factor,
                )
                page.wait_for_timeout(1000)
                check.settle()
                visited_levels.add(
                    page.evaluate(
                        "()=>window.__hictBenchmark.manager().viewAndLayersManager.getVisibleSourceResolutionDescriptors().primary.bpResolution"
                    )
                )
                _, digest = primary_tile()
                assert (
                    digest == original_hash
                ), "Primary tile changed after source switch/zoom"
            assert post("/render_pipeline/get") == graph
            page.screenshot(path=str(out / "oe-overlay.png"))
            assert {1000, 2000}.issubset(visited_levels), visited_levels
            results["checks"].append(
                {"sourceSwitchAndRepeatedZoomStable": sorted(visited_levels)}
            )
            set_mode("EXPECTED")
            assert post("/render_pipeline/get") == graph
            set_mode("OBSERVED")
            tile, _ = primary_tile()
            assert tile.getchannel(
                "A"
            ).getbbox(), "Observed trans contacts must remain visible"
            assert post("/render_pipeline/get") == graph
            results["checks"].append({"expectedObservedRoundtrip": True})
            assert not check.errors, check.errors
            results["status"] = "passed"
        except BaseException as error:
            results["status"] = "failed"
            results["error"] = str(error)
            page.screenshot(path=str(out / "failure.png"))
            raise
        finally:
            results["browserErrors"] = check.errors
            (out / "results.json").write_text(json.dumps(results, indent=2) + "\n")
            browser.close()


if __name__ == "__main__":
    main()
