#!/usr/bin/env python3
"""Verify custom dotplot generation and mixed-resolution painting on a disposable Borrelia server."""

import argparse
import io
import json
import time
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from PIL import Image, ImageChops
from playwright.sync_api import sync_playwright
from sequence_browser_harness import BrowserCheck, install_download_picker


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for name in ("url", "api", "output"):
        p.add_argument("--" + name, required=True)
    args = p.parse_args()
    for address in (args.url, args.api):
        if urlsplit(address).hostname not in ("127.0.0.1", "localhost"):
            p.error("Use a local server with disposable Borrelia inputs")
    out = Path(args.output)
    out.mkdir(exist_ok=True, parents=True)
    checks = []
    errors = []
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        page = browser.new_page(viewport={"width": 1440, "height": 900})
        install_download_picker(page)
        page.set_default_timeout(30000)
        page.add_init_script("window.__HICT_BENCHMARK_ENABLED__=true")
        page.route(
            "http://localhost:5000/**",
            lambda route: route.continue_(
                url=args.api + route.request.url.removeprefix("http://localhost:5000")
            ),
        )
        page.on("pageerror", lambda error: errors.append(str(error)))
        tile_failures = []
        fetched = []
        def record_tile(response):
            parsed = urlsplit(response.url)
            if parsed.path.endswith("/get_tile") and response.ok:
                fetched.append({k: vals[0] for k, vals in parse_qs(parsed.query).items()})
        page.on("response", record_tile)
        page.on(
            "response",
            lambda rsp: (
                tile_failures.append((rsp.status, rsp.url))
                if rsp.status >= 400 and "/get_tile" in rsp.url
                else None
            ),
        )
        check = BrowserCheck(page, out)

        def request(path, data):
            res = page.request.post(args.api + path, data=data)
            assert res.ok, (res.status, res.text())
            return res.json()

        def select(start, end, row_start=None, row_end=None):
            page.evaluate(
                """([start,end,rowStart,rowEnd]) => {
                const v=window.__hictBenchmark.manager().viewAndLayersManager.currentViewState.selectionBorders;
                v.leftBP=[start,rowStart];v.rightBP=[end,rowEnd];
            }""",
                [start, end, row_start or start, row_end or end],
            )

        def observe(kind, numeric_levels, sample, expected, position, mode, letters=False, whole=False, wide=False):
            state = request("/secondary/status", {})
            actual = sorted(state["compatibility"]["secondaryResolutions"])
            assert state["attached"] and actual == numeric_levels, state
            started = time.monotonic()
            x, y = position
            view = page.evaluate(
                """([bp,x,y,letters,whole])=>{
                  const m=window.__hictBenchmark.manager(), l=m.viewAndLayersManager;
                  if (whole) l.fitViewToActiveExtent();
                  else l.restoreViewPosition({centerBp:[x+0.5,y+0.5],
                      resolution:l.getPixelResolutionForBpResolution(bp)/(letters ? 24 : 1)});
                  m.getMap().renderSync();
                  return {center:l.captureViewPosition().centerBp,
                      viewport:m.getMap().getSize(),
                      primary:l.getNativeResolutionDescriptor('PRIMARY').bpResolution,
                      secondary:l.getNativeResolutionDescriptor('SECONDARY').bpResolution};
                }""", [expected,x,y,letters,whole])
            check.settle()
            if whole:
                expected = view["secondary"]
                assert expected >= 1000 and expected == view["primary"], view
                if wide:
                    assert expected == 1000, view
                    # Frame the real assembly, including its final contig. The rendering
                    # extent can also include empty padding to a coarser native grid.
                    source_bp, full_extent, viewport_extent = page.evaluate("""()=>{
                        const m=window.__hictBenchmark.manager(), h=m.contigDimensionHolder;
                        const l=m.viewAndLayersManager, d=l.currentViewState.resolutionDesciptor;
                        const last=h.prefix_sum_bp[h.contig_count]-1;
                        const edge=(h.getFractionalPxOfBp(last,d.bpResolution)+1/d.bpResolution)*d.pixelResolution;
                        return [last+1,[0,-edge,edge,0],m.getView().calculateExtent(m.getMap().getSize())];
                    }""")
                    assert source_bp == 1453623, source_bp
                    view["assemblyLengthBp"] = source_bp
                    assert all(viewport_extent[i] <= full_extent[i]+1 for i in (0,1)), (viewport_extent,full_extent)
                    assert all(viewport_extent[i] >= full_extent[i]-1 for i in (2,3)), (viewport_extent,full_extent)
                    view["fullExtent"] = full_extent
                    view["viewportExtent"] = viewport_extent
                sample = 1
            else:
                assert view["primary"] == 1000 and view["secondary"] == expected, view
                if expected < 1000:
                    assert max(abs(a-b-.5) for a,b in zip(view["center"], position)) < 1, view
            if letters:
                for axis in ("horizontal", "vertical"):
                    page.locator("canvas.sequence-axis."+axis).wait_for(state="visible")
            primary = view["primary"]
            grid = "sequenceScale=true" if letters else f"gridBpResolution={primary}&gridSampleFactor={sample}"
            query = f"/get_tile?source=SECONDARY&bpResolution={expected}&{grid}&row={int(y/primary*sample if not letters else y)-24}&col={int(x/primary*sample if not letters else x)-24}&rows=48&cols=48&format=PNG_BY_PIXELS"
            result = page.request.get(args.api + query)
            assert result.ok and result.body().startswith(b"\x89PNG"), (
                result.status,
                result.text(),
            )
            rgba = Image.open(io.BytesIO(result.body())).convert("RGBA")
            alpha_pixels = sum(rgba.getchannel("A").histogram()[1:])
            assert alpha_pixels > 0, (kind, mode)
            (out / f"{kind}-{mode}-projected.png").write_bytes(result.body())
            painted = page.evaluate("""([x,y]) => {
              const m=window.__hictBenchmark.manager(), l=m.viewAndLayersManager;
              const d=l.currentViewState.resolutionDesciptor, h=m.contigDimensionHolder;
              const point=m.getMap().getPixelFromCoordinate([x,y].map((bp,i)=>
                 (i?-1:1)*h.getFractionalPxOfBp(bp+.5,d.bpResolution)*d.pixelResolution));
              const layers=l.layersHolder.secondaryHiCDataLayers.filter(layer=>layer.getVisible());
              for(let dx=-3; dx<=3; dx++) for(let dy=-3; dy<=3; dy++)
                for(const layer of layers) {
                  const color=layer.getData([point[0]+dx,point[1]+dy]);
                  if(color?.[3]>0) return {point, offset:[dx,dy], rgba:Array.from(color)};
                }
              return null;
            }""", [x,y])
            assert painted, (kind, mode, expected, position, view)
            suffix = 'whole' if whole else 'letters' if letters else str(expected)
            if wide:
                suffix += "-wide"
            page.locator("#hic-contact-map").screenshot(
                path=str(out / f"{kind}-{mode}-{suffix}-view.png")
            )

            def visible():
                return page.evaluate("""() => {
                    const l=window.__hictBenchmark.manager().viewAndLayersManager.layersHolder;
                    return l.hicDataLayers.filter(layer=>layer.getVisible()).map(layer=>({source:layer.get('sourceName'),bp:layer.get('bpResolution'),projection:layer.get('projectionKey')}))
                }""")

            if letters:
                assert sorted(row["source"] for row in visible()) == ["PRIMARY", "SECONDARY"], visible()
            assert letters or any(
                row.get("projection") == f"{primary}/{expected}/{sample}"
                for row in visible()
            ), visible()
            checks.append(
                {
                    "tool": kind,
                    "warmNavigationSeconds": time.monotonic()-started,
                    "stored": actual,
                    "primary": primary, "view": view, "painted": painted, "letters": letters, "whole": whole,
                    "visible": expected,
                    "sampleFactor": sample,
                    "mode": mode,
                    "visiblePixels": alpha_pixels,
                    "layers": visible(),
                }
            )

        try:
            page.goto(args.url)
            page.wait_for_function("window.__hictBenchmark")
            page.evaluate(
                "api=>window.__hictBenchmark.open('borrelia.hict.hdf5',api,'borrelia-matching.fa')",
                args.api,
            )
            check.settle()
            nan = page.get_by_role(
                "button", name="Show raw contacts (no balancing)", exact=True
            )
            if nan.is_visible():
                nan.click()
            select(435400, 436376, 438700, 439700)
            check.menu("Tools", "ModDotPlot…")
            dialog = page.get_by_role("dialog", name="ModDotPlot", exact=True)
            sizes = dialog.get_by_label("ModDotPlot window sizes (bp)", exact=True)
            sizes.fill("37, 50, 250")
            sizes.press("Tab")
            dialog.locator('input[value="1000"]').check()
            assert sizes.input_value() == "37, 50, 250, 1000"
            dialog.locator('input[value="50"]').uncheck()
            assert sizes.input_value() == "37, 250, 1000"
            dialog.locator('input[value="50"]').check()
            assert sizes.input_value() == "37, 250, 1000, 50"
            sizes.fill("1, 37, 50, 250, 1000")
            assert (
                dialog.get_by_role("alert")
                .get_by_text(
                    "Each ModDotPlot window must be at least as long as its k-mer size."
                )
                .is_visible()
            )
            assert dialog.get_by_role(
                "button", name="Generate", exact=True
            ).is_disabled()
            sizes.fill("37, 50")
            dialog.get_by_label("Analysis scope").select_option("assembly")
            recovery = dialog.get_by_role("button", name="Use current rectangle", exact=True)
            recovery.wait_for()
            assert dialog.get_by_text("Budget exceeded at", exact=False).is_visible()
            recovery.click()
            assert sizes.input_value() == "37, 50"
            dialog.get_by_text("Estimated storage", exact=False).wait_for()
            assert dialog.get_by_label("Analysis scope").input_value() == "selection"
            dialog.get_by_label("Show result as").select_option("below")
            if not dialog.get_by_label("Worker threads", exact=True).is_visible():
                check.advanced()
            dialog.get_by_label("Worker threads").fill("2")
            cold_start = time.monotonic()
            with page.expect_response(
                lambda r: (
                    "/sequence/moddotplot/jobs" in r.url and r.request.method == "POST"
                )
            ) as submitted:
                dialog.get_by_role(
                    "button", name="Generate", exact=True
                ).click()
            assert submitted.value.ok, submitted.value.text()
            assert submitted.value.request.post_data_json["resolutions"] == [37, 50]
            dialog.wait_for(state="hidden", timeout=180000)
            checks.append({"job": submitted.value.json()["jobId"], "generationAndAttachmentSeconds": time.monotonic()-cold_start})
            native = request("/secondary/status", {})["compatibility"]["primaryResolutions"]
            def pyramid(requested):
                anchors = sorted(set(requested) | {r for r in native if r > min(requested)})
                levels = [anchors[0]]
                for anchor in anchors[1:]:
                    while anchor > 4*levels[-1]:
                        levels.append(4*levels[-1])
                    levels.append(anchor)
                return levels
            overview = pyramid([37, 50])
            for mode in ("below","overlay","above"):
                page.get_by_label("Sequence overlay view").select_option(mode)
                position = (439080,435835) if mode == "above" else (435835,439080)
                observe("moddotplot", overview, 1, 1000, position, mode, whole=True)
                observe("moddotplot", overview, 20, 50, position, mode)
                observe("moddotplot", overview, 55, 37, position, mode)
            # Ch1 has identical 601 bp intervals at offsets 435535 and 438780.
            # Their separation makes the hit visible on either side of the diagonal.
            select(435535, 436136, 438780, 439381)
            check.menu("Tools", "Dotplot from current assembly…")
            dialog = page.get_by_role(
                "dialog", name="Dotplot from current assembly", exact=True
            )
            sizes = dialog.get_by_label("Dotplot resolutions (bp/bin)")
            sizes.fill("1 50")
            dialog.locator('input[value="1000"]').check()
            assert sizes.input_value() == "1, 50, 1000"
            dialog.get_by_label("Analysis scope").select_option("selection")
            dialog.get_by_label("Show result as").select_option("above")
            check.advanced()
            dialog.get_by_label("Sample every (bp)", exact=True).fill("1")
            cold_start = time.monotonic()
            with page.expect_response(
                lambda r: (
                    "/sequence/dotplot/jobs" in r.url and r.request.method == "POST"
                )
            ) as submitted:
                dialog.get_by_role(
                    "button", name="Generate", exact=True
                ).click()
            assert submitted.value.ok, submitted.value.text()
            assert submitted.value.request.post_data_json["resolutions"] == [
                1,
                50,
                1000,
            ]
            dialog.wait_for(state="hidden", timeout=180000)
            checks.append({"job": submitted.value.json()["jobId"], "generationAndAttachmentSeconds": time.monotonic()-cold_start})
            levels = pyramid([1, 50, 1000])
            for mode in ("above", "overlay", "below"):
                page.get_by_label("Sequence overlay view").select_option(mode)
                position = (435835,439080) if mode == "below" else (439080,435835)
                observe("dotplot", levels, 1, 1000, position, mode, whole=True)
                for expected, sample in ((1000, 1), (1, 1000)):
                    observe("dotplot", levels, sample, expected, position, mode)
                observe("dotplot", levels, 1000, 1, position, mode, letters=True)
                observe("dotplot", levels, 1, 1000, position, mode, whole=True)
            # The complete Borrelia extent also fits at the primary 1 kb level on a larger display.
            page.set_viewport_size({"width": 2100, "height": 1620})
            check.settle()
            for mode in ("above", "overlay", "below"):
                page.get_by_label("Sequence overlay view").select_option(mode)
                position = (435835,439080) if mode == "below" else (439080,435835)
                observe("dotplot", levels, 1, 1000, position, mode, whole=True, wide=True)
                observe("dotplot", levels, 1000, 1, position, mode, letters=True, wide=True)
                observe("dotplot", levels, 1, 1000, position, mode, whole=True, wide=True)
            page.set_viewport_size({"width": 1440, "height": 900})
            check.settle()
            with page.expect_download() as download:
                check.menu("File", "Save session…")
            saved = out/"fine-overlay-session.json"
            download.value.save_as(saved)
            check.menu("File", "Close map")
            page.get_by_role("heading", name="No contact map open").wait_for()
            with page.expect_file_chooser() as chosen:
                check.menu("File", "Open session…")
            chosen.value.set_files(saved)
            page.get_by_text("Session restored. Undo history starts from the restored assembly.").wait_for()
            observe("dotplot", levels, 1, 1000, (435835,439080), "restored")
            previous = request("/secondary/status", {})["filename"]
            retry = submitted.value.request.post_data_json.copy()
            retry.update(mergeCompatible=True, region={"rowStart":435535,"rowEnd":436136,
                                                      "columnStart":438780,"columnEnd":439381})
            pending = request("/sequence/dotplot/jobs", retry)["jobId"]
            stopped = page.request.post(args.api+f"/sequence/jobs/{pending}/stop", data={})
            assert stopped.ok, stopped.text()
            deadline = time.monotonic()+30
            while time.monotonic()<deadline:
                row = next(j for j in request("/sequence/jobs/list", {}) if j["jobId"]==pending)
                if row["status"] in ("cancelled","finished","failed"):
                    break
                time.sleep(.1)
            assert row["status"]=="cancelled" and not row["outputFile"], row
            assert request("/secondary/status", {})["filename"] == previous
            checks.append({"cancelled":pending,"retainedOverlay":previous})
            # Reverse the chromosome containing both landmarks and follow the
            # mirrored source bases into the same lower-triangle alignment.
            length = 910724
            seq = request("/sequence/region", {"startBp":435830,"endBp":435840})["sequence"]
            page.evaluate("""n=>window.__hictBenchmark.manager().viewAndLayersManager.selectEntityRangeByBp(0,n)""",length)
            with page.expect_response("**/reverse_selection_range") as edit:
                page.get_by_role("button",name="Reverse Selection",exact=True).click()
            assert edit.value.ok,edit.value.text()
            check.settle()
            reverse = seq.translate(str.maketrans("ACGT", "TGCA"))[::-1]
            assert request("/sequence/region", {"startBp":length-435840,"endBp":length-435830})["sequence"] == reverse
            observe("dotplot", levels, 1000, 1, (length-1-439080,length-1-435835), "reversed", letters=True)
            # Export the same fixed viewport with the known lower-triangle
            # alignment visible and then assigned to the opposite triangle.
            exported = []
            for triangle in ("below", "above"):
                page.get_by_label("Sequence overlay view").select_option(triangle)
                check.settle()
                with page.expect_download(timeout=90000) as delivery:
                    page.evaluate("""()=>window.__hictBenchmark.manager().exportCurrentMapPng(
                        undefined,{scope:'viewport',cropToMap:true})""")
                filename = out / f"reversed-{triangle}-export.png"
                delivery.value.save_as(filename)
                with Image.open(filename) as figure:
                    exported.append(figure.convert("RGB"))
            assert exported[0].size == exported[1].size
            width,height = exported[0].size
            interior = (width//4,height//4,3*width//4,3*height//4)
            changed = ImageChops.difference(*exported).crop(interior).getbbox()
            assert changed, "The lower-triangle landmark disappeared from the figure export"
            checks.append({"exportPixels":list(exported[0].size),"interiorDifferenceBounds":list(changed)})
            fetched_native = {(v.get("source"),v.get("bpResolution"),v.get("sequenceScale")) for v in fetched}
            assert {("PRIMARY","1000","true"),("SECONDARY","1","true")} <= fetched_native
            assert any(s=="SECONDARY" and int(bp or 0)>=1000 for s,bp,_ in fetched_native)
            assert not errors and not tile_failures, (errors, tile_failures)
        finally:
            (out / "results.json").write_text(
                json.dumps(
                    {"checks": checks, "errors": errors, "tileFailures": tile_failures, "fetchedLevels": sorted(set((v.get("source",""),v.get("bpResolution",""),v.get("sequenceScale","")) for v in fetched))},
                    indent=2,
                )
                + "\n"
            )
            if not page.is_closed() and checks:
                try:
                    page.screenshot(path=str(out / "last.png"), timeout=3000)
                except Exception:
                    pass
            browser.close()


if __name__ == "__main__":
    main()
