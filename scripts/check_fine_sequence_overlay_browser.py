#!/usr/bin/env python3
"""Verify custom dotplot generation and mixed-resolution painting on a disposable Borrelia server."""

import argparse
import io
import json
import time
from pathlib import Path
from urllib.parse import urlsplit

from PIL import Image
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

        def select(start, end):
            page.evaluate(
                """([start,end]) => {
                const v=window.__hictBenchmark.manager().viewAndLayersManager.currentViewState.selectionBorders;
                v.leftBP=[start,start];v.rightBP=[end,end];
            }""",
                [start, end],
            )

        def zoom(bp, position):
            return page.evaluate(
                """([bp,position])=>{
              const m=window.__hictBenchmark.manager(), l=m.viewAndLayersManager;
              const units=l.getPixelResolutionForBpResolution(bp);
              const p=l.getPixelResolutionForBpResolution(1000);
              const coord=m.contigDimensionHolder.getFractionalPxOfBp(position,1000)*p;
              m.getView().setCenter([coord,-coord]);m.getView().setResolution(units);
              m.getMap().renderSync();
              return l.getVisibleSourceResolutionDescriptors();
            }""",
                [bp, position],
            )

        def observe(kind, numeric_levels, sample, expected, position, mode):
            state = request("/secondary/status", {})
            actual = sorted(state["compatibility"]["secondaryResolutions"])
            assert state["attached"] and actual == numeric_levels, state
            started = time.monotonic()
            descriptors = zoom(expected, position)
            assert descriptors["primary"]["bpResolution"] == 1000, descriptors
            assert descriptors["secondary"]["bpResolution"] == expected, descriptors
            query = f"/get_tile?source=SECONDARY&bpResolution={expected}&gridBpResolution=1000&gridSampleFactor={sample}&row={position // expected - 24}&col={position // expected - 24}&rows=48&cols=48&format=PNG_BY_PIXELS"
            result = page.request.get(args.api + query)
            assert result.ok and result.body().startswith(b"\x89PNG"), (
                result.status,
                result.text(),
            )
            rgba = Image.open(io.BytesIO(result.body())).convert("RGBA")
            alpha_pixels = sum(rgba.getchannel("A").histogram()[1:])
            assert alpha_pixels > 0, (kind, mode)
            (out / f"{kind}-{mode}-projected.png").write_bytes(result.body())
            check.settle()
            page.locator("#hic-contact-map").screenshot(
                path=str(out / f"{kind}-{mode}-view.png")
            )

            def visible():
                return page.evaluate("""() => {
                    const l=window.__hictBenchmark.manager().viewAndLayersManager.layersHolder;
                    return l.hicDataLayers.filter(layer=>layer.getVisible()).map(layer=>({source:layer.get('sourceName'),bp:layer.get('bpResolution'),projection:layer.get('projectionKey')}))
                }""")

            assert any(
                row.get("projection") == f"1000/{expected}/{sample}"
                for row in visible()
            ), visible()
            checks.append(
                {
                    "tool": kind,
                    "warmNavigationSeconds": time.monotonic()-started,
                    "stored": actual,
                    "primary": 1000,
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
                "button", name="Do not use Cooler weights", exact=True
            )
            if nan.is_visible():
                nan.click()
            select(613568, 620534)
            check.menu("Tools", "Generate ModDotPlot…")
            dialog = page.get_by_role("dialog", name="Generate ModDotPlot", exact=True)
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
                "button", name="Generate ModDotPlot", exact=True
            ).is_disabled()
            sizes.fill("50")
            dialog.get_by_label("Analysis scope").select_option("selection")
            dialog.get_by_label("Display completed result").select_option("below")
            if not dialog.get_by_label("Worker threads", exact=True).is_visible():
                dialog.get_by_text("Advanced", exact=True).click()
            dialog.get_by_label("Worker threads").fill("2")
            cold_start = time.monotonic()
            with page.expect_response(
                lambda r: (
                    "/sequence/moddotplot/jobs" in r.url and r.request.method == "POST"
                )
            ) as submitted:
                dialog.get_by_role(
                    "button", name="Generate ModDotPlot", exact=True
                ).click()
            assert submitted.value.ok, submitted.value.text()
            assert submitted.value.request.post_data_json["resolutions"] == [50]
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
            overview = pyramid([50])
            observe("moddotplot", overview, 1, 1000, 617000, "below")
            observe("moddotplot", overview, 20, 50, 617000, "below")
            select(616750, 617350)
            check.menu("Tools", "Generate dotplot from active assembly…")
            dialog = page.get_by_role(
                "dialog", name="Generate dotplot from active assembly", exact=True
            )
            sizes = dialog.get_by_label("Dotplot resolutions (bp/bin)")
            sizes.fill("1 50")
            dialog.locator('input[value="1000"]').check()
            assert sizes.input_value() == "1, 50, 1000"
            dialog.get_by_label("Analysis scope").select_option("selection")
            dialog.get_by_label("Display completed result").select_option("above")
            dialog.get_by_text("Advanced", exact=True).click()
            dialog.get_by_label("Sample every (bp)", exact=True).fill("1")
            cold_start = time.monotonic()
            with page.expect_response(
                lambda r: (
                    "/sequence/dotplot/jobs" in r.url and r.request.method == "POST"
                )
            ) as submitted:
                dialog.get_by_role(
                    "button", name="Generate dotplot from active assembly", exact=True
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
                for expected, sample in ((1000, 1), (1, 1000), (1000, 1)):
                    observe("dotplot", levels, sample, expected, 617000, mode)
            with page.expect_download() as download:
                check.menu("File", "Save session")
            saved = out/"fine-overlay-session.json"
            download.value.save_as(saved)
            check.menu("File", "Close")
            page.get_by_role("heading", name="No contact map open").wait_for()
            with page.expect_file_chooser() as chosen:
                check.menu("File", "Open session")
            chosen.value.set_files(saved)
            page.get_by_text("Session restored. Undo history starts from the restored assembly.").wait_for()
            observe("dotplot", levels, 1, 1000, 617000, "restored")
            assert not errors and not tile_failures, (errors, tile_failures)
        finally:
            (out / "results.json").write_text(
                json.dumps(
                    {"checks": checks, "errors": errors, "tileFailures": tile_failures},
                    indent=2,
                )
                + "\n"
            )
            page.screenshot(path=str(out / "last.png"))
            browser.close()


if __name__ == "__main__":
    main()
