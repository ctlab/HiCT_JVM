#!/usr/bin/env python3
"""Generate both regions through the UI and check the painted secondary tiles."""

import argparse
import base64
from functools import partial
from http.server import ThreadingHTTPServer
import io
import json
from pathlib import Path
import shutil
import tempfile
import threading

from PIL import Image
from playwright.sync_api import expect, sync_playwright

from check_shift_selection_browser import Quiet
from sequence_browser_harness import BrowserCheck, HictTestServer, configure_test_api


def check_accumulation(browser, web, api_url, output, directory=""):
    page = browser.new_page(viewport={"width": 1440, "height": 900})
    configure_test_api(page, api_url)
    page.add_init_script("window.__HICT_BENCHMARK_ENABLED__ = true")
    check = BrowserCheck(page, output)
    page.goto(web)
    page.evaluate("""([api, path]) => window.__hictBenchmark.open(
        path + 'borrelia.hict.hdf5', api, path + 'borrelia-matching.fa')""", [api_url, directory])
    check.settle()

    def generate(start, end, bp, index):
        page.evaluate("""([start,end]) => {
            const layers = window.__hictBenchmark.manager().viewAndLayersManager;
            layers.setRegionBounds({rowStart:start,rowEnd:end,columnStart:start,columnEnd:end});
        }""", [start, end])
        check.menu("Tools", "ModDotPlot…")
        dialog = page.get_by_role("dialog", name="ModDotPlot", exact=True)
        dialog.get_by_label("Finest detail", exact=True).select_option(str(bp))
        dialog.get_by_label("Show result as", exact=True).select_option("below")
        with page.expect_response("**/sequence/moddotplot/jobs") as submitted:
            dialog.get_by_role("button", name="Generate", exact=True).click()
        assert submitted.value.ok, submitted.value.text()
        assert submitted.value.request.post_data_json["mergeCompatible"]
        expect(dialog.get_by_text(f"Added region {index}", exact=False)).to_be_visible(timeout=90_000)
        expect(dialog.get_by_text("is shown below the diagonal", exact=False)).to_be_visible()
        page.screenshot(path=str(output / f"region-{index}-finished.png"))
        dialog.get_by_role("button", name="Generate another region", exact=True).click()
        check.settle()
        result_list = page.request.post(f"{api_url}/sequence/jobs/list", data={}).json()
        return next(job for job in result_list if job["jobId"] == submitted.value.json()["jobId"])

    def alpha_at(position, resolution, projected=False):
        pixel = page.evaluate("""([bp,res]) => Math.floor(
            window.__hictBenchmark.manager().contigDimensionHolder.getFractionalPxOfBp(bp,res)
        )""", [position, resolution])
        tile, offset = divmod(pixel, 256)
        grid = f"&gridBpResolution={resolution}&gridSampleFactor=1" if projected else ""
        response = page.request.get(
            f"{api_url}/get_tile?source=SECONDARY&bpResolution={resolution}&row={tile}&col={tile}"
            f"&tile_size=256&format=JSON_PNG_WITH_RANGES&version=0{grid}"
        )
        assert response.ok, response.text()
        png = base64.b64decode(response.json()["image"].split(",", 1)[1])
        image = Image.open(io.BytesIO(png)).convert("RGBA")
        # The visualization places the secondary source below the diagonal.
        lower = image.getchannel("A").crop((offset - 1, offset, offset + 1, offset + 2))
        return lower.getextrema()[1]

    first = generate(10_000, 80_000, 1000, 1)
    second = generate(500_000, 600_000, 2000, 2)
    assert first["resultSet"]["id"] == second["resultSet"]["id"]
    assert first["outputFile"] != second["outputFile"]
    levels = page.request.post(f"{api_url}/secondary/status", data={}).json()["compatibility"]["secondaryResolutions"]
    assert {1000, 2000}.issubset(levels), levels
    (output / "results.json").write_text(json.dumps({"versions": [first, second], "levels": levels}, indent=2))
    for projected in (False, True):
        shared = [alpha_at(bp, 2000, projected) for bp in (40_000, 550_000)]
        fine = [alpha_at(bp, 1000, projected) for bp in (40_000, 550_000)]
        assert shared[0] > 0 and shared[1] > 0, (projected, shared)
        assert fine[0] > 0 and fine[1] == 0, (projected, fine)

    page.keyboard.press("Escape")
    anchor = page.locator("#map-anchor-lock")
    if anchor.get_attribute("aria-pressed") == "true":
        anchor.click()
    check.select_range(region=False)
    with page.expect_response("**/reverse_selection_range") as reversed_selection:
        page.get_by_role("button", name="Reverse Selection", exact=True).click()
    assert reversed_selection.value.ok, reversed_selection.value.text()
    assert reversed_selection.value.request.post_data_json["endBP"] == 910724
    check.settle()
    for projected in (False, True):
        moved = [alpha_at(bp, 1000, projected) for bp in (40_000, 910_724 - 40_000)]
        assert moved[0] == 0 and moved[1] > 0, (projected, moved)
    with page.expect_response("**/assembly/history/restore") as restored:
        page.keyboard.press("Control+z")
    assert restored.value.ok, restored.value.text()
    assert not check.errors, check.errors
    page.close()
    print("Two compatible regions accumulated, fine-level missing cells were transparent, and reversal moved region 1.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("jar", "webui", "borrelia", "fasta", "output"):
        parser.add_argument("--" + name, type=Path, required=name == "output")
    parser.add_argument("--url", help="Use a running local preview with a disposable fixture directory")
    parser.add_argument("--fixture-directory", default="", help="Data-relative fixture directory in the preview")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    if args.url:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch()
            directory = args.fixture_directory.rstrip("/") + "/" if args.fixture_directory else ""
            check_accumulation(browser, args.url, args.url.rstrip("/"), args.output, directory)
            browser.close()
        return
    if not all((args.jar, args.webui, args.borrelia, args.fasta)):
        parser.error("Provide --url or all of --jar, --webui, --borrelia and --fasta")
    with (
        tempfile.TemporaryDirectory(prefix="hict-accumulation-") as temporary,
        ThreadingHTTPServer(("127.0.0.1", 0), partial(Quiet, directory=args.webui.resolve())) as web,
        sync_playwright() as playwright,
    ):
        data = Path(temporary)
        shutil.copyfile(args.borrelia, data / "borrelia.hict.hdf5")
        shutil.copyfile(args.fasta, data / "borrelia-matching.fa")
        threading.Thread(target=web.serve_forever, daemon=True).start()
        try:
            with HictTestServer(args.jar, data, args.output / "server.log") as api:
                browser = playwright.chromium.launch()
                check_accumulation(browser, f"http://127.0.0.1:{web.server_port}/", api.url, args.output)
                browser.close()
        finally:
            web.shutdown()


if __name__ == "__main__":
    main()
