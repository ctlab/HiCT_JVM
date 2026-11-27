#!/usr/bin/env python3
"""Check coverage persistence, tile responsiveness and resolution fits on a real map."""

import argparse
from functools import partial
from http.server import ThreadingHTTPServer
import json
import math
from pathlib import Path
import shutil
import tempfile
import threading
import time
from urllib.parse import urlsplit

from playwright.sync_api import expect, sync_playwright

from check_shift_selection_browser import Quiet
from sequence_browser_harness import BrowserCheck, HictTestServer, configure_test_api


def check_coverage(browser, web, api, output, filename):
    diagnostics = {}
    with browser.new_context(viewport={"width": 1440, "height": 900}) as context:
        configure_test_api(context, api.url)
        context.add_init_script("window.__HICT_BENCHMARK_ENABLED__ = true")
        for cold in (True, False):
            page = context.new_page()
            check = BrowserCheck(page, output)
            page.goto(web)
            page.evaluate("([name, api]) => window.__hictBenchmark.open(name, api)", [filename, api.url])
            check.settle()
            page.evaluate("""() => {
                const view = window.__hictBenchmark.manager().getView();
                view.setZoom(view.getZoom() + 3);
            }""")
            check.settle()
            tiles = []
            measuring_tiles = False

            def tile_finished(request):
                if measuring_tiles and urlsplit(request.url).path == "/get_tile":
                    elapsed = request.timing["responseEnd"]
                    if elapsed >= 0:
                        tiles.append(elapsed)

            page.on("requestfinished", tile_finished)
            start = time.monotonic()
            measuring_tiles = cold
            with page.expect_response("**/normalization/coverage/prepare") as preparation:
                page.locator("#normalization-mode").select_option("sqrt")
            assert preparation.value.ok, preparation.value.text()
            if cold:
                expect(page.locator("#normalization-progress")).to_contain_text("preparing")
                expect(page.locator("#normalization-cancel")).to_be_visible()
                page.screenshot(path=str(output / "preparing.png"))
            for step in range(240):
                status = json.loads(api.http("/normalization/coverage/status", {}))
                assert status["state"] not in {"FAILED", "CANCELLED"}, status
                if status["state"] == "READY":
                    break
                if cold:
                    page.evaluate("""i => {
                        const map = window.__hictBenchmark.manager().getMap();
                        const [left, bottom, right, top] = map.getView().getProjection().getExtent();
                        const x = .2 + ((i * 3) % 7) * .1;
                        const y = .2 + ((i * 2) % 7) * .1;
                        map.getView().setCenter([left + (right - left) * x, bottom + (top - bottom) * y]);
                    }""", step)
                page.wait_for_timeout(200 if cold else 20)
            else:
                raise AssertionError("Coverage never became ready")
            seconds = time.monotonic() - start
            measuring_tiles = False
            expect(page.locator("#normalization-coverage-ready")).to_contain_text("ready (saved for this map)")
            assert status["message"] == ("Saved coverage" if cold else "Loaded saved coverage"), status
            if cold:
                assert len(tiles) >= 10, tiles
                p95 = sorted(tiles)[math.ceil(len(tiles) * 0.95) - 1]
                assert p95 <= 300, p95
                diagnostics.update(coldSeconds=seconds, tileCount=len(tiles), tileP95Ms=p95)
                diagnostics["fits"] = check_resolution_fits(page, api.url)
            else:
                assert seconds < 2, seconds
                diagnostics["savedSeconds"] = seconds
                page.screenshot(path=str(output / "ready.png"))
            assert not check.errors, check.errors
            page.close()
            api.http("/close", {})
        files = json.loads(api.http("/storage/processed"))["files"]
        coverage = [item for item in files if item["category"] == "coverage"]
        assert len(coverage) == 1 and coverage[0]["action"] == "cache" and coverage[0]["bytes"] > 0, coverage
        diagnostics["cacheBytes"] = coverage[0]["bytes"]
        cleanup = json.loads(api.http("/storage/processed/cleanup", {"action": "cache", "ids": [coverage[0]["id"]]}))
        assert cleanup["deleted"] == 1 and cleanup["bytes"] == diagnostics["cacheBytes"], cleanup
    (output / "results.json").write_text(json.dumps(diagnostics, indent=2))
    print(json.dumps(diagnostics), flush=True)


def check_resolution_fits(page, api_url):
    # The Visualization panel draws its own viewport histogram even when the
    # fit is cached. Unmount it so the network log measures automatic fits.
    page.get_by_role("button", name="Entities", exact=True).click()
    page.evaluate("window.__hictBenchmark.manager().visualizationManager.waitForPendingOptionRequests()")
    queries = []

    def record(request):
        if urlsplit(request.url).path == "/matrix/query":
            payload = request.post_data_json
            if payload.get("signalMode") in {"TRADITIONAL_NORMALIZED", "PIPELINE_SIGNAL"}:
                queries.append(payload["bpResolution"])

    page.on("request", record)
    levels = page.evaluate("""() => window.__hictBenchmark.manager().contigDimensionHolder.resolutions
        .filter(level => level > 0).sort((a, b) => a - b).slice(0, 3)""")
    assert len(levels) == 3
    visits = []
    limits_by_level = {}
    for level in [levels[2], levels[0]] * 3:
        before = len(queries)
        active = page.evaluate("""level => {
            const manager = window.__hictBenchmark.manager(), map = manager.getMap();
            const layers = manager.viewAndLayersManager;
            manager.getView().setResolution(layers.getPixelResolutionForBpResolution(level));
            map.renderSync();
            map.dispatchEvent("moveend");
            return layers.getVisibleSourceResolutionDescriptors().primary.bpResolution;
        }""", level)
        assert active == level, (active, level)
        page.wait_for_function("""bp => {
            const vm = window.__hictBenchmark.manager().visualizationManager;
            const key = vm.coverageFits.get("PRIMARY")?.key;
            return key && JSON.parse(key)[1] === bp && vm.savedCoverageFits.has(key);
        }""", arg=level)
        page.evaluate("window.__hictBenchmark.manager().visualizationManager.waitForPendingOptionRequests()")
        options = page.request.post(f"{api_url}/get_visualization_options", data={"source": "PRIMARY"}).json()
        limits = [options["colormap"]["minSignal"], options["colormap"]["maxSignal"]]
        assert limits == limits_by_level.setdefault(level, limits), (level, limits, limits_by_level)
        visits.append({"resolution": level, "histogramQueries": len(queries) - before, "limits": limits})
    assert any(visit["histogramQueries"] > 0 for visit in visits[:2]), visits
    assert all(visit["histogramQueries"] == 0 for visit in visits[2:]), visits
    return visits


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("jar", "webui", "map", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    with (
        tempfile.TemporaryDirectory(prefix="hict-coverage-cache-") as temporary,
        ThreadingHTTPServer(("127.0.0.1", 0), partial(Quiet, directory=args.webui.resolve())) as web,
        sync_playwright() as playwright,
    ):
        data = Path(temporary)
        shutil.copyfile(args.map, data / args.map.name)
        threading.Thread(target=web.serve_forever, daemon=True).start()
        try:
            with HictTestServer(args.jar, data, args.output / "server.log") as api:
                browser = playwright.chromium.launch()
                check_coverage(browser, f"http://127.0.0.1:{web.server_port}/", api, args.output, args.map.name)
                browser.close()
        finally:
            web.shutdown()


if __name__ == "__main__":
    main()
