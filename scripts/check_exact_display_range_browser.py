#!/usr/bin/env python3
"""Qualify whole-map display-signal limits and cancellation through actual controls."""

import argparse
import functools
import hashlib
import http.server
import json
import math
from pathlib import Path
import subprocess
import threading
from playwright.sync_api import expect, sync_playwright
from sequence_browser_harness import BrowserCheck, HictTestServer


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ("jar", "webui", "data-dir", "output"):
        parser.add_argument("--" + key, required=True)
    args = parser.parse_args()
    data, out = Path(args.data_dir).absolute(), Path(args.output).absolute()
    if any(p != p.resolve() or not p.is_relative_to("/tmp") for p in (data, out)):
        parser.error("Use fresh nonsymlinked disposable /tmp paths")
    data.mkdir(exist_ok=False)
    out.mkdir(exist_ok=False)
    jar = Path(args.jar).resolve()
    with (out / "fixture.log").open("w") as log:
        subprocess.run(
            [
                "java",
                "--class-path",
                str(jar),
                str(
                    Path(__file__).resolve().parents[1]
                    / "src/test/java/ru/itmo/ctlab/hict/fixtures/ReinforcementHdf5Fixture.java"
                ),
                "statistics",
                str(data),
            ],
            check=True,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
    source = data / "scan.hict.hdf5"
    before = hashlib.sha256(source.read_bytes()).hexdigest()

    class Quiet(http.server.SimpleHTTPRequestHandler):
        def log_message(self, *_):
            pass

    web = http.server.ThreadingHTTPServer(
        ("127.0.0.1", 0),
        functools.partial(Quiet, directory=str(Path(args.webui).resolve())),
    )
    threading.Thread(target=web.serve_forever, daemon=True).start()
    results = []
    requests = []
    try:
        with HictTestServer(
            jar, data, out / "server.log"
        ) as api, sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            page = browser.new_page(viewport={"width": 1440, "height": 1000})
            page.set_default_timeout(30000)
            page.add_init_script("window.__HICT_BENCHMARK_ENABLED__=true")
            page.route(
                "http://localhost:5000/**",
                lambda r: r.continue_(
                    url=api.url + r.request.url.removeprefix("http://localhost:5000")
                ),
            )
            check = BrowserCheck(page, out)
            page.on(
                "request",
                lambda r: (
                    requests.append(json.loads(r.post_data))
                    if r.url.endswith("/matrix/statistics")
                    else None
                ),
            )

            def post(path, body=None):
                return json.loads(api.http(path, body or {}))

            def options():
                return post("/get_visualization_options")

            def tile_hash():
                response = page.request.get(
                    api.url + "/get_tile",
                    params={
                        "bpResolution": 4,
                        "format": "PNG_BY_PIXELS",
                        "row": 0,
                        "col": 0,
                        "rows": 5,
                        "cols": 5,
                    },
                )
                assert response.ok, response.text()
                return hashlib.sha256(response.body()).hexdigest()

            def wait():
                page.evaluate(
                    "()=>window.__hictBenchmark.manager().visualizationManager.waitForPendingOptionRequests()"
                )
                check.settle()

            def record(name):
                page.screenshot(path=str(out / (name + ".png")))
                results.append({"test": name, "status": "passed"})
                print("Passed", name, flush=True)

            def mode(value):
                control = page.locator('select[title^="Expected and O/E"]')
                control.select_option(value)
                expect(control).to_be_enabled()
                wait()
                assert options()["signalDisplayMode"] == value

            def scan():
                page.get_by_role("button", name="Scan whole map", exact=True).click()
                expect(page.get_by_test_id("exact-range-status")).to_contain_text(
                    "finite cells", timeout=60000
                )
                expect(
                    page.get_by_role("button", name="Cancel scan", exact=True)
                ).to_have_count(0)

            def bounds():
                return [
                    page.get_by_role(
                        "spinbutton", name="Slider minimum signal", exact=True
                    ).input_value(),
                    page.get_by_role(
                        "spinbutton", name="Slider maximum signal", exact=True
                    ).input_value(),
                ]

            try:
                page.goto(f"http://127.0.0.1:{web.server_port}/")
                page.wait_for_function("window.__hictBenchmark")
                page.evaluate(
                    'api=>window.__hictBenchmark.open("scan.hict.hdf5",api)', api.url
                )
                check.settle()
                page.locator("#normalization-mode").select_option("none")
                wait()
                page.locator("#signal-transform-linear").click()
                wait()
                advanced = page.locator("#signal-advanced")
                advanced.locator(":scope > summary").click()
                page.locator("#scaling-across-zoom").select_option("none")
                wait()
                page.get_by_role(
                    "spinbutton", name="Lower signal threshold", exact=True
                ).fill("0")
                page.get_by_role(
                    "spinbutton", name="Upper signal threshold", exact=True
                ).fill("8")
                page.get_by_role("button", name="Apply", exact=True).click()
                wait()
                details = page.locator(".range-details")
                details.locator(":scope > summary").click()
                graph = post("/render_pipeline/get")
                opts = options()
                start = len(requests)
                scan()
                expect(page.get_by_test_id("exact-range-status")).to_contain_text(
                    "263169 finite cells"
                )
                assert list(map(float, bounds())) == [0, 8], bounds()
                assert all(
                    r.get("signalMode") == "PIPELINE_SIGNAL" for r in requests[start:]
                )
                assert graph == post("/render_pipeline/get") and opts == options()
                record("observed-scan-uses-graph-input-and-retains-committed-options")
                for value, maximum, count in [
                    ("EXPECTED", 5, 263169),
                    ("OBSERVED_OVER_EXPECTED", 1.6, 515),
                ]:
                    mode(value)
                    # Deliberately fit a different viewport profile (mean 2, not whole-map mean 5).
                    post(
                        "/visualization/expected_profile",
                        {
                            "bpResolution": 4,
                            "startRowPx": 0,
                            "endRowPx": 1,
                            "startColPx": 0,
                            "endColPx": 1,
                        },
                    )
                    opts = options()
                    graph = post("/render_pipeline/get")
                    before_tile = tile_hash()
                    start = len(requests)
                    scan()
                    status = page.get_by_test_id("exact-range-status")
                    expect(status).to_contain_text("whole-map")
                    expect(status).to_contain_text(str(count) + " finite cells")
                    assert list(map(float, bounds())) == [0, maximum], bounds()
                    blocks = [r for r in requests[start:] if not r.get("metadataOnly")]
                    assert len(blocks) == 4 and all(
                        r.get("signalMode") == "TRADITIONAL_NORMALIZED"
                        and "expression" not in r
                        for r in blocks
                    ), blocks
                    assert graph == post("/render_pipeline/get") and opts == options()
                    assert before_tile == tile_hash(), "Scan changed rendered pixels"
                    # The viewport's single-cell fit remains 2, so its supported O/E is 1.
                    visible = post(
                        "/matrix/query",
                        {
                            "bpResolution": 4,
                            "unit": "PIXELS",
                            "startRowPx": 0,
                            "endRowPx": 1,
                            "startColPx": 0,
                            "endColPx": 1,
                            "signalMode": "TRADITIONAL_NORMALIZED",
                            "format": "JSON",
                        },
                    )
                    assert visible["values"][0] == (
                        2 if value == "EXPECTED" else 1
                    ), visible
                    expect(
                        page.get_by_role(
                            "button", name="Use full loaded range", exact=True
                        )
                    ).to_have_count(0)
                    record(
                        value.lower()
                        + "-scan-uses-global-profile-across-four-blocks-without-changing-viewport"
                    )
                    start = len(requests)
                    scan()
                    assert all(
                        r.get("metadataOnly") for r in requests[start:]
                    ), requests[start:]
                    record(
                        value.lower()
                        + "-repeat-reuses-exact-range-within-the-same-revision"
                    )
                # Hold a completed first block after a new mode invalidates the previous cache.
                mode("EXPECTED")
                held = []

                def hold(route):
                    body = json.loads(route.request.post_data)
                    if not body.get("metadataOnly") and not held:
                        held.append((route, route.fetch()))
                    else:
                        route.continue_()

                page.route("**/matrix/statistics", hold)
                previous = bounds()
                page.get_by_role("button", name="Scan whole map", exact=True).click()
                page.wait_for_function("true")
                for _ in range(300):
                    if held:
                        break
                    page.wait_for_timeout(20)
                assert held
                page.get_by_role("button", name="Cancel scan", exact=True).click()
                expect(page.get_by_test_id("exact-range-status")).to_contain_text(
                    "cancelled"
                )
                held[0][0].fulfill(response=held[0][1])
                page.unroute("**/matrix/statistics", hold)
                page.wait_for_timeout(300)
                assert bounds() == previous
                record("cancelled-block-cannot-publish-late-bounds")
                scan()
                record("cancelled-scan-can-be-retried")
                mode("OBSERVED_OVER_EXPECTED")
                held = []
                page.route("**/matrix/statistics", hold)
                page.get_by_role("button", name="Scan whole map", exact=True).click()
                for _ in range(300):
                    if held:
                        break
                    page.wait_for_timeout(20)
                assert held
                mode("OBSERVED")
                previous = bounds()
                held[0][0].fulfill(response=held[0][1])
                page.unroute("**/matrix/statistics", hold)
                page.wait_for_timeout(300)
                assert bounds() == previous
                expect(page.get_by_test_id("exact-range-status")).not_to_contain_text(
                    "whole-map O/E"
                )
                record("mode-switch-rejects-obsolete-derived-limits")
                page.locator("#signal-transform-log").click()
                scan()
                assert options()["postLogBase"] == 2
                assert abs(float(bounds()[1]) - math.log2(9)) < 1e-10, bounds()
                record("scan-waits-for-the-new-processing-draft-before-reading-limits")
                page.set_viewport_size({"width": 640, "height": 800})
                check.settle()
                page.get_by_role(
                    "button", name="Scan whole map", exact=True
                ).scroll_into_view_if_needed()
                scan()
                record("exact-scan-controls-and-results-remain-usable-at-640")
                assert before == hashlib.sha256(source.read_bytes()).hexdigest()
                assert not check.errors, check.errors
                (out / "results.json").write_text(
                    json.dumps(
                        {
                            "status": "ok",
                            "checks": results,
                            "sourceSha256": before,
                            "errors": check.errors,
                            "statisticsRequests": requests,
                        },
                        indent=2,
                    )
                )
            except BaseException:
                page.screenshot(path=str(out / "failure.png"))
                (out / "requests.json").write_text(json.dumps(requests, indent=2))
                (out / "errors.json").write_text(json.dumps(check.errors))
                raise
            finally:
                browser.close()
    finally:
        web.shutdown()
        web.server_close()


if __name__ == "__main__":
    main()
