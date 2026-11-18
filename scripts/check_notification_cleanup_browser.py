#!/usr/bin/env python3
"""Repeated real PNG saves must dispose notification listeners without changing the figure."""

import argparse
import functools
import hashlib
import http.server
import json
from pathlib import Path
import subprocess
import threading

from PIL import Image
from playwright.sync_api import expect, sync_playwright
from sequence_browser_harness import BrowserCheck, HictTestServer


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ("jar", "output"):
        parser.add_argument("--" + key, required=True)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--webui")
    group.add_argument("--url", help="Existing owned Vite development server")
    parser.add_argument("--cycles", type=int, default=12)
    args = parser.parse_args()
    assert 1 <= args.cycles <= 100
    out = Path(args.output).absolute()
    assert out == out.resolve() and out.is_relative_to("/tmp")
    out.mkdir(exist_ok=False)
    data = out / "data"
    data.mkdir()
    jar = Path(args.jar).resolve()
    with (out / "fixture.log").open("w") as log:
        subprocess.run(
            [
                "java",
                "--class-path",
                str(jar),
                "src/test/java/ru/itmo/ctlab/hict/fixtures/ReinforcementHdf5Fixture.java",
                str(data),
            ],
            check=True,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
    native = data / "tiny-primary.hict.hdf5"
    original = hashlib.sha256(native.read_bytes()).hexdigest()
    web = None
    if args.webui:

        class Quiet(http.server.SimpleHTTPRequestHandler):
            def log_message(self, *_):
                pass

        web = http.server.ThreadingHTTPServer(
            ("127.0.0.1", 0),
            functools.partial(Quiet, directory=str(Path(args.webui).resolve())),
        )
        threading.Thread(target=web.serve_forever, daemon=True).start()
    url = args.url or f"http://127.0.0.1:{web.server_port}/"
    rows, errors = [], []
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
            page.on("pageerror", lambda e: errors.append(str(e)))
            page.on(
                "response",
                lambda r: (
                    errors.append(f"{r.status} {r.url}") if r.status >= 400 else None
                ),
            )
            cdp = page.context.new_cdp_session(page)

            def listener_count():
                target = cdp.send(
                    "Runtime.evaluate",
                    {"expression": "document", "objectGroup": "notification-audit"},
                )["result"]["objectId"]
                listeners = cdp.send(
                    "DOMDebugger.getEventListeners", {"objectId": target}
                )["listeners"]
                result = sum(row["type"] == "visibilitychange" for row in listeners)
                identifiers = {
                    row[key]["objectId"]
                    for row in listeners
                    for key in ("handler", "originalHandler")
                    if row.get(key, {}).get("objectId")
                }
                for identifier in identifiers:
                    cdp.send("Runtime.releaseObject", {"objectId": identifier})
                cdp.send(
                    "Runtime.releaseObjectGroup", {"objectGroup": "notification-audit"}
                )
                return result

            def dismiss_notifications():
                while page.locator(
                    '[data-sonner-toast] [data-close-button="true"]'
                ).count():
                    page.locator(
                        '[data-sonner-toast] [data-close-button="true"]'
                    ).first.click()
                    page.wait_for_timeout(250)
                expect(page.locator("[data-sonner-toast]")).to_have_count(0)

            try:
                page.goto(url)
                page.evaluate(
                    "api=>window.__hictBenchmark.open('tiny-primary.hict.hdf5',api)",
                    api.url,
                )
                check.settle()
                dismiss_notifications()
                baseline = listener_count()
                for cycle in range(args.cycles):
                    page.locator("#export-dropdown-toggle").click()
                    dialog = page.get_by_role(
                        "dialog", name="Export figure", exact=True
                    )
                    expect(dialog.locator("#export-png-button")).to_be_enabled(
                        timeout=90000
                    )
                    with page.expect_download() as download:
                        dialog.locator("#export-png-button").click()
                    png = out / "latest.png"
                    download.value.save_as(png)
                    with Image.open(png) as image:
                        assert image.format == "PNG" and min(image.size) > 0
                    dialog.get_by_role("button", name="Close", exact=True).last.click()
                    expect(dialog).to_have_count(0)
                    dismiss_notifications()
                    cdp.send("Runtime.discardConsoleEntries")
                    cdp.send("HeapProfiler.collectGarbage")
                    count = listener_count()
                    rows.append(
                        {
                            "cycle": cycle,
                            "documentVisibilityListeners": count,
                            "baseline": baseline,
                        }
                    )
                    assert count == baseline, rows[-1]
                assert hashlib.sha256(native.read_bytes()).hexdigest() == original
                assert not errors and not check.errors, errors + check.errors
            finally:
                page.screenshot(path=str(out / "final.png"))
                (out / "results.json").write_text(
                    json.dumps(
                        {
                            "cycles": rows,
                            "errors": errors + check.errors,
                            "sourceSha256": original,
                            "url": url,
                        },
                        indent=2,
                    )
                    + "\n"
                )
                browser.close()
    finally:
        if web:
            web.shutdown()
            web.server_close()
    print(json.dumps({"cycles": len(rows), "output": str(out)}))


if __name__ == "__main__":
    main()
