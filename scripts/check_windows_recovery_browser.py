#!/usr/bin/env python3
"""Exercise recovery and accessibility on a disposable local map in Chromium."""

import argparse
from functools import partial
from http.server import ThreadingHTTPServer
import json
from pathlib import Path
import shutil
import tempfile
import threading
from urllib.parse import urlsplit

from playwright.sync_api import expect, sync_playwright

from check_shift_selection_browser import Quiet
from sequence_browser_harness import BrowserCheck, HictTestServer, configure_test_api


def await_route(page, routes, count):
    for _ in range(400):
        if len(routes) >= count:
            return routes[count - 1]
        page.wait_for_timeout(50)
    raise AssertionError(f"Expected {count} requests, got {len(routes)}")


def opening_overlay(page, api, axe, output):
    opens = []
    def hold_open(route):
        opens.append(route)

    page.route("**/open", hold_open)
    page.route("**/open_progress", lambda route: route.fulfill(
        json={"stage": "Reading resolutions…", "progress": 0.42}))
    page.evaluate("api => { window.__hictBenchmark.open('borrelia.hict.hdf5', api); }", api.url)
    request = await_route(page, opens, 1)
    overlay = page.get_by_role("dialog", name="Opening file", exact=True)
    progress = overlay.get_by_role("progressbar", name="Opening map", exact=True)
    expect(progress).to_have_attribute("aria-valuenow", "42")
    expect(overlay.get_by_role("button", name="Close", exact=True)).to_be_visible()
    page.add_script_tag(path=str(axe))
    audit = page.evaluate("async () => window.axe.run('.open-progress-modal')")
    (output / "loading-axe.json").write_text(json.dumps({
        "violations": audit["violations"],
        "passed": [rule["id"] for rule in audit["passes"]],
    }, indent=2))
    assert not audit["violations"], [(rule["id"], rule["description"]) for rule in audit["violations"]]
    page.screenshot(path=str(output / "loading-overlay.png"))
    overlay.get_by_role("button", name="Close", exact=True).click()
    expect(overlay).to_be_hidden()
    request.continue_()
    page.unroute("**/open", hold_open)


def fasta_discovery(page, browser_check, output):
    calls = []
    should_fail = True

    def list_fasta(route):
        calls.append(should_fail)
        if should_fail:
            route.fulfill(status=503, json={"message": "Unavailable"})
        else:
            route.continue_()

    page.route("**/list_fasta_files", list_fasta)
    for title in ("Dotplot", "ModDotPlot"):
        should_fail = True
        browser_check.menu("Tools", f"{title} from FASTA files…")
        dialog = page.get_by_role("dialog").filter(has_text=f"{title if title == 'ModDotPlot' else 'Dotplots'} from FASTA files")
        alert = dialog.get_by_role("alert")
        expect(alert).to_have_text(
            "Could not list FASTA files. The server did not answer (HTTP 503). Choose a file with Browse…, or try again."
        )
        expect(dialog.get_by_text("No FASTA files listed", exact=False)).to_have_count(0)
        expect(dialog.get_by_role("button", name="Browse…", exact=True)).to_be_enabled()
        page.screenshot(path=str(output / f"{title.lower()}-fasta-error.png"))
        should_fail = False
        dialog.get_by_role("button", name="Try again", exact=True).click()
        expect(dialog.get_by_text("No FASTA files listed", exact=False)).to_be_visible()
        expect(alert).to_have_count(0)
        page.keyboard.press("Escape")
        expect(dialog).to_be_hidden()
    assert calls == [True, False, True, False], calls
    page.unroute("**/list_fasta_files", list_fasta)


def export_retry(page, api, output):
    validations = []
    history = []
    def hold_validation(route):
        validations.append(route)

    page.route("**/visualization/captures/*/validate", hold_validation)

    def history_request(request):
        path = urlsplit(request.url).path
        if path in ("/assembly/history", "/assembly/history/restore"):
            history.append(path)

    page.on("request", history_request)
    page.locator("#export-dropdown-toggle").click()
    dialog = page.get_by_role("dialog").filter(has_text="Export figure")
    blocked = await_route(page, validations, 1)
    api.http("/reverse_selection_range", {"startBP": 0, "endBP": 25000})
    blocked.continue_()
    retry = dialog.get_by_role("button", name="Retry preview", exact=True)
    expect(retry).to_be_visible()
    start = len(history)
    retry.click()
    next_validation = await_route(page, validations, 2)
    assert history[start:start+2] == ["/assembly/history", "/assembly/history/restore"], history[start:]
    expect(dialog.get_by_text(
        "The assembly was changed in another window. HiCT reloaded it and is making the preview again.",
        exact=True,
    )).to_be_visible()
    page.screenshot(path=str(output / "export-refresh.png"))
    next_validation.continue_()
    expect(dialog.get_by_text("Figure preview ready.", exact=True)).to_be_visible()
    expect(dialog.locator("#export-png-button")).to_be_enabled()
    assert len(validations) == 2
    page.keyboard.press("Escape")
    expect(dialog).to_be_hidden()
    page.unroute("**/visualization/captures/*/validate", hold_validation)


def context_focus(page):
    diagram = page.get_by_role("region", name="Contact map", exact=True)
    start, end = page.evaluate("""() => {
        const map = window.__hictBenchmark.manager().getMap();
        const [width, height] = map.getSize();
        return [[width * 0.30, height * 0.28], [width * 0.38, height * 0.36]];
    }""")
    origin = diagram.bounding_box()
    at = lambda point: (origin["x"] + point[0], origin["y"] + point[1])
    page.keyboard.down("Control")
    page.mouse.move(*at(start))
    page.mouse.down()
    page.mouse.move(*at(end), steps=12)
    page.mouse.up()
    page.keyboard.up("Control")
    page.mouse.click(*at(end), button="right")
    page.get_by_role("menuitem", name="Analyse", exact=True).hover()
    page.get_by_role("menuitem", name="Dotplot of region…", exact=True).click()
    dialog = page.locator('[role="dialog"]:visible').last
    expect(dialog).to_be_visible()
    page.keyboard.press("Escape")
    expect(dialog).to_be_hidden()
    expect(diagram).to_be_focused()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("jar", "webui", "borrelia", "axe", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    with (
        tempfile.TemporaryDirectory(prefix="hict-windows-recovery-") as temporary,
        ThreadingHTTPServer(("127.0.0.1", 0), partial(Quiet, directory=args.webui.resolve())) as web,
        sync_playwright() as playwright,
    ):
        data = Path(temporary)
        shutil.copyfile(args.borrelia, data / "borrelia.hict.hdf5")
        threading.Thread(target=web.serve_forever, daemon=True).start()
        try:
            with HictTestServer(args.jar, data, args.output / "server.log") as api:
                browser = playwright.chromium.launch()
                page = browser.new_page(viewport={"width": 1440, "height": 900})
                page.set_default_timeout(15000)
                page.add_init_script("window.__HICT_BENCHMARK_ENABLED__=true")
                configure_test_api(page, api.url)
                check = BrowserCheck(page, args.output)
                page.goto(f"http://127.0.0.1:{web.server_port}/")
                page.wait_for_function("window.__hictBenchmark")
                opening_overlay(page, api, args.axe, args.output)
                check.settle()
                print("Opening-overlay accessibility passed.", flush=True)
                fasta_discovery(page, check, args.output)
                print("Both FASTA generator failure and retry states passed.", flush=True)
                export_retry(page, api, args.output)
                print("Concurrent-assembly export retry passed.", flush=True)
                context_focus(page)
                print("Context dialog returned focus to Contact map.", flush=True)
                browser.close()
        finally:
            web.shutdown()


if __name__ == "__main__":
    main()
