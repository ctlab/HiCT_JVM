#!/usr/bin/env python3
"""Check the result manager, immutable downloads, reopening and session round-trip."""

import argparse
from functools import partial
import hashlib
from http.server import ThreadingHTTPServer
import json
from pathlib import Path
import shutil
import tempfile
import threading

from playwright.sync_api import expect, sync_playwright

from check_shift_selection_browser import Quiet
from sequence_browser_harness import BrowserCheck, HictTestServer, configure_test_api


def check_results(browser, web, app, data, output):
    page = browser.new_page(viewport={"width": 1440, "height": 900})
    configure_test_api(page, app.url)
    page.add_init_script("window.__HICT_BENCHMARK_ENABLED__ = true")
    check = BrowserCheck(page, output)
    page.goto(web)

    def open_map(filename, fasta=None):
        page.evaluate("([file, api, fasta]) => window.__hictBenchmark.open(file, api, fasta)", [filename, app.url, fasta])
        check.settle()

    def sets():
        return json.loads(app.http("/sequence/overlays/list", {}))

    def generate(start, end, bp, index):
        page.evaluate("""([start,end]) => window.__hictBenchmark.manager().viewAndLayersManager.setRegionBounds(
            {rowStart:start,rowEnd:end,columnStart:start,columnEnd:end})""", [start, end])
        check.menu("Tools", "ModDotPlot…")
        dialog = page.get_by_role("dialog", name="ModDotPlot", exact=True)
        dialog.get_by_label("Finest detail", exact=True).select_option(str(bp))
        dialog.get_by_label("Show result as", exact=True).select_option("below")
        dialog.get_by_role("button", name="Generate", exact=True).click()
        expect(dialog.get_by_text(f"Added region {index}", exact=False)).to_be_visible(timeout=90_000)
        return dialog

    def open_results():
        check.menu("Tools", "Dotplot results…")
        return page.get_by_role("dialog", name="Dotplot results", exact=True)

    open_map("borrelia.hict.hdf5", "borrelia-matching.fa")
    first = generate(10_000, 80_000, 1000, 1)
    first.get_by_role("button", name="Generate another region", exact=True).click()
    finished = generate(500_000, 600_000, 2000, 2)
    expect(finished.get_by_role("button", name="Save a copy…", exact=True)).to_be_visible()
    page.screenshot(path=str(output / "finished.png"))
    finished.get_by_role("button", name="Dotplot results…", exact=True).click()
    dialog = page.get_by_role("dialog", name="Dotplot results", exact=True)
    expect(dialog.get_by_role("button", name="Go to", exact=True)).to_have_count(2)
    expect(dialog.get_by_text("Shown below diagonal", exact=True)).to_be_visible()
    page.screenshot(path=str(output / "result-sets.png"))
    result, = sets()
    first_region, second_region = result["regions"]
    assert first_region["rows"] == {"start": 10_000, "end": 80_000}, first_region
    assert second_region["rows"] == {"start": 500_000, "end": 600_000}, second_region

    dialog.get_by_role("button", name="Go to", exact=True).first.click()
    expect(dialog).to_be_hidden()
    check.settle()
    expect(page.locator('.dotplot-region-outlines g[data-region="1"]')).to_be_visible()
    assert page.evaluate("window.__hictBenchmark.manager().viewAndLayersManager.getVisibleSourceResolutionDescriptors().secondary.bpResolution") == 1000

    check.menu("Tools", "ModDotPlot…")
    settings = page.get_by_role("dialog", name="ModDotPlot", exact=True)
    check.advanced()
    settings.locator("#moddotplot-identity").fill("86")
    expect(settings.get_by_text("These settings differ from your existing ModDotPlot results", exact=False)).to_be_visible()
    settings.get_by_role("button", name="Close", exact=True).first.click()

    dialog = open_results()
    with page.expect_download() as downloaded:
        dialog.get_by_role("button", name="Save a copy…", exact=True).click()
    assert downloaded.value.suggested_filename == "borrelia.moddotplot.hict.hdf5"
    copy = data / "saved.moddotplot.hict.hdf5"
    downloaded.value.save_as(copy)
    generated = data / result["file"]
    digest = hashlib.sha256(generated.read_bytes()).hexdigest()
    assert hashlib.sha256(copy.read_bytes()).hexdigest() == digest

    dialog.get_by_role("button", name="Delete…", exact=True).click()
    confirmation = page.get_by_role("alertdialog", name="Delete this result set?")
    with page.expect_response("**/sequence/overlays/delete") as deleted:
        confirmation.get_by_role("button", name="Delete", exact=True).click()
    assert deleted.value.ok, deleted.value.text()
    expect(dialog.get_by_text("No dotplot results for this map.", exact=True)).to_be_visible()
    assert not generated.exists() and copy.is_file()

    def open_copy(status):
        dialog.get_by_role("button", name="Open dotplot file…", exact=True).click()
        picker = page.get_by_role("dialog", name="Open dotplot file", exact=True)
        check.choose_file(copy.name)
        with page.expect_response("**/sequence/overlays/open") as opened:
            picker.get_by_role("button", name="Load", exact=True).click()
        assert opened.value.status == status, opened.value.text()

    open_copy(200)
    expect(dialog.get_by_role("button", name="Go to", exact=True)).to_have_count(2)
    restored, = sets()
    assert restored["id"] != result["id"] and restored["shown"]
    assert hashlib.sha256((data / restored["file"]).read_bytes()).hexdigest() == digest
    dialog.get_by_role("button", name="Close", exact=True).last.click()

    with page.expect_download() as session_download:
        check.menu("File", "Save session…")
    saved_session = output / "dotplot-session.json"
    session_download.value.save_as(saved_session)
    check.menu("File", "Close map")
    page.get_by_role("heading", name="No contact map open").wait_for()
    with page.expect_file_chooser() as chooser:
        check.menu("File", "Open session…")
    chooser.value.set_files(saved_session)
    page.get_by_text("Session restored. Undo history starts from the restored assembly.").wait_for()
    check.settle()
    session_result, = sets()
    assert session_result["file"] == restored["file"] and session_result["shown"]
    assert session_result["displayMode"] == "below"

    open_map("different.hict.hdf5")
    dialog = open_results()
    open_copy(409)
    expect(dialog.get_by_role("alert")).to_contain_text("This dotplot file belongs to a different map.")
    expect(dialog.get_by_role("alert")).to_contain_text("It was made for “borrelia.hict.hdf5” (other contigs or resolutions).")
    page.screenshot(path=str(output / "mismatched-map.png"))
    (output / "checks.json").write_text(json.dumps({"first": result, "savedCopySha256": digest, "restored": restored}, indent=2))
    expected_conflict = "Failed to load resource: the server responded with a status of 409 (Conflict)"
    if expected_conflict in check.errors:
        check.errors.remove(expected_conflict)
    assert not check.errors, check.errors
    page.close()
    print("Both regions, Go to, byte-identical download, deletion, reopen, session and mismatch passed.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("jar", "webui", "borrelia", "fasta", "other", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    with (tempfile.TemporaryDirectory(prefix="hict-result-check-") as temporary,
          ThreadingHTTPServer(("127.0.0.1", 0), partial(Quiet, directory=args.webui.resolve())) as web,
          sync_playwright() as playwright):
        data = Path(temporary)
        for source, filename in [(args.borrelia, "borrelia.hict.hdf5"), (args.fasta, "borrelia-matching.fa"),
                                 (args.other, "different.hict.hdf5")]:
            shutil.copyfile(source, data / filename)
        threading.Thread(target=web.serve_forever, daemon=True).start()
        try:
            with HictTestServer(args.jar, data, args.output / "server.log") as app:
                browser = playwright.chromium.launch()
                check_results(browser, f"http://127.0.0.1:{web.server_port}/", app, data, args.output)
                browser.close()
        finally:
            web.shutdown()


if __name__ == "__main__":
    main()
