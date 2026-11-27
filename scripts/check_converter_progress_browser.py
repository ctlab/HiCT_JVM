#!/usr/bin/env python3
"""Exercise real conversion progress and output opening on disposable matrices."""

import argparse
from functools import partial
from http.server import ThreadingHTTPServer
from pathlib import Path
import re
import shutil
import tempfile
import threading

from playwright.sync_api import expect, sync_playwright

from check_shift_selection_browser import Quiet
from sequence_browser_harness import BrowserCheck, HictTestServer, configure_test_api


def check_converter(page, web_url, api_url, output):
    page.add_init_script("window.__HICT_BENCHMARK_ENABLED__ = true")
    page.set_default_timeout(15_000)
    configure_test_api(page, api_url)
    check = BrowserCheck(page, output)
    page.goto(web_url)
    page.wait_for_function("window.__hictBenchmark")
    dialog = page.get_by_role("dialog", name="Convert matrices for HiCT")
    open_when_ready = dialog.get_by_role("checkbox", name="Open the map when it is ready")
    card = dialog.locator(".conversion-job")

    def start(input_name, auto_open=True):
        check.menu("File", "Convert matrices…")
        expect(dialog.locator(".modal-content")).to_have_css("background-color", "rgb(255, 255, 255)")
        if auto_open:
            expect(open_when_ready).to_be_checked()
        else:
            open_when_ready.uncheck()
        dialog.get_by_role("checkbox", name=f"Select {input_name}", exact=True).check()
        with page.expect_response("**/convert/jobs/batch") as submit:
            dialog.get_by_role("button", name="Convert 1 file").click()
        assert submit.value.ok, submit.value.text()
        return submit.value.json()["jobIds"][0]

    def opened(filename):
        page.wait_for_function(
            "name => window.__hictBenchmark.manager()?.getOptions().filename === name",
            arg=filename,
            timeout=180_000,
        )
        expect(dialog).not_to_be_visible()
        check.settle()

    large_id = start("ind2.self.1kb.cool")
    expect(card.get_by_text("estimating time left…", exact=False)).to_be_visible()
    expect(card.get_by_text(re.compile(r"Step \d+ of \d+"))).to_be_visible()
    page.screenshot(path=str(output / "running.png"))
    card.get_by_text("Show log", exact=True).click()
    log = card.get_by_label("Conversion log")
    expect(log).to_contain_text(re.compile(r"\b\d\d:\d\d:\d\d Using .*hictk"))
    expect(card.get_by_text("Hide log", exact=True)).to_be_visible()
    page.wait_for_function("""() => {
        const log = document.querySelector('[aria-label="Conversion log"]');
        return log && log.scrollHeight - log.clientHeight - log.scrollTop <= 4;
    }""")
    page.screenshot(path=str(output / "log-open.png"))
    expect(card.get_by_text(re.compile(r"Elapsed [0-9:]+ · about [0-9:]+ left"))).to_be_visible(timeout=90_000)
    opened("ind2.self.1kb.hict.hdf5")
    done = page.request.get(f"{api_url}/convert/jobs/{large_id}").json()
    assert done["elapsedMillis"] >= 5000 and done["outputSizeBytes"] > 0, done
    assert done["status"] == "finished", done

    start("borrelia.mcool", auto_open=False)
    open_button = dialog.get_by_role("button", name="Open map", exact=True)
    expect(open_button).to_have_class(re.compile(r"\bbtn-success\b"), timeout=90_000)
    expect(card.get_by_text(re.compile(r"Took [0-9:]+"))).to_be_visible()
    assert page.evaluate("window.__hictBenchmark.manager().getOptions().filename") == "ind2.self.1kb.hict.hdf5"
    page.screenshot(path=str(output / "finished.png"))
    open_button.click()
    opened("borrelia.hict.hdf5")

    start("truncated.mcool", auto_open=False)
    expect(card.get_by_text("failed", exact=True)).to_be_visible(timeout=90_000)
    expect(card.locator("details.analysis-log-details")).to_have_attribute("open", "")
    expect(log).to_contain_text(re.compile(r"\b\d\d:\d\d:\d\d ERROR:"))
    assert log.evaluate("el => el.scrollHeight - el.clientHeight - el.scrollTop <= 4")
    assert page.evaluate("window.__hictBenchmark.manager().getOptions().filename") == "borrelia.hict.hdf5"
    page.screenshot(path=str(output / "failed.png"))
    dialog.get_by_role("button", name="Close", exact=True).last.click()

    start("background.mcool", auto_open=False)
    dialog.get_by_role("button", name="Close", exact=True).last.click()
    completion = page.locator("[data-sonner-toast]").filter(has_text="Converted background.hict.hdf5")
    expect(completion).to_be_visible(timeout=60_000)
    assert page.evaluate("window.__hictBenchmark.manager().getOptions().filename") == "borrelia.hict.hdf5"
    completion.get_by_role("button", name="Open", exact=True).click()
    opened("background.hict.hdf5")
    assert not check.errors, check.errors
    print(f"Real hictk conversion took {done['elapsedMillis']} ms and opened the result.")
    print("Live timing and logs, manual opening, truncated-input recovery and background completion passed.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("jar", "webui", "ind2", "borrelia", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    with (
        tempfile.TemporaryDirectory(prefix="hict-converter-") as temporary,
        ThreadingHTTPServer(("127.0.0.1", 0), partial(Quiet, directory=args.webui.resolve())) as web,
        sync_playwright() as playwright,
    ):
        data = Path(temporary)
        shutil.copyfile(args.ind2, data / "ind2.self.1kb.cool")
        shutil.copyfile(args.borrelia, data / "borrelia.mcool")
        shutil.copyfile(args.borrelia, data / "background.mcool")
        broken = data / "truncated.mcool"
        shutil.copyfile(args.borrelia, broken)
        with broken.open("r+b") as stream:
            stream.truncate(broken.stat().st_size // 2)
        threading.Thread(target=web.serve_forever, daemon=True).start()
        try:
            with HictTestServer(args.jar, data, args.output / "server.log") as api:
                browser = playwright.chromium.launch()
                page = browser.new_page(viewport={"width": 1440, "height": 900})
                check_converter(page, f"http://127.0.0.1:{web.server_port}/", api.url, args.output)
                browser.close()
        finally:
            web.shutdown()


if __name__ == "__main__":
    main()
