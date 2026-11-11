#!/usr/bin/env python3
"""Open the public quick-start example in three clicks in a fresh browser profile."""

import argparse
import io
import json
import time
from pathlib import Path

import numpy as np
from PIL import Image
from playwright.sync_api import sync_playwright

parser = argparse.ArgumentParser()
for name in ("url", "api", "output"):
    parser.add_argument("--" + name, required=True)
args = parser.parse_args()
out = Path(args.output)
out.mkdir(parents=True, exist_ok=True)
with sync_playwright() as pw:
    browser = pw.chromium.launch(headless=True)
    page = browser.new_page(viewport={"width": 1440, "height": 1000})
    errors, console_errors = [], []
    page.on("pageerror", lambda error: errors.append(str(error)))
    page.on(
        "crash",
        lambda: (out / "crash.json").write_text(
            json.dumps({"errors": errors, "consoleErrors": console_errors})
        ),
    )
    page.on(
        "console",
        lambda message: (
            console_errors.append(message.text) if message.type == "error" else None
        ),
    )
    page.add_init_script(
        """window.__HICT_BENCHMARK_ENABLED__=true;
      window.quickStartClicks=[];
      document.addEventListener('click',event=>{if(event.isTrusted)
        window.quickStartClicks.push((event.target.textContent||event.target.ariaLabel||'').trim());},true);"""
    )
    page.route(
        "http://localhost:5000/**",
        lambda route: route.continue_(
            url=args.api + route.request.url.removeprefix("http://localhost:5000")
        ),
    )
    try:
        page.goto(args.url)
        page.wait_for_function("window.__hictBenchmark")
        started = time.monotonic()
        page.get_by_role("link", name="Help", exact=True).click()
        page.get_by_role("link", name="Examples...", exact=True).click()
        dialog = page.get_by_role("dialog", name="Examples", exact=True)
        card = dialog.locator('[data-example="dnazoo-tardigrade"]')
        card.get_by_role("button", name="Open example", exact=True).click()
        page.wait_for_function(
            """() => window.__hictBenchmark.manager()?.getOptions().filename
          ==='examples/dnazoo-tardigrade/nHd_3.1_HiC.hict.hdf5'""",
            timeout=300000,
        )
        page.wait_for_function(
            "!document.querySelector('.open-progress-modal')", timeout=60000
        )
        page.wait_for_timeout(1500)
        clicks = page.evaluate("window.quickStartClicks")
        assert len(clicks) == 3, clicks
        assert (
            page.get_by_role("dialog", name="Open File Wizard", exact=True).count() == 0
        )
        state = page.evaluate(
            """() => {const m=window.__hictBenchmark.manager();return {
          filename:m.getOptions().filename,fasta:m.getOptions().fastaFilename,
          contigs:m.contigDimensionHolder.contig_count,
          resolutions:m.viewAndLayersManager.getAllNavigationBpResolutions()};}"""
        )
        assert state["contigs"] == 5, state
        response = page.request.post(
            args.api + "/sequence/region", data={"startBp": 0, "endBp": 1000}
        )
        assert response.ok, response.text()
        sequence = response.json()["sequence"]
        assert (
            len(sequence) >= 1000 and sum(base in "ACGTacgt" for base in sequence) > 900
        )
        viewport = page.evaluate_handle(
            "() => window.__hictBenchmark.manager().getMap().getViewport()"
        ).as_element()
        pixels = np.array(
            Image.open(io.BytesIO(viewport.screenshot())).convert("RGB")
        ).astype(int)
        red_pixels = int(
            (
                (pixels[:, :, 0] > pixels[:, :, 1] + 20)
                & (pixels[:, :, 0] > pixels[:, :, 2] + 20)
            ).sum()
        )
        assert red_pixels > 1000, red_pixels
        assert not errors, errors
        assert not console_errors, console_errors
        page.screenshot(path=str(out / "quick-start.png"))
        result = {
            "secondsToMap": time.monotonic() - started,
            "clicks": clicks,
            "map": state,
            "redSignalPixels": red_pixels,
            "sequenceStart": sequence[:100],
            "errors": errors,
            "consoleErrors": console_errors,
            "jobs": page.request.get(args.api + "/jobs").json(),
        }
        (out / "results.json").write_text(json.dumps(result, indent=2))
        print(json.dumps(result), flush=True)
    except BaseException:
        import traceback

        (out / "failure.txt").write_text(traceback.format_exc())
        (out / "errors.json").write_text(
            json.dumps({"errors": errors, "consoleErrors": console_errors})
        )
        try:
            page.screenshot(path=str(out / "failure.png"))
            (out / "failure.html").write_text(page.content())
        except Exception:
            pass
        raise
    finally:
        browser.close()
