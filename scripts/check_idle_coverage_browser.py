#!/usr/bin/env python3
"""Fail if an idle map repeatedly requests square-root coverage status."""

import argparse
import json
import os
from pathlib import Path
import time
from urllib.parse import urlsplit

from playwright.sync_api import sync_playwright

from sequence_browser_harness import BrowserCheck


def cpu_ticks(pid):
    fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
    # The first field after the process name is stat field 3. Use utime + stime.
    return int(fields[11]) + int(fields[12])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ("api", "url", "file", "output"):
        parser.add_argument("--" + key, required=True)
    parser.add_argument("--seconds", type=int, default=30)
    parser.add_argument("--server-pid", type=int, help="Measure CPU for the owned Linux test server")
    args = parser.parse_args()
    if args.seconds < 1:
        parser.error("The sample window must be positive")
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    requests = {"coverage": 0, "all": 0}

    def count(request):
        requests["all"] += 1
        if urlsplit(request.url).path == "/normalization/coverage/status":
            requests["coverage"] += 1

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1440, "height": 900})
        page.add_init_script("window.__HICT_BENCHMARK_ENABLED__ = true")
        page.route("http://localhost:5000/**", lambda route: route.continue_(
            url=args.api + route.request.url.removeprefix("http://localhost:5000")))
        page.on("request", count)
        check = BrowserCheck(page, output)
        try:
            page.goto(args.url)
            page.get_by_role("button", name="Open map…", exact=True).click()
            picker = page.get_by_role("dialog", name="Open Hi-C dataset", exact=True)
            picker.get_by_text(args.file, exact=True).first.wait_for()
            check.choose_file(args.file)
            picker.get_by_role("button", name="Load", exact=True).click()
            page.wait_for_function("window.__hictBenchmark.manager()")
            check.settle()
            prompt = page.get_by_role("button", name="Show raw contacts (no balancing)", exact=True)
            if prompt.is_visible():
                prompt.click()
                check.settle()
            baseline = requests.copy()
            cpu_before = cpu_ticks(args.server_pid) if args.server_pid else None
            started = time.monotonic()
            page.wait_for_timeout(args.seconds * 1000)
            elapsed = time.monotonic() - started
            status_requests = requests["coverage"] - baseline["coverage"]
            result = {
                "file": args.file,
                "idle_seconds": elapsed,
                "status_requests": status_requests,
                "status_per_s": status_requests / elapsed,
                "all_per_s": (requests["all"] - baseline["all"]) / elapsed,
                "server_cpu_pct": None,
                "page_errors": check.errors,
            }
            if cpu_before is not None:
                ticks = cpu_ticks(args.server_pid) - cpu_before
                result["server_cpu_pct"] = 100 * ticks / os.sysconf("SC_CLK_TCK") / elapsed
            receipt = json.dumps(result, indent=2) + "\n"
            (output / "result.json").write_text(receipt)
            print(receipt, end="")
            assert result["status_per_s"] <= 0.1, result
            if cpu_before is not None:
                assert result["server_cpu_pct"] < 2, result
            assert not check.errors, check.errors
        finally:
            browser.close()


if __name__ == "__main__":
    main()
