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


def measure_window(page, requests, seconds, server_pid):
    baseline = requests.copy()
    cpu_before = cpu_ticks(server_pid) if server_pid else None
    started = time.monotonic()
    page.wait_for_timeout(seconds * 1000)
    elapsed = time.monotonic() - started
    status_requests = requests["coverage"] - baseline["coverage"]
    server_cpu_pct = None
    if cpu_before is not None:
        ticks = cpu_ticks(server_pid) - cpu_before
        server_cpu_pct = 100 * ticks / os.sysconf("SC_CLK_TCK") / elapsed
    return {
        "idle_seconds": elapsed,
        "status_requests": status_requests,
        "status_per_s": status_requests / elapsed,
        "all_per_s": (requests["all"] - baseline["all"]) / elapsed,
        "server_cpu_pct": server_cpu_pct,
    }


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
            # The JVM can finish compiling map-opening code after the first
            # paint. Measure that window too, and gate CPU on steady-state idle.
            startup = None
            if args.server_pid is not None:
                startup = measure_window(page, requests, args.seconds, args.server_pid)
            result = {
                "file": args.file,
                **measure_window(page, requests, args.seconds, args.server_pid),
                "page_errors": check.errors,
            }
            if startup is not None:
                result["post_open_window"] = startup
            receipt = json.dumps(result, indent=2) + "\n"
            (output / "result.json").write_text(receipt)
            print(receipt, end="")
            assert result["status_per_s"] <= 0.1, result
            if startup is not None:
                assert startup["status_per_s"] <= 0.1, result
                assert result["server_cpu_pct"] < 2, result
            assert not check.errors, check.errors
        finally:
            browser.close()


if __name__ == "__main__":
    main()
