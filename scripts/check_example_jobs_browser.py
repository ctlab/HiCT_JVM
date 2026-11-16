#!/usr/bin/env python3
"""Exercise real, controlled TLS example downloads through catalog and Jobs UI."""

import re
import sys
import shutil
import argparse, functools, hashlib, http.server, json, os, signal, subprocess, threading, time
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import Request, urlopen
from playwright.sync_api import expect, sync_playwright
from sequence_browser_harness import BrowserCheck, HictTestServer


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ("jar", "webui", "data-dir", "output"):
        parser.add_argument("--" + key, required=True)
    parser.add_argument("--hdf-python", default=sys.executable)
    args = parser.parse_args()
    data, out = Path(args.data_dir).absolute(), Path(args.output).absolute()
    if any(p != p.resolve() or not p.is_relative_to("/tmp") for p in (data, out)):
        parser.error("Use fresh disposable /tmp paths")
    data.mkdir(exist_ok=False)
    out.mkdir(exist_ok=False)
    original_jar = Path(args.jar).resolve()
    snapshot = original_jar.stat()
    jar = out / "example-jobs-runtime.jar"
    shutil.copyfile(original_jar, jar)
    after = original_jar.stat()
    assert (snapshot.st_ino, snapshot.st_size, snapshot.st_mtime_ns) == (
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
    ), "Finish the build before testing"
    scripts = Path(__file__).resolve().parent
    with (out / "fixture.log").open("w") as log:
        subprocess.run(
            [
                "java",
                "--class-path",
                str(jar),
                str(
                    scripts.parent
                    / "src/test/java/ru/itmo/ctlab/hict/fixtures/ReinforcementHdf5Fixture.java"
                ),
                str(data),
            ],
            check=True,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        subprocess.run(
            [
                args.hdf_python,
                str(scripts / "prepare_example_jobs_cooler.py"),
                str(data),
            ],
            check=True,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        subprocess.run(
            [
                "keytool",
                "-genkeypair",
                "-alias",
                "fixture",
                "-keystore",
                str(out / "fixture.p12"),
                "-storetype",
                "PKCS12",
                "-storepass",
                "fixture-only",
                "-keypass",
                "fixture-only",
                "-keyalg",
                "RSA",
                "-keysize",
                "2048",
                "-dname",
                "CN=127.0.0.1",
                "-ext",
                "SAN=ip:127.0.0.1",
                "-validity",
                "2",
                "-noprompt",
            ],
            check=True,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        subprocess.run(
            [
                "javac",
                "-cp",
                str(jar),
                "-d",
                str(out / "classes"),
                str(
                    scripts.parent
                    / "src/test/java/ru/itmo/ctlab/hict/hict_server/examples/ExampleJobsBrowserServer.java"
                ),
            ],
            check=True,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
    source_hash = hashlib.sha256(
        (data / "tiny-primary.hict.hdf5").read_bytes()
    ).hexdigest()

    class Quiet(http.server.SimpleHTTPRequestHandler):
        def log_message(self, *_):
            pass

    web = http.server.ThreadingHTTPServer(
        ("127.0.0.1", 0),
        functools.partial(Quiet, directory=str(Path(args.webui).resolve())),
    )
    threading.Thread(target=web.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{web.server_port}/"
    fixture = None
    rows = []

    def api_json(base, path, body=None):
        request = Request(
            base + path,
            data=None if body is None else json.dumps(body).encode(),
            headers={"Content-Type": "application/json"},
        )
        with urlopen(request, timeout=20) as r:
            return json.loads(r.read())

    try:
        with (out / "download-server.log").open("w") as fixture_log:
            fixture = subprocess.Popen(
                [
                    "java",
                    "-Djavax.net.ssl.trustStore=" + str(out / "fixture.p12"),
                    "-Djavax.net.ssl.trustStorePassword=fixture-only",
                    "-cp",
                    str(out / "classes") + os.pathsep + str(jar),
                    "ru.itmo.ctlab.hict.hict_server.examples.ExampleJobsBrowserServer",
                    str(data),
                    str(out),
                    url.rstrip("/"),
                ],
                stdout=fixture_log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
            deadline = time.monotonic() + 30
            while not (out / "fixture-info.json").exists():
                if fixture.poll() is not None:
                    raise RuntimeError("Fixture exited, see download-server.log")
                if time.monotonic() > deadline:
                    raise TimeoutError("Fixture startup timed out")
                time.sleep(0.1)
            fixture_info = json.loads((out / "fixture-info.json").read_text())
            fx = fixture_info["api"]
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
                        url=api.url
                        + r.request.url.removeprefix("http://localhost:5000")
                    ),
                )
                page.route(
                    "**/examples/**",
                    lambda r: r.continue_(url=fx + urlparse(r.request.url).path),
                )
                held_jobs = []
                hold_job_example = None
                held_conversions = []
                hold_conversion_start = False
                conversion_submissions = []
                cors = {"Access-Control-Allow-Origin": url.rstrip("/")}

                def combined_jobs(route):
                    if urlparse(route.request.url).path != "/jobs":
                        return route.fallback()
                    values = api_json(fx, "/jobs") + api_json(api.url, "/jobs")
                    values.sort(
                        key=lambda job: job.get("createdAtMs") or 0, reverse=True
                    )
                    if hold_job_example and any(
                        j.get("details", {}).get("exampleId") == hold_job_example
                        and j["kind"] == "conversion"
                        for j in values
                    ):
                        held_jobs.append((route, values))
                        return
                    route.fulfill(json=values, headers=cors)

                def cancel_job(route):
                    host = (
                        fx
                        if route.request.post_data_json["id"].startswith("download/")
                        else api.url
                    )
                    route.continue_(url=host + "/jobs/cancel")

                def start_conversion(route):
                    response = route.fetch(url=api.url + "/convert/jobs")
                    if response.ok:
                        conversion_submissions.append(
                            {
                                "request": route.request.post_data_json,
                                "response": response.json(),
                            }
                        )
                    if hold_conversion_start:
                        held_conversions.append((route, response))
                    else:
                        route.fulfill(response=response)

                page.route("**/jobs", combined_jobs)
                page.route("**/jobs/cancel", cancel_job)
                page.route("**/convert/jobs", start_conversion)
                check = BrowserCheck(page, out)
                main_opens = []
                page.on(
                    "request",
                    lambda request: (
                        main_opens.append(json.loads(request.post_data))
                        if urlparse(request.url).path == "/open"
                        else None
                    ),
                )

                def record(name):
                    page.screenshot(path=str(out / (name + ".png")))
                    rows.append({"test": name, "status": "passed"})
                    print("Passed", name, flush=True)

                def control(**values):
                    return api_json(fx, "/fixture/control", values)

                def jobs():
                    return api_json(fx, "/jobs") + api_json(api.url, "/jobs")

                def current():
                    return api_json(fx, "/examples/download/status", {})

                def filename():
                    return page.evaluate(
                        "()=>window.__hictBenchmark.manager()?.getOptions().filename ?? null"
                    )

                def open_catalog():
                    check.menu("File", "Open Examples…")
                    dialog = page.get_by_role("dialog", name="Examples", exact=True)
                    expect(dialog).to_be_visible()
                    return dialog

                def card(dialog, key):
                    return dialog.locator('[data-example="jobs-' + key + '"]')

                def panel(auto=False):
                    dialog = page.get_by_role(
                        "dialog", name="Background jobs", exact=True
                    )
                    if not auto and not dialog.is_visible():
                        page.get_by_role("button", name="Jobs", exact=False).click()
                    expect(dialog).to_be_visible()
                    return dialog

                def start(key):
                    d = open_catalog()
                    c = card(d, key)
                    c.get_by_role("button", name="Open example", exact=True).click()
                    p = panel(True)
                    expect(d).to_have_count(0)
                    state = current()
                    assert state["exampleId"] == "jobs-" + key, state
                    row = p.locator('[data-job-id="download/' + state["jobId"] + '"]')
                    expect(row).to_be_visible()
                    expect(row).to_contain_text("Workflow " + key)
                    return p, row, state["jobId"]

                def await_state(state):
                    deadline = time.monotonic() + 30
                    while time.monotonic() < deadline:
                        value = current()
                        if value["state"] == state:
                            return value
                        if (
                            value["state"] in ("FAILED", "CANCELLED")
                            and state == "READY"
                        ):
                            raise AssertionError(value)
                        page.wait_for_timeout(100)
                    raise AssertionError(current())

                def await_file(key):
                    page.wait_for_function(
                        "(expected)=>window.__hictBenchmark.manager()?.getOptions().filename===expected",
                        arg="examples/jobs-" + key + "/example.hict.hdf5",
                    )

                try:
                    page.goto(url)
                    page.wait_for_function("window.__hictBenchmark")
                    assert (
                        page.request.post(
                            api.url + "/examples/download/start", data={}
                        ).status
                        == 400
                    )
                    assert (
                        page.request.post(
                            api.url + "/examples/download/start",
                            data={"exampleId": "missing-fixture-id"},
                        ).status
                        == 404
                    )
                    # A recoverable catalog transport failure, preserving a clear Retry action.
                    failed = [False]

                    def fail_catalog(route):
                        if not failed[0]:
                            failed[0] = True
                            route.fulfill(
                                status=503,
                                content_type="application/json",
                                body='{"error":"Controlled catalog outage"}',
                            )
                        else:
                            route.continue_(url=fx + "/examples/catalog")

                    page.route("**/examples/catalog", fail_catalog)
                    d = open_catalog()
                    expect(
                        d.get_by_role(
                            "button", name="Retry loading examples", exact=True
                        )
                    ).to_be_visible()
                    d.get_by_role(
                        "button", name="Retry loading examples", exact=True
                    ).click()
                    expect(card(d, "automatic")).to_be_visible()
                    page.unroute("**/examples/catalog", fail_catalog)
                    assert all("503" in e for e in check.errors), check.errors
                    check.errors.clear()
                    record("file-examples-recovers-from-catalog-failure")
                    card(d, "automatic").get_by_role(
                        "button", name="Open example", exact=True
                    ).click()
                    p = panel(True)
                    automatic = current()
                    row = p.locator(
                        '[data-job-id="download/' + automatic["jobId"] + '"]'
                    )
                    expect(row).to_contain_text("Workflow automatic")
                    expect(row.get_by_role("progressbar")).to_have_attribute(
                        "value", re.compile(r"[0-9]")
                    )
                    page.wait_for_function("true")
                    page.wait_for_timeout(1000)
                    assert current()["downloadedBytes"] > 0
                    expect(p).to_contain_text("This example will open when ready")
                    record("example-download-hands-off-to-named-byte-progress-in-jobs")
                    control(paused=False)
                    await_state("READY")
                    await_file("automatic")
                    expect(p).to_have_count(0)
                    assert (
                        hashlib.sha256(
                            (
                                data / "examples/jobs-automatic/example.hict.hdf5"
                            ).read_bytes()
                        ).hexdigest()
                        == fixture_info["sha256"]
                    )
                    tensor = api_json(
                        api.url,
                        "/matrix/query",
                        {
                            "bpResolution": 4,
                            "unit": "PIXELS",
                            "startRowPx": 0,
                            "endRowPx": 5,
                            "startColPx": 0,
                            "endColPx": 5,
                            "signalMode": "RAW_COUNTS",
                            "format": "JSON",
                        },
                    )
                    assert tensor["rows"] == 5 and tensor["cols"] == 5
                    assert (
                        sum(tensor["values"]) == 32
                        and tensor["values"][0] == 1
                        and tensor["values"][1] == 3
                        and tensor["values"][7] == 7
                        and tensor["values"][24] == 11
                    )
                    record("quick-start-opens-the-verified-downloaded-map-once")
                    control(paused=True)
                    p, row, manual_id = start("manual")
                    stale = page.request.post(
                        fx + "/jobs/cancel",
                        data={"id": "download/" + automatic["jobId"]},
                    )
                    assert stale.status == 404, stale.text()
                    assert (
                        current()["jobId"] == manual_id
                        and current()["state"] == "RUNNING"
                    )
                    p.get_by_role("button", name="Close", exact=True).click()
                    control(paused=False)
                    await_state("READY")
                    page.wait_for_timeout(1400)
                    assert filename() == "examples/jobs-automatic/example.hict.hdf5"
                    p = panel()
                    expect(
                        p.locator('[data-job-id="download/' + automatic["jobId"] + '"]')
                    ).to_be_visible()
                    expect(
                        p.locator('[data-job-id="download/' + manual_id + '"]')
                    ).to_contain_text("Optional files not downloaded")
                    expect(
                        p.locator('[data-job-id="download/' + manual_id + '"]')
                    ).to_contain_text("optional.bed")
                    p.locator('[data-job-id="download/' + manual_id + '"]').get_by_role(
                        "button", name="Open result", exact=True
                    ).click()
                    await_file("manual")
                    assert current()["jobId"] != manual_id
                    record("historical-example-result-is-reverified-under-a-new-job")
                    record(
                        "closing-jobs-keeps-download-running-and-retains-earlier-results-without-auto-open"
                    )
                    control(paused=True, fail=True)
                    p, row, failed_id = start("failure")
                    await_state("FAILED")
                    expect(row).to_contain_text("failed")
                    expect(
                        p.get_by_text("This example will open when ready.", exact=False)
                    ).to_have_count(0)
                    row.get_by_role(
                        "button", name="Open Examples to resume", exact=True
                    ).click()
                    d = page.get_by_role("dialog", name="Examples", exact=True)
                    expect(
                        card(d, "failure").get_by_role(
                            "button", name="Resume download", exact=True
                        )
                    ).to_be_visible()
                    expect(
                        card(d, "failure").get_by_role(
                            "button", name="Resume download", exact=True
                        )
                    ).to_be_in_viewport()
                    assert card(d, "failure").evaluate(
                        "e=>e.contains(document.activeElement)"
                    )
                    record("failed-job-clears-auto-open-and-offers-catalog-recovery")
                    control(fail=False)
                    card(d, "failure").get_by_role(
                        "button", name="Resume download", exact=True
                    ).click()
                    p = panel(True)
                    retry_id = current()["jobId"]
                    assert retry_id != failed_id
                    expect(
                        p.locator('[data-job-id="download/' + failed_id + '"]')
                    ).to_be_visible()
                    control(paused=False)
                    await_state("READY")
                    await_file("failure")
                    record("failed-download-retries-under-a-new-retained-job-identity")
                    control(paused=True)
                    p, row, cancel_id = start("cancel")
                    page.wait_for_timeout(900)
                    assert current()["downloadedBytes"] > 0
                    row.get_by_role("button", name="Cancel", exact=True).click()
                    await_state("CANCELLED")
                    expect(row).to_contain_text("cancelled")
                    assert filename() == "examples/jobs-failure/example.hict.hdf5"
                    row.get_by_role(
                        "button", name="Open Examples to resume", exact=True
                    ).click()
                    d = page.get_by_role("dialog", name="Examples", exact=True)
                    card(d, "cancel").get_by_role(
                        "button", name="Resume download", exact=True
                    ).click()
                    p = panel(True)
                    control(paused=False)
                    await_state("READY")
                    await_file("cancel")
                    assert "bytes=" in (out / "ranges.json").read_text()
                    record("cancel-and-resume-use-owned-jobs-and-real-range-requests")
                    control(paused=True)
                    p, row, unknown_id = start("unknown")
                    page.wait_for_timeout(1000)
                    assert current()["totalBytes"] == 0, current()
                    assert row.get_by_role("progressbar").get_attribute("value") is None
                    expect(row).to_contain_text("total size unknown")
                    p.get_by_role("button", name="Close", exact=True).click()
                    control(paused=False)
                    await_state("READY")
                    page.wait_for_timeout(1100)
                    assert filename() == "examples/jobs-cancel/example.hict.hdf5"
                    record(
                        "unknown-size-transfer-is-indeterminate-and-does-not-claim-a-percentage"
                    )
                    control(paused=True)
                    p, row, changed_id = start("changed")
                    page.evaluate(
                        'api=>window.__hictBenchmark.open("tiny-primary.hict.hdf5",api)',
                        api.url,
                    )
                    check.settle()
                    control(paused=False)
                    await_state("READY")
                    page.wait_for_timeout(1300)
                    assert filename() == "tiny-primary.hict.hdf5"
                    expect(
                        p.get_by_text("This example will open when ready.", exact=False)
                    ).to_have_count(0)
                    p.get_by_role("button", name="Close", exact=True).click()
                    record("a-new-workspace-revokes-the-pending-example-opening-intent")
                    # Closing the catalog while the accepted start reply is delayed must not reopen progress.
                    control(paused=True)
                    held = []

                    def hold_start(route):
                        held.append(
                            (route, route.fetch(url=fx + "/examples/download/start"))
                        )

                    page.route("**/examples/download/start", hold_start)
                    d = open_catalog()
                    card(d, "starting").get_by_role(
                        "button", name="Open example", exact=True
                    ).click()
                    for _ in range(200):
                        if held:
                            break
                        page.wait_for_timeout(20)
                    assert held
                    d.get_by_role("button", name="Close", exact=True).click()
                    held[0][0].fulfill(response=held[0][1])
                    page.unroute("**/examples/download/start", hold_start)
                    page.wait_for_timeout(500)
                    expect(
                        page.get_by_role("dialog", name="Background jobs", exact=True)
                    ).to_have_count(0)
                    control(paused=False)
                    await_state("READY")
                    page.wait_for_timeout(1200)
                    assert filename() == "tiny-primary.hict.hdf5"
                    record(
                        "late-start-response-after-catalog-close-does-not-open-jobs-or-replace-the-map"
                    )
                    cached = data / "examples/jobs-automatic/example.hict.hdf5"
                    before = cached.stat()
                    previous_id = current()["jobId"]
                    control(paused=True)
                    d = open_catalog()
                    card(d, "automatic").get_by_role(
                        "button", name="Open example", exact=True
                    ).click()
                    expect(d).to_have_count(0)
                    cached_id = current()["jobId"]
                    assert cached_id != previous_id
                    await_state("READY")
                    await_file("automatic")
                    assert cached.stat().st_mtime_ns == before.st_mtime_ns
                    record("cached-example-is-verified-in-jobs-without-redownloading")
                    page.evaluate(
                        "api=>window.__hictBenchmark.open('tiny-primary.hict.hdf5',api)",
                        api.url,
                    )
                    check.settle()
                    # Same size and mtime defeats discovery metadata, but must not
                    # defeat the full digest performed by an opening job.
                    with cached.open("r+b") as stream:
                        stream.seek(-1, 2)
                        last = stream.read(1)
                        stream.seek(-1, 2)
                        stream.write(bytes([last[0] ^ 1]))
                    os.utime(cached, ns=(before.st_atime_ns, before.st_mtime_ns))
                    assert (
                        hashlib.sha256(cached.read_bytes()).hexdigest()
                        != fixture_info["sha256"]
                    )
                    assert (
                        next(
                            e
                            for e in api_json(fx, "/examples/catalog", {})["examples"]
                            if e["id"] == "jobs-automatic"
                        )["downloaded"]
                        is True
                    )
                    p, row, repaired_id = start("automatic")
                    assert repaired_id != cached_id
                    expect(row).to_contain_text("Downloading example.hict.hdf5")
                    assert filename() == "tiny-primary.hict.hdf5"
                    record("tampered-cached-example-cannot-open-before-repair")
                    control(paused=False)
                    await_state("READY")
                    await_file("automatic")
                    assert (
                        hashlib.sha256(cached.read_bytes()).hexdigest()
                        == fixture_info["sha256"]
                    )
                    record("same-size-same-mtime-corruption-is-repaired-before-opening")
                    catalog_file = data / "examples/catalogs/jobs-fixture.json"
                    declaration = json.loads(catalog_file.read_text())
                    changed = next(
                        e
                        for e in declaration["examples"]
                        if e["id"] == "jobs-automatic"
                    )
                    changed["attribution"][
                        "acknowledgement"
                    ] = "Updated fixture attribution."
                    catalog_file.write_text(json.dumps(declaration))
                    assert (
                        next(
                            e
                            for e in api_json(fx, "/examples/catalog", {})["examples"]
                            if e["id"] == "jobs-automatic"
                        )["downloaded"]
                        is False
                    )
                    previous_id = current()["jobId"]
                    d = open_catalog()
                    card(d, "automatic").get_by_role(
                        "button", name="Open example", exact=True
                    ).click()
                    expect(d).to_have_count(0)
                    declaration_id = current()["jobId"]
                    assert declaration_id != previous_id
                    await_state("READY")
                    expect(
                        page.get_by_role("dialog", name="Background jobs", exact=True)
                    ).to_have_count(0)
                    manifest = json.loads((cached.parent / "manifest.json").read_text())
                    assert (
                        manifest["catalogExample"]["attribution"]["acknowledgement"]
                        == "Updated fixture attribution."
                    )
                    assert (
                        next(
                            e
                            for e in api_json(fx, "/examples/catalog", {})["examples"]
                            if e["id"] == "jobs-automatic"
                        )["downloaded"]
                        is True
                    )
                    record("changed-catalog-invalidates-cache-and-refreshes-provenance")
                    page.set_viewport_size({"width": 640, "height": 600})
                    p = panel()
                    assert p.evaluate("(e)=>e.scrollWidth<=e.clientWidth+1")
                    body = p.locator(".modal-body")
                    body.evaluate("e=>e.scrollTop=e.scrollHeight")
                    header = p.locator(".modal-header").bounding_box()
                    dialog_box = p.bounding_box()
                    assert header and dialog_box and header["y"] >= dialog_box["y"]
                    expect(
                        p.get_by_role("button", name="Close", exact=True)
                    ).to_be_in_viewport()
                    page.screenshot(path=str(out / "jobs-640-scrolled-history.png"))
                    p.get_by_role("button", name="Close", exact=True).focus()
                    page.keyboard.press("Shift+Tab")
                    assert p.evaluate("(e)=>e.contains(document.activeElement)")
                    page.keyboard.press("Escape")
                    expect(p).to_have_count(0)
                    record(
                        "jobs-history-remains-scrollable-and-keyboard-accessible-at-640"
                    )
                    page.set_viewport_size({"width": 1440, "height": 1000})

                    def begin_conversion(key):
                        control(paused=False)
                        d = open_catalog()
                        card(d, key).get_by_role(
                            "button", name="Open example", exact=True
                        ).click()
                        deadline = time.monotonic() + 60
                        while time.monotonic() < deadline:
                            submitted = next(
                                (
                                    c
                                    for c in conversion_submissions
                                    if c["request"].get("exampleId") == "jobs-" + key
                                ),
                                None,
                            )
                            if submitted:
                                return "conversion/" + submitted["response"]["jobId"]
                            page.wait_for_timeout(50)
                        raise AssertionError((key, jobs()))

                    def converted_job(job_id, state="finished"):
                        deadline = time.monotonic() + 90
                        while time.monotonic() < deadline:
                            job = next((j for j in jobs() if j["id"] == job_id), None)
                            if job and job["state"] == state:
                                return job
                            if (
                                job
                                and job["state"] in ("failed", "cancelled")
                                and state == "finished"
                            ):
                                raise AssertionError(job)
                            page.wait_for_timeout(100)
                        raise AssertionError(jobs())

                    def release_jobs():
                        for route, values in held_jobs:
                            route.fulfill(json=values, headers=cors)
                        held_jobs.clear()

                    def assert_reference(key):
                        check.settle()
                        page.wait_for_function(
                            "expected=>window.__hictBenchmark.manager()?.getOptions().fastaFilename===expected",
                            arg="examples/jobs-" + key + "/example.fa",
                        )
                        response = page.request.post(
                            api.url + "/get_fasta_for_assembly", data={}
                        )
                        assert response.ok, response.text()
                        bases = "".join(
                            line.strip()
                            for line in response.text().splitlines()
                            if not line.startswith(">")
                        )
                        assert bases == "ACGTACGTACGTACGTA", response.text()

                    automatic_conversion = begin_conversion("convert-auto")
                    converted = converted_job(automatic_conversion)
                    await_file("convert-auto")
                    assert_reference("convert-auto")
                    assert converted["outputs"][0]["action"] == "example"
                    assert converted["outputs"][0]["filename"] == "jobs-convert-auto"
                    assert converted["details"]["title"] == "Workflow convert-auto"
                    tensor = api_json(
                        api.url,
                        "/matrix/query",
                        {
                            "bpResolution": 4,
                            "unit": "PIXELS",
                            "startRowPx": 0,
                            "endRowPx": 5,
                            "startColPx": 0,
                            "endColPx": 5,
                            "signalMode": "RAW_COUNTS",
                            "format": "JSON",
                        },
                    )
                    assert tensor["values"] == [
                        1,
                        3,
                        0,
                        0,
                        0,
                        3,
                        0,
                        7,
                        0,
                        0,
                        0,
                        7,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        11,
                    ], tensor
                    for bad in (
                        {
                            "filename": "tiny-primary.mcool",
                            "exampleId": "jobs-convert-auto",
                            "direction": "mcool-to-hict",
                            "overwrite": True,
                        },
                        {
                            "filename": "examples/jobs-convert-auto/example.mcool",
                            "exampleId": "jobs-convert-auto",
                            "direction": "mcool-to-hict",
                            "outputFilename": "misrouted-example.hict.hdf5",
                            "overwrite": True,
                        },
                    ):
                        rejected = page.request.post(
                            api.url + "/convert/jobs", data=bad
                        )
                        assert rejected.status == 400, rejected.text()
                    assert not (data / "misrouted-example.hict.hdf5").exists()
                    record(
                        "example-conversion-opens-once-with-the-actual-reference-and-contact-values"
                    )

                    hold_job_example = "jobs-convert-close"
                    close_conversion = begin_conversion("convert-close")
                    p = panel(True)
                    expect(p).to_contain_text(
                        "The converted example will open when ready"
                    )
                    record(
                        "example-conversion-hands-progress-and-opening-intent-to-jobs"
                    )
                    p.get_by_role("button", name="Close", exact=True).click()
                    converted_job(close_conversion)
                    hold_job_example = None
                    release_jobs()
                    page.wait_for_timeout(1200)
                    assert filename() == "examples/jobs-convert-auto/example.hict.hdf5"
                    p = panel()
                    p.locator('[data-job-id="' + close_conversion + '"]').get_by_role(
                        "button", name="Open result", exact=True
                    ).click()
                    await_file("convert-close")
                    assert_reference("convert-close")
                    assert (
                        len(
                            [
                                j
                                for j in jobs()
                                if j["kind"] == "conversion"
                                and j.get("details", {}).get("exampleId")
                                == "jobs-convert-close"
                            ]
                        )
                        == 1
                    )
                    record(
                        "closing-conversion-progress-preserves-result-and-manual-open-reuses-conversion"
                    )

                    hold_job_example = "jobs-convert-change"
                    change_conversion = begin_conversion("convert-change")
                    p = panel(True)
                    expect(p).to_contain_text(
                        "The converted example will open when ready"
                    )
                    page.evaluate(
                        "api=>window.__hictBenchmark.open('tiny-primary.hict.hdf5',api)",
                        api.url,
                    )
                    check.settle()
                    converted_job(change_conversion)
                    hold_job_example = None
                    release_jobs()
                    page.wait_for_timeout(1200)
                    assert filename() == "tiny-primary.hict.hdf5"
                    expect(
                        p.get_by_text(
                            "The converted example will open when ready", exact=False
                        )
                    ).to_have_count(0)
                    p.get_by_role("button", name="Close", exact=True).click()
                    record("workspace-change-revokes-conversion-opening-intent")

                    hold_conversion_start = True
                    late_conversion = begin_conversion("convert-late")
                    assert held_conversions
                    page.evaluate(
                        "api=>window.__hictBenchmark.open('tiny-fine.hict.hdf5',api)",
                        api.url,
                    )
                    check.settle()
                    hold_conversion_start = False
                    for route, response in held_conversions:
                        route.fulfill(response=response)
                    held_conversions.clear()
                    converted_job(late_conversion)
                    page.wait_for_timeout(1200)
                    assert filename() == "tiny-fine.hict.hdf5"
                    expect(
                        page.get_by_role("dialog", name="Background jobs", exact=True)
                    ).to_have_count(0)
                    record(
                        "late-conversion-submission-cannot-reopen-progress-or-replace-a-new-workspace"
                    )

                    failed_conversion = begin_conversion("convert-fail")
                    converted_job(failed_conversion, "failed")
                    p = panel(True)
                    row = p.locator('[data-job-id="' + failed_conversion + '"]')
                    expect(row).to_contain_text("failed")
                    expect(row.get_by_role("alert")).to_contain_text(
                        "Conversion could not finish."
                    )
                    expect(row.locator(".job-error")).to_be_hidden()
                    expect(
                        p.get_by_text(
                            "The converted example will open when ready", exact=False
                        )
                    ).to_have_count(0)
                    assert filename() == "tiny-fine.hict.hdf5"
                    record(
                        "failed-example-conversion-stays-in-jobs-without-replacing-the-map"
                    )
                    row.get_by_text("Error details", exact=True).click()
                    expect(row.locator(".job-error")).to_contain_text(
                        "Not a valid Cooler"
                    )
                    page.screenshot(path=str(out / "conversion-error-details.png"))
                    row.get_by_role(
                        "button", name="Open Examples to retry", exact=True
                    ).click()
                    retry_dialog = page.get_by_role(
                        "dialog", name="Examples", exact=True
                    )
                    retry_card = card(retry_dialog, "convert-fail")
                    expect(
                        retry_card.get_by_role(
                            "button", name="Open example", exact=True
                        )
                    ).to_be_in_viewport()
                    assert retry_card.evaluate("e=>e.contains(document.activeElement)")
                    retry_dialog.get_by_role("button", name="Close", exact=True).click()
                    page.get_by_role(
                        "button", name="Open notifications", exact=True
                    ).click()
                    notifications = page.get_by_role("dialog").filter(
                        has=page.get_by_role(
                            "heading", name="Notifications", exact=True
                        )
                    )
                    expect(notifications).to_be_visible()
                    expect(notifications).not_to_contain_text(
                        "Sensitivity threshold for slide to diagonal"
                    )
                    expect(notifications).not_to_contain_text("Preparing Workflow")
                    record(
                        "opening-examples-does-not-accumulate-startup-debug-and-progress-notifications"
                    )
                    notifications.locator(".btn-close").click()
                    for key in (
                        "convert-auto",
                        "convert-close",
                        "convert-change",
                        "convert-late",
                        "convert-fail",
                    ):
                        assert (
                            len(
                                [
                                    c
                                    for c in conversion_submissions
                                    if c["request"].get("exampleId") == "jobs-" + key
                                ]
                            )
                            == 1
                        )
                    assert (
                        source_hash
                        == hashlib.sha256(
                            (data / "tiny-primary.hict.hdf5").read_bytes()
                        ).hexdigest()
                    )
                    assert not check.errors, check.errors
                    assert (
                        sum(
                            body.get("filename")
                            == "examples/jobs-automatic/example.hict.hdf5"
                            for body in main_opens
                        )
                        == 4
                    ), main_opens
                    assert not any(
                        body.get("filename")
                        in (
                            "examples/jobs-unknown/example.hict.hdf5",
                            "examples/jobs-changed/example.hict.hdf5",
                            "examples/jobs-starting/example.hict.hdf5",
                        )
                        for body in main_opens
                    ), main_opens
                    (out / "results.json").write_text(
                        json.dumps(
                            {
                                "status": "ok",
                                "checks": rows,
                                "jobs": jobs(),
                                "mapOpenRequests": main_opens,
                                "conversionSubmissions": conversion_submissions,
                                "protocolChecks": {
                                    "invalidExampleRequest": 400,
                                    "unknownExample": 404,
                                    "staleCancellation": 404,
                                },
                                "sourceSha256": source_hash,
                                "downloadedSha256": fixture_info["sha256"],
                                "errors": check.errors,
                            },
                            indent=2,
                        )
                    )
                except BaseException:
                    page.screenshot(path=str(out / "failure.png"))
                    (out / "errors.json").write_text(json.dumps(check.errors))
                    (out / "jobs.json").write_text(json.dumps(jobs(), indent=2))
                    raise
                finally:
                    browser.close()
    finally:
        if fixture and fixture.poll() is None:
            os.killpg(fixture.pid, signal.SIGTERM)
            try:
                fixture.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(fixture.pid, signal.SIGKILL)
                fixture.wait(timeout=5)
        web.shutdown()
        web.server_close()


if __name__ == "__main__":
    main()
