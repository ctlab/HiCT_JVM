#!/usr/bin/env python3
"""Retrieve an existing approved public RID through the UI without submitting any search."""

import argparse
import functools
import hashlib
import http.server
import json
import os
from pathlib import Path
import re
import subprocess
import threading
import time
from playwright.sync_api import expect, sync_playwright
from sequence_browser_harness import BrowserCheck, HictTestServer


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("jar", "webui", "output"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument(
        "--public-rid",
        required=True,
        choices=["FR9P7TGF016"],
        help="Explicit read-only retrieval of the already submitted public-reference test",
    )
    args = parser.parse_args()
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
    query_text = Path(
        "src/test/resources/remote-blast/ncbi-public-160.fasta"
    ).read_text()
    native = data / "tiny-primary.hict.hdf5"
    source_hash = hashlib.sha256(native.read_bytes()).hexdigest()
    old_blast = os.environ.get("HICT_BLASTN_BIN")
    os.environ["HICT_BLASTN_BIN"] = str(out / "missing-blastn")

    class Quiet(http.server.SimpleHTTPRequestHandler):
        def log_message(self, *_):
            pass

    web = http.server.ThreadingHTTPServer(
        ("127.0.0.1", 0),
        functools.partial(Quiet, directory=str(Path(args.webui).resolve())),
    )
    threading.Thread(target=web.serve_forever, daemon=True).start()
    checks, errors, submissions, recoveries = [], [], [], []
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

            # A regression must fail safely rather than resubmit the public query.
            def forbid_submission(route):
                submissions.append(route.request.post_data_json)
                route.fulfill(
                    status=409,
                    content_type="application/json",
                    body='{"error":"Test forbids all new search submissions"}',
                )

            page.route("**/sequence/search/jobs", forbid_submission)
            page.on(
                "request",
                lambda r: (
                    recoveries.append(r.post_data_json)
                    if r.url.endswith("/sequence/remote_blast/retrieve")
                    else None
                ),
            )
            page.on("pageerror", lambda e: errors.append(str(e)))
            check = BrowserCheck(page, out)
            search = page.get_by_role(
                "dialog", name="Search NCBI databases", exact=True
            )
            results = page.get_by_role("dialog", name="NCBI BLAST results", exact=True)

            def record(name):
                page.screenshot(path=str(out / (name + ".png")))
                checks.append({"test": name, "status": "passed"})
                print("Passed", name, flush=True)

            def post(path, payload):
                response = page.request.post(api.url + path, data=payload)
                assert response.ok, (response.status, response.text())
                return response.json()

            def open_recovery():
                check.menu("Tools", "Search NCBI databases…")
                expect(search).to_be_visible()
                search.locator("summary").filter(
                    has_text="Recover an existing NCBI request"
                ).click()
                search.get_by_label(
                    "FASTA records, or one DNA sequence per line", exact=True
                ).fill(query_text)
                search.get_by_label("NCBI database", exact=True).select_option(
                    "16S_ribosomal_RNA"
                )
                search.get_by_label("NCBI request ID (RID)", exact=True).fill(
                    args.public_rid
                )

            try:
                page.goto(f"http://127.0.0.1:{web.server_port}/")
                page.evaluate(
                    "api=>window.__hictBenchmark.open('tiny-primary.hict.hdf5',api)",
                    api.url,
                )
                check.settle()
                settings = post("/sequence/settings", {"checkTools": True})
                assert settings["remoteBlastRidRetrieval"] is True
                assert settings["tools"]["blastn"]["available"] is False
                base = dict(
                    queries=query_text,
                    mode="blast",
                    scope="database",
                    searchTarget="NCBI_REMOTE",
                    bothStrands=True,
                    remoteConsent=True,
                    remoteDatabase="16S_ribosomal_RNA",
                    remoteRequestId=args.public_rid,
                )
                for change in (
                    {"remoteRequestId": ""},
                    {"remoteRequestId": "RID&CMD=Put"},
                    {"remoteConsent": False},
                ):
                    response = page.request.post(
                        api.url + "/sequence/remote_blast/retrieve",
                        data={**base, **change},
                    )
                    assert response.status == 400, response.text()
                response = page.request.post(
                    api.url + "/sequence/search/jobs", data=base
                )
                assert response.status == 400, response.text()
                assert post("/sequence/jobs/list", {}) == []
                record("invalid-recovery-cannot-queue-or-fall-back-to-a-submission")

                def old_server(route):
                    response = route.fetch()
                    body = response.json()
                    body.pop("remoteBlastRidRetrieval", None)
                    route.fulfill(response=response, json=body)

                page.route("**/sequence/settings", old_server)
                open_recovery()
                search.locator("#blast-remote-consent").check()
                expect(
                    search.get_by_role(
                        "button", name="Retrieve NCBI results", exact=True
                    )
                ).to_be_disabled()
                expect(search).to_contain_text(
                    "This server does not support NCBI request recovery"
                )
                assert recoveries == [] and submissions == []
                search.get_by_role("button", name="Close", exact=True).last.click()
                page.unroute("**/sequence/settings", old_server)
                record("older-server-support-is-checked-without-upload-or-fallback")
                open_recovery()
                expect(search).to_contain_text("HiCT sends only the RID to NCBI")
                button = search.get_by_role(
                    "button", name="Retrieve NCBI results", exact=True
                )
                expect(button).to_be_disabled()
                search.locator("#blast-remote-consent").check()
                expect(button).to_be_enabled()
                with page.expect_response(
                    "**/sequence/remote_blast/retrieve"
                ) as response:
                    button.click()
                assert response.value.status == 202, response.value.text()
                job_id = response.value.json()["jobId"]
                search.get_by_role("button", name="Close", exact=True).last.click()
                check.menu("File", "Close map")
                page.wait_for_function("!window.__hictBenchmark.manager()")
                deadline = time.monotonic() + 90
                while time.monotonic() < deadline:
                    job = next(
                        j
                        for j in post("/sequence/jobs/list", {})
                        if j["jobId"] == job_id
                    )
                    if job["status"] == "finished":
                        break
                    assert job["status"] not in ("failed", "cancelled"), job
                    page.wait_for_timeout(200)
                else:
                    raise AssertionError(job)
                assert job["remoteRequestId"] == args.public_rid
                assert submissions == [] and len(recoveries) == 1
                assert recoveries[0]["queries"] == query_text
                assert recoveries[0]["remoteRequestId"] == args.public_rid
                record(
                    "completed-rid-download-finishes-after-closing-the-map-without-local-blast"
                )
                page.get_by_role("button", name=re.compile(r"^Jobs")).click()
                jobs = page.get_by_role("dialog", name="Background jobs", exact=True)
                row = jobs.locator(".job-row").filter(has_text=job_id)
                # The accessible job list may show only a shortened ID.
                if row.count() == 0:
                    row = jobs.locator(".job-row").filter(has_text="NCBI")
                row.get_by_role("button", name="Open result", exact=True).click()
                expect(results).to_be_visible()
                expect(results).to_contain_text(
                    "Recovered NCBI request " + args.public_rid
                )
                expect(results.locator("tbody tr")).to_have_count(10)
                report = post("/sequence/remote_blast/result", {"jobId": job_id})
                assert (
                    report["submissionPerformed"] is False and report["hitCount"] == 10
                )
                assert report["queryVerification"][0]["scope"] == "FULL_QUERY"
                assert report["queryVerification"][0]["alignedBasesVerified"] == 160
                first = report["hits"][0]
                assert (
                    first["accession"] == "NR_180316.1"
                    and first["subjectStart"] == 10
                    and first["subjectEnd"] == 169
                )
                results.get_by_text("Queries and provenance", exact=True).click()
                expect(results).to_contain_text(
                    "All query bases matched returned alignments"
                )
                record("jobs-delivers-external-coordinates-and-full-query-verification")
                page.set_viewport_size({"width": 640, "height": 900})
                expect(results.get_by_role("button", name="Close", exact=True).last).to_be_in_viewport()
                expect(results.get_by_role("button", name="Save JSON report…", exact=True)).to_be_in_viewport()
                assert page.evaluate("document.documentElement.scrollWidth <= innerWidth")
                expect(results).to_contain_text("All query bases matched returned alignments")
                record("recovered-provenance-and-export-controls-fit-a-640-pixel-window")
                page.set_viewport_size({"width": 1440, "height": 1000})
                for label, suffix in [
                    ("Save JSON report…", "json"),
                    ("Save raw TSV…", "tsv"),
                ]:
                    with page.expect_download() as download:
                        results.get_by_role("button", name=label, exact=True).click()
                    path = out / ("recovered-result." + suffix)
                    download.value.save_as(path)
                    if suffix == "json":
                        assert (
                            json.loads(path.read_text())["retrievedRequestId"]
                            == args.public_rid
                        )
                    else:
                        assert len(path.read_text().splitlines()) == 10
                record("recovered-results-export-through-confirmed-destinations")
                assert hashlib.sha256(native.read_bytes()).hexdigest() == source_hash
                assert not errors and not check.errors, errors + check.errors
            finally:
                page.screenshot(path=str(out / "final.png"))
                (out / "results.json").write_text(
                    json.dumps(
                        {
                            "checks": checks,
                            "errors": errors + check.errors,
                            "blockedSubmissions": submissions,
                            "recoveryRequests": recoveries,
                            "nativeSha256": source_hash,
                        },
                        indent=2,
                    )
                    + "\n"
                )
                browser.close()
    finally:
        web.shutdown()
        web.server_close()
        if old_blast is None:
            os.environ.pop("HICT_BLASTN_BIN", None)
        else:
            os.environ["HICT_BLASTN_BIN"] = old_blast


if __name__ == "__main__":
    main()
