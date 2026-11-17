#!/usr/bin/env python3
"""Exercise opt-in remote BLAST through actual handlers and a controlled external executable."""

import argparse
import functools
import hashlib
import http.server
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
from playwright.sync_api import expect, sync_playwright
from remote_blast_fixture import make_executable
from sequence_browser_harness import BrowserCheck, HictTestServer


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("jar", "webui", "output"):
        parser.add_argument("--" + name, required=True)
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
    (data / "private.fa").write_text(">tiny private local record\nACGTACGTACGTACGTA\n")
    source_hashes = {
        p.name: hashlib.sha256(p.read_bytes()).hexdigest()
        for p in data.iterdir()
        if p.is_file()
    }
    fixture = out / "remote-fixture"
    executable = make_executable(fixture)
    old_override = os.environ.get("HICT_BLASTN_BIN")
    os.environ["HICT_BLASTN_BIN"] = str(executable)

    class Quiet(http.server.SimpleHTTPRequestHandler):
        def log_message(self, *_):
            pass

    web = http.server.ThreadingHTTPServer(
        ("127.0.0.1", 0),
        functools.partial(Quiet, directory=str(Path(args.webui).resolve())),
    )
    threading.Thread(target=web.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{web.server_port}/"
    sequence = "ACGT" * 20
    query_text = (
        ">private description <em>literal</em>\n"
        + sequence
        + "\n>second query\n"
        + sequence
    )
    checks, timings = [], {}
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
                lambda route: route.continue_(
                    url=api.url
                    + route.request.url.removeprefix("http://localhost:5000")
                ),
            )
            check = BrowserCheck(page, out)
            search = page.get_by_role(
                "dialog", name="Search NCBI databases", exact=True
            )
            results = page.get_by_role("dialog", name="NCBI BLAST results", exact=True)

            def record(name):
                page.screenshot(path=str(out / (name + ".png")))
                checks.append({"test": name, "status": "passed"})
                print("Passed", name, flush=True)

            def post(path, payload=None):
                response = page.request.post(
                    api.url + path, data={} if payload is None else payload
                )
                assert response.ok, (path, response.status, response.text())
                return response.json()

            def calls():
                return [
                    json.loads(p.read_text())
                    for p in sorted((fixture / "calls").glob("*.json"))
                ]

            def alive(receipt):
                try:
                    state = (
                        Path("/proc/" + str(receipt["pid"]) + "/stat")
                        .read_text()
                        .rsplit(")", 1)[1]
                        .split()
                    )
                    return state[0] != "Z" and state[19] == receipt["startTicks"]
                except FileNotFoundError:
                    return False

            def configure(mode, count=125):
                (fixture / "control.json").write_text(
                    json.dumps({"mode": mode, "count": count})
                )

            def jobs():
                return post("/sequence/jobs/list")

            def wait_job(job_id, state, timeout=90):
                deadline = time.monotonic() + timeout
                while time.monotonic() < deadline:
                    job = next(j for j in jobs() if j["jobId"] == job_id)
                    if job["status"] == state:
                        return job
                    if (
                        job["status"] in ("failed", "cancelled", "finished")
                        and job["status"] != state
                    ):
                        raise AssertionError(job)
                    page.wait_for_timeout(100)
                raise AssertionError(jobs())

            def open_search():
                check.menu("Tools", "Search NCBI databases…")
                expect(search).to_be_visible()
                assert (
                    search.locator(".modal-content").evaluate(
                        "element => getComputedStyle(element).backgroundColor"
                    )
                    == "rgb(255, 255, 255)"
                )
                expect(search.get_by_label("BLAST target", exact=True)).to_have_value(
                    "NCBI_REMOTE"
                )
                search.get_by_label(
                    "FASTA records, or one DNA sequence per line", exact=True
                ).fill(query_text)
                expect(
                    search.get_by_role("button", name="Find sequences", exact=True)
                ).to_be_disabled()
                return search

            def start(mode="success", count=125, timeout=10, database="nt"):
                configure(mode, count)
                open_search()
                search.get_by_label("NCBI database", exact=True).select_option(database)
                search.get_by_label("Timeout (minutes)", exact=True).fill(str(timeout))
                search.get_by_label(
                    "I agree to send these query sequences to NCBI.", exact=True
                ).check()
                with page.expect_response(
                    lambda response: response.url.endswith("/sequence/search/jobs")
                ) as response:
                    search.get_by_role(
                        "button", name="Find sequences", exact=True
                    ).click()
                assert response.value.status == 202, response.value.text()
                assert "x-hict-history-revision" not in response.value.request.headers
                return response.value.json()["jobId"]

            def close_results():
                results.get_by_role("button", name="Close", exact=True).last.click()
                expect(results).to_have_count(0)

            def panel():
                page.get_by_role("button", name="Jobs", exact=False).click()
                dialog = page.get_by_role("dialog", name="Background jobs", exact=True)
                expect(dialog).to_be_visible()
                return dialog

            try:
                page.goto(url)
                base_request = {
                    "queries": sequence,
                    "mode": "blast",
                    "scope": "database",
                    "bothStrands": True,
                    "searchTarget": "NCBI_REMOTE",
                    "remoteDatabase": "nt",
                }
                for consent in (None, False, "true"):
                    response = page.request.post(
                        api.url + "/sequence/search/jobs",
                        data={**base_request, "remoteConsent": consent},
                    )
                    assert response.status == 400
                assert calls() == []
                record(
                    "remote-consent-is-required-by-the-server-before-any-provider-command"
                )
                open_search()
                expect(search.get_by_label("Search scope", exact=True)).to_have_count(0)
                expect(
                    search.get_by_role("button", name="Link FASTA…", exact=True)
                ).to_have_count(0)
                search.get_by_label(
                    "I agree to send these query sequences to NCBI.", exact=True
                ).check()
                search.get_by_label("NCBI database", exact=True).select_option(
                    "refseq_rna"
                )
                expect(
                    search.get_by_label(
                        "I agree to send these query sequences to NCBI.", exact=True
                    )
                ).not_to_be_checked()
                record(
                    "remote-search-is-available-without-a-map-and-does-not-reuse-upload-consent"
                )
                search.get_by_role("button", name="Close", exact=True).last.click()
                first = start()
                wait_job(first, "finished")
                expect(results).to_be_visible()
                expect(results.locator("tbody tr")).to_have_count(50)
                expect(results.get_by_text("180–102", exact=True).first).to_be_visible()
                report = post("/sequence/remote_blast/result", {"jobId": first})
                assert (
                    report["externalSubjects"]
                    and report["coordinateSystem"] == "ONE_BASED_INCLUSIVE"
                )
                assert (
                    report["hitCount"] == 125
                    and report["hits"][0]["subjectStart"] == 180
                    and report["hits"][0]["subjectEnd"] == 102
                )
                assert (
                    report["hits"][0]["queryStart"] == 2
                    and report["hits"][0]["queryEnd"] == 79
                )
                assert results.locator("img, em").count() == 0
                assert page.evaluate("window.__remoteAttack===undefined")
                assert results.locator('a[href^="javascript:"]').count() == 0
                expect(
                    results.locator(
                        'a[href="https://www.ncbi.nlm.nih.gov/nuccore/NC_000001.1"]'
                    ).first
                ).to_be_visible()
                first_call = calls()[0]
                assert (
                    "-remote" in first_call["args"]
                    and "-subject" not in first_call["args"]
                    and "-num_threads" not in first_call["args"]
                )
                assert (
                    first_call["queryFile"]
                    == ">query_1\n" + sequence + "\n>query_2\n" + sequence + "\n"
                )
                record(
                    "external-coordinates-scores-and-literal-subject-text-are-shown-with-safe-accession-links"
                )
                results.get_by_role("button", name="Next page", exact=True).click()
                expect(results).to_contain_text("Page 2 of 3")
                results.get_by_label("Query", exact=True).select_option("query_2")
                expect(results).to_contain_text("Page 1 of 2")
                results.get_by_label("Subject accession or title", exact=True).fill(
                    "Controlled external subject 3"
                )
                expect(results.locator("tbody tr")).to_have_count(6)
                record(
                    "external-results-use-bounded-pages-and-query-and-subject-filters"
                )
                for format, name in (
                    ("JSON", "Save JSON report…"),
                    ("TSV", "Save raw TSV…"),
                ):
                    with page.expect_download() as download:
                        results.get_by_role("button", name=name, exact=True).click()
                    target = out / ("saved-result." + format.lower())
                    download.value.save_as(target)
                    if format == "JSON":
                        saved = json.loads(target.read_text())
                        assert saved["hitCount"] == 125
                        assert (
                            saved["toolSha256"]
                            == hashlib.sha256(executable.read_bytes()).hexdigest()
                        )
                    else:
                        assert len(target.read_text().splitlines()) == 125
                record(
                    "exports-ask-for-a-destination-and-retain-the-complete-result-beyond-current-filters"
                )
                page.set_viewport_size({"width": 640, "height": 600})
                assert results.evaluate("e=>e.scrollWidth<=e.clientWidth+1")
                expect(
                    results.get_by_role("button", name="Save JSON report…", exact=True)
                ).to_be_in_viewport()
                expect(
                    results.get_by_role("button", name="Save raw TSV…", exact=True)
                ).to_be_in_viewport()
                assert results.locator(".result-table").evaluate(
                    "e=>e.scrollWidth>e.clientWidth"
                )
                record(
                    "external-result-controls-fit-a-640-pixel-window-with-scrollable-data-columns"
                )
                page.set_viewport_size({"width": 1440, "height": 1000})
                close_results()
                page.evaluate(
                    "async api=>await window.__hictBenchmark.open('tiny-primary.hict.hdf5',api)",
                    api.url,
                )
                check.menu("Tools", "Find sequences in linked FASTA…")
                local_dialog = page.get_by_role(
                    "dialog", name="Find sequences in the linked FASTA", exact=True
                )
                local_dialog.get_by_label("Matching", exact=True).select_option("blast")
                expect(
                    local_dialog.get_by_label("BLAST target", exact=True)
                ).to_have_value("LOCAL_ASSEMBLY")
                expect(
                    local_dialog.get_by_label(
                        "I agree to send these query sequences to NCBI.", exact=True
                    )
                ).to_have_count(0)
                record("ordinary-sequence-search-keeps-local-blast-as-the-default")
                local_dialog.get_by_role(
                    "button", name="Close", exact=True
                ).last.click()
                tracks_before = post("/tracks/list")
                p = panel()
                p.locator('[data-job-id="sequence/' + first + '"]').get_by_role(
                    "button", name="Open result", exact=True
                ).click()
                expect(results).to_be_visible()
                assert post("/tracks/list") == tracks_before
                record(
                    "jobs-reopens-external-results-after-a-map-change-without-adding-assembly-tracks"
                )
                close_results()
                held_results = []

                def hold_report(route):
                    response = route.fetch(
                        url=api.url + "/sequence/remote_blast/result"
                    )
                    held_results.append((route, response))

                page.route("**/sequence/remote_blast/result", hold_report)
                p = panel()
                p.locator('[data-job-id="sequence/' + first + '"]').get_by_role(
                    "button", name="Open result", exact=True
                ).click()
                expect(
                    results.get_by_text("Loading external results…", exact=True)
                ).to_be_visible()
                deadline = time.monotonic() + 30
                while not held_results:
                    assert time.monotonic() < deadline
                    page.wait_for_timeout(25)
                close_results()
                page.unroute("**/sequence/remote_blast/result", hold_report)
                for route, response in held_results:
                    route.fulfill(response=response)
                page.evaluate(
                    "()=>new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve)))"
                )
                expect(results).to_have_count(0)
                record("late-report-loading-cannot-reopen-a-dismissed-external-viewer")
                empty = start("empty")
                wait_job(empty, "finished")
                expect(
                    results.get_by_text(
                        "NCBI returned no alignments matching these settings.",
                        exact=True,
                    )
                ).to_be_visible()
                assert post("/tracks/list") == tracks_before
                record("zero-hit-remote-search-has-a-completed-exportable-report")
                close_results()
                slow = start("slow")
                wait_job(slow, "running")
                search.get_by_role("button", name="Close", exact=True).last.click()
                queued_one = post(
                    "/sequence/search/jobs", {**base_request, "remoteConsent": True}
                )["jobId"]
                queued_two = post(
                    "/sequence/search/jobs", {**base_request, "remoteConsent": True}
                )["jobId"]
                directories = set((data / "processed/sequence-analysis").iterdir())
                rejected = page.request.post(
                    api.url + "/sequence/search/jobs",
                    data={**base_request, "remoteConsent": True},
                )
                assert (
                    rejected.status == 429
                    and set((data / "processed/sequence-analysis").iterdir())
                    == directories
                )
                count_before = len(calls())
                p = panel()
                p.locator('[data-job-id="sequence/' + queued_one + '"]').get_by_role(
                    "button", name="Cancel", exact=True
                ).click()
                wait_job(queued_one, "cancelled")
                replacement = post(
                    "/sequence/search/jobs", {**base_request, "remoteConsent": True}
                )["jobId"]
                for queued in (queued_two, replacement):
                    post("/jobs/cancel", {"id": "sequence/" + queued})
                    wait_job(queued, "cancelled")
                assert len(calls()) == count_before
                record(
                    "remote-queue-is-bounded-and-cancelling-queued-work-frees-capacity-without-uploading"
                )
                p.get_by_role("button", name="Close", exact=True).click()
                linked = post("/link_fasta", {"fastaFilename": "private.fa"})
                assert not linked.get("requiresConfirmation", False), linked
                assert post("/sequence/settings")["fastaLinked"] is True
                local = post(
                    "/sequence/search/jobs",
                    {
                        "queries": "ACGT",
                        "mode": "exact",
                        "scope": "assembly",
                        "bothStrands": True,
                        "degenerate": False,
                        "threads": 1,
                    },
                )
                assert wait_job(local["jobId"], "finished", 15)["hits"] > 0
                assert (
                    next(j for j in jobs() if j["jobId"] == slow)["status"] == "running"
                )
                record("waiting-for-ncbi-does-not-block-real-local-sequence-analysis")
                p = panel()
                row = p.locator('[data-job-id="sequence/' + slow + '"]')
                started = time.monotonic()
                with page.expect_response(
                    lambda response: response.url.endswith("/jobs/cancel")
                ):
                    row.get_by_role("button", name="Cancel", exact=True).click()
                timings["cancelAcknowledgementMs"] = (time.monotonic() - started) * 1000
                assert timings["cancelAcknowledgementMs"] < 1000
                wait_job(slow, "cancelled")
                receipt = [call for call in calls() if call["mode"] == "slow"][-1]
                assert receipt["parent"] == api.process.pid and not alive(receipt)
                assert not (
                    data / "processed/sequence-analysis" / slow / "remote-blast.json"
                ).exists()
                expect(
                    row.get_by_role("button", name="Open result", exact=True)
                ).to_have_count(0)
                record(
                    "jobs-cancels-a-running-provider-client-without-publishing-a-result"
                )
                p.get_by_role("button", name="Close", exact=True).click()
                failed = start("failure")
                wait_job(failed, "failed")
                expect(search.get_by_role("alert")).to_contain_text(
                    "Controlled provider unavailable"
                )
                record(
                    "provider-failure-retains-useful-diagnostics-without-automatic-resubmission"
                )
                search.get_by_role("button", name="Close", exact=True).last.click()
                timeout_job = start("slow", timeout=1)
                search.get_by_role("button", name="Close", exact=True).last.click()
                timed_out = wait_job(timeout_job, "failed", 80)
                assert (
                    "timeout" in timed_out["error"] and timed_out["outputFile"] is None
                )
                assert not alive(
                    [call for call in calls() if call["mode"] == "slow"][-1]
                )
                assert len([c for c in calls() if c["mode"] == "slow"]) == 2
                record("selected-provider-timeout-stops-waiting-and-does-not-resubmit")
                ribosomal = start("success", count=2, database="16S_ribosomal_RNA")
                wait_job(ribosomal, "finished")
                expect(results).to_be_visible()
                ribosomal_report = post(
                    "/sequence/remote_blast/result", {"jobId": ribosomal}
                )
                assert (
                    ribosomal_report["remoteDatabaseName"]
                    == "rRNA_typestrains/16S_ribosomal_RNA"
                )
                last_call = calls()[-1]
                assert (
                    last_call["args"][last_call["args"].index("-db") + 1]
                    == "rRNA_typestrains/16S_ribosomal_RNA"
                )
                record(
                    "16s-selection-uses-the-remote-service-name-and-records-it-in-the-report"
                )
                close_results()
                configure("malformed")
                malformed = post(
                    "/sequence/search/jobs", {**base_request, "remoteConsent": True}
                )["jobId"]
                assert "table format" in wait_job(malformed, "failed")["error"]
                assert not (
                    data
                    / "processed/sequence-analysis"
                    / malformed
                    / "remote-blast.json"
                ).exists()
                record(
                    "malformed-provider-data-fails-without-an-exportable-or-assembly-result"
                )
                truncated = start("success", count=20001)
                wait_job(truncated, "finished")
                expect(results).to_be_visible()
                expect(results.locator("tbody tr")).to_have_count(50)
                expect(results).to_contain_text("first 20,000 alignments")
                with page.expect_download() as download:
                    results.get_by_role(
                        "button", name="Save raw TSV…", exact=True
                    ).click()
                target = out / "all-truncated-provider-rows.tsv"
                download.value.save_as(target)
                assert len(target.read_text().splitlines()) == 20001
                record(
                    "large-provider-results-cap-the-json-view-and-preserve-every-row-in-raw-tsv"
                )
                close_results()
                assert not check.errors, check.errors
                assert source_hashes == {
                    name: hashlib.sha256((data / name).read_bytes()).hexdigest()
                    for name in source_hashes
                }
                (out / "results.json").write_text(
                    json.dumps(
                        {
                            "checks": checks,
                            "errors": check.errors,
                            "timings": timings,
                            "jobs": jobs(),
                            "providerCalls": calls(),
                            "sourceHashes": source_hashes,
                        },
                        indent=2,
                    )
                )
            except BaseException:
                if not page.is_closed():
                    page.screenshot(path=str(out / "failure.png"), timeout=5000)
                (out / "errors.json").write_text(json.dumps(check.errors))
                raise
            finally:
                browser.close()
    finally:
        web.shutdown()
        web.server_close()
        if old_override is None:
            os.environ.pop("HICT_BLASTN_BIN", None)
        else:
            os.environ["HICT_BLASTN_BIN"] = old_override


if __name__ == "__main__":
    main()
