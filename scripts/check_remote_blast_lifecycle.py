#!/usr/bin/env python3
"""Verify bounded remote output and parent-only JVM shutdown with an owned command."""

import argparse
import json
import os
from pathlib import Path
import time

from remote_blast_fixture import make_executable
from sequence_browser_harness import HictTestServer


def alive(receipt):
    try:
        fields = (
            Path(f"/proc/{receipt['pid']}/stat").read_text().rsplit(")", 1)[1].split()
        )
        return fields[0] != "Z" and fields[19] == receipt["startTicks"]
    except FileNotFoundError:
        return False


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--jar", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    out = Path(args.output).absolute()
    assert out == out.resolve() and out.is_relative_to("/tmp")
    out.mkdir(exist_ok=False)
    data = out / "data"
    data.mkdir()
    fixture = out / "remote-fixture"
    executable = make_executable(fixture)
    previous = os.environ.get("HICT_BLASTN_BIN")
    os.environ["HICT_BLASTN_BIN"] = str(executable)
    receipt = None
    try:
        with HictTestServer(args.jar, data, out / "server.log") as api:

            def post(path, body=None):
                return json.loads(api.http(path, {} if body is None else body))

            def submit(mode):
                (fixture / "control.json").write_text(json.dumps({"mode": mode}))
                return post(
                    "/sequence/search/jobs",
                    {
                        "queries": "ACGT" * 20,
                        "mode": "blast",
                        "scope": "database",
                        "searchTarget": "NCBI_REMOTE",
                        "remoteConsent": True,
                        "bothStrands": True,
                        "remoteDatabase": "nt",
                        "remoteTimeoutMinutes": 1,
                    },
                )["jobId"]

            oversized = submit("oversized")
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                job = next(
                    j for j in post("/sequence/jobs/list") if j["jobId"] == oversized
                )
                if job["status"] == "failed":
                    break
                time.sleep(0.1)
            assert job["status"] == "failed" and job["outputFile"] is None, job
            directory = data / "processed/sequence-analysis" / oversized
            assert not (directory / "remote-blast.tsv").exists()
            assert not (directory / "remote-blast.json").exists()
            slow = submit("slow")
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                receipts = [
                    json.loads(p.read_text())
                    for p in (fixture / "calls").glob("*.json")
                ]
                receipt = next((r for r in receipts if r["mode"] == "slow"), None)
                if receipt is not None:
                    break
                time.sleep(0.1)
            assert receipt and alive(receipt) and receipt["parent"] == api.process.pid
            started = time.monotonic()
            # Signal the JVM only. Signalling its process group would mask missing cleanup.
            api.process.terminate()
            api.process.wait(timeout=20)
            deadline = time.monotonic() + 5
            while alive(receipt) and time.monotonic() < deadline:
                time.sleep(0.05)
            assert not alive(receipt), receipt
            directory = data / "processed/sequence-analysis" / slow
            assert not (directory / "remote-blast.json").exists()
            assert not (directory / "remote-blast.tsv").exists()
            (out / "results.json").write_text(
                json.dumps(
                    {
                        "checks": [
                            "oversized-result-rejected-without-publication",
                            "parent-only-shutdown-stops-owned-provider-client",
                        ],
                        "shutdownMs": (time.monotonic() - started) * 1000,
                        "providerReceipt": receipt,
                        "oversizedJob": job,
                    },
                    indent=2,
                )
            )
            print("Bounded output and parent-only shutdown passed", flush=True)
    finally:
        # Never leave the controlled sleeper behind if shutdown cleanup fails.
        if receipt and alive(receipt):
            os.kill(receipt["pid"], 9)
        if previous is None:
            os.environ.pop("HICT_BLASTN_BIN", None)
        else:
            os.environ["HICT_BLASTN_BIN"] = previous


if __name__ == "__main__":
    main()
