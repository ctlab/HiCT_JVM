#!/usr/bin/env python3
"""Check per-source provenance on a fresh physical copy of the example browser fixture."""

import argparse
import hashlib
import json
from pathlib import Path
import shutil
from urllib.error import HTTPError

from sequence_browser_harness import HictTestServer


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("jar", "source-dir", "output"):
        parser.add_argument("--" + name, required=True)
    args = parser.parse_args()
    source, out = Path(args.source_dir).absolute(), Path(args.output).absolute()
    if any(p != p.resolve() or not p.is_relative_to("/tmp") for p in (source, out)):
        parser.error("Use nonsymlinked disposable /tmp paths")
    if any(p.is_symlink() for p in source.rglob("*")):
        parser.error("Fixture copies must contain no symlinks")
    out.mkdir(exist_ok=False)
    data = out / "data"
    shutil.copytree(source, data, copy_function=shutil.copy2)
    hashes = {
        p.relative_to(data).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in data.rglob("*.hict.hdf5")
    }
    checks = []
    primary = "examples/jobs-automatic/example.hict.hdf5"
    secondary = "examples/jobs-wizard/example.hict.hdf5"
    capture = {
        "verifyAssemblyRoundTrip": True,
        "fingerprintFiles": True,
        "verifyTrackDefinitions": True,
    }
    with HictTestServer(args.jar, data, out / "server.log") as api:

        def post(path, body=None):
            return json.loads(api.http(path, {} if body is None else body))

        def record(name):
            checks.append({"test": name, "status": "passed"})
            print("Passed", name, flush=True)

        post("/open", {"filename": primary})
        status = post(
            "/secondary/open", {"filename": secondary, "allowMismatch": False}
        )
        assert status["attached"] and not status.get("requiresConfirmation"), status
        saved = post("/session/snapshot", capture)
        assert (
            saved["primary"]["exampleProvenance"]["catalogExample"]["id"]
            == "jobs-automatic"
        )
        assert (
            saved["secondary"]["exampleProvenance"]["catalogExample"]["id"]
            == "jobs-wizard"
        )
        record("primary-and-secondary-retain-independent-recorded-datasets")
        catalog = data / "examples/catalogs/jobs-fixture.json"
        catalog.unlink()
        post("/session/restore", saved)
        restored = post("/session/snapshot", capture)
        for name in ("primary", "secondary"):
            assert (
                restored[name]["exampleProvenance"] == saved[name]["exampleProvenance"]
            )
        record("both-records-survive-restore-without-a-live-catalog")
        bad = json.loads(json.dumps(saved))
        bad["secondary"]["exampleProvenance"][
            "matrixInputFilename"
        ] = "unrelated.hict.hdf5"
        try:
            post("/session/restore", bad)
            raise AssertionError("Mismatched Secondary provenance was accepted")
        except HTTPError as error:
            assert error.code == 400 and "exampleProvenance" in error.read().decode()
        unchanged = post("/session/snapshot", capture)
        for name in ("primary", "secondary"):
            assert (
                unchanged[name]["exampleProvenance"] == saved[name]["exampleProvenance"]
            )
        record("invalid-secondary-record-does-not-replace-either-source")
        post("/secondary/close")
        detached = post("/session/snapshot", capture)
        assert detached.get("secondary") is None
        assert (
            detached["primary"]["exampleProvenance"]
            == saved["primary"]["exampleProvenance"]
        )
        record("detaching-secondary-keeps-primary-provenance")
        post("/open", {"filename": "tiny-primary.hict.hdf5"})
        ordinary = post("/session/snapshot", capture)
        assert "exampleProvenance" not in ordinary["primary"]
        record("ordinary-replacement-does-not-inherit-either-dataset")
    assert hashes == {
        name: hashlib.sha256((data / name).read_bytes()).hexdigest() for name in hashes
    }
    (out / "results.json").write_text(
        json.dumps({"checks": checks, "sourceHashes": hashes}, indent=2)
    )


if __name__ == "__main__":
    main()
