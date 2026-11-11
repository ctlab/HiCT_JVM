#!/usr/bin/env python3
"""Assign stable release tags or run-specific portable package versions."""

import argparse
import os
from pathlib import Path
import re

STABLE_VERSION_PATTERN = r"(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)"


def release_version(base: str, run_number: str, attempt: str, sha: str) -> str:
    match = re.fullmatch(r"(\d+)\.(\d+)\.(\d+)(?:-[\w.-]+)?", base.strip())
    if not match or not run_number.isdecimal() or int(run_number) < 1:
        raise ValueError(
            "Expected a semantic base version and positive GitHub run number"
        )
    if (
        not attempt.isdecimal()
        or int(attempt) < 1
        or not re.fullmatch(r"[0-9a-f]{7,40}", sha)
    ):
        raise ValueError("Invalid GitHub run attempt or source SHA")
    major, minor, patch = map(int, match.groups())
    # run_number is shared by platform jobs and increases for every new workflow
    # run; attempt distinguishes re-runs without changing the committed baseline.
    return f"{major}.{minor}.{patch + int(run_number)}-ci{attempt}-{sha[:7]}"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--version", help="Assign an explicit stable semantic release version"
    )
    args = parser.parse_args()
    path = Path("version.txt")
    release_tag = os.environ.get("GITHUB_REF_NAME", "").removeprefix("v")
    if (
        args.version is None
        and os.environ.get("GITHUB_EVENT_NAME") == "release"
        and re.fullmatch(STABLE_VERSION_PATTERN, release_tag)
    ):
        args.version = release_tag
    if args.version is not None:
        if not re.fullmatch(STABLE_VERSION_PATTERN, args.version):
            parser.error("--version must be a stable semantic version such as 2.0.0")
        version = args.version
    else:
        version = release_version(
            path.read_text(),
            os.environ["GITHUB_RUN_NUMBER"],
            os.environ["GITHUB_RUN_ATTEMPT"],
            os.environ["GITHUB_SHA"],
        )
    path.write_text(version + "\n")
    print(f"Portable release version: {version}")
