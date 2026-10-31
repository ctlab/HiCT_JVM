#!/usr/bin/env python3
"""Assign the same unique version to every job of a Portable Packages run."""
import os
from pathlib import Path
import re


def release_version(base: str, run_number: str, attempt: str, sha: str) -> str:
    match = re.fullmatch(r"(\d+)\.(\d+)\.(\d+)(?:-[\w.-]+)?", base.strip())
    if not match or not run_number.isdecimal() or int(run_number) < 1:
        raise ValueError("Expected a semantic base version and positive GitHub run number")
    if not attempt.isdecimal() or int(attempt) < 1 or not re.fullmatch(r"[0-9a-f]{7,40}", sha):
        raise ValueError("Invalid GitHub run attempt or source SHA")
    major, minor, patch = map(int, match.groups())
    # run_number is shared by platform jobs and increases for every new workflow
    # run; attempt distinguishes re-runs without changing the committed baseline.
    return f"{major}.{minor}.{patch + int(run_number)}-ci{attempt}-{sha[:7]}"


if __name__ == "__main__":
    path = Path("version.txt")
    version = release_version(path.read_text(), os.environ["GITHUB_RUN_NUMBER"],
                              os.environ["GITHUB_RUN_ATTEMPT"], os.environ["GITHUB_SHA"])
    path.write_text(version + "\n")
    print(f"Portable release version: {version}")
