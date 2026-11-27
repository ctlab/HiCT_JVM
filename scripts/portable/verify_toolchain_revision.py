#!/usr/bin/env python3
"""Require a verified toolchain built from the current sources before packaging."""
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts/toolchains"))

from prepare_bundled_tools import digest, source_key, verify


def verify_revision(platform):
    target = ROOT / "toolchains-dist" / platform
    if not target.is_dir():
        return
    try:
        recorded = json.loads((target / "prepared-tools.json").read_text(encoding="utf-8"))
        if recorded != {
            "platform": platform,
            "sourceKey": source_key(),
            "manifestSha256": digest(target / "manifest.json"),
        }:
            raise ValueError("toolchain source key or manifest digest differs")
        verify(target, platform)
    except (OSError, ValueError, KeyError) as exc:
        raise SystemExit(f"{target}: {exc}. Prepare the current bundled tools before packaging.") from exc


if __name__ == "__main__":
    verify_revision(sys.argv[1])
