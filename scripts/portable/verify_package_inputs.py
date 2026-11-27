#!/usr/bin/env python3
"""Check a version-selected JAR before staging a portable package."""
from pathlib import Path
import subprocess
import sys
from zipfile import ZipFile

from verify_toolchain_revision import ROOT, verify_revision


def verify_jar(jar_path, platform):
    verify_revision(platform)
    expected_revision = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
    ).strip()
    expected_webui = (ROOT / "webui-revision.txt").read_text(encoding="utf-8").strip()
    expected_version = (ROOT / "build/resources/main/version.txt").read_text(encoding="utf-8").strip()
    with ZipFile(jar_path) as jar:
        for name, expected in (
            ("backend-revision.txt", expected_revision),
            ("webui/webui-revision.txt", expected_webui),
            ("version.txt", expected_version),
        ):
            if jar.read(name).decode("utf-8").strip() != expected:
                raise ValueError(f"JAR {name} differs from the current build")
        target = ROOT / "toolchains-dist" / platform
        if target.is_dir():
            for name in ("manifest.json", "prepared-tools.json"):
                if jar.read(f"toolchains/{platform}/{name}") != (target / name).read_bytes():
                    raise ValueError(f"JAR toolchain {name} differs from the verified payload")


if __name__ == "__main__":
    try:
        verify_jar(Path(sys.argv[1]), sys.argv[2])
    except (OSError, ValueError, KeyError) as exc:
        raise SystemExit(f"Cannot package {sys.argv[1]}: {exc}. Rebuild the fat JAR.") from exc
