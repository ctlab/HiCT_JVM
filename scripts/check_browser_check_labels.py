#!/usr/bin/env python3
"""Check literal Playwright labels against the pinned WebUI and bundled guide."""

import argparse
import ast
from collections import defaultdict
from html import unescape
import json
from pathlib import Path
import re
import sys
import unicodedata


ROOT = Path(__file__).resolve().parents[1]
TEXT_SUFFIXES = {".vue", ".ts", ".tsx", ".js", ".json", ".html"}


def normalize(text):
    return re.sub(r"\s+", " ", unicodedata.normalize("NFC", unescape(text))).strip().casefold()


def check_labels(path):
    module = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for node in ast.walk(module):
        if not isinstance(node, ast.Call):
            continue
        method = getattr(node.func, "attr", "")
        candidates = [
            kw.value for kw in node.keywords
            if kw.arg == "has_text"
            or (method == "get_by_role" and kw.arg == "name")
            or (method == "get_by_label" and kw.arg == "text")
        ]
        if method == "get_by_label":
            candidates.extend(node.args[:1])
        for value in candidates:
            if isinstance(value, ast.Constant) and isinstance(value.value, str) and value.value.strip():
                yield value.value, value.lineno


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--webui", type=Path, default=ROOT.parent / "HiCT_WebUI")
    parser.add_argument("--allowlist", type=Path, default=ROOT / "scripts/browser-check-dynamic-labels.json")
    args = parser.parse_args()
    source = args.webui / "src"
    if not (source / "app").is_dir():
        parser.error(f"WebUI source is missing at {source}")

    sources = sorted(path for path in source.rglob("*") if path.suffix in TEXT_SUFFIXES and path.is_file())
    sources.append(ROOT / "docs/user-guide.md")
    corpus = "\n".join(normalize(path.read_text(encoding="utf-8")) for path in sources)
    allowed = json.loads(args.allowlist.read_text(encoding="utf-8"))
    labels = defaultdict(list)
    for check in sorted((ROOT / "scripts").glob("check_*.py")):
        for label, line in check_labels(check):
            labels[label].append((check.name, line))

    missing = False
    for label, locations in sorted(labels.items()):
        if normalize(label) in corpus:
            continue
        for filename, line in locations:
            if allowed.get(label, {}).get(filename):
                continue
            print(f"{filename}:{line}: label missing from the WebUI: {label!r}", file=sys.stderr)
            missing = True
    if missing:
        return 1
    print(f"Verified {len(labels)} distinct browser selector labels against the WebUI.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
