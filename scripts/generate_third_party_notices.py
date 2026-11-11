#!/usr/bin/env python3
"""Aggregate installed production-package and runtime-JAR upstream notices."""

import argparse
import hashlib
import json
import re
from pathlib import Path
import zipfile


def is_notice(name):
    return (
        re.search(
            r"(?:^|[_.-])(licen[cs]e|notice|copying|copyright|patents|attribution)",
            name,
            re.IGNORECASE,
        )
        is not None
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--webui", type=Path, required=True)
    parser.add_argument(
        "--jars",
        type=Path,
        required=True,
        help="UTF-8 file with one resolved runtime JAR path per line",
    )
    parser.add_argument("--toolchains", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    texts = {}
    rows = []

    def register(name, license_name, notices):
        references = []
        for filename, raw in notices:
            content = "\n".join(
                line.rstrip()
                for line in raw.decode("utf-8", errors="strict").splitlines()
            ).strip()
            if not content:
                continue
            digest = hashlib.sha256(content.encode()).hexdigest()
            texts.setdefault(digest, content)
            references.append(f"{filename} [{digest[:12]}](#notice-{digest[:12]})")
        rows.append((name, str(license_name).replace("|", "\\|"), references))

    lock = json.loads((args.webui / "package-lock.json").read_text())
    for location, info in sorted(lock["packages"].items()):
        if not location or info.get("dev"):
            continue
        folder = args.webui / location
        metadata = json.loads((folder / "package.json").read_text())
        notices = [
            (p.name, p.read_bytes())
            for p in sorted(folder.iterdir())
            if p.is_file() and is_notice(p.name)
        ]
        register(
            f"npm {metadata['name']} {metadata['version']}",
            metadata.get("license", "See upstream package metadata"),
            notices,
        )
    for line in args.jars.read_text().splitlines():
        jar = Path(line)
        if not jar.is_file() or jar.suffix != ".jar":
            raise ValueError(f"Missing runtime JAR {jar.name}")
        with zipfile.ZipFile(jar) as archive:
            notices = [
                (name, archive.read(name))
                for name in sorted(archive.namelist())
                if not name.endswith("/") and is_notice(Path(name).name)
            ]
        register(
            f"JVM {jar.name}", "See embedded notices and upstream Maven POM", notices
        )
    for path in sorted(args.toolchains.rglob("*")):
        if (
            path.is_file()
            and "/share/licenses/" in path.as_posix()
            and is_notice(path.name)
        ):
            register(
                "Native " + path.relative_to(args.toolchains).as_posix(),
                "See upstream notice",
                [(path.name, path.read_bytes())],
            )

    output = [
        "# Third-party licenses and notices",
        "",
        "Generated from the installed production npm lock, resolved JVM runtime JARs",
        "and the staged native-tool license directories. Original upstream text is",
        "preserved below with normalized line endings and trailing whitespace. Identical",
        "texts share a SHA-256 reference. Packages without",
        "an embedded notice retain their upstream package metadata or Maven POM terms.",
        "",
        "Portable Java runtimes also include their complete notices under runtime/legal.",
        "Platform browser runtimes preserve their bundled licenses. Dataset terms and",
        "citations are recorded separately in each example manifest.",
        "",
        "## Component manifest",
        "",
        "| Component | Declared terms | Embedded notices |",
        "|---|---|---|",
    ]
    for name, terms, references in rows:
        output.append(
            f"| {name} | {terms} | {'<br>'.join(references) or 'Upstream metadata'} |"
        )
    for digest, content in sorted(texts.items()):
        output.extend(
            [
                "",
                f'<a id="notice-{digest[:12]}"></a>',
                "",
                f"## Notice {digest[:12]}",
                "",
                "SHA-256 `" + digest + "`.",
                "",
                "````text",
                content,
                "````",
            ]
        )
    args.output.write_text("\n".join(output) + "\n")
    print(f"Wrote {len(rows)} components and {len(texts)} unique notices")


if __name__ == "__main__":
    main()
