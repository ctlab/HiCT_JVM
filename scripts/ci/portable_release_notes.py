#!/usr/bin/env python3
"""Generate notes for one verified set of portable HiCT downloads."""

import argparse
from collections import Counter
from pathlib import Path
import re
import subprocess


QUICK_DOWNLOADS = "**For quick testing, download the exact artifact for your OS:**"
PLATFORMS = (
    ("Windows x64", "windows-x86_64.exe"),
    ("macOS Apple Silicon (ARM64)", "darwin-arm64.run"),
    ("macOS Intel (x64)", "darwin-x86_64.run"),
    ("Linux x64 runfile", "linux-x86_64.run"),
    ("Linux x64 AppImage", "x86_64.AppImage"),
)


def git(*args):
    return subprocess.check_output(["git", *args], text=True).strip()


def previous_release(repo, channel, tag, source):
    tags = git("tag", "--merged", source, "--sort=-version:refname", "--list", f"{channel}-*-ci1-*")
    pattern = re.compile(rf"{re.escape(channel)}-\d+\.\d+\.\d+-ci1-[0-9a-f]{{7}}")
    candidates = [item for item in tags.splitlines() if item != tag and pattern.fullmatch(item)]
    for candidate in candidates[:10]:
        result = subprocess.run(
            ["gh", "release", "view", candidate, "--repo", repo, "--json", "isDraft", "--jq", ".isDraft"],
            capture_output=True, text=True, check=False,
        )
        if result.returncode == 0 and result.stdout.strip() == "false":
            return candidate
    return None


def highlights(previous, source):
    if previous and git("rev-parse", f"{previous}^{{commit}}") == source:
        return ["Rebuilt the portable packages from the same source revision."]

    def entries(ref):
        notes = git("show", f"{ref}:CHANGELOG.md")
        return re.findall(r"^- (.+)$", notes.split("\n## ", 2)[1], re.MULTILINE)

    if previous:
        old = Counter(entries(previous))
        changes = []
        for line in entries(source):
            if old[line]:
                old[line] -= 1
            else:
                changes.append(line)
        if changes:
            return changes[:6]
    revision = f"{previous}..{source}" if previous else source
    subjects = git("log", "--first-parent", "--no-merges", "-6", "--format=%s", revision)
    if subjects:
        return subjects.splitlines()
    return ["Updated the portable packages."]


def notes(repo, tag, version, source, run_id, assets, changes):
    root = f"https://github.com/{repo}"
    base = f"{root}/releases/download/{tag}"
    prefix = f"HiCT-{version}-"
    files = set(assets)
    downloads = [(label, prefix + suffix) for label, suffix in PLATFORMS if prefix + suffix in files]
    windows = prefix + "windows-x86_64.exe"
    windows_zip = prefix + "windows-x86_64-portable.zip"
    if windows not in files and windows_zip in files:
        downloads.insert(0, ("Windows x64 portable ZIP", windows_zip))
    linux_run = prefix + "linux-x86_64.run"
    appimage = prefix + "x86_64.AppImage"

    lines = [
        f"Automated HiCT portable build from [`{source[:7]}`]({root}/commit/{source}).",
        "",
        f"[Latest Main build]({root}/releases/tag/latest-master) · "
        f"[Latest Development build]({root}/releases/tag/latest-jvm-sfx-native) · "
        f"[Workflow run]({root}/actions/runs/{run_id})",
        "",
        QUICK_DOWNLOADS,
        "",
    ]
    lines += [f"- **{label}** [`{filename}`]({base}/{filename})" for label, filename in downloads]
    if windows in files or windows_zip in files:
        lines += [
            "", "### Windows security warning", "",
            "If Microsoft Defender SmartScreen shows **Windows protected your PC**, verify the download "
            "against the SHA-256 checksums below. Select **More info**, then **Run anyway** if offered. "
            "Managed computers may require approval from IT. "
            "[Microsoft SmartScreen guidance]"
            "(https://learn.microsoft.com/en-us/windows/security/operating-system-security/"
            "virus-and-threat-protection/microsoft-defender-smartscreen/)",
        ]
    if any(filename.endswith(".run") and "-darwin-" in filename for _, filename in downloads):
        lines += [
            "", "### macOS security warning", "",
            "Choose the runfile for your processor. In Terminal, use `chmod +x` on the downloaded runfile, "
            "then launch that file. If macOS blocks the app because it cannot verify the developer, "
            "check the SHA-256 digest and look for **Open Anyway** in **System Settings → Privacy & Security**. "
            "[Apple Gatekeeper guidance](https://support.apple.com/en-us/102445)",
        ]
    if linux_run in files or appimage in files:
        lines += ["", "### Linux choice summary", ""]
        if linux_run in files:
            lines.append(f"- Start with the runfile on most Linux installations. Run `bash {linux_run}`.")
        if appimage in files:
            lines += [
                f"- Use the AppImage if the runfile reports a runtime error or on an older or minimal system such as an Alpine container. "
                f"Run `chmod +x {appimage}` and then `./{appimage}`.",
                f"- If FUSE is unavailable, run `APPIMAGE_EXTRACT_AND_RUN=1 ./{appimage}`.",
            ]
        lines += [
            "- Add `start-server` after the package name to run without the desktop launcher.",
            "- For an SSH cluster, forward the web and API ports with "
            "`ssh -L 8080:127.0.0.1:8080 -L 5000:127.0.0.1:5000 user@cluster.example.org`. "
            "Run HiCT on the remote host and open `http://localhost:8080` locally.",
        ]
    lines += ["", "### Changelog", "", *(f"- {change}" for change in changes), ""]
    lines += [
        f"See [`{prefix}build-origin.txt`]({base}/{prefix}build-origin.txt) and "
        f"[`{prefix}release-assets.sha256`]({base}/{prefix}release-assets.sha256) for provenance and checksums.",
    ]
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for argument in ("repo", "tag", "channel", "version", "source", "run-id"):
        parser.add_argument(f"--{argument}", required=True)
    parser.add_argument("--assets", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    previous = previous_release(args.repo, args.channel, args.tag, args.source)
    args.output.write_text(
        notes(args.repo, args.tag, args.version, args.source, args.run_id,
              (item.name for item in args.assets.iterdir() if item.is_file()),
              highlights(previous, args.source)),
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
