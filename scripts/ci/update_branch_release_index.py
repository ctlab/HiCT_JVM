#!/usr/bin/env python3
"""Update stable download links after an exact release is verified."""

import argparse
import json
from pathlib import Path
import re
from tempfile import TemporaryDirectory

from publish_versioned_release import file_digest, gh, matches, uploaded_assets


CHANNELS = {"main": ("latest-master", "Main"),
            "jvm-sfx-native": ("latest-jvm-sfx-native", "Development")}
START = "<!-- hict-current-downloads"
END = "<!-- /hict-current-downloads -->"
VERSION = r"\d+\.\d+\.\d+-ci1-[a-f0-9]{7}"
DOWNLOADS = {
    "Linux x64 runfile": "linux-x86_64.run",
    "Linux x64 AppImage": "x86_64.AppImage",
    "Windows x64 installer": "windows-x86_64.exe",
    "Windows x64 portable ZIP": "windows-x86_64-portable.zip",
    "macOS Apple Silicon": "darwin-arm64.run",
    "macOS Intel": "darwin-x86_64.run",
}


def release_by_tag(repo, tag):
    release = json.loads(gh("api", f"repos/{repo}/releases/tags/{tag}"))
    if release["tag_name"] != tag or release["draft"]:
        raise RuntimeError(f"Expected a published release at {tag}")
    return release


def verified_links(repo, release, version):
    tag = release["tag_name"]
    prefix = f"HiCT-{version}-"
    manifest_name = prefix + "release-assets.sha256"
    assets = uploaded_assets(repo, release["id"])
    names = {title: prefix + suffix for title, suffix in DOWNLOADS.items()}
    with TemporaryDirectory() as directory:
        manifest = Path(directory) / manifest_name
        gh("release", "download", tag, "--repo", repo, "--pattern", manifest_name,
           "--output", str(manifest))
        if not matches(assets[manifest_name], manifest, file_digest(manifest)):
            raise RuntimeError(f"Manifest checksum differs on {tag}")
        digests = {name: digest for digest, name in
                   (line.split(maxsplit=1) for line in manifest.read_text().splitlines())}
    expected = set(names.values()) | {prefix + suffix for suffix in (
        "linux-x86_64.tar.gz", "linux-x86_64.sha256", "x86_64.AppImage.sha256",
        "windows-x86_64.sha256", "darwin-arm64.tar.gz", "darwin-arm64.sha256",
        "darwin-x86_64.tar.gz", "darwin-x86_64.sha256", "build-origin.txt",
    )} | {f"hict_server-{version}-fat.jar"}
    if set(digests) != expected:
        raise RuntimeError(f"Manifest does not list all platform files for {tag}")
    for name in expected:
        asset = assets.get(name)
        if not asset or not digests.get(name) or asset["state"] != "uploaded" or asset["size"] <= 0 or (
            asset["digest"] != f"sha256:{digests[name]}"
        ):
            raise RuntimeError(f"Missing or unverified release asset: {name}")
    names["SHA-256 checksums"] = manifest_name
    return {title: assets[name]["browser_download_url"] for title, name in names.items()}


def index_body(repo, channel, version, release, links, previous_body):
    alias, title = CHANNELS[channel]
    other_tag, other_title = next(value for key, value in CHANNELS.items() if key != channel)
    body = previous_body or ""
    if body.startswith(START):
        marker, separator, original = body.partition(END)
        if not separator:
            raise RuntimeError("Incomplete previous build index marker")
        previous = re.match(rf"{re.escape(START)} ({VERSION}) -->", marker)
        if not previous:
            raise RuntimeError("Invalid previous build index version")
        version_key = lambda value: tuple(map(int, value.split("-", 1)[0].split(".")))
        if version_key(previous[1]) > version_key(version):
            return body
        body = original.lstrip("\n")
    lines = [f"{START} {version} -->", f"## Latest {title} build",
             f"Current build: **{version}** · [Release and downloads]({release['html_url']})", "",
             f"[Latest {other_title} build](https://github.com/{repo}/releases/tag/{other_tag})", ""]
    lines.extend(f"- [{label}]({url})" for label, url in links.items())
    if release["tag_name"] != alias:
        lines.extend(["", "The Assets section preserves the files originally attached to this channel page."])
    return "\n".join([*lines, END, "", body])


def update(repo, channel, tag, version):
    alias, title = CHANNELS[channel]
    if not re.fullmatch(VERSION, version) or tag not in (f"{channel}-{version}", alias):
        raise RuntimeError("Expected a versioned release for the selected channel")
    current = release_by_tag(repo, alias)
    release = current if tag == alias else release_by_tag(repo, tag)
    links = verified_links(repo, release, version)
    body = index_body(repo, channel, version, release, links, current["body"])
    options = ("--prerelease=false", "--latest=true") if channel == "main" else ("--latest=false",)
    with TemporaryDirectory() as directory:
        notes = Path(directory) / "notes.md"
        notes.write_text(body, encoding="utf-8")
        gh("release", "edit", alias, "--repo", repo, "--title", f"Latest {title} build",
           "--notes-file", str(notes), *options)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", required=True)
    parser.add_argument("--channel", choices=CHANNELS, required=True)
    parser.add_argument("--tag", required=True)
    parser.add_argument("--version", required=True)
    args = parser.parse_args()
    update(args.repo, args.channel, args.tag, args.version)
