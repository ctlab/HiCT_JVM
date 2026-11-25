#!/usr/bin/env python3
"""Add verified assets to an exact branch release, then publish its draft."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
from tempfile import TemporaryDirectory
import time


def gh(*args):
    return subprocess.check_output(["gh", *args], text=True)


def github_pages(endpoint):
    pages = json.loads(gh("api", "--paginate", "--slurp", endpoint))
    return [entry for page in pages for entry in page]


def file_digest(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def matches(asset, file, digest):
    return (
        asset["state"] == "uploaded"
        and asset["size"] == file.stat().st_size
        and asset["digest"] == f"sha256:{digest}"
    )


def current_release(repo, tag):
    releases = github_pages(f"repos/{repo}/releases?per_page=100")
    return next((release for release in releases if release["tag_name"] == tag), None)


def uploaded_assets(repo, release_id):
    assets = github_pages(f"repos/{repo}/releases/{release_id}/assets?per_page=100")
    by_name = {asset["name"]: asset for asset in assets}
    if len(by_name) != len(assets):
        raise RuntimeError("The release has duplicate asset names")
    return by_name


def use_previous_origin(repo, tag, asset, origin):
    with TemporaryDirectory() as directory:
        previous = Path(directory) / origin.name
        gh("release", "download", tag, "--repo", repo, "--pattern", origin.name,
           "--output", str(previous))
        if not matches(asset, previous, file_digest(previous)):
            raise RuntimeError("The existing build origin checksum differs")
        attempt_line = r"^run_attempt: \d+\n"
        if re.sub(attempt_line, "", previous.read_text(), flags=re.M) != re.sub(
            attempt_line, "", origin.read_text(), flags=re.M,
        ):
            raise RuntimeError("The existing release has different build provenance")
        origin.write_bytes(previous.read_bytes())


def upload_missing_asset(repo, tag, release_id, file, digest):
    for attempt in range(3):
        present = uploaded_assets(repo, release_id).get(file.name)
        if present:
            break
        print(f"Uploading {file.name} (attempt {attempt + 1})", flush=True)
        result = subprocess.run(["gh", "release", "upload", tag, "--repo", repo, str(file)])
        if result.returncode == 0:
            break
        time.sleep(2 ** attempt)
    # The asset list can lag behind the upload response.
    for delay in (0, 1, 2, 4, 8, 16):
        if delay:
            time.sleep(delay)
        present = uploaded_assets(repo, release_id).get(file.name)
        if not present or present["state"] != "uploaded":
            continue
        if matches(present, file, digest):
            return
        break
    raise RuntimeError(f"Could not verify uploaded asset: {file.name}")


def publish(repo, tag, version, source, directory, notes, *, channel="jvm-sfx-native"):
    if (
        not re.fullmatch(r"\d+\.\d+\.\d+-ci1-[0-9a-f]{7}", version)
        or not version.endswith(f"-{source[:7]}")
        or channel not in ("main", "jvm-sfx-native")
        or tag != f"{channel}-{version}"
    ):
        raise RuntimeError("Expected an exact versioned branch release tag")
    prerelease = channel == "jvm-sfx-native"
    ref = json.loads(gh("api", f"repos/{repo}/git/ref/tags/{tag}"))["object"]
    if ref["type"] != "commit" or ref["sha"] != source:
        raise RuntimeError(f"Release tag {tag} does not point to {source}")

    origin = directory / f"HiCT-{version}-build-origin.txt"
    manifest = directory / f"HiCT-{version}-release-assets.sha256"
    digests = {}
    for line in manifest.read_text().splitlines():
        digest, name = line.split(maxsplit=1)
        if not re.fullmatch(r"[a-f0-9]{64}", digest) or name in digests:
            raise RuntimeError(f"Invalid checksum entry: {name}")
        digests[name] = digest
    staged_files = {file.name for file in directory.iterdir() if file.is_file()}
    if file_digest(origin) != digests.get(origin.name) or set(digests) | {manifest.name} != staged_files:
        raise RuntimeError("The staged files do not match the release manifest")

    release = current_release(repo, tag)
    if release is None:
        gh("release", "create", tag, "--repo", repo, "--verify-tag", "--draft",
           f"--prerelease={str(prerelease).lower()}", "--latest=false",
           "--title", f"HiCT {version} ({'Development' if prerelease else 'Main'})", "--notes-file", str(notes))
        # The authenticated release listing can lag behind draft creation.
        for delay in (1, 2, 4, 8, 16, 30):
            time.sleep(delay)
            release = current_release(repo, tag)
            if release is not None:
                break
        if release is None:
            raise RuntimeError(f"Created draft for {tag} but it did not become visible")
    if release["prerelease"] != prerelease or f"from `{source}`" not in (release["body"] or ""):
        raise RuntimeError(f"Unexpected release metadata for {tag}")

    assets = uploaded_assets(repo, release["id"])
    if origin.name in assets:
        use_previous_origin(repo, tag, assets[origin.name], origin)
        digests[origin.name] = file_digest(origin)
        manifest.write_text("".join(f"{digests[name]}  {name}\n" for name in sorted(digests)))
    digests[manifest.name] = file_digest(manifest)
    for name, asset in assets.items():
        if name not in digests or not matches(asset, directory / name, digests[name]):
            raise RuntimeError(f"Unexpected or mismatched existing asset: {name}")

    if release["draft"]:
        for name in [origin.name, *sorted(set(digests) - {origin.name, manifest.name}), manifest.name]:
            if name not in assets:
                upload_missing_asset(repo, tag, release["id"], directory / name, digests[name])
    final = uploaded_assets(repo, release["id"])
    if set(final) != set(digests) or any(
        not matches(asset, directory / name, digests[name]) for name, asset in final.items()
    ):
        raise RuntimeError("Release assets do not match the complete manifest")
    if release["draft"]:
        gh("release", "edit", tag, "--repo", repo, "--draft=false", "--latest=false")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--notes", type=Path, required=True)
    args = parser.parse_args()
    publish(os.environ["GITHUB_REPOSITORY"], os.environ["HICT_PORTABLE_RELEASE_TAG"],
            os.environ["HICT_BUILD_VERSION"], os.environ["GITHUB_SHA"], args.directory, args.notes,
            channel=os.environ.get("HICT_RELEASE_CHANNEL", "jvm-sfx-native"))
