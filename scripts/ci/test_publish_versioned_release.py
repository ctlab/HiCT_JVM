import json
from pathlib import Path
from subprocess import CompletedProcess
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from publish_versioned_release import file_digest, publish


class DraftPublicationTest(unittest.TestCase):
    repo = "ctlab/HiCT_JVM"
    source = "0123456" + "7" * 33
    version = "2.0.500-ci1-0123456"
    tag = f"jvm-sfx-native-{version}"

    def setUp(self):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        root = Path(directory.name)
        self.dist = root / "release-dist"
        self.dist.mkdir()
        self.notes = root / "release-body.md"
        self.notes.write_text(f"Automated build from `{self.source}`.\n")
        self.origin = self.dist / f"HiCT-{self.version}-build-origin.txt"
        self.manifest = self.dist / f"HiCT-{self.version}-release-assets.sha256"
        self.package = self.dist / f"HiCT-{self.version}-linux-x86_64.run"
        self.package.write_bytes(b"portable installer")
        self.origin.write_text(f"source_sha: {self.source}\nrun_attempt: 1\n")
        self.write_manifest()
        self.release = None
        self.assets = {}
        self.contents = {}
        self.writes = []
        self.upload_failures = 0

    def write_manifest(self):
        self.manifest.write_text("".join(
            f"{file_digest(file)}  {file.name}\n" for file in (self.origin, self.package)
        ))

    def save_asset(self, file):
        self.contents[file.name] = file.read_bytes()
        self.assets[file.name] = {
            "name": file.name, "state": "uploaded", "size": file.stat().st_size,
            "digest": f"sha256:{file_digest(file)}",
        }

    def fake_gh(self, *args):
        command = args[:2]
        if command == ("api", f"repos/{self.repo}/git/ref/tags/{self.tag}"):
            return json.dumps({"object": {"type": "commit", "sha": self.source}})
        if command == ("release", "create"):
            self.assertIn("--verify-tag", args)
            self.assertIn("--draft", args)
            self.assertIn("--latest=false", args)
            self.writes.append("create")
            self.release = {"id": 8, "tag_name": self.tag, "draft": True,
                            "prerelease": True, "body": self.notes.read_text()}
            return ""
        if command == ("release", "download"):
            path = Path(args[args.index("--output") + 1])
            path.write_bytes(self.contents[path.name])
            return ""
        if command == ("release", "edit"):
            self.assertIn("--draft=false", args)
            self.assertEqual({self.origin.name, self.package.name, self.manifest.name}, set(self.assets))
            self.release["draft"] = False
            self.writes.append("publish")
            return ""
        raise AssertionError(args)

    def fake_upload(self, args):
        self.assertEqual(args[:4], ["gh", "release", "upload", self.tag])
        self.assertNotIn("--clobber", args)
        file = Path(args[-1])
        self.assertNotIn(file.name, self.assets)
        self.writes.append(file.name)
        if self.upload_failures:
            self.upload_failures -= 1
            return CompletedProcess(args, 1)
        self.save_asset(file)
        return CompletedProcess(args, 0)

    def fake_pages(self, endpoint):
        if endpoint == f"repos/{self.repo}/releases?per_page=100":
            return [self.release] if self.release else []
        self.assertEqual(endpoint, f"repos/{self.repo}/releases/8/assets?per_page=100")
        return list(self.assets.values())

    def publish(self):
        with (
            patch("publish_versioned_release.gh", self.fake_gh),
            patch("publish_versioned_release.github_pages", self.fake_pages),
            patch("publish_versioned_release.subprocess.run", self.fake_upload),
            patch("publish_versioned_release.time.sleep"),
        ):
            publish(self.repo, self.tag, self.version, self.source, self.dist, self.notes)

    def test_uploads_serially_and_retries_before_publishing(self):
        self.upload_failures = 1
        self.publish()
        self.assertEqual(self.writes, ["create", self.origin.name, self.origin.name,
                                       self.package.name, self.manifest.name, "publish"])

    def test_resumes_draft_with_original_origin(self):
        self.fake_gh("release", "create", "--verify-tag", "--draft", "--latest=false")
        self.save_asset(self.origin)
        self.origin.write_text(self.origin.read_text().replace("run_attempt: 1", "run_attempt: 2"))
        self.write_manifest()
        self.publish()
        self.assertEqual(self.writes, ["create", self.package.name, self.manifest.name, "publish"])
        self.assertIn("run_attempt: 1", self.origin.read_text())
        self.assertIn(file_digest(self.origin), self.manifest.read_text())

    def test_mismatched_existing_asset_leaves_draft_and_assets_intact(self):
        self.fake_gh("release", "create", "--verify-tag", "--draft", "--latest=false")
        self.save_asset(self.package)
        self.assets[self.package.name]["digest"] = "sha256:" + "0" * 64
        with self.assertRaisesRegex(RuntimeError, "mismatched existing asset"):
            self.publish()
        self.assertTrue(self.release["draft"])
        self.assertEqual(self.writes, ["create"])

    def test_existing_public_release_is_read_only(self):
        self.fake_gh("release", "create", "--verify-tag", "--draft", "--latest=false")
        self.release["draft"] = False
        self.save_asset(self.origin)
        with self.assertRaisesRegex(RuntimeError, "complete manifest"):
            self.publish()
        self.assertEqual(self.writes, ["create"])


if __name__ == "__main__":
    unittest.main()
