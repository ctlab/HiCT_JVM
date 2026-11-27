#!/usr/bin/env python3
"""Regression checks for download order and release notes carried into the channel."""

import unittest
from unittest.mock import patch

from portable_release_notes import QUICK_DOWNLOADS, highlights, notes
from update_branch_release_index import END, index_body


class PortableReleaseNotesTest(unittest.TestCase):
    version = "2.0.513-ci1-123abcd"
    repo = "ctlab/HiCT_JVM"
    tag = f"jvm-sfx-native-{version}"

    def render(self, suffixes):
        return notes(
            self.repo, self.tag, self.version, "123abcd" + "0" * 33, "123",
            [f"HiCT-{self.version}-{suffix}" for suffix in suffixes], ["Move contigs to assembly ends."]
        )

    def test_platform_links_come_before_guidance_and_changelog(self):
        suffixes = [
            "windows-x86_64.exe", "darwin-arm64.run", "darwin-x86_64.run",
            "linux-x86_64.run", "x86_64.AppImage",
        ]
        text = self.render(suffixes)
        positions = [text.index(QUICK_DOWNLOADS)]
        positions += [text.index(f"/releases/download/{self.tag}/HiCT-{self.version}-{suffix}") for suffix in suffixes]
        positions += [text.index(heading) for heading in (
            "### Windows security warning", "### macOS security warning", "### Linux choice summary",
            "### Changelog", "for provenance and checksums.",
        )]
        self.assertEqual(positions, sorted(positions))
        self.assertIn("from `123abcd" + "0" * 33 + "`", text)
        self.assertIn("**More info**, then **Run anyway**", text)
        self.assertIn("**Open Anyway**", text)
        self.assertIn("`ssh -L 8765:127.0.0.1:8765 user@cluster.example.org`", text)
        self.assertIn("`HICT_PORT=8765`", text)

    def test_optional_package_links_are_only_shown_for_available_files(self):
        text = self.render(["windows-x86_64-portable.zip", "linux-x86_64.run"])
        self.assertIn("Windows x64 portable ZIP", text)
        self.assertNotIn("windows-x86_64.exe", text)
        self.assertNotIn(".AppImage", text)
        self.assertNotIn("### macOS security warning", text)

    def test_changelog_uses_new_unreleased_entries_and_omits_old_ones(self):
        answers = {
            ("rev-parse", "prior^{commit}"): "xyz",
            ("show", "prior:CHANGELOG.md"): "# Changes\n\n## Unreleased\n\n- Existing\n\n## 2.0.0\n- Old",
            ("show", "abc:CHANGELOG.md"): "# Changes\n\n## Unreleased\n\n- New\n- Existing\n\n## 2.0.0\n- Old",
        }
        with patch("portable_release_notes.git", side_effect=lambda *args: answers[args]):
            self.assertEqual(highlights("prior", "abc"), ["New"])

    def test_channel_retains_prior_body_and_links_to_exact_downloads(self):
        body = self.render(["windows-x86_64.exe"])
        release = {"tag_name": self.tag, "html_url": f"https://github.com/{self.repo}/releases/tag/{self.tag}",
                   "body": body}
        text = index_body(self.repo, "jvm-sfx-native", self.version, release, {}, "Original channel notes")
        before, _, after = text.partition(END)
        self.assertIn(QUICK_DOWNLOADS, before)
        self.assertIn(f"/releases/download/{self.tag}/", before)
        self.assertIn("### Changelog", before)
        self.assertEqual(after.strip(), "Original channel notes")


if __name__ == "__main__":
    unittest.main()
