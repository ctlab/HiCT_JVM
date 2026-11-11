import unittest
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from set_release_version import release_version


class ReleaseVersionTest(unittest.TestCase):
    def test_shared_and_incrementing(self):
        self.assertEqual(
            release_version("1.0.211", "20", "1", "abcdef012345"), "1.0.231-ci1-abcdef0"
        )
        self.assertEqual(
            release_version("1.0.211-old-webui_123", "21", "1", "abcdef012345"),
            "1.0.232-ci1-abcdef0",
        )
        self.assertEqual(
            release_version("1.0.211", "20", "2", "abcdef012345"), "1.0.231-ci2-abcdef0"
        )

    def test_explicit_release_cli(self):
        script = Path(__file__).with_name("set_release_version.py").resolve()
        with tempfile.TemporaryDirectory() as folder:
            version_file = Path(folder) / "version.txt"
            version_file.write_text("1.0.260\n")
            subprocess.run(
                [sys.executable, str(script), "--version", "2.0.0"],
                cwd=folder,
                check=True,
            )
            self.assertEqual(version_file.read_text(), "2.0.0\n")
            result = subprocess.run(
                [sys.executable, str(script), "--version", "02.0.0"],
                cwd=folder,
                capture_output=True,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(version_file.read_text(), "2.0.0\n")

    def test_stable_release_tag_keeps_its_version(self):
        script = Path(__file__).with_name("set_release_version.py").resolve()
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "version.txt"
            path.write_text("2.0.0\n")
            environment = {
                **os.environ,
                "GITHUB_EVENT_NAME": "release",
                "GITHUB_REF_NAME": "v2.0.0",
            }
            subprocess.run(
                [sys.executable, str(script)], cwd=folder, env=environment, check=True
            )
            self.assertEqual(path.read_text(), "2.0.0\n")

    def test_reject_invalid(self):
        for base, run, attempt, sha in [
            ("bad", "1", "1", "abcdef0"),
            ("1.0.211", "0", "1", "abcdef0"),
            ("1.0.211", "1", "0", "abcdef0"),
            ("1.0.211", "1", "1", "bad"),
        ]:
            with self.assertRaises(ValueError):
                release_version(base, run, attempt, sha)


if __name__ == "__main__":
    unittest.main()
