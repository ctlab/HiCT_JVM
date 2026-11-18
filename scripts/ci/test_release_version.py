import unittest
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from set_release_version import release_version


class ReleaseVersionTest(unittest.TestCase):
    def test_explicit_ci_environment_receives_exact_written_version(self):
        script = Path(__file__).with_name("set_release_version.py").resolve()
        with tempfile.TemporaryDirectory() as folder:
            version_file = Path(folder) / "version.txt"
            env_file = Path(folder) / "github env.txt"
            version_file.write_text("2.0.49\n")
            env_file.write_text("PREVIOUS=value\n")
            environment = {**os.environ, "GITHUB_ENV": str(env_file)}
            subprocess.run(
                [sys.executable, str(script), "--version", "2.0.50", "--github-env"],
                cwd=folder,
                env=environment,
                check=True,
                capture_output=True,
            )
            self.assertEqual(version_file.read_text(), "2.0.50\n")
            self.assertEqual(
                env_file.read_text(), "PREVIOUS=value\nHICT_BUILD_VERSION=2.0.50\n"
            )
            # Local stamping has no implicit side effect on an inherited CI file.
            subprocess.run(
                [sys.executable, str(script), "--version", "2.0.51"],
                cwd=folder,
                env=environment,
                check=True,
                capture_output=True,
            )
            self.assertEqual(
                env_file.read_text(), "PREVIOUS=value\nHICT_BUILD_VERSION=2.0.50\n"
            )
            environment.pop("GITHUB_ENV")
            result = subprocess.run(
                [sys.executable, str(script), "--version", "2.0.52", "--github-env"],
                cwd=folder,
                env=environment,
                capture_output=True,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(version_file.read_text(), "2.0.51\n")

    def test_shared_and_incrementing(self):
        self.assertEqual(
            release_version("1.0.211", "20", "abcdef012345"), "1.0.231-ci1-abcdef0"
        )
        self.assertEqual(
            release_version("1.0.211-old-webui_123", "21", "abcdef012345"),
            "1.0.232-ci1-abcdef0",
        )

    def test_retry_uses_the_same_version_as_successful_jobs(self):
        script = Path(__file__).with_name("set_release_version.py").resolve()
        with tempfile.TemporaryDirectory() as folder:
            version = Path(folder) / "version.txt"
            environment = {
                **os.environ,
                "GITHUB_EVENT_NAME": "push",
                "GITHUB_REF_NAME": "jvm-sfx-native",
                "GITHUB_RUN_NUMBER": "20",
                "GITHUB_SHA": "abcdef012345",
            }
            for attempt in ("1", "2", "3"):
                version.write_text("1.0.211\n")
                subprocess.run(
                    [sys.executable, str(script)],
                    cwd=folder,
                    env={**environment, "GITHUB_RUN_ATTEMPT": attempt},
                    check=True,
                    capture_output=True,
                )
                self.assertEqual(version.read_text(), "1.0.231-ci1-abcdef0\n")

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
        for base, run, sha in [
            ("bad", "1", "abcdef0"),
            ("1.0.211", "0", "abcdef0"),
            ("1.0.211", "1", "bad"),
        ]:
            with self.assertRaises(ValueError):
                release_version(base, run, sha)


if __name__ == "__main__":
    unittest.main()
