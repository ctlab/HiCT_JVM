"""Exercise the generated Linux wrapper with a small, real tar.xz payload."""

import hashlib
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
import unittest

PACKAGER_PATH = Path(__file__).resolve().parents[1] / "portable/build_portable_linux.sh"


@unittest.skipUnless(sys.platform.startswith("linux"), "Linux portable wrapper")
class PortableExtractionTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="hict-portable-extraction-")
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.package = "HiCT-2.0.0-linux-x86_64"
        app = self.root / "source" / self.package
        (app / "bin").mkdir(parents=True)
        (app / "complete.txt").write_text("complete\n")
        launcher = app / "bin/hict"
        launcher.write_text(
            '#!/usr/bin/env bash\nset -eu\ntest "$(cat "$(dirname "$0")/../complete.txt")" = complete\nprintf "ready %s\\n" "$*"\n'
        )
        launcher.chmod(0o755)
        payload = self.root / "payload.tar.xz"
        with tarfile.open(payload, "w:xz") as archive:
            archive.add(app, arcname=self.package)
        self.digest = hashlib.sha256(payload.read_bytes()).hexdigest()
        builder = PACKAGER_PATH.read_text()
        start = builder.index('cat > "${RUN_PATH}" <<EOF\n')
        end = builder.index('\nEOF\ncat "${PAYLOAD_PATH}"', start) + len("\nEOF\n")
        self.wrapper = self.root / "HiCT.run"
        self.environment = {
            **os.environ,
            "APP_NAME": "HiCT",
            "VERSION": "2.0.0",
            "PLATFORM": "linux-x86_64",
            "PACKAGE_NAME": self.package,
            "ABI_SUFFIX": "",
            "PAYLOAD_SHA": self.digest,
            "BUNDLED_TAURI_BROWSER": "0",
            "RUN_PATH": str(self.wrapper),
            "XDG_CACHE_HOME": str(self.root / "cache"),
            "TMPDIR": str(self.root),
        }
        subprocess.run(
            ["bash"],
            input=builder[start:end],
            text=True,
            env=self.environment,
            check=True,
        )
        with self.wrapper.open("ab") as output:
            output.write(payload.read_bytes())
        self.wrapper.chmod(0o755)
        fake_bin = self.root / "fake-bin"
        fake_bin.mkdir()
        fake_tar = fake_bin / "tar"
        fake_tar.write_text(f"#!{sys.executable}\n" + """import os
from pathlib import Path
import sys
import time

args = sys.argv[1:]
with open(os.environ['HICT_TEST_TAR_CALLS'], 'a') as stream:
    stream.write('extract\\n')
if os.environ.get('HICT_TEST_TAR_FAIL') == '1':
    sys.exit(1)
if os.environ.get('HICT_TEST_TAR_SLOW') == '1':
    index = args.index('-C') + 1
    os.chdir(args[index])
    args[index] = '.'
    Path(os.environ['HICT_TEST_TAR_STARTED']).touch()
    deadline = time.monotonic() + 15
    while not Path(os.environ['HICT_TEST_TAR_RELEASE']).exists():
        if time.monotonic() >= deadline:
            raise RuntimeError('Timed out waiting for concurrent extraction')
        time.sleep(0.01)
real = os.environ['HICT_TEST_REAL_TAR']
os.execv(real, [real, *args])
""")
        fake_tar.chmod(0o755)
        self.calls = self.root / "tar-calls"
        self.started = self.root / "first-started"
        self.release = self.root / "release-first"
        self.environment.update(
            {
                "PATH": str(fake_bin) + os.pathsep + os.environ["PATH"],
                "HICT_TEST_REAL_TAR": shutil.which("tar"),
                "HICT_TEST_TAR_CALLS": str(self.calls),
                "HICT_TEST_TAR_STARTED": str(self.started),
                "HICT_TEST_TAR_RELEASE": str(self.release),
            }
        )
        self.cache = self.root / "HiCT.portable/payloads"

    def launch(self, **environment):
        return subprocess.run(
            [str(self.wrapper), "test"],
            env={**self.environment, **environment},
            text=True,
            capture_output=True,
            timeout=20,
        )

    def assert_ready(self, result):
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("ready test", result.stdout)

    def test_simultaneous_first_starts_publish_one_complete_cache(self):
        first = subprocess.Popen(
            [str(self.wrapper), "test"],
            env={**self.environment, "HICT_TEST_TAR_SLOW": "1"},
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            deadline = time.monotonic() + 10
            while not self.started.exists():
                self.assertIsNone(
                    first.poll(), "First wrapper stopped before extraction"
                )
                self.assertLess(
                    time.monotonic(), deadline, "First extraction did not start"
                )
                time.sleep(0.01)
            second = self.launch()
            self.release.touch()
            stdout, stderr = first.communicate(timeout=20)
            self.assert_ready(second)
            self.assertEqual(first.returncode, 0, stderr)
            self.assertIn("ready test", stdout)
            complete = list(self.cache.glob("*.complete"))
            self.assertEqual(len(complete), 1)
            self.assertEqual(
                (complete[0] / ".payload.sha256").read_text().strip(), self.digest
            )
            self.assertFalse(list(self.cache.glob(".*-extract.*")))
        finally:
            self.release.touch()
            if first.poll() is None:
                first.kill()
            first.communicate(timeout=20)

    def test_completed_payload_is_reused(self):
        self.assert_ready(self.launch())
        self.assert_ready(self.launch())
        self.assertEqual(self.calls.read_text().splitlines(), ["extract"])

    def test_invalid_existing_cache_is_preserved_and_recovered(self):
        invalid = self.cache / f"{self.package}-{self.digest}.complete"
        invalid.mkdir(parents=True)
        (invalid / "keep-me").write_text("original\n")
        self.assert_ready(self.launch())
        self.assert_ready(self.launch())
        self.assertEqual((invalid / "keep-me").read_text(), "original\n")
        self.assertEqual(self.calls.read_text().splitlines(), ["extract"])
        self.assertTrue(
            (Path(str(invalid) + ".recovered.1") / ".payload.sha256").is_file()
        )

    def test_failed_extraction_removes_only_its_staging_directory(self):
        failed = self.launch(HICT_TEST_TAR_FAIL="1")
        self.assertNotEqual(failed.returncode, 0)
        self.assertFalse(list(self.cache.glob("*.complete")))
        self.assertFalse(list(self.cache.glob(".*-extract.*")))
        self.assert_ready(self.launch())


if __name__ == "__main__":
    unittest.main()
