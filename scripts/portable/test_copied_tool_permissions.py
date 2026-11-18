#!/usr/bin/env python3
"""Exercise permissions of staged manifest commands without changing their bytes."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

from verify_copied_tools import restore_command_permissions, verify
from prepare_bundled_tools import required_commands


@unittest.skipIf(os.name == "nt", "POSIX package permission checks")
class CopiedCommandPermissionsTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name) / "copied tools"
        self.root.mkdir()
        self.manifest = {"commands": {}, "files": [], "sha256": {}}
        for command in sorted(required_commands("linux_x86_64")):
            entry = f"sequence/payload/bin/{command}"
            path = self.root / entry
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("#!/bin/sh\nprintf 'fixture command\\n'\n")
            path.chmod(0o644)
            self.manifest["commands"][command] = entry
            self.manifest["files"].append(entry)
            self.manifest["sha256"][entry] = hashlib.sha256(path.read_bytes()).hexdigest()
        self.license = self.root / "share/LICENSE.txt"
        self.license.parent.mkdir()
        self.license.write_text("fixture license\n")
        self.license.chmod(0o644)
        self.manifest["files"].append("share/LICENSE.txt")
        self.manifest["sha256"]["share/LICENSE.txt"] = hashlib.sha256(self.license.read_bytes()).hexdigest()
        self.write_manifest()

    def write_manifest(self):
        (self.root / "manifest.json").write_text(json.dumps(self.manifest))

    def assert_not_executable(self):
        for entry in self.manifest["commands"].values():
            if isinstance(entry, str) and ".." not in Path(entry).parts:
                self.assertFalse(os.access(self.root / entry, os.X_OK))

    def test_nested_commands_execute_and_payload_hashes_are_unchanged(self):
        command = self.root / self.manifest["commands"]["seqkit"]
        with self.assertRaises(PermissionError):
            subprocess.run([str(command)], check=True)
        before = {entry: (self.root / entry).read_bytes() for entry in self.manifest["files"]}
        self.assertEqual(restore_command_permissions(self.root, "linux_x86_64"), 8)
        verify(self.root, "linux_x86_64")
        for entry in self.manifest["commands"].values():
            self.assertEqual(subprocess.check_output([str(self.root / entry)], text=True), "fixture command\n")
        self.assertEqual(before, {entry: (self.root / entry).read_bytes() for entry in before})
        self.assertFalse(os.access(self.license, os.X_OK))

    def test_restoration_is_idempotent_on_darwin_manifest_commands(self):
        for _ in range(2):
            restore_command_permissions(self.root, "darwin_arm64")
            verify(self.root, "darwin_arm64")

    def test_corrupt_command_fails_before_any_permissions_change(self):
        entry = self.manifest["commands"]["seqkit"]
        (self.root / entry).write_text("corrupt\n")
        with self.assertRaisesRegex(ValueError, "corrupt"):
            restore_command_permissions(self.root, "linux_x86_64")
        self.assert_not_executable()

    def test_undeclared_command_fails_before_any_permissions_change(self):
        entry = self.manifest["commands"]["seqkit"]
        self.manifest["files"].remove(entry)
        self.write_manifest()
        with self.assertRaisesRegex(ValueError, "checksummed"):
            restore_command_permissions(self.root, "linux_x86_64")
        self.assert_not_executable()

    def test_traversal_cannot_change_a_file_outside_the_copy(self):
        outside = self.root.parent / "outside"
        outside.write_text("untouched\n")
        outside.chmod(0o644)
        self.manifest["commands"]["bad"] = "../outside"
        self.write_manifest()
        with self.assertRaisesRegex(ValueError, "Unsafe"):
            restore_command_permissions(self.root, "linux_x86_64")
        self.assert_not_executable()
        self.assertFalse(os.access(outside, os.X_OK))

    def test_symlink_escape_is_rejected_before_any_permissions_change(self):
        outside = self.root.parent / "outside"
        outside.write_text("untouched\n")
        outside.chmod(0o644)
        (self.root / "escape").symlink_to(outside)
        self.manifest["commands"]["bad"] = "escape"
        self.write_manifest()
        with self.assertRaisesRegex(ValueError, "escapes"):
            restore_command_permissions(self.root, "linux_x86_64")
        self.assert_not_executable()
        self.assertFalse(os.access(outside, os.X_OK))

    def test_unsupported_platform_does_not_change_permissions(self):
        with self.assertRaisesRegex(ValueError, "POSIX"):
            restore_command_permissions(self.root, "windows_x86_64")
        self.assert_not_executable()


if __name__ == "__main__":
    unittest.main()
