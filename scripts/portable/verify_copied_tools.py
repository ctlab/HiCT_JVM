#!/usr/bin/env python3
"""Verify copied toolchain bytes and optionally restore POSIX command permissions."""
import argparse
import json
from pathlib import Path
import stat
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'toolchains'))
from prepare_bundled_tools import digest, verify


def restore_command_permissions(directory, platform):
    """JAR extraction drops executable bits, including nested sequence commands."""
    if platform not in ("linux_x86_64", "darwin_arm64", "darwin_x86_64"):
        raise ValueError("Command permission restoration is only for POSIX packages")
    root = directory.resolve(strict=True)
    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    commands = manifest.get("commands")
    if not isinstance(commands, dict) or not commands:
        raise ValueError("The copied toolchain must declare its command paths")
    targets = []
    # Validate every command before changing any file metadata. Signed bytes stay intact.
    for command, entry in commands.items():
        if not isinstance(entry, str) or not entry or Path(entry).is_absolute() or ".." in Path(entry).parts:
            raise ValueError("Unsafe command path: " + str(command))
        path = root / entry
        if not path.resolve(strict=True).is_relative_to(root) or not path.is_file():
            raise ValueError("Command escapes the copied toolchain: " + str(command))
        if entry not in manifest.get("files", []) or digest(path) != manifest.get("sha256", {}).get(entry):
            raise ValueError("Command is not checksummed or is corrupt: " + str(command))
        targets.append(path)
    for path in targets:
        path.chmod(stat.S_IMODE(path.stat().st_mode) | 0o111)
    return len(set(targets))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', type=Path, required=True)
    parser.add_argument('--platform', required=True)
    parser.add_argument('--restore-command-permissions', action='store_true',
                        help='Restore execute bits of checksummed manifest commands after JAR extraction')
    args = parser.parse_args()
    if args.restore_command_permissions:
        count = restore_command_permissions(args.directory, args.platform)
        print('Restored copied command permissions:', count)
    verify(args.directory, args.platform)
    print('Verified copied toolchain checksums:', args.directory)


if __name__ == '__main__':
    main()
