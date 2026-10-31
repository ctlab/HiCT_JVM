"""Backport the zlib platform guard for the pinned BLAST macOS source build.

TARGET_OS_MAC also denotes modern macOS; it must not enable the classic Mac
OS fdopen replacement. Upstream zlib 1.3.1 uses only MACOS for that branch.
"""
import argparse
import hashlib
import json
from pathlib import Path

HEADER = Path('include/util/compress/zlib/zutil.h')
OLD = b'#if defined(MACOS) || defined(TARGET_OS_MAC)\n'
NEW = (b'/* HiCT backport: TARGET_OS_MAC is not classic Mac OS (zlib 1.3.1). */\n'
       b'#if defined(MACOS)\n')
UPSTREAM = 'https://github.com/madler/zlib/blob/v1.3.1/zutil.h'


def patch_source(source):
    header = source / HEADER
    original = header.read_bytes()
    if original.count(OLD) == 1 and NEW not in original:
        patched = original.replace(OLD, NEW, 1)
        header.write_bytes(patched)
    elif original.count(NEW) == 1 and OLD not in original:
        patched = original  # Allow rebuilding the same extracted source.
    else:
        raise ValueError(f'Unexpected BLAST zlib platform guard in {header}; review the pinned source')
    return dict(patch='zlib-classic-macos-guard', upstream=UPSTREAM,
                file=HEADER.as_posix(), beforeSha256=hashlib.sha256(original).hexdigest(),
                afterSha256=hashlib.sha256(patched).hexdigest())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('source', type=Path, help='Extracted BLAST c++ directory')
    parser.add_argument('--report', type=Path, required=True)
    args = parser.parse_args()
    report = patch_source(args.source)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
    print('Applied BLAST zlib classic-Mac-OS guard backport: ' + report['afterSha256'])


if __name__ == '__main__':
    main()
