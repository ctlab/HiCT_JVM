"""Apply checked compatibility patches to the pinned BLAST macOS source build.

TARGET_OS_MAC also denotes modern macOS; it must not enable the classic Mac
OS fdopen replacement. Upstream zlib 1.3.1 uses only MACOS for that branch.
BitMagic's bulk-insert assignments still reference removed bvector methods;
use the block allocator API already used by its constructors and destructor.
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
BITMAGIC_HEADER = Path('include/util/bitset/bm.h')
BITMAGIC_REPLACEMENTS = (
    (b'buf_ = bvect_->allocate_tempblock();',
     b'buf_ = (value_type*) bvect_->blockman_.get_allocator().alloc_bit_block();'
     b' // HiCT: match the constructor allocator.'),
    (b'bvect_->free_tempblock(buf_);',
     b'bvect_->blockman_.get_allocator().free_bit_block((bm::word_t*)buf_);'
     b' // HiCT: match the destructor allocator.'),
)
PATCHES = (
    ('zlib-classic-macos-guard', HEADER, UPSTREAM, ((OLD, NEW),)),
    ('bitmagic-bulk-insert-allocator', BITMAGIC_HEADER,
     'https://github.com/ncbi/ncbi-cxx-toolkit-public/issues/20', BITMAGIC_REPLACEMENTS),
)


def patch_source(source):
    pending = []
    reports = []
    # Validate every edit before writing any file. Refuse changed/partial sources.
    for name, relative, upstream, replacements in PATCHES:
        header = source / relative
        original = header.read_bytes()
        pristine = all(original.count(old) == 1 and new not in original
                       for old, new in replacements)
        applied = all(original.count(new) == 1 and old not in original
                      for old, new in replacements)
        if not pristine and not applied:
            raise ValueError(f'Unexpected BLAST {name} in {header}; review the pinned source')
        patched = original
        if pristine:
            for old, new in replacements:
                patched = patched.replace(old, new, 1)
            pending.append((header, patched))
        reports.append(dict(patch=name, upstream=upstream, file=relative.as_posix(),
                            beforeSha256=hashlib.sha256(original).hexdigest(),
                            afterSha256=hashlib.sha256(patched).hexdigest()))
    for header, patched in pending:
        header.write_bytes(patched)
    return dict(patches=reports)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('source', type=Path, help='Extracted BLAST c++ directory')
    parser.add_argument('--report', type=Path, required=True)
    args = parser.parse_args()
    report = patch_source(args.source)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
    for entry in report['patches']:
        print('Applied BLAST ' + entry['patch'] + ': ' + entry['afterSha256'])


if __name__ == '__main__':
    main()
