import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'scripts/toolchains'))
import patch_blast_source as patches


class BlastSourcePatchesTest(unittest.TestCase):
    def test_patch_preserves_other_bytes_and_is_repeatable(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory)
            header = source / patches.HEADER
            header.parent.mkdir(parents=True)
            original = b'/* upstream notice */\n' + patches.OLD + b'#endif\n'
            header.write_bytes(original)
            report = patches.patch_source(source)
            self.assertEqual(header.read_bytes(), original.replace(patches.OLD, patches.NEW))
            self.assertNotEqual(report['beforeSha256'], report['afterSha256'])
            repeated = patches.patch_source(source)
            self.assertEqual(repeated['beforeSha256'], report['afterSha256'])
            self.assertEqual(repeated['afterSha256'], report['afterSha256'])

    def test_unexpected_or_ambiguous_header_is_rejected_without_changes(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory)
            header = source / patches.HEADER
            header.parent.mkdir(parents=True)
            for data in (b'changed upstream\n', patches.OLD * 2, patches.OLD + patches.NEW):
                header.write_bytes(data)
                with self.assertRaisesRegex(ValueError, 'Unexpected BLAST zlib'):
                    patches.patch_source(source)
                self.assertEqual(header.read_bytes(), data)

    @unittest.skipUnless(os.name == 'posix' and shutil.which('cc'), 'Requires a POSIX C compiler')
    def test_modern_apple_macro_keeps_posix_fdopen_and_gzip_os_code(self):
        # Minimal reproduction of the bundled zlib guard and include order.
        # Native packaging additionally compiles the real zutil.c with its SDK.
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory)
            header = source / patches.HEADER
            header.parent.mkdir(parents=True)
            header.write_bytes(patches.OLD + b'''#define OS_CODE 7
#define fdopen(fd,mode) NULL
#endif
#ifdef __APPLE__
#define OS_CODE 19
#endif
''')
            probe = source / 'probe.c'
            probe.write_text('''#include "zutil.h"
#include <stdio.h>
#ifdef fdopen
#error POSIX fdopen was replaced
#endif
#if OS_CODE != 19
#error Wrong gzip OS code for modern macOS
#endif
int main(void) {
    FILE *(*open_file)(int, const char *) = fdopen;
    return open_file == NULL;
}
''')
            command = ['cc', '-std=gnu17', '-D__APPLE__=1', '-I' + str(header.parent),
                       '-fsyntax-only', str(probe)]
            before = subprocess.run(command + ['-DTARGET_OS_MAC=1'], capture_output=True, text=True)
            self.assertNotEqual(before.returncode, 0)
            self.assertIn('fdopen', before.stderr)
            patches.patch_source(source)
            for value in ('0', '1'):
                after = subprocess.run(command + ['-DTARGET_OS_MAC=' + value],
                                       capture_output=True, text=True)
                self.assertEqual(after.returncode, 0, after.stderr)


if __name__ == '__main__':
    unittest.main()
