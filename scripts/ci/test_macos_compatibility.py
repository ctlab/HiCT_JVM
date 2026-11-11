"""Portable audit regressions; no Apple tools required for parser tests."""
import unittest
from pathlib import Path
from unittest.mock import patch
from check_macos_compatibility import Auditor


def commands(install_name, dependency='/usr/lib/libSystem.B.dylib', minimum='12.3'):
    return f'''Load command 0
          cmd LC_BUILD_VERSION
     platform MACOS
        minos {minimum}
Load command 1
          cmd LC_ID_DYLIB
         name {install_name} (offset 24)
Load command 2
          cmd LC_LOAD_DYLIB
         name {dependency} (offset 24)
'''


class MacosCompatibilityTests(unittest.TestCase):
    def audit(self, install_name, **kwargs):
        auditor = Auditor('12.3', 'arm64')
        with patch('subprocess.check_output', side_effect=['arm64\n', commands(install_name, **kwargs)]):
            auditor.inspect_binary(Path('library'), 'hict.jar!native/libhict_native.dylib')
        return auditor.errors

    def test_relocatable_library(self):
        self.assertEqual(self.audit('@rpath/libhict_native.dylib'), [])

    def test_absolute_build_id_is_not_a_dependency(self):
        errors = self.audit('/Users/runner/work/HiCT/build/libhict_native.dylib')
        self.assertEqual(len(errors), 1)
        self.assertIn('absolute install name', errors[0])

    def test_absolute_dependency_with_spaces_is_rejected(self):
        errors = self.audit('@rpath/libhict_native.dylib', dependency='/Users/runner/build files/libbad.dylib')
        self.assertEqual(len(errors), 1)
        self.assertIn('absolute dependency', errors[0])

    def test_newer_os_is_rejected(self):
        self.assertIn('requires macOS 15.0', self.audit('@rpath/libhict_native.dylib', minimum='15.0')[0])


if __name__ == '__main__':
    unittest.main()
