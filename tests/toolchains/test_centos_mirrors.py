import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location('mirrors', Path(__file__).resolve().parents[2] / 'scripts/ci/configure_centos_mirrors.py')
mirrors = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mirrors)


class CentosMirrorsTest(unittest.TestCase):
    def test_preserves_verification_and_unrelated_repositories(self):
        original = ('[base]\nbaseurl=https://vault.centos.org/centos/$releasever/os/$basearch/\n'
                    'gpgcheck=1\ngpgkey=file:///etc/pki/rpm-gpg/RPM-GPG-KEY-CentOS-7\n'
                    '[sclo]\nbaseurl=https://vault.centos.org/centos/7/sclo/$basearch/rh/\n'
                    '[custom]\nbaseurl=https://example.org/packages/\ngpgcheck=1\n')
        result = mirrors.add_fallback(original)
        self.assertIn('https://archive.kernel.org/centos-vault/7.9.2009/os/$basearch/', result)
        self.assertIn('https://archive.kernel.org/centos-vault/7.9.2009/sclo/$basearch/rh/', result)
        self.assertEqual(result.count('gpgcheck=1'), 2)
        self.assertIn('gpgkey=file:///etc/pki/rpm-gpg/RPM-GPG-KEY-CentOS-7', result)
        self.assertIn('[custom]\nbaseurl=https://example.org/packages/\ngpgcheck=1\n', result)
        self.assertEqual(mirrors.add_fallback(result), result)


if __name__ == '__main__':
    unittest.main()
