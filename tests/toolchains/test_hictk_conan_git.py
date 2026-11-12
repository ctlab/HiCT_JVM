"""Exercise Git URL resolution without requiring Savannah or the mirror online."""
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
WRAPPER = ROOT / 'scripts/toolchains/conan-git/git'
MIRROR = 'https://github.com/chipp/gnu-config.git'


@unittest.skipIf(os.name == 'nt', 'POSIX hictk builders use this wrapper')
class HictkConanGitTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='hict conan git ')
        self.addCleanup(self.tmp.cleanup)
        self.home = Path(self.tmp.name)
        self.git = shutil.which('git')
        self.env = dict(os.environ, HOME=str(self.home),
                        GIT_CONFIG_NOSYSTEM='1', HICTK_CONAN_REAL_GIT=self.git)
        # Never read/write the developer's configuration, even in CI overrides.
        self.env['GIT_CONFIG_GLOBAL'] = str(self.home / '.gitconfig')
        self.original_config = '[user]\n\tname = Original User\n'
        (self.home / '.gitconfig').write_text(self.original_config)

    def run_git(self, *args, wrapped=True, **kwargs):
        return subprocess.run([str(WRAPPER) if wrapped else self.git, *args],
                              cwd=self.home, env=self.env, text=True,
                              capture_output=True, **kwargs)

    def test_only_gnu_config_transport_is_rewritten(self):
        for origin in ('https://https.git.savannah.gnu.org/git/config.git',
                       'https://git.savannah.gnu.org/git/config.git'):
            for suffix in ('', '/'):
                with self.subTest(origin=origin, suffix=suffix):
                    result = self.run_git('ls-remote', '--get-url', origin + suffix, check=True)
                    self.assertEqual(result.stdout.strip(), MIRROR + suffix)
        for origin in ('https://github.com/paulsengroup/hictk.git',
                       'https://https.git.savannah.gnu.org/git/other.git'):
            self.assertEqual(self.run_git('ls-remote', '--get-url', origin,
                                         check=True).stdout.strip(), origin)
        self.assertEqual((self.home / '.gitconfig').read_text(), self.original_config)
        origin = 'https://https.git.savannah.gnu.org/git/config.git'
        self.assertEqual(self.run_git('ls-remote', '--get-url', origin,
                                     wrapped=False, check=True).stdout.strip(), origin)

    def test_preserves_git_arguments_exit_status_and_existing_config(self):
        self.assertEqual(self.run_git('config', 'user.name', check=True).stdout.strip(),
                         'Original User')
        self.run_git('init', check=True)
        # An unavailable pinned object must fail rather than switch to HEAD.
        result = self.run_git('checkout', '--detach', '191bcb948f7191c36eefe634336f5fc5c0c4c2be')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('191bcb948f7191c36eefe634336f5fc5c0c4c2be', result.stderr)

    def test_missing_original_git_fails_clearly(self):
        del self.env['HICTK_CONAN_REAL_GIT']
        result = self.run_git('--version')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('HICTK_CONAN_REAL_GIT', result.stderr)


if __name__ == '__main__':
    unittest.main()
