import importlib.util
import os
from pathlib import Path
import subprocess
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('cache_sources', Path(__file__).resolve().parents[2] / 'scripts/ci/toolchain_cache_sources.py')
sources = importlib.util.module_from_spec(spec)
spec.loader.exec_module(sources)


class SourceTests(unittest.TestCase):
    def test_annotated_tag_fingerprints_both_tag_and_commit(self):
        answer = 'a' * 40 + '\trefs/tags/v1\n' + 'b' * 40 + '\trefs/tags/v1^{}\n'
        with patch.dict(os.environ, {'HICTK_REF': 'v1'}), patch.object(sources.subprocess, 'run', return_value=subprocess.CompletedProcess([], 0, answer)) as run:
            name, record = sources.resolve(('HICTK', ('owner/repo', 'v0')))
        self.assertEqual(name, 'HICTK')
        self.assertEqual(record['revisions'], sorted(answer.splitlines()))
        self.assertIn('refs/tags/v1^{}', run.call_args.args[0])
        self.assertTrue(run.call_args.kwargs['check'])

    def test_commit_pin_does_not_need_a_remote_ref(self):
        with patch.dict(os.environ, {'MINIMAP2_REF': 'a' * 40}), patch.object(sources.subprocess, 'run') as run:
            _, record = sources.resolve(('MINIMAP2', ('owner/repo', 'v0')))
        run.assert_not_called()
        self.assertEqual(record['revisions'], ['a' * 40])

    def test_missing_ref_fails_closed(self):
        with patch.object(sources.subprocess, 'run', side_effect=subprocess.CalledProcessError(2, ['git'])):
            with self.assertRaises(subprocess.CalledProcessError):
                sources.resolve(('HICTK', ('owner/repo', 'v0')))
