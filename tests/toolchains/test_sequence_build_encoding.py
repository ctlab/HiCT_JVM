"""Go's UTF-8 package metadata must survive Windows cp1252 hosts."""
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location(
    'build_sequence_tools', Path(__file__).resolve().parents[2] / 'scripts/toolchains/build_sequence_tools.py')
build = importlib.util.module_from_spec(spec)
spec.loader.exec_module(build)


class BuildEncodingTest(unittest.TestCase):
    def test_utf8_json_is_decoded_when_locale_defaults_to_cp1252(self):
        value = {'Doc': '\u201csequence\u201d \u03b1'}
        payload = json.dumps(value, ensure_ascii=False).encode('utf-8')
        with patch.object(subprocess, '_text_encoding', return_value='cp1252'):
            result = build.run([sys.executable, '-c',
                'import sys;sys.stdout.buffer.write(' + repr(payload) + ')'],
                capture_output=True, text=True)
        self.assertEqual(list(build.json_sequence(result.stdout)), [value])


if __name__ == '__main__':
    unittest.main()
