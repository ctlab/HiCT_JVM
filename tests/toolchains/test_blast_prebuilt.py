import hashlib
import io
from pathlib import Path
import struct
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'scripts/toolchains'))
import blast_prebuilt as blast


class BlastPrebuiltTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def archive(self, windows=False, link=False, duplicate=False):
        path = self.root / 'source.tar.gz'
        command = 'bin/blastn.exe' if windows else 'bin/blastn'
        names = [command, 'LICENSE', 'BLAST_PRIVACY', 'README', 'ncbi_package_info', '../escape', 'bin/blastp']
        if windows:
            names += ['bin/nghttp2.dll', 'bin/blastn.exe.manifest']
        if duplicate:
            names.append(command)
        with tarfile.open(path, 'w:gz') as archive:
            for name in names:
                member = tarfile.TarInfo('ncbi-blast-2.16.0+/' + name)
                if link and name == command:
                    member.type = tarfile.SYMTYPE; member.linkname = '../outside'; archive.addfile(member)
                else:
                    value = name.encode(); member.size = len(value); archive.addfile(member, io.BytesIO(value))
        return path, blast.digest(path)

    def test_checksum_before_any_extraction(self):
        archive, _ = self.archive()
        with self.assertRaisesRegex(RuntimeError, 'checksum'):
            blast.unpack(archive, self.root / 'out', 'darwin_arm64', '0' * 64)
        self.assertFalse((self.root / 'out').exists())

    def test_extract_only_required_members_and_notices(self):
        archive, sha = self.archive(windows=True)
        output = self.root / 'out'
        self.assertEqual(blast.unpack(archive, output, 'windows_x86_64', sha), 'bin/blast/blastn.exe')
        files = {p.relative_to(output).as_posix() for p in output.rglob('*') if p.is_file()}
        self.assertEqual(files, {'bin/blast/blastn.exe', 'bin/blast/nghttp2.dll', 'bin/blast/blastn.exe.manifest'} |
                         {'share/licenses/sequence-analysis/blast/upstream/' + n for n in ('LICENSE', 'BLAST_PRIVACY', 'README', 'ncbi_package_info')})
        self.assertFalse((self.root / 'escape').exists())

    def test_symlink_command_rejected(self):
        archive, sha = self.archive(link=True)
        with self.assertRaisesRegex(RuntimeError, 'regular file'):
            blast.unpack(archive, self.root / 'out', 'darwin_x86_64', sha)

    def test_duplicate_command_rejected(self):
        archive, sha = self.archive(duplicate=True)
        with self.assertRaisesRegex(RuntimeError, 'regular file'):
            blast.unpack(archive, self.root / 'out', 'darwin_arm64', sha)

    def test_native_macos_architecture_selection(self):
        with patch.object(blast.platform, 'system', return_value='Darwin'):
            for machine, expected in [('arm64', 'darwin_arm64'), ('x86_64', 'darwin_x86_64')]:
                with patch.object(blast.platform, 'machine', return_value=machine):
                    self.assertEqual(blast.host_platform(), expected)
                    self.assertIn(expected, blast.ARCHIVES)

    def make_crt(self, machine=0x8664):
        source = self.root / 'VC/Redist/MSVC/14.44/x64/Microsoft.VC143.CRT'
        source.mkdir(parents=True)
        data = b'MZ' + b'\0' * 58 + struct.pack('<I', 64) + b'PE\0\0' + struct.pack('<H', machine)
        for file in blast.CRT_REQUIRED:
            (source / file).write_bytes(data)
        return source

    def test_release_crt_copied_next_to_executable_with_hashes(self):
        source = self.make_crt()
        with patch.dict(blast.os.environ, {'VCToolsRedistDir': str(source.parents[1]), 'ProgramFiles(x86)': str(self.root / 'absent')}):
            self.assertEqual(blast.find_crt(), source)
        output = self.root / 'bin/blast'
        receipt = blast.copy_crt(source, output)
        self.assertEqual({v['name'] for v in receipt}, blast.CRT_REQUIRED)
        for row in receipt:
            self.assertEqual(row['sha256'], blast.digest(output / row['name']))

    def test_wrong_architecture_crt_rejected_before_copy(self):
        source = self.make_crt(0x14c)
        with self.assertRaisesRegex(RuntimeError, 'AMD64'):
            blast.copy_crt(source, self.root / 'out')
        self.assertFalse((self.root / 'out').exists())

    def test_missing_crt_does_not_rely_on_system_install(self):
        source = self.root / 'missing'; source.mkdir()
        with self.assertRaisesRegex(RuntimeError, 'Incomplete'):
            blast.copy_crt(source, self.root / 'out')


if __name__ == '__main__':
    unittest.main()
