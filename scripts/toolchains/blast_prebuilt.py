"""Pinned NCBI BLAST distributions for platforms without the Linux ABI constraint."""
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import struct
import subprocess
import tarfile
import urllib.request

VERSION = '2.16.0'
BASE = 'https://ftp.ncbi.nlm.nih.gov/blast/executables/blast+/2.16.0/'
ARCHIVES = {
    'windows_x86_64': ('x64-win64', '3b6f27a72e9a875f1c02119704775a8e04ae97b5e4a148b560f9be9acd39e943'),
    'darwin_x86_64': ('x64-macosx', '7eee1e7720f5daaf06e39d9c92b125d9072de7eb81f499c005dedb08b932324c'),
    'darwin_arm64': ('aarch64-macosx', 'e8da4f34b0421da051b1c2d9e5f8e1e43bf76e38cf9361a1dbe2fbc44b6224db'),
}
CRT_REQUIRED = {'msvcp140.dll', 'vcruntime140.dll', 'vcruntime140_1.dll'}


def digest(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def host_platform():
    arch = {'amd64': 'x86_64', 'aarch64': 'arm64'}.get(platform.machine().lower(), platform.machine().lower())
    system = {'Windows': 'windows', 'Darwin': 'darwin'}.get(platform.system(), platform.system().lower())
    return f'{system}_{arch}'


def copy_member(archive, name, destination):
    """Extract only an exact pinned member: no paths, links or extra executables."""
    members = [m for m in archive.getmembers() if m.name == name]
    if len(members) != 1 or not members[0].isfile():
        raise RuntimeError('BLAST archive needs one regular file: ' + name)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with archive.extractfile(members[0]) as source, destination.open('xb') as target:
        shutil.copyfileobj(source, target)
    destination.chmod(0o755 if '/bin/' in name else 0o644)


def unpack(archive_path, payload, name, expected_sha):
    if digest(archive_path) != expected_sha:
        raise RuntimeError('BLAST archive checksum differs from pinned release')
    prefix = f'ncbi-blast-{VERSION}+/'
    command = 'bin/blast/blastn.exe' if name == 'windows_x86_64' else 'bin/blastn'
    notices = payload / 'share/licenses/sequence-analysis/blast/upstream'
    with tarfile.open(archive_path) as archive:
        copy_member(archive, prefix + ('bin/blastn.exe' if name == 'windows_x86_64' else 'bin/blastn'), payload / command)
        for doc in ('LICENSE', 'BLAST_PRIVACY', 'README', 'ncbi_package_info'):
            copy_member(archive, prefix + doc, notices / doc)
        if name == 'windows_x86_64':
            for file in ('nghttp2.dll', 'blastn.exe.manifest'):
                copy_member(archive, prefix + 'bin/' + file, payload / 'bin/blast' / file)
    return command


def check_amd64(path):
    with path.open('rb') as source:
        if source.read(2) != b'MZ':
            raise RuntimeError('Not a PE runtime: ' + str(path))
        source.seek(60)
        offset = struct.unpack('<I', source.read(4))[0]
        source.seek(offset)
        if source.read(6) != b'PE\0\0\x64\x86':
            raise RuntimeError('BLAST requires an AMD64 runtime: ' + str(path))


def find_crt():
    """Use release DLLs from the licensed VS installation, never System32."""
    roots = []
    if os.environ.get('VCToolsRedistDir'):
        roots.append(Path(os.environ['VCToolsRedistDir']))
    vswhere = Path(os.environ.get('ProgramFiles(x86)', 'C:/Program Files (x86)')) / 'Microsoft Visual Studio/Installer/vswhere.exe'
    if vswhere.is_file():
        installed = subprocess.check_output([str(vswhere), '-latest', '-products', '*', '-requires',
            'Microsoft.VisualStudio.Component.VC.Tools.x86.x64', '-property', 'installationPath'], text=True).strip()
        if installed:
            roots.append(Path(installed) / 'VC/Redist/MSVC')
    candidates = []
    for root in roots:
        candidates.extend(root.glob('x64/Microsoft.VC*.CRT'))
        candidates.extend(root.glob('*/x64/Microsoft.VC*.CRT'))
    for candidate in sorted(set(candidates), reverse=True):
        if CRT_REQUIRED <= {f.name.lower() for f in candidate.glob('*.dll')}:
            return candidate
    raise RuntimeError('Windows BLAST packaging requires the Visual Studio x64 C++ release redistributable. Install the C++ build tools or set VCToolsRedistDir to their licensed Redist/MSVC/<version> directory.')


def copy_crt(source, target):
    files = sorted(source.glob('*.dll'))
    if not CRT_REQUIRED <= {f.name.lower() for f in files}:
        raise RuntimeError('Incomplete Visual C++ release redistributable')
    for file in files:
        check_amd64(file)
    target.mkdir(parents=True, exist_ok=True)
    receipt = []
    for file in files:
        destination = target / file.name
        with file.open('rb') as src, destination.open('xb') as dst:
            shutil.copyfileobj(src, dst)
        receipt.append(dict(name=file.name, sha256=digest(destination), source=str(file)))
    return receipt


def collect_source_notices(work, notices):
    # Binary archives only carry NCBI's top-level notice. Retain third-party
    # toolkit notices from the matching, independently pinned source archive.
    from add_blast import SOURCE_URL, SOURCE_SHA256
    archive = work / 'blast-source-notices.tar.gz'
    with urllib.request.urlopen(SOURCE_URL, timeout=60) as response, archive.open('xb') as output:
        shutil.copyfileobj(response, output)
    if digest(archive) != SOURCE_SHA256:
        raise RuntimeError('BLAST source-notice checksum differs from pinned release')
    prefix = f'ncbi-blast-{VERSION}+-src/c++/'
    embedded = ('src/connect/mbedtls/', 'src/util/regexp/', 'src/util/compress/',
                'include/util/bitset/', 'include/misc/jsonwrapp/')
    with tarfile.open(archive) as source, (notices / 'embedded-notices.txt').open('x', encoding='utf-8') as headers:
        for member in source:
            if not member.isfile() or not member.name.startswith(prefix):
                continue
            relative = member.name[len(prefix):]
            if '..' in relative.split('/') or relative.startswith('/'):
                raise RuntimeError('Unsafe BLAST notice path')
            if any(word in Path(relative).name.lower() for word in ('license', 'licence', 'copying', 'copyright', 'notice', 'authors')):
                destination = notices / 'toolkit' / relative
                destination.parent.mkdir(parents=True, exist_ok=True)
                with source.extractfile(member) as src, destination.open('xb') as dst:
                    shutil.copyfileobj(src, dst)
            elif relative.startswith(embedded):
                with source.extractfile(member) as src:
                    lines = src.read(65536).decode('utf-8', errors='replace').splitlines()[:160]
                text = '\n'.join(lines)
                if any(word in text.lower() for word in ('redistribution', 'permission', 'licence', 'license')):
                    headers.write('\nSOURCE: ' + relative + '\n' + text + '\n')
    return dict(url=SOURCE_URL, sha256=SOURCE_SHA256)


def add_prebuilt(payload, work, root, smoke):
    name = host_platform()
    if name not in ARCHIVES:
        raise RuntimeError('No supported BLAST payload for ' + name)
    suffix, sha = ARCHIVES[name]
    url = BASE + f'ncbi-blast-{VERSION}+-{suffix}.tar.gz'
    archive = work / ('blast-' + suffix + '.tar.gz')
    with urllib.request.urlopen(url, timeout=60) as response, archive.open('xb') as output:
        shutil.copyfileobj(response, output)
    command = unpack(archive, payload, name, sha)
    notices = payload / 'share/licenses/sequence-analysis/blast'
    shutil.copytree(root / 'third_party/blast', notices, dirs_exist_ok=True)
    source_notices = collect_source_notices(work, notices)
    runtime = []
    if name == 'windows_x86_64':
        runtime = copy_crt(find_crt(), (payload / command).parent)
        check_amd64(payload / command)
        check_amd64((payload / command).parent / 'nghttp2.dll')
    result = smoke(payload / command, work / 'blast-smoke')
    result.update(version=VERSION, platform=name, archive=url, archiveSha256=sha,
                  packaging='Official NCBI native distribution', visualCppRuntime=runtime, sourceNotices=source_notices)
    docs = payload / 'share/doc/sequence-analysis'
    docs.mkdir(parents=True, exist_ok=True)
    (docs / 'blast-smoke.json').write_text(json.dumps(result, indent=2), encoding='utf-8')
    return command
