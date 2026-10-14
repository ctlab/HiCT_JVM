"""Exercise PyInstaller after merge/extraction, under a realistic Windows temp path."""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'toolchains'))
from merge_sequence_payload import merge

p = argparse.ArgumentParser()
p.add_argument('--source', required=True, type=Path)
p.add_argument('--output', required=True, type=Path)
a = p.parse_args()
assert sys.platform == 'win32'
root = Path(tempfile.mkdtemp(prefix='hict-packaged-tool-check-'))
merged = root / 'merged'
merged.mkdir()
label = 'hict-toolchain-windows-x86_64-hictk-minimap2-mm2plus-v1.2'
(merged/'manifest.json').write_text(json.dumps(dict(id=label, commands={}, files=[])))
merge(a.source, merged)
manifest = json.loads((merged/'manifest.json').read_text())
digest = hashlib.sha256(json.dumps(manifest,separators=(',',':')).encode()).hexdigest()[:20]
parent = root/'hict-toolchains/windows_x86_64'
parent.mkdir(parents=True)
long = parent/(label+'-'+digest)
shutil.move(str(merged), long)
def probe(folder):
    command = folder/manifest['commands']['hict-moddotplot']
    result = subprocess.run([str(command),'--help'],capture_output=True,text=True,errors='replace',timeout=60)
    return dict(command=str(command),path_length=len(str(command)),exit_code=result.returncode,
                stdout=result.stdout,stderr=result.stderr)
legacy = probe(long)
short = parent/(manifest['id']+'-'+digest)
long.rename(short)
current = probe(short)
record = dict(legacy=legacy,current=current,passed=current['exit_code']==0,
              note='Legacy probe is diagnostic; current deployment layout must execute.')
a.output.parent.mkdir(parents=True,exist_ok=True)
a.output.write_text(json.dumps(record,indent=2))
print(json.dumps(record,indent=2),flush=True)
assert record['passed'],current
