import hashlib
import importlib.util
import json
from pathlib import Path
import pytest

spec=importlib.util.spec_from_file_location('merge_sequence',Path(__file__).resolve().parents[2]/'scripts/toolchains/merge_sequence_payload.py')
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)


def payload(root):
    (root/'bin').mkdir(parents=True)
    (root/'bin/seqkit').write_bytes(b'fixture tool')
    manifest=dict(commands={'seqkit':'bin/seqkit'},files=['bin/seqkit'],sha256={'bin/seqkit':hashlib.sha256(b'fixture tool').hexdigest()},notices=['new notice'])
    (root/'manifest.json').write_text(json.dumps(manifest))
    return manifest


def test_merge_checks_all_files_before_changing_existing_payload_and_preserves_tools(tmp_path):
    source,target=tmp_path/'source',tmp_path/'target'
    manifest=payload(source);target.mkdir()
    original=dict(commands={'hictk':'bin/hictk'},files=['bin/hictk'],notices=['original'])
    (target/'manifest.json').write_text(json.dumps(original))
    (source/'bin/seqkit').write_bytes(b'tampered')
    with pytest.raises(ValueError,match='checksum'):module.merge(source,target)
    assert json.loads((target/'manifest.json').read_text())==original
    assert not (target/'sequence').exists()
    (source/'bin/seqkit').write_bytes(b'fixture tool')
    module.merge(source,target)
    merged=json.loads((target/'manifest.json').read_text())
    assert merged['commands']['hictk']=='bin/hictk'
    assert (target/merged['commands']['seqkit']).read_bytes()==b'fixture tool'
    assert merged['notices']==['original','new notice']
    module.merge(source,target)
    assert json.loads((target/'manifest.json').read_text())==merged


def test_merge_rejects_path_escape_and_missing_command(tmp_path):
    source,target=tmp_path/'source',tmp_path/'target'
    manifest=payload(source)
    manifest['files']=['../outside']
    (source/'manifest.json').write_text(json.dumps(manifest))
    with pytest.raises(ValueError,match='Unsafe'):module.merge(source,target)
    assert not target.exists()
    manifest['files']=['bin/seqkit'];manifest['commands']['seqkit']='bin/absent'
    (source/'manifest.json').write_text(json.dumps(manifest))
    with pytest.raises(ValueError,match='Command missing'):module.merge(source,target)
    assert not target.exists()
