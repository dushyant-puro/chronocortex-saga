"""Synthetic reviewed wheels, real hashes/ZIP/pip, no provider or index access."""
import base64
import csv
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import zipfile

from packaging.markers import default_environment
import pytest

from scripts import dependency_audit as audit
from scripts import wheelhouse as wheels
from tests.test_dependency_audit import fixture


def wheel(path,name,version,*,change=None):
    dist=name.replace('-','_')
    parent=f'{dist}-{version}.dist-info'
    data={f'{dist}/__init__.py':b'VALUE = 1\n',
          parent+'/METADATA':f'Metadata-Version: 2.1\nName: {name}\nVersion: {version}\n\nSIMULATED fixture\n'.encode(),
          parent+'/WHEEL':b'Wheel-Version: 1.0\nRoot-Is-Purelib: true\nTag: py3-none-any\n'}
    if change: change(data,parent)
    record=io.StringIO(); writer=csv.writer(record,lineterminator='\n')
    for member,raw in data.items():
        digest=base64.urlsafe_b64encode(hashlib.sha256(raw).digest()).rstrip(b'=').decode()
        writer.writerow((member,'sha256='+digest,len(raw)))
    writer.writerow((parent+'/RECORD','',''))
    data[parent+'/RECORD']=record.getvalue().encode()
    with zipfile.ZipFile(path,'w',compression=zipfile.ZIP_DEFLATED) as archive:
        for member,raw in data.items(): archive.writestr(member,raw)


def update(root,hashes):
    for lock in audit.LOCKS.values():
        lines=[]
        for requirement in audit.parse_lock((root/lock).read_text()):
            name=audit.canonicalize_name(requirement.name)
            lines.append(str(requirement)+' --hash=sha256:'+hashes[name])
        (root/lock).write_text('\n'.join(sorted(audit.OPTIONS))+'\n'+'\n'.join(lines)+'\n')
    path=root/audit.MANIFEST
    manifest=json.loads(path.read_text())
    manifest['locks']={name:audit.digest(root/name) for name in audit.LOCKS.values()}
    path.write_text(json.dumps(manifest))


def setup(tmp_path,profile='runtime',change=None):
    root=tmp_path/'repo'; root.mkdir(); fixture(root)
    all_wheels=tmp_path/'all'; all_wheels.mkdir()
    hashes={}
    for name,version in [('sample','1.0'),('windows-only','3.0'),('viewer','2.0'),('auditor','4.0')]:
        path=all_wheels/f"{name.replace('-','_')}-{version}-py3-none-any.whl"
        wheel(path,name,version,change=change if name=='sample' else None)
        hashes[name]=hashlib.sha256(path.read_bytes()).hexdigest()
    update(root,hashes)
    _,active=audit.load_locks(root,default_environment())
    source=tmp_path/'source'; source.mkdir()
    for path in all_wheels.iterdir():
        name,_,_,_=wheels.parse_wheel_filename(path.name)
        if name in active[profile]: shutil.copyfile(path,source/path.name)
    return root,source,tmp_path/'archive',hashes


@pytest.mark.parametrize('profile',tuple(audit.LOCKS))
def test_capture_and_read_only_verify_complete_profile(tmp_path,profile):
    root,source,target,_=setup(tmp_path,profile)
    (source/'PRIVATE-key.txt').write_text('NEVER COPY OR REPORT THIS')
    result=wheels.capture(source,target,profile,root=root)
    assert result['passed'] and not result['installed'] and not result['publisher_authenticated']
    assert result['wheel_count']==len(list(source.glob('*.whl')))
    before={p.name:p.read_bytes() for p in target.iterdir()}
    verified=wheels.verify(target,profile,root=root)
    assert verified['passed'] and before=={p.name:p.read_bytes() for p in target.iterdir()}
    assert 'PRIVATE' not in json.dumps(result) and not (target/'PRIVATE-key.txt').exists()


def test_lock_parser_preserves_per_requirement_archive_hash_binding(tmp_path):
    root,source,target,hashes=setup(tmp_path)
    hashes['sample']=hashes['auditor']
    update(root,hashes)
    assert not wheels.capture(source,target,'runtime',root=root)['passed'] and not target.exists()
    records=audit.parse_lock_records((root/'requirements.txt').read_text())
    assert str(records[0][0]).startswith('sample==1.0') and records[0][1]=={hashes['auditor']}
    assert [str(r) for r in audit.parse_lock((root/'requirements.txt').read_text())]==[str(r) for r,_ in records]


def test_unknown_hash_fails_before_zip_parsing(tmp_path,monkeypatch):
    root,source,target,_=setup(tmp_path)
    path=source/'sample-1.0-py3-none-any.whl'; path.write_bytes(path.read_bytes()+b'TAMPER')
    monkeypatch.setattr(wheels.zipfile,'ZipFile',lambda *a,**k:pytest.fail('Untrusted bytes reached ZIP parsing'))
    assert not wheels.capture(source,target,'runtime',root=root)['passed'] and not target.exists()


@pytest.mark.parametrize('mutation',['missing','extra','duplicate','wrong_tag','sdist'])
def test_source_must_have_exactly_one_compatible_locked_wheel_per_package(tmp_path,mutation):
    root,source,target,_=setup(tmp_path)
    sample=source/'sample-1.0-py3-none-any.whl'
    if mutation=='missing': sample.unlink()
    elif mutation=='extra': shutil.copyfile(sample,source/'unreviewed-1.0-py3-none-any.whl')
    elif mutation=='duplicate': shutil.copyfile(sample,source/'sample-1.0-1-py3-none-any.whl')
    elif mutation=='wrong_tag': sample.rename(source/'sample-1.0-cp999-cp999-win_amd64.whl')
    else: sample.rename(source/'sample-1.0.tar.gz')
    assert not wheels.capture(source,target,'runtime',root=root)['passed'] and not target.exists()


@pytest.mark.parametrize('mutation',['name','version','tag','future_wheel','traversal','metadata_capacity','duplicate_metadata','requires_python'])
def test_reviewed_hash_does_not_replace_metadata_and_layout_validation(tmp_path,mutation):
    def change(data,parent):
        if mutation=='name': data[parent+'/METADATA']=b'Name: foreign\nVersion: 1.0\n'
        elif mutation=='version': data[parent+'/METADATA']=b'Name: sample\nVersion: 2.0\n'
        elif mutation=='tag': data[parent+'/WHEEL']=b'Wheel-Version: 1.0\nTag: cp999-none-any\n'
        elif mutation=='future_wheel': data[parent+'/WHEEL']=b'Wheel-Version: 2.0\nTag: py3-none-any\n'
        elif mutation=='traversal': data['../escape']=b'NEVER EXTRACT'
        elif mutation=='metadata_capacity': data[parent+'/METADATA']=b'X'*(wheels.MAX_METADATA+1)
        elif mutation=='duplicate_metadata': data['other-1.0.dist-info/METADATA']=b'Name: other\nVersion: 1.0\n'
        else: data[parent+'/METADATA']=b'Name: sample\nVersion: 1.0\nRequires-Python: >=99\n'
    root,source,target,_=setup(tmp_path,change=change)
    assert not wheels.capture(source,target,'runtime',root=root)['passed'] and not target.exists()
    assert not (tmp_path/'escape').exists()


@pytest.mark.parametrize('mutation',['tampered','missing','extra','target','profile','lock','version','bool_schema','float_size','duplicate_key','nonfinite'])
def test_complete_archive_rejects_drift_and_ambiguous_manifest(tmp_path,mutation):
    root,source,target,_=setup(tmp_path)
    assert wheels.capture(source,target,'runtime',root=root)['passed']
    path=target/wheels.MANIFEST; data=json.loads(path.read_text())
    sample=target/'sample-1.0-py3-none-any.whl'
    if mutation=='tampered': sample.write_bytes(sample.read_bytes()+b'X')
    elif mutation=='missing': sample.unlink()
    elif mutation=='extra': (target/'unlisted.txt').write_text('PRIVATE')
    elif mutation=='target': data['target']['python']='3.14'
    elif mutation=='profile': data['profile']='security'
    elif mutation=='lock': data['lock_sha256']='0'*64
    elif mutation=='version': data['wheels'][0]['version']='9.9'
    elif mutation=='bool_schema': data['schema']=True
    elif mutation=='float_size': data['wheels'][0]['bytes']=float(data['wheels'][0]['bytes'])
    elif mutation=='duplicate_key': path.write_text(path.read_text().replace('"schema": 1','"schema": 1, "schema": 1'))
    else: path.write_text(path.read_text().replace('"schema": 1','"schema": NaN'))
    if mutation not in {'duplicate_key','nonfinite'}: path.write_text(json.dumps(data))
    before={p.name:p.read_bytes() for p in target.iterdir()}
    result=wheels.verify(target,'runtime',root=root)
    assert not result['passed'] and 'PRIVATE' not in json.dumps(result)
    assert before=={p.name:p.read_bytes() for p in target.iterdir()}


def test_existing_output_and_nested_source_destination_are_preserved(tmp_path):
    root,source,target,_=setup(tmp_path)
    target.mkdir(); prior=target/'evidence'; prior.write_bytes(b'PRIOR')
    assert not wheels.capture(source,target,'runtime',root=root)['passed'] and prior.read_bytes()==b'PRIOR'
    assert not wheels.capture(source,source/'nested','runtime',root=root)['passed'] and not (source/'nested').exists()


def test_hard_linked_source_is_rejected(tmp_path):
    root,source,target,_=setup(tmp_path)
    os.link(source/'sample-1.0-py3-none-any.whl',tmp_path/'alias.whl')
    assert not wheels.capture(source,target,'runtime',root=root)['passed'] and not target.exists()


def test_zip_symlink_is_rejected_even_when_its_hash_was_reviewed(tmp_path):
    root,source,target,hashes=setup(tmp_path)
    sample=source/'sample-1.0-py3-none-any.whl'
    with zipfile.ZipFile(sample,'a') as archive:
        entry=zipfile.ZipInfo('link'); entry.create_system=3; entry.external_attr=0o120777<<16
        archive.writestr(entry,b'../outside')
    hashes['sample']=hashlib.sha256(sample.read_bytes()).hexdigest(); update(root,hashes)
    assert not wheels.capture(source,target,'runtime',root=root)['passed'] and not target.exists()


@pytest.mark.parametrize('field,value',[('python_version','3.14'),('implementation_name','pypy'),('sys_platform','darwin')])
def test_unsupported_host_cannot_claim_archive_compatibility(tmp_path,monkeypatch,field,value):
    root,source,target,_=setup(tmp_path)
    env=default_environment(); env[field]=value
    monkeypatch.setattr(wheels,'default_environment',lambda:env)
    assert not wheels.capture(source,target,'runtime',root=root)['passed'] and not target.exists()


@pytest.mark.parametrize('limit',['wheel','total','manifest'])
def test_resource_bounds_fail_closed(tmp_path,monkeypatch,limit):
    root,source,target,_=setup(tmp_path)
    if limit=='manifest':
        assert wheels.capture(source,target,'runtime',root=root)['passed']
        monkeypatch.setattr(wheels,'MAX_MANIFEST',8)
        assert not wheels.verify(target,'runtime',root=root)['passed']
    else:
        monkeypatch.setattr(wheels,'MAX_WHEEL' if limit=='wheel' else 'MAX_TOTAL',8)
        assert not wheels.capture(source,target,'runtime',root=root)['passed'] and not target.exists()


def test_flush_failure_retains_unverified_partial_without_completion_manifest(tmp_path,monkeypatch):
    root,source,target,_=setup(tmp_path)
    def fail(*a,**k): raise OSError('PRIVATE filesystem failure')
    monkeypatch.setattr(wheels.os,'fsync',fail)
    report=wheels.capture(source,target,'runtime',root=root)
    assert not report['passed'] and target.exists() and not (target/wheels.MANIFEST).exists()
    assert not wheels.verify(target,'runtime',root=root)['passed'] and 'PRIVATE' not in json.dumps(report)


def test_manifest_flush_failure_cannot_return_a_passing_capture(tmp_path,monkeypatch):
    root,source,target,_=setup(tmp_path)
    original=wheels.os.fsync; calls=[]
    count=len(list(source.glob('*.whl')))
    def fail(descriptor):
        calls.append(descriptor)
        if len(calls)==count+1: raise OSError('PRIVATE manifest flush failure')
        return original(descriptor)
    monkeypatch.setattr(wheels.os,'fsync',fail)
    result=wheels.capture(source,target,'runtime',root=root)
    assert not result['passed'] and target.exists() and (target/wheels.MANIFEST).exists()
    assert 'PRIVATE' not in json.dumps(result)
    # Logical readback cannot certify a previous flush or power-loss durability.
    assert wheels.verify(target,'runtime',root=root)['passed']


def test_corruption_during_copy_never_gets_a_completion_manifest(tmp_path,monkeypatch):
    root,source,target,_=setup(tmp_path)
    original=wheels.inventory; calls=[]
    def instrument(directory,*args,**kwargs):
        if Path(directory)==target:
            calls.append('destination inspection')
            sample=target/'sample-1.0-py3-none-any.whl'
            sample.write_bytes(sample.read_bytes()+b'CORRUPTION')
        return original(directory,*args,**kwargs)
    monkeypatch.setattr(wheels,'inventory',instrument)
    assert not wheels.capture(source,target,'runtime',root=root)['passed'] and calls
    assert target.is_dir() and not (target/wheels.MANIFEST).exists()


def test_real_offline_install_requires_exact_archives_and_no_index(tmp_path):
    root,source,target,_=setup(tmp_path)
    assert wheels.capture(source,target,'runtime',root=root)['passed']
    env=tmp_path/'venv'
    created=subprocess.run([sys.executable,'-m','venv',str(env)],capture_output=True,text=True,timeout=60)
    assert created.returncode==0
    python=env/('Scripts/python.exe' if os.name=='nt' else 'bin/python')
    command=[str(python),'-m','pip','--isolated','--disable-pip-version-check','install','--no-index',
             '--find-links',str(target),'--require-hashes','--only-binary=:all:','-r',str(root/'requirements.txt')]
    result=subprocess.run(command,capture_output=True,text=True,timeout=60)
    assert result.returncode==0, result.stderr[-2000:]
    check=subprocess.run([str(python),'-m','pip','--isolated','check'],capture_output=True,text=True,timeout=30)
    assert check.returncode==0
    installed=subprocess.run([str(python),'-c','import importlib.metadata as m; print(m.version("sample"))'],capture_output=True,text=True,timeout=10)
    assert installed.returncode==0 and installed.stdout.strip()=='1.0'


def test_cli_has_no_download_install_or_environment_fallback(tmp_path):
    result=subprocess.run([sys.executable,'-m','scripts.wheelhouse','verify','--directory',str(tmp_path/'missing'),'--profile','runtime'],capture_output=True,text=True,timeout=20)
    assert result.returncode==2 and not json.loads(result.stdout)['passed'] and not result.stderr
    assert str(tmp_path) not in result.stdout
    invalid=subprocess.run([sys.executable,'-m','scripts.wheelhouse','install'],capture_output=True,text=True,timeout=10)
    assert invalid.returncode==2
