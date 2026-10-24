"""Real Fernet and SQLite; synthetic local evidence, no external providers."""
import json
from pathlib import Path
import subprocess
import sys

from cryptography.fernet import Fernet
import pytest

from scripts import encrypted_backup as encrypted
from scripts import journal_backup as plain
from tests.test_recovery_preflight import populate


def setup(tmp_path):
    journal, saga, action = populate(tmp_path)
    storage, keys = tmp_path/'storage', tmp_path/'keys'
    storage.mkdir(mode=0o700); keys.mkdir(mode=0o700)
    key = keys/'backup.key'; bundle = storage/'snapshot.ccse'
    assert encrypted.create_key(key)['passed']
    return journal, key, bundle, storage, saga, action


def test_encrypted_capture_and_memory_drill_preserve_evidence_without_key_disclosure(tmp_path):
    journal, key, bundle, storage, _, _ = setup(tmp_path)
    before = journal.read_bytes()
    captured = encrypted.capture(journal, bundle, key)
    assert captured['passed'] and captured['encrypted']
    raw = bundle.read_bytes()
    assert raw.startswith(encrypted.MAGIC)
    assert b'NEVER-REPORT-THIS' not in raw and b'SQLite format 3' not in raw
    assert key.read_bytes() not in raw
    restored = encrypted.drill(bundle, storage, key)
    assert restored['passed'] and restored['ciphertext_authenticated'] and restored['restored_to_memory']
    assert restored['counts'] == captured['counts']
    assert not restored['freshness_verified'] and not restored['authenticity_verified'] and not restored['restore_authorized']
    assert key.read_text() not in json.dumps([captured,restored])
    assert journal.read_bytes() == before and not list(storage.glob('.ccs-*'))


@pytest.mark.parametrize('mutation', ['wrong_key','tamper','truncated','trailing','whitespace','magic','domain'])
def test_unauthenticated_or_wrong_domain_data_never_writes_plaintext_or_opens_sqlite(tmp_path, monkeypatch, mutation):
    journal, key, bundle, storage, _, _ = setup(tmp_path)
    assert encrypted.capture(journal,bundle,key)['passed']
    raw = bundle.read_bytes()
    if mutation == 'wrong_key': key.write_bytes(Fernet.generate_key())
    elif mutation == 'tamper':
        position = len(encrypted.MAGIC)+20
        raw = raw[:position] + (b'B' if raw[position:position+1] == b'A' else b'A') + raw[position+1:]
    elif mutation == 'truncated': raw = raw[:-8]
    elif mutation == 'trailing': raw += b'!'
    elif mutation == 'whitespace': raw += b'\n'
    elif mutation == 'magic': raw = b'x'+raw[1:]
    else: raw = encrypted.MAGIC+Fernet(key.read_bytes()).encrypt(b'OTHER-PROTOCOL'+plain.MAGIC)
    bundle.write_bytes(raw)
    monkeypatch.setattr(plain, 'new_file', lambda *a, **k: pytest.fail('Unverified ciphertext wrote plaintext'))
    monkeypatch.setattr(plain.sqlite3, 'connect', lambda *a, **k: pytest.fail('Unverified ciphertext opened SQLite'))
    report = encrypted.drill(bundle,storage,key)
    assert not report['passed'] and not report['ciphertext_authenticated']
    assert not list(storage.glob('.ccs-*'))


def test_legacy_plaintext_bundle_requires_explicit_legacy_tool(tmp_path):
    journal, key, bundle, storage, _, _ = setup(tmp_path)
    assert plain.capture(journal,bundle)['passed']
    assert not encrypted.drill(bundle,storage,key)['passed']
    assert plain.drill(bundle,storage)['passed']


@pytest.mark.parametrize('value', [b'', b'password', b'X'*44, b'A'*43+b'=\n', b'A'*128])
def test_malformed_keys_never_publish(tmp_path,value):
    journal,key,bundle,_,_,_ = setup(tmp_path)
    key.write_bytes(value)
    report = encrypted.capture(journal,bundle,key)
    assert not report['passed'] and not bundle.exists()
    if value: assert value.decode('ascii') not in json.dumps(report)


def test_key_in_backup_storage_is_refused_and_existing_key_is_never_replaced(tmp_path):
    journal,key,bundle,storage,_,_ = setup(tmp_path)
    existing = key.read_bytes()
    assert not encrypted.create_key(key)['passed'] and key.read_bytes()==existing
    local = storage/'unsafe.key'
    local.write_bytes(existing); local.chmod(0o600)
    assert not encrypted.capture(journal,bundle,local)['passed'] and not bundle.exists()


def test_existing_encrypted_bundle_and_source_sidecars_never_overwritten(tmp_path):
    journal,key,bundle,storage,_,_ = setup(tmp_path)
    bundle.write_bytes(b'PRIOR-EVIDENCE')
    assert not encrypted.capture(journal,bundle,key)['passed']
    assert bundle.read_bytes()==b'PRIOR-EVIDENCE'
    for suffix in ('','-journal','-wal','-shm'):
        assert not encrypted.capture(journal,Path(str(journal)+suffix),key)['passed']


@pytest.mark.asyncio
async def test_unknown_receipt_and_stale_encrypted_snapshot_never_authorize_replay(tmp_path):
    journal,key,bundle,storage,saga,action = setup(tmp_path)
    calls=[]
    async def execute(args):
        calls.append('SIMULATED remote effect')
        raise TimeoutError('lost receipt')
    with pytest.raises(TimeoutError): await saga.commit_write(action.action_id,execute)
    assert encrypted.capture(journal,bundle,key)['passed']
    report=encrypted.drill(bundle,storage,key)
    assert report['passed'] and report['uncertain_actions']==1 and len(calls)==1
    assert report['counts']['key_claims']==1 and not report['provider_reconciled']


def test_encryption_failure_never_publishes_plaintext_fallback(tmp_path,monkeypatch):
    journal,key,bundle,storage,_,_=setup(tmp_path)
    def fail(*a,**k): raise ValueError('SECRET ERROR')
    monkeypatch.setattr(Fernet,'encrypt',fail)
    report=encrypted.capture(journal,bundle,key)
    assert not report['passed'] and not bundle.exists() and not list(storage.glob('.ccs-*'))
    assert 'SECRET ERROR' not in json.dumps(report)


def test_cli_requires_key_file_and_never_prints_generated_secret(tmp_path):
    keys=tmp_path/'keys'; keys.mkdir(mode=0o700)
    key=keys/'new.key'
    result=subprocess.run([sys.executable,'-m','scripts.encrypted_backup','create-key','--key-file',str(key)],capture_output=True,text=True)
    assert result.returncode==0 and json.loads(result.stdout)['passed']
    assert key.read_text() not in result.stdout+result.stderr
    result=subprocess.run([sys.executable,'-m','scripts.encrypted_backup','capture','--journal','absent','--bundle','absent'],capture_output=True,text=True)
    assert result.returncode==2 and not result.stdout


@pytest.mark.parametrize('operation', ['capture','drill'])
def test_outer_cleanup_failure_cannot_leave_successful_verdict(tmp_path,monkeypatch,operation):
    journal,key,bundle,storage,_,_=setup(tmp_path)
    if operation=='drill': assert encrypted.capture(journal,bundle,key)['passed']
    original=encrypted.tempfile.TemporaryDirectory
    class Failure(original):
        def __init__(self,*args,**kwargs):
            self.outer=kwargs.get('prefix') in {'.ccs-encrypted-','.ccs-decrypted-'}
            super().__init__(*args,**kwargs)
        def __exit__(self,*args):
            super().__exit__(*args)
            if self.outer: raise OSError('PRIVATE cleanup failure')
    monkeypatch.setattr(encrypted.tempfile,'TemporaryDirectory',Failure)
    report=encrypted.capture(journal,bundle,key) if operation=='capture' else encrypted.drill(bundle,storage,key)
    assert not report['passed'] and bundle.exists()
    assert 'PRIVATE' not in json.dumps(report)


def test_oversize_ciphertext_and_missing_key_do_not_fall_back(tmp_path,monkeypatch):
    journal,key,bundle,storage,_,_=setup(tmp_path)
    assert encrypted.capture(journal,bundle,key)['passed']
    monkeypatch.setenv('CCS_BACKUP_KEY',key.read_text())
    missing=key.parent/'missing.key'
    assert not encrypted.drill(bundle,storage,missing)['passed']
    monkeypatch.setattr(encrypted,'MAX_BYTES',32)
    monkeypatch.setattr(plain,'new_file',lambda *a,**k: pytest.fail('Oversize ciphertext wrote plaintext'))
    assert not encrypted.drill(bundle,storage,key)['passed']


def test_hard_linked_key_is_refused(tmp_path):
    import os
    journal,key,bundle,_,_,_=setup(tmp_path)
    os.link(key,key.parent/'alias.key')
    assert not encrypted.capture(journal,bundle,key)['passed'] and not bundle.exists()
