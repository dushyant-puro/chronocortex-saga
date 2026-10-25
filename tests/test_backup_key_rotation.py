"""Real cryptography/SQLite/process boundaries; provider effects are SIMULATED."""
import base64
from contextlib import contextmanager
import json
import os
from pathlib import Path
import subprocess
import sys

from cryptography.exceptions import UnsupportedAlgorithm
from cryptography.fernet import Fernet, InvalidToken, MultiFernet
import pytest

from scripts import encrypted_backup as encrypted
from scripts import journal_backup as plain
from tests.test_encrypted_backup import setup


def prepared(tmp_path):
    journal, old_key, source, storage, saga, action = setup(tmp_path)
    new_key = old_key.parent / 'next.key'
    workspace = tmp_path / 'workspace'; workspace.mkdir(mode=0o700)
    assert encrypted.create_key(new_key)['passed']
    assert encrypted.capture(journal, source, old_key)['passed']
    return journal, old_key, new_key, source, storage/'rotated.ccse', workspace, saga, action


def rotate(values):
    _, old, new, source, target, workspace, *_ = values
    return encrypted.rotate(source, target, workspace, old, new)


def test_rotation_preserves_exact_payload_timestamp_and_originals(tmp_path):
    values = prepared(tmp_path)
    journal, old, new, source, target, workspace, *_ = values
    previous = [path.read_bytes() for path in (journal, old, new, source)]
    original = Fernet(previous[1]).decrypt(encrypted.read_ciphertext(source))
    # An old token remains old; rotation must not manufacture freshness.
    source.write_bytes(encrypted.MAGIC + Fernet(previous[1]).encrypt_at_time(original, 123456789))
    previous[3] = source.read_bytes()
    report = rotate(values)
    assert report['passed'] and report['payload_preserved'] and report['token_timestamp_preserved']
    assert report['ciphertext_authenticated'] and report['encrypted']
    token = encrypted.read_ciphertext(target)
    assert Fernet(new.read_bytes()).decrypt(token) == original
    assert Fernet(new.read_bytes()).extract_timestamp(token) == 123456789
    with pytest.raises(InvalidToken): Fernet(old.read_bytes()).decrypt(token)
    assert [path.read_bytes() for path in (journal, old, new, source)] == previous
    assert all(report[k] is False for k in ('freshness_verified','restore_authorized',
                                          'dispatch_authorized','authenticity_verified','provider_reconciled'))
    assert encrypted.drill(target, workspace, new)['passed']
    assert not encrypted.drill(target, workspace, old)['passed']
    assert not list(workspace.iterdir()) and not list(target.parent.glob('.ccs-*'))
    rendered = json.dumps(report)
    assert 'NEVER-REPORT-THIS' not in rendered
    for path in (old, new): assert path.read_text() not in rendered
    for path in (source, target, workspace): assert str(path) not in rendered


@pytest.mark.parametrize('failure', ['wrong_old','tampered','truncated','trailing','domain','new_only','legacy'])
def test_invalid_source_never_extracts_plaintext_or_publishes(tmp_path, monkeypatch, failure):
    values = prepared(tmp_path)
    journal, old, new, source, target, workspace, *_ = values
    if failure == 'wrong_old': old.write_bytes(Fernet.generate_key())
    elif failure == 'tampered':
        raw=bytearray(source.read_bytes()); raw[-20] = ord('A') if raw[-20]!=ord('A') else ord('B')
        source.write_bytes(raw)
    elif failure == 'truncated': source.write_bytes(source.read_bytes()[:-8])
    elif failure == 'trailing': source.write_bytes(source.read_bytes()+b'!')
    elif failure == 'domain': source.write_bytes(encrypted.MAGIC+Fernet(old.read_bytes()).encrypt(b'OTHER'+plain.MAGIC))
    elif failure == 'new_only':
        raw=Fernet(old.read_bytes()).decrypt(encrypted.read_ciphertext(source))
        source.write_bytes(encrypted.MAGIC+Fernet(new.read_bytes()).encrypt(raw))
    else:
        source.unlink()
        assert plain.capture(journal,source)['passed']
    previous=source.read_bytes()
    monkeypatch.setattr(plain,'new_file',lambda *a,**k: pytest.fail('Unverified input wrote a file'))
    monkeypatch.setattr(plain.sqlite3,'connect',lambda *a,**k: pytest.fail('Unverified input opened SQLite'))
    report=rotate(values)
    assert not report['passed'] and not target.exists() and source.read_bytes()==previous
    assert not list(workspace.iterdir())


@pytest.mark.parametrize('kind', ['same_path','same_bytes','same_decoded_key','missing','malformed'])
def test_invalid_or_unchanged_keys_fail_before_plaintext(tmp_path, monkeypatch, kind):
    values=list(prepared(tmp_path))
    old,new=values[1:3]
    if kind=='same_path': values[2]=old
    elif kind=='same_bytes': new.write_bytes(old.read_bytes())
    elif kind=='same_decoded_key':
        alphabet=b'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_'
        raw=old.read_bytes()
        # Nonzero padding bits can alias the same 32 decoded key bytes.
        new.write_bytes(raw[:-2]+bytes([alphabet[alphabet.index(raw[-2]) ^ 1]])+b'=')
        assert base64.urlsafe_b64decode(new.read_bytes())==base64.urlsafe_b64decode(raw)
    elif kind=='missing': new.unlink()
    else: new.write_bytes(b'PRIVATE bad key')
    monkeypatch.setattr(plain,'new_file',lambda *a,**k: pytest.fail('Invalid key wrote plaintext'))
    report=rotate(values)
    assert not report['passed'] and not values[4].exists()
    assert 'PRIVATE' not in json.dumps(report)


@pytest.mark.parametrize('location', ['source','destination','workspace'])
def test_both_keys_must_be_outside_all_storage_and_workspace_trees(tmp_path, location):
    values=list(prepared(tmp_path))
    if location=='source':
        new_storage=tmp_path/'new-storage'; new_storage.mkdir(mode=0o700)
        values[4]=new_storage/'rotated.ccse'
        key=values[3].parent/'colocated.key'
    elif location=='destination':
        new_storage=tmp_path/'new-storage'; new_storage.mkdir(mode=0o700)
        values[4]=new_storage/'rotated.ccse'
        key=new_storage/'colocated.key'
    else: key=values[5]/'colocated.key'
    key.write_bytes(values[2].read_bytes()); key.chmod(0o600)
    values[2]=key
    assert not rotate(values)['passed'] and not values[4].exists()


def test_authenticated_but_invalid_journal_is_not_reencrypted(tmp_path):
    values=prepared(tmp_path)
    _,old,_,source,target,_,*_=values
    source.write_bytes(encrypted.MAGIC+Fernet(old.read_bytes()).encrypt(encrypted.DOMAIN+plain.MAGIC+b'invalid manifest PRIVATE'))
    report=rotate(values)
    assert not report['passed'] and not target.exists() and 'PRIVATE' not in json.dumps(report)


def test_existing_source_and_destination_are_never_replaced(tmp_path):
    values=list(prepared(tmp_path))
    source,target=values[3:5]
    previous=source.read_bytes()
    values[4]=source
    assert not rotate(values)['passed'] and source.read_bytes()==previous
    values[4]=target
    target.write_bytes(b'PRIOR EVIDENCE')
    assert not rotate(values)['passed'] and target.read_bytes()==b'PRIOR EVIDENCE'


def test_competing_publisher_wins_without_clobber_or_fallback(tmp_path, monkeypatch):
    values=prepared(tmp_path)
    publication=plain.publish_no_clobber
    def race(pending,target):
        target.write_bytes(b'CONCURRENT EVIDENCE')
        publication(pending,target)
    monkeypatch.setattr(plain,'publish_no_clobber',race)
    report=rotate(values)
    assert not report['passed'] and values[4].read_bytes()==b'CONCURRENT EVIDENCE'


@pytest.mark.parametrize('failure', ['fsync','link','corrupt_write'])
def test_file_failures_do_not_publish_unverified_or_plaintext_output(tmp_path, monkeypatch, failure):
    values=prepared(tmp_path)
    original=plain.new_file
    if failure=='fsync':
        def fail(*a,**k): raise OSError('PRIVATE flush failure')
        monkeypatch.setattr(encrypted.os,'fsync',fail)
    elif failure=='link':
        def fail(*a,**k): raise OSError('PRIVATE unsupported hard link')
        monkeypatch.setattr(encrypted.os,'link',fail)
    else:
        @contextmanager
        def corrupt(path):
            with original(path) as stream: yield stream
            if path.name=='encrypted.partial': path.write_bytes(path.read_bytes()[:-8])
        monkeypatch.setattr(plain,'new_file',corrupt)
    report=rotate(values)
    assert not report['passed'] and not values[4].exists() and 'PRIVATE' not in json.dumps(report)
    assert not list(values[5].iterdir())


@pytest.mark.parametrize('failure', ['crypto','backend','payload','timestamp','publication','cleanup'])
def test_rotation_failures_never_return_success_or_plaintext_fallback(tmp_path, monkeypatch, failure):
    values=prepared(tmp_path)
    _,old,new,source,target,workspace,*_=values
    previous=[p.read_bytes() for p in (old,new,source)]
    if failure in {'crypto','backend'}:
        def fail(*a,**k):
            raise (UnsupportedAlgorithm if failure=='backend' else ValueError)('PRIVATE failure')
        monkeypatch.setattr(MultiFernet,'rotate',fail)
    elif failure in {'payload','timestamp'}:
        raw=Fernet(old.read_bytes()).decrypt(encrypted.read_ciphertext(source))
        token=Fernet(new.read_bytes()).encrypt_at_time(raw if failure=='timestamp' else raw+b'x',1)
        monkeypatch.setattr(MultiFernet,'rotate',lambda *a,**k: token)
    elif failure=='publication':
        def fail(*a,**k): raise OSError('PRIVATE publication failure')
        monkeypatch.setattr(plain,'publish_no_clobber',fail)
    else:
        original=encrypted.tempfile.TemporaryDirectory
        class Failure(original):
            def __init__(self,*a,**k):
                self.outer=k.get('prefix')=='.ccs-rotation-'
                super().__init__(*a,**k)
            def __exit__(self,*a):
                super().__exit__(*a)
                if self.outer: raise OSError('PRIVATE cleanup failure')
        monkeypatch.setattr(encrypted.tempfile,'TemporaryDirectory',Failure)
    report=rotate(values)
    assert not report['passed'] and 'PRIVATE' not in json.dumps(report)
    assert [p.read_bytes() for p in (old,new,source)]==previous
    assert not list(workspace.iterdir())
    if failure=='cleanup': assert encrypted.drill(target,workspace,new)['passed']
    else: assert not target.exists()


@pytest.mark.parametrize('boundary', ['before','after'])
def test_process_death_around_publication_preserves_recoverable_evidence(tmp_path, boundary):
    values=prepared(tmp_path)
    journal,old,new,source,target,workspace,*_=values
    previous=[p.read_bytes() for p in (journal,old,new,source)]
    program='''import os, sys
from scripts import encrypted_backup as e
from scripts import journal_backup as p
original=p.publish_no_clobber
def stop(pending,target):
    if sys.argv[1]=='after': original(pending,target)
    os._exit(73)
p.publish_no_clobber=stop
e.rotate(*sys.argv[2:])
'''
    result=subprocess.run([sys.executable,'-c',program,boundary,str(source),str(target),str(workspace),str(old),str(new)],capture_output=True,text=True)
    assert result.returncode==73 and not result.stdout and not result.stderr
    assert [p.read_bytes() for p in (journal,old,new,source)]==previous
    assert not list(workspace.iterdir())
    assert encrypted.drill(source,workspace,old)['passed']
    if boundary=='after': assert encrypted.drill(target,workspace,new)['passed']
    else: assert not target.exists()
    # Abrupt death can retain private temporary CIPHERTEXT. Never sweep it by
    # guessing the command failed or delete the only original/key generation.
    pending=list(target.parent.glob('.ccs-rotation-*/*'))
    assert pending and all(p.read_bytes().startswith(encrypted.MAGIC) for p in pending)


@pytest.mark.asyncio
async def test_rotation_keeps_unknown_claim_and_does_not_renew_old_evidence(tmp_path):
    values=prepared(tmp_path)
    journal,old,new,source,target,workspace,saga,action=values
    calls=[]
    async def effect(args):
        calls.append('SIMULATED external effect')
        raise TimeoutError('lost receipt')
    with pytest.raises(TimeoutError): await saga.commit_write(action.action_id,effect)
    unknown=source.parent/'unknown.ccse'
    assert encrypted.capture(journal,unknown,old)['passed']
    report=encrypted.rotate(unknown,target,workspace,old,new)
    assert report['passed'] and report['uncertain_actions']==1 and report['counts']['key_claims']==1
    assert len(calls)==1 and not report['provider_reconciled'] and not report['restore_authorized']
    older=encrypted.rotate(source,source.parent/'older-rotated.ccse',workspace,old,new)
    assert older['passed'] and older['counts']['events']==1 and not older['freshness_verified']


def test_documented_rotation_cli_is_explicit_and_sanitized(tmp_path):
    values=prepared(tmp_path)
    _,old,new,source,target,workspace,*_=values
    command=[sys.executable,'-m','scripts.encrypted_backup','rotate','--bundle',str(source),
             '--destination',str(target),'--workspace',str(workspace),'--old-key-file',str(old),'--new-key-file',str(new)]
    result=subprocess.run(command,capture_output=True,text=True)
    assert result.returncode==0 and json.loads(result.stdout)['passed'] and not result.stderr
    for key in (old,new): assert key.read_text() not in result.stdout
    missing=subprocess.run(command[:-2],capture_output=True,text=True)
    assert missing.returncode==2 and not missing.stdout
    repeated=subprocess.run(command,capture_output=True,text=True)
    assert repeated.returncode==2 and not json.loads(repeated.stdout)['passed'] and not repeated.stderr
