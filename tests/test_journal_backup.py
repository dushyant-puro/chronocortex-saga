"""Real SQLite capture/restoration; all external effects remain SIMULATED."""
import json
import os
from pathlib import Path
import sqlite3
import struct
import subprocess
import sys
import threading

import pytest

from scripts import journal_backup as backup
from tests.test_recovery_preflight import populate, change


def setup(tmp_path):
    path, saga, action = populate(tmp_path)
    target = tmp_path / 'evidence.ccsb'
    return path, saga, action, target


def test_capture_and_memory_restore_preserve_all_evidence_without_authority(tmp_path):
    path, saga, action, target = setup(tmp_path)
    original = path.read_bytes()
    report = backup.capture(path, target)
    assert report['passed']
    assert path.read_bytes() == original
    assert target.read_bytes().startswith(backup.MAGIC)
    assert 'NEVER-REPORT-THIS' not in json.dumps(report)
    before = target.read_bytes()
    restored = backup.drill(target, tmp_path)
    assert restored['passed'] and restored['restored_to_memory']
    assert restored['counts'] == report['counts']
    assert all(restored[key] is False for key in ('restore_authorized','dispatch_authorized','freshness_verified','provider_reconciled','authenticity_verified'))
    assert target.read_bytes() == before and path.read_bytes() == original
    assert not list(tmp_path.glob('.ccs-*'))


@pytest.mark.asyncio
async def test_lost_receipt_backup_retains_unknown_claims_without_replay(tmp_path):
    path, saga, action, target = setup(tmp_path)
    calls = []
    async def execute(args):
        calls.append('SIMULATED applied')
        raise TimeoutError('lost receipt')
    with pytest.raises(TimeoutError):
        await saga.commit_write(action.action_id, execute)
    assert backup.capture(path, target)['passed']
    report = backup.drill(target, tmp_path)
    assert report['passed'] and report['uncertain_actions'] == 1
    assert report['counts']['key_claims'] == 1 and len(calls) == 1


@pytest.mark.parametrize('suffix', ['', '-journal', '-wal', '-shm'])
def test_source_and_sidecar_destinations_refused(tmp_path, suffix):
    path, _, _, _ = setup(tmp_path)
    before = path.read_bytes()
    assert not backup.capture(path, Path(str(path)+suffix))['passed']
    assert path.read_bytes() == before


def test_existing_bundle_is_never_overwritten(tmp_path):
    path, _, _, target = setup(tmp_path)
    target.write_bytes(b'existing evidence')
    assert not backup.capture(path, target)['passed']
    assert target.read_bytes() == b'existing evidence'


@pytest.mark.parametrize('fault', ['link', 'fsync', 'copy'])
def test_failed_capture_never_publishes_partial_bundle(tmp_path, monkeypatch, fault):
    path, _, _, target = setup(tmp_path)
    def fail(*a, **k): raise OSError('PRIVATE FAILURE')
    if fault == 'copy': monkeypatch.setattr(backup, 'copy_database', fail)
    else: monkeypatch.setattr(backup.os, fault, fail)
    report = backup.capture(path, target)
    assert not report['passed'] and not target.exists()
    assert not list(tmp_path.glob('.ccs-*'))
    assert 'PRIVATE' not in json.dumps(report)


def test_competing_publisher_is_preserved(tmp_path, monkeypatch):
    path, _, _, target = setup(tmp_path)
    link = backup.os.link
    def race(source, destination):
        Path(destination).write_bytes(b'other publisher')
        return link(source, destination)
    monkeypatch.setattr(backup.os, 'link', race)
    assert not backup.capture(path, target)['passed']
    assert target.read_bytes() == b'other publisher'


@pytest.mark.parametrize('mutation', ['truncated','trailing','hash','magic','oversized','duplicate','version'])
def test_bad_bundle_fails_before_any_sqlite_open(tmp_path, monkeypatch, mutation):
    path, _, _, target = setup(tmp_path)
    assert backup.capture(path, target)['passed']
    raw = target.read_bytes()
    if mutation == 'truncated': raw = raw[:-5]
    elif mutation == 'trailing': raw += b'x'
    elif mutation == 'hash': raw = raw[:-1]+bytes([raw[-1]^1])
    elif mutation == 'magic': raw = b'x'+raw[1:]
    elif mutation == 'oversized': raw = backup.MAGIC+struct.pack('>I', 100000)+raw
    else:
        offset = len(backup.MAGIC)+4
        size = struct.unpack('>I', raw[len(backup.MAGIC):offset])[0]
        metadata = raw[offset:offset+size]
        metadata = b'{"schema":1,'+metadata[1:] if mutation == 'duplicate' else metadata.replace(b'"schema":1', b'"schema":2')
        raw = backup.MAGIC+struct.pack('>I',len(metadata))+metadata+raw[offset+size:]
    target.write_bytes(raw)
    monkeypatch.setattr(backup.sqlite3, 'connect', lambda *a, **k: pytest.fail('Unverified bundle reached SQLite'))
    assert not backup.drill(target, tmp_path)['passed']
    assert not list(tmp_path.glob('.ccs-*'))


def test_invalid_journal_or_locked_source_never_published(tmp_path):
    path, _, _, target = setup(tmp_path)
    with sqlite3.connect(path) as db:
        db.execute('BEGIN EXCLUSIVE')
        assert not backup.capture(path, target)['passed']
    change(path, 'DELETE FROM key_claims')
    assert not backup.capture(path, target)['passed'] and not target.exists()


@pytest.mark.asyncio
async def test_stale_backup_drill_never_claims_freshness(tmp_path):
    path, saga, action, target = setup(tmp_path)
    assert backup.capture(path, target)['passed']
    async def execute(args): return {'receipt':'SIMULATED later effect'}
    await saga.commit_write(action.action_id, execute)
    report = backup.drill(target, tmp_path)
    assert report['passed'] and not report['freshness_verified'] and not report['restore_authorized']
    assert report['counts']['events'] == 1


def test_cli_drill_failure_is_sanitized(tmp_path):
    result = subprocess.run([sys.executable,'-m','scripts.journal_backup','drill','--bundle',str(tmp_path/'missing'),'--workspace',str(tmp_path)], capture_output=True,text=True)
    assert result.returncode == 2
    assert not json.loads(result.stdout)['passed'] and not result.stderr


@pytest.mark.parametrize('operation', ['capture', 'drill'])
def test_cleanup_failure_revokes_success_and_preserves_published_evidence(tmp_path, monkeypatch, operation):
    path, _, _, target = setup(tmp_path)
    if operation == 'drill': assert backup.capture(path, target)['passed']
    original = backup.tempfile.TemporaryDirectory
    class FailingCleanup(original):
        def __exit__(self, *args):
            super().__exit__(*args)
            raise OSError('PRIVATE CLEANUP FAILURE')
    monkeypatch.setattr(backup.tempfile, 'TemporaryDirectory', FailingCleanup)
    report = backup.capture(path, target) if operation == 'capture' else backup.drill(target, tmp_path)
    assert not report['passed'] and 'failure' in report
    assert target.exists() and 'PRIVATE' not in json.dumps(report)


def test_git_directory_refused_before_creating_sensitive_data(tmp_path):
    path, _, _, target = setup(tmp_path)
    (tmp_path/'.git').mkdir()
    assert not backup.capture(path, target)['passed'] and not target.exists()


@pytest.mark.skipif(os.name == 'nt', reason='POSIX permission mode check; Windows requires protected inherited ACL')
def test_shared_posix_directory_refused(tmp_path):
    path, _, _, target = setup(tmp_path)
    tmp_path.chmod(0o755)
    try: assert not backup.capture(path, target)['passed'] and not target.exists()
    finally: tmp_path.chmod(0o700)


def test_writer_committing_during_capture_cannot_mix_snapshot_generations(tmp_path, monkeypatch):
    path, _, _, target = setup(tmp_path)
    connect = sqlite3.connect
    with connect(path) as db:
        before = db.execute('SELECT count(*) FROM security_events').fetchone()[0]
    started, finished, errors = threading.Event(), threading.Event(), []
    def write():
        try:
            with connect(path, timeout=5) as db:
                db.execute("INSERT INTO security_events(kind,record) VALUES ('AFTER_SNAPSHOT','{}')")
                db.execute('UPDATE security_clock SET last_seen_ms=last_seen_ms+1')
                started.set()
        except Exception as error:
            errors.append(type(error).__name__)
            started.set()
        finally: finished.set()
    worker = threading.Thread(target=write)
    class SnapshotConnection(sqlite3.Connection):
        def backup(self, destination, **kwargs):
            worker.start()
            assert started.wait(5)
            # The writer has changed its private pages but cannot commit until
            # the already-pinned source read transaction ends.
            assert not finished.is_set()
            return super().backup(destination, **kwargs)
    def instrument(database, *args, **kwargs):
        if str(database).startswith(path.resolve().as_uri()+'?'):
            kwargs['factory'] = SnapshotConnection
        return connect(database, *args, **kwargs)
    monkeypatch.setattr(backup.sqlite3, 'connect', instrument)
    try:
        captured = backup.capture(path, target)
    finally:
        if worker.ident is not None: worker.join(6)
    assert finished.is_set() and not errors and captured['passed']
    assert captured['counts']['security_events'] == before
    with connect(path) as db:
        assert db.execute('SELECT count(*) FROM security_events').fetchone()[0] == before+1
