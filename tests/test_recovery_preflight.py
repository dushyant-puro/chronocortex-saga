"""Real SQLite evidence inspection; provider callbacks are SIMULATED."""
import json
import sqlite3
import subprocess
import sys

import pytest

from runtime.action_journal import SQLiteActionJournal
from scripts.recovery_preflight import inspect
from tests.test_durable_action_intent import manager


def populate(tmp_path):
    path = tmp_path / 'private journal #1.db'
    saga = manager(path)
    action = saga.stage_write('change', {'secret': 'NEVER-REPORT-THIS'})
    return path, saga, action


def change(path, sql, args=()):
    with sqlite3.connect(path) as db:
        db.execute(sql, args)


def test_read_only_inventory_does_not_change_database_or_disclose_payload(tmp_path):
    path, saga, action = populate(tmp_path)
    before = path.read_bytes()
    report = inspect(path)
    assert report['passed'] and report['counts']['actions'] == 1
    assert report['uncertain_actions'] == 0
    assert all(report[k] is False for k in ('restore_authorized', 'dispatch_authorized', 'freshness_verified', 'provider_reconciled'))
    assert path.read_bytes() == before
    assert 'NEVER-REPORT-THIS' not in json.dumps(report)
    assert str(path) not in json.dumps(report)


@pytest.mark.parametrize('sql', [
    'PRAGMA user_version=5', 'PRAGMA application_id=1',
    'DROP TABLE control_events', 'DELETE FROM key_claims',
    "UPDATE key_claims SET operation_key='different'",
    'DELETE FROM events', 'UPDATE actions SET version=20',
    "UPDATE events SET record='{}'", "UPDATE actions SET record='broken-secret'",
    'DELETE FROM chains', "UPDATE chains SET task_id='different'",
    "INSERT INTO events(action_id,version,kind,record) VALUES ('orphan',0,'STAGED','{}')",
    "UPDATE actions SET record=printf('%1100000s','x')",
])
def test_corrupted_or_incomplete_evidence_fails_without_repair(tmp_path, sql):
    path, _, _ = populate(tmp_path)
    change(path, sql)
    before = path.read_bytes()
    report = inspect(path)
    assert not report['passed'] and report['failure'] == 'JOURNAL_EVIDENCE_UNVERIFIED'
    assert path.read_bytes() == before
    assert 'broken-secret' not in json.dumps(report)


@pytest.mark.parametrize('field,value', [('state','NEW'), ('compensation_state','NEW'),
                                       ('chain_id',12), ('idempotency_key',None), ('step_index',0)])
def test_matching_but_invalid_action_and_event_fail(tmp_path, field, value):
    path, _, _ = populate(tmp_path)
    with sqlite3.connect(path) as db:
        record = json.loads(db.execute('SELECT record FROM actions').fetchone()[0])
        record[field] = value
        payload = json.dumps(record)
        db.execute('UPDATE actions SET record=?', (payload,))
        db.execute('UPDATE events SET record=?', (payload,))
    assert not inspect(path)['passed']


def test_missing_foreign_corrupt_and_locked_sources_fail_closed(tmp_path):
    missing = tmp_path / 'missing.db'
    assert not inspect(missing)['passed'] and not missing.exists()
    missing.write_bytes(b'not a database PRIVATE')
    assert not inspect(missing)['passed']
    path, _, _ = populate(tmp_path)
    with sqlite3.connect(path) as db:
        db.execute('BEGIN EXCLUSIVE')
        assert not inspect(path)['passed']
    assert inspect(path)['passed']


@pytest.mark.asyncio
async def test_unknown_effect_survives_inspection_with_claim_and_no_replay(tmp_path):
    path, saga, action = populate(tmp_path)
    calls = []
    async def execute(args):
        calls.append('SIMULATED applied with lost receipt')
        raise TimeoutError('NEVER-REPORT-THIS')
    with pytest.raises(TimeoutError):
        await saga.commit_write(action.action_id, execute)
    before = path.read_bytes()
    report = inspect(path)
    assert report['passed'] and report['uncertain_actions'] == 1
    assert report['counts']['key_claims'] == 1 and len(calls) == 1
    assert path.read_bytes() == before


@pytest.mark.asyncio
async def test_committed_actions_and_task_history(tmp_path):
    path, saga, action = populate(tmp_path)
    async def execute(args):
        return {'receipt': 'SIMULATED'}
    await saga.commit_write(action.action_id, execute)
    saga.journal.save_task('task-1', {'task_id':'task-1'}, None)
    saga.journal.save_task('task-1', {'task_id':'task-1','extra':1}, 0)
    assert inspect(path)['passed']
    change(path, 'DELETE FROM task_events WHERE version=0')
    assert not inspect(path)['passed']


def test_duplicate_json_keys_cannot_hide_record_identity(tmp_path):
    path, _, _ = populate(tmp_path)
    with sqlite3.connect(path) as db:
        payload = db.execute('SELECT record FROM actions').fetchone()[0]
        payload = '{"state":"COMMITTED",' + payload[1:]
        db.execute('UPDATE actions SET record=?', (payload,))
        db.execute('UPDATE events SET record=?', (payload,))
    assert not inspect(path)['passed']


def test_aborted_operation_keeps_history_but_releases_claim(tmp_path):
    path, saga, action = populate(tmp_path)
    saga.abort_write(action.action_id)
    report = inspect(path)
    assert report['passed'] and report['counts']['key_claims'] == 0
    assert report['counts']['events'] == 2


@pytest.mark.parametrize('payload', ['{}', '[]', '{"task_id":"other"}'])
def test_invalid_task_projection_is_not_cleared_by_matching_event(tmp_path, payload):
    path, saga, _ = populate(tmp_path)
    saga.journal.save_task('task-1', {'task_id':'task-1'}, None)
    change(path, 'UPDATE task_checkpoints SET checkpoint=?', (payload,))
    change(path, 'UPDATE task_events SET checkpoint=?', (payload,))
    assert not inspect(path)['passed']


def test_time_budget_failure_never_returns_partial_clearance(tmp_path, monkeypatch):
    from scripts import recovery_preflight
    path, _, _ = populate(tmp_path)
    ticks = iter([0, 100])
    monkeypatch.setattr(recovery_preflight.time, 'monotonic', lambda: next(ticks, 100))
    report = inspect(path)
    assert not report['passed'] and 'counts' not in report


@pytest.mark.asyncio
async def test_consistent_old_copy_is_not_freshness_or_restore_authority(tmp_path):
    path, saga, action = populate(tmp_path)
    old = tmp_path / 'old.db'
    with sqlite3.connect(path) as source, sqlite3.connect(old) as destination:
        source.backup(destination)
    async def execute(args):
        return {'receipt':'SIMULATED after snapshot'}
    await saga.commit_write(action.action_id, execute)
    report = inspect(old)
    assert report['passed'] and not report['freshness_verified'] and not report['restore_authorized']
    assert report['counts']['events'] < inspect(path)['counts']['events']


def test_empty_journal_and_cli_exit_contract(tmp_path):
    path = tmp_path / 'empty.db'
    SQLiteActionJournal(path)
    result = subprocess.run([sys.executable, '-m', 'scripts.recovery_preflight', '--journal', str(path)], capture_output=True, text=True)
    assert result.returncode == 0 and json.loads(result.stdout)['passed']
    result = subprocess.run([sys.executable, '-m', 'scripts.recovery_preflight', '--journal', str(path)+'absent'], capture_output=True, text=True)
    assert result.returncode == 2 and not json.loads(result.stdout)['passed']
