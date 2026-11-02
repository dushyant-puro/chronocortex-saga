"""Controlled SQLite contention, not a claimed reproduction of historical CI."""
from contextlib import closing
from concurrent.futures import ThreadPoolExecutor
import json
import queue
import sqlite3
import subprocess
import sys
import threading

import pytest

from runtime.action_journal import JournalError, SQLiteActionJournal
from runtime import journal_observation as observation


def report(caplog):
    messages = [r.getMessage() for r in caplog.records if r.name == 'ccs.journal']
    assert len(messages) == 1
    return json.loads(messages[0].split('; ', 1)[1])


def test_same_thread_nested_transaction_identifies_local_peer_and_never_retries(tmp_path, caplog, monkeypatch):
    journal = SQLiteActionJournal(tmp_path / 'PRIVATE-journal.db')
    original = sqlite3.connect
    attempts = []
    with journal._transaction():
        def connect(*args, **kwargs):
            attempts.append(kwargs['timeout'])
            return original(*args, **kwargs)
        monkeypatch.setattr(sqlite3, 'connect', connect)
        with pytest.raises(JournalError) as caught:
            journal.records()
        evidence = report(caplog)
        assert caught.value.__cause__.sqlite_errorcode & 255 == sqlite3.SQLITE_BUSY
        assert evidence['phase'] == 'BEGIN' and evidence['tracked_peer_count'] == 1
        assert evidence['kind'] == 'READ_ACTIONS'
        assert evidence['peers'] == [{'kind': 'GENERAL', 'phase': 'BODY', 'thread': 'SAME_THREAD'}]
        assert attempts == [.25]
        assert 'PRIVATE' not in caplog.text and str(tmp_path) not in caplog.text
    assert not observation._entries


def test_other_thread_writer_is_observed_without_serializing_away_real_contention(tmp_path, caplog):
    journal = SQLiteActionJournal(tmp_path / 'journal.db')
    entered, release = threading.Event(), threading.Event()
    def hold():
        with journal._transaction():
            entered.set()
            assert release.wait(5)
    with ThreadPoolExecutor(1) as pool:
        holder = pool.submit(hold)
        try:
            assert entered.wait(5)
            with pytest.raises(JournalError):
                journal.records()
            assert report(caplog)['peers'] == [{'kind': 'GENERAL', 'phase': 'BODY', 'thread': 'OTHER_THREAD'}]
        finally:
            release.set()
            holder.result(timeout=5)
    assert not observation._entries


def test_external_process_writer_cannot_be_misrepresented_as_a_tracked_owner(tmp_path, caplog):
    journal = SQLiteActionJournal(tmp_path / 'journal.db')
    source = ("import sqlite3,sys; db=sqlite3.connect(sys.argv[1]); db.execute('BEGIN IMMEDIATE'); "
              "print('READY',flush=True); sys.stdin.readline(); db.rollback(); db.close()")
    child = subprocess.Popen([sys.executable, '-c', source, str(journal.path)],
                             stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                             text=True, creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
    ready = queue.Queue()
    reader = threading.Thread(target=lambda: ready.put(child.stdout.readline()), daemon=True)
    reader.start()
    try:
        assert ready.get(timeout=5).strip() == 'READY'
        with pytest.raises(JournalError):
            journal.records()
        evidence = report(caplog)
        assert evidence['scope'] == 'PROCESS_LOCAL_ONLY'
        assert evidence['tracked_peer_count'] == 0 and evidence['peers'] == []
    finally:
        try:
            if child.poll() is None:
                child.stdin.write('release\n')
                child.stdin.flush()
                child.wait(timeout=5)
        finally:
            if child.poll() is None:
                child.kill()
                child.wait(timeout=5)
            child.stdin.close()
            child.stdout.close()
            reader.join(timeout=5)
    assert not reader.is_alive() and not observation._entries


def test_busy_commit_observes_commit_phase_and_preserves_rollback(tmp_path, caplog):
    journal = SQLiteActionJournal(tmp_path / 'journal.db')
    with closing(sqlite3.connect(journal.path, isolation_level=None)) as reader:
        reader.execute('BEGIN')
        before = reader.execute('SELECT count(*) FROM security_events').fetchone()[0]
        with pytest.raises(JournalError):
            with journal._transaction() as writer:
                writer.execute("INSERT INTO security_events(kind,record) VALUES ('PRIVATE','PRIVATE')")
        assert report(caplog)['phase'] == 'COMMIT'
        reader.rollback()
        assert reader.execute('SELECT count(*) FROM security_events').fetchone()[0] == before
    assert 'PRIVATE' not in caplog.text and not observation._entries


def test_broken_logging_sink_cannot_replace_original_busy_or_keep_an_observation(tmp_path, monkeypatch):
    journal = SQLiteActionJournal(tmp_path / 'journal.db')
    def broken(*args, **kwargs):
        assert not observation._lock.locked()
        assert len(observation._entries) == 1  # Only the outer transaction remains.
        raise RuntimeError('PRIVATE LOG FAILURE')
    monkeypatch.setattr(observation.logger, 'warning', broken)
    with journal._transaction():
        with pytest.raises(JournalError) as caught:
            journal.records()
        assert caught.value.__cause__.sqlite_errorcode & 255 == sqlite3.SQLITE_BUSY
    assert not observation._entries


def test_commit_contention_is_logged_only_after_rollback_and_connection_close(tmp_path, monkeypatch):
    journal = SQLiteActionJournal(tmp_path / 'journal.db')
    captured = []
    def sink(*args):
        assert not observation._entries
        # A new reserved writer can begin: the failed writer already released
        # its transaction. The still-open reader intentionally blocks COMMIT.
        with closing(sqlite3.connect(journal.path, timeout=0, isolation_level=None)) as probe:
            probe.execute('BEGIN IMMEDIATE')
            probe.rollback()
        captured.append(args)
    monkeypatch.setattr(observation.logger, 'warning', sink)
    with closing(sqlite3.connect(journal.path, isolation_level=None)) as reader:
        reader.execute('BEGIN')
        reader.execute('SELECT count(*) FROM security_events').fetchone()
        with pytest.raises(JournalError):
            with journal._transaction() as writer:
                writer.execute("INSERT INTO security_events(kind,record) VALUES ('SIMULATED','{}')")
        assert len(captured) == 1
        reader.rollback()


@pytest.mark.parametrize('code', [True, None, -251, 2**40 + 5, '5'])
def test_malformed_error_codes_are_not_promoted_to_contention(code, caplog):
    error = sqlite3.OperationalError('PRIVATE')
    error.sqlite_errorcode = code
    observation.contention(None, 'PRIVATE', error)
    assert not caplog.records


def test_observation_capacity_is_bounded_and_partial_not_complete(caplog):
    tickets = [observation.begin('PRIVATE-path', 'PRIVATE-kind') for _ in range(observation.LIMIT + 1)]
    try:
        observation.stage(tickets[1], 'PRIVATE-phase')
        error = sqlite3.OperationalError('PRIVATE exception text')
        error.sqlite_errorcode = sqlite3.SQLITE_BUSY
        observation.contention(tickets[0], 'PRIVATE-path', error)
        evidence = report(caplog)
        assert len(observation._entries) == observation.LIMIT
        assert evidence['coverage'] == 'PARTIAL' and evidence['peers_truncated']
        assert len(evidence['peers']) == observation.PEER_LIMIT
        assert len(json.dumps(evidence)) < 2048
        assert 'PRIVATE' not in caplog.text
    finally:
        for ticket in tickets:
            observation.finish(ticket)
    assert not observation._entries and observation._overflow == 0


def test_success_nonbusy_and_non_sql_errors_do_not_emit_contention(tmp_path, caplog):
    journal = SQLiteActionJournal(tmp_path / 'journal.db')
    journal.records()
    for error in (ValueError('PRIVATE'), sqlite3.OperationalError('PRIVATE')):
        observation.contention(None, journal.path, error)
    with pytest.raises(RuntimeError):
        with journal._transaction():
            raise RuntimeError('PRIVATE')
    assert not caplog.records and not observation._entries


def test_reentrant_logging_sink_does_not_recursively_report_contention(tmp_path, monkeypatch):
    journal = SQLiteActionJournal(tmp_path / 'journal.db')
    calls = []
    def reentrant(*args):
        calls.append(1)
        assert len(calls) == 1
        with pytest.raises(JournalError):
            journal.records()
    monkeypatch.setattr(observation.logger, 'warning', reentrant)
    with journal._transaction():
        with pytest.raises(JournalError):
            journal.records()
    assert calls == [1] and not observation._entries


def test_observer_mutex_never_spans_a_sqlite_call(tmp_path, monkeypatch):
    original = sqlite3.connect
    class Probe:
        def __init__(self, db): self.db = db
        def __getattr__(self, name):
            member = getattr(self.db, name)
            if callable(member):
                def call(*args, **kwargs):
                    assert not observation._lock.locked()
                    return member(*args, **kwargs)
                return call
            return member
    def connect(*args, **kwargs):
        assert not observation._lock.locked()
        return Probe(original(*args, **kwargs))
    monkeypatch.setattr(sqlite3, 'connect', connect)
    journal = SQLiteActionJournal(tmp_path / 'journal.db')
    assert journal.records() == [] and not observation._entries


def test_clock_preservation_failure_is_observed_without_changing_error_semantics(tmp_path, monkeypatch, caplog):
    from runtime.policy_engine import PermissionDeniedError
    journal = SQLiteActionJournal(tmp_path / 'journal.db')
    original = sqlite3.connect
    error = sqlite3.OperationalError('PRIVATE clock commit failure')
    error.sqlite_errorcode = sqlite3.SQLITE_BUSY
    class Probe:
        def __init__(self, db): self.db = db
        def __getattr__(self, name): return getattr(self.db, name)
        def execute(self, sql, *args):
            if sql == 'COMMIT': raise error
            return self.db.execute(sql, *args)
    monkeypatch.setattr(sqlite3, 'connect', lambda *args, **kwargs: Probe(original(*args, **kwargs)))
    with pytest.raises(sqlite3.OperationalError) as caught:
        with journal._transaction():
            raise PermissionDeniedError('PRIVATE denial')
    assert caught.value is error and report(caplog)['phase'] == 'CLOCK_COMMIT'
    assert 'PRIVATE' not in caplog.text and not observation._entries
