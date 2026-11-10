"""Deterministic local serialization; no retry or replacement of SQL fences."""
from concurrent.futures import ThreadPoolExecutor
import threading

import pytest

from runtime.action_journal import SQLiteActionJournal, JournalError


def test_connections_share_gate_before_sql_setup_and_preserve_both_writes(tmp_path):
    a = SQLiteActionJournal(tmp_path / 'journal.db', local_timeout=2)
    b = SQLiteActionJournal(a.path, local_timeout=2)
    assert a._local_gate is b._local_gate
    entered, release, queued = threading.Event(), threading.Event(), threading.Event()
    def first():
        with a._transaction() as db:
            db.execute('CREATE TABLE gate_probe(value INTEGER)')
            entered.set()
            assert release.wait(3)
            db.execute('INSERT INTO gate_probe VALUES (1)')
    def second():
        queued.set()
        with b._transaction() as db:
            db.execute('INSERT INTO gate_probe VALUES (2)')
    with ThreadPoolExecutor(2) as pool:
        x = pool.submit(first)
        assert entered.wait(3)
        y = pool.submit(second)
        assert queued.wait(3)
        assert not y.done()
        release.set()
        x.result(timeout=3); y.result(timeout=3)
    with a._transaction() as db:
        assert db.execute('SELECT value FROM gate_probe ORDER BY value').fetchall() == [(1,), (2,)]


def test_queue_timeout_runs_no_body_and_gate_recovers_after_failure(tmp_path):
    a = SQLiteActionJournal(tmp_path / 'journal.db', local_timeout=.01)
    b = SQLiteActionJournal(a.path, local_timeout=.01)
    called = False
    with a._transaction():
        with pytest.raises(JournalError, match='queue timed out'):
            with b._transaction():
                called = True
    assert not called
    with pytest.raises(JournalError) as failed:
        with a._transaction():
            raise ValueError('test body')
    assert isinstance(failed.value.__cause__, ValueError)
    with b._transaction() as db:
        assert db.execute('SELECT 1').fetchone() == (1,)


def test_unrelated_journals_do_not_share_a_queue(tmp_path):
    a = SQLiteActionJournal(tmp_path / 'a.db')
    b = SQLiteActionJournal(tmp_path / 'b.db')
    assert a._local_gate is not b._local_gate
    with a._transaction(), b._transaction():
        pass


def test_negative_local_timeout_cannot_become_an_unbounded_wait(tmp_path):
    a = SQLiteActionJournal(tmp_path / 'journal.db', local_timeout=-1)
    with a._transaction():
        with pytest.raises(JournalError, match='queue timed out'):
            a.records()


@pytest.mark.parametrize('timeout', [float('nan'), float('inf'), 10**500])
def test_invalid_queue_timeout_cannot_open_or_create_a_journal(tmp_path, timeout):
    path = tmp_path / 'journal.db'
    with pytest.raises(JournalError):
        SQLiteActionJournal(path, local_timeout=timeout)
    assert not path.exists()


def test_sql_busy_and_local_wait_defaults_are_independent(tmp_path):
    journal = SQLiteActionJournal(tmp_path / 'journal.db')
    assert journal.timeout == .25
    assert journal.local_timeout == 5
    explicit = SQLiteActionJournal(journal.path, timeout=.01, local_timeout=.02)
    assert explicit.timeout == .01 and explicit.local_timeout == .02
