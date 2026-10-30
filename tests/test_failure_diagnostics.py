"""Native SQLite errors and adversarial exception objects, no provider access."""
import asyncio
from contextlib import closing
import sqlite3

import pytest

from runtime.action_journal import SQLiteActionJournal, JournalError, JournalLeaseError
from runtime.failure_diagnostics import CATEGORIES, classify_failure
from runtime.policy_engine import PermissionDeniedError, PolicyError


SECRET = "PRIVATE_CREDENTIAL_TRANSCRIPT_AND_PATH"


@pytest.mark.parametrize("code,category", [
    (sqlite3.SQLITE_BUSY, "SQLITE_BUSY"), (sqlite3.SQLITE_BUSY | 256, "SQLITE_BUSY"),
    (sqlite3.SQLITE_LOCKED, "SQLITE_LOCKED"), (sqlite3.SQLITE_IOERR | 256, "SQLITE_IO"),
    (sqlite3.SQLITE_NOTADB, "SQLITE_CORRUPT"), (sqlite3.SQLITE_CORRUPT, "SQLITE_CORRUPT"),
    (sqlite3.SQLITE_READONLY, "SQLITE_READONLY"), (sqlite3.SQLITE_FULL, "SQLITE_FULL"),
    (99999, "SQLITE_OTHER"), (-251, "SQLITE_OTHER"), (2**100+5, "SQLITE_OTHER"),
    (True, "SQLITE_OTHER"), ("5", "SQLITE_OTHER"), (None, "SQLITE_OTHER"),
])
def test_native_primary_codes_override_generic_journal_wrapper(code, category):
    cause = sqlite3.OperationalError(SECRET)
    cause.sqlite_errorcode = code
    error = JournalError(SECRET)
    error.__cause__ = cause
    assert classify_failure(error) == category and category in CATEGORIES
    assert SECRET not in classify_failure(error)


@pytest.mark.parametrize("error,category", [
    (JournalError(SECRET), "JOURNAL_FAILURE"), (JournalLeaseError(SECRET), "JOURNAL_LEASE"),
    (PermissionDeniedError(SECRET), "AUTHORITY_DENIED"), (PolicyError(SECRET), "POLICY_FAILURE"),
    (OSError(SECRET), "OS_ERROR"), (TimeoutError(SECRET), "TIMEOUT"),
    (asyncio.CancelledError(SECRET), "CANCELLED"), (ValueError(SECRET), "UNCLASSIFIED"),
])
def test_categories_never_serialize_exception_messages(error, category):
    assert classify_failure(error) == category


def test_unknown_exception_string_and_repr_are_never_called():
    class Poison(Exception):
        def __str__(self): pytest.fail("Exception text accessed")
        def __repr__(self): pytest.fail("Exception representation accessed")
    error = Poison(SECRET)
    error.sqlite_errorcode = sqlite3.SQLITE_BUSY  # A foreign type cannot claim SQLite evidence.
    assert classify_failure(error) == "UNCLASSIFIED"


def test_cycles_and_deep_causes_are_bounded():
    cycle = JournalError(SECRET); cycle.__cause__ = cycle
    assert classify_failure(cycle) == "JOURNAL_FAILURE"
    error = sqlite3.OperationalError(SECRET); error.sqlite_errorcode = sqlite3.SQLITE_BUSY
    for _ in range(9):
        outer = JournalError(SECRET); outer.__cause__ = error; error = outer
    assert classify_failure(error) == "JOURNAL_FAILURE"  # No claim about uninspected causes.


def test_broken_cause_inspection_cannot_escape_the_classifier():
    class Poison(Exception):
        @property
        def __cause__(self): raise RuntimeError(SECRET)
    assert classify_failure(Poison()) == "UNCLASSIFIED"


def test_explicit_exclusive_sqlite_lock_has_a_real_busy_cause(tmp_path):
    journal = SQLiteActionJournal(tmp_path / "contended.db", timeout=.01)
    before = journal.records()
    with closing(sqlite3.connect(journal.path, isolation_level=None)) as holder:
        holder.execute("BEGIN EXCLUSIVE")
        try:
            with pytest.raises(JournalError) as failure:
                journal.records()
            assert isinstance(failure.value.__cause__, sqlite3.Error)
            assert classify_failure(failure.value) == "SQLITE_BUSY"
        finally:
            holder.rollback()
    assert journal.records() == before
