"""Bounded diagnostic categories; never serialize exception text or tracebacks."""
import asyncio
import sqlite3

from runtime.action_journal import JournalError, JournalLeaseError
from runtime.policy_engine import PolicyError, PermissionDeniedError


CATEGORIES = frozenset({"SQLITE_BUSY", "SQLITE_LOCKED", "SQLITE_IO", "SQLITE_CORRUPT",
                        "SQLITE_READONLY", "SQLITE_FULL", "SQLITE_OTHER", "TIMEOUT",
                        "CANCELLED", "OS_ERROR", "AUTHORITY_DENIED", "POLICY_FAILURE", "JOURNAL_LEASE",
                        "JOURNAL_FAILURE", "UNCLASSIFIED"})
_SQLITE = {sqlite3.SQLITE_BUSY: "SQLITE_BUSY", sqlite3.SQLITE_LOCKED: "SQLITE_LOCKED",
           sqlite3.SQLITE_IOERR: "SQLITE_IO", sqlite3.SQLITE_CORRUPT: "SQLITE_CORRUPT",
           sqlite3.SQLITE_NOTADB: "SQLITE_CORRUPT", sqlite3.SQLITE_READONLY: "SQLITE_READONLY",
           sqlite3.SQLITE_FULL: "SQLITE_FULL"}


def classify_failure(error):
    """Inspect at most eight explicit causes; the result is an enum, not a verdict.

    A SQLite primary category takes precedence over a wrapping JournalError.
    Generic OS/timeout categories do not prove a particular disk or network cause.
    Unknown codes, cyclic/deep chains and unavailable evidence stay conservative.
    """
    category, seen = "UNCLASSIFIED", set()
    try:
        for _ in range(8):
            if not isinstance(error, BaseException) or id(error) in seen:
                break
            seen.add(id(error))
            if isinstance(error, sqlite3.Error):
                code = getattr(error, "sqlite_errorcode", None)
                return _SQLITE.get(code & 255, "SQLITE_OTHER") if type(code) is int and 0 <= code <= 2**31-1 else "SQLITE_OTHER"
            if isinstance(error, asyncio.CancelledError): category = "CANCELLED"
            elif isinstance(error, TimeoutError): category = "TIMEOUT"
            elif isinstance(error, OSError): category = "OS_ERROR"
            elif isinstance(error, PermissionDeniedError): category = "AUTHORITY_DENIED"
            elif isinstance(error, PolicyError): category = "POLICY_FAILURE"
            elif isinstance(error, JournalLeaseError): category = "JOURNAL_LEASE"
            elif isinstance(error, JournalError): category = "JOURNAL_FAILURE"
            error = error.__cause__
    except Exception:
        return "UNCLASSIFIED"
    return category
