"""Bounded process-local transaction observations, never lock/lease authority.

Paths and thread identifiers stay in memory. Tracked peers are observations,
not proof of SQLite lock ownership; other processes/connections are invisible.
"""
from dataclasses import dataclass, field
import json
import logging
import os
import sqlite3
import threading


logger = logging.getLogger('ccs.journal')
PHASES = frozenset({'CONNECT', 'CONFIGURE', 'BEGIN', 'SCHEMA', 'LEASE', 'BODY', 'COMMIT',
                    'CLOCK_ROLLBACK', 'CLOCK_BEGIN', 'CLOCK_COMMIT', 'ROLLBACK', 'CLOSE'})
KINDS = frozenset({'GENERAL', 'BOUND', 'ACQUIRED', 'RENEWED', 'RELEASED', 'STAGED', 'TASK_CHECKPOINT',
                   'CHAIN_CLOSED', 'DISPATCH_INTENT', 'FORWARD_COMMITTED', 'FORWARD_COMMITTED_STALE',
                   'FORWARD_UNKNOWN', 'FORWARD_NOT_APPLIED', 'TURN_INVALIDATED', 'TURN_SUPERSEDED',
                   'COMPENSATION_INTENT', 'COMPENSATION_UNKNOWN', 'COMPENSATION_SUCCEEDED'})
KINDS = KINDS | {'OPEN', 'READ_ACTIONS', 'READ_TASK', 'AUTHENTICATE'}
LIMIT = 128
PEER_LIMIT = 8
_lock = threading.Lock()
_entries = {}
_overflow = 0
_OVERFLOW = object()
_reporting = threading.local()


@dataclass
class _Entry:
    path: str = field(repr=False)
    thread: int = field(repr=False)
    kind: str
    phase: str = 'CONNECT'


@dataclass(frozen=True)
class ContentionObservation:
    _encoded: str = field(repr=False)


def begin(path, kind):
    global _overflow
    try:
        entry = _Entry(os.path.normcase(str(path)), threading.get_ident(),
                       kind if type(kind) is str and kind in KINDS else 'GENERAL' if kind is None else 'OTHER')
        with _lock:
            if len(_entries) >= LIMIT:
                _overflow += 1
                return _OVERFLOW
            ticket = object()
            _entries[ticket] = entry
            return ticket
    except Exception:
        return None


def stage(ticket, phase):
    try:
        with _lock:
            if ticket in _entries:
                _entries[ticket].phase = phase if type(phase) is str and phase in PHASES else 'OTHER'
    except Exception:
        pass


def finish(ticket):
    global _overflow
    try:
        with _lock:
            if ticket is _OVERFLOW:
                _overflow -= 1
            else:
                _entries.pop(ticket, None)
    except Exception:
        pass


def capture(ticket, path, error):
    """Capture before cleanup; no logging or SQLite calls while taking a snapshot."""
    try:
        if getattr(_reporting, 'active', False):
            return
        code = getattr(error, 'sqlite_errorcode', None)
        if (not isinstance(error, sqlite3.Error) or type(code) is not int or not 0 <= code <= 2**31 - 1 or
                code & 255 not in {sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED}):
            return
        with _lock:
            own = _entries.get(ticket)
            peers = [entry for key, entry in _entries.items()
                     if key is not ticket and entry.path == os.path.normcase(str(path))]
            evidence = {'schema': 1, 'scope': 'PROCESS_LOCAL_ONLY',
                        'coverage': 'UNAVAILABLE' if own is None else 'PARTIAL' if _overflow else 'TRACKED_LOCAL',
                        'category': 'SQLITE_BUSY' if code & 255 == sqlite3.SQLITE_BUSY else 'SQLITE_LOCKED',
                        'phase': own.phase if own else 'UNAVAILABLE', 'kind': own.kind if own else 'UNAVAILABLE',
                        'tracked_peer_count': len(peers), 'peers_truncated': len(peers) > PEER_LIMIT,
                        'peers': [{'phase': entry.phase, 'kind': entry.kind,
                                   'thread': 'SAME_THREAD' if entry.thread == threading.get_ident() else 'OTHER_THREAD'}
                                  for entry in peers[:PEER_LIMIT]]}
        return ContentionObservation(json.dumps(evidence, sort_keys=True, separators=(',', ':')))
    except Exception:
        return None


def emit(evidence):
    """Journal calls this after cleanup, never from inside its observer mutex."""
    try:
        if type(evidence) is not ContentionObservation or getattr(_reporting, 'active', False):
            return
        _reporting.active = True
        try:
            logger.warning('Journal contention; %s', evidence._encoded)
        finally:
            _reporting.active = False
    except Exception:
        pass  # Diagnostic failure must never replace the original SQL error.


def contention(ticket, path, error):
    """Test/host convenience; the journal separates capture from emission."""
    emit(capture(ticket, path, error))
