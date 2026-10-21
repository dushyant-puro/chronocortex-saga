"""Local SQLite dispatch-intent journal. No network-filesystem/lease guarantee.

Each action update and its event commit in one transaction. SQL CAS and key
claims serialize writers sharing this database, not arbitrary provider clients.
"""
from contextlib import contextmanager
import json
from pathlib import Path
import sqlite3
from typing import Any


class JournalError(RuntimeError):
    """Persistence could not establish the required durable fact. Stop dispatch."""


class JournalConflict(JournalError):
    """The expected action version no longer owns this transition."""


class JournalDuplicate(JournalError):
    """An existing durable operation retains the requested key."""


_APP_ID = 0x43435341
_VERSION = 1
_IDENTITY = ("action_id", "tool_name", "args", "digest", "idempotency_key",
             "capture_epoch", "session_id", "task_id", "step_index")
_TRANSITIONS = {
    "PENDING": {"PENDING", "IN_FLIGHT", "IN_FLIGHT_UNKNOWN", "COMMITTED_STALE", "ABORTED"},
    "IN_FLIGHT": {"IN_FLIGHT", "IN_FLIGHT_UNKNOWN", "COMMITTED", "COMMITTED_STALE", "ABORTED"},
    "IN_FLIGHT_UNKNOWN": {"IN_FLIGHT_UNKNOWN", "COMMITTED", "COMMITTED_STALE", "ABORTED"},
    "COMMITTED": {"COMMITTED", "COMMITTED_STALE", "COMPENSATED"},
    "COMMITTED_STALE": {"COMMITTED_STALE", "COMPENSATED"},
    "ABORTED": {"ABORTED"},
    "COMPENSATED": {"COMPENSATED"},
}
_COMP_TRANSITIONS = {
    "NOT_STARTED": {"NOT_STARTED", "IN_FLIGHT", "SUCCEEDED"},
    "IN_FLIGHT": {"IN_FLIGHT", "UNKNOWN", "NOT_APPLIED", "SUCCEEDED"},
    "UNKNOWN": {"UNKNOWN", "SUCCEEDED"},
    "NOT_APPLIED": {"NOT_APPLIED", "SUCCEEDED"},
    "SUCCEEDED": {"SUCCEEDED"},
}


def _json(value: Any) -> str:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError, RecursionError) as exc:
        raise JournalError("Journal data must be finite JSON") from exc


class SQLiteActionJournal:
    """Schema v1; rollback journal + synchronous EXTRA on every connection.

    Use a local disk with reliable locking/flush semantics and protect its ACLs.
    The database contains request/result data. Do not place it on a share or
    delete its sidecars. Connections/transactions never span provider awaits.
    """

    def __init__(self, path: str | Path, *, timeout: float = 0.25):
        if not str(path) or str(path) == ":memory:" or str(path).startswith("file:"):
            raise JournalError("A file-backed journal path is required")
        self.path = Path(path).resolve()
        self.timeout = timeout
        try:
            with self._transaction(initialize=True) as db:
                version = db.execute("PRAGMA user_version").fetchone()[0]
                app_id = db.execute("PRAGMA application_id").fetchone()[0]
                tables = db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
                if version == 0 and app_id == 0 and not tables:
                    db.execute("CREATE TABLE actions (action_id TEXT PRIMARY KEY, version INTEGER NOT NULL, record TEXT NOT NULL)")
                    db.execute("CREATE TABLE key_claims (operation_key TEXT PRIMARY KEY, action_id TEXT NOT NULL UNIQUE REFERENCES actions(action_id))")
                    db.execute("CREATE TABLE events (seq INTEGER PRIMARY KEY AUTOINCREMENT, action_id TEXT NOT NULL REFERENCES actions(action_id), version INTEGER NOT NULL, kind TEXT NOT NULL, record TEXT NOT NULL, created_utc TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')), UNIQUE(action_id,version))")
                    db.execute("CREATE TABLE task_checkpoints (task_id TEXT PRIMARY KEY, version INTEGER NOT NULL, checkpoint TEXT NOT NULL)")
                    db.execute("CREATE TABLE task_events (seq INTEGER PRIMARY KEY AUTOINCREMENT, task_id TEXT NOT NULL REFERENCES task_checkpoints(task_id), version INTEGER NOT NULL, checkpoint TEXT NOT NULL, UNIQUE(task_id,version))")
                    db.execute(f"PRAGMA application_id={_APP_ID}")
                    db.execute(f"PRAGMA user_version={_VERSION}")
                elif version != _VERSION or app_id != _APP_ID:
                    raise JournalError("Unsupported or foreign journal schema; explicit migration required")
                if db.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                    raise JournalError("Journal integrity check failed")
                required = {"actions", "key_claims", "events", "task_checkpoints", "task_events"}
                present = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                if not required <= present:
                    raise JournalError("Journal schema tables are missing")
        except sqlite3.Error as exc:
            raise JournalError("Cannot initialize journal") from exc

    @contextmanager
    def _transaction(self, *, initialize=False):
        db = None
        try:
            db = sqlite3.connect(self.path, timeout=self.timeout, isolation_level=None)
            db.execute("PRAGMA foreign_keys=ON")
            db.execute("PRAGMA synchronous=EXTRA")
            # Do not silently weaken an existing deployment's journal mode.
            mode = db.execute("PRAGMA journal_mode").fetchone()[0]
            if mode != "delete" or db.execute("PRAGMA synchronous").fetchone()[0] != 3:
                raise JournalError("Journal requires DELETE mode and synchronous EXTRA")
            db.execute("BEGIN IMMEDIATE")
            if not initialize:
                if (db.execute("PRAGMA user_version").fetchone()[0] != _VERSION or
                        db.execute("PRAGMA application_id").fetchone()[0] != _APP_ID):
                    raise JournalError("Journal schema changed")
            yield db
            db.execute("COMMIT")
        except (sqlite3.Error, ValueError, TypeError, KeyError, RecursionError) as exc:
            raise JournalError("Journal transaction failed") from exc
        finally:
            if db is not None:
                try:
                    if db.in_transaction:
                        db.rollback()
                except sqlite3.Error as exc:
                    raise JournalError("Journal rollback failed; recovery required") from exc
                finally:
                    db.close()

    def create(self, record: dict) -> int:
        payload = _json(record)
        if record["state"] != "PENDING" or record["compensation_state"] != "NOT_STARTED":
            raise JournalError("New intent must be pending with no compensation attempt")
        with self._transaction() as db:
            if db.execute("SELECT 1 FROM key_claims WHERE operation_key=?", (record["idempotency_key"],)).fetchone():
                raise JournalDuplicate("Operation key is already durably claimed")
            db.execute("INSERT INTO actions VALUES (?,?,?)", (record["action_id"], 0, payload))
            db.execute("INSERT INTO key_claims VALUES (?,?)", (record["idempotency_key"], record["action_id"]))
            db.execute("INSERT INTO events(action_id,version,kind,record) VALUES (?,?,?,?)",
                       (record["action_id"], 0, "STAGED", payload))
        return 0

    def transition(self, record: dict, expected_version: int, kind: str) -> int:
        payload = _json(record)
        with self._transaction() as db:
            row = db.execute("SELECT version,record FROM actions WHERE action_id=?", (record["action_id"],)).fetchone()
            if row is None or row[0] != expected_version:
                raise JournalConflict("Action version changed; no dispatch or automatic retry")
            old = json.loads(row[1])
            if _json({key: old[key] for key in _IDENTITY}) != _json({key: record[key] for key in _IDENTITY}):
                raise JournalError("Journal action identity is immutable")
            if (record["state"] not in _TRANSITIONS[old["state"]] or
                    record["compensation_state"] not in _COMP_TRANSITIONS[old["compensation_state"]]):
                raise JournalError("Invalid journal transition")
            version = expected_version + 1
            db.execute("UPDATE actions SET version=?,record=? WHERE action_id=? AND version=?",
                       (version, payload, record["action_id"], expected_version))
            db.execute("INSERT INTO events(action_id,version,kind,record) VALUES (?,?,?,?)",
                       (record["action_id"], version, kind, payload))
            if (record["state"] in {"ABORTED", "COMPENSATED"} and
                    record["compensation_state"] in {"NOT_STARTED", "SUCCEEDED"}):
                db.execute("DELETE FROM key_claims WHERE action_id=?", (record["action_id"],))
        return version

    def records(self) -> list[tuple[int, dict]]:
        with self._transaction() as db:
            rows = db.execute("SELECT version,record FROM actions ORDER BY rowid").fetchall()
            return [(version, json.loads(record)) for version, record in rows]

    def events(self, action_id: str) -> list[dict]:
        with self._transaction() as db:
            rows = db.execute("SELECT seq,version,kind,record FROM events WHERE action_id=? ORDER BY seq", (action_id,)).fetchall()
        return [{"seq": seq, "version": version, "kind": kind, "record": json.loads(record)}
                for seq, version, kind, record in rows]

    def save_task(self, task_id: str, checkpoint: dict, expected_version: int | None) -> int:
        if checkpoint.get("task_id") != task_id:
            raise JournalError("Task checkpoint identity mismatch")
        payload = _json(checkpoint)
        with self._transaction() as db:
            row = db.execute("SELECT version FROM task_checkpoints WHERE task_id=?", (task_id,)).fetchone()
            if (row[0] if row else None) != expected_version:
                raise JournalConflict("Task checkpoint version changed")
            version = 0 if expected_version is None else expected_version + 1
            if row is None:
                db.execute("INSERT INTO task_checkpoints VALUES (?,?,?)", (task_id, version, payload))
            else:
                db.execute("UPDATE task_checkpoints SET version=?,checkpoint=? WHERE task_id=? AND version=?",
                           (version, payload, task_id, expected_version))
            db.execute("INSERT INTO task_events(task_id,version,checkpoint) VALUES (?,?,?)", (task_id, version, payload))
        return version

    def load_task(self, task_id: str) -> tuple[int, dict] | None:
        with self._transaction() as db:
            row = db.execute("SELECT version,checkpoint FROM task_checkpoints WHERE task_id=?", (task_id,)).fetchone()
            if row is None:
                return None
            checkpoint = json.loads(row[1])
            if checkpoint.get("task_id") != task_id:
                raise JournalError("Stored task identity mismatch")
            return row[0], checkpoint
