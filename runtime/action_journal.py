"""Local SQLite dispatch-intent journal with exclusive worker execution leases.

Each action update and its event commit in one transaction. SQL CAS and key
claims serialize writers sharing this database, not arbitrary provider clients.
"""
from contextlib import contextmanager
from dataclasses import dataclass
import json
from pathlib import Path
import sqlite3
import time
from typing import Any
from runtime.workflow_contract import WorkflowContractError, canonical, validate_record
from runtime.policy_engine import PolicyError, PermissionDeniedError
from runtime.security_authority import SecurityAuthority, AuthRequest, create_schema as create_security_schema


class JournalError(RuntimeError):
    """Persistence could not establish the required durable fact. Stop dispatch."""


class JournalConflict(JournalError):
    """The expected action version no longer owns this transition."""


class JournalDuplicate(JournalError):
    """An existing durable operation retains the requested key."""


class JournalLeaseError(JournalConflict):
    """Worker execution authority is absent, expired or superseded."""


@dataclass(frozen=True)
class WorkerLease:
    """Trusted host capability, not authentication or a provider fencing token."""
    owner: str
    generation: int


_APP_ID = 0x43435341
_VERSION = 6
_IDENTITY = ("action_id", "tool_name", "args", "digest", "idempotency_key",
             "capture_epoch", "session_id", "task_id", "step_index", "generation", "chain_id", "authorization_policy")
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
    """Schema v6; rollback journal + synchronous EXTRA on every connection.

    Use a local disk with reliable locking/flush semantics and protect its ACLs.
    The database contains request/result data. Do not place it on a share or
    delete its sidecars. Connections/transactions never span provider awaits.
    """

    def __init__(self, path: str | Path, *, timeout: float = 0.25, migrate: bool = False,
                 clock_ms=None):
        if not str(path) or str(path) == ":memory:" or str(path).startswith("file:"):
            raise JournalError("A file-backed journal path is required")
        self.path = Path(path).resolve()
        self.timeout = timeout
        self._clock_ms = clock_ms if clock_ms is not None else lambda: time.time_ns() // 1_000_000
        self._lease = None
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
                    self._create_ownership_tables(db)
                    self._create_lease_tables(db)
                    create_security_schema(db)
                    self._create_control_tables(db)
                    db.execute(f"PRAGMA application_id={_APP_ID}")
                    db.execute(f"PRAGMA user_version={_VERSION}")
                elif version in {1, 2, 3, 4, 5} and app_id == _APP_ID and migrate is True:
                    if version >= 3:
                        lease_row = db.execute("SELECT owner,generation,expires_ms,last_seen_ms FROM worker_lease WHERE singleton=1").fetchone()
                        now = self._now()
                        self._clock_guard(lease_row, now)
                        if lease_row is not None and now < lease_row[2]:
                            raise JournalError("Offline workflow migration requires an expired/released worker lease")
                    if version == 1:
                        self._migrate_v1(db)
                    if version < 3:
                        self._create_lease_tables(db)
                    if version < 5:
                        create_security_schema(db)
                    self._create_control_tables(db)
                    db.execute(f"PRAGMA user_version={_VERSION}")
                elif version != _VERSION or app_id != _APP_ID:
                    raise JournalError("Unsupported or foreign journal schema; explicit migration required")
                if db.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                    raise JournalError("Journal integrity check failed")
                required = {"actions", "key_claims", "events", "task_checkpoints", "task_events",
                            "chains", "operation_slots", "chain_events", "worker_lease", "worker_events",
                            "principals", "credentials", "approvals", "security_clock", "security_events",
                            "operator_sessions", "control_commands", "control_events"}
                present = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                if not required <= present:
                    raise JournalError("Journal schema tables are missing")
        except sqlite3.Error as exc:
            raise JournalError("Cannot initialize journal") from exc
        self.security = SecurityAuthority(self)

    @staticmethod
    def _create_control_tables(db):
        db.execute("CREATE TABLE operator_sessions (session_hash TEXT PRIMARY KEY, csrf_salt TEXT NOT NULL, credential_id TEXT NOT NULL REFERENCES credentials(id), principal_version INTEGER NOT NULL, expires_ms INTEGER NOT NULL, revoked INTEGER NOT NULL DEFAULT 0)")
        db.execute("CREATE TABLE control_commands (principal_id TEXT NOT NULL REFERENCES principals(id), operation_id TEXT NOT NULL, request_digest TEXT NOT NULL, kind TEXT NOT NULL, target_id TEXT NOT NULL, result TEXT NOT NULL, created_ms INTEGER NOT NULL, PRIMARY KEY(principal_id,operation_id))")
        db.execute("CREATE TABLE control_events (seq INTEGER PRIMARY KEY AUTOINCREMENT, principal_id TEXT NOT NULL, kind TEXT NOT NULL, target_id TEXT, operation_id TEXT, observed_ms INTEGER NOT NULL)")
        db.execute("CREATE INDEX operator_credential_sessions ON operator_sessions(credential_id,expires_ms)")
        db.execute("CREATE INDEX operator_decision_history ON control_commands(principal_id,created_ms DESC)")
        db.execute("CREATE INDEX control_target_requests ON control_commands(kind,target_id)")

    @staticmethod
    def _create_ownership_tables(db):
        db.execute("CREATE TABLE chains (chain_id TEXT PRIMARY KEY, task_id TEXT, status TEXT NOT NULL CHECK(status IN ('OPEN','CLOSED')))")
        db.execute("CREATE TABLE operation_slots (task_id TEXT NOT NULL, step_index INTEGER NOT NULL, generation INTEGER NOT NULL, action_id TEXT NOT NULL UNIQUE REFERENCES actions(action_id), PRIMARY KEY(task_id,step_index,generation))")
        db.execute("CREATE TABLE chain_events (seq INTEGER PRIMARY KEY AUTOINCREMENT, chain_id TEXT NOT NULL REFERENCES chains(chain_id), kind TEXT NOT NULL)")

    @staticmethod
    def _create_lease_tables(db):
        db.execute("CREATE TABLE worker_lease (singleton INTEGER PRIMARY KEY CHECK(singleton=1), owner TEXT NOT NULL, generation INTEGER NOT NULL, expires_ms INTEGER NOT NULL, last_seen_ms INTEGER NOT NULL)")
        db.execute("CREATE TABLE worker_events (seq INTEGER PRIMARY KEY AUTOINCREMENT, owner TEXT NOT NULL, generation INTEGER NOT NULL, kind TEXT NOT NULL, subject TEXT, observed_ms INTEGER NOT NULL)")

    def _now(self):
        now = self._clock_ms()
        if type(now) is not int or not 0 <= now <= 9_223_372_036_768_375_807:
            raise JournalLeaseError("Invalid lease clock")
        return now

    @staticmethod
    def _ttl(ttl_ms):
        if type(ttl_ms) is not int or not 1 <= ttl_ms <= 86_400_000:
            raise JournalLeaseError("lease TTL must be 1..86400000 integer milliseconds")

    @staticmethod
    def _clock_guard(row, now):
        if row is not None and now < row[3]:
            raise JournalLeaseError("lease clock moved backward; stop writes until clock is repaired")

    def _require_lease(self, db):
        row = db.execute("SELECT owner,generation,expires_ms,last_seen_ms FROM worker_lease WHERE singleton=1").fetchone()
        now = self._now()
        self._clock_guard(row, now)
        if row is not None:
            db.execute("UPDATE worker_lease SET last_seen_ms=? WHERE singleton=1", (now,))
        if (self._lease is None or row is None or
                (row[0], row[1]) != (self._lease.owner, self._lease.generation) or now >= row[2]):
            raise JournalLeaseError("Worker lease missing, expired or superseded; reopen for recovery")
        return now

    def acquire_lease(self, owner: str, *, ttl_ms: int = 30_000) -> WorkerLease:
        """Acquire only on an unbound instance. Takeover requires expiry, never replay."""
        self._ttl(ttl_ms)
        if self._lease is not None or type(owner) is not str or not owner.strip() or len(owner) > 256:
            raise JournalLeaseError("New unbound journal instance and nonempty lease owner required")
        with self._transaction() as db:
            row = db.execute("SELECT owner,generation,expires_ms,last_seen_ms FROM worker_lease WHERE singleton=1").fetchone()
            now = self._now()
            self._clock_guard(row, now)
            if row is not None and now < row[2]:
                db.execute("UPDATE worker_lease SET last_seen_ms=? WHERE singleton=1", (now,))
                # Remember denied observations too; a later clock rollback must
                # not restore authority that has already been observed later.
                db.execute("COMMIT")
                raise JournalLeaseError("Worker lease already held; no forced takeover")
            generation = 1 if row is None else row[1] + 1
            db.execute("INSERT OR REPLACE INTO worker_lease VALUES (1,?,?,?,?)",
                       (owner, generation, now + ttl_ms, now))
            db.execute("INSERT INTO worker_events(owner,generation,kind,observed_ms) VALUES (?,?,'ACQUIRED',?)",
                       (owner, generation, now))
        self._lease = WorkerLease(owner, generation)
        return self._lease

    def bind_lease(self, lease: WorkerLease) -> None:
        """Share one trusted worker's authority across connections, never across workers."""
        if (self._lease is not None or type(lease) is not WorkerLease or
                type(lease.owner) is not str or not lease.owner or
                type(lease.generation) is not int or lease.generation < 1):
            raise JournalLeaseError("Invalid lease binding or journal already bound")
        # Bind once, even on failure: this object must not acquire newer authority.
        self._lease = lease
        with self._transaction(writer_kind="BOUND"):
            pass

    def renew_lease(self, *, ttl_ms: int = 30_000) -> None:
        self._ttl(ttl_ms)
        with self._transaction(writer_kind="RENEWED") as db:
            now = db.execute("SELECT last_seen_ms FROM worker_lease WHERE singleton=1").fetchone()[0]
            db.execute("UPDATE worker_lease SET expires_ms=? WHERE singleton=1", (now + ttl_ms,))

    def release_lease(self) -> None:
        """Stop dispatch first; release does not stop an outstanding remote request."""
        with self._transaction(writer_kind="RELEASED") as db:
            db.execute("UPDATE worker_lease SET expires_ms=last_seen_ms WHERE singleton=1")

    def worker_events(self) -> list[dict]:
        with self._transaction() as db:
            rows = db.execute("SELECT seq,owner,generation,kind,subject,observed_ms FROM worker_events ORDER BY seq").fetchall()
        return [dict(zip(("seq", "owner", "generation", "kind", "subject", "observed_ms"), row)) for row in rows]

    @staticmethod
    def _resolved(record):
        return (record["state"] in {"ABORTED", "COMPENSATED"} and
                record["compensation_state"] in {"NOT_STARTED", "SUCCEEDED"})

    @staticmethod
    def _validate_ownership(record):
        try:
            owner, idx, generation, chain = (record[key] for key in
                                             ("task_id", "step_index", "generation", "chain_id"))
            if type(chain) is not str or not chain:
                raise JournalError("Invalid chain identity")
            if owner is None:
                if idx is not None or generation is not None or chain.startswith("task:"):
                    raise JournalError("Partial or reserved task ownership")
            elif (type(owner) is not str or not owner or type(idx) is not int or idx < 0 or
                  type(generation) is not int or generation < 0 or chain != f"task:{owner}"):
                raise JournalError("Invalid task operation identity")
        except KeyError as exc:
            raise JournalError("Missing ownership fields") from exc

    @staticmethod
    def _precedes(before, before_position, after, after_position):
        if before["task_id"] is not None:
            return (before["step_index"], before["generation"]) < (after["step_index"], after["generation"])
        return before_position < after_position

    @staticmethod
    def _task_projection_guard(db, record):
        if record["task_id"] is None:
            return
        row = db.execute("SELECT checkpoint FROM task_checkpoints WHERE task_id=?", (record["task_id"],)).fetchone()
        if row is not None:
            cp = json.loads(row[0])
            idx = record["step_index"]
            if (cp["state"] in {"CANCELLED", "FAILED", "COMPLETED"} or
                    not 0 <= idx < len(cp["step_generations"]) or
                    cp["step_generations"][idx] != record["generation"]):
                raise JournalError("Task projection does not authorize this active generation")

    def _migrate_v1(self, db):
        """Offline, opt-in migration; preserve old events/keys and fence old versions."""
        self._create_ownership_tables(db)
        previous = {}
        for action_id, version, payload in db.execute("SELECT action_id,version,record FROM actions ORDER BY rowid").fetchall():
            record = json.loads(payload)
            owner = record["task_id"]
            slot = (owner, record["step_index"])
            if owner is not None:
                if (type(owner) is not str or not owner or
                        type(record["step_index"]) is not int or record["step_index"] < 0):
                    raise JournalError("Legacy action has invalid task ownership")
                prior = previous.get(slot)
                if prior is not None and not self._resolved(prior):
                    raise JournalError("Legacy task slot has unresolved overlapping operations; manual audit required")
                record["generation"] = 0 if prior is None else prior["generation"] + 1
                record["chain_id"] = f"task:{owner}"
                previous[slot] = record
            else:
                record["generation"] = None
                record["chain_id"] = f"legacy:{action_id}"
            db.execute("INSERT OR IGNORE INTO chains VALUES (?,?, 'OPEN')", (record["chain_id"], owner))
            if owner is not None:
                db.execute("INSERT INTO operation_slots VALUES (?,?,?,?)", (*slot, record["generation"], action_id))
            updated = _json(record)
            db.execute("UPDATE actions SET version=?,record=? WHERE action_id=?", (version + 1, updated, action_id))
            db.execute("INSERT INTO events(action_id,version,kind,record) VALUES (?,?,?,?)", (action_id, version + 1, "SCHEMA_V2_IDENTITY", updated))
        for task_id, version, payload in db.execute("SELECT task_id,version,checkpoint FROM task_checkpoints").fetchall():
            cp = json.loads(payload)
            cp["step_generations"] = [previous.get((task_id, idx), {}).get("generation", 0)
                                      for idx in range(len(cp["step_states"]))]
            cp["replan_from"] = 0 if cp["state"] == "REPLANNING" else None
            updated = _json(cp)
            db.execute("UPDATE task_checkpoints SET version=?,checkpoint=? WHERE task_id=?", (version + 1, updated, task_id))
            db.execute("INSERT INTO task_events(task_id,version,checkpoint) VALUES (?,?,?)", (task_id, version + 1, updated))
        db.execute("PRAGMA user_version=2")

    @contextmanager
    def _transaction(self, *, initialize=False, writer_kind=None, subject=None):
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
            observed = None
            if writer_kind is not None:
                try:
                    observed = self._require_lease(db)
                except JournalLeaseError:
                    # Only the guard has run. Commit its clock high-water mark,
                    # never action/task/chain changes or a successful write event.
                    db.execute("COMMIT")
                    raise
            yield db
            if writer_kind is not None:
                db.execute("INSERT INTO worker_events(owner,generation,kind,subject,observed_ms) VALUES (?,?,?,?,?)",
                           (self._lease.owner, self._lease.generation, writer_kind, subject, observed))
            db.execute("COMMIT")
        except PolicyError:
            # Roll back approval consumption, action/worker events and all other
            # writes. Persist only observed time so expiry cannot resurrect after
            # a clock rollback. Never commit partial authorization on denial.
            if db is not None and db.in_transaction:
                security_time = db.execute("SELECT last_seen_ms FROM security_clock WHERE singleton=1").fetchone()
                lease_time = db.execute("SELECT last_seen_ms FROM worker_lease WHERE singleton=1").fetchone()
                db.rollback()
                db.execute("BEGIN IMMEDIATE")
                if security_time is not None:
                    db.execute("UPDATE security_clock SET last_seen_ms=max(last_seen_ms,?) WHERE singleton=1", security_time)
                if lease_time is not None:
                    db.execute("UPDATE worker_lease SET last_seen_ms=max(last_seen_ms,?) WHERE singleton=1", lease_time)
                db.execute("COMMIT")
            raise
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
        self._validate_ownership(record)
        payload = _json(record)
        if record["state"] != "PENDING" or record["compensation_state"] != "NOT_STARTED":
            raise JournalError("New intent must be pending with no compensation attempt")
        with self._transaction(writer_kind="STAGED", subject=record["action_id"]) as db:
            self._task_projection_guard(db, record)
            chain = db.execute("SELECT task_id,status FROM chains WHERE chain_id=?", (record["chain_id"],)).fetchone()
            if chain is not None and (chain[0] != record["task_id"] or chain[1] != "OPEN"):
                raise JournalError("Chain ownership mismatch or chain is closed")
            for (member_json,) in db.execute("SELECT record FROM actions"):
                member = json.loads(member_json)
                if (member["chain_id"] == record["chain_id"] and
                        member["compensation_state"] in {"IN_FLIGHT", "UNKNOWN", "NOT_APPLIED"}):
                    raise JournalDuplicate("Chain repair is unresolved")
            if record["task_id"] is not None:
                slot = (record["task_id"], record["step_index"])
                prior = db.execute("SELECT s.generation,a.record FROM operation_slots s JOIN actions a ON a.action_id=s.action_id WHERE s.task_id=? AND s.step_index=? ORDER BY s.generation DESC LIMIT 1", slot).fetchone()
                if prior is not None:
                    if record["generation"] <= prior[0]:
                        raise JournalDuplicate("Task step generation already has an operation")
                    if record["generation"] != prior[0] + 1 or not self._resolved(json.loads(prior[1])):
                        raise JournalDuplicate("Prior task generation must be resolved before advancing")
                elif record["generation"] != 0:
                    raise JournalError("First task generation must be zero")
            if db.execute("SELECT 1 FROM key_claims WHERE operation_key=?", (record["idempotency_key"],)).fetchone():
                raise JournalDuplicate("Operation key is already durably claimed")
            db.execute("INSERT INTO actions VALUES (?,?,?)", (record["action_id"], 0, payload))
            if chain is None:
                db.execute("INSERT INTO chains VALUES (?,?, 'OPEN')", (record["chain_id"], record["task_id"]))
            if record["task_id"] is not None:
                db.execute("INSERT INTO operation_slots VALUES (?,?,?,?)", (record["task_id"], record["step_index"], record["generation"], record["action_id"]))
            db.execute("INSERT INTO key_claims VALUES (?,?)", (record["idempotency_key"], record["action_id"]))
            db.execute("INSERT INTO events(action_id,version,kind,record) VALUES (?,?,?,?)",
                       (record["action_id"], 0, "STAGED", payload))
        return 0

    def transition(self, record: dict, expected_version: int, kind: str, *, authorization: AuthRequest | None = None) -> int:
        return self._transition(record, expected_version, kind, authorization=authorization)[0]

    def finalize_forward(self, record: dict, expected_version: int, kind: str) -> tuple[int, str]:
        """Retain a returned/reconciled receipt; classify current intent atomically.

        A control connection can close a chain or cancel its checkpoint while
        the worker awaits a provider. Its receipt remains evidence, but cannot
        become a current commit. Lease, immutable identity and CAS still apply.
        """
        if kind not in {"FORWARD_COMMITTED", "RECONCILED_PRESENT"} or record["state"] != "COMMITTED":
            raise JournalError("Current forward result required")
        return self._transition(record, expected_version, kind, classify_forward=True)

    def _transition(self, record, expected_version, kind, *, authorization=None, classify_forward=False):
        if type(kind) is not str or not kind:
            raise JournalError("Nonempty journal event kind required")
        record = dict(record)
        self._validate_ownership(record)
        with self._transaction(writer_kind=kind, subject=record["action_id"]) as db:
            if kind in {"DISPATCH_INTENT", "FORWARD_COMMITTED", "RECONCILED_PRESENT"}:
                chain = db.execute("SELECT status FROM chains WHERE chain_id=?", (record["chain_id"],)).fetchone()
                stale = chain is None or chain[0] != "OPEN"
                if classify_forward and not stale:
                    try:
                        self._task_projection_guard(db, record)
                    except JournalError:
                        stale = True
                if stale and classify_forward:
                    record["state"] = "COMMITTED_STALE"
                    kind += "_STALE"
                elif stale:
                    raise JournalError("Chain is closed; dispatch denied")
            if kind == "DISPATCH_INTENT":
                self._task_projection_guard(db, record)
                position = db.execute("SELECT rowid FROM actions WHERE action_id=?", (record["action_id"],)).fetchone()
                if position is None:
                    raise JournalError("Unknown dispatch action")
                for rowid, member_json in db.execute("SELECT rowid,record FROM actions"):
                    member = json.loads(member_json)
                    if member["chain_id"] == record["chain_id"] and (
                            member["compensation_state"] in {"IN_FLIGHT", "UNKNOWN", "NOT_APPLIED"} or
                            (self._precedes(member, rowid, record, position[0]) and
                             member["state"] not in {"COMMITTED", "ABORTED", "COMPENSATED"})):
                        raise JournalConflict("Chain predecessor or repair is unresolved")
            if kind == "COMPENSATION_INTENT":
                position = db.execute("SELECT rowid FROM actions WHERE action_id=?", (record["action_id"],)).fetchone()
                if position is None:
                    raise JournalError("Unknown compensation action")
                for rowid, descendant_json in db.execute("SELECT rowid,record FROM actions"):
                    descendant = json.loads(descendant_json)
                    if (descendant["chain_id"] == record["chain_id"] and
                            self._precedes(record, position[0], descendant, rowid) and not self._resolved(descendant)):
                        raise JournalConflict("Descendant operation must resolve before ancestor repair")
            row = db.execute("SELECT version,record FROM actions WHERE action_id=?", (record["action_id"],)).fetchone()
            if row is None or row[0] != expected_version:
                raise JournalConflict("Action version changed; no dispatch or automatic retry")
            old = json.loads(row[1])
            if _json({key: old.get(key) for key in _IDENTITY}) != _json({key: record.get(key) for key in _IDENTITY}):
                raise JournalError("Journal action identity is immutable")
            if (record["state"] not in _TRANSITIONS[old["state"]] or
                    record["compensation_state"] not in _COMP_TRANSITIONS[old["compensation_state"]]):
                raise JournalError("Invalid journal transition")
            forward_intent = old["state"] == "PENDING" and record["state"] == "IN_FLIGHT"
            repair_intent = old["compensation_state"] == "NOT_STARTED" and record["compensation_state"] == "IN_FLIGHT"
            if forward_intent or repair_intent or kind in {"DISPATCH_INTENT", "COMPENSATION_INTENT"}:
                direction = "FORWARD" if forward_intent else "COMPENSATION"
                expected_kind = "DISPATCH_INTENT" if forward_intent else "COMPENSATION_INTENT"
                if (not (forward_intent or repair_intent) or kind != expected_kind or
                        type(authorization) is not AuthRequest or authorization.direction != direction):
                    raise PermissionDeniedError("Authenticated authorization required for dispatch intent")
                self.security.authorize(db, record, old, authorization)
            payload = _json(record)
            version = expected_version + 1
            db.execute("UPDATE actions SET version=?,record=? WHERE action_id=? AND version=?",
                       (version, payload, record["action_id"], expected_version))
            db.execute("INSERT INTO events(action_id,version,kind,record) VALUES (?,?,?,?)",
                       (record["action_id"], version, kind, payload))
            if (record["state"] in {"ABORTED", "COMPENSATED"} and
                    record["compensation_state"] in {"NOT_STARTED", "SUCCEEDED"}):
                db.execute("DELETE FROM key_claims WHERE action_id=?", (record["action_id"],))
        return version, record["state"]

    def close_chain(self, chain_id: str, owner_task_id: str | None = None) -> None:
        with self._transaction(writer_kind="CHAIN_CLOSED", subject=chain_id) as db:
            row = db.execute("SELECT status,task_id FROM chains WHERE chain_id=?", (chain_id,)).fetchone()
            if row is None:
                db.execute("INSERT INTO chains VALUES (?,?, 'CLOSED')", (chain_id, owner_task_id))
            elif row[1] != owner_task_id:
                raise JournalError("Chain owner mismatch")
            if row is None or row[0] == "OPEN":
                db.execute("UPDATE chains SET status='CLOSED' WHERE chain_id=?", (chain_id,))
                db.execute("INSERT INTO chain_events(chain_id,kind) VALUES (?, 'CLOSED_FOR_UNWIND')", (chain_id,))

    def records(self) -> list[tuple[int, dict]]:
        with self._transaction() as db:
            rows = db.execute("SELECT version,record FROM actions ORDER BY rowid").fetchall()
            return [(version, json.loads(record)) for version, record in rows]

    def closed_chains(self) -> set[str]:
        with self._transaction() as db:
            return {row[0] for row in db.execute("SELECT chain_id FROM chains WHERE status='CLOSED'")}

    def events(self, action_id: str) -> list[dict]:
        with self._transaction() as db:
            rows = db.execute("SELECT seq,version,kind,record FROM events WHERE action_id=? ORDER BY seq", (action_id,)).fetchall()
        return [{"seq": seq, "version": version, "kind": kind, "record": json.loads(record)}
                for seq, version, kind, record in rows]

    def save_task(self, task_id: str, checkpoint: dict, expected_version: int | None) -> int:
        if checkpoint.get("task_id") != task_id:
            raise JournalError("Task checkpoint identity mismatch")
        payload = _json(checkpoint)
        with self._transaction(writer_kind="TASK_CHECKPOINT", subject=task_id) as db:
            contract = checkpoint.get("workflow_contract")
            if contract is not None:
                try:
                    validate_record(contract)
                except WorkflowContractError as exc:
                    raise JournalError("Invalid workflow contract") from exc
                definitions = [{"tool_name": s["tool_name"], "kind": s["kind"]} for s in contract["steps"]]
                if checkpoint.get("step_definitions") != definitions:
                    raise JournalError("Task definitions disagree with workflow contract")
            row = db.execute("SELECT version,checkpoint FROM task_checkpoints WHERE task_id=?", (task_id,)).fetchone()
            if (row[0] if row else None) != expected_version:
                raise JournalConflict("Task checkpoint version changed")
            if row is not None and canonical(json.loads(row[1]).get("workflow_contract")) != canonical(contract):
                raise JournalError("Task workflow contract is immutable; legacy records cannot be adopted implicitly")
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
