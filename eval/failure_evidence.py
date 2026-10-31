"""Bounded, read-only SIMULATED drill observations; never recovery authority.

Only fixed counters and allowlisted protocol fields leave this module. Peers
and separate database snapshots are not a globally atomic view of an effect.
"""
from contextlib import closing
from dataclasses import dataclass, field
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import time
import uuid

from runtime.failure_diagnostics import CATEGORIES, classify_failure

CASES = ("lost_response_after_apply", "worker_killed_after_apply", "worker_killed_before_dispatch",
         "request_discarded_before_forward", "effect_hidden_from_status_reads")

EVENTS = frozenset({"READY", "PEER_FAILED", "HTTP_HANDLER_FAILED", "BARRIER_EXPIRED",
                   "REQUEST_DISCARDED_BEFORE_FORWARD", "RESPONSE_WITHHELD_AFTER_APPLY",
                   "DROP_RELEASED", "VISIBILITY_RELEASED", "STRIPE_SANDBOX_APPROVAL_REQUIRED",
                   "STRIPE_SANDBOX_DRILL_BLOCKED", "STRIPE_SANDBOX_STATUS_UNAVAILABLE",
                   "STRIPE_SANDBOX_READBACK_UNAVAILABLE", "STRIPE_SANDBOX_DRILL_STOPPED",
                   "STRIPE_SANDBOX_DRILL_FINISHED"})
STATES = frozenset({"PENDING", "IN_FLIGHT", "IN_FLIGHT_UNKNOWN", "COMMITTED", "COMMITTED_STALE",
                   "ABORTED", "COMPENSATED", "STOPPED", "RECOVERY_REQUIRED"})
WORKER_EVENTS = frozenset({"ACQUIRED", "RENEWED", "RELEASED", "HOST_STARTING", "HOST_READY",
                          "HOST_HEARTBEAT", "HOST_DRAINING", "HOST_STOPPED", "HOST_FAILED",
                          "HOST_ABANDONED", "PROVIDER_READBACK", "BOUND", "STAGED", "DISPATCH_INTENT",
                          "FORWARD_COMMITTED", "FORWARD_COMMITTED_STALE", "FORWARD_UNKNOWN",
                          "FORWARD_NOT_APPLIED", "TASK_CHECKPOINT", "CHAIN_CLOSED", "TURN_INVALIDATED",
                          "TURN_SUPERSEDED", "COMPENSATION_INTENT", "COMPENSATION_UNKNOWN"})
FAILURES = frozenset({"RENEWAL_FAILED", "MONOTONIC_DEADLINE", "AUTHORITY_LOST", "OBSERVATION_FAILED",
                     "STOP_PERSISTENCE_FAILED", "RENEWAL_WAITER_CANCELLED", "STARTUP_FAILED"})

@dataclass(frozen=True)
class CapturedEvidence:
    """Typed internal snapshot, not arbitrary data attached to an exception."""
    _encoded: str = field(repr=False)

    def data(self):
        return json.loads(self._encoded)


def enum(value, choices, default=None):
    return value if type(value) is str and value in choices else default


def peer_observation(peer):
    events = getattr(peer, "events", [])
    records = []
    for event in events[-8:] if type(events) is list else ():
        if type(event) is not dict:
            continue
        name = enum(event.get("event"), EVENTS)
        if name is not None:
            records.append({"event": name, "state": enum(event.get("state"), STATES),
                            "failure_category": enum(event.get("failure_category"), CATEGORIES),
                            "failure_code": enum(event.get("failure_code"), FAILURES)})
    code = peer.process.poll()
    return {"role": enum(getattr(peer, "role", None), {"service", "proxy", "worker", "recover"}, "UNKNOWN"),
            "exit_observed": code is not None,
            "exit_code": code if type(code) is int and code in {0, 1, 2, -9, -15} else None,
            "invalid_output": getattr(peer, "invalid_output", None) is True,
            "events": records}


def count(db, query):
    value = db.execute(query).fetchone()[0]
    if type(value) is not int or not 0 <= value <= 1_000_000_000:
        raise ValueError("Observation count exceeds capacity")
    return value


def database_observation(path, kind):
    """Fixed queries over trusted fixture files; never create/migrate a database."""
    try:
        path = Path(path).resolve(strict=True)
        with path.open("rb") as stream:
            header = stream.read(20)
        if header[:16] != b"SQLite format 3\x00" or header[18:20] != b"\x01\x01":
            return {"status": "UNAVAILABLE", "category": "UNSUPPORTED_STORAGE"}
        deadline = time.monotonic() + .25
        with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=.05,
                                     isolation_level=None)) as db:
            db.execute("PRAGMA query_only=ON")
            db.execute("PRAGMA trusted_schema=OFF")
            db.set_progress_handler(lambda: int(time.monotonic() >= deadline), 1000)
            db.execute("BEGIN")
            if kind == "journal":
                counts = {"actions": count(db, "SELECT count(*) FROM actions"),
                          "key_claims": count(db, "SELECT count(*) FROM key_claims"),
                          "dispatch_intents": count(db, "SELECT count(*) FROM events WHERE kind='DISPATCH_INTENT'")}
                states = [enum(row[0], STATES, "OTHER") for row in db.execute(
                    "SELECT json_extract(record,'$.state') FROM actions ORDER BY rowid LIMIT 16")]
                workers = [enum(row[0], WORKER_EVENTS, "OTHER") for row in db.execute(
                    "SELECT kind FROM worker_events ORDER BY seq DESC LIMIT 16")]
                result = {"counts": counts, "action_state_sample": states, "recent_worker_events": workers}
            else:
                result = {"counts": {"requests": count(db, "SELECT count(*) FROM requests"),
                                     "posts": count(db, "SELECT count(*) FROM requests WHERE method='POST'"),
                                     "effects": count(db, "SELECT count(*) FROM effects")}}
            db.rollback()
            return {"status": "OBSERVED", **result}
    except (OSError, sqlite3.Error, ValueError, TypeError) as error:
        return {"status": "UNAVAILABLE", "category": classify_failure(error)}


def snapshot(root, peers):
    root = Path(root)
    evidence = {"schema": 1, "label": "SIMULATED", "before_peer_cleanup": True,
            "consistency": "SEPARATE_OBSERVATIONS_NOT_ATOMIC", "dispatch_authorized": False,
            "retry_authorized": False, "root_cause_confirmed": False,
            "peers": [peer_observation(peer) for peer in peers[:8]],
            "journal": database_observation(root / "ccs.db", "journal"),
            "proxy": database_observation(root / "proxy.db", "ledger"),
            "provider": database_observation(root / "provider.db", "ledger")}
    encoded = json.dumps(evidence, sort_keys=True, allow_nan=False)
    if len(encoded.encode("ascii")) > 16384:
        raise ValueError("Observation exceeds capacity")
    return CapturedEvidence(encoded)


def publish(directory, case, evidence):
    """New redacted artifact only; no raw fixture directories or overwrite."""
    path = Path(directory)
    path.mkdir(parents=True, exist_ok=True)
    if type(evidence) is not CapturedEvidence:
        raise TypeError("Captured observation required")
    raw = json.dumps({"case": enum(case, CASES, "UNKNOWN_CASE"), **evidence.data()},
                     sort_keys=True, allow_nan=False).encode("ascii")
    if len(raw) > 16384:
        raise ValueError("Failure evidence exceeds capacity")
    target = path / ("failure-" + uuid.uuid4().hex + ".json")
    with tempfile.NamedTemporaryFile(mode="wb", dir=path, suffix=".partial", delete=False) as stream:
        pending = Path(stream.name)
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    os.link(pending, target)  # Atomic no-clobber publication; no overwrite fallback.
    pending.unlink()  # Failed/partial captures remain unadvertised as .partial.
