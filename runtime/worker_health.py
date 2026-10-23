"""Read-only projections of lease validity and bounded host observations.

Fresh evidence is not proof that a process is alive or a provider is healthy.
"""
import json
import re


PHASES = {"STARTING", "READY", "DRAINING", "FAILED", "STOPPED", "RECOVERY_REQUIRED"}
KINDS = {"HOST_STARTING", "HOST_READY", "HOST_HEARTBEAT", "HOST_DRAINING", "HOST_STOPPED", "HOST_FAILED", "HOST_ABANDONED"}


def worker_view(db, now):
    lease = db.execute("SELECT owner,generation,expires_ms,last_seen_ms FROM worker_lease WHERE singleton=1").fetchone()
    if lease is None:
        return {"state": "NO_LEASE", "heartbeat": "NOT_OBSERVED", "readiness": "NOT_READY", "supervision": None}
    clock_ok = now >= lease[3]
    result = {"owner": lease[0], "generation": lease[1], "expires_ms": lease[2],
              "state": "CLOCK_UNTRUSTED" if not clock_ok else "LEASE_CURRENT" if now < lease[2] else "LEASE_EXPIRED",
              "heartbeat": "NOT_OBSERVED", "readiness": "NOT_READY", "supervision": None}
    row = db.execute("SELECT kind,subject,observed_ms FROM worker_events WHERE owner=? AND generation=? AND kind GLOB 'HOST_*' ORDER BY seq DESC LIMIT 1", lease[:2]).fetchone()
    if row is None:
        return result
    try:
        summary = json.loads(row[1])
        if (row[0] not in KINDS or type(summary) is not dict or set(summary) != {"phase", "active_operations", "failure_code", "heartbeat_ttl_ms"}
                or summary["phase"] not in PHASES
                or type(summary["active_operations"]) is not int or not 0 <= summary["active_operations"] <= 1024
                or type(summary["heartbeat_ttl_ms"]) is not int or not 1 <= summary["heartbeat_ttl_ms"] <= 86_400_000
                or (summary["failure_code"] is not None and (type(summary["failure_code"]) is not str or re.fullmatch(r"[A-Z_]{1,64}", summary["failure_code"]) is None))
                or type(row[2]) is not int or row[2] < 0):
            raise ValueError()
        age = now - row[2]
        result["supervision"] = {**summary, "kind": row[0], "observed_ms": row[2], "age_ms": max(0, age)}
        result["heartbeat"] = "CLOCK_UNTRUSTED" if age < 0 or not clock_ok else "FRESH" if age < summary["heartbeat_ttl_ms"] else "STALE"
        if (result["state"] == "LEASE_CURRENT" and result["heartbeat"] == "FRESH"
                and row[0] in {"HOST_READY", "HOST_HEARTBEAT"}
                and summary["phase"] == "READY" and summary["failure_code"] is None):
            result["readiness"] = "READY_REPORTED"
    except (ValueError, TypeError, KeyError, RecursionError):
        result["heartbeat"] = "INVALID_OBSERVATION"
        result["supervision"] = None
    return result
