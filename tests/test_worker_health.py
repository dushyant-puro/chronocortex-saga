"""Health is observed evidence, independent of lease expiry or remote effects."""
from contextlib import closing
import json
import sqlite3

import pytest
from starlette.testclient import TestClient

from control.app import create_app, ControlConfig
from runtime.action_journal import SQLiteActionJournal
from runtime.worker_health import worker_view
from tests.control_test_support import Clock, seed_control


SUMMARY = {"phase": "READY", "active_operations": 0, "failure_code": None, "heartbeat_ttl_ms": 11000}


@pytest.fixture
def journal(tmp_path):
    return SQLiteActionJournal(tmp_path / "health.db", clock_ms=lambda: 1000)


def read(journal, now=1000):
    with journal._transaction() as db:
        return worker_view(db, now)


def test_opening_a_journal_or_manually_holding_a_lease_does_not_claim_supervision(journal):
    assert read(journal)["readiness"] == "NOT_READY"
    journal.acquire_lease("SIMULATED-manual-worker")
    view = read(journal)
    assert view["state"] == "LEASE_CURRENT"
    assert view["heartbeat"] == "NOT_OBSERVED" and view["readiness"] == "NOT_READY"


def test_fresh_observation_can_be_stale_while_lease_is_still_current(journal):
    journal.acquire_lease("SIMULATED-supervised-worker")
    journal.observe_worker("HOST_HEARTBEAT", SUMMARY)
    assert read(journal)["readiness"] == "READY_REPORTED"
    view = read(journal, 12000)
    assert view["state"] == "LEASE_CURRENT" and view["heartbeat"] == "STALE"
    assert view["readiness"] == "NOT_READY"


def test_fresh_heartbeat_cannot_outlive_lease_authority(journal):
    journal.acquire_lease("SIMULATED-worker")
    journal.observe_worker("HOST_READY", {**SUMMARY, "heartbeat_ttl_ms": 60000})
    view = read(journal, 31000)
    assert view["heartbeat"] == "FRESH" and view["state"] == "LEASE_EXPIRED"
    assert view["readiness"] == "NOT_READY"


@pytest.mark.parametrize("phase", ["STARTING", "DRAINING", "FAILED", "STOPPED", "RECOVERY_REQUIRED"])
def test_nonready_phases_never_report_ready(journal, phase):
    journal.acquire_lease("SIMULATED-worker")
    journal.observe_worker("HOST_HEARTBEAT", {**SUMMARY, "phase": phase})
    assert read(journal)["readiness"] == "NOT_READY"


@pytest.mark.parametrize("change", [{"active_operations": True}, {"active_operations": -1}, {"phase": []}, {"heartbeat_ttl_ms": True}, {"heartbeat_ttl_ms": 0}, {"failure_code": "<script>"}, {"extra": 1}])
def test_malformed_observations_cannot_claim_readiness(journal, change):
    journal.acquire_lease("SIMULATED-worker")
    journal.observe_worker("HOST_READY", {**SUMMARY, **change})
    view = read(journal)
    assert view["heartbeat"] == "INVALID_OBSERVATION"
    assert view["readiness"] == "NOT_READY" and view["supervision"] is None


def test_old_generation_observation_cannot_describe_a_replacement(journal):
    journal.acquire_lease("same-worker-name")
    journal.observe_worker("HOST_READY", SUMMARY)
    journal.release_lease()
    replacement = SQLiteActionJournal(journal.path, clock_ms=lambda: 1000)
    replacement.acquire_lease("same-worker-name")
    view = read(replacement)
    assert view["generation"] == 2 and view["heartbeat"] == "NOT_OBSERVED"


def test_clock_rollback_and_observed_failure_cannot_report_ready(journal):
    journal.acquire_lease("SIMULATED-worker")
    journal.observe_worker("HOST_READY", SUMMARY)
    assert read(journal, 999)["heartbeat"] == "CLOCK_UNTRUSTED"
    assert read(journal, 999)["readiness"] == "NOT_READY"
    journal.observe_worker("HOST_FAILED", {**SUMMARY, "failure_code": "RENEWAL_FAILED"})
    assert read(journal)["readiness"] == "NOT_READY"


def test_stopped_or_failed_event_cannot_claim_ready_through_a_conflicting_summary(journal):
    journal.acquire_lease("SIMULATED-worker")
    for kind in ("HOST_STOPPED", "HOST_ABANDONED", "HOST_FAILED"):
        journal.observe_worker(kind, SUMMARY)
        assert read(journal)["readiness"] == "NOT_READY"


def test_control_snapshot_reads_same_transaction_supervision_evidence(tmp_path):
    env = seed_control(tmp_path / "console.db", Clock())
    env.journal.observe_worker("HOST_READY", SUMMARY)
    origin = "http://127.0.0.1:8877"
    with TestClient(create_app(env.journal, ControlConfig(origin)), base_url=origin) as client:
        assert client.get("/api/snapshot").status_code == 401
        response = client.post("/api/login", json={"credential": env.operator.token}, headers={"Origin": origin})
        assert response.status_code == 200
        worker = client.get("/api/snapshot").json()["worker"]
        assert worker["readiness"] == "READY_REPORTED"
        assert worker["supervision"]["active_operations"] == 0
        env.clock.now += 11000
        worker = client.get("/api/snapshot").json()["worker"]
        assert worker["readiness"] == "NOT_READY" and worker["state"] == "LEASE_CURRENT"
