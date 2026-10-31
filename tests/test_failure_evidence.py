"""Strictly redacted pre-cleanup observations, never remote-effect authority."""
from contextlib import closing, ExitStack
import json
import sqlite3
from types import SimpleNamespace

import pytest

from eval import failure_evidence as capture, provider_faults as drill
from eval.provider_fault_peer import initialize


def peer(events, **kwargs):
    return SimpleNamespace(role=kwargs.get("role", "worker"), events=events,
                           invalid_output=kwargs.get("invalid", False), errors=["PRIVATE STDERR"],
                           process=SimpleNamespace(poll=lambda: kwargs.get("code")))


def test_redaction_includes_other_peer_and_ledger_observations_without_private_data(tmp_path):
    journal = drill.setup(tmp_path / "case")
    root = journal.path.parent
    initialize(root / "provider.db")
    initialize(root / "proxy.db")
    with closing(sqlite3.connect(root / "provider.db")) as db:
        db.execute("INSERT INTO effects(operation_key) VALUES ('PRIVATE OPERATION')")
        db.commit()
    events = [{"event": "PEER_FAILED", "failure_category": "SQLITE_BUSY", "raw": "PRIVATE ERROR"},
              {"event": "STRIPE_SANDBOX_DRILL_STOPPED", "failure_code": "RENEWAL_FAILED",
               "state": "RECOVERY_REQUIRED", "credential": "PRIVATE TOKEN"},
              {"event": "PRIVATE EVENT", "state": "PRIVATE STATE"}]
    before = {p.name: p.read_bytes() for p in root.iterdir() if p.is_file()}
    evidence = capture.snapshot(root, [peer(events, code=1)]).data()
    assert evidence["provider"]["counts"]["effects"] == 1
    assert evidence["journal"]["counts"] == {"actions": 0, "key_claims": 0, "dispatch_intents": 0}
    assert evidence["peers"][0]["events"][0]["failure_category"] == "SQLITE_BUSY"
    assert evidence["peers"][0]["events"][1]["failure_code"] == "RENEWAL_FAILED"
    assert evidence["before_peer_cleanup"] and not evidence["retry_authorized"]
    assert not evidence["root_cause_confirmed"] and "PRIVATE" not in json.dumps(evidence)
    assert before == {p.name: p.read_bytes() for p in root.iterdir() if p.is_file()}


def test_missing_locked_and_wal_databases_are_unavailable_not_zero_effects(tmp_path):
    missing = tmp_path / "missing.db"
    assert capture.database_observation(missing, "ledger")["status"] == "UNAVAILABLE"
    assert not missing.exists()
    path = tmp_path / "ledger.db"
    initialize(path)
    with closing(sqlite3.connect(path, isolation_level=None)) as db:
        db.execute("BEGIN EXCLUSIVE")
        evidence = capture.database_observation(path, "ledger")
        assert evidence == {"status": "UNAVAILABLE", "category": "SQLITE_BUSY"}
        assert "counts" not in evidence
        db.rollback()
        db.execute("PRAGMA journal_mode=WAL")
    before = {p.name for p in tmp_path.iterdir()}
    assert capture.database_observation(path, "ledger")["category"] == "UNSUPPORTED_STORAGE"
    assert {p.name for p in tmp_path.iterdir()} == before


def test_unknown_events_exit_codes_and_large_peer_sets_are_bounded(tmp_path):
    events = [{"event": "READY", "failure_category": ["PRIVATE"], "state": "PRIVATE"}] * 128
    evidence = capture.snapshot(tmp_path, [peer(events, role="PRIVATE", code=999)] * 64).data()
    assert len(evidence["peers"]) == 8
    assert all(len(p["events"]) == 8 and p["role"] == "UNKNOWN" and p["exit_code"] is None
               for p in evidence["peers"])
    assert "PRIVATE" not in json.dumps(evidence)


def test_failure_observed_before_peer_cleanup_and_original_exception_is_preserved(tmp_path):
    observed = peer([{"event": "READY"}])
    failure = drill.DrillFailure("PEER_BARRIER_TIMEOUT", role="proxy", wait="BARRIER")
    output = tmp_path / "published"
    with pytest.raises(drill.DrillFailure) as caught:
        with ExitStack() as stack, drill.failure_boundary(tmp_path, drill.CASES[-1], [observed], output):
            stack.callback(observed.events.append, {"event": "DROP_RELEASED"})
            raise failure
    assert caught.value is failure
    artifact = json.loads(next(output.glob("*.json")).read_text(encoding="ascii"))
    assert [r["event"] for r in artifact["peers"][0]["events"]] == ["READY"]
    assert observed.events[-1]["event"] == "DROP_RELEASED"


@pytest.mark.parametrize("sink", ["snapshot", "publish", "logger"])
def test_failed_diagnostics_cannot_replace_original_error(tmp_path, monkeypatch, sink):
    failure = ValueError("PRIVATE ORIGINAL")
    def broken(*_, **__):
        raise OSError("PRIVATE SINK ERROR")
    if sink == "logger":
        monkeypatch.setattr(drill.logger, "error", broken)
    else:
        monkeypatch.setattr(capture, sink, broken)
    output = tmp_path / "published"
    with pytest.raises(ValueError) as caught:
        with drill.failure_boundary(tmp_path, drill.CASES[0], [], output):
            raise failure
    assert caught.value is failure
    if sink == "logger":
        assert len(list(output.glob("*.json"))) == 1


def test_publication_never_clobbers_existing_evidence_or_publishes_partial_write(tmp_path, monkeypatch):
    evidence = capture.snapshot(tmp_path, [])
    monkeypatch.setattr(capture.uuid, "uuid4", lambda: SimpleNamespace(hex="fixed"))
    capture.publish(tmp_path, "PRIVATE CASE", evidence)
    target = tmp_path / "failure-fixed.json"
    original = target.read_bytes()
    assert json.loads(original)["case"] == "UNKNOWN_CASE"
    with pytest.raises(FileExistsError):
        capture.publish(tmp_path, drill.CASES[0], evidence)
    assert target.read_bytes() == original
    def failed_flush(*_):
        raise OSError("PRIVATE DISK FAILURE")
    monkeypatch.setattr(capture.os, "fsync", failed_flush)
    output = tmp_path / "failed"
    with pytest.raises(OSError):
        capture.publish(output, drill.CASES[0], evidence)
    assert not list(output.glob("*.json")) and list(output.glob("*.partial"))


def test_scorecard_keeps_captured_evidence_but_rejects_arbitrary_exception_data(tmp_path, monkeypatch):
    def failed(*_):
        with drill.failure_boundary(tmp_path, drill.CASES[0], [], None):
            raise drill.DrillFailure("PEER_BARRIER_TIMEOUT")
    monkeypatch.setattr(drill, "run_case", failed)
    report = drill.score()
    assert report["cases_failed"] == 5
    assert all(c["failure_evidence"]["label"] == "SIMULATED" for c in report["cases"])
    def private(*_):
        error = ValueError("PRIVATE")
        error.failure_evidence = {"PRIVATE": "NEVER SERIALIZE"}
        raise error
    monkeypatch.setattr(drill, "run_case", private)
    assert "PRIVATE" not in json.dumps(drill.score())
