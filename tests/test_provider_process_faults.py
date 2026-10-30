"""Real subprocess/TCP acceptance plus adversarial evidence/egress checks.

Every remote effect is SIMULATED. No network service or real key is required.
"""
import copy
import json
import subprocess
import sys

import httpx2 as httpx
import pytest

from eval import provider_faults as evaluation
from eval.provider_fault_peer import ACCOUNT, INTENT, KEY, LoopbackTransport, factory, port
from providers.stripe_sandbox import API_VERSION, CONTRACT, StripeSandboxConfig


@pytest.mark.parametrize("name", evaluation.CASES)
def test_independent_process_and_tcp_fault_acceptance(tmp_path, name):
    result = evaluation.run_case(tmp_path / name, name)
    assert result["passed"], {k: v for k, v in result["checks"].items() if not v}
    evidence = result["evidence"]
    assert set(evidence["recovery_http_methods"]) == {"GET"}
    assert evidence["provider_post_requests"] <= 1
    assert evidence["proxy_post_attempts"] <= 1
    if name == "worker_killed_before_dispatch":
        assert evidence["durable_state"] == "PENDING"
        assert evidence["first_recovery_projection"] == "IN_FLIGHT_UNKNOWN"
    if name == "effect_hidden_from_status_reads":
        assert evidence["provider_effects"] == 1
        assert evidence["first_recovery_projection"] == "IN_FLIGHT_UNKNOWN"
        assert evidence["durable_state"] == "COMMITTED_STALE"


@pytest.mark.parametrize("value", [True, 0, -1, 65536, "443", None])
def test_loopback_target_requires_exact_bounded_port(value):
    with pytest.raises(ValueError):
        port(value)


@pytest.mark.asyncio
@pytest.mark.parametrize("url,method,key", [
    ("https://evil.example/v1/account", "GET", KEY),
    ("http://api.stripe.com/v1/account", "GET", KEY),
    ("https://api.stripe.com:444/v1/account", "GET", KEY),
    ("https://api.stripe.com/v1/account?redirect=https://evil.example", "GET", KEY),
    ("https://api.stripe.com/v1/payment_intents", "POST", KEY),
    ("https://api.stripe.com/v1/account", "DELETE", KEY),
    ("https://api.stripe.com/v1/account", "GET", "rk_test_" + "different" * 3),
])
async def test_simulation_transport_cannot_forward_external_or_unscoped_request(url, method, key):
    # Rejected before the inner transport can perform any socket operation.
    transport = LoopbackTransport(1)
    class Forbidden:
        async def handle_async_request(self, _):
            pytest.fail("Rejected simulation scope reached TCP")
        async def aclose(self):
            pass
    await transport.inner.aclose()
    transport.inner = Forbidden()
    try:
        with pytest.raises(ValueError, match="SIMULATED scope"):
            await transport.handle_async_request(httpx.Request(method, url, headers={"Authorization": "Bearer " + key}))
    finally:
        await transport.aclose()


def test_real_key_or_account_cannot_be_used_by_simulated_factory():
    with pytest.raises(ValueError, match="fake identities"):
        factory(1)(StripeSandboxConfig(ACCOUNT, "rk_test_" + "different" * 3))
    with pytest.raises(ValueError, match="fake identities"):
        factory(1)(StripeSandboxConfig("acct_Different12345", KEY))


def oracle():
    record = {"state": "COMMITTED_STALE", "idempotency_key": "operation-1",
              "result": {"observed_via": "GET", "account_id": ACCOUNT, "payment_intent_id": INTENT,
                         "provider": "stripe", "mode": "test", "provider_contract": CONTRACT, "api_version": API_VERSION,
                         "status": "canceled", "effect": "terminal_cancellation", "canceled_at": 1700000000,
                         "idempotency_key": "operation-1", "actor_attribution": "not_claimed"}}
    requests = [(1, "POST", "/v1/payment_intents/" + INTENT + "/cancel", "operation-1")]
    return record, requests, copy.deepcopy(requests), [(1, "operation-1")]


@pytest.mark.parametrize("corrupt,failed_check", [
    ("duplicate_request", "provider_post_count"), ("duplicate_effect", "independent_effect_count"),
    ("wrong_wire_key", "wire_operation_identity_matches_journal"),
    ("wrong_effect_key", "effect_identity_matches_wire"),
    ("wrong_receipt_scope", "receipt_requires_positive_terminal_evidence"),
    ("open_receipt", "receipt_requires_positive_terminal_evidence"),
    ("invented_receipt_on_unknown", "receipt_requires_positive_terminal_evidence"),
    ("inverse_request", "no_inverse_or_unscoped_requests"),
])
def test_independent_oracle_rejects_fabricated_assurance(corrupt, failed_check):
    record, requests, proxy, effects = oracle()
    state = "COMMITTED_STALE"
    if corrupt == "duplicate_request":
        requests.append(requests[0])
    elif corrupt == "duplicate_effect":
        effects.append(effects[0])
    elif corrupt == "wrong_wire_key":
        requests[0] = (1, "POST", requests[0][2], "operation-OTHER")
    elif corrupt == "wrong_effect_key":
        effects[0] = (1, "operation-OTHER")
    elif corrupt == "wrong_receipt_scope":
        record["result"]["payment_intent_id"] = "pi_OTHER123456789"
    elif corrupt == "open_receipt":
        record["result"]["status"] = "requires_payment_method"
    elif corrupt == "invented_receipt_on_unknown":
        state = record["state"] = "IN_FLIGHT_UNKNOWN"
    elif corrupt == "inverse_request":
        requests.append((2, "POST", "/v1/refunds", "operation-1"))
    checks = evaluation.validate_evidence(record, requests, proxy, effects, expected_state=state,
                                          expected_posts=1, expected_effects=1)
    assert checks[failed_check] is False


def test_incomplete_drills_are_failures_with_no_exception_payload(monkeypatch):
    def fail(*_):
        raise ValueError("PRIVATE EXCEPTION PAYLOAD")
    monkeypatch.setattr(evaluation, "run_case", fail)
    report = evaluation.score()
    assert report["cases_failed"] == len(evaluation.CASES)
    assert report["cases_passed"] == 0 and report["checks_passed"] == 0
    assert report["checks_failed"] == len(evaluation.CASES)
    assert "PRIVATE" not in json.dumps(report)
    assert report["real_provider"] == "NOT_RUN" and report["label"] == "SIMULATED"


def test_incomplete_drill_retains_native_category_without_private_payload(monkeypatch):
    import sqlite3
    from runtime.action_journal import JournalError
    def fail(*_):
        cause = sqlite3.OperationalError("PRIVATE SQLITE QUERY AND PATH")
        cause.sqlite_errorcode = sqlite3.SQLITE_BUSY
        raise JournalError("PRIVATE CREDENTIAL") from cause
    monkeypatch.setattr(evaluation, "run_case", fail)
    report = evaluation.score()
    assert report["cases_failed"] == 5 and report["checks_failed"] == 5
    assert all(c["diagnostic"] == {"category": "SQLITE_BUSY"} and c["failure"] == "INCOMPLETE_DRILL" for c in report["cases"])
    assert "PRIVATE" not in json.dumps(report)


@pytest.mark.parametrize("code", ["PRIVATE SECRET CODE", True, ["PRIVATE"]])
def test_drill_failure_constructor_never_accepts_arbitrary_diagnostic_text(code):
    failure = evaluation.DrillFailure(code, role="PRIVATE ROLE", wait="PRIVATE WAIT", event="PRIVATE EVENT", category="PRIVATE CAUSE")
    assert str(failure) == "INCOMPLETE_DRILL"
    assert failure.diagnostic == {"category": "UNCLASSIFIED", "peer_role": "UNKNOWN", "wait": "UNKNOWN", "expected_event": None}
    assert "PRIVATE" not in json.dumps(failure.diagnostic)


@pytest.mark.parametrize("category,expected", [("SQLITE_BUSY", "SQLITE_BUSY"), ("PRIVATE PEER TEXT", "UNCLASSIFIED"), (True, "UNCLASSIFIED")])
def test_peer_failure_transmits_only_allowlisted_category(category, expected, caplog):
    import queue
    peer = object.__new__(evaluation.Peer)
    peer.role = "worker"
    peer.messages = queue.Queue()
    peer.messages.put({"event": "PEER_FAILED", "failure_category": category, "raw_exception": "PRIVATE EXCEPTION"})
    with pytest.raises(evaluation.DrillFailure) as failure:
        peer.receive("READY")
    assert failure.value.code == "PEER_ENDED_BEFORE_BARRIER"
    assert failure.value.diagnostic == {"category": expected, "peer_role": "worker", "wait": "BARRIER", "expected_event": "READY"}
    assert "PRIVATE" not in json.dumps(failure.value.diagnostic)
    assert "PRIVATE" not in caplog.text
    assert "SIMULATED peer failure" in caplog.text


def test_peer_exit_timeout_reports_role_and_wait_without_process_arguments(caplog):
    from types import SimpleNamespace
    def expired(**kwargs):
        assert kwargs == {"timeout": 20}  # Existing acceptance budget is unchanged.
        raise subprocess.TimeoutExpired("PRIVATE PROCESS ARGUMENTS", 20)
    peer = object.__new__(evaluation.Peer)
    peer.role = "recover"
    peer.process = SimpleNamespace(wait=expired)
    with pytest.raises(evaluation.DrillFailure) as failure:
        peer.finish(0)
    assert failure.value.code == "PEER_EXIT_TIMEOUT"
    assert failure.value.diagnostic == {"category": "TIMEOUT", "peer_role": "recover", "wait": "EXIT", "expected_event": None}
    assert "PRIVATE" not in str(failure.value)
    assert "'peer_role': 'recover'" in caplog.text and "'wait': 'EXIT'" in caplog.text
    assert "PRIVATE" not in caplog.text


def test_peer_diagnostic_sink_failure_preserves_timeout(monkeypatch):
    def broken(*_, **__): raise RuntimeError("PRIVATE LOGGER PAYLOAD")
    monkeypatch.setattr(evaluation.logger, "error", broken)
    peer = object.__new__(evaluation.Peer); peer.role = "recover"
    failure = peer.failure("PEER_EXIT_TIMEOUT", wait="EXIT", category="TIMEOUT")
    assert failure.code == "PEER_EXIT_TIMEOUT" and failure.diagnostic["category"] == "TIMEOUT"


def test_real_peer_exception_returns_only_a_safe_category(tmp_path):
    private_path = tmp_path / "PRIVATE_LEDGER_PATH"
    private_path.mkdir()  # SQLite cannot open an existing directory as a database.
    result = subprocess.run([sys.executable, "-m", "eval.provider_fault_peer", "service", "--ledger", str(private_path)],
                            capture_output=True, text=True, timeout=20)
    assert result.returncode == 1
    assert json.loads(result.stdout) == {"event": "PEER_FAILED", "source": "SIMULATED", "failure_category": "SQLITE_OTHER"}
    assert "PRIVATE" not in result.stdout + result.stderr


def test_http_handler_failure_reports_current_cause_without_request_details(monkeypatch):
    import sqlite3
    from eval import provider_fault_peer as peer
    events = []
    monkeypatch.setattr(peer, "emit", lambda event, **fields: events.append({"event": event, **fields}))
    error = sqlite3.OperationalError("PRIVATE SQL AND REQUEST")
    error.sqlite_errorcode = sqlite3.SQLITE_BUSY
    try:
        raise error
    except sqlite3.Error:
        peer.Server.handle_error(None, "PRIVATE REQUEST HEADERS")
    assert events == [{"event": "HTTP_HANDLER_FAILED", "failure_category": "SQLITE_BUSY"}]
    assert "PRIVATE" not in json.dumps(events)


def test_scorecard_cli_has_no_live_or_transport_override():
    result = subprocess.run([sys.executable, "-m", "eval.provider_faults", "--endpoint", "https://example.invalid"],
                            capture_output=True, text=True, timeout=15)
    assert result.returncode == 2
    assert "unrecognized arguments" in result.stderr


def test_unavailable_git_cannot_claim_clean_source(monkeypatch):
    def unavailable(*args, **kwargs):
        raise FileNotFoundError()
    monkeypatch.setattr(evaluation.subprocess, "run", unavailable)
    identity = evaluation.provenance()
    assert identity["source_commit"] is None and identity["source_dirty"] is None
    assert identity["python"] and identity["sqlite"]


def test_uncommitted_source_is_explicit_in_provenance(monkeypatch):
    from types import SimpleNamespace
    results = iter([SimpleNamespace(returncode=0, stdout="a" * 40 + "\n"),
                    SimpleNamespace(returncode=0, stdout=" M runtime/speculative_saga.py\n")])
    monkeypatch.setattr(evaluation.subprocess, "run", lambda *a, **kw: next(results))
    identity = evaluation.provenance()
    assert identity["source_commit"] == "a" * 40 and identity["source_dirty"] is True
