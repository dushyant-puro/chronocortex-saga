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


def test_scorecard_cli_has_no_live_or_transport_override():
    result = subprocess.run([sys.executable, "-m", "eval.provider_faults", "--endpoint", "https://example.invalid"],
                            capture_output=True, text=True, timeout=15)
    assert result.returncode == 2
    assert "unrecognized arguments" in result.stderr
