"""GET-only setup checks with SIMULATED Stripe responses; no provider credentials."""
import asyncio
import json
import subprocess
import sys
from types import SimpleNamespace

import httpx2 as httpx
import pytest

from providers import preflight_stripe as command
from providers.stripe_sandbox import API_VERSION, StripeSandboxCancellation, ProviderUnavailable, ProviderProtocolError
from runtime.credential_input import CredentialInputError
from test_stripe_reconciler import secret
from test_stripe_sandbox import adapter, ACCOUNT, INTENT, KEY, account, intent, response


def arguments(tmp_path, payment_intent_id=None):
    return SimpleNamespace(account_id=ACCOUNT, payment_intent_id=payment_intent_id,
                           stripe_key_file=secret(tmp_path, "stripe.key", KEY))


def simulated(handler):
    return lambda config: StripeSandboxCancellation(config, transport=httpx.MockTransport(handler))


@pytest.mark.asyncio
async def test_account_only_has_one_get_no_journal_and_no_object_or_write_claim(tmp_path, monkeypatch):
    args = arguments(tmp_path)
    before = set(tmp_path.iterdir())
    calls = []
    monkeypatch.setenv("HTTPS_PROXY", "http://attacker.invalid")
    monkeypatch.setenv("STRIPE_SECRET_KEY", "sk_live_MUST_NOT_BE_USED")
    def handle(request):
        calls.append((request.method, str(request.url)))
        assert request.headers["authorization"] == "Bearer " + KEY
        assert request.headers["stripe-version"] == API_VERSION
        assert "stripe-account" not in request.headers and "idempotency-key" not in request.headers
        return account()
    result = await command.run(args, provider_factory=simulated(handle))
    assert calls == [("GET", "https://api.stripe.com/v1/account")]
    assert result == {"event": "STRIPE_SANDBOX_PREFLIGHT_COMPLETED", "source": "SIMULATED",
                      "account_id": ACCOUNT, "api_version": API_VERSION,
                      "account_request_id": "req_Simulated12345", "object_mode_verified": False,
                      "payment_intent": None, "mutation_attempted": False,
                      "write_permission_verified": False, "journal_updated": False}
    assert set(tmp_path.iterdir()) == before
    assert KEY not in json.dumps(result) and "PRIVATE:" not in json.dumps(result)


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["requires_payment_method", "requires_confirmation", "requires_action",
                                   "processing", "requires_capture", "canceled", "succeeded"])
async def test_known_test_object_is_only_a_read_observation_for_every_status(tmp_path, status):
    args = arguments(tmp_path, INTENT)
    calls = []
    def handle(request):
        calls.append((request.method, request.url.path))
        assert request.method == "GET"
        return account() if request.url.path == "/v1/account" else response(intent(status=status))
    result = await command.run(args, provider_factory=simulated(handle))
    assert calls == [("GET", "/v1/account"), ("GET", "/v1/payment_intents/" + INTENT)]
    assert result["object_mode_verified"] is True
    assert result["payment_intent"] == {"id": INTENT, "status": status, "livemode": False,
                                        "request_id": "req_Simulated12345"}
    assert result["journal_updated"] is False and result["write_permission_verified"] is False
    assert not {"effect", "idempotency_key", "action_id", "receipt"} & result.keys()
    assert KEY not in json.dumps(result) and "PRIVATE:" not in json.dumps(result)


@pytest.mark.asyncio
@pytest.mark.parametrize("bad", ["pi_../different", INTENT + "/cancel", INTENT + "?key=" + KEY,
                                INTENT + "\n", "", 17, True])
async def test_invalid_object_scope_is_rejected_before_account_http(bad):
    async with adapter(lambda _: pytest.fail("invalid scope performed HTTP")) as provider:
        with pytest.raises(ValueError):
            await provider.preflight(bad)


@pytest.mark.asyncio
async def test_wrong_account_stops_before_object_read():
    calls = []
    def handle(request):
        calls.append(request.url.path)
        return response({"object": "account", "id": "acct_Different1234", "secret": KEY})
    async with adapter(handle) as provider:
        with pytest.raises(ProviderProtocolError, match="STRIPE_ACCOUNT_MISMATCH"):
            await provider.preflight(INTENT)
    assert calls == ["/v1/account"]


@pytest.mark.asyncio
@pytest.mark.parametrize("updates", [{"livemode": True}, {"livemode": 0}, {"livemode": None},
                                    {"object": "customer"}, {"id": "pi_Different1234"},
                                    {"status": "new_or_unknown"}, {"status": None},
                                    {"canceled_at": True}, {"canceled_at": 0},
                                    {"amount_capturable": False}, {"amount_capturable": 100}])
async def test_wrong_mode_scope_or_terminal_evidence_never_passes_preflight(updates):
    calls = []
    def handle(request):
        calls.append(request.method)
        return account() if request.url.path == "/v1/account" else response(intent(**updates))
    async with adapter(handle) as provider:
        with pytest.raises(ProviderProtocolError):
            await provider.preflight(INTENT)
    assert calls == ["GET", "GET"]


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["account", "object"])
@pytest.mark.parametrize("status", [301, 401, 403, 404, 409, 429, 500, 503])
async def test_failed_or_redirected_get_is_not_retried_or_upgraded(stage, status):
    calls = []
    def handle(request):
        calls.append((request.method, request.url.path))
        if stage == "object" and request.url.path == "/v1/account":
            return account()
        return response({"private": KEY}, status=status, headers={"location": "https://attacker.invalid"})
    async with adapter(handle) as provider:
        with pytest.raises(ProviderUnavailable, match="STRIPE_HTTP_" + str(status)):
            await provider.preflight(INTENT)
    assert len(calls) == (1 if stage == "account" else 2)
    assert all(method == "GET" for method, _ in calls)


@pytest.mark.asyncio
@pytest.mark.parametrize("bad_response", [
    {"raw": b'{"object":"account","object":"account"}'},
    {"raw": b'{"private":NaN}'}, {"raw": b"[]"},
    {"headers": {"stripe-version": "incompatible"}},
    {"headers": {"content-encoding": "gzip"}},
    {"headers": {"request-id": "req_bad\n" + KEY}},
    {"chunks": [b"x" * 32768, b"x" * 32769]},
])
async def test_preflight_retains_bounded_strict_protocol_validation(bad_response):
    async with adapter(lambda _: response(**bad_response)) as provider:
        with pytest.raises(ProviderProtocolError):
            await provider.preflight(INTENT)


@pytest.mark.asyncio
async def test_deadline_is_bounded_and_never_calls_a_mutation(monkeypatch):
    original_timeout = asyncio.timeout
    budgets, calls = [], []
    def immediate_timeout(seconds):
        budgets.append(seconds)
        return original_timeout(0)
    async def blocked(request):
        calls.append(request.method)
        await asyncio.Event().wait()
    monkeypatch.setattr(asyncio, "timeout", immediate_timeout)
    async with adapter(blocked) as provider:
        with pytest.raises(ProviderUnavailable, match="STRIPE_DEADLINE_EXCEEDED"):
            await provider.preflight(INTENT)
    assert budgets == [15] and calls == ["GET"]


@pytest.mark.asyncio
async def test_cancelled_inspection_propagates_without_retry():
    entered, calls = asyncio.Event(), []
    async def blocked(request):
        calls.append(request.method)
        entered.set()
        await asyncio.Event().wait()
    async with adapter(blocked) as provider:
        running = asyncio.create_task(provider.preflight(INTENT))
        await asyncio.wait_for(entered.wait(), 3)
        running.cancel()
        with pytest.raises(asyncio.CancelledError):
            await running
    assert calls == ["GET"]


@pytest.mark.parametrize("error, expected", [
    (CredentialInputError(KEY), "CREDENTIAL_INPUT_INVALID"),
    (ValueError(KEY), "SCOPE_INVALID"),
    (ProviderUnavailable("STRIPE_HTTP_403"), "STRIPE_HTTP_403"),
    (ProviderUnavailable("STRIPE_HTTP_403\n" + KEY), "PREFLIGHT_UNAVAILABLE"),
    (ProviderProtocolError("STRIPE_ACCOUNT_MISMATCH"), "STRIPE_ACCOUNT_MISMATCH"),
    (RuntimeError(KEY), "PREFLIGHT_UNAVAILABLE"),
])
def test_command_failure_is_sanitized_machine_readable_and_nonzero(monkeypatch, capsys, error, expected):
    monkeypatch.setattr(sys, "argv", ["preflight", "--account-id", ACCOUNT, "--stripe-key-file", "unused"])
    async def fail(args):
        raise error
    monkeypatch.setattr(command, "run", fail)
    with pytest.raises(SystemExit) as stopped:
        command.main()
    assert stopped.value.code == 1
    output = capsys.readouterr()
    assert json.loads(output.out) == {"event": "STRIPE_SANDBOX_PREFLIGHT_FAILED", "code": expected,
                                      "mutation_attempted": False, "journal_updated": False}
    assert KEY not in output.out + output.err and "Traceback" not in output.err


def test_successful_cli_prints_only_the_result_and_exits_zero(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["preflight", "--account-id", ACCOUNT, "--stripe-key-file", "unused"])
    async def succeed(args):
        assert args.payment_intent_id is None
        return {"source": "SIMULATED", "mutation_attempted": False}
    monkeypatch.setattr(command, "run", succeed)
    with pytest.raises(SystemExit) as stopped:
        command.main()
    assert stopped.value.code == 0
    assert json.loads(capsys.readouterr().out) == {"source": "SIMULATED", "mutation_attempted": False}


@pytest.mark.parametrize("extra", [["--api-key", KEY], ["--base-url", "https://attacker.invalid/" + KEY],
                                  ["--stripe-key", KEY], ["--account-id"], ["--cancel", INTENT]])
def test_cli_rejects_secret_transport_mutation_and_abbreviated_arguments_without_echo(extra):
    result = subprocess.run([sys.executable, "-m", "providers.preflight_stripe", *extra],
                            capture_output=True, text=True, timeout=15)
    assert result.returncode == 2 and KEY not in result.stdout + result.stderr
    assert "Invalid preflight arguments" in result.stderr


@pytest.mark.parametrize("key", ["sk_test_" + "x" * 32, "rk_live_" + "x" * 32, KEY + "\n"])
def test_cli_invalid_key_fails_before_http_without_creating_assets(tmp_path, key):
    path = secret(tmp_path, "invalid.key", key)
    before = set(tmp_path.iterdir())
    result = subprocess.run([sys.executable, "-m", "providers.preflight_stripe", "--account-id", ACCOUNT,
                             "--stripe-key-file", path], capture_output=True, text=True, timeout=15)
    assert result.returncode == 1 and json.loads(result.stdout)["code"] == "CREDENTIAL_INPUT_INVALID"
    assert key not in result.stdout + result.stderr and "Traceback" not in result.stderr
    assert set(tmp_path.iterdir()) == before


def test_help_explains_read_only_contract_and_has_no_raw_secret_or_journal_option():
    result = subprocess.run([sys.executable, "-m", "providers.preflight_stripe", "--help"],
                            capture_output=True, text=True, timeout=15)
    assert result.returncode == 0 and "GET only" in result.stdout
    assert "--payment-intent-id" in result.stdout
    assert all(option not in result.stdout for option in ["--api-key", "--journal", "--cancel", "--base-url"])
