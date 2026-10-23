"""SIMULATED HTTP protocol fixtures, real SQLite/auth and deterministic barriers.

These never access Stripe and are not REAL PROVIDER validation.
"""
import asyncio
from dataclasses import replace
import json
import os
from types import SimpleNamespace

import httpx2 as httpx
import pytest

from providers.stripe_sandbox import (
    API_VERSION, CONTRACT, TOOL, PERMISSION, MAX_RESPONSE_BYTES, StripeSandboxConfig,
    StripeSandboxCancellation, ProviderUnavailable, ProviderProtocolError,
)
from runtime.action_journal import SQLiteActionJournal
from runtime.credential_input import read_stripe_key_file, CredentialInputError
from runtime.policy_engine import AuthorizationContext, ConfirmationRequiredError, PermissionDeniedError
from runtime.speculative_saga import (
    SpeculativeSagaManager, TurnEpochClock, ActionState, DuplicateOperationError,
)
from runtime.tool_contract import ToolRegistry


ACCOUNT = "acct_Simulated12345"
INTENT = "pi_Simulated123456789"
KEY = "rk_test_" + "SIMULATED" * 4


class Chunks(httpx.AsyncByteStream):
    def __init__(self, chunks):
        self.chunks = chunks

    async def __aiter__(self):
        for chunk in self.chunks:
            yield chunk


def response(data=None, *, status=200, headers=None, raw=None, chunks=None):
    metadata = {"content-type": "application/json", "request-id": "req_Simulated12345", "stripe-version": API_VERSION}
    metadata.update(headers or {})
    if chunks is None:
        chunks = [raw if raw is not None else json.dumps(data).encode()]
    return httpx.Response(status, headers=metadata, stream=Chunks(chunks))


def account():
    return response({"id": ACCOUNT, "object": "account", "email": "PRIVATE:do-not-retain"})


def intent(**updates):
    return {"id": INTENT, "object": "payment_intent", "livemode": False,
            "status": "canceled", "canceled_at": 1700000000, "amount_capturable": 0,
            "client_secret": "PRIVATE:do-not-retain", "customer": "PRIVATE:do-not-retain", **updates}


def adapter(handler):
    return StripeSandboxCancellation(StripeSandboxConfig(ACCOUNT, KEY), transport=httpx.MockTransport(handler))


def runtime(tmp_path, provider):
    journal = SQLiteActionJournal(tmp_path / "provider.db")
    journal.acquire_lease("SIMULATED-worker", ttl_ms=120_000)
    permissions = {PERMISSION, "ccs:reconcile", "ccs:repair"}
    journal.security.provision_principal("worker", permissions)
    journal.security.provision_principal("approver", permissions | {"ccs:approve"})
    credential = journal.security.issue_credential("worker")
    issuer = journal.security.issue_credential("approver")
    registry = ToolRegistry()
    registry.register(provider.manifest())
    auth = AuthorizationContext(credential=credential.token)
    saga = SpeculativeSagaManager(TurnEpochClock(), registry=registry, journal=journal, auth_context=auth)
    return SimpleNamespace(journal=journal, saga=saga, auth=auth, issuer=issuer)


def approve(env, action):
    env.auth.approval_id = env.journal.security.issue_approval(env.issuer.token, "worker", action.action_id)


@pytest.mark.parametrize("key", ["sk_live_abc", "rk_live_" + "x" * 32, "sk_test_" + "x" * 32,
                                "pk_test_" + "x" * 32, KEY + "\n", "", KEY + "\r\n"])
def test_reject_live_unrestricted_publishable_and_malformed_keys(key):
    with pytest.raises(ValueError, match="restricted Stripe test key"):
        StripeSandboxConfig(ACCOUNT, key)
    assert KEY not in repr(StripeSandboxConfig(ACCOUNT, KEY))


@pytest.mark.parametrize("value", ["", "acct_bad/path", "acct_bad\r\nHeader: x", "wrongaccount"])
def test_reject_account_path_and_header_injection(value):
    with pytest.raises(ValueError):
        StripeSandboxConfig(value, KEY)


@pytest.mark.asyncio
async def test_scope_is_exact_and_endpoint_is_fixed():
    async with adapter(lambda _: pytest.fail("invalid scope reached HTTP")) as provider:
        args = provider.arguments(INTENT)
        assert provider.operation_key(args) == provider.operation_key(dict(args))
        for invalid in ("../pi_secret", "pi_12345678/cancel", INTENT + "?x=y", INTENT + "\n", None):
            with pytest.raises(ValueError):
                provider.arguments(invalid)
        for field, value in (("account_id", "acct_Another1234"), ("api_version", "future"),
                             ("provider_contract", "different"), ("url", "https://attacker.invalid")):
            with pytest.raises(ValueError):
                await provider.execute({**args, field: value})


@pytest.mark.asyncio
async def test_journal_approval_and_intent_precede_single_post_and_redacted_receipt(tmp_path, monkeypatch):
    calls, holder = [], []
    # Ambient proxies and keys must not become authority or redirect credentials.
    monkeypatch.setenv("HTTPS_PROXY", "http://127.0.0.1:1")
    monkeypatch.setenv("STRIPE_SECRET_KEY", "sk_live_MUST_NOT_BE_USED")
    def handle(request):
        calls.append((request.method, str(request.url)))
        assert request.url.host == "api.stripe.com" and request.url.scheme == "https"
        assert request.headers["authorization"] == "Bearer " + KEY
        assert request.headers["stripe-version"] == API_VERSION
        assert "stripe-account" not in request.headers
        assert holder[0].journal.events(holder[1].action_id)[-1]["kind"] == "DISPATCH_INTENT"
        if request.url.path == "/v1/account":
            assert "idempotency-key" not in request.headers
            return account()
        assert request.headers["idempotency-key"] == holder[1].idempotency_key
        assert request.content == b""
        return response(intent())
    async with adapter(handle) as provider:
        env = runtime(tmp_path, provider)
        action = provider.stage(env.saga, INTENT)
        holder.extend([env, action])
        with pytest.raises(ConfirmationRequiredError):
            await env.saga.commit_write(action.action_id, provider.execute)
        assert calls == []
        approve(env, action)
        result = await env.saga.commit_write(action.action_id, provider.execute)
        assert action.state == ActionState.COMMITTED and result == action.result
        assert result["observed_via"] == "POST" and result["actor_attribution"] == "not_claimed"
        assert len(calls) == 2 and [v[0] for v in calls] == ["GET", "POST"]
        durable = json.dumps(env.journal.records()) + json.dumps(env.journal.events(action.action_id))
        assert "PRIVATE:" not in durable and KEY not in durable
        manifest = provider.manifest()
        assert manifest.compensate is None and manifest.cancellable is False


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [301, 307, 400, 401, 403, 404, 409, 429, 500, 503])
async def test_http_errors_never_mean_not_applied_and_never_retry(tmp_path, status):
    calls = []
    def handle(request):
        calls.append(request.method)
        return account() if request.url.path == "/v1/account" else response(
            {"secret": KEY}, status=status, headers={"location": "https://attacker.invalid/collect"})
    async with adapter(handle) as provider:
        env = runtime(tmp_path, provider)
        action = provider.stage(env.saga, INTENT)
        approve(env, action)
        with pytest.raises(ProviderUnavailable) as error:
            await env.saga.commit_write(action.action_id, provider.execute)
        assert KEY not in str(error.value) and "attacker" not in str(error.value)
        assert calls == ["GET", "POST"] and action.state == ActionState.IN_FLIGHT_UNKNOWN
        with pytest.raises(DuplicateOperationError):
            provider.stage(env.saga, INTENT)


@pytest.mark.asyncio
@pytest.mark.parametrize("updates", [{"livemode": True}, {"livemode": 0}, {"id": "pi_Different123"},
    {"object": "customer"}, {"status": "unrecognized"}, {"status": "succeeded"},
    {"canceled_at": True}, {"canceled_at": 0}, {"amount_capturable": 1}, {"amount_capturable": False}])
async def test_invalid_or_nonterminal_success_is_unknown_not_commit(tmp_path, updates):
    async with adapter(lambda r: account() if r.url.path == "/v1/account" else response(intent(**updates))) as provider:
        env = runtime(tmp_path, provider)
        action = provider.stage(env.saga, INTENT)
        approve(env, action)
        with pytest.raises(ProviderProtocolError):
            await env.saga.commit_write(action.action_id, provider.execute)
        assert action.state == ActionState.IN_FLIGHT_UNKNOWN and action.result is None


@pytest.mark.asyncio
@pytest.mark.parametrize("at_post", [False, True])
@pytest.mark.parametrize("bad", ["duplicate", "nan", "encoding", "length", "chunks", "reference", "version", "json", "type"])
async def test_response_bounds_and_protocol_validation(bad, at_post):
    variants = {
        "duplicate": {"raw": b'{"id":1,"id":2}'}, "nan": {"raw": b'{"x":NaN}'},
        "encoding": {"headers": {"content-encoding": "gzip"}},
        "length": {"headers": {"content-length": str(MAX_RESPONSE_BYTES + 1)}},
        "chunks": {"chunks": [b" " * 32768, b" " * 32768, b"x"]},
        "reference": {"headers": {"request-id": "req_bad\nsecret"}},
        "version": {"headers": {"stripe-version": "incompatible"}},
        "json": {"raw": b"not json"}, "type": {"headers": {"content-type": "text/html"}},
    }
    calls = []
    def handle(request):
        calls.append(request.method)
        if at_post and request.url.path == "/v1/account":
            return account()
        return response(intent(), **variants[bad])
    async with adapter(handle) as provider:
        with pytest.raises(ProviderProtocolError):
            await provider.execute(provider.arguments(INTENT))
        assert calls == (["GET", "POST"] if at_post else ["GET"])


@pytest.mark.asyncio
async def test_wrong_credential_account_blocks_mutation():
    calls = []
    def handle(request):
        calls.append(request.method)
        return response({"id": "acct_Different123", "object": "account"})
    async with adapter(handle) as provider:
        with pytest.raises(ProviderProtocolError, match="ACCOUNT_MISMATCH"):
            await provider.execute(provider.arguments(INTENT))
        assert calls == ["GET"]


@pytest.mark.asyncio
async def test_lost_ack_restart_resolves_known_resource_without_post_replay(tmp_path):
    calls, applied = [], []
    def handle(request):
        calls.append(request.method)
        if request.url.path == "/v1/account":
            return account()
        if request.method == "POST":
            applied.append(INTENT)  # Independent SIMULATED remote ledger.
            raise httpx.ReadTimeout("PRIVATE:" + KEY, request=request)
        assert applied == [INTENT]
        return response(intent())
    async with adapter(handle) as provider:
        env = runtime(tmp_path, provider)
        action = provider.stage(env.saga, INTENT)
        approve(env, action)
        with pytest.raises(ProviderUnavailable) as error:
            await env.saga.commit_write(action.action_id, provider.execute)
        assert KEY not in str(error.value) and action.state == ActionState.IN_FLIGHT_UNKNOWN
        recovered = SpeculativeSagaManager(TurnEpochClock(), journal=env.journal, registry=env.saga.registry, auth_context=env.auth)
        old = recovered._actions[action.action_id]
        assert old.result is None and old.compensate is None
        await recovered.reconcile_write(old.action_id, provider.status_check)
        assert old.state == ActionState.COMMITTED_STALE and old.result["observed_via"] == "GET"
        assert env.journal.records()[0][1]["result"] == old.result
        assert calls == ["GET", "POST", "GET", "GET"] and applied == [INTENT]


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["requires_payment_method", "requires_confirmation", "requires_action",
                                       "requires_capture", "processing", "succeeded", "missing"])
async def test_status_never_inferrs_absence_or_retries_delayed_mutation(tmp_path, status):
    calls = []
    def handle(request):
        calls.append(request.method)
        if request.url.path == "/v1/account":
            return account()
        if request.method == "POST":
            raise httpx.ReadError("SIMULATED response lost", request=request)
        if status == "missing":
            return response({}, status=404)
        return response(intent(status=status, canceled_at=None))
    async with adapter(handle) as provider:
        env = runtime(tmp_path, provider)
        action = provider.stage(env.saga, INTENT)
        approve(env, action)
        with pytest.raises(ProviderUnavailable):
            await env.saga.commit_write(action.action_id, provider.execute)
        for _ in range(2):
            if status == "missing":
                with pytest.raises(ProviderUnavailable):
                    await env.saga.reconcile_write(action.action_id, provider.status_check)
            else:
                await env.saga.reconcile_write(action.action_id, provider.status_check)
        assert action.state == ActionState.IN_FLIGHT_UNKNOWN and action.result is None
        assert calls.count("POST") == 1


@pytest.mark.asyncio
async def test_epoch_advance_while_provider_awaits_preserves_irreversible_stale_effect(tmp_path):
    entered, release = asyncio.Event(), asyncio.Event()
    effects = []
    async def handle(request):
        if request.url.path == "/v1/account":
            return account()
        effects.append(INTENT)
        entered.set()
        await release.wait()
        return response(intent())
    async with adapter(handle) as provider:
        env = runtime(tmp_path, provider)
        action = provider.stage(env.saga, INTENT)
        approve(env, action)
        running = asyncio.create_task(env.saga.commit_write(action.action_id, provider.execute))
        await asyncio.wait_for(entered.wait(), 3)
        await env.saga.epoch_clock.advance("SIMULATED correction after remote apply")
        release.set()
        await asyncio.wait_for(running, 3)
        assert action.state == ActionState.COMMITTED_STALE and action.result["status"] == "canceled"
        assert action.compensate is None and effects == [INTENT] and env.saga._chain == []


@pytest.mark.asyncio
async def test_reconcile_cannot_change_account_payload_or_operation_key(tmp_path):
    async with adapter(lambda _: pytest.fail("substituted action queried provider")) as provider:
        env = runtime(tmp_path, provider)
        original = provider.stage(env.saga, INTENT)
        for change in ({"tool_name": "another-tool"}, {"idempotency_key": "new-retry-key"},
                       {"args": {**original.args, "payment_intent_id": "pi_Different123"}}):
            with pytest.raises(PermissionDeniedError):
                await provider.status_check(replace(original, **change))


@pytest.mark.asyncio
async def test_open_status_then_delayed_apply_resolves_without_second_post(tmp_path):
    calls, ledger = [], {"applied": False}
    def handle(request):
        calls.append(request.method)
        if request.url.path == "/v1/account":
            return account()
        if request.method == "POST":
            raise httpx.ReadTimeout("SIMULATED request still queued", request=request)
        return response(intent() if ledger["applied"] else intent(status="requires_confirmation", canceled_at=None))
    async with adapter(handle) as provider:
        env = runtime(tmp_path, provider)
        action = provider.stage(env.saga, INTENT)
        approve(env, action)
        with pytest.raises(ProviderUnavailable):
            await env.saga.commit_write(action.action_id, provider.execute)
        await env.saga.reconcile_write(action.action_id, provider.status_check)
        assert action.state == ActionState.IN_FLIGHT_UNKNOWN
        with pytest.raises(DuplicateOperationError):
            provider.stage(env.saga, INTENT)
        ledger["applied"] = True  # Independent SIMULATED late remote application.
        await env.saga.reconcile_write(action.action_id, provider.status_check)
        assert action.state == ActionState.COMMITTED and calls.count("POST") == 1


@pytest.mark.asyncio
async def test_coroutine_cancel_after_post_is_unknown_and_never_undoes_remote_goal(tmp_path):
    entered = asyncio.Event()
    ledger = []
    async def handle(request):
        if request.url.path == "/v1/account":
            return account()
        ledger.append(INTENT)
        entered.set()
        await asyncio.Event().wait()
    async with adapter(handle) as provider:
        env = runtime(tmp_path, provider)
        action = provider.stage(env.saga, INTENT)
        approve(env, action)
        operation = asyncio.create_task(env.saga.commit_write(action.action_id, provider.execute))
        await asyncio.wait_for(entered.wait(), 3)
        operation.cancel()
        with pytest.raises(asyncio.CancelledError):
            await operation
        assert action.state == ActionState.IN_FLIGHT_UNKNOWN and ledger == [INTENT]
        assert env.journal.records()[0][1]["state"] == "IN_FLIGHT_UNKNOWN"


@pytest.mark.asyncio
async def test_provider_staging_cannot_use_in_memory_simulation():
    async with adapter(lambda _: pytest.fail("unjournaled provider call")) as provider:
        with pytest.raises(PermissionDeniedError, match="durable journal"):
            provider.stage(SpeculativeSagaManager(TurnEpochClock(), simulation=True), INTENT)


@pytest.mark.parametrize("bad", [KEY + "\n", "rk_live_" + "x" * 32, "sk_test_" + "x" * 32, "x" * 300])
def test_private_provider_key_file_rejects_other_formats(tmp_path, bad):
    path = tmp_path / "stripe.key"
    path.write_text(bad, encoding="ascii")
    path.chmod(0o600)
    with pytest.raises(CredentialInputError):
        read_stripe_key_file(path)


def test_private_provider_key_file_and_hardlink_rejection(tmp_path):
    path = tmp_path / "stripe.key"
    path.write_text(KEY, encoding="ascii")
    path.chmod(0o600)
    assert StripeSandboxConfig.from_file(ACCOUNT, path).key == KEY
    if os.name != "nt":
        path.chmod(0o644)
        with pytest.raises(CredentialInputError):
            read_stripe_key_file(path)
        path.chmod(0o600)
    alias = tmp_path / "alias.key"
    try:
        os.link(path, alias)
    except OSError:
        pytest.skip("Filesystem does not support hard links")
    with pytest.raises(CredentialInputError):
        read_stripe_key_file(path)
