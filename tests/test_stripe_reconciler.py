"""Real supervised recovery command with SIMULATED provider responses."""
import json
import subprocess
import sys
from types import SimpleNamespace

import httpx2 as httpx
import pytest

from providers.reconcile_stripe import run
from providers.stripe_sandbox import StripeSandboxCancellation, ProviderUnavailable, PERMISSION
from runtime.action_journal import SQLiteActionJournal
from runtime.policy_engine import PermissionDeniedError
from runtime.speculative_saga import ActionState
from test_stripe_sandbox import adapter, runtime, approve, ACCOUNT, INTENT, KEY, account, intent, response


def secret(tmp_path, name, value):
    path = tmp_path / name
    path.write_text(value, encoding="ascii")
    path.chmod(0o600)
    return str(path)


async def setup(tmp_path, *, historical=False):
    def lost(request):
        return account() if request.url.path == "/v1/account" else response({}, status=503)
    async with adapter(lost) as provider:
        env = runtime(tmp_path, provider)
        action = provider.stage(env.saga, INTENT)
        approve(env, action)
        with pytest.raises(ProviderUnavailable):
            await env.saga.commit_write(action.action_id, provider.execute)
        if historical:
            action.state = ActionState.COMMITTED_STALE
            action.result = {"historical": "SIMULATED:terminal-receipt"}
            env.saga._persist_action(action, "RECONCILED_PRESENT_STALE")
    env.journal.security.provision_principal("reconciler", {"ccs:work", "ccs:reconcile", PERMISSION})
    credential = env.journal.security.issue_credential("reconciler")
    env.journal.release_lease()
    args = SimpleNamespace(journal=str(env.journal.path), action_id=action.action_id, account_id=ACCOUNT,
                           credential_file=secret(tmp_path, "worker.key", credential.token),
                           stripe_key_file=secret(tmp_path, "stripe.key", KEY))
    return env, action, args


@pytest.mark.asyncio
async def test_command_acquires_ownership_and_records_status_without_post(tmp_path, capsys):
    env, action, args = await setup(tmp_path)
    calls = []
    def handle(request):
        calls.append(request.method)
        assert request.method == "GET"
        return account() if request.url.path == "/v1/account" else response(intent())
    result = await run(args, provider_factory=lambda config: StripeSandboxCancellation(config, transport=httpx.MockTransport(handle)))
    assert result == 0 and calls == ["GET", "GET"]
    record = env.journal.records()[0][1]
    assert record["state"] == "COMMITTED_STALE" and record["result"]["observed_via"] == "GET"
    outputs = [json.loads(row) for row in capsys.readouterr().out.splitlines()]
    assert outputs[-1]["source"] == "SIMULATED" and outputs[-1]["automatic_replay"] is False
    assert outputs[0]["lease_released"] is True and KEY not in str(outputs)
    # New ownership follows a clean release, without forced takeover.
    other = SQLiteActionJournal(args.journal)
    other.acquire_lease("next-worker")
    other.release_lease()


@pytest.mark.asyncio
async def test_unknown_status_has_distinct_exit_code_and_releases_owner(tmp_path, capsys):
    env, action, args = await setup(tmp_path)
    def handle(request):
        return account() if request.url.path == "/v1/account" else response(intent(status="processing", canceled_at=None))
    result = await run(args, provider_factory=lambda config: StripeSandboxCancellation(config, transport=httpx.MockTransport(handle)))
    assert result == 2
    assert env.journal.records()[0][1]["state"] == "IN_FLIGHT_UNKNOWN"
    assert "IN_FLIGHT_UNKNOWN" in capsys.readouterr().out
    assert env.journal.worker_events()[-1]["kind"] == "RELEASED"


@pytest.mark.asyncio
async def test_historical_receipt_is_not_current_terminal_status(tmp_path, capsys):
    env, action, args = await setup(tmp_path, historical=True)
    def handle(request):
        return account() if request.url.path == "/v1/account" else response(intent(status="processing", canceled_at=None))
    result = await run(args, provider_factory=lambda config: StripeSandboxCancellation(config, transport=httpx.MockTransport(handle)))
    assert result == 2
    output = json.loads(capsys.readouterr().out.splitlines()[-1])
    assert output["receipt_present"] is True and output["terminal_status_observed"] is False
    assert env.journal.records()[0][1]["result"] == {"historical": "SIMULATED:terminal-receipt"}


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", ["ccs:work", "ccs:reconcile", PERMISSION])
async def test_command_requires_current_host_and_domain_reconciliation_authority(tmp_path, missing):
    env, action, args = await setup(tmp_path)
    env.journal.security.update_principal("reconciler", permissions={"ccs:work", "ccs:reconcile", PERMISSION} - {missing})
    with pytest.raises(PermissionDeniedError):
        await run(args, provider_factory=lambda config: StripeSandboxCancellation(
            config, transport=httpx.MockTransport(lambda _: pytest.fail("unauthorized provider read"))))


@pytest.mark.asyncio
@pytest.mark.parametrize("bad", ["missing-action", "account", "live-key", "api-version", "key"])
async def test_incompatible_scope_never_queries_provider(tmp_path, bad):
    env, action, args = await setup(tmp_path)
    if bad == "missing-action":
        args.action_id = "another-action"
    elif bad == "account":
        args.account_id = "acct_Different1234"
    elif bad == "live-key":
        secret(tmp_path, "stripe.key", "rk_live_" + "x" * 32)
    else:
        # A test-only corruption must not change adapter selection/query scope.
        original = StripeSandboxCancellation.status_check
        async def substitute(provider, selected):
            if bad == "api-version":
                selected.args["api_version"] = "incompatible"
            else:
                selected.idempotency_key = "new-unknown-retry"
            return await original(provider, selected)
        from unittest.mock import patch
        with patch.object(StripeSandboxCancellation, "status_check", substitute):
            with pytest.raises((ValueError, PermissionDeniedError)):
                await run(args, provider_factory=lambda config: StripeSandboxCancellation(
                    config, transport=httpx.MockTransport(lambda _: pytest.fail("incompatible provider read"))))
        return
    with pytest.raises((ValueError, PermissionDeniedError)):
        await run(args, provider_factory=lambda config: StripeSandboxCancellation(
            config, transport=httpx.MockTransport(lambda _: pytest.fail("incompatible provider read"))))


def test_cli_rejects_absent_journal_without_creating_assets(tmp_path):
    missing = tmp_path / "does-not-exist.db"
    result = subprocess.run([sys.executable, "-m", "providers.reconcile_stripe", "--journal", str(missing),
                             "--credential-file", str(tmp_path / "no-worker.key"),
                             "--stripe-key-file", str(tmp_path / "no-stripe.key"), "--account-id", ACCOUNT,
                             "--action-id", "missing"], capture_output=True, text=True, timeout=15)
    assert result.returncode == 2 and "existing journal" in result.stderr
    assert not missing.exists()


def test_cli_failure_sanitizes_secret_and_never_puts_key_in_help(tmp_path):
    journal = SQLiteActionJournal(tmp_path / "empty.db")
    args = [sys.executable, "-m", "providers.reconcile_stripe", "--journal", str(journal.path),
            "--credential-file", secret(tmp_path, "invalid-worker.key", "PRIVATE:credential"),
            "--stripe-key-file", secret(tmp_path, "stripe.key", KEY), "--account-id", ACCOUNT,
            "--action-id", "missing"]
    result = subprocess.run(args, capture_output=True, text=True, timeout=15)
    assert result.returncode == 1 and "failed closed" in result.stderr
    assert KEY not in result.stdout + result.stderr and "PRIVATE:" not in result.stdout + result.stderr
