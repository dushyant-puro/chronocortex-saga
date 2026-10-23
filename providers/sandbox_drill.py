"""One supervised, console-approved cancellation of a known Stripe test intent.

No create/confirm/capture/refund requests, approval issuance, recovered-action dispatch,
POST retry, automatic compensation, task completion or voice connection.
"""
import argparse
import asyncio
from contextlib import nullcontext
from dataclasses import asdict
import json
from pathlib import Path

from providers.stripe_sandbox import StripeSandboxConfig, StripeSandboxCancellation, PERMISSION, ProviderUnavailable
from runtime.action_journal import SQLiteActionJournal, JournalError
from runtime.credential_input import read_credential_file
from runtime.policy_engine import AuthorizationContext, PolicyError, PermissionDeniedError
from runtime.speculative_saga import SpeculativeSagaManager, TurnEpochClock, ActionState
from runtime.tool_contract import ToolRegistry
from runtime.worker_supervisor import WorkerSupervisor, WorkerSettings
from worker.__main__ import shutdown_signals


class _SafeParser(argparse.ArgumentParser):
    def error(self, message):
        self.exit(2, "Invalid sandbox drill arguments. Use --help; secrets belong only in protected files.\n")


def _permissions(journal, credential):
    actor = journal.security.authenticate(credential)
    if not {"ccs:work", "ccs:reconcile", PERMISSION} <= actor.granted_permissions:
        raise PermissionDeniedError("Current worker and sandbox reconciliation grants required")
    return actor


async def _approval(host, credential, action_id, deadline, clock):
    while not host.stopping.is_set() and clock() < deadline:
        selected = await asyncio.to_thread(host.journal.security.find_forward_approval, credential, action_id)
        if host.stopping.is_set() or clock() >= deadline:
            return None
        if selected is not None:
            return selected
        try:
            await asyncio.wait_for(host.stopping.wait(), timeout=min(.25, max(0, deadline - clock())))
        except TimeoutError:
            pass
    return None


async def run(args, *, provider_factory=None, event_sink=None, settings=None, clock=None, handle_signals=True):
    """Trusted Python seams support SIMULATED tests; none are CLI options."""
    if (type(handle_signals) is not bool or args.allow_sandbox_cancellation is not True or type(args.approval_wait_seconds) is not int or
            not 1 <= args.approval_wait_seconds <= 300 or not Path(args.journal).is_file()):
        raise ValueError("Explicit bounded sandbox drill and existing journal required")
    credential = read_credential_file(args.credential_file)
    config = StripeSandboxConfig.from_file(args.account_id, args.stripe_key_file)
    make_provider = StripeSandboxCancellation if provider_factory is None else provider_factory
    source = "REAL_PROVIDER_TEST_MODE" if provider_factory is None else "SIMULATED"
    clock = clock or asyncio.get_running_loop().time

    def emit(event, **fields):
        # Presentation receives detached, finite JSON; it cannot edit staged args.
        record = json.loads(json.dumps({"event": event, "source": source, **fields}, allow_nan=False))
        if event_sink is None:
            print(json.dumps(record), flush=True)
        else:
            event_sink(record)

    outcome = None
    result_code = 3
    async with make_provider(config) as provider:
        provider.arguments(args.payment_intent_id)  # Reject substitution before journal/HTTP.
        host = WorkerSupervisor(SQLiteActionJournal(args.journal), credential,
                                settings=settings or WorkerSettings(drain_timeout_ms=30_000))
        registry = ToolRegistry()
        registry.register(provider.manifest())

        def build(journal, auth):
            return SpeculativeSagaManager(TurnEpochClock(), journal=journal, registry=registry, auth_context=auth)

        async def inspect():
            host.runtime  # Recheck admission before each new read; the drill task owns its lifetime.
            _permissions(host.journal, credential)
            return await provider.preflight(args.payment_intent_id)

        async def drill(saga):
            nonlocal outcome, result_code
            actor = _permissions(host.journal, credential)
            preflight = await inspect()
            if (host.stopping.is_set() or
                    preflight["payment_intent"]["status"] not in {"requires_payment_method", "requires_confirmation"}):
                emit("STRIPE_SANDBOX_DRILL_BLOCKED", reason="STOPPED_OR_OBJECT_NOT_ELIGIBLE", preflight=preflight)
            else:
                action = provider.stage(saga, args.payment_intent_id)
                deadline = clock() + args.approval_wait_seconds
                emit("STRIPE_SANDBOX_APPROVAL_REQUIRED", action_id=action.action_id,
                     executor_principal=actor.principal_id, tool=action.tool_name, args=action.args,
                     digest=action._staged_digest, chain_id=action.chain_id,
                     approval_wait_seconds=args.approval_wait_seconds, irreversible=True,
                     preflight_status=preflight["payment_intent"]["status"], remote_state_locked=False,
                     dispatch_intent_recorded=False)
                selected = await _approval(host, credential, action.action_id, deadline, clock)
                if selected is None or host.stopping.is_set() or clock() >= deadline:
                    saga.abort_write(action.action_id)  # This fresh local action never entered dispatch.
                else:
                    auth = AuthorizationContext(credential=credential, approval_id=selected)

                    async def forward(current):
                        if clock() >= deadline:
                            return  # Selection expired before this operation was admitted.
                        await current.commit_write(action.action_id, provider.execute, timeout=20.0, auth_context=auth)

                    try:
                        await forward(saga)
                    except (PolicyError, JournalError, ProviderUnavailable, TimeoutError):
                        # Inspect durable/local evidence below; an exception proves no remote absence.
                        pass
                    if action.state == ActionState.PENDING:
                        saga.abort_write(action.action_id)  # Atomic admission denial; no intent was written.
                    if action.state == ActionState.IN_FLIGHT_UNKNOWN and not host.stopping.is_set():
                        try:
                            await saga.reconcile_write(action.action_id, provider.status_check)
                        except (PolicyError, JournalError, ProviderUnavailable, TimeoutError):
                            emit("STRIPE_SANDBOX_STATUS_UNAVAILABLE", action_id=action.action_id)

                readback = None
                readback_verified = False
                if action.state in {ActionState.COMMITTED, ActionState.COMMITTED_STALE} and not host.stopping.is_set():
                    try:
                        readback = await inspect()
                        readback_verified = readback["payment_intent"]["status"] == "canceled"
                        await asyncio.to_thread(host.journal.observe_worker, "PROVIDER_READBACK",
                                                {"action_id": action.action_id, "terminal_observed": readback_verified,
                                                 "account_request_id": readback["account_request_id"],
                                                 "payment_intent_request_id": readback["payment_intent"]["request_id"]})
                    except (PolicyError, JournalError, ProviderUnavailable, TimeoutError):
                        readback_verified = False
                        emit("STRIPE_SANDBOX_READBACK_UNAVAILABLE", action_id=action.action_id)
                # Retain claims/receipts. Closing this standalone drill chain
                # prevents continuation; it does not reverse the remote effect.
                host.journal.close_chain(action.chain_id)
                events = host.journal.events(action.action_id)
                outcome = {"action_id": action.action_id, "state": action.state.name,
                           "receipt_present": action.result is not None, "readback_verified": readback_verified,
                           "readback": readback,
                           "dispatch_intent_recorded": any(row["kind"] == "DISPATCH_INTENT" for row in events),
                           "automatic_replay": False, "automatic_compensation": False}
                if action.state == ActionState.COMMITTED and action.result is not None and readback_verified:
                    result_code = 0
                elif action.state == ActionState.IN_FLIGHT_UNKNOWN:
                    result_code = 2

        with (shutdown_signals(asyncio.get_running_loop(), host) if handle_signals else nullcontext()):
            try:
                await host.start(build)
                await host.submit(drill).wait()
            finally:
                report = await host.stop()
                emit("STRIPE_SANDBOX_DRILL_STOPPED", **asdict(report))
    if outcome is not None:
        emit("STRIPE_SANDBOX_DRILL_FINISHED", **outcome)
    if not report.drained or not report.lease_released or report.failure_code is not None:
        return 1
    return result_code


def main():
    parser = _SafeParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--journal", required=True, help="Existing compatible journal in a protected directory")
    parser.add_argument("--credential-file", required=True, help="Protected separate worker credential file")
    parser.add_argument("--stripe-key-file", required=True, help="Protected restricted rk_test_ key file")
    parser.add_argument("--account-id", required=True)
    parser.add_argument("--payment-intent-id", required=True, help="Known disposable unconfirmed test PaymentIntent")
    parser.add_argument("--approval-wait-seconds", type=int, default=120, help="Bounded console approval wait: 1..300 seconds")
    parser.add_argument("--allow-sandbox-cancellation", action="store_true", required=True,
                        help="Enable this one-write host; exact durable console approval is still required")
    args = parser.parse_args()
    try:
        result = asyncio.run(run(args))
    except Exception:
        parser.exit(1, "Sandbox drill failed closed. Preserve the journal; uncertain effects require GET-only reconciliation.\n")
    raise SystemExit(result)


if __name__ == "__main__":
    main()
