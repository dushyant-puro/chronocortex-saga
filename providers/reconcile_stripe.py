"""Reconcile one recorded Stripe test cancellation with GET only; never replay."""
import argparse
import asyncio
from dataclasses import asdict
import json
from pathlib import Path

from providers.stripe_sandbox import StripeSandboxConfig, StripeSandboxCancellation, TOOL
from runtime.action_journal import SQLiteActionJournal
from runtime.credential_input import read_credential_file
from runtime.reconciliation import ReconciledEffect
from runtime.speculative_saga import SpeculativeSagaManager, TurnEpochClock, ActionState, CompensationState
from runtime.tool_contract import ToolRegistry
from runtime.worker_supervisor import WorkerSupervisor
from worker.__main__ import shutdown_signals


async def run(args, *, provider_factory=None):
    """Factory injection is a Python test seam, never a command-line option."""
    credential = read_credential_file(args.credential_file)
    config = StripeSandboxConfig.from_file(args.account_id, args.stripe_key_file)
    host = WorkerSupervisor(SQLiteActionJournal(args.journal), credential)
    make_provider = provider_factory or StripeSandboxCancellation
    outcome = None
    async with make_provider(config) as provider:
        registry = ToolRegistry()
        registry.register(provider.manifest())
        def build(journal, auth):
            return SpeculativeSagaManager(TurnEpochClock(), journal=journal, registry=registry, auth_context=auth)
        with shutdown_signals(asyncio.get_running_loop(), host):
            try:
                saga = await host.start(build)
                action = saga._actions.get(args.action_id)
                if (action is None or action.tool_name != TOOL or
                        action.state not in {ActionState.IN_FLIGHT_UNKNOWN, ActionState.COMMITTED_STALE} or
                        action.compensation_state != CompensationState.NOT_STARTED):
                    raise ValueError("Eligible recorded provider operation required")
                terminal_observed = False
                async def query(selected):
                    nonlocal terminal_observed
                    evidence = await provider.status_check(selected)
                    terminal_observed = type(evidence) is ReconciledEffect
                    return evidence
                operation = host.submit(lambda current: current.reconcile_write(action.action_id, query))
                await operation.wait()
                outcome = {"event": "STRIPE_STATUS_QUERY_COMPLETED", "action_id": action.action_id,
                           "state": action.state.name, "receipt_present": action.result is not None,
                           "terminal_status_observed": terminal_observed,
                           "source": "REAL_PROVIDER_TEST_MODE" if provider_factory is None else "SIMULATED",
                           "automatic_replay": False, "automatic_compensation": False}
            finally:
                report = await host.stop()
                print(json.dumps({"event": "RECONCILER_STOPPED", **asdict(report)}), flush=True)
    print(json.dumps(outcome), flush=True)
    if not report.drained or not report.lease_released or report.failure_code is not None:
        return 1
    # 0 proves recorded positive status, not current task validity or repair.
    return 0 if outcome["terminal_status_observed"] else 2


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--journal", required=True)
    parser.add_argument("--credential-file", required=True)
    parser.add_argument("--stripe-key-file", required=True)
    parser.add_argument("--account-id", required=True)
    parser.add_argument("--action-id", required=True)
    args = parser.parse_args()
    if not Path(args.journal).is_file():
        parser.error("An existing journal is required; initialize/migrate offline first")
    try:
        result = asyncio.run(run(args))
    except Exception:
        parser.exit(1, "Reconciliation failed closed. Preserve the journal and inspect scope, authority and provider access.\n")
    raise SystemExit(result)


if __name__ == "__main__":
    main()
