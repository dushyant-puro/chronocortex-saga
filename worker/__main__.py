"""Run a lease-supervised recovery host without replay, provider calls or voice."""
import argparse
import asyncio
from contextlib import contextmanager
from dataclasses import asdict
import json
from pathlib import Path
import signal

from runtime.action_journal import SQLiteActionJournal
from runtime.credential_input import read_credential_file
from runtime.speculative_saga import SpeculativeSagaManager, TurnEpochClock
from runtime.tool_contract import ToolRegistry
from runtime.worker_supervisor import WorkerSettings, WorkerSupervisor


@contextmanager
def shutdown_signals(loop, host):
    # Windows lacks loop.add_signal_handler. Python signal handlers only queue
    # work; they do not take locks, perform I/O or cancel a provider coroutine.
    previous = {}
    def stop(*_):
        loop.call_soon_threadsafe(host.request_stop)
    try:
        for kind in (signal.SIGINT, signal.SIGTERM, getattr(signal, "SIGBREAK", None)):
            if kind is not None:
                previous[kind] = signal.getsignal(kind)
                signal.signal(kind, stop)
        yield
    finally:
        for kind, handler in previous.items():
            signal.signal(kind, handler)


def recovery_runtime(journal, auth):
    # Hydrates evidence with NO executors, compensators, query handlers or task
    # workflow rebinding. Recovered writes remain explicitly non-replayable.
    return SpeculativeSagaManager(TurnEpochClock(), registry=ToolRegistry(), journal=journal, auth_context=auth)


async def run(args, settings):
    credential = read_credential_file(args.credential_file)
    host = WorkerSupervisor(SQLiteActionJournal(args.journal), credential, settings=settings)
    with shutdown_signals(asyncio.get_running_loop(), host):
        try:
            runtime = await host.start(recovery_runtime)
            print(json.dumps({"event": "WORKER_STARTED", "mode": "RECOVERY_ONLY", "provider": "NOT_CONFIGURED",
                              "generation": host._lease.generation, "recovered_action_records": len(runtime._actions)}), flush=True)
            if not args.check:
                await host.stopping.wait()
        finally:
            report = await host.stop()
    print(json.dumps({"event": "WORKER_STOPPED", **asdict(report)}), flush=True)
    return 0 if report.drained and report.lease_released and report.failure_code is None else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--journal", required=True)
    parser.add_argument("--credential-file", required=True)
    parser.add_argument("--check", action="store_true", help="Validate startup/hydration and graceful release, then exit; no provider calls")
    parser.add_argument("--lease-ttl-ms", type=int, default=30_000)
    parser.add_argument("--renew-interval-ms", type=int, default=5_000)
    parser.add_argument("--safety-margin-ms", type=int, default=1_000)
    parser.add_argument("--drain-timeout-ms", type=int, default=10_000)
    args = parser.parse_args()
    if not Path(args.journal).is_file():
        parser.error("An existing journal is required; initialize/migrate offline first")
    try:
        settings = WorkerSettings(lease_ttl_ms=args.lease_ttl_ms, renew_interval_ms=args.renew_interval_ms,
                                  safety_margin_ms=args.safety_margin_ms, drain_timeout_ms=args.drain_timeout_ms)
    except ValueError:
        parser.error("Invalid worker timing configuration")
    try:
        code = asyncio.run(run(args, settings))
    except Exception:
        # Never log credential bytes, argument payloads or exception text.
        parser.exit(1, "Worker failed closed. Inspect journal evidence and offline authority before recovery.\n")
    raise SystemExit(code)


if __name__ == "__main__":
    main()
