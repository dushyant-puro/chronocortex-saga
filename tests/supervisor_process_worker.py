"""Subprocess drill with an independent fsynced SIMULATED provider ledger."""
import asyncio
from dataclasses import asdict
import json
import os
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from runtime.action_journal import SQLiteActionJournal, JournalError
from runtime.credential_input import read_credential_file
from runtime.speculative_saga import SpeculativeSagaManager, TurnEpochClock
from runtime.tool_contract import ToolManifest, ToolRegistry
from runtime.worker_supervisor import WorkerSettings, WorkerSupervisor


def emit(**data):
    print(json.dumps(data), flush=True)


async def run():
    journal_path, secret_path, ledger_path, clock_path, mode = sys.argv[1:]
    def wall():
        return int(Path(clock_path).read_text())
    journal = SQLiteActionJournal(journal_path, clock_ms=wall)
    host = WorkerSupervisor(journal, read_credential_file(secret_path), monotonic=lambda: wall() / 1000,
                            settings=WorkerSettings(drain_timeout_ms=1 if mode == "timeout" else 10000))
    def build(journal, auth):
        registry = ToolRegistry()
        registry.register(ToolManifest("change", "write", required_permissions=["test:write"]))
        return SpeculativeSagaManager(TurnEpochClock(), registry=registry, journal=journal, auth_context=auth)
    saga = await host.start(build)
    started, finish = asyncio.Event(), asyncio.Event()
    def applied(direction):
        with open(ledger_path, "a", encoding="utf-8") as stream:
            stream.write(json.dumps({"provider": "SIMULATED", "direction": direction}) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
    async def execute(_):
        applied("FORWARD")
        if mode == "forward_crash":
            os._exit(23)
        if mode != "compensation_crash":
            started.set()
            await finish.wait()
        return {"provider": "SIMULATED", "receipt": "sim-effect-1"}
    async def undo(_):
        applied("COMPENSATION")
        os._exit(23)
    action = saga.stage_write("change", {"operation": "SIMULATED"}, compensate=undo if mode == "compensation_crash" else None)
    if mode == "compensation_crash":
        await saga.commit_write(action.action_id, execute)
        journal.security.provision_principal("SIMULATED-approver", {"ccs:approve", "ccs:repair", "test:write"})
        approver = journal.security.issue_credential("SIMULATED-approver")
        saga.auth_context.repair_approval_id = journal.security.issue_approval(approver.token, "worker", action.action_id, direction="COMPENSATION")
        await saga.compensate_action(action.action_id)
        raise AssertionError("Crash window was not reached")
    handle = host.submit(lambda runtime: runtime.commit_write(action.action_id, execute))
    if mode == "forward_crash":
        await handle.wait()
        raise AssertionError("Crash window was not reached")
    await started.wait()
    emit(event="APPLIED", action_id=action.action_id)
    assert await asyncio.to_thread(sys.stdin.readline) == "drain\n"
    stopping = asyncio.create_task(host.stop())
    await host.stopping.wait()
    if mode == "timeout":
        report = await stopping
        emit(event="TIMEOUT", **asdict(report))
    else:
        emit(event="DRAINING")
    assert await asyncio.to_thread(sys.stdin.readline) == "return\n"
    finish.set()
    try:
        await handle.wait()
    except JournalError:
        emit(event="LATE_FENCED", receipt=action.result, local_state=action.state.name)
    else:
        emit(event="RESULT_RETAINED", receipt=action.result)
    report = await stopping
    emit(event="STOPPED", **asdict(report))


if __name__ == "__main__":
    asyncio.run(run())
