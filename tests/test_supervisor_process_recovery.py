"""Actual processes + SQLite + independent effect oracle; no REAL PROVIDER."""
import json
import os
from pathlib import Path
import queue
import subprocess
import sys
import threading
import time

import pytest

from control.provision import provision
from runtime.action_journal import SQLiteActionJournal, JournalLeaseError, JournalError
from runtime.credential_input import read_credential_file
from runtime.speculative_saga import ActionState, CompensationState
from runtime.worker_supervisor import WorkerSupervisor
from tests.test_worker_supervisor import factory


def prepare(tmp_path):
    clock = tmp_path / "clock.txt"
    clock.write_text("1000")
    journal = SQLiteActionJournal(tmp_path / "journal.db", clock_ms=lambda: int(clock.read_text()))
    secret = tmp_path / "worker.secret"
    provision(journal, "worker", {"ccs:work", "test:write", "ccs:repair", "ccs:reconcile"}, secret, console_access=False)
    ledger = tmp_path / "provider.jsonl"
    return journal, secret, ledger, clock


def command(journal, secret, ledger, clock, mode):
    return [sys.executable, str(Path(__file__).with_name("supervisor_process_worker.py")),
            str(journal.path), str(secret), str(ledger), str(clock), mode]


def effects(ledger):
    return [json.loads(line)["direction"] for line in ledger.read_text().splitlines()]


def advance_clock(path):
    # The child may read its controlled clock during a heartbeat. Never expose
    # a partially rewritten value. Windows may briefly hold the old file open.
    pending = path.with_suffix(".next")
    pending.write_text("31000")
    deadline = time.monotonic() + 5
    while True:
        try:
            os.replace(pending, path)
            return
        except PermissionError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(.01)


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["forward_crash", "compensation_crash"])
async def test_process_death_after_apply_preserves_unknown_and_never_replays(tmp_path, mode):
    journal, secret, ledger, clock = prepare(tmp_path)
    result = subprocess.run(command(journal, secret, ledger, clock, mode), capture_output=True, text=True, timeout=20)
    assert result.returncode == 23, result.stderr
    assert not any(event["kind"] == "RELEASED" for event in journal.worker_events())
    advance_clock(clock)  # controlled test clock; original lease now expired
    fresh = SQLiteActionJournal(journal.path, clock_ms=lambda: int(clock.read_text()))
    host = WorkerSupervisor(fresh, read_credential_file(secret), monotonic=lambda: int(clock.read_text()) / 1000)
    saga = await host.start(factory)
    try:
        action = next(iter(saga._actions.values()))
        assert action.recovered and action.compensate is None
        if mode == "forward_crash":
            assert effects(ledger) == ["FORWARD"]
            assert action.state == ActionState.IN_FLIGHT_UNKNOWN
            with pytest.raises(JournalError, match="Recovered"):
                await saga.commit_write(action.action_id, lambda _: pytest.fail("crashed mutation replayed"))
        else:
            assert effects(ledger) == ["FORWARD", "COMPENSATION"]
            assert action.state == ActionState.COMMITTED_STALE and action.compensation_state == CompensationState.UNKNOWN
            await saga.compensate_action(action.action_id)
            assert effects(ledger) == ["FORWARD", "COMPENSATION"]
        assert fresh._lease.generation == 2
    finally:
        await host.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["graceful", "timeout"])
async def test_process_drain_and_late_result_takeover_protocol(tmp_path, mode):
    journal, secret, ledger, clock = prepare(tmp_path)
    process = subprocess.Popen(command(journal, secret, ledger, clock, mode), stdin=subprocess.PIPE,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                               creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
    messages = queue.Queue()
    def reader():
        for line in process.stdout:
            messages.put(json.loads(line))
    thread = threading.Thread(target=reader, daemon=True)
    thread.start()
    replacement = None
    def receive(event):
        message = messages.get(timeout=15)
        assert message["event"] == event, message
        return message
    def send(message):
        process.stdin.write(message + "\n")
        process.stdin.flush()
    try:
        receive("APPLIED")
        send("drain")
        stopped = receive("TIMEOUT" if mode == "timeout" else "DRAINING")
        with pytest.raises(JournalLeaseError):
            SQLiteActionJournal(journal.path, clock_ms=lambda: int(clock.read_text())).acquire_lease("premature-replacement")
        if mode == "timeout":
            assert not stopped["drained"] and not stopped["lease_released"]
            assert stopped["pending_operations"] == 1
            advance_clock(clock)
            fresh = SQLiteActionJournal(journal.path, clock_ms=lambda: int(clock.read_text()))
            replacement = WorkerSupervisor(fresh, read_credential_file(secret), monotonic=lambda: int(clock.read_text()) / 1000)
            recovered = await replacement.start(factory)
            assert next(iter(recovered._actions.values())).state == ActionState.IN_FLIGHT_UNKNOWN
        send("return")
        returned = receive("LATE_FENCED" if mode == "timeout" else "RESULT_RETAINED")
        assert returned["receipt"]["provider"] == "SIMULATED"
        final = receive("STOPPED")
        assert final["lease_released"] is (mode == "graceful")
        process.wait(timeout=15)
        assert process.returncode == 0, process.stderr.read()
        assert effects(ledger) == ["FORWARD"]
        record = journal.records()[0][1]
        assert record["state"] == ("IN_FLIGHT" if mode == "timeout" else "COMMITTED")
        if mode == "graceful":
            assert record["result"]["receipt"] == "sim-effect-1"
    finally:
        if process.poll() is None:
            process.kill()  # test-fixture teardown only, never a rollback claim
            process.wait(timeout=5)
        process.stdin.close()
        thread.join(timeout=2)
        process.stdout.close()
        process.stderr.close()
        if replacement is not None:
            await replacement.stop()
