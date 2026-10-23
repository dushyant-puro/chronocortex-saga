"""Actual supervised journal/workflow, SIMULATED effects; voice has no authority."""
import asyncio
from types import SimpleNamespace

import pytest

from runtime.action_journal import SQLiteActionJournal
from runtime.policy_engine import ConfirmationRequiredError
from runtime.speculative_saga import SpeculativeSagaManager, TurnEpochClock, ActionState
from runtime.task_engine import TaskManager, TaskStep, TaskState
from runtime.tool_contract import ToolManifest, ToolRegistry
from runtime.worker_supervisor import WorkerSupervisor
from runtime.workflow_contract import WorkflowVersion, StepVersions
from voice.livekit_observer import LiveKitEvidenceStream
from runtime.voice_ingress import VoiceIngress
from test_livekit_voice_observer import event


class Tasks(TaskManager):
    def __init__(self, saga, executor):
        super().__init__(saga, lambda _: None)
        self.executor = executor

    def _get_executor(self, step):
        return self.executor


async def environment(tmp_path, executor, *, confirmation=False):
    journal = SQLiteActionJournal(tmp_path / "voice-independent-task.db")
    journal.security.provision_principal("worker", {"ccs:work", "test:write"})
    credential = journal.security.issue_credential("worker")
    host = WorkerSupervisor(journal, credential.token)
    def factory(owned, auth):
        registry = ToolRegistry()
        registry.register(ToolManifest("SIMULATED_change", "write", required_permissions=["test:write"],
                                       requires_confirmation=confirmation))
        saga = SpeculativeSagaManager(TurnEpochClock(), journal=owned, registry=registry, auth_context=auth)
        tasks = Tasks(saga, executor)
        return SimpleNamespace(saga=saga, tasks=tasks)
    runtime = await host.start(factory)
    steps = [TaskStep("SIMULATED_change", build_args=lambda t, i: {"index": i},
                      versions=StepVersions(f"step-{i}", "v1", "SIMULATED:v1", "SIMULATED", "local-ledger"))
             for i in range(2)]
    task = runtime.tasks.create_task("durable independent objective", steps, workflow=WorkflowVersion("SIMULATED:voice-isolation", "v1"))
    return host, runtime, task


def conversational_events():
    capture = LiveKitEvidenceStream(VoiceIngress())
    capture.feed(event())
    capture.feed(event("yes cancel this task"))
    capture.feed(event("Chennai wait Bengaluru", final=True))
    capture.complete("Chennai wait Bengaluru", "voice-turn-1")
    capture.feed(event())  # A separate conversational turn, with no task command.
    capture.close()  # Voice process/stream loss clears only ephemeral evidence.


@pytest.mark.asyncio
@pytest.mark.parametrize("unknown", [False, True])
async def test_durable_two_step_work_is_independent_of_voice_revisions_and_disconnect(tmp_path, unknown):
    entered, release = asyncio.Event(), asyncio.Event()
    ledger = []
    async def execute(args):
        ledger.append(args["index"])
        if args["index"] == 0:
            entered.set()
            await release.wait()
            if unknown:
                raise ConnectionError("SIMULATED effect applied, acknowledgement lost")
        return {"SIMULATED_receipt": args["index"]}
    host, runtime, task = await environment(tmp_path, execute)
    operation = host.submit(lambda _: runtime.tasks.run_task(task.task_id))
    try:
        await asyncio.wait_for(entered.wait(), 3)
        before_epoch = runtime.saga.epoch_clock.current
        conversational_events()
        assert runtime.saga.epoch_clock.current == before_epoch
        assert task.state == TaskState.RUNNING and ledger == [0]
        release.set()
        await asyncio.wait_for(operation.wait(), 3)
        first = runtime.saga._actions[task.step_action_ids[0]]
        if unknown:
            assert task.state == TaskState.NEEDS_RECONCILIATION
            assert first.state == ActionState.IN_FLIGHT_UNKNOWN and ledger == [0]
            conversational_events()
            assert task.state == TaskState.NEEDS_RECONCILIATION and ledger == [0]
        else:
            assert task.state == TaskState.COMPLETED and ledger == [0, 1]
            assert all(record["state"] == "COMMITTED" for _, record in runtime.saga.journal.records())
    finally:
        release.set()
        await asyncio.wait_for(operation.wait(), 3)
        report = await host.stop()
        assert report.drained and report.lease_released


@pytest.mark.asyncio
async def test_spoken_yes_and_cancel_cannot_approve_or_cancel_parked_durable_work(tmp_path):
    async def execute(_):
        pytest.fail("Spoken text authorized a remote side effect")
    host, runtime, task = await environment(tmp_path, execute, confirmation=True)
    try:
        operation = host.submit(lambda _: runtime.tasks.run_task(task.task_id))
        with pytest.raises(ConfirmationRequiredError):
            await asyncio.wait_for(operation.wait(), 3)
        assert task.state == TaskState.WAITING_FOR_AUTHORIZATION
        before = runtime.saga.journal.records()
        conversational_events()
        assert task.state == TaskState.WAITING_FOR_AUTHORIZATION
        assert runtime.saga.journal.records() == before
        assert before[0][1]["state"] == "PENDING"
    finally:
        report = await host.stop()
        assert report.drained and report.lease_released
