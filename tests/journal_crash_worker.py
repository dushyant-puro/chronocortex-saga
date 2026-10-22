"""Subprocess-only fault fixture: os._exit skips Python cleanup and compensation."""
import asyncio
import json
import os
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from runtime.action_journal import SQLiteActionJournal
from runtime.policy_engine import AuthorizationContext
from runtime.speculative_saga import SpeculativeSagaManager, TurnEpochClock
from runtime.tool_contract import ToolManifest, ToolRegistry
from runtime.task_engine import TaskManager, TaskStep
from tests.lease_test_support import shared_test_journal
from tests.workflow_test_support import VersionedTestTasks
from tests.security_test_support import AuthenticatedTestSaga


async def run():
    path, ledger, window = sys.argv[1:]

    class CrashJournal(SQLiteActionJournal):
        def transition(self, record, version, kind, **kwargs):
            result = super().transition(record, version, kind, **kwargs)
            if (window, kind) in {("intent", "DISPATCH_INTENT"),
                                  ("committed", "FORWARD_COMMITTED"),
                                  ("compensation_intent", "COMPENSATION_INTENT"),
                                  ("compensated", "COMPENSATED"),
                                  ("replan_compensated", "COMPENSATED")}:
                os._exit(23)
            return result

        def save_task(self, task_id, checkpoint, version):
            result = super().save_task(task_id, checkpoint, version)
            if (window == "replan_generation" and checkpoint["step_generations"] == [1]
                    and checkpoint["replan_from"] is not None):
                os._exit(23)
            return result

    def effect(name):
        # Independent SIMULATED provider ledger; not a saga-state oracle.
        with open(ledger, "a", encoding="utf-8") as stream:
            stream.write(json.dumps(name) + "\n")
            stream.flush()
            os.fsync(stream.fileno())

    async def execute(args):
        effect("forward")
        if window == "applied":
            os._exit(23)
        return {"id": "simulated-provider-operation"}

    async def undo(result):
        effect("undo")
        if window == "compensation_applied":
            os._exit(23)

    registry = ToolRegistry()
    registry.register(ToolManifest("change", "write"))
    saga = AuthenticatedTestSaga(TurnEpochClock(), auth_context=AuthorizationContext(),
                                  journal=shared_test_journal(path, CrashJournal), registry=registry)
    if window.startswith("replan_"):
        class Tasks(VersionedTestTasks, TaskManager):
            def _get_executor(self, step):
                return execute

        tasks = Tasks(saga, lambda _: None)
        task = tasks.create_task("crash during replan", [TaskStep("change", compensate=undo)])
        await tasks.run_task(task.task_id)
        await tasks._replan_from_step(task, 0)
        raise AssertionError("Replan crash window was not reached")
    action = saga.stage_write("change", {}, compensate=undo, manifest=ToolManifest("change", "write"))
    if window == "staged":
        os._exit(23)
    await saga.commit_write(action.action_id, execute)
    await saga.compensate_action(action.action_id)
    raise AssertionError("Crash window was not reached")


if __name__ == "__main__":
    asyncio.run(run())
