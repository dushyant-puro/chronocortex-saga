"""Crash after a SIMULATED provider effect, before forward outcome persistence."""
import asyncio
import json
import os
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from runtime.policy_engine import AuthorizationContext
from runtime.speculative_saga import SpeculativeSagaManager, TurnEpochClock
from runtime.task_engine import TaskManager, TaskStep
from runtime.tool_contract import ToolManifest, ToolRegistry
from runtime.workflow_contract import WorkflowVersion, StepVersions
from tests.lease_test_support import shared_test_journal


async def run():
    path, ledger = sys.argv[1:]
    registry = ToolRegistry()
    registry.register(ToolManifest("change", "write"))
    manager = SpeculativeSagaManager(TurnEpochClock(), registry=registry,
                                    auth_context=AuthorizationContext(), journal=shared_test_journal(path))

    async def execute(args):
        with open(ledger, "w", encoding="utf-8") as stream:
            stream.write(json.dumps("SIMULATED forward") + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        os._exit(23)

    class Tasks(TaskManager):
        def _get_executor(self, step):
            return execute

    tasks = Tasks(manager, lambda _: None)
    step = TaskStep("change", versions=StepVersions(
        "change", "empty:v1", "execute:v1", "provider:v1", "SIMULATED:local-test"))
    task = tasks.create_task("SIMULATED crash", [step], workflow=WorkflowVersion("SIMULATED:crash-contract", "v1"))
    await tasks.run_task(task.task_id)
    raise AssertionError("crash window not reached")


if __name__ == "__main__":
    asyncio.run(run())
