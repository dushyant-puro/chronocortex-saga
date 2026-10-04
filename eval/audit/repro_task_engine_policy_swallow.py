"""
eval/audit/repro_task_engine_policy_swallow.py

Minimal reproduction script for the Phase 13 cross-layer bug:
TaskManager.run_task swallows PolicyError (ConfirmationRequiredError / PermissionDeniedError),
misclassifies policy denials as unexpected executor failures, corrupts task state to FAILED,
and erroneously compensates previously committed steps.

Core bug:
  In runtime/task_engine.py:
  run_task wraps step dispatch in:
      except Exception as exc:
          task.step_states[idx] = StepState.FAILED
          task.state = TaskState.FAILED
          task.error = f"step {idx} ({step.tool_name}): {exc}"
          await self._compensate_committed_steps(task, idx)
          return task

  Because ConfirmationRequiredError and PermissionDeniedError inherit from Exception,
  run_task catches them, suppresses them, sets task.state = FAILED, and triggers
  _compensate_committed_steps, rolling back previously committed steps and preventing
  resumption.
"""

import asyncio
import sys
from pathlib import Path

# Ensure repo root in sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from runtime.policy_engine import (
    ConfirmationRequiredError,
    PolicyEngine,
)
from runtime.speculative_saga import (
    ActionState,
    SpeculativeSagaManager,
    TurnEpochClock,
)
from runtime.task_engine import (
    StepState,
    TaskManager,
    TaskState,
    TaskStep,
)
from runtime.tool_contract import ToolManifest


async def main() -> None:
    print("=== Reproducing TaskManager PolicyError swallowing / task corruption ===")
    clock = TurnEpochClock()
    saga = SpeculativeSagaManager(clock)
    policy_engine = PolicyEngine()

    manifest_reroute = ToolManifest(
        tool_name="reroute_truck",
        kind="write",
        requires_confirmation=True,
    )

    compensated_steps: list[str] = []

    async def compensate_step0(res: dict) -> None:
        compensated_steps.append("step0_telemetry")

    class TestTaskManager(TaskManager):
        """TaskManager that wires policy_engine to commit_write."""
        async def _dispatch_write_step(self, task, idx, step, args):
            action = self._saga.stage_write(
                step.tool_name,
                args,
                compensate=step.compensate,
                manifest=manifest_reroute if step.tool_name == "reroute_truck" else None,
            )
            task.step_action_ids[idx] = action.action_id
            # Step 1 requires confirmation, but confirmed=False
            return await self._saga.commit_write(
                action.action_id,
                lambda a: asyncio.sleep(0.001),
                manifest=manifest_reroute if step.tool_name == "reroute_truck" else None,
                policy_engine=policy_engine,
                confirmed=False,
            )

    tm = TestTaskManager(saga=saga, resolve_field=lambda f: "val")

    # Step 0: committed telemetry lookup
    # Step 1: reroute_truck requiring confirmation
    task = tm.create_task(
        "fleet_task_repro",
        [
            TaskStep(tool_name="step0_telemetry", kind="write", compensate=compensate_step0),
            TaskStep(tool_name="reroute_truck", kind="write"),
        ],
    )

    # Simulate Step 0 already successfully committed
    task.step_states[0] = StepState.COMMITTED
    task.step_results[0] = {"truck": "t17"}
    task.current_step_index = 1

    print(f"[Init] Task {task.task_id}: Step 0 is COMMITTED, Step 1 is PENDING (requires confirmation)")

    # Execute run_task: Step 1 will raise ConfirmationRequiredError inside commit_write
    try:
        res = await tm.run_task(task.task_id)
        print(f"[1] run_task returned cleanly (swallowed exception!): task.state = {res.state.name}")
        print(f"[1] Step 0 state: {res.step_states[0].name}")
        print(f"[1] Step 1 state: {res.step_states[1].name}")
        print(f"[1] Compensated steps: {compensated_steps}")
        print(f"[1] Task error recorded: {res.error}")

        if res.state == TaskState.FAILED and "step0_telemetry" in compensated_steps:
            print("\n*** REAL CROSS-LAYER BUG REPRODUCED SUCCESSFULLY ***")
            print("ConfirmationRequiredError was caught by run_task's generic except Exception:,")
            print("causing task.state to become FAILED, rolling back Step 0 via compensation,")
            print("and preventing the task from being resumed.")
            sys.exit(0)
        else:
            print("\nBug did not reproduce as expected.")
            sys.exit(1)

    except ConfirmationRequiredError:
        print("\nConfirmationRequiredError surfaced cleanly (bug fixed or not present)!")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
