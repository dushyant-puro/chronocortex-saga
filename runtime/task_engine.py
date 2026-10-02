"""
runtime/task_engine.py

Uninterruptible Task Engine (Phase 6).

Core principle: "voice turn != task."
A task is a multi-step unit of work that can span many conversational turns
and many epoch advances. An epoch advance (barge-in, self-correction) must
NEVER by itself cancel, pause, or corrupt a running task. Only three things
may change a task's lifecycle:
  1. Explicit user cancellation ("stop", "cancel that")
  2. Normal task completion (all steps committed)
  3. Unrecoverable failure (step failure after compensation)

Every task step dispatches through the EXISTING SpeculativeSagaManager.
The task engine is purely an orchestration layer on top — it is NOT a
second dispatch path, and it does NOT replace or duplicate the saga's
state machine.

Re-grounding on epoch advance:
  When an epoch advance invalidates a grounded value that a pending (not yet
  dispatched) task step depends on, the task engine replans that step by
  re-resolving the field via GroundingGuard.resolve_current_value(). If
  resolution returns None, the step is parked (WAITING_FOR_GROUNDING) until
  the field is re-grounded. The task itself remains RUNNING — it does not
  abort or pause at the task level.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Awaitable, Callable, Optional

from runtime.speculative_saga import (
    ActionState,
    DuplicateOperationError,
    SagaAbortedError,
    SpeculativeSagaManager,
    StaleEpochError,
)

logger = logging.getLogger("ccs.task_engine")


# --------------------------------------------------------------------------
# Task lifecycle states
# --------------------------------------------------------------------------

class TaskState(Enum):
    """Lifecycle states for a multi-step task."""
    PENDING = auto()                 # created, no steps dispatched yet
    RUNNING = auto()                 # at least one step is in flight or committed
    WAITING_FOR_GROUNDING = auto()   # a step's required field was evicted; waiting for re-grounding
    COMPLETED = auto()               # all steps successfully committed
    FAILED = auto()                  # unrecoverable step failure (after compensation)
    CANCELLED = auto()               # user explicitly cancelled


class StepState(Enum):
    """Lifecycle states for an individual task step."""
    PENDING = auto()                 # not yet dispatched
    IN_FLIGHT = auto()               # dispatched through saga, awaiting result
    COMMITTED = auto()               # saga confirmed COMMITTED
    FAILED = auto()                  # saga reports ABORTED/failure
    WAITING_FOR_GROUNDING = auto()   # required grounded field was evicted
    SKIPPED = auto()                 # skipped due to upstream failure/cancel


# --------------------------------------------------------------------------
# TaskStep — a single step in a multi-step task
# --------------------------------------------------------------------------

@dataclass
class TaskStep:
    """
    Defines one step in a multi-step task.

    tool_name: which tool to call (must be in WRITE_TOOLS or READ_TOOLS)
    required_fields: list of field_name strings that must be resolvable via
        GroundingGuard.resolve_current_value() before this step can dispatch.
        If any field is evicted (returns None), the step parks itself.
    build_args: a callable (task, step_index) -> dict[str, Any] that builds
        the args dict for the saga stage_write/fire_speculative_read call.
        This is called lazily at dispatch time, NOT at task creation time,
        so re-grounding works naturally.
    kind: "read" or "write" — determines whether to use fire_speculative_read
        or stage_write + commit_write.
    compensate: optional compensation handler (for writes).
    """
    tool_name: str
    required_fields: list[str] = field(default_factory=list)
    build_args: Optional[Callable[["Task", int], dict[str, Any]]] = None
    kind: str = "write"
    compensate: Optional[Callable[[Any], Awaitable[None]]] = None


# --------------------------------------------------------------------------
# Task — a multi-step unit of work
# --------------------------------------------------------------------------

@dataclass
class Task:
    """
    Represents a multi-step task that survives epoch advances.
    """
    task_id: str
    name: str
    steps: list[TaskStep]
    state: TaskState = TaskState.PENDING
    step_states: list[StepState] = field(default_factory=list)
    step_results: list[Any] = field(default_factory=list)
    step_action_ids: list[Optional[str]] = field(default_factory=list)
    current_step_index: int = 0
    created_at: float = field(default_factory=time.monotonic)
    completed_at: Optional[float] = None
    error: Optional[str] = None

    def __post_init__(self) -> None:
        if not self.step_states:
            self.step_states = [StepState.PENDING] * len(self.steps)
        if not self.step_results:
            self.step_results = [None] * len(self.steps)
        if not self.step_action_ids:
            self.step_action_ids = [None] * len(self.steps)


# --------------------------------------------------------------------------
# TaskManager — orchestration layer on top of SpeculativeSagaManager
# --------------------------------------------------------------------------

class TaskManager:
    """
    Orchestrates multi-step tasks. Every step dispatches through the existing
    SpeculativeSagaManager — this class adds ONLY:
      - step sequencing (steps run in order)
      - re-grounding on epoch-triggered eviction
      - task-level lifecycle that survives epoch advances
      - compensation unwind on step failure

    It does NOT introduce a second dispatch path.
    """

    def __init__(
        self,
        saga: SpeculativeSagaManager,
        resolve_field: Callable[[str], Optional[str]],
    ) -> None:
        """
        Args:
            saga: the existing SpeculativeSagaManager instance
            resolve_field: typically GroundingGuard.resolve_current_value
        """
        self._saga = saga
        self._resolve_field = resolve_field
        self._tasks: dict[str, Task] = {}
        self._evicted_fields: set[str] = set()

    # ---- task creation --------------------------------------------------

    def create_task(self, name: str, steps: list[TaskStep]) -> Task:
        """Create a new task with the given steps. Does NOT start execution."""
        task_id = f"task-{uuid.uuid4().hex[:8]}"
        task = Task(task_id=task_id, name=name, steps=steps)
        self._tasks[task_id] = task
        logger.info("created task %s (%s) with %d steps", task_id, name, len(steps))
        return task

    # ---- eviction notification (wired from GroundingGuard) ---------------

    def on_field_evicted(self, field_name: str, evicted_value: str) -> None:
        """
        Called by GroundingGuard's on_eviction hook when a staged candidate
        is tombstone-evicted. Records the evicted field so that running tasks
        can detect that a required field needs re-grounding.
        """
        self._evicted_fields.add(field_name)
        logger.info("field evicted: %s (value was %r)", field_name, evicted_value)

        # Check all running tasks: if any pending step depends on this field,
        # park it at WAITING_FOR_GROUNDING.
        for task in self._tasks.values():
            if task.state not in (TaskState.RUNNING, TaskState.PENDING):
                continue
            idx = task.current_step_index
            if idx < len(task.steps):
                step = task.steps[idx]
                if field_name in step.required_fields:
                    task.step_states[idx] = StepState.WAITING_FOR_GROUNDING
                    task.state = TaskState.WAITING_FOR_GROUNDING
                    logger.info(
                        "task %s step %d parked: field %s evicted",
                        task.task_id, idx, field_name,
                    )

    # ---- field re-grounding check ---------------------------------------

    def _check_fields_resolved(self, step: TaskStep) -> bool:
        """Return True if all required_fields for this step currently resolve."""
        for f in step.required_fields:
            if self._resolve_field(f) is None:
                return False
        return True

    def try_resume_waiting_tasks(self) -> list[str]:
        """
        Check all WAITING_FOR_GROUNDING tasks. If the required fields are
        now resolvable, move them back to RUNNING (step back to PENDING).
        Returns list of task_ids that were resumed.

        Called by the agent layer after new grounding data arrives.
        """
        resumed: list[str] = []
        for task in self._tasks.values():
            if task.state != TaskState.WAITING_FOR_GROUNDING:
                continue
            idx = task.current_step_index
            if idx >= len(task.steps):
                continue
            step = task.steps[idx]
            if self._check_fields_resolved(step):
                task.step_states[idx] = StepState.PENDING
                task.state = TaskState.RUNNING
                # Clear the evicted field now that it's re-grounded
                for f in step.required_fields:
                    self._evicted_fields.discard(f)
                resumed.append(task.task_id)
                logger.info(
                    "task %s step %d resumed: fields re-grounded",
                    task.task_id, idx,
                )
        return resumed

    # ---- task execution -------------------------------------------------

    async def run_task(self, task_id: str) -> Task:
        """
        Execute a task's steps sequentially. Each step dispatches through
        the existing SpeculativeSagaManager.

        If a step's required fields are not resolved, the task parks itself
        at WAITING_FOR_GROUNDING and returns. The caller should call
        resume_task() after re-grounding.

        On step failure: compensates all previously committed steps in reverse
        order (via saga.abort_chain_from) and sets task state to FAILED.
        """
        task = self._tasks[task_id]
        if task.state == TaskState.CANCELLED:
            return task
        task.state = TaskState.RUNNING

        while task.current_step_index < len(task.steps):
            if task.state == TaskState.CANCELLED:
                # Mark remaining steps as SKIPPED
                for i in range(task.current_step_index, len(task.steps)):
                    task.step_states[i] = StepState.SKIPPED
                return task

            idx = task.current_step_index
            step = task.steps[idx]

            # Check required fields are grounded
            if not self._check_fields_resolved(step):
                task.step_states[idx] = StepState.WAITING_FOR_GROUNDING
                task.state = TaskState.WAITING_FOR_GROUNDING
                logger.info(
                    "task %s step %d waiting for grounding: %s",
                    task.task_id, idx, step.required_fields,
                )
                return task

            # Build args using the lazy builder
            if step.build_args is not None:
                args = step.build_args(task, idx)
            else:
                args = {}

            # Dispatch through saga
            try:
                if step.kind == "write":
                    result = await self._dispatch_write_step(task, idx, step, args)
                else:
                    result = await self._dispatch_read_step(task, idx, step, args)

                task.step_results[idx] = result
                task.step_states[idx] = StepState.COMMITTED
                task.current_step_index += 1
                logger.info(
                    "task %s step %d (%s) committed",
                    task.task_id, idx, step.tool_name,
                )

            except StaleEpochError:
                # Epoch advanced before dispatch — NOT a task failure.
                # Re-check grounding and retry the same step.
                logger.info(
                    "task %s step %d (%s) StaleEpochError — will re-ground and retry",
                    task.task_id, idx, step.tool_name,
                )
                # Re-check fields — they may have been evicted
                if not self._check_fields_resolved(step):
                    task.step_states[idx] = StepState.WAITING_FOR_GROUNDING
                    task.state = TaskState.WAITING_FOR_GROUNDING
                    return task
                # Fields still good — retry immediately (stay at same index)
                continue

            except (SagaAbortedError, DuplicateOperationError, asyncio.TimeoutError) as exc:
                # Unrecoverable step failure
                task.step_states[idx] = StepState.FAILED
                task.state = TaskState.FAILED
                task.error = f"step {idx} ({step.tool_name}): {exc}"
                task.completed_at = time.monotonic()
                logger.error(
                    "task %s step %d (%s) failed: %s — compensating",
                    task.task_id, idx, step.tool_name, exc,
                )
                # Compensate previously committed steps
                await self._compensate_committed_steps(task, idx)
                return task

            except Exception as exc:  # noqa: BLE001
                task.step_states[idx] = StepState.FAILED
                task.state = TaskState.FAILED
                task.error = f"step {idx} ({step.tool_name}): {exc}"
                task.completed_at = time.monotonic()
                logger.error(
                    "task %s step %d (%s) unexpected failure: %s — compensating",
                    task.task_id, idx, step.tool_name, exc,
                )
                await self._compensate_committed_steps(task, idx)
                return task

        # All steps completed
        task.state = TaskState.COMPLETED
        task.completed_at = time.monotonic()
        logger.info("task %s completed: all %d steps committed", task.task_id, len(task.steps))
        return task

    async def resume_task(self, task_id: str) -> Task:
        """
        Resume a WAITING_FOR_GROUNDING task. Delegates to run_task().
        """
        task = self._tasks[task_id]
        if task.state not in (TaskState.WAITING_FOR_GROUNDING, TaskState.RUNNING):
            logger.warning(
                "resume_task called on task %s in state %s — no-op",
                task_id, task.state.name,
            )
            return task
        task.state = TaskState.RUNNING
        idx = task.current_step_index
        if idx < len(task.steps):
            task.step_states[idx] = StepState.PENDING
        return await self.run_task(task_id)

    # ---- cancellation ---------------------------------------------------

    def cancel_task(self, task_id: str) -> Task:
        """
        Explicitly cancel a task. This is the ONLY way to stop a task other
        than completion or unrecoverable failure.
        """
        task = self._tasks[task_id]
        if task.state in (TaskState.COMPLETED, TaskState.FAILED):
            logger.warning(
                "cancel_task called on task %s in terminal state %s — no-op",
                task_id, task.state.name,
            )
            return task
        task.state = TaskState.CANCELLED
        task.completed_at = time.monotonic()
        # Mark pending/waiting steps as SKIPPED
        for i in range(task.current_step_index, len(task.steps)):
            if task.step_states[i] in (StepState.PENDING, StepState.WAITING_FOR_GROUNDING):
                task.step_states[i] = StepState.SKIPPED
        logger.info("task %s cancelled by user", task_id)
        return task

    # ---- dispatch helpers (all go through saga) -------------------------

    async def _dispatch_write_step(
        self,
        task: Task,
        idx: int,
        step: TaskStep,
        args: dict[str, Any],
    ) -> Any:
        """Stage and commit a write through the existing saga manager."""
        action = self._saga.stage_write(
            step.tool_name,
            args,
            compensate=step.compensate,
        )
        task.step_action_ids[idx] = action.action_id
        task.step_states[idx] = StepState.IN_FLIGHT
        result = await self._saga.commit_write(action.action_id, self._get_executor(step))
        return result

    async def _dispatch_read_step(
        self,
        task: Task,
        idx: int,
        step: TaskStep,
        args: dict[str, Any],
    ) -> Any:
        """Fire a speculative read through the existing saga manager."""
        entity_hash = f"task-{task.task_id}-step-{idx}"
        action = self._saga.fire_speculative_read(
            step.tool_name,
            args,
            executor=self._get_executor(step),
            entity_hash=entity_hash,
        )
        task.step_action_ids[idx] = action.action_id
        task.step_states[idx] = StepState.IN_FLIGHT
        # Wait for the read to complete
        if action._task:
            await action._task
        if action.state == ActionState.COMMITTED:
            return action.result
        raise SagaAbortedError(
            f"read step {step.tool_name} did not commit: {action.state.name}"
        )

    def _get_executor(self, step: TaskStep) -> Callable[[dict[str, Any]], Any]:
        """
        Get the executor function for a step. Uses the tool_name to look up
        from the registered tools. This is intentionally simple — the actual
        executor binding happens in the agent layer or fleet_tools.
        """
        # This is resolved by the build_args / or by direct executor reference
        # For now, we return a passthrough that the caller must override
        # via step configuration. The real executors come from READ_TOOLS/WRITE_TOOLS.
        raise NotImplementedError(
            f"Step executor for {step.tool_name} must be provided via "
            f"TaskManager dispatch methods"
        )

    async def _compensate_committed_steps(self, task: Task, failed_idx: int) -> None:
        """
        Compensate all previously committed steps in reverse order.
        Uses individual step compensate handlers directly, not saga.abort_chain_from,
        because task steps may span multiple epoch chains.
        """
        for i in range(failed_idx - 1, -1, -1):
            if task.step_states[i] != StepState.COMMITTED:
                continue
            step = task.steps[i]
            result = task.step_results[i]
            if step.compensate is not None and result is not None:
                try:
                    await step.compensate(result)
                    task.step_states[i] = StepState.SKIPPED  # compensated
                    logger.info(
                        "task %s step %d (%s) compensated",
                        task.task_id, i, step.tool_name,
                    )
                except Exception:  # noqa: BLE001
                    logger.critical(
                        "COMPENSATION FAILED for task %s step %d (%s) — "
                        "MANUAL RECONCILIATION REQUIRED",
                        task.task_id, i, step.tool_name,
                        exc_info=True,
                    )

    # ---- inspection ------------------------------------------------------

    def get_task(self, task_id: str) -> Optional[Task]:
        return self._tasks.get(task_id)

    def snapshot(self) -> list[dict[str, Any]]:
        """Return a snapshot of all tasks for debug/observability."""
        return [
            {
                "task_id": t.task_id,
                "name": t.name,
                "state": t.state.name,
                "current_step": t.current_step_index,
                "total_steps": len(t.steps),
                "step_states": [s.name for s in t.step_states],
                "error": t.error,
                "age_ms": round((time.monotonic() - t.created_at) * 1000, 1),
            }
            for t in self._tasks.values()
        ]
