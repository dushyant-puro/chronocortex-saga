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

from runtime.persistence import CheckpointStore
from runtime.speculative_saga import (
    ActionKind,
    ActionState,
    DuplicateOperationError,
    SagaAbortedError,
    SpeculativeSagaManager,
    StagedAction,
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
    REPLANNING = auto()              # a committed step's dispatched args were invalidated;
                                     # compensation in progress, will rewind and redispatch
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
    COMPENSATED = auto()             # was COMMITTED, then compensated for replanning
    NEEDS_RECONCILIATION = auto()    # was IN_FLIGHT at restart; requires status_check before advancing


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
    status_check: optional reconciliation handler for NEEDS_RECONCILIATION steps.
    """
    tool_name: str
    required_fields: list[str] = field(default_factory=list)
    build_args: Optional[Callable[["Task", int], dict[str, Any]]] = None
    kind: str = "write"
    compensate: Optional[Callable[[Any], Awaitable[None]]] = None
    status_check: Optional[Callable[[Any], Awaitable[Optional[bool]]]] = None


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
    step_dispatched_args: list[Optional[dict[str, Any]]] = field(default_factory=list)
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
        if not self.step_dispatched_args:
            self.step_dispatched_args = [None] * len(self.steps)


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
      - optional durable checkpoint persistence and process-restart recovery

    It does NOT introduce a second dispatch path.
    """

    def __init__(
        self,
        saga: SpeculativeSagaManager,
        resolve_field: Callable[[str], Optional[str]],
        store: Optional[CheckpointStore] = None,
        on_task_replanned: Optional[Callable[[str, dict[str, Any]], None]] = None,
    ) -> None:
        """
        Args:
            saga: the existing SpeculativeSagaManager instance
            resolve_field: typically GroundingGuard.resolve_current_value
            store: optional CheckpointStore for task persistence (default None)
            on_task_replanned: optional callback invoked when a replan successfully completes
        """
        self._saga = saga
        self._resolve_field = resolve_field
        self.store = store
        self.on_task_replanned = on_task_replanned
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

    def _build_replan_summary(self, task: Task, field_name: str = "") -> dict[str, Any]:
        summary: dict[str, Any] = {}
        if field_name:
            val = self._resolve_field(field_name)
            summary[field_name] = val
        for f in getattr(task, "_replanning_fields", set()):
            val = self._resolve_field(f)
            summary[f] = val
        for r in task.step_results:
            if isinstance(r, dict):
                summary.update(r)
        for d in task.step_dispatched_args:
            if isinstance(d, dict):
                summary.update(d)
        return summary

    def _notify_task_replanned(self, task: Task, field_name: str = "") -> None:
        if self.on_task_replanned is not None:
            summary = self._build_replan_summary(task, field_name)
            try:
                res = self.on_task_replanned(task.task_id, summary)
                if asyncio.iscoroutine(res):
                    asyncio.create_task(res)
            except Exception:
                logger.exception("Error in on_task_replanned callback for task %s", task.task_id)

    # ---- eviction notification (wired from GroundingGuard) ---------------

    def on_field_evicted(self, field_name: str, evicted_value: str) -> None:
        """
        Called by GroundingGuard's on_eviction hook when a staged candidate
        is tombstone-evicted.

        TASK-INV-5: if a COMMITTED step's recorded dispatched_args actually
        contained the evicted field and value (exact match), that step must be
        compensated and the task rewound to redispatch with corrected data.
        If no committed step used that value, only not-yet-dispatched steps are
        parked.
        """
        self._evicted_fields.add(field_name)
        logger.info("field evicted: %s (value was %r)", field_name, evicted_value)

        for task in self._tasks.values():
            if task.state in (TaskState.FAILED, TaskState.CANCELLED):
                continue

            # --- TASK-INV-5: check COMMITTED steps for exact field+value match ---
            rewind_to: Optional[int] = None
            for i in range(len(task.steps)):
                if task.step_states[i] != StepState.COMMITTED:
                    continue
                dispatched = task.step_dispatched_args[i]
                if dispatched is None:
                    continue

                # Exact match: the step's recorded dispatched args must contain
                # field_name as a key AND that key's value must equal evicted_value exactly.
                if dispatched.get(field_name) == evicted_value:
                    if rewind_to is None or i < rewind_to:
                        rewind_to = i

            if rewind_to is not None:
                # Compensate from the latest committed step down to rewind_to
                # and mark them for redispatch.
                task.state = TaskState.REPLANNING
                if not hasattr(task, "_replanning_fields"):
                    task._replanning_fields = set()
                task._replanning_fields.add(field_name)
                logger.info(
                    "task %s REPLANNING: committed step %d used evicted field %s=%r",
                    task.task_id, rewind_to, field_name, evicted_value,
                )
                # Schedule async compensation
                asyncio.ensure_future(
                    self._replan_from_step(task, rewind_to, field_name, evicted_value)
                )
                continue

            # --- not-yet-dispatched steps: park if they depend on this field ---
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

    async def _replan_from_step(
        self,
        task: Task,
        rewind_to: int,
        field_name: str = "",
        evicted_value: str = "",
    ) -> None:
        """
        Compensate all committed steps from the latest back to rewind_to
        (inclusive), reset them for redispatch, rewind current_step_index, and
        transition the task to WAITING_FOR_GROUNDING or RUNNING as appropriate.
        """
        upper_idx = (
            task.current_step_index
            if task.current_step_index < len(task.steps)
            else len(task.steps)
        )
        for i in range(upper_idx - 1, rewind_to - 1, -1):
            if task.step_states[i] != StepState.COMMITTED:
                continue
            step = task.steps[i]
            result = task.step_results[i]
            if step.compensate is not None and result is not None:
                try:
                    await step.compensate(result)
                    logger.info(
                        "task %s step %d (%s) compensated for replanning",
                        task.task_id, i, step.tool_name,
                    )
                except Exception:  # noqa: BLE001
                    logger.critical(
                        "COMPENSATION FAILED during replan for task %s step %d (%s) — "
                        "MANUAL RECONCILIATION REQUIRED",
                        task.task_id, i, step.tool_name,
                        exc_info=True,
                    )
                    task.state = TaskState.FAILED
                    task.error = f"compensation failed during replan at step {i}"
                    task.completed_at = time.monotonic()
                    return
            elif step.compensate is None and step.kind == "write":
                logger.critical(
                    "COMMITTED step %d (%s) used evicted field %s=%r but has NO "
                    "compensate() handler — MANUAL RECONCILIATION REQUIRED; remote "
                    "side-effect cannot be rolled back automatically",
                    i, step.tool_name, field_name, evicted_value,
                )

            # Mark step for redispatch
            task.step_states[i] = StepState.COMPENSATED
            task.step_results[i] = None
            task.step_dispatched_args[i] = None
            task.step_action_ids[i] = None

        # Rewind
        task.current_step_index = rewind_to
        task.completed_at = None

        # Transition back to RUNNING or WAITING_FOR_GROUNDING
        if self._check_fields_resolved(task.steps[rewind_to]):
            task.step_states[rewind_to] = StepState.PENDING
            task.state = TaskState.RUNNING
            await self.run_task(task.task_id)
        else:
            task.step_states[rewind_to] = StepState.WAITING_FOR_GROUNDING
            task.state = TaskState.WAITING_FOR_GROUNDING

        logger.info(
            "task %s rewound to step %d (state: %s)",
            task.task_id, rewind_to, task.state.name,
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

            # Check if step NEEDS_RECONCILIATION (restored from IN_FLIGHT state after restart)
            if task.step_states[idx] == StepState.NEEDS_RECONCILIATION:
                action_id = task.step_action_ids[idx]
                status_check = step.status_check

                if action_id is None or status_check is None:
                    task.step_states[idx] = StepState.FAILED
                    task.state = TaskState.FAILED
                    task.error = f"step {idx} ({step.tool_name}) NEEDS_RECONCILIATION but action_id or status_check is missing"
                    task.completed_at = time.monotonic()
                    self._save_checkpoint(task)
                    return task

                # Ensure action exists in saga so reconcile_write can be called on it
                action = self._saga._actions.get(action_id)
                if action is None:
                    action = StagedAction(
                        action_id=action_id,
                        kind=ActionKind.STAGED_WRITE,
                        tool_name=step.tool_name,
                        args=task.step_dispatched_args[idx] or {},
                        capture_epoch=self._saga.epoch_clock.current,
                        idempotency_key=action_id,
                        compensate=step.compensate,
                        state=ActionState.IN_FLIGHT_UNKNOWN,
                    )
                    self._saga._actions[action_id] = action

                try:
                    await self._saga.reconcile_write(action_id, status_check)
                    action = self._saga._actions.get(action_id)
                    if action and action.state in (ActionState.COMMITTED, ActionState.COMMITTED_STALE):
                        task.step_states[idx] = StepState.COMMITTED
                        task.step_results[idx] = action.result
                        task.current_step_index += 1
                        self._save_checkpoint(task)
                        logger.info("reconciled step %d (%s) -> COMMITTED", idx, step.tool_name)
                        continue
                    else:
                        task.step_states[idx] = StepState.FAILED
                        task.state = TaskState.FAILED
                        task.error = f"reconciliation for step {idx} ({step.tool_name}) failed"
                        task.completed_at = time.monotonic()
                        self._save_checkpoint(task)
                        return task
                except Exception as exc:
                    task.step_states[idx] = StepState.FAILED
                    task.state = TaskState.FAILED
                    task.error = f"reconciliation error for step {idx} ({step.tool_name}): {exc}"
                    task.completed_at = time.monotonic()
                    self._save_checkpoint(task)
                    return task

            # Check required fields are grounded
            if not self._check_fields_resolved(step):
                task.step_states[idx] = StepState.WAITING_FOR_GROUNDING
                task.state = TaskState.WAITING_FOR_GROUNDING
                logger.info(
                    "task %s step %d waiting for grounding: %s",
                    task.task_id, idx, step.required_fields,
                )
                self._save_checkpoint(task)
                return task

            # Build args using the lazy builder
            if step.build_args is not None:
                args = step.build_args(task, idx)
            else:
                args = {}

            # Record what args were actually dispatched (for TASK-INV-5
            # eviction matching — exact dispatched value comparison)
            task.step_dispatched_args[idx] = dict(args)

            # Dispatch through saga
            try:
                if step.kind == "write":
                    result = await self._dispatch_write_step(task, idx, step, args)
                else:
                    result = await self._dispatch_read_step(task, idx, step, args)

                task.step_results[idx] = result
                task.step_states[idx] = StepState.COMMITTED
                task.current_step_index += 1
                self._save_checkpoint(task)
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
                    self._save_checkpoint(task)
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
                self._save_checkpoint(task)
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
                self._save_checkpoint(task)
                return task

        # All steps completed
        task.state = TaskState.COMPLETED
        task.completed_at = time.monotonic()
        self._save_checkpoint(task)
        logger.info("task %s completed: all %d steps committed", task.task_id, len(task.steps))
        if getattr(task, "_replanning_fields", None):
            self._notify_task_replanned(task)
            task._replanning_fields.clear()
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

    async def cancel_task(self, task_id: str) -> Task:
        """
        Explicitly cancel a task. This is the ONLY way to stop a task other
        than completion or unrecoverable failure.

        Compensates all committed steps in reverse order before marking CANCELLED.
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

        # Compensate committed steps in reverse order
        for i in range(len(task.steps) - 1, -1, -1):
            if task.step_states[i] != StepState.COMMITTED:
                continue
            step = task.steps[i]
            result = task.step_results[i]
            if step.compensate is not None and result is not None:
                try:
                    await step.compensate(result)
                    task.step_states[i] = StepState.COMPENSATED
                    logger.info(
                        "task %s step %d (%s) compensated on cancel",
                        task.task_id, i, step.tool_name,
                    )
                except Exception:  # noqa: BLE001
                    logger.critical(
                        "COMPENSATION FAILED during cancel for task %s step %d (%s)",
                        task.task_id, i, step.tool_name,
                        exc_info=True,
                    )

        # Mark pending/waiting steps as SKIPPED
        for i in range(len(task.steps)):
            if task.step_states[i] in (StepState.PENDING, StepState.WAITING_FOR_GROUNDING):
                task.step_states[i] = StepState.SKIPPED
        logger.info("task %s cancelled by user", task_id)
        self._save_checkpoint(task)
        return task

    # ---- persistence & checkpointing ------------------------------------

    def _save_checkpoint(self, task: Task) -> None:
        """Save a task checkpoint to the configured store, if present."""
        if self.store is not None:
            try:
                cp = self.checkpoint(task.task_id)
                self.store.save(task.task_id, cp)
            except Exception:
                logger.exception("Failed to save checkpoint for task %s", task.task_id)

    def restore_task(
        self,
        task_id: str,
        steps: Optional[list[TaskStep]] = None,
    ) -> Optional[Task]:
        """
        Restore a task from the persistence store by task_id.
        Reconstructs the Task object and marks any step that was IN_FLIGHT or
        IN_FLIGHT_UNKNOWN at checkpoint time as NEEDS_RECONCILIATION rather than
        trusting its last-known state.

        Idempotent: calling restore_task multiple times on the same task_id
        returns the same task instance without duplicate dispatch or state reset.
        """
        if task_id in self._tasks:
            return self._tasks[task_id]

        if self.store is None:
            logger.warning("restore_task called but no CheckpointStore is configured")
            return None

        cp = self.store.load(task_id)
        if cp is None:
            logger.warning("No checkpoint found for task %s", task_id)
            return None

        if steps is None:
            num_steps = len(cp.get("step_states", []))
            steps = [
                TaskStep(
                    tool_name=f"restored_step_{i}",
                    kind="write",
                )
                for i in range(num_steps)
            ]

        task = Task(
            task_id=cp["task_id"],
            name=cp["name"],
            steps=steps,
            state=TaskState[cp["state"]],
            step_states=[StepState[s] for s in cp["step_states"]],
            step_results=list(cp.get("step_results", [])),
            step_action_ids=list(cp.get("step_action_ids", [None] * len(steps))),
            step_dispatched_args=list(cp.get("step_dispatched_args", [None] * len(steps))),
            current_step_index=cp["current_step_index"],
            error=cp.get("error"),
        )

        # CRITICAL CONSTRAINT: Mark any IN_FLIGHT or IN_FLIGHT_UNKNOWN steps as NEEDS_RECONCILIATION
        for i in range(len(task.step_states)):
            if task.step_states[i] == StepState.IN_FLIGHT:
                task.step_states[i] = StepState.NEEDS_RECONCILIATION
                if task.state not in (TaskState.CANCELLED, TaskState.FAILED):
                    task.state = TaskState.RUNNING

        self._tasks[task_id] = task
        logger.info("restored task %s from checkpoint (state: %s)", task_id, task.state.name)
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

    def checkpoint(self, task_id: str) -> dict[str, Any]:
        """
        Serialize the current state of a task into a plain dict that can
        be round-tripped via restore_checkpoint() or FileCheckpointStore.
        """
        task = self._tasks[task_id]
        return {
            "task_id": task.task_id,
            "name": task.name,
            "state": task.state.name,
            "current_step_index": task.current_step_index,
            "step_states": [s.name for s in task.step_states],
            "step_results": list(task.step_results),
            "step_action_ids": list(task.step_action_ids),
            "step_dispatched_args": list(task.step_dispatched_args),
            "error": task.error,
        }

    def restore_checkpoint(self, task_id: str, cp: dict[str, Any]) -> Task:
        """
        Restore task state from a checkpoint dict. Overwrites in-place.
        """
        task = self._tasks[task_id]
        task.state = TaskState[cp["state"]]
        task.current_step_index = cp["current_step_index"]
        task.step_states = [StepState[s] for s in cp["step_states"]]
        task.step_results = cp["step_results"]
        if "step_action_ids" in cp:
            task.step_action_ids = cp["step_action_ids"]
        task.step_dispatched_args = cp["step_dispatched_args"]
        task.error = cp["error"]
        return task
