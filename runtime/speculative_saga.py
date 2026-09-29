"""
runtime/speculative_saga.py

Two-phase speculative staging + distributed saga transaction manager.

Core invariants this module enforces:
  1. Every action (speculative read OR staged write) is bound to a
     monotonically increasing TurnEpoch. If the epoch advances before
     the action commits, the action is auto-aborted — no LLM judgment
     call, no race window.
  2. Idempotent reads may fire speculatively and be cancelled cheaply.
  3. Mutating writes are NEVER dispatched until the turn boundary is
     confirmed by Cortex-A. Between "sent" and "confirmed response",
     a write lives in IN_FLIGHT_UNKNOWN — a third state distinct from
     PENDING/COMMITTED/ABORTED — so a lost response can never be
     silently treated as "didn't happen" (which would cause a
     duplicate retry) or "definitely happened" (which would cause a
     stuck lock).
  4. Committed steps in a multi-step chain expose compensate() saga
     handlers; if any later step aborts, all committed predecessors
     are unwound in reverse order.
"""

from __future__ import annotations

import asyncio
import itertools
import logging
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Awaitable, Callable, Optional

logger = logging.getLogger("ccs.saga")


# --------------------------------------------------------------------------
# Turn epoch — the single source of truth for "is this action still valid"
# --------------------------------------------------------------------------

class TurnEpochClock:
    """
    Monotonic epoch counter. ONLY Cortex-A (the acoustic reflex layer)
    is allowed to call advance(); it does so exactly once per confirmed
    turn boundary (endpointing) or on a hard user barge-in.
    """

    def __init__(self) -> None:
        self._epoch = 0
        self._lock = asyncio.Lock()
        self._subscribers: list[Callable[[int], None]] = []

    @property
    def current(self) -> int:
        return self._epoch

    async def advance(self, reason: str = "endpoint") -> int:
        async with self._lock:
            self._epoch += 1
            e = self._epoch
            logger.debug("epoch advance -> %d (%s)", e, reason)
            for cb in self._subscribers:
                try:
                    cb(e)
                except Exception:
                    logger.exception("epoch subscriber failed")
            return e

    def on_advance(self, cb: Callable[[int], None]) -> None:
        self._subscribers.append(cb)


class ActionKind(Enum):
    SPECULATIVE_READ = auto()
    STAGED_WRITE = auto()


class ActionState(Enum):
    PENDING = auto()            # created, not yet dispatched
    IN_FLIGHT = auto()          # dispatched, awaiting response
    IN_FLIGHT_UNKNOWN = auto()  # dispatched, response lost/timed out
    COMMITTED = auto()          # response received, effect is real
    ABORTED = auto()            # cancelled/superseded before or after dispatch
    COMPENSATED = auto()        # was committed, then rolled back via saga


@dataclass
class StagedAction:
    action_id: str
    kind: ActionKind
    tool_name: str
    args: dict[str, Any]
    capture_epoch: int
    entity_hash: Optional[str] = None          # for dedup/debounce of reads
    state: ActionState = ActionState.PENDING
    result: Any = None
    error: Optional[BaseException] = None
    compensate: Optional[Callable[[Any], Awaitable[None]]] = None
    _task: Optional[asyncio.Task] = field(default=None, repr=False)
    created_at: float = field(default_factory=time.monotonic)


class StaleEpochError(RuntimeError):
    """Raised when an action attempts to commit against a superseded epoch."""


class SagaAbortedError(RuntimeError):
    """Raised to unwind a chain when a downstream step fails."""


# --------------------------------------------------------------------------
# The transaction manager
# --------------------------------------------------------------------------

ReadExecutor = Callable[[dict[str, Any]], Awaitable[Any]]
WriteExecutor = Callable[[dict[str, Any]], Awaitable[Any]]

_READ_DEBOUNCE_SECONDS = 0.18  # suppress flicker from interim ASR re-firing


class SpeculativeSagaManager:
    def __init__(self, epoch_clock: TurnEpochClock) -> None:
        self.epoch_clock = epoch_clock
        self._actions: dict[str, StagedAction] = {}
        self._debounce: dict[str, tuple[str, float]] = {}  # entity_hash -> (action_id, ts)
        self._chain: list[StagedAction] = []  # committed order, for compensation unwind
        self._id_gen = itertools.count()
        self.epoch_clock.on_advance(self._on_epoch_advance)

    # ---- epoch reaction -------------------------------------------------

    def _on_epoch_advance(self, new_epoch: int) -> None:
        """Fire-and-forget: abort every in-flight action bound to a stale epoch."""
        for action in list(self._actions.values()):
            if action.capture_epoch < new_epoch and action.state in (
                ActionState.PENDING,
                ActionState.IN_FLIGHT,
            ):
                if action._task and not action._task.done():
                    action._task.cancel()
                action.state = ActionState.ABORTED
                logger.info(
                    "aborted stale action %s (%s) epoch %d < %d",
                    action.action_id, action.tool_name, action.capture_epoch, new_epoch,
                )

    # ---- speculative reads ------------------------------------------------

    def fire_speculative_read(
        self,
        tool_name: str,
        args: dict[str, Any],
        executor: ReadExecutor,
        entity_hash: str,
    ) -> StagedAction:
        """
        Dispatch an idempotent read the moment an entity is recognized.
        Debounced: if the same entity_hash fired within the debounce
        window, the earlier in-flight call is cancelled first (ASR
        flicker protection) rather than allowed to pile up.
        """
        now = time.monotonic()
        prior = self._debounce.get(entity_hash)
        if prior:
            prior_id, prior_ts = prior
            if now - prior_ts < _READ_DEBOUNCE_SECONDS:
                prior_action = self._actions.get(prior_id)
                if prior_action and prior_action.state in (
                    ActionState.PENDING, ActionState.IN_FLIGHT,
                ):
                    if prior_action._task:
                        prior_action._task.cancel()
                    prior_action.state = ActionState.ABORTED

        action_id = f"read-{next(self._id_gen)}-{uuid.uuid4().hex[:6]}"
        action = StagedAction(
            action_id=action_id,
            kind=ActionKind.SPECULATIVE_READ,
            tool_name=tool_name,
            args=args,
            capture_epoch=self.epoch_clock.current,
            entity_hash=entity_hash,
        )
        self._actions[action_id] = action
        self._debounce[entity_hash] = (action_id, now)
        action._task = asyncio.create_task(self._run_read(action, executor))
        return action

    async def _run_read(self, action: StagedAction, executor: ReadExecutor) -> None:
        action.state = ActionState.IN_FLIGHT
        try:
            result = await executor(action.args)
            if action.capture_epoch != self.epoch_clock.current:
                action.state = ActionState.ABORTED
                return
            action.result = result
            action.state = ActionState.COMMITTED
        except asyncio.CancelledError:
            action.state = ActionState.ABORTED
            raise
        except Exception as exc:  # noqa: BLE001
            action.error = exc
            action.state = ActionState.ABORTED
            logger.warning("speculative read %s failed: %s", action.tool_name, exc)

    # ---- staged writes ------------------------------------------------

    def stage_write(
        self,
        tool_name: str,
        args: dict[str, Any],
        compensate: Optional[Callable[[Any], Awaitable[None]]] = None,
    ) -> StagedAction:
        """
        Create a write in the uncommitted in-memory envelope. It is NOT
        dispatched here — only staged. Call commit_write() once the turn
        boundary is confirmed.
        """
        action_id = f"write-{next(self._id_gen)}-{uuid.uuid4().hex[:6]}"
        action = StagedAction(
            action_id=action_id,
            kind=ActionKind.STAGED_WRITE,
            tool_name=tool_name,
            args=args,
            capture_epoch=self.epoch_clock.current,
            compensate=compensate,
        )
        self._actions[action_id] = action
        return action

    def abort_write(self, action_id: str) -> None:
        """Used for disfluency/self-correction slot rewriting: kill a staged
        (not yet committed) write outright — zero network calls ever made."""
        action = self._actions.get(action_id)
        if action and action.state == ActionState.PENDING:
            action.state = ActionState.ABORTED

    async def commit_write(self, action_id: str, executor: WriteExecutor, *, timeout: float = 4.0) -> Any:
        """
        Dispatch a staged write. Guarantees:
          - Aborts immediately (no dispatch) if the turn epoch moved on.
          - On timeout, the write enters IN_FLIGHT_UNKNOWN rather than
            being assumed failed — callers must NOT retry an
            IN_FLIGHT_UNKNOWN write without an idempotency key /
            reconciliation read, or you risk duplicate mutating calls.
        """
        action = self._actions[action_id]
        if action.state != ActionState.PENDING:
            raise SagaAbortedError(f"write {action_id} not in PENDING state: {action.state}")
        if action.capture_epoch != self.epoch_clock.current:
            action.state = ActionState.ABORTED
            raise StaleEpochError(f"write {action_id} epoch stale: {action.capture_epoch}")

        action.state = ActionState.IN_FLIGHT
        try:
            result = await asyncio.wait_for(executor(action.args), timeout=timeout)
        except asyncio.TimeoutError:
            action.state = ActionState.IN_FLIGHT_UNKNOWN
            logger.error(
                "write %s (%s) timed out — IN_FLIGHT_UNKNOWN, requires reconciliation "
                "before any retry", action.action_id, action.tool_name,
            )
            raise
        except Exception:
            action.state = ActionState.ABORTED
            raise

        action.result = result
        action.state = ActionState.COMMITTED
        self._chain.append(action)
        return result

    # ---- saga compensation ------------------------------------------------

    async def abort_chain_from(self, failed_action_id: str) -> None:
        """
        A downstream step failed. Unwind every previously committed step
        in this chain, in reverse order, via its compensate() handler.
        """
        idx = next((i for i, a in enumerate(self._chain) if a.action_id == failed_action_id), None)
        to_unwind = self._chain if idx is None else self._chain[:idx]
        for action in reversed(to_unwind):
            if action.state != ActionState.COMMITTED or action.compensate is None:
                continue
            try:
                await action.compensate(action.result)
                action.state = ActionState.COMPENSATED
                logger.info("compensated %s (%s)", action.action_id, action.tool_name)
            except Exception:
                logger.exception(
                    "COMPENSATION FAILED for %s (%s) — manual reconciliation required",
                    action.action_id, action.tool_name,
                )
        self._chain = [a for a in self._chain if a.state == ActionState.COMMITTED]

    # ---- inspector (for the Round-2 live debug overlay) -------------------

    def snapshot(self) -> list[dict[str, Any]]:
        return [
            {
                "id": a.action_id,
                "kind": a.kind.name,
                "tool": a.tool_name,
                "state": a.state.name,
                "epoch": a.capture_epoch,
                "age_ms": round((time.monotonic() - a.created_at) * 1000, 1),
            }
            for a in self._actions.values()
        ]
