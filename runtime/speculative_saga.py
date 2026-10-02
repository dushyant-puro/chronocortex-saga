"""
runtime/speculative_saga.py

Two-phase speculative staging + distributed saga transaction manager.

Core invariants this module enforces:
  1. Every action (speculative read OR staged write) is bound to a
     monotonically increasing TurnEpoch. If the epoch advances before
     dispatch, the action is immediately ABORTED (nothing was ever sent).
  2. Idempotent reads may fire speculatively and be cancelled cheaply.
  3. Mutating writes are NEVER dispatched until the turn boundary is
     confirmed by Cortex-A. If an epoch advance fires *while* a write
     executor is in-flight, the result is COMMITTED_STALE (the remote
     side-effect occurred but belongs to a superseded turn) — never
     silently COMMITTED, and never silently dropped.
  4. Committed steps in a multi-step chain expose compensate() saga
     handlers; if any later step aborts, all committed predecessors are
     unwound in reverse order.
  5. CancelledError observed after dispatch on a staged write is treated
     as IN_FLIGHT_UNKNOWN, not ABORTED. Cancellation of an
     already-dispatched call is not proof the remote endpoint did not
     receive or process the request.

IMPORTANT — cancel behaviour per action kind:
  SPECULATIVE_READ:  task.cancel() is safe (reads are idempotent).
  STAGED_WRITE (IN_FLIGHT): _on_epoch_advance marks superseded_epoch but
     intentionally does NOT cancel the executor task. The executor is
     allowed to run to completion so commit_write can observe the true
     outcome and classify it as COMMITTED_STALE. Attempting to cancel a
     write executor is explicitly a best-effort signal that the spec says
     does NOT prove non-execution; for writes we forgo even that signal
     to give commit_write a deterministic view of the actual outcome.

ASSUMPTION documented here (Phase 1):
  For the ABORTED outcome (normal Exception path), we assume the executor
  guarantees "exception raised implies no persisted remote effect." If an
  executor cannot provide this guarantee (e.g., it raised after a partial
  write), callers MUST use the IN_FLIGHT_UNKNOWN / reconcile_write path
  instead, or supply a compensate() handler.
"""

from __future__ import annotations

import asyncio
import hashlib
import itertools
import json
import logging
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Awaitable, Callable, Optional

from runtime.tool_contract import (
    InvalidToolKindError,
    ToolContractError,
    ToolManifest,
    ToolRegistry,
)

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
    PENDING = auto()             # created, not yet dispatched
    IN_FLIGHT = auto()           # dispatched, awaiting response
    IN_FLIGHT_UNKNOWN = auto()   # dispatched; response lost / timed-out / cancelled after dispatch
    COMMITTED = auto()           # response received, effect is real and epoch is still current
    COMMITTED_STALE = auto()     # remote mutation succeeded, but the conversational epoch had
                                 # already advanced before it did — this IS a real side-effect
                                 # in the world; it must be compensated (or flagged for manual
                                 # reconciliation if no compensate handler is available)
    ABORTED = auto()             # cancelled before dispatch (PENDING→ABORTED), or executor raised
                                 # an exception guaranteeing no remote effect was persisted
    COMPENSATED = auto()         # was COMMITTED / COMMITTED_STALE, then rolled back via saga

    def is_terminal(self) -> bool:
        return self in (
            ActionState.COMMITTED,
            ActionState.COMMITTED_STALE,
            ActionState.IN_FLIGHT_UNKNOWN,
            ActionState.ABORTED,
            ActionState.COMPENSATED,
        )


# States that must block duplicate-idempotency-key staging
_BLOCKING_STATES = frozenset({
    ActionState.PENDING,
    ActionState.IN_FLIGHT,
    ActionState.IN_FLIGHT_UNKNOWN,
    ActionState.COMMITTED_STALE,  # unreconciled stale commits also block
})


@dataclass
class StagedAction:
    action_id: str
    kind: ActionKind
    tool_name: str
    args: dict[str, Any]
    capture_epoch: int
    idempotency_key: str                       # hash of tool_name + sorted(args) by default
    entity_hash: Optional[str] = None          # for dedup/debounce of reads
    state: ActionState = ActionState.PENDING
    result: Any = None
    error: Optional[BaseException] = None
    compensate: Optional[Callable[[Any], Awaitable[None]]] = None
    superseded_epoch: Optional[int] = None     # set by _on_epoch_advance when write is IN_FLIGHT
    compensation_failed: bool = False          # set True if _auto_compensate() raises; allows
                                               # a failed auto-compensation to be distinguished
                                               # from one that was never attempted
    dispatch_latency_ms: Optional[float] = None # measured latency in ms for speculative read dispatch-to-resolution
    manifest: Optional[ToolManifest] = None     # manifested contract, if known at staging/call time
    _task: Optional[asyncio.Task] = field(default=None, repr=False)
    created_at: float = field(default_factory=time.monotonic)

    def _set_terminal(self, new_state: ActionState) -> bool:
        """
        Guard: write a terminal state only if we are not already in one.
        Returns True if the assignment was made, False if the state was
        already terminal (a concurrent code path already resolved it).
        This prevents any second resolution from silently overwriting the
        first, ensuring exactly-one terminal state per action lifetime.
        """
        if self.state.is_terminal():
            logger.warning(
                "Skipped setting state %s for action %s — already in terminal state %s",
                new_state.name, self.action_id, self.state.name,
            )
            return False
        self.state = new_state
        return True


class StaleEpochError(RuntimeError):
    """Raised when an action attempts to commit against a superseded epoch."""


class SagaAbortedError(RuntimeError):
    """Raised to unwind a chain when a downstream step fails."""


class DuplicateOperationError(RuntimeError):
    """
    Raised by stage_write when the supplied (or computed) idempotency_key
    already belongs to an action that is in a blocking state:
    PENDING, IN_FLIGHT, IN_FLIGHT_UNKNOWN, or unreconciled COMMITTED_STALE.
    Do NOT call commit_write on an existing IN_FLIGHT_UNKNOWN write to retry
    it; use reconcile_write instead.
    """


# --------------------------------------------------------------------------
# The transaction manager
# --------------------------------------------------------------------------

ReadExecutor = Callable[[dict[str, Any]], Awaitable[Any]]
WriteExecutor = Callable[[dict[str, Any]], Awaitable[Any]]
StatusCheck = Callable[["StagedAction"], Awaitable[Optional[bool]]]

_READ_DEBOUNCE_SECONDS = 0.18  # suppress flicker from interim ASR re-firing


def _compute_idempotency_key(tool_name: str, args: dict[str, Any]) -> str:
    """
    Stable deterministic key: first 24 hex chars of SHA-256 of
    tool_name + JSON-encoded sorted args.
    Callers may override by passing an explicit idempotency_key to
    stage_write.
    """
    payload = json.dumps(
        {"tool": tool_name, "args": args},
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode()).hexdigest()[:24]


class SpeculativeSagaManager:
    def __init__(
        self,
        epoch_clock: TurnEpochClock,
        registry: Optional[ToolRegistry] = None,
    ) -> None:
        self.epoch_clock = epoch_clock
        self.registry = registry
        self._actions: dict[str, StagedAction] = {}
        self._ikey_index: dict[str, str] = {}  # idempotency_key -> action_id
        self._debounce: dict[str, tuple[str, float]] = {}  # entity_hash -> (action_id, ts)
        self._read_dedup_cache: dict[tuple[str, str, int], str] = {}  # (tool_name, canon_args, epoch) -> action_id
        self._prediction_hook: Optional[Callable[[StagedAction, Any], None]] = None
        self._chain: list[StagedAction] = []  # committed order, for compensation unwind
        self._id_gen = itertools.count()
        self.epoch_clock.on_advance(self._on_epoch_advance)

    def set_tool_registry(self, registry: Optional[ToolRegistry]) -> None:
        """Assign or update the ToolRegistry used for tool manifest lookups."""
        self.registry = registry

    # ---- prediction hook registration -----------------------------------

    def set_prediction_hook(self, hook: Optional[Callable[[StagedAction, Any], None]]) -> None:
        """
        Register a domain-specific prediction callback invoked synchronously
        when a speculative read completes and commits in the current epoch.
        """
        self._prediction_hook = hook

    # ---- epoch reaction -------------------------------------------------

    def _on_epoch_advance(self, new_epoch: int) -> None:
        """
        Called synchronously (inside the epoch clock's lock) when the
        epoch advances.

        Deduplication cache:
          -> Invalidated when the epoch advances (stale entries evicted).

        PENDING actions (never dispatched):
          -> ABORTED immediately. Safe: nothing was ever sent, no remote
             effect is possible.

        IN_FLIGHT SPECULATIVE_READ actions:
          -> attempt task.cancel() (reads are idempotent; cancellation is
             safe). Mark ABORTED.

        IN_FLIGHT STAGED_WRITE actions:
          -> record superseded_epoch = new_epoch so commit_write can later
             classify the outcome as COMMITTED_STALE.
          -> intentionally do NOT cancel the executor task. The executor
             must be allowed to run to completion so commit_write observes
             the true remote outcome. Cancellation of a write executor
             after dispatch does not prove non-execution (see module-level
             docstring), and forgoing cancel gives commit_write a clean,
             deterministic view of the actual result.
        """
        # Invalidate read dedup cache on epoch advance
        self._read_dedup_cache.clear()

        for action in list(self._actions.values()):
            if action.capture_epoch < new_epoch:
                if action.state == ActionState.PENDING:
                    # Safe: nothing dispatched yet, no remote effect possible.
                    action.state = ActionState.ABORTED
                    logger.info(
                        "aborted stale PENDING action %s (%s) epoch %d < %d",
                        action.action_id, action.tool_name,
                        action.capture_epoch, new_epoch,
                    )
                elif action.state == ActionState.IN_FLIGHT:
                    if action.kind == ActionKind.SPECULATIVE_READ:
                        # Reads are idempotent; safe to cancel immediately.
                        if action._task and not action._task.done():
                            action._task.cancel()
                        action.state = ActionState.ABORTED
                        logger.info(
                            "cancelled stale IN_FLIGHT read %s (%s) epoch %d < %d",
                            action.action_id, action.tool_name,
                            action.capture_epoch, new_epoch,
                        )
                    else:
                        # STAGED_WRITE in flight: mark superseded, do not cancel.
                        # commit_write will resolve to COMMITTED_STALE.
                        # Only record the FIRST advance that superseded this action;
                        # subsequent advances must not overwrite superseded_epoch,
                        # preserving the exact epoch that first made this write stale.
                        if action.superseded_epoch is None:
                            action.superseded_epoch = new_epoch
                        logger.info(
                            "superseded IN_FLIGHT write %s (%s) epoch %d < %d — "
                            "executor allowed to complete; will resolve as COMMITTED_STALE",
                            action.action_id, action.tool_name,
                            action.capture_epoch, new_epoch,
                        )

    # ---- speculative reads ------------------------------------------------

    def get_current_result(self, entity_hash: str) -> Any:
        """
        Return the result of the most recent speculative read for this
        entity_hash, BUT ONLY IF:
          - the most recent action for this entity_hash is in COMMITTED state, AND
          - its capture_epoch equals the current epoch (i.e. the result is
            still valid for this conversational turn).

        Returns None in ALL other cases, including:
          - No action has ever been fired for this entity_hash.
          - The most recent action is IN_FLIGHT, ABORTED, IN_FLIGHT_UNKNOWN,
            COMMITTED_STALE, or COMPENSATED.
          - The action is COMMITTED but its capture_epoch is stale (the epoch
            advanced after the read landed, making the result obsolete).

        This is the ONLY sanctioned way for write-staging code to consume
        a prior speculative read's result. Do NOT read action.result directly
        from an action object to decide write arguments — epoch-staleness
        checks are silently skipped that way, enabling a new class of
        'stale-read feeds live-write' bugs that are invisible to the saga
        state machine.

        Usage pattern::

            result = saga.get_current_result("truck-17")
            if result is None:
                return  # read not ready or stale — do not stage the write yet
            action = saga.stage_write("reroute_truck", {"destination": result["route"]})
        """
        entry = self._debounce.get(entity_hash)
        if entry is None:
            return None
        action_id, _ = entry
        action = self._actions.get(action_id)
        if action is None:
            return None
        if (
            action.state == ActionState.COMMITTED
            and action.capture_epoch == self.epoch_clock.current
        ):
            return action.result
        return None

    def fire_speculative_read(
        self,
        tool_name: str,
        args: dict[str, Any],
        executor: ReadExecutor,
        entity_hash: str,
        *,
        manifest: Optional[ToolManifest] = None,
        registry: Optional[ToolRegistry] = None,
    ) -> StagedAction:
        """
        Dispatch an idempotent read the moment an entity is recognized.
        Kind-enforced: verifies the tool is manifested with kind="read" if a manifest is provided or found.
        Deduplicated: if a speculative read for the same (tool_name, args, epoch)
        is already in-flight or completed, returns the existing action instead
        of firing a second real executor call.
        Debounced: if the same entity_hash fired within the debounce
        window, the earlier in-flight call is cancelled first (ASR
        flicker protection) rather than allowed to pile up.
        """
        # Kind enforcement: verify tool is manifested as 'read'
        mf = manifest
        if mf is None:
            reg = registry or self.registry
            if reg is not None:
                mf = reg.get(tool_name)
        if mf is not None and mf.kind != "read":
            raise InvalidToolKindError(
                f"fire_speculative_read requires a tool with kind='read', but '{tool_name}' "
                f"is manifested as kind='{mf.kind}'"
            )

        now = time.monotonic()
        curr_epoch = self.epoch_clock.current
        canon_args = json.dumps(args, sort_keys=True, separators=(",", ":"))
        cache_key = (tool_name, canon_args, curr_epoch)

        # Check deduplication cache within the current epoch
        cached_action_id = self._read_dedup_cache.get(cache_key)
        if cached_action_id:
            cached_action = self._actions.get(cached_action_id)
            if (
                cached_action
                and cached_action.capture_epoch == curr_epoch
                and cached_action.state in (ActionState.PENDING, ActionState.IN_FLIGHT, ActionState.COMMITTED)
            ):
                logger.debug(
                    "deduplicated speculative read for (%s, %s, epoch=%d) -> action %s",
                    tool_name, canon_args, curr_epoch, cached_action_id,
                )
                return cached_action

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
            capture_epoch=curr_epoch,
            idempotency_key=_compute_idempotency_key(tool_name, args),
            entity_hash=entity_hash,
            manifest=mf,
        )
        self._actions[action_id] = action
        self._debounce[entity_hash] = (action_id, now)
        self._read_dedup_cache[cache_key] = action_id
        action._task = asyncio.create_task(self._run_read(action, executor))
        return action

    async def _run_read(self, action: StagedAction, executor: ReadExecutor) -> None:
        action.state = ActionState.IN_FLIGHT
        t0 = time.monotonic()
        try:
            result = await executor(action.args)
            action.dispatch_latency_ms = (time.monotonic() - t0) * 1000.0
            if action.capture_epoch != self.epoch_clock.current:
                action.state = ActionState.ABORTED
                return
            action.result = result
            action.state = ActionState.COMMITTED

            # Invoke prediction hook if registered and epoch still current
            if self._prediction_hook is not None:
                try:
                    self._prediction_hook(action, result)
                except Exception:
                    logger.exception("prediction hook failed for action %s", action.action_id)
        except asyncio.CancelledError:
            action.dispatch_latency_ms = (time.monotonic() - t0) * 1000.0
            action.state = ActionState.ABORTED
            raise
        except Exception as exc:  # noqa: BLE001
            action.dispatch_latency_ms = (time.monotonic() - t0) * 1000.0
            action.error = exc
            action.state = ActionState.ABORTED
            logger.warning("speculative read %s failed: %s", action.tool_name, exc)

    # ---- staged writes ------------------------------------------------

    def stage_write(
        self,
        tool_name: str,
        args: dict[str, Any],
        compensate: Optional[Callable[[Any], Awaitable[None]]] = None,
        *,
        idempotency_key: Optional[str] = None,
        manifest: Optional[ToolManifest] = None,
        registry: Optional[ToolRegistry] = None,
    ) -> StagedAction:
        """
        Create a write in the uncommitted in-memory envelope. It is NOT
        dispatched here — only staged. Call commit_write() once the turn
        boundary is confirmed.

        If idempotency_key is not supplied, one is computed as a SHA-256
        hash of tool_name + sorted(args). If an action with the same key
        is already in a blocking state (PENDING, IN_FLIGHT,
        IN_FLIGHT_UNKNOWN, or unreconciled COMMITTED_STALE),
        DuplicateOperationError is raised.
        """
        mf = manifest
        if mf is None:
            reg = registry or self.registry
            if reg is not None:
                mf = reg.get(tool_name)

        ikey = idempotency_key or _compute_idempotency_key(tool_name, args)

        # Idempotency enforcement: reject if the key is already live.
        existing_id = self._ikey_index.get(ikey)
        if existing_id is not None:
            existing = self._actions.get(existing_id)
            if existing is not None and existing.state in _BLOCKING_STATES:
                raise DuplicateOperationError(
                    f"stage_write rejected: idempotency_key {ikey!r} already exists for "
                    f"action {existing_id} in state {existing.state.name}. "
                    f"Use reconcile_write to resolve IN_FLIGHT_UNKNOWN or COMMITTED_STALE "
                    f"before retrying."
                )

        action_id = f"write-{next(self._id_gen)}-{uuid.uuid4().hex[:6]}"
        action = StagedAction(
            action_id=action_id,
            kind=ActionKind.STAGED_WRITE,
            tool_name=tool_name,
            args=args,
            capture_epoch=self.epoch_clock.current,
            idempotency_key=ikey,
            compensate=compensate,
            manifest=mf,
        )
        self._actions[action_id] = action
        self._ikey_index[ikey] = action_id
        return action

    def abort_write(self, action_id: str) -> None:
        """Used for disfluency/self-correction slot rewriting: kill a staged
        (not yet committed) write outright — zero network calls ever made."""
        action = self._actions.get(action_id)
        if action and action.state == ActionState.PENDING:
            action.state = ActionState.ABORTED

    async def commit_write(
        self,
        action_id: str,
        executor: WriteExecutor,
        *,
        timeout: float = 4.0,
        manifest: Optional[ToolManifest] = None,
        registry: Optional[ToolRegistry] = None,
    ) -> Any:
        """
        Dispatch a staged write. Outcome classification:

        executor succeeds AND superseded_epoch is None
            -> COMMITTED (normal, epoch is still current)

        executor succeeds AND superseded_epoch is set
            -> COMMITTED_STALE; auto-compensate if a handler is available,
               otherwise log CRITICAL (manual reconciliation required —
               never silently dropped)

        executor raises a normal Exception
            -> ABORTED. ASSUMPTION: exception raised means no persisted
               remote effect. Executors that cannot guarantee this must
               handle reconciliation themselves before raising.

        asyncio.TimeoutError
            -> IN_FLIGHT_UNKNOWN; caller must use reconcile_write before
               any retry. Do not issue a fresh commit_write on the same key.

        asyncio.CancelledError observed after dispatch
            -> IN_FLIGHT_UNKNOWN; cancellation of an already-dispatched
               call is NOT proof the remote endpoint did not process it.

        Every terminal state assignment is guarded by _set_terminal() so
        no second code path can overwrite a terminal state already written
        by a concurrent resolution (exactly-one terminal state guarantee).
        """
        action = self._actions[action_id]
        if action.state != ActionState.PENDING:
            raise SagaAbortedError(
                f"write {action_id} not in PENDING state: {action.state}"
            )
        if action.capture_epoch != self.epoch_clock.current:
            action._set_terminal(ActionState.ABORTED)
            raise StaleEpochError(
                f"write {action_id} epoch stale: {action.capture_epoch}"
            )

        # Kind enforcement: verify tool is manifested with kind="write" before dispatching
        mf = manifest
        if mf is None:
            reg = registry or self.registry
            if reg is not None:
                mf = reg.get(action.tool_name)
        if mf is None:
            mf = action.manifest

        if mf is not None and mf.kind != "write":
            raise InvalidToolKindError(
                f"commit_write requires a tool with kind='write', but '{action.tool_name}' "
                f"is manifested as kind='{mf.kind}'"
            )

        action.state = ActionState.IN_FLIGHT

        try:
            result = await asyncio.wait_for(executor(action.args), timeout=timeout)

        except asyncio.TimeoutError:
            # We do NOT know whether the remote executed the call.
            # Do not retry without reconciliation.
            if not action._set_terminal(ActionState.IN_FLIGHT_UNKNOWN):
                return action.result  # already resolved by another code path
            logger.error(
                "write %s (%s) timed out — IN_FLIGHT_UNKNOWN, requires reconciliation "
                "before any retry",
                action.action_id, action.tool_name,
            )
            raise

        except asyncio.CancelledError:
            # Cancellation is a local event only. The remote call may have
            # completed before the cancellation was processed. Classify as
            # IN_FLIGHT_UNKNOWN, not ABORTED.
            if not action._set_terminal(ActionState.IN_FLIGHT_UNKNOWN):
                return action.result
            logger.error(
                "write %s (%s) was cancelled after dispatch — IN_FLIGHT_UNKNOWN, "
                "requires reconciliation; do not retry blindly",
                action.action_id, action.tool_name,
            )
            raise

        except Exception:
            # Normal exception. ASSUMPTION: executor guarantees no remote
            # effect was persisted when it raises (see module docstring).
            if not action._set_terminal(ActionState.ABORTED):
                return action.result
            raise

        # ----------------------------------------------------------------
        # Executor returned successfully. Classify: COMMITTED or STALE?
        # ----------------------------------------------------------------
        action.result = result

        if action.superseded_epoch is None:
            # Epoch is still current — normal commit.
            if not action._set_terminal(ActionState.COMMITTED):
                return action.result
            self._chain.append(action)
            return result

        # Epoch advanced while the executor was running. The mutation
        # succeeded remotely and IS a real side-effect in the world, but
        # it belongs to an obsolete conversational turn. Classify as
        # COMMITTED_STALE and trigger compensation.
        if not action._set_terminal(ActionState.COMMITTED_STALE):
            return action.result

        if action.compensate is not None:
            logger.warning(
                "COMMITTED_STALE: write %s (%s) succeeded after epoch advanced "
                "(capture=%d, superseded_by=%d) — auto-compensating now",
                action.action_id, action.tool_name,
                action.capture_epoch, action.superseded_epoch,
            )
            asyncio.create_task(self._auto_compensate(action))
        else:
            logger.critical(
                "COMMITTED_STALE: write %s (%s) succeeded after epoch advanced "
                "(capture=%d, superseded_by=%d) and has NO compensate() handler — "
                "MANUAL RECONCILIATION REQUIRED; remote side-effect is unresolved",
                action.action_id, action.tool_name,
                action.capture_epoch, action.superseded_epoch,
            )
        return result

    async def _auto_compensate(self, action: StagedAction) -> None:
        """
        Run automatic compensation for a COMMITTED_STALE action.

        On success: action.state -> COMPENSATED.
        On failure: logs CRITICAL and sets action.compensation_failed = True
          so the failure is observable via snapshot()/inspection, not just in
          log output. Mirrors the error-handling pattern of abort_chain_from().
        """
        assert action.compensate is not None
        try:
            await action.compensate(action.result)
            action.state = ActionState.COMPENSATED
            logger.info(
                "auto-compensation succeeded for COMMITTED_STALE action %s (%s)",
                action.action_id, action.tool_name,
            )
        except Exception:
            action.compensation_failed = True
            logger.critical(
                "AUTO-COMPENSATION FAILED for COMMITTED_STALE action %s (%s) — "
                "MANUAL RECONCILIATION REQUIRED; action.compensation_failed=True",
                action.action_id, action.tool_name,
                exc_info=True,
            )

    # ---- reconciliation ---------------------------------------------------

    async def reconcile_write(
        self,
        action_id: str,
        status_check: StatusCheck,
    ) -> None:
        """
        Deterministically resolve an IN_FLIGHT_UNKNOWN or unreconciled
        COMMITTED_STALE action by querying the remote system.

        status_check(action) must return an Awaitable that yields:
          True  — remote confirms the operation was executed and persisted
          False — remote confirms the operation was NOT executed / rolled back
          None  — still unknown; no state change, caller may retry later

        For IN_FLIGHT_UNKNOWN:
          True  -> COMMITTED (if epoch current) or COMMITTED_STALE (if stale)
                   + auto-compensate if stale and compensate is available
          False -> ABORTED

        For COMMITTED_STALE (unreconciled, not yet compensated):
          True  -> remote confirms it happened; trigger compensation if available
          False -> remote confirms it was rolled back; set COMPENSATED

        NEVER use reconcile_write to re-attempt the original executor.
        If you need to redo the operation, call stage_write + commit_write
        with the appropriate idempotency key cleared.
        """
        action = self._actions.get(action_id)
        if action is None:
            raise KeyError(f"No action with id {action_id!r}")
        if action.state not in (
            ActionState.IN_FLIGHT_UNKNOWN,
            ActionState.COMMITTED_STALE,
        ):
            raise SagaAbortedError(
                f"reconcile_write only valid for IN_FLIGHT_UNKNOWN or COMMITTED_STALE; "
                f"action {action_id} is in {action.state.name}"
            )

        outcome = await status_check(action)

        if outcome is None:
            logger.info(
                "reconcile_write for %s (%s): status_check returned None "
                "(still unknown) — no-op",
                action_id, action.tool_name,
            )
            return

        if outcome is True:
            # Remote confirms the operation executed.
            if action.state == ActionState.IN_FLIGHT_UNKNOWN:
                # Classify based on current epoch vs capture epoch.
                is_current = (
                    action.superseded_epoch is None
                    and action.capture_epoch == self.epoch_clock.current
                )
                if is_current:
                    # reconcile_write is the authorized resolver for IN_FLIGHT_UNKNOWN;
                    # we bypass _set_terminal here because IN_FLIGHT_UNKNOWN is terminal
                    # by design, but reconciliation is its explicit resolution path.
                    action.state = ActionState.COMMITTED
                    self._chain.append(action)
                    logger.info(
                        "reconcile_write %s (%s): confirmed executed -> COMMITTED",
                        action_id, action.tool_name,
                    )
                else:
                    action.state = ActionState.COMMITTED_STALE
                    logger.warning(
                        "reconcile_write %s (%s): confirmed executed but epoch stale "
                        "(capture=%d, current=%d) -> COMMITTED_STALE",
                        action_id, action.tool_name,
                        action.capture_epoch, self.epoch_clock.current,
                    )
                    if action.compensate is not None:
                        await self._auto_compensate(action)
                    else:
                        logger.critical(
                            "COMMITTED_STALE via reconciliation for %s (%s): "
                            "no compensate() handler — MANUAL RECONCILIATION REQUIRED",
                            action_id, action.tool_name,
                        )
            else:
                # Already COMMITTED_STALE; remote confirms it happened. Compensate.
                logger.warning(
                    "reconcile_write %s (%s): COMMITTED_STALE confirmed by status_check; "
                    "triggering compensation",
                    action_id, action.tool_name,
                )
                if action.compensate is not None:
                    await self._auto_compensate(action)
                else:
                    logger.critical(
                        "COMMITTED_STALE reconcile confirmed for %s (%s): "
                        "no compensate() handler — MANUAL RECONCILIATION REQUIRED",
                        action_id, action.tool_name,
                    )

        else:
            # outcome is False — remote confirms NOT executed / rolled back.
            # Direct assignment since we're the authorized resolver for these states.
            action.state = ActionState.ABORTED
            logger.info(
                "reconcile_write %s (%s): confirmed NOT executed -> ABORTED",
                action_id, action.tool_name,
            )

    # ---- saga compensation ------------------------------------------------

    async def abort_chain_from(self, failed_action_id: str) -> None:
        """
        A downstream step failed. Unwind every previously committed step
        in this chain, in reverse order, via its compensate() handler.
        """
        idx = next(
            (i for i, a in enumerate(self._chain) if a.action_id == failed_action_id),
            None,
        )
        to_unwind = self._chain if idx is None else self._chain[:idx]
        for action in reversed(to_unwind):
            if action.state != ActionState.COMMITTED or action.compensate is None:
                continue
            try:
                await action.compensate(action.result)
                action.state = ActionState.COMPENSATED
                logger.info(
                    "compensated %s (%s)", action.action_id, action.tool_name
                )
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
                "superseded_epoch": a.superseded_epoch,
                "idempotency_key": a.idempotency_key,
                "compensation_failed": a.compensation_failed,
                "dispatch_latency_ms": a.dispatch_latency_ms,
                "age_ms": round((time.monotonic() - a.created_at) * 1000, 1),
            }
            for a in self._actions.values()
        ]

    # ---- latency observability -------------------------------------------

    def get_latency_stats(self) -> dict[str, Any]:
        """
        Report min/max/mean dispatch latency across all resolved speculative
        reads in the current session that recorded a latency measurement.
        """
        latencies = [
            a.dispatch_latency_ms
            for a in self._actions.values()
            if a.kind == ActionKind.SPECULATIVE_READ and a.dispatch_latency_ms is not None
        ]
        if not latencies:
            return {
                "count": 0,
                "min_ms": None,
                "max_ms": None,
                "mean_ms": None,
            }
        return {
            "count": len(latencies),
            "min_ms": min(latencies),
            "max_ms": max(latencies),
            "mean_ms": sum(latencies) / len(latencies),
        }

