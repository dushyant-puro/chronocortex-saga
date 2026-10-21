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

Post-dispatch failures:
  Ordinary exceptions do not establish whether a remote effect occurred.
  They preserve IN_FLIGHT_UNKNOWN and block same-key retries. Only a trusted
  adapter's MutationNotAppliedError, with an explicit evidence reference,
  establishes no effect for this operation and permits ABORTED. A compensator
  being available is not evidence of non-execution and does not resolve unknown.
"""

from __future__ import annotations

import asyncio
import copy
import hashlib
import itertools
import json
import logging
import time
import uuid
from dataclasses import dataclass, field, replace
from enum import Enum, auto
from typing import Any, Awaitable, Callable, Optional

from runtime.tool_contract import (
    InvalidToolKindError,
    ToolContractError,
    ToolManifest,
    ToolRegistry,
)
from runtime.policy_engine import (
    AuthorizationContext, ConfirmationRequiredError, PermissionDeniedError,
    PolicyEngine, payload_digest,
)
from runtime.action_journal import JournalError, JournalDuplicate, SQLiteActionJournal

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
    IN_FLIGHT = auto()           # dispatch intent recorded; call may have reached provider
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


class CompensationState(Enum):
    """Outcome of the single automatic compensation attempt, not a retry policy."""
    NOT_STARTED = auto()
    IN_FLIGHT = auto()
    UNKNOWN = auto()
    NOT_APPLIED = auto()  # explicit adapter evidence; still no automatic retry
    SUCCEEDED = auto()


_BLOCKING_COMPENSATION_STATES = frozenset({
    CompensationState.IN_FLIGHT, CompensationState.UNKNOWN, CompensationState.NOT_APPLIED,
})


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
    not_applied_evidence: Optional[str] = None  # adapter-supplied redacted evidence reference
    compensation_state: CompensationState = CompensationState.NOT_STARTED
    compensation_error: Optional[BaseException] = field(default=None, repr=False)
    compensation_not_applied_evidence: Optional[str] = None
    _resolution_lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False, compare=False)
    _compensation_task: Optional[asyncio.Task] = field(default=None, repr=False, compare=False)
    _staged_digest: Optional[str] = field(default=None, repr=False, compare=False)
    _journal_version: Optional[int] = field(default=None, repr=False, compare=False)
    intent_session: Optional[str] = None
    owner_task_id: Optional[str] = None
    owner_step_index: Optional[int] = None
    owner_generation: Optional[int] = None
    chain_id: Optional[str] = None
    recovered: bool = False
    durability_error: Optional[str] = None

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


class ActionOwnershipError(ValueError):
    """An action is not owned by the requested task/step/generation or chain."""


class SagaAbortedError(RuntimeError):
    """Raised to unwind a chain when a downstream step fails."""


class MutationNotAppliedError(RuntimeError):
    """Trusted adapter asserts this operation made no persisted remote change.

    ``evidence`` must identify a documented atomic rejection/no-dispatch fact
    for this operation. Use a redacted code/reference, never a raw response or
    secret. Do not wrap generic network/HTTP/decoding failures in this type.
    The runtime validates presence, not provider truth; adapters remain a trust
    boundary. Non-execution must also exclude a delayed future application.
    """

    def __init__(self, message: str, *, evidence: str) -> None:
        if not isinstance(evidence, str) or not evidence.strip():
            raise ValueError("MutationNotAppliedError requires a non-empty evidence reference")
        super().__init__(message)
        self.evidence = evidence.strip()


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
        auth_context: Optional[AuthorizationContext] = None,
        *,
        journal: Optional[SQLiteActionJournal] = None,
        simulation: bool = False,
    ) -> None:
        self.epoch_clock = epoch_clock
        self.registry = registry
        # Trusted host credential injection only. No credentials inferred from args,
        # grounding, ASR or model output. Missing credentials deny mutation dispatch.
        self.auth_context = auth_context
        if type(simulation) is not bool or (simulation and journal is not None):
            raise ValueError("Simulation must be explicit and cannot bypass a configured journal")
        self.journal = journal
        self.simulation = simulation
        self._session_id = uuid.uuid4().hex
        self._durability_fault: Optional[JournalError] = None
        self._actions: dict[str, StagedAction] = {}
        self._ikey_index: dict[str, str] = {}  # idempotency_key -> action_id
        self._debounce: dict[str, tuple[str, float]] = {}  # entity_hash -> (action_id, ts)
        self._read_dedup_cache: dict[tuple[str, str, int], str] = {}  # (tool_name, canon_args, epoch) -> action_id
        self._prediction_hook: Optional[Callable[[StagedAction, Any], None]] = None
        self._chain: list[StagedAction] = []  # committed order, for compensation unwind
        self._closed_chains: set[str] = set()
        self._chain_unwind_locks: dict[str, asyncio.Lock] = {}
        self._id_gen = itertools.count()
        self.epoch_clock.on_advance(self._on_epoch_advance)
        if self.journal is not None:
            self._load_journal()

    def _load_journal(self) -> None:
        """Hydrate evidence only. Never auto-bind compensators or revive old intent."""
        self._closed_chains.update(self.journal.closed_chains())
        for version, record in self.journal.records():
            state = ActionState[record["state"]]
            if state in (ActionState.PENDING, ActionState.IN_FLIGHT):
                state = ActionState.IN_FLIGHT_UNKNOWN
            elif state == ActionState.COMMITTED:
                state = ActionState.COMMITTED_STALE
            comp = CompensationState[record["compensation_state"]]
            if comp == CompensationState.IN_FLIGHT:
                comp = CompensationState.UNKNOWN
            action = StagedAction(
                action_id=record["action_id"], kind=ActionKind.STAGED_WRITE,
                tool_name=record["tool_name"], args=record["args"],
                capture_epoch=record["capture_epoch"], idempotency_key=record["idempotency_key"],
                state=state, result=record["result"], compensation_state=comp,
                compensation_failed=(record["compensation_failed"] or
                                     comp in (CompensationState.UNKNOWN, CompensationState.NOT_APPLIED)),
                not_applied_evidence=record["not_applied_evidence"],
                compensation_not_applied_evidence=record["compensation_not_applied_evidence"],
                superseded_epoch=record["superseded_epoch"],
                _staged_digest=record["digest"], _journal_version=version,
                intent_session=record["session_id"], owner_task_id=record["task_id"],
                owner_step_index=record["step_index"], recovered=True,
                owner_generation=record["generation"], chain_id=record["chain_id"],
            )
            self._actions[action.action_id] = action
            self._ikey_index[action.idempotency_key] = action.action_id

    def _journal_record(self, action: StagedAction) -> dict:
        return {
            "action_id": action.action_id, "tool_name": action.tool_name,
            "args": action.args, "digest": action._staged_digest,
            "idempotency_key": action.idempotency_key, "capture_epoch": action.capture_epoch,
            "session_id": action.intent_session, "task_id": action.owner_task_id,
            "step_index": action.owner_step_index, "state": action.state.name,
            "generation": action.owner_generation, "chain_id": action.chain_id,
            "compensation_state": action.compensation_state.name, "result": action.result,
            "compensation_failed": action.compensation_failed,
            "error_type": type(action.error).__name__ if action.error is not None else None,
            "compensation_error_type": (type(action.compensation_error).__name__
                                         if action.compensation_error is not None else None),
            "not_applied_evidence": action.not_applied_evidence,
            "compensation_not_applied_evidence": action.compensation_not_applied_evidence,
            "superseded_epoch": action.superseded_epoch,
        }

    def _require_journal(self) -> None:
        if self._durability_fault is not None:
            raise JournalError("journal fault latched; stop dispatch and reopen for recovery") from self._durability_fault
        if self.journal is None and not self.simulation:
            raise JournalError("Durable journal required before mutation dispatch")

    def _persist_action(self, action: StagedAction, event: str, *, create=False) -> None:
        self._require_journal()
        if self.journal is None:  # Explicit SIMULATED execution only.
            return
        try:
            record = self._journal_record(action)
            action._journal_version = (self.journal.create(record) if create else
                                       self.journal.transition(record, action._journal_version, event))
        except JournalDuplicate as exc:
            raise DuplicateOperationError("Durable operation key remains claimed") from exc
        except JournalError as exc:
            self._durability_fault = exc
            action.durability_error = type(exc).__name__
            # The last committed dispatch marker remains the recovery authority.
            # Do not report task success if a returned effect cannot be recorded.
            if action.state != ActionState.PENDING:
                action.state = ActionState.IN_FLIGHT_UNKNOWN
            if action.compensation_state == CompensationState.IN_FLIGHT:
                action.compensation_state = CompensationState.UNKNOWN
            raise

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
                    if action.kind == ActionKind.STAGED_WRITE and self.journal is not None:
                        self._persist_action(action, "TURN_INVALIDATED")
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
                        if self.journal is not None:
                            self._persist_action(action, "TURN_SUPERSEDED")
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

    def _ordered_chain_members(self, chain_id: str) -> list[StagedAction]:
        members = [a for a in self._actions.values() if a.kind == ActionKind.STAGED_WRITE and a.chain_id == chain_id]
        if members and members[0].owner_task_id is not None:
            members.sort(key=lambda a: (a.owner_step_index, a.owner_generation))
        return members

    def stage_write(
        self,
        tool_name: str,
        args: dict[str, Any],
        compensate: Optional[Callable[[Any], Awaitable[None]]] = None,
        *,
        idempotency_key: Optional[str] = None,
        manifest: Optional[ToolManifest] = None,
        registry: Optional[ToolRegistry] = None,
        owner_task_id: Optional[str] = None,
        owner_step_index: Optional[int] = None,
        owner_generation: Optional[int] = None,
        chain_id: Optional[str] = None,
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
        if owner_task_id is None:
            if owner_step_index is not None or owner_generation is not None:
                raise ActionOwnershipError("Partial task ownership is not valid")
            if chain_id is not None and (type(chain_id) is not str or chain_id.startswith("task:")):
                raise ActionOwnershipError("Task chain IDs are reserved for their task owner")
            chain_id = chain_id or f"action:{uuid.uuid4().hex}"
        else:
            owner_generation = 0 if owner_generation is None else owner_generation
            if (type(owner_task_id) is not str or not owner_task_id or
                    type(owner_step_index) is not int or owner_step_index < 0 or
                    type(owner_generation) is not int or owner_generation < 0):
                raise ActionOwnershipError("Task ownership requires task ID, step index and generation")
            expected_chain = f"task:{owner_task_id}"
            if chain_id is not None and chain_id != expected_chain:
                raise ActionOwnershipError("Task action cannot join a different chain")
            chain_id = expected_chain
        if type(chain_id) is not str or not chain_id:
            raise ActionOwnershipError("Chain ID must be a non-empty string")
        if chain_id in self._closed_chains:
            raise ActionOwnershipError("Chain is closed for unwind")
        if any(a.chain_id == chain_id and a.compensation_state in _BLOCKING_COMPENSATION_STATES
               for a in self._actions.values() if a.kind == ActionKind.STAGED_WRITE):
            raise DuplicateOperationError("Chain repair is unresolved")
        if owner_task_id is not None:
            prior = [a for a in self._actions.values() if a.kind == ActionKind.STAGED_WRITE and
                     a.owner_task_id == owner_task_id and a.owner_step_index == owner_step_index]
            if prior:
                latest = max(prior, key=lambda a: a.owner_generation)
                if (owner_generation != latest.owner_generation + 1 or
                        latest.state not in (ActionState.ABORTED, ActionState.COMPENSATED) or
                        latest.compensation_state not in (CompensationState.NOT_STARTED, CompensationState.SUCCEEDED)):
                    raise DuplicateOperationError("Task slot generation exists or prior operation/repair is unresolved")
            elif owner_generation != 0:
                raise ActionOwnershipError("First task generation must be zero")
        reg = self.registry or registry
        mf = manifest if manifest is not None else (reg.get(tool_name) if reg is not None else None)
        args = copy.deepcopy(args)
        digest = payload_digest(tool_name, args)
        ikey = idempotency_key or (
            "task-op:" + hashlib.sha256(json.dumps([owner_task_id, owner_step_index, owner_generation],
                                                   separators=(",", ":")).encode()).hexdigest()
            if owner_task_id is not None else _compute_idempotency_key(tool_name, args))

        # Idempotency enforcement: reject if the key is already live.
        # A later completed operation may have replaced the index entry while
        # an older operation's compensation is still unresolved. Check every
        # retained write with this key, not only the most recent index entry.
        for existing in self._actions.values():
            if (existing.kind == ActionKind.STAGED_WRITE and existing.idempotency_key == ikey and
                    (existing.state in _BLOCKING_STATES or
                     existing.compensation_state in _BLOCKING_COMPENSATION_STATES)):
                raise DuplicateOperationError(
                    f"stage_write rejected: idempotency_key {ikey!r} already exists for "
                    f"action {existing.action_id} in state {existing.state.name} "
                    f"(compensation={existing.compensation_state.name}). "
                    f"Use reconcile_write to resolve IN_FLIGHT_UNKNOWN or COMMITTED_STALE "
                    f"before retrying."
                )

        action_id = f"write-{next(self._id_gen)}-{uuid.uuid4().hex}"
        action = StagedAction(
            action_id=action_id,
            kind=ActionKind.STAGED_WRITE,
            tool_name=tool_name,
            args=args,
            capture_epoch=self.epoch_clock.current,
            idempotency_key=ikey,
            compensate=compensate,
            manifest=(replace(mf, required_permissions=list(mf.required_permissions),
                              depends_on=list(mf.depends_on)) if mf is not None else None),
            _staged_digest=digest,
            intent_session=self._session_id,
            owner_task_id=owner_task_id, owner_step_index=owner_step_index,
            owner_generation=owner_generation, chain_id=chain_id,
        )
        if self.journal is not None:
            self._persist_action(action, "STAGED", create=True)
        self._actions[action_id] = action
        self._ikey_index[ikey] = action_id
        return action

    def abort_write(self, action_id: str) -> None:
        """Used for disfluency/self-correction slot rewriting: kill a staged
        (not yet committed) write outright — zero network calls ever made."""
        action = self._actions.get(action_id)
        if action and action.state == ActionState.PENDING:
            action.state = ActionState.ABORTED
            if self.journal is not None:
                self._persist_action(action, "ABORTED_BEFORE_DISPATCH")

    async def commit_write(
        self,
        action_id: str,
        executor: WriteExecutor,
        *,
        timeout: float = 4.0,
        manifest: Optional[ToolManifest] = None,
        registry: Optional[ToolRegistry] = None,
        policy_engine: Optional[PolicyEngine] = None,
        auth_context: Optional[AuthorizationContext] = None,
        confirmed: bool = False,
    ) -> Any:
        """
        Dispatch a staged write. Outcome classification:

        executor succeeds AND superseded_epoch is None
            -> COMMITTED (normal, epoch is still current)

        executor succeeds AND superseded_epoch is set
            -> COMMITTED_STALE; auto-compensate if a handler is available,
               otherwise log CRITICAL (manual reconciliation required —
               never silently dropped)

        executor raises MutationNotAppliedError with adapter evidence
            -> ABORTED; this operation is known not to have applied.

        executor raises any other Exception
            -> IN_FLIGHT_UNKNOWN. A network failure, malformed reply or other
               exception is not evidence that the remote mutation failed.
               The original exception propagates; callers must reconcile.

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
        if action.recovered:
            raise JournalError("Recovered journal intent cannot be dispatched; reconciliation required")
        if action.chain_id in self._closed_chains:
            raise ActionOwnershipError("Chain is closed; dispatch denied")
        chain_members = self._ordered_chain_members(action.chain_id)
        if (any(a.compensation_state in _BLOCKING_COMPENSATION_STATES for a in chain_members) or
                any(a.state not in (ActionState.COMMITTED, ActionState.ABORTED, ActionState.COMPENSATED)
                    for a in chain_members[:chain_members.index(action)])):
            raise DuplicateOperationError("Chain predecessor or repair is unresolved; dispatch denied")
        if action.state != ActionState.PENDING:
            raise SagaAbortedError(
                f"write {action_id} not in PENDING state: {action.state}"
            )
        if action.capture_epoch != self.epoch_clock.current:
            action._set_terminal(ActionState.ABORTED)
            raise StaleEpochError(
                f"write {action_id} epoch stale: {action.capture_epoch}"
            )

        def check_payload_seal():
            try:
                unchanged = action._staged_digest == payload_digest(action.tool_name, action.args)
            except (ValueError, TypeError) as exc:
                raise PermissionDeniedError("Invalid staged payload") from exc
            if not unchanged:
                raise PermissionDeniedError("Staged payload changed; abort and stage a new action")

        check_payload_seal()

        # Registry policies and the staging snapshot both apply. Call-time
        # metadata may tighten policy but cannot weaken either authority.
        reg = self.registry or registry
        registered = reg.get(action.tool_name) if reg is not None else None
        if reg is not None and registered is None:
            raise PermissionDeniedError("Tool is absent from the trusted registry")
        manifests = []
        for candidate in (registered, action.manifest, manifest):
            if candidate is not None and candidate not in manifests:
                manifests.append(candidate)
        if not manifests:
            raise PermissionDeniedError("A write requires an explicit tool manifest")
        context = auth_context if auth_context is not None else self.auth_context
        baseline = PolicyEngine()
        for mf in manifests:
            if mf.tool_name != action.tool_name:
                raise PermissionDeniedError("Manifest tool name does not match staged action")
            if mf.kind != "write":
                raise InvalidToolKindError(
                    f"commit_write requires a tool with kind='write', but '{action.tool_name}' "
                    f"is manifested as kind='{mf.kind}'"
                )
            baseline.check(mf, context, confirmed=confirmed)
            if mf.requires_confirmation:
                if context.approval is None or not context.approval.matches(action):
                    raise ConfirmationRequiredError("Approval must match staged action, epoch and payload")
            # Validation receives a detached payload; it cannot mutate the approved request.
            if mf.validate_args is not None:
                mf.validate_args(copy.deepcopy(action.args))
            if policy_engine is not None:
                policy_engine.check(mf, context, confirmed=confirmed)
        # No await between authorization and IN_FLIGHT. Seal the executor's
        # payload before scheduling; edits to action.args cannot change the request.
        check_payload_seal()
        dispatch_args = copy.deepcopy(action.args)
        if action.state != ActionState.PENDING:
            raise SagaAbortedError("Action left PENDING during pre-dispatch checks")
        if action.capture_epoch != self.epoch_clock.current:
            action._set_terminal(ActionState.ABORTED)
            raise StaleEpochError("Epoch changed during pre-dispatch checks")

        self._require_journal()
        action.state = ActionState.IN_FLIGHT
        self._persist_action(action, "DISPATCH_INTENT")

        try:
            result = await asyncio.wait_for(executor(dispatch_args), timeout=timeout)

        except asyncio.TimeoutError as exc:
            # We do NOT know whether the remote executed the call.
            # Do not retry without reconciliation.
            if not action._set_terminal(ActionState.IN_FLIGHT_UNKNOWN):
                return action.result  # already resolved by another code path
            action.error = exc
            self._persist_action(action, "FORWARD_UNKNOWN")
            logger.error(
                "write %s (%s) timed out — IN_FLIGHT_UNKNOWN, requires reconciliation "
                "before any retry",
                action.action_id, action.tool_name,
            )
            raise

        except asyncio.CancelledError as exc:
            # Cancellation is a local event only. The remote call may have
            # completed before the cancellation was processed. Classify as
            # IN_FLIGHT_UNKNOWN, not ABORTED.
            if not action._set_terminal(ActionState.IN_FLIGHT_UNKNOWN):
                return action.result
            action.error = exc
            self._persist_action(action, "FORWARD_UNKNOWN")
            logger.error(
                "write %s (%s) was cancelled after dispatch — IN_FLIGHT_UNKNOWN, "
                "requires reconciliation; do not retry blindly",
                action.action_id, action.tool_name,
            )
            raise

        except MutationNotAppliedError as exc:
            if not action._set_terminal(ActionState.ABORTED):
                return action.result
            action.error = exc
            action.not_applied_evidence = exc.evidence
            self._persist_action(action, "FORWARD_NOT_APPLIED")
            raise

        except Exception as exc:
            # Even a local decoding error can follow an applied remote write.
            # Preserve uncertainty and the original operation identity.
            if not action._set_terminal(ActionState.IN_FLIGHT_UNKNOWN):
                return action.result
            action.error = exc
            self._persist_action(action, "FORWARD_UNKNOWN")
            logger.error(
                "write %s (%s) raised %s after dispatch — IN_FLIGHT_UNKNOWN; "
                "requires reconciliation before retry",
                action.action_id, action.tool_name, type(exc).__name__,
            )
            raise

        # ----------------------------------------------------------------
        # Executor returned successfully. Classify: COMMITTED or STALE?
        # ----------------------------------------------------------------
        action.result = result
        if self._durability_fault is not None:
            action.state = ActionState.IN_FLIGHT_UNKNOWN
            action.durability_error = type(self._durability_fault).__name__
            self._require_journal()

        if action.superseded_epoch is None and action.chain_id not in self._closed_chains:
            # Epoch is still current — normal commit.
            if not action._set_terminal(ActionState.COMMITTED):
                return action.result
            self._persist_action(action, "FORWARD_COMMITTED")
            self._chain.append(action)
            return result

        # Epoch advanced while the executor was running. The mutation
        # succeeded remotely and IS a real side-effect in the world, but
        # it belongs to an obsolete conversational turn. Classify as
        # COMMITTED_STALE and trigger compensation.
        if not action._set_terminal(ActionState.COMMITTED_STALE):
            return action.result
        self._persist_action(action, "FORWARD_COMMITTED_STALE")

        if action.compensate is not None and action.chain_id not in self._closed_chains:
            logger.warning(
                "COMMITTED_STALE: write %s (%s) succeeded after epoch advanced "
                "(capture=%d, superseded_by=%s) — auto-compensating now",
                action.action_id, action.tool_name,
                action.capture_epoch, action.superseded_epoch,
            )
            action._compensation_task = asyncio.create_task(self._auto_compensate(action))
        else:
            logger.critical(
                "COMMITTED_STALE: write %s (%s) succeeded after epoch advanced "
                "(capture=%d, superseded_by=%s); automatic repair unavailable or chain closed — "
                "MANUAL RECONCILIATION REQUIRED; remote side-effect is unresolved",
                action.action_id, action.tool_name,
                action.capture_epoch, action.superseded_epoch,
            )
        return result

    async def _auto_compensate(self, action: StagedAction) -> None:
        """Background entry point sharing ownership with all saga repair paths."""
        await self.compensate_action(action.action_id)

    async def compensate_action(self, action_id: str) -> None:
        """Make at most one automatic compensation attempt in this process.

        This is local serialization, not durable/distributed ownership. An
        ambiguous or explicitly rejected attempt is never automatically repeated.
        """
        action = self._actions[action_id]
        async with action._resolution_lock:
            await self._compensate_locked(action)

    async def _compensate_locked(self, action: StagedAction) -> None:
        if action.state not in (ActionState.COMMITTED, ActionState.COMMITTED_STALE):
            return
        if action.compensate is None or action.compensation_state != CompensationState.NOT_STARTED:
            return
        members = self._ordered_chain_members(action.chain_id)
        if action in members:
            if any(a.chain_id == action.chain_id and
                   (a.state not in (ActionState.ABORTED, ActionState.COMPENSATED) or
                    a.compensation_state in _BLOCKING_COMPENSATION_STATES)
                   for a in members[members.index(action) + 1:]):
                return  # never undo an ancestor while a descendant can still apply
        self._require_journal()
        action.compensation_state = CompensationState.IN_FLIGHT
        self._persist_action(action, "COMPENSATION_INTENT")
        try:
            await action.compensate(action.result)
        except asyncio.CancelledError as exc:
            action.compensation_state = CompensationState.UNKNOWN
            action.compensation_error = exc
            action.compensation_failed = True
            self._persist_action(action, "COMPENSATION_UNKNOWN")
            logger.critical(
                "AUTO-COMPENSATION FAILED for %s (%s): cancelled after possible dispatch; "
                "outcome UNKNOWN, no automatic retry", action.action_id, action.tool_name,
            )
            raise
        except MutationNotAppliedError as exc:
            action.compensation_state = CompensationState.NOT_APPLIED
            action.compensation_error = exc
            action.compensation_not_applied_evidence = exc.evidence
            action.compensation_failed = True
            self._persist_action(action, "COMPENSATION_NOT_APPLIED")
            logger.critical("AUTO-COMPENSATION FAILED for %s (%s): known not applied; repair required",
                            action.action_id, action.tool_name)
        except Exception as exc:
            action.compensation_state = CompensationState.UNKNOWN
            action.compensation_error = exc
            action.compensation_failed = True
            self._persist_action(action, "COMPENSATION_UNKNOWN")
            logger.critical("AUTO-COMPENSATION FAILED for %s (%s): %s; outcome UNKNOWN, no automatic retry",
                            action.action_id, action.tool_name, type(exc).__name__)
        else:
            action.compensation_state = CompensationState.SUCCEEDED
            action.state = ActionState.COMPENSATED
            self._persist_action(action, "COMPENSATED")
            self._chain = [a for a in self._chain if a.state != ActionState.COMPENSATED]
            logger.info("compensated %s (%s)", action.action_id, action.tool_name)

    # ---- reconciliation ---------------------------------------------------

    async def reconcile_write(
        self,
        action_id: str,
        status_check: StatusCheck,
    ) -> None:
        """Serialize status resolution and saga compensation for one action.

        Repeated resolution of a resolved action is a no-op. Status callbacks
        must return exactly True, False or None and must not re-enter resolution
        for the same action. False requires authoritative absence, including no
        delayed future application; an eventually consistent miss is insufficient.
        """
        action = self._actions[action_id]
        async with action._resolution_lock:
            self._require_journal()
            if (action.state in (ActionState.COMMITTED, ActionState.ABORTED, ActionState.COMPENSATED)
                    and action.compensation_state not in _BLOCKING_COMPENSATION_STATES):
                return
            await self._reconcile_write_locked(action, status_check)

    async def _reconcile_write_locked(self, action: StagedAction, status_check: StatusCheck) -> None:
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
          True  -> remote confirms it happened; consider the first compensation
                   attempt if available, never repeat an existing attempt
          False -> remote confirms it was rolled back; set COMPENSATED

        NEVER use reconcile_write to re-attempt the original executor.
        If you need to redo the operation, call stage_write + commit_write
        with the appropriate idempotency key cleared.
        """
        action_id = action.action_id
        if (action.state not in (ActionState.IN_FLIGHT_UNKNOWN, ActionState.COMMITTED_STALE)
                and action.compensation_state not in _BLOCKING_COMPENSATION_STATES):
            raise SagaAbortedError(
                f"reconcile_write only valid for IN_FLIGHT_UNKNOWN or COMMITTED_STALE; "
                f"action {action_id} is in {action.state.name}"
            )

        outcome = await status_check(action)
        if outcome is not None and type(outcome) is not bool:
            raise TypeError("status_check must return exactly True, False, or None")

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
                    not action.recovered and
                    action.chain_id not in self._closed_chains and
                    action.superseded_epoch is None
                    and action.capture_epoch == self.epoch_clock.current
                )
                if is_current:
                    # reconcile_write is the authorized resolver for IN_FLIGHT_UNKNOWN;
                    # we bypass _set_terminal here because IN_FLIGHT_UNKNOWN is terminal
                    # by design, but reconciliation is its explicit resolution path.
                    action.state = ActionState.COMMITTED
                    self._persist_action(action, "RECONCILED_PRESENT")
                    self._chain.append(action)
                    logger.info(
                        "reconcile_write %s (%s): confirmed executed -> COMMITTED",
                        action_id, action.tool_name,
                    )
                else:
                    action.state = ActionState.COMMITTED_STALE
                    self._persist_action(action, "RECONCILED_PRESENT_STALE")
                    logger.warning(
                        "reconcile_write %s (%s): confirmed executed but epoch stale "
                        "(capture=%d, current=%d) -> COMMITTED_STALE",
                        action_id, action.tool_name,
                        action.capture_epoch, self.epoch_clock.current,
                    )
                    if action.compensate is not None:
                        await self._compensate_locked(action)
                    else:
                        logger.critical(
                            "COMMITTED_STALE via reconciliation for %s (%s): "
                            "no compensate() handler — MANUAL RECONCILIATION REQUIRED",
                            action_id, action.tool_name,
                        )
            else:
                # A known effect persists. Only a first repair attempt is allowed.
                self._persist_action(action, "RECONCILED_PRESENT_STALE")
                logger.warning(
                    "reconcile_write %s (%s): effect confirmed, compensation=%s; "
                    "existing attempts will not be retried",
                    action_id, action.tool_name, action.compensation_state.name,
                )
                if action.compensate is not None:
                    await self._compensate_locked(action)
                else:
                    logger.critical(
                        "COMMITTED_STALE reconcile confirmed for %s (%s): "
                        "no compensate() handler — MANUAL RECONCILIATION REQUIRED",
                        action_id, action.tool_name,
                    )

        else:
            # outcome is False — remote confirms NOT executed / rolled back.
            # Direct assignment since we're the authorized resolver for these states.
            if action.state in (ActionState.IN_FLIGHT_UNKNOWN, ActionState.ABORTED):
                action.state = ActionState.ABORTED
            else:
                # A known applied effect is now authoritatively absent; do not
                # rewrite its history as if it had never happened.
                action.state = ActionState.COMPENSATED
                action.compensation_state = CompensationState.SUCCEEDED
                action.compensation_failed = False
                self._chain = [a for a in self._chain if a.state != ActionState.COMPENSATED]
            self._persist_action(action, "RECONCILED_ABSENT")
            logger.info(
                "reconcile_write %s (%s): authoritative absence -> %s",
                action_id, action.tool_name, action.state.name,
            )

    # ---- saga compensation ------------------------------------------------

    def close_chain(self, chain_id: str, *, owner_task_id: Optional[str] = None) -> None:
        """Persist stop intent before repair; never reopen or imply remote rollback."""
        if (owner_task_id is not None and chain_id != f"task:{owner_task_id}") or (
                owner_task_id is None and chain_id.startswith("task:")):
            raise ActionOwnershipError("Chain closure must identify its task owner")
        if self.journal is not None:
            try:
                self._require_journal()
                self.journal.close_chain(chain_id, owner_task_id)
            except JournalError as exc:
                self._durability_fault = exc
                raise
        self._closed_chains.add(chain_id)
        for action in list(self._actions.values()):
            if action.kind == ActionKind.STAGED_WRITE and action.chain_id == chain_id:
                self.abort_write(action.action_id)

    async def abort_chain_from(self, failed_action_id: str) -> bool:
        """Close one known chain and unwind only proven-safe predecessors.

        Unknown IDs never imply a global chain. Closing prevents new forward
        dispatch; unknown effects/repair or changed journal versions stop unwind.
        False means manual reconciliation is needed, not successful rollback.
        """
        failed = self._actions.get(failed_action_id)
        if failed is None or failed.kind != ActionKind.STAGED_WRITE or failed.chain_id is None:
            raise ActionOwnershipError("A recorded failed write and chain owner are required")
        lock = self._chain_unwind_locks.setdefault(failed.chain_id, asyncio.Lock())
        async with lock:
            self.close_chain(failed.chain_id, owner_task_id=failed.owner_task_id)
            if self.journal is not None:
                try:
                    for version, record in self.journal.records():
                        if record["chain_id"] == failed.chain_id:
                            local = self._actions.get(record["action_id"])
                            if local is None or local._journal_version != version:
                                return False
                except JournalError as exc:
                    self._durability_fault = exc
                    raise
            if failed.recovered or failed.state not in (ActionState.PENDING, ActionState.ABORTED):
                return False
            members = self._ordered_chain_members(failed.chain_id)
            idx = members.index(failed)
            if any(a.recovered or a.state in (ActionState.IN_FLIGHT, ActionState.IN_FLIGHT_UNKNOWN,
                                              ActionState.COMMITTED_STALE) or
                   a.compensation_state in _BLOCKING_COMPENSATION_STATES for a in members):
                return False
            if any(a.state not in (ActionState.PENDING, ActionState.ABORTED, ActionState.COMPENSATED)
                   for a in members[idx + 1:]):
                return False
            for action in members:
                if action.state == ActionState.PENDING:
                    self.abort_write(action.action_id)
            for action in reversed(members[:idx]):
                if action.state in (ActionState.ABORTED, ActionState.COMPENSATED):
                    continue
                if action.state != ActionState.COMMITTED or action.compensate is None:
                    return False
                await self.compensate_action(action.action_id)
                if action.state != ActionState.COMPENSATED:
                    return False
            return True

    # ---- inspector (for the Round-2 live debug overlay) -------------------

    def snapshot(self) -> list[dict[str, Any]]:
        return [
            {
                "id": a.action_id,
                "kind": a.kind.name,
                "tool": a.tool_name,
                "state": a.state.name,
                "recovered": a.recovered,
                "journal_version": a._journal_version,
                "durability_error": a.durability_error,
                "execution_mode": ("SIMULATED" if self.simulation else
                                   "JOURNALED" if self.journal is not None else "DISPATCH_DISABLED"),
                "epoch": a.capture_epoch,
                "superseded_epoch": a.superseded_epoch,
                "idempotency_key": a.idempotency_key,
                "chain_id": a.chain_id,
                "owner_task_id": a.owner_task_id,
                "owner_step_index": a.owner_step_index,
                "owner_generation": a.owner_generation,
                "compensation_failed": a.compensation_failed,
                "compensation_state": a.compensation_state.name,
                "compensation_error_type": (type(a.compensation_error).__name__
                                            if a.compensation_error is not None else None),
                "compensation_not_applied_evidence": a.compensation_not_applied_evidence,
                "not_applied_evidence": a.not_applied_evidence,
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

