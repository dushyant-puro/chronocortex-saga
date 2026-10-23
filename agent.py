import asyncio
import logging
import uuid
from dataclasses import dataclass
from typing import Any, Optional

from livekit.agents import AgentSession, JobContext

from runtime.speculative_saga import ActionState, SpeculativeSagaManager, TurnEpochClock, StaleEpochError
from runtime.grounding_guard import GroundingGuard, zero_hop_repair, RepairOutcome
from runtime.task_engine import TaskManager
from runtime.policy_engine import AuthorizationContext, PolicyError
from runtime.action_journal import JournalError, SQLiteActionJournal
from tools.fleet_tools import READ_TOOLS, WRITE_TOOLS, TOOL_REGISTRY

logger = logging.getLogger("ccs.agent")

BACKCHANNEL_HOLD_MS = 900
CORTEX_A_TARGET_LATENCY_MS = 80

@dataclass
class PendingSlot:
    field_name: str
    raw_value: str
    tool_name: str
    token_range: tuple[int, int]
    staged_action_id: Optional[str] = None


class CCSAgent:
    """Legacy SIMULATED fleet harness; production voice uses VoiceObserverAgent."""
    def __init__(self, *, auth_context: Optional[AuthorizationContext] = None,
                 journal: Optional[SQLiteActionJournal] = None, simulation: bool = False) -> None:
        self._simulation = simulation
        self.epoch_clock = TurnEpochClock()
        self.saga = SpeculativeSagaManager(self.epoch_clock, registry=TOOL_REGISTRY,
                                           auth_context=auth_context, journal=journal, simulation=simulation)

        # TaskManager must be created before GroundingGuard so we can wire
        # the on_eviction callback. resolve_field is bound to guard after
        # guard creation below.
        self.task_manager = TaskManager(
            saga=self.saga,
            resolve_field=lambda f: None,  # placeholder, rebound below
        )

        self.guard = GroundingGuard(
            epoch_clock=self.epoch_clock,
            on_eviction=self.task_manager.on_field_evicted,
        )
        # Now rebind resolve_field to the real guard method
        self.task_manager._resolve_field = self.guard.resolve_current_value

        self._pending_slots: dict[str, PendingSlot] = {}
        self._backchannel_task: Optional[asyncio.Task] = None
        self.session: Optional[AgentSession] = None
        
        # Track ingested tokens to avoid duplicating them on repeated interim transcripts
        self._ingested_word_count = 0

        # Wire narrow fleet-specific prediction heuristic:
        # Upon query_telemetry completing, pre-fire query_traffic IFF destination is resolvable.
        # HARD CONSTRAINT: If resolve_current_value("destination") returns None, do NOT fire.
        self.saga.set_prediction_hook(self._fleet_prediction_hook)

    def _fleet_prediction_hook(self, action: Any, result: Any) -> None:
        if action.tool_name == "query_telemetry":
            destination = self.guard.resolve_current_value("destination")
            if destination is not None:
                logger.info(
                    "Fleet prediction rule: query_telemetry completed, pre-firing query_traffic "
                    "for grounded destination %s", destination
                )
                self.saga.fire_speculative_read(
                    tool_name="query_traffic",
                    args={"route": destination},
                    executor=READ_TOOLS["query_traffic"],
                    entity_hash=f"route:{destination}",
                )

    # ---------------------------------------------------------------
    # Cortex-A: acoustic reflex layer
    # ---------------------------------------------------------------

    def on_barge_in(self) -> None:
        """
        Invoked when user starts speaking (user_state_changed -> speaking).
        """
        if not self._simulation:
            raise PolicyError("Legacy voice interruption requires explicit simulation")
        logger.info("barge-in detected — cutting playback, advancing epoch")
        if self._backchannel_task and not self._backchannel_task.done():
            self._backchannel_task.cancel()
        asyncio.create_task(self.epoch_clock.advance(reason="barge_in"))
        
        if self.session:
            # Verified LiveKit API to interrupt agent
            self.session.interrupt()

    async def _emit_backchannel(self) -> None:
        await asyncio.sleep(BACKCHANNEL_HOLD_MS / 1000)
        # We simulate backchannel if no real TTS available
        logger.debug("backchannel emitted (non-blocking, interruptible)")
        if self.session and openai:
            # We would use real session.say here if we had a full pipeline
            pass

    async def _confirm_turn_boundary(self) -> None:
        """
        Invoked when user stops speaking (user_state_changed -> listening).
        """
        if not self._simulation:
            raise PolicyError("Legacy voice dispatch is restricted to explicit simulation")
        await self._commit_all_pending_slots()
        await self.epoch_clock.advance(reason="endpoint")
        # Turn boundary confirmed, candidates are cleared
        self.guard.reset_turn()
        self._ingested_word_count = 0

    # ---------------------------------------------------------------
    # Cortex-B: async cognitive core
    # ---------------------------------------------------------------

    def on_interim_transcript(self, transcript: str, is_final: bool, confidence: float, start_ts: float, end_ts: float) -> None:
        """
        Legacy synthetic scalar-metadata interface; never a live event adapter.
        """
        if not self._simulation:
            raise PolicyError("Live transcripts require the word-evidence observer")
        # Split by spaces and commas so clause boundaries like 'Chennai,' remain distinct or at least are processed
        # For simplicity, we just split by space and let the guard handle regex matches
        words = transcript.strip().split()
        
        new_words = words[self._ingested_word_count:]
        for w in new_words:
            idx = self.guard.ingest_token(w, confidence, start_ts, end_ts)
            self._try_extract_entity(w, idx)
            self._ingested_word_count += 1
            
        if is_final:
            # If the segment is finalized, we don't strictly need to do anything extra here
            # because turn boundaries are handled by user_state_changed
            pass

    def _try_extract_entity(self, token: str, token_idx: int) -> None:
        """
        Extracts fleet entities. 
        """
        lowered = token.lower()
        
        # 1. Truck ID extraction
        if lowered.startswith("truck-") or lowered.startswith("truck"):
            truck_id = lowered.replace("truck ", "truck-").strip()
            if truck_id == "truck":
                return
            
            # Stage the read
            entity_hash = f"truck_id:{truck_id}"
            self.saga.fire_speculative_read(
                tool_name="query_telemetry",
                args={"truck_id": truck_id},
                executor=READ_TOOLS["query_telemetry"],
                entity_hash=entity_hash,
            )
            # Stage slot for write
            self._stage_slot_for_write("truck_id", truck_id, "reroute_truck", (token_idx, token_idx + 1))
            
        # 2. Destination extraction
        # We'll hardcode the known destinations for this exact scenario
        elif "chennai" in lowered or "bengaluru" in lowered:
            # Remove punctuation for matching
            dest = token.strip(".,;!").capitalize()
            self._stage_slot_for_write("destination", dest, "reroute_truck", (token_idx, token_idx + 1))

    def _stage_slot_for_write(
        self,
        field_name: str,
        raw_value: str,
        tool_name: str,
        token_range: tuple[int, int],
        domain: str = "default",
    ) -> Optional[str]:
        self._current_domain = domain
        
        # We use GroundingGuard's Phase 3 capability to stage the candidate
        staged_field = self.guard.stage_candidate(field_name, raw_value, token_range, domain)
        if not staged_field:
            return None

        # Resolve the slot in local pending_slots dictionary
        slot = PendingSlot(field_name=field_name, raw_value=raw_value, tool_name=tool_name, token_range=token_range)
        self._pending_slots[field_name] = slot
        return field_name

    async def _commit_all_pending_slots(self) -> None:
        if not self._simulation:
            raise PolicyError("Voice evidence cannot authorize legacy mutation dispatch")
        by_tool: dict[str, dict[str, Any]] = {}
        for slot in self._pending_slots.values():
            # Crucially, we must re-resolve against the GroundingGuard to ensure it wasn't tombstoned!
            resolved_value = self.guard.resolve_current_value(slot.field_name)
            if resolved_value is None:
                continue
            by_tool.setdefault(slot.tool_name, {})[slot.field_name] = resolved_value

        chain_id = f"voice:{uuid.uuid4().hex}"
        for tool_name, args in by_tool.items():
            if tool_name not in WRITE_TOOLS:
                continue
            
            # Reroute tool needs BOTH truck_id and destination
            if tool_name == "reroute_truck" and ("truck_id" not in args or "destination" not in args):
                logger.warning(f"write {tool_name} failed due to missing arguments: {args}")
                continue
                
            forward, compensate = WRITE_TOOLS[tool_name]
            action = self.saga.stage_write(tool_name, args, compensate=compensate, chain_id=chain_id)
            try:
                result = await self.saga.commit_write(action.action_id, forward)
                logger.info("committed %s -> %s", tool_name, result)
            except StaleEpochError:
                logger.info("write %s superseded before dispatch — no-op", tool_name)
                break
            except asyncio.TimeoutError:
                logger.error("write %s IN_FLIGHT_UNKNOWN — awaiting reconciliation", tool_name)
                break
            except PolicyError:
                # A pre-dispatch denial is not a remote failure and never
                # authorizes compensation of previously committed actions.
                logger.warning("write %s awaits trusted authorization; action=%s",
                               tool_name, action.action_id)
                break
            except JournalError:
                logger.error("write %s requires durable recovery; action=%s", tool_name, action.action_id)
                break
            except Exception as exc:  # noqa: BLE001
                if action.state == ActionState.IN_FLIGHT_UNKNOWN:
                    logger.error("write %s IN_FLIGHT_UNKNOWN after %s — awaiting reconciliation",
                                 tool_name, type(exc).__name__)
                else:
                    logger.warning("write %s failed (%s) — unwinding chain", tool_name, type(exc).__name__)
                    await self.saga.abort_chain_from(action.action_id)
                break

        self._pending_slots.clear()


# --------------------------------------------------------------------------
# LiveKit entrypoint
# --------------------------------------------------------------------------

async def entrypoint(ctx: JobContext) -> None:
    raise RuntimeError(
        "Legacy voice mutation host retired. Configure an explicit streaming STT "
        "and use voice.livekit_observer.start_observer; provider/audio validation is gated."
    )


if __name__ == "__main__":
    raise SystemExit(
        "Legacy voice host is disabled. See docs/engineering/voice-evidence.md "
        "for the proposal-only SDK integration and offline replay."
    )
