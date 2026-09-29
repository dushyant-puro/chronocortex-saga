"""
agent.py

ChronoCortex-Saga (CCS-Agent) — LiveKit Agents SDK entrypoint.

Binds:
  - Cortex-A (acoustic reflex layer): VAD + pitch-slope endpointing,
    barge-in cutoff, non-blocking backchannels. Runs inline in the
    LiveKit room I/O path.
  - Cortex-B (async cognitive core): interim-transcript repair parsing,
    entity extraction, speculative read dispatch, staged-write commit
    on confirmed turn boundary.
  - runtime.speculative_saga / runtime.grounding_guard: the invariant
    engine underneath both cortices.

NOTE ON LIVEKIT API SURFACE: this targets the livekit-agents >=0.9 API
shape (AgentSession / STT-LLM-TTS pipeline with interim-transcript
callbacks). If your installed SDK version differs, the integration
points are clearly marked with `# LIVEKIT-HOOK:` comments — swap the
decorator/callback names for your version without touching the
cortex/runtime logic itself, which is SDK-agnostic.
"""

from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass
from typing import Any, Optional

from livekit import agents
from livekit.agents import AgentSession, JobContext, WorkerOptions, cli
from livekit.plugins import deepgram, silero, cartesia

from runtime.speculative_saga import SpeculativeSagaManager, TurnEpochClock, StaleEpochError
from runtime.grounding_guard import GroundingGuard, zero_hop_repair, RepairOutcome
from tools.fleet_tools import READ_TOOLS, WRITE_TOOLS

logger = logging.getLogger("ccs.agent")

# Pitch-slope endpointing threshold: a falling pitch contour at an
# intra-turn pause is treated as end-of-turn; a flat/rising contour
# (typical of hesitation, not completion) holds the floor.
PITCH_FALL_THRESHOLD_HZ_PER_S = -12.0
BACKCHANNEL_HOLD_MS = 900
CORTEX_A_TARGET_LATENCY_MS = 80


@dataclass
class PendingSlot:
    """A recognized entity/argument awaiting either commit (turn boundary
    confirmed) or invalidation (disfluency/correction)."""
    field_name: str
    raw_value: str
    tool_name: str
    token_range: tuple[int, int]
    staged_action_id: Optional[str] = None


class CCSAgent:
    def __init__(self) -> None:
        self.epoch_clock = TurnEpochClock()
        self.saga = SpeculativeSagaManager(self.epoch_clock)
        self.guard = GroundingGuard()
        self._pending_slots: dict[str, PendingSlot] = {}
        self._backchannel_task: Optional[asyncio.Task] = None
        self._current_domain = "default"

    # ---------------------------------------------------------------
    # Cortex-A: acoustic reflex layer
    # ---------------------------------------------------------------

    def on_pitch_frame(self, pitch_slope_hz_per_s: float, in_silence: bool) -> None:
        """
        LIVEKIT-HOOK: wire to your VAD/pitch-tracking frame callback.
        Called at frame rate (~every 10-20ms) directly in the audio path.
        Must stay allocation-light to hold the <80ms reflex budget.
        """
        if in_silence and pitch_slope_hz_per_s > PITCH_FALL_THRESHOLD_HZ_PER_S:
            # Flat/rising contour during a pause => still speaking, hold floor.
            self._maybe_start_backchannel()
        elif in_silence and pitch_slope_hz_per_s <= PITCH_FALL_THRESHOLD_HZ_PER_S:
            # Falling contour => genuine turn boundary.
            asyncio.create_task(self._confirm_turn_boundary())

    def on_barge_in(self) -> None:
        """
        LIVEKIT-HOOK: wire to your playback-interrupt / user-speech-while-
        agent-speaking event. Must cut audio in <80ms and immediately
        advance the epoch so any in-flight speculative work tied to the
        interrupted turn is invalidated.
        """
        logger.info("barge-in detected — cutting playback, advancing epoch")
        if self._backchannel_task and not self._backchannel_task.done():
            self._backchannel_task.cancel()
        asyncio.create_task(self.epoch_clock.advance(reason="barge_in"))
        # LIVEKIT-HOOK: session.interrupt() / equivalent playback-stop call here.

    def _maybe_start_backchannel(self) -> None:
        if self._backchannel_task and not self._backchannel_task.done():
            return
        self._backchannel_task = asyncio.create_task(self._emit_backchannel())

    async def _emit_backchannel(self) -> None:
        await asyncio.sleep(BACKCHANNEL_HOLD_MS / 1000)
        # LIVEKIT-HOOK: session.say("Checking that now...", allow_interruptions=True)
        logger.debug("backchannel emitted (non-blocking, interruptible)")

    async def _confirm_turn_boundary(self) -> None:
        await self.epoch_clock.advance(reason="endpoint")
        await self._commit_all_pending_slots()

    # ---------------------------------------------------------------
    # Cortex-B: async cognitive core
    # ---------------------------------------------------------------

    def on_interim_transcript(self, token: str, confidence: float, start_ts: float, end_ts: float) -> None:
        """
        LIVEKIT-HOOK: wire to your STT interim-result callback
        (e.g. Deepgram's is_final=False events). Feeds the grounding
        guard's token buffer and reacts to repair cues in real time.
        """
        idx = self.guard.ingest_token(token, confidence, start_ts, end_ts)
        self._try_extract_entity(token, idx)

    def _try_extract_entity(self, token: str, token_idx: int) -> None:
        """
        Placeholder entity recognizer: in production this is a small
        streaming NER/slot-filler. On recognizing a stable candidate
        value for a known field, fire a speculative read (if the field
        feeds an idempotent lookup) or stage a write slot.
        """
        # Example wiring for the fleet domain: recognizing a truck id.
        if token.lower().startswith("truck-") or token.lower().startswith("truck "):
            truck_id = token.lower().replace("truck ", "truck-").strip()
            entity_hash = f"truck_id:{truck_id}"
            self.saga.fire_speculative_read(
                tool_name="query_telemetry",
                args={"truck_id": truck_id},
                executor=READ_TOOLS["query_telemetry"],
                entity_hash=entity_hash,
            )

    def stage_slot_for_write(
        self,
        field_name: str,
        raw_value: str,
        tool_name: str,
        token_range: tuple[int, int],
        domain: str = "default",
    ) -> Optional[str]:
        """
        Called by Cortex-B once a mutating-tool argument is recognized.
        Runs ATGG grounding + ZH-SR repair; only stages (never dispatches)
        the write. A prior slot for the same field is invalidated first —
        this is the "instantaneous retraction on self-correction" path.
        """
        self._current_domain = domain
        grounded, conf, reason = self.guard.check_argument_grounding(raw_value, token_range, domain)
        if not grounded:
            logger.info("rejected ungrounded arg for %s: %s (conf=%.2f)", field_name, reason, conf)
            return None

        outcome, repaired = zero_hop_repair(field_name, raw_value, expected_type="text")
        value = repaired if outcome is RepairOutcome.OK else raw_value

        # Retract any prior staged slot for this field (self-correction).
        prior = self._pending_slots.get(field_name)
        if prior and prior.staged_action_id:
            self.saga.abort_write(prior.staged_action_id)

        slot = PendingSlot(field_name=field_name, raw_value=value, tool_name=tool_name, token_range=token_range)
        self._pending_slots[field_name] = slot
        return field_name

    async def _commit_all_pending_slots(self) -> None:
        """
        Called once Cortex-A confirms the turn boundary. Groups pending
        slots by tool, stages the write, and commits it through the saga
        manager. If a chain step fails, unwinds prior commits.
        """
        by_tool: dict[str, dict[str, Any]] = {}
        for slot in self._pending_slots.values():
            by_tool.setdefault(slot.tool_name, {})[slot.field_name] = slot.raw_value

        for tool_name, args in by_tool.items():
            if tool_name not in WRITE_TOOLS:
                continue
            forward, compensate = WRITE_TOOLS[tool_name]
            action = self.saga.stage_write(tool_name, args, compensate=compensate)
            try:
                result = await self.saga.commit_write(action.action_id, forward)
                logger.info("committed %s -> %s", tool_name, result)
            except StaleEpochError:
                logger.info("write %s superseded before dispatch — no-op", tool_name)
            except asyncio.TimeoutError:
                logger.error("write %s IN_FLIGHT_UNKNOWN — awaiting reconciliation", tool_name)
            except Exception as exc:  # noqa: BLE001
                logger.warning("write %s failed (%s) — unwinding chain", tool_name, exc)
                await self.saga.abort_chain_from(action.action_id)

        self._pending_slots.clear()
        self.guard.reset_turn()

    # ---------------------------------------------------------------
    # Debug overlay for Round-2 live jury interrogation
    # ---------------------------------------------------------------

    def debug_state(self) -> dict[str, Any]:
        return {
            "epoch": self.epoch_clock.current,
            "pending_slots": list(self._pending_slots.keys()),
            "actions": self.saga.snapshot(),
        }


# --------------------------------------------------------------------------
# LiveKit entrypoint
# --------------------------------------------------------------------------

async def entrypoint(ctx: JobContext) -> None:
    await ctx.connect()

    ccs = CCSAgent()

    session = AgentSession(
        stt=deepgram.STT(model="nova-3", interim_results=True),
        llm=None,  # LIVEKIT-HOOK: plug Gemini 2.5 Flash / Groq Llama-3.3-70B / gpt-4o-mini here
        tts=cartesia.TTS(model="sonic"),
        vad=silero.VAD.load(),
    )

    # LIVEKIT-HOOK: exact event names depend on SDK version — these map
    # conceptually to "interim STT result", "user speech interrupted
    # playback", and "VAD-detected pause with pitch metadata" respectively.
    session.on("user_speech_committed", lambda ev: ccs.on_interim_transcript(
        ev.transcript, getattr(ev, "confidence", 1.0), ev.start_time, ev.end_time,
    ))
    session.on("agent_speech_interrupted", lambda ev: ccs.on_barge_in())

    await session.start(room=ctx.room)
    logger.info("CCS-Agent session started; epoch clock initialized at 0")


if __name__ == "__main__":
    cli.run_app(WorkerOptions(entrypoint_fnc=entrypoint))
