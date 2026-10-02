"""
eval/audit/repro_commit_write_race.py

Reproduction / regression script for the Phase 0 race condition in
speculative_saga.SpeculativeSagaManager.commit_write.

Original bug (pre-Phase 1):
  If the epoch advanced while a staged write executor was in-flight,
  _on_epoch_advance marked the action ABORTED, but commit_write() did not
  check the current epoch or action.state after awaiting the executor.
  It unconditionally overwrote action.state with COMMITTED and appended
  the action to _chain.

Phase 1 fix:
  commit_write() now uses _set_terminal() guards and checks superseded_epoch
  after the executor completes. If superseded_epoch is set, the action
  resolves to COMMITTED_STALE (not COMMITTED), and auto-compensation fires
  if a handler is available. The race condition no longer reproduces.

  _on_epoch_advance intentionally does NOT cancel or force-abort IN_FLIGHT
  writes; it only records superseded_epoch. The executor is allowed to run
  to completion for deterministic outcome classification.
"""

import asyncio
import os
import sys
from pathlib import Path

# Ensure repo root is in sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from runtime.speculative_saga import (
    ActionState,
    SpeculativeSagaManager,
    TurnEpochClock,
)


async def main() -> None:
    print("=== Reproducing commit_write race condition ===")
    clock = TurnEpochClock()
    saga = SpeculativeSagaManager(clock)

    # 1. Stage a mutating write in epoch 0
    initial_epoch = clock.current
    print(f"[Init] Initial epoch: {initial_epoch}")

    action = saga.stage_write("test_mutate", {"key": "val"})
    print(f"[1] Staged action {action.action_id} at epoch {action.capture_epoch}, state: {action.state.name}")
    assert action.state == ActionState.PENDING

    # Event to control executor timing precisely
    executor_started = asyncio.Event()
    executor_can_finish = asyncio.Event()

    async def slow_executor(args: dict) -> dict:
        print(f"[Executor] slow_executor called with {args}")
        executor_started.set()
        await executor_can_finish.wait()
        print("[Executor] slow_executor finished work, returning result")
        return {"result": "success"}

    # 2. Launch commit_write
    commit_task = asyncio.create_task(saga.commit_write(action.action_id, slow_executor))

    # Wait until executor is actively running inside commit_write
    await executor_started.wait()
    # Give a tiny slice to ensure commit_write set IN_FLIGHT
    await asyncio.sleep(0.01)
    print(f"[2] Inside commit_write: action state is {action.state.name}")
    assert action.state == ActionState.IN_FLIGHT

    # 3. Advance epoch (e.g. barge-in or turn boundary) while executor is in-flight
    print("[3] Advancing epoch via clock.advance(reason='barge_in')...")
    new_epoch = await clock.advance(reason="barge_in")
    print(f"[3] New epoch is {new_epoch}. Clock current: {clock.current}")
    print(f"[3] Immediately after epoch advance, action state is: {action.state.name}")

    if action.state == ActionState.ABORTED:
        print("    -> Pre-Phase-1 behaviour: _on_epoch_advance force-aborted the write.")
    elif action.state == ActionState.IN_FLIGHT:
        print("    -> Expected (Phase 1): action remains IN_FLIGHT (superseded_epoch set, "
              "not force-aborted); executor allowed to complete for deterministic outcome.")
    else:
        print(f"    -> Unexpected: action state is {action.state.name}")

    # 4. Now allow the in-flight executor to complete
    print("[4] Allowing executor to finish...")
    executor_can_finish.set()
    commit_result = await commit_task
    print(f"[4] commit_write returned: {commit_result}")

    # 5. Inspect final action state
    print(f"[5] Final action state: {action.state.name}")
    print(f"[5] Action in saga._chain: {any(a.action_id == action.action_id for a in saga._chain)}")

    if action.state == ActionState.COMMITTED:
        print("\n*** RACE CONDITION REPRODUCED SUCCESSFULLY ***")
        print(
            f"BUG CONFIRMED: Action {action.action_id} was ABORTED due to epoch advance "
            f"({action.capture_epoch} < {clock.current}), but commit_write unconditionally "
            f"overwrote ABORTED with {action.state.name} and appended it to _chain!"
        )
        sys.exit(0)
    else:
        print(f"\nRace condition did NOT reproduce. State is: {action.state.name}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
