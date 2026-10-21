"""Subprocess-only fault fixture: os._exit skips Python cleanup and compensation."""
import asyncio
import json
import os
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from runtime.action_journal import SQLiteActionJournal
from runtime.policy_engine import AuthorizationContext
from runtime.speculative_saga import SpeculativeSagaManager, TurnEpochClock
from runtime.tool_contract import ToolManifest


async def run():
    path, ledger, window = sys.argv[1:]

    class CrashJournal(SQLiteActionJournal):
        def transition(self, record, version, kind):
            result = super().transition(record, version, kind)
            if (window, kind) in {("intent", "DISPATCH_INTENT"),
                                  ("committed", "FORWARD_COMMITTED"),
                                  ("compensation_intent", "COMPENSATION_INTENT"),
                                  ("compensated", "COMPENSATED")}:
                os._exit(23)
            return result

    def effect(name):
        # Independent SIMULATED provider ledger; not a saga-state oracle.
        with open(ledger, "a", encoding="utf-8") as stream:
            stream.write(json.dumps(name) + "\n")
            stream.flush()
            os.fsync(stream.fileno())

    async def execute(args):
        effect("forward")
        if window == "applied":
            os._exit(23)
        return {"id": "simulated-provider-operation"}

    async def undo(result):
        effect("undo")
        if window == "compensation_applied":
            os._exit(23)

    saga = SpeculativeSagaManager(TurnEpochClock(), auth_context=AuthorizationContext(),
                                  journal=CrashJournal(path))
    action = saga.stage_write("change", {}, compensate=undo, manifest=ToolManifest("change", "write"))
    if window == "staged":
        os._exit(23)
    await saga.commit_write(action.action_id, execute)
    await saga.compensate_action(action.action_id)
    raise AssertionError("Crash window was not reached")


if __name__ == "__main__":
    asyncio.run(run())
