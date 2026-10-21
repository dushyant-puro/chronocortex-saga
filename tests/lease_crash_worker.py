"""Independent process and fsynced SIMULATED effect oracle for lease takeover."""
import asyncio
import json
import os
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from runtime.action_journal import JournalLeaseError, SQLiteActionJournal
from runtime.policy_engine import AuthorizationContext
from runtime.speculative_saga import SpeculativeSagaManager, TurnEpochClock
from runtime.tool_contract import ToolManifest, ToolRegistry


async def run():
    path, ledger, mode = sys.argv[1:]
    now = [1000]
    journal = SQLiteActionJournal(path, clock_ms=lambda: now[0])
    journal.acquire_lease("original-process", ttl_ms=10)
    registry = ToolRegistry()
    registry.register(ToolManifest("change", "write"))
    manager = SpeculativeSagaManager(TurnEpochClock(), registry=registry,
                                    auth_context=AuthorizationContext(), journal=journal)

    async def execute(args):
        with open(ledger, "w", encoding="utf-8") as stream:
            stream.write(json.dumps("SIMULATED forward") + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        if mode == "crash":
            os._exit(23)
        if sys.stdin.readline() != "return\n":
            raise AssertionError("release protocol failed")
        now[0] = 1010
        return {"id": "SIMULATED:applied"}

    action = manager.stage_write("change", {})
    try:
        await manager.commit_write(action.action_id, execute)
    except JournalLeaseError:
        print(json.dumps({"state": action.state.name, "receipt": action.result}), flush=True)
    else:
        raise AssertionError("expired worker published success")


if __name__ == "__main__":
    asyncio.run(run())
