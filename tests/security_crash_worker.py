"""os._exit fault cuts after atomic approval consumption and simulated apply."""
import asyncio
import json
import os
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from runtime.action_journal import SQLiteActionJournal
from runtime.policy_engine import AuthorizationContext
from runtime.speculative_saga import SpeculativeSagaManager, TurnEpochClock
from runtime.tool_contract import ToolManifest, ToolRegistry


async def run():
    path, ledger, window = sys.argv[1:]

    class CrashJournal(SQLiteActionJournal):
        def transition(self, record, version, kind, **kwargs):
            result = super().transition(record, version, kind, **kwargs)
            if kind == "DISPATCH_INTENT" and window == "intent":
                os._exit(23)
            return result

    journal = CrashJournal(path)
    journal.acquire_lease("SIMULATED worker")
    journal.security.provision_principal("executor", set())
    journal.security.provision_principal("approver", {"ccs:approve"})
    worker = journal.security.issue_credential("executor")
    approver = journal.security.issue_credential("approver")
    context = AuthorizationContext(credential=worker.token)
    registry = ToolRegistry()
    registry.register(ToolManifest("change", "write", requires_confirmation=True))
    manager = SpeculativeSagaManager(TurnEpochClock(), journal=journal, registry=registry, auth_context=context)
    action = manager.stage_write("change", {})
    context.approval_id = journal.security.issue_approval(approver.token, "executor", action.action_id)

    async def execute(args):
        with open(ledger, "w", encoding="utf-8") as stream:
            stream.write(json.dumps("SIMULATED forward") + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        os._exit(23)

    await manager.commit_write(action.action_id, execute)
    raise AssertionError("crash window not reached")


if __name__ == "__main__":
    asyncio.run(run())
