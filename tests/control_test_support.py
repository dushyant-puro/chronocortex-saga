"""SIMULATED effect evidence, created only by tests. Production starts empty."""
import asyncio
from types import SimpleNamespace

from runtime.action_journal import SQLiteActionJournal
from runtime.policy_engine import AuthorizationContext
from runtime.speculative_saga import SpeculativeSagaManager, TurnEpochClock
from runtime.task_engine import TaskManager, TaskStep
from runtime.tool_contract import ToolManifest, ToolRegistry
from runtime.workflow_contract import WorkflowVersion, StepVersions


class Clock:
    def __init__(self, now=1000):
        self.now = now

    def __call__(self):
        return self.now


def seed_control(path, clock=None):
    journal = SQLiteActionJournal(path, clock_ms=clock)
    lease = journal.acquire_lease("SIMULATED:worker", ttl_ms=3_600_000)
    grants = {"ccs:observe", "ccs:approve", "ccs:cancel", "ccs:reconcile", "ccs:repair", "test:write"}
    journal.security.provision_principal("operator", grants)
    journal.security.provision_principal("viewer", {"ccs:observe", "test:write"})
    journal.security.provision_principal("executor", {"test:write", "ccs:repair", "ccs:reconcile"})
    operator = journal.security.issue_credential("operator")
    viewer = journal.security.issue_credential("viewer")
    worker = journal.security.issue_credential("executor")
    registry = ToolRegistry()
    registry.register(ToolManifest("reserve_delivery", "write", required_permissions=["test:write"], requires_confirmation=True))
    registry.register(ToolManifest("private-change", "write", required_permissions=["private:write"]))
    context = AuthorizationContext(credential=worker.token)
    saga = SpeculativeSagaManager(TurnEpochClock(), registry=registry, journal=journal, auth_context=context)

    class Tasks(TaskManager):
        def _get_executor(self, step):
            async def execute(args):
                raise AssertionError("The test task must not dispatch from a console decision")
            return execute

    tasks = Tasks(saga, lambda _: None)
    step = TaskStep("reserve_delivery", required_fields=["destination"],
                    versions=StepVersions("reserve", "SIMULATED:args:v1", "SIMULATED:execute:v1", "SIMULATED:provider:v1", "SIMULATED:sandbox"))
    task = tasks.create_task("Outbound delivery · TRK-204", [step], workflow=WorkflowVersion("SIMULATED:delivery", "v1"))
    asyncio.run(tasks.run_task(task.task_id))
    pending = saga.stage_write("reserve_delivery", {"truck_id": "TRK-204", "destination": "Pune"},
                               owner_task_id=task.task_id, owner_step_index=0)
    unknown = saga.stage_write("reserve_delivery", {"truck_id": "TRK-871", "destination": "Bengaluru"})
    committed = saga.stage_write("reserve_delivery", {"truck_id": "TRK-139", "destination": "Surat"})

    async def uncertain(args):
        raise ConnectionError("SIMULATED provider response lost after possible apply")

    context.approval_id = journal.security.issue_approval(operator.token, "executor", unknown.action_id)
    try:
        asyncio.run(saga.commit_write(unknown.action_id, uncertain))
    except ConnectionError:
        pass
    context.approval_id = journal.security.issue_approval(operator.token, "executor", committed.action_id)
    asyncio.run(saga.commit_write(committed.action_id, lambda _: asyncio.sleep(0, result={"provider": "SIMULATED", "reservation_id": "sim-dock-139"})))
    approval = journal.security.issue_approval(operator.token, "executor", pending.action_id)
    foreign = saga.stage_write("private-change", {"private": "not visible to operator"})
    return SimpleNamespace(journal=journal, lease=lease, saga=saga, tasks=tasks, task=task, pending=pending,
                           unknown=unknown, committed=committed, foreign=foreign, approval=approval,
                           operator=operator, viewer=viewer, worker=worker, clock=clock)
