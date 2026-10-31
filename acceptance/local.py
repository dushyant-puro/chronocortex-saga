"""Local two-approval walkthrough using real task/auth/control boundaries.

All effects go to a separate SQLite fixture ledger. No live voice, network
provider, credential injection, resume or automatic mutation retry is exposed.
"""
import argparse
import asyncio
from contextlib import asynccontextmanager, closing
from dataclasses import dataclass
import json
from pathlib import Path
import socket
import sqlite3

from starlette.responses import Response
from starlette.routing import Route
import uvicorn

from control.app import ControlConfig, STATIC, create_app as console_app
from control.provision import provision
from runtime.action_journal import SQLiteActionJournal
from runtime.credential_input import read_credential_file
from runtime.failure_diagnostics import classify_failure
from runtime.grounding_guard import TokenEvidence
from runtime.policy_engine import AuthorizationContext, ConfirmationRequiredError
from runtime.proposal_admission import AdmissionTemplate, ProposalAdmissionService
from runtime.speculative_saga import SpeculativeSagaManager, TurnEpochClock
from runtime.task_engine import TaskManager, TaskState, TaskStep
from runtime.tool_contract import ToolManifest, ToolRegistry
from runtime.voice_ingress import VoiceIngress
from runtime.worker_supervisor import WorkerSupervisor
from runtime.workflow_contract import StepVersions, WorkflowVersion, canonical
from scripts.journal_backup import private_directory


PERMISSION = "simulation:write"
WORKER = "SIMULATED-local-worker"
TOOLS = ("SIMULATED.reserve", "SIMULATED.confirm")
ARGUMENTS = tuple(canonical({"sample": "SIMULATED cargo", "step": i}) for i in (1, 2))
WORKFLOW = WorkflowVersion("SIMULATED:local-acceptance", "v1")
BANNER = '<aside class="notice" role="note" aria-label="Simulation mode">SIMULATED LOCAL ACCEPTANCE · Two separate approvals · No real payments or live voice</aside>'


@dataclass(frozen=True)
class Files:
    root: Path

    @property
    def journal(self):
        return self.root / "acceptance.db"

    @property
    def ledger(self):
        return self.root / "simulated-effects.db"


def prepare(directory):
    """New empty protected directory only; preserve partial/existing runs."""
    root = private_directory(directory)
    if any(root.iterdir()):
        raise ValueError("A new empty protected acceptance directory is required")
    files = Files(root)
    journal = SQLiteActionJournal(files.journal)
    provision(journal, WORKER, {"ccs:work", PERMISSION}, root / "worker.secret", console_access=False)
    provision(journal, "SIMULATED-local-operator", {PERMISSION, "ccs:approve", "ccs:cancel", "ccs:reconcile"},
              root / "operator.secret")
    with closing(sqlite3.connect(files.ledger, isolation_level=None)) as db:
        db.execute("PRAGMA synchronous=EXTRA")
        db.execute("CREATE TABLE effects(step INTEGER PRIMARY KEY, payload TEXT NOT NULL)")
    return files


class LocalTasks(TaskManager):
    def __init__(self, saga, ledger):
        self.ledger = ledger
        super().__init__(saga, resolve_field=lambda _: None)

    def _get_executor(self, step):
        number = TOOLS.index(step.tool_name) + 1
        async def execute(args):
            if canonical(args) != ARGUMENTS[number - 1]:
                raise ValueError("SIMULATED fixture arguments changed")
            with closing(sqlite3.connect(self.ledger, isolation_level=None)) as db:
                db.execute("PRAGMA synchronous=EXTRA")
                db.execute("BEGIN IMMEDIATE")
                db.execute("INSERT INTO effects VALUES (?,?)", (number, canonical(args)))
                db.commit()
            return {"label": "SIMULATED", "step": number, "receipt": f"SIMULATED-local-step-{number}"}
        return execute


def seed_task(manager):
    """Canned pre-admitted fixture, NOT an interactive human/voice review flow."""
    journal = manager._saga.journal
    journal.security.provision_principal("SIMULATED-fixture-seeder", {"ccs:task:create", PERMISSION})
    credential = journal.security.issue_credential("SIMULATED-fixture-seeder", ttl_ms=60000)
    voice = VoiceIngress()
    stream = voice.open_stream()
    turn = voice.begin_turn(stream)
    text = "SIMULATED cargo"
    words = tuple(TokenEvidence(w, .95, i * .5, i * .5 + .4) for i, w in enumerate(text.split()))
    voice.replace(turn, 1, text, words, final=True)
    proposal = voice.complete(turn, text)
    steps = [TaskStep(tool, versions=StepVersions(f"step-{i}", "SIMULATED:static-json:v1",
             "SIMULATED:sqlite-executor:v1", "SIMULATED:fixture-ledger:v1", "SIMULATED:local-only"))
             for i, tool in enumerate(TOOLS, 1)]
    template = AdmissionTemplate("SIMULATED local acceptance: two approvals", WORKFLOW, tuple(steps))
    admissions = ProposalAdmissionService(manager, voice, [template], source_id="SIMULATED:canned-evidence")
    try:
        review = admissions.review(proposal, WORKFLOW.workflow_id, [json.loads(v) for v in ARGUMENTS], token=credential.token)
        receipt = admissions.admit(review.review_id, token=credential.token)
    finally:
        journal.security.revoke_credential(credential.id)
    return manager.restore_task(receipt.task_id, steps=steps, workflow=WORKFLOW)


async def drive(host, manager, task, credential, stopped):
    """Resume only an existing PENDING action after explicit console approval."""
    try:
        while not stopped.is_set() and not host.stopping.is_set():
            # Console cancellation is authoritative; do not overwrite its newer
            # checkpoint with this process's cached projection.
            found = await asyncio.to_thread(host.journal.load_task, task.task_id)
            if found is None or found[1]["state"] == "CANCELLED":
                return
            try:
                await manager.run_task(task.task_id)
            except ConfirmationRequiredError:
                pass
            if task.state != TaskState.WAITING_FOR_AUTHORIZATION:
                return "COMPLETED" if task.state == TaskState.COMPLETED else "PARKED"
            action_id = task.step_action_ids[task.current_step_index]
            selected = None
            while not stopped.is_set() and not host.stopping.is_set():
                found = await asyncio.to_thread(host.journal.load_task, task.task_id)
                if found is None or found[1]["state"] == "CANCELLED":
                    return
                selected = await asyncio.to_thread(host.journal.security.find_forward_approval, credential, action_id)
                if selected is not None:
                    break
                try:
                    await asyncio.wait_for(stopped.wait(), timeout=.25)
                except TimeoutError:
                    pass
            if selected is None or stopped.is_set() or host.stopping.is_set():
                return
            manager._saga.auth_context = AuthorizationContext(credential=credential, approval_id=selected)
    except Exception as error:
        # Preserve all evidence, stop admission, and never turn failure into a
        # provider retry. Error output is a bounded category, never a secret.
        host.request_stop()
        print(json.dumps({"label": "SIMULATED", "event": "WALKTHROUGH_STOPPED_ON_ERROR",
                          "category": classify_failure(error)}), flush=True)
        return "STOPPED_ON_ERROR"


def create_app(directory, origin="http://127.0.0.1:8877"):
    config = ControlConfig(origin)
    files = prepare(directory)
    app = console_app(SQLiteActionJournal(files.journal), config)
    app.state.acceptance_files = files

    async def page(_):
        html = (STATIC / "index.html").read_text(encoding="utf-8")
        html = html.replace("<body>", "<body>" + BANNER, 1)
        html = html.replace("LOCAL CONTROL HOST · PROVIDER EXECUTION NOT CONFIGURED", "SIMULATED LOCAL ACCEPTANCE ONLY")
        return Response(html, media_type="text/html")
    app.router.routes.insert(0, Route("/", page))

    @asynccontextmanager
    async def lifespan(_):
        credential = read_credential_file(files.root / "worker.secret")
        registry = ToolRegistry()
        for tool in TOOLS:
            registry.register(ToolManifest(tool, "write", idempotent=False, cancellable=False,
                requires_authoritative_commit=True, requires_confirmation=True,
                required_permissions=[PERMISSION], telemetry_category="SIMULATED"))
        host = WorkerSupervisor(SQLiteActionJournal(files.journal), credential)
        app.state.acceptance_host = host
        stopped = asyncio.Event()
        try:
            saga = await host.start(lambda journal, auth: SpeculativeSagaManager(
                TurnEpochClock(), registry=registry, journal=journal, auth_context=auth))
            manager = LocalTasks(saga, files.ledger)
            task = seed_task(manager)
            app.state.acceptance_task_id = task.task_id
            app.state.acceptance_operation = host.submit(lambda _: drive(host, manager, task, credential, stopped))
            print(json.dumps({"label": "SIMULATED", "event": "LOCAL_ACCEPTANCE_READY",
                              "task_id": task.task_id, "executor": WORKER, "url": config.origin}), flush=True)
            yield
        finally:
            stopped.set()
            await asyncio.shield(host.stop())
    app.router.lifespan_context = lifespan
    return app


class SafeParser(argparse.ArgumentParser):
    def error(self, _):
        self.exit(2, "Invalid local acceptance arguments. Use --help; no provider/key/host overrides are supported.\n")


def stopped_cleanly(app):
    host = getattr(app.state, "acceptance_host", None)
    report = getattr(host, "report", None)
    operation = getattr(app.state, "acceptance_operation", None)
    if (report is None or not report.drained or not report.lease_released or report.failure_code is not None or
            operation is None or not operation.done()):
        return False
    try:
        return operation.result() not in {"STOPPED_ON_ERROR", "PARKED"}
    except BaseException:
        return False


def main():
    parser = SafeParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--directory", required=True, help="New empty protected directory outside Git")
    parser.add_argument("--port", type=int, default=8877)
    args = parser.parse_args()
    if not 1024 <= args.port <= 65535:
        parser.error("port")
    try:
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", args.port))
            app = create_app(args.directory, f"http://127.0.0.1:{args.port}")
            server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=args.port, proxy_headers=False,
                access_log=False, log_level="warning", limit_concurrency=32, timeout_keep_alive=5))
            try:
                server.run(sockets=[listener])
            except KeyboardInterrupt:
                # Uvicorn re-raises SIGINT after graceful shutdown. Accept it
                # only when actual owned drain/release evidence confirms it.
                if not stopped_cleanly(app):
                    parser.exit(1, "Acceptance interrupted before verified clean shutdown; preserve the run directory.\n")
                return
            if not server.started or not stopped_cleanly(app):
                raise RuntimeError("Acceptance startup/shutdown did not complete cleanly")
    except Exception:
        parser.exit(1, "Local acceptance could not start. Preserve the directory; use a new protected run and inspect port/environment setup.\n")


if __name__ == "__main__":
    main()
