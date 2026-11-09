"""Manual human review -> admission -> two approvals; all effects SIMULATED.

One task is selected per fresh run. Other admitted tasks remain parked: this
fixture is not a general-purpose provider worker or a restart/retry facility.
"""
import asyncio
from contextlib import asynccontextmanager, closing
import json
import sqlite3

from starlette.applications import Starlette
from starlette.responses import Response
from starlette.routing import Mount, Route

from acceptance import local
from control.app import ControlConfig, STATIC, create_app as console_app
from control.proposal_reviews import ProposalReviewController
from runtime.action_journal import SQLiteActionJournal
from runtime.credential_input import read_credential_file
from runtime.grounding_guard import TokenEvidence
from runtime.proposal_admission import AdmissionTemplate, ProposalAdmissionService
from runtime.speculative_saga import SpeculativeSagaManager, TurnEpochClock
from runtime.task_engine import TaskStep
from runtime.tool_contract import ToolManifest, ToolRegistry
from runtime.voice_ingress import VoiceIngress
from runtime.worker_supervisor import WorkerSupervisor
from runtime.workflow_contract import StepVersions

BANNER = ('<aside class="notice" role="note" aria-label="Simulation mode">'
          'SIMULATED MANUAL REVIEW · Review and admit one task, then approve each step separately. '
          'One task executes per fresh run; additional admissions stay parked. '
          'Synthetic transcript retention enabled · No real payments or live voice</aside>')


def source(manager):
    """Evidence and arguments are constructed only by this trusted fixture host."""
    voice = VoiceIngress()
    stream = voice.open_stream()
    turn = voice.begin_turn(stream)
    text = "SIMULATED cargo"
    words = tuple(TokenEvidence(w, .95, i * .5, i * .5 + .4) for i, w in enumerate(text.split()))
    voice.replace(turn, 1, text, words, final=True)
    voice.complete(turn, text)
    steps = [TaskStep(tool, versions=StepVersions(f"step-{i}", "SIMULATED:static-json:v1",
             "SIMULATED:sqlite-executor:v1", "SIMULATED:fixture-ledger:v1", "SIMULATED:local-only"))
             for i, tool in enumerate(local.TOOLS, 1)]
    template = AdmissionTemplate("SIMULATED local acceptance: two approvals", local.WORKFLOW, tuple(steps))
    service = ProposalAdmissionService(manager, voice, [template],
        source_id="SIMULATED:manual-review", retain_reviews=True)
    return ProposalReviewController(service, local.WORKFLOW.workflow_id,
                                    [json.loads(v) for v in local.ARGUMENTS]), steps


async def select_and_drive(app, host, manager, steps, credential, stopped):
    # Select once, from durable admission evidence. Refresh/login never selects
    # another task or retries a mutation, even after cancellation or uncertainty.
    while not stopped.is_set() and not host.stopping.is_set():
        def first():
            with closing(sqlite3.connect(host.journal.path)) as db:
                return db.execute("SELECT task_id FROM task_checkpoints ORDER BY rowid LIMIT 1").fetchone()
        row = await asyncio.to_thread(first)
        if row:
            app.state.acceptance_task_id = row[0]
            task = manager.restore_task(row[0], steps=steps, workflow=local.WORKFLOW)
            return await local.drive(host, manager, task, credential, stopped)
        try:
            await asyncio.wait_for(stopped.wait(), timeout=.25)
        except TimeoutError:
            pass


def create_app(directory, origin="http://127.0.0.1:8877"):
    config = ControlConfig(origin)
    files = local.prepare(directory, interactive_review=True)
    inner = None

    async def delegate(scope, receive, send):
        # The server completes lifespan startup before serving requests. Fail
        # closed if an ASGI caller bypasses that contract.
        if inner is None:
            await Response(status_code=503)(scope, receive, send)
            return
        await inner(scope, receive, send)

    @asynccontextmanager
    async def lifespan(app):
        nonlocal inner
        credential = read_credential_file(files.root / "worker.secret")
        registry = ToolRegistry()
        for tool in local.TOOLS:
            registry.register(ToolManifest(tool, "write", idempotent=False, cancellable=False,
                requires_authoritative_commit=True, requires_confirmation=True,
                required_permissions=[local.PERMISSION], telemetry_category="SIMULATED"))
        host = WorkerSupervisor(SQLiteActionJournal(files.journal), credential)
        app.state.acceptance_host = host
        stopped = asyncio.Event()
        try:
            saga = await host.start(lambda journal, auth: SpeculativeSagaManager(
                TurnEpochClock(), registry=registry, journal=journal, auth_context=auth))
            manager = local.LocalTasks(saga, files.ledger)
            controller, steps = source(manager)
            inner = console_app(host.journal, config, proposal_reviews=controller)
            async def page(_):
                html = (STATIC / "index.html").read_text(encoding="utf-8")
                html = html.replace("<body>", "<body>" + BANNER, 1)
                html = html.replace("LOCAL CONTROL HOST · PROVIDER EXECUTION NOT CONFIGURED", "SIMULATED MANUAL REVIEW ONLY")
                return Response(html, media_type="text/html")
            inner.router.routes.insert(0, Route("/", page))
            app.state.acceptance_operation = host.submit(
                lambda _: select_and_drive(app, host, manager, steps, credential, stopped))
            print(json.dumps({"label": "SIMULATED", "event": "MANUAL_REVIEW_READY",
                              "executor": local.WORKER, "url": config.origin}), flush=True)
            yield
        finally:
            inner = None
            stopped.set()
            await asyncio.shield(host.stop())

    app = Starlette(routes=[Mount("/", app=delegate)], lifespan=lifespan)
    app.state.acceptance_files = files
    return app


def main():
    local.main(app_factory=create_app)


if __name__ == "__main__":
    main()
