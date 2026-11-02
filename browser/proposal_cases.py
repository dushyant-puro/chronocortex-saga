"""Protected human review in real Chromium; voice and tools are SIMULATED."""
import asyncio
from contextlib import asynccontextmanager, closing
from pathlib import Path
import json
import socket
import sqlite3
import threading
import time
from types import SimpleNamespace

import pytest
import uvicorn
from playwright.sync_api import expect

from browser.console_cases import chromium, connect
from control.app import ControlConfig, create_app
from control.proposal_reviews import ProposalReviewController
from runtime.action_journal import SQLiteActionJournal
from runtime.grounding_guard import TokenEvidence
from runtime.policy_engine import AuthorizationContext
from runtime.proposal_admission import AdmissionTemplate, ProposalAdmissionService
from runtime.speculative_saga import SpeculativeSagaManager, TurnEpochClock
from runtime.task_engine import TaskManager, TaskStep
from runtime.tool_contract import ToolManifest, ToolRegistry
from runtime.voice_ingress import VoiceIngress
from runtime.workflow_contract import StepVersions, WorkflowVersion


@pytest.fixture
def review_console(tmp_path, chromium):
    env = SimpleNamespace(ready=threading.Event(), errors=[])
    sock = socket.socket()
    sock.bind(('127.0.0.1', 0))
    origin = f'http://127.0.0.1:{sock.getsockname()[1]}'
    def run():
        try:
            journal = SQLiteActionJournal(tmp_path / 'review.db')
            journal.acquire_lease('SIMULATED-review-host', ttl_ms=3600000)
            for name, grants in [('reviewer', {'ccs:observe', 'ccs:task:create', 'review:write'}),
                                 ('viewer', {'ccs:observe', 'review:write'}), ('worker', {'review:write'})]:
                journal.security.provision_principal(name, grants)
            env.operator = journal.security.issue_credential('reviewer')
            env.viewer = journal.security.issue_credential('viewer')
            worker = journal.security.issue_credential('worker')
            registry = ToolRegistry()
            registry.register(ToolManifest('SIMULATED.move', 'write', required_permissions=['review:write'], requires_confirmation=True))
            saga = SpeculativeSagaManager(TurnEpochClock(), registry=registry, journal=journal,
                                          auth_context=AuthorizationContext(credential=worker.token))
            manager = TaskManager(saga, lambda _: None)
            voice = VoiceIngress()
            voice.open_stream()
            def revise(text='SIMULATED move cargo'):
                turn = voice.begin_turn(voice._stream)
                words = tuple(TokenEvidence(w, .99, i*.5, i*.5+.4) for i, w in enumerate(text.split()))
                voice.replace(turn, 1, text, words, final=True)
                return voice.complete(turn, text)
            revise()
            step = TaskStep('SIMULATED.move', versions=StepVersions('move', 'static:v1', 'SIMULATED:none:v1', 'SIMULATED:none:v1', 'SIMULATED:local'))
            template = AdmissionTemplate('SIMULATED reviewed cargo', WorkflowVersion('SIMULATED:review', 'v1'), (step,))
            service = ProposalAdmissionService(manager, voice, [template], source_id='SIMULATED:browser-review')
            arguments = [{'destination': "SIMULATED <img src=x onerror='window.__proposalXss=true'>", 'units': 1}]
            controller = ProposalReviewController(service, template.workflow.workflow_id, arguments)
            app = create_app(journal, ControlConfig(origin), proposal_reviews=controller)
            @asynccontextmanager
            async def lifespan(_):
                env.loop = asyncio.get_running_loop()
                yield
            app.router.lifespan_context = lifespan
            env.journal, env.revise = journal, revise
            env.server = uvicorn.Server(uvicorn.Config(app, log_level='critical', access_log=False, proxy_headers=False))
            env.ready.set()
            env.server.run(sockets=[sock])
        except Exception as error:
            env.errors.append(type(error).__name__)
            env.ready.set()
    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    context = None
    try:
        assert env.ready.wait(10) and not env.errors
        deadline = time.monotonic() + 10
        while not env.server.started:
            assert thread.is_alive() and time.monotonic() < deadline
            time.sleep(.01)
        context = chromium.new_context(viewport={'width': 1440, 'height': 1100}, reduced_motion='reduce')
        page = context.new_page()
        page.on('pageerror', lambda error: env.errors.append(type(error).__name__))
        page.goto(origin)
        connect(env, page)
        yield env, page
    finally:
        if context:
            context.close()
        if hasattr(env, 'server'):
            env.server.should_exit = True
        thread.join(timeout=10)
        sock.close()
        if not thread.is_alive() and hasattr(env, 'journal'):
            env.journal.release_lease()
    assert not thread.is_alive() and not env.errors


def review(page):
    page.get_by_role('button', name='Review current proposal', exact=True).click()
    expect(page.locator('#proposal-transcript')).to_have_text('SIMULATED move cargo')
    expect(page.locator('#proposal-admit')).to_be_disabled()
    page.get_by_label('I reviewed this transcript and these exact arguments.', exact=True).check()
    expect(page.locator('#proposal-admit')).to_be_enabled()


def counts(env):
    with closing(sqlite3.connect(env.journal.path)) as db:
        return {table: db.execute('SELECT count(*) FROM ' + table).fetchone()[0]
                for table in ('task_checkpoints', 'task_events', 'actions', 'approvals')}


def test_review_maps_exact_text_to_explicit_admission_without_dispatch(review_console):
    env, page = review_console
    review(page)
    assert counts(env) == dict(task_checkpoints=0, task_events=0, actions=0, approvals=0)
    expect(page.locator('#proposal-arguments')).to_contain_text('<img')
    assert page.locator('#proposal-panel img').count() == 0 and page.evaluate('window.__proposalXss') is None
    artifacts = Path('results/proposal-browser')
    artifacts.mkdir(parents=True, exist_ok=True)
    page.evaluate('scrollTo(0, 0)')
    page.screenshot(path=str(artifacts / 'review-desktop.png'), full_page=True)
    with page.expect_request('**/api/proposals/admit') as request:
        page.locator('#proposal-admit').click()
    assert set(request.value.post_data_json) == {'review_id', 'binding_digest'}
    expect(page.locator('#proposal-message')).to_contain_text('Task admitted')
    assert counts(env) == dict(task_checkpoints=1, task_events=1, actions=0, approvals=0)
    stored = page.evaluate('JSON.stringify(Object.entries(sessionStorage))')
    assert env.operator.token not in stored and 'destination' not in stored and 'SIMULATED move cargo' not in stored
    page.reload()
    expect(page.locator('#proposal-message')).to_contain_text('receipt identity')
    expect(page.locator('#proposal-admit')).to_be_disabled()
    page.locator('#proposal-status').click()
    expect(page.locator('#proposal-message')).to_contain_text('Journal confirms')
    page.locator('#proposal-finish').click()
    expect(page.locator('#proposal-panel')).to_be_hidden()
    assert page.evaluate('sessionStorage.length') == 0


@pytest.mark.parametrize('response_failure', ['network', 'invalid_json', 'wrong_identity'])
def test_uncertain_admission_survives_reload_and_reads_status_without_replay(review_console, response_failure):
    env, page = review_console
    review(page)
    attempts = []
    def lose(route):
        attempts.append(1)
        result = route.fetch()
        assert result.status == 200
        if response_failure == 'network':
            route.abort('failed')
        elif response_failure == 'wrong_identity':
            receipt = result.json()
            receipt['task_id'] = 'task-SIMULATED-wrong-identity'
            route.fulfill(status=200, content_type='application/json', body=json.dumps(receipt))
        else:
            route.fulfill(status=200, content_type='application/json', body='INVALID RECEIPT')
    page.route('**/api/proposals/admit', lose)
    page.locator('#proposal-admit').click()
    expect(page.locator('#proposal-message')).to_contain_text('outcome is unconfirmed')
    task_id = page.locator('#proposal-task-id').input_value()
    expect(page.locator('#proposal-start')).to_be_disabled()
    page.reload()
    expect(page.locator('#proposal-task-id')).to_have_value(task_id)
    page.locator('#proposal-status').click()
    expect(page.locator('#proposal-message')).to_contain_text('Journal confirms')
    assert attempts == [1] and counts(env) == dict(task_checkpoints=1, task_events=1, actions=0, approvals=0)


def test_unapplied_uncertain_request_does_not_enable_a_new_admission(review_console):
    env, page = review_console
    review(page)
    page.route('**/api/proposals/admit', lambda route: route.abort('failed'))
    page.locator('#proposal-admit').click()
    expect(page.locator('#proposal-message')).to_contain_text('outcome is unconfirmed')
    page.locator('#proposal-status').click()
    expect(page.locator('#proposal-message')).to_contain_text('remains unconfirmed')
    for selector in ('#proposal-start', '#proposal-admit', '#proposal-finish'):
        expect(page.locator(selector)).to_be_disabled()
    assert counts(env)['task_checkpoints'] == 0


def test_receipt_storage_failure_prevents_the_admission_request(review_console):
    env, page = review_console
    review(page)
    attempts = []
    page.on('request', lambda request: attempts.append(1) if request.url.endswith('/api/proposals/admit') else None)
    page.evaluate("() => { Storage.prototype.setItem = () => { throw new Error('SIMULATED unavailable'); }; }")
    page.locator('#proposal-admit').click()
    expect(page.locator('#proposal-message')).to_contain_text('No admission request was sent')
    assert attempts == [] and counts(env)['task_checkpoints'] == 0


def test_corrected_proposal_cannot_be_admitted_from_old_review(review_console):
    env, page = review_console
    review(page)
    async def correction(): env.revise('SIMULATED corrected cargo')
    asyncio.run_coroutine_threadsafe(correction(), env.loop).result(timeout=5)
    page.locator('#proposal-admit').click()
    expect(page.locator('#proposal-message')).to_contain_text('outcome is unconfirmed')
    assert counts(env)['task_checkpoints'] == 0


def test_old_review_response_cannot_leak_after_different_principal_connects(review_console):
    env, page = review_console
    held = []
    pattern = '**/api/proposals/review'
    page.route(pattern, lambda route: held.append((route, route.fetch())))
    with page.expect_request(pattern):
        page.locator('#proposal-start').click()
    expect(page.locator('#proposal-start')).to_be_disabled()
    page.get_by_role('button', name='Disconnect', exact=True).click()
    connect(env, page, env.viewer)
    assert len(held) == 1
    with page.expect_response(pattern):
        held[0][0].fulfill(response=held[0][1])
    page.evaluate('() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve)))')
    expect(page.locator('#proposal-panel')).to_be_hidden()
    expect(page.locator('#proposal-start')).to_be_hidden()
    expect(page.locator('#proposal-transcript')).to_be_empty()


def test_mobile_review_and_corrupt_receipt_fail_closed(review_console):
    _, page = review_console
    page.set_viewport_size({'width': 390, 'height': 844})
    review(page)
    assert page.evaluate('document.documentElement.scrollWidth <= window.innerWidth')
    Path('results/proposal-browser').mkdir(parents=True, exist_ok=True)
    page.evaluate('scrollTo(0, 0)')
    page.screenshot(path='results/proposal-browser/review-mobile.png', full_page=True)
    page.evaluate("sessionStorage.setItem('ccs:admission:reviewer', '{}')")
    page.reload()
    expect(page.locator('#proposal-message')).to_contain_text('unavailable or invalid')
    expect(page.locator('#proposal-start')).to_be_disabled()
    expect(page.locator('#proposal-finish')).to_be_disabled()
