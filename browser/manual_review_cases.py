"""User entry and manual host integration through real Chromium; SIMULATED."""
from pathlib import Path
from types import SimpleNamespace
import socket
import threading
import time

import pytest
import uvicorn
from playwright.sync_api import expect

from acceptance.review import create_app
from acceptance.local import WORKER, stopped_cleanly
from browser.console_cases import chromium, connect, inspect, confirm, acceptance_pending
from runtime.credential_input import read_credential_file
from runtime.security_authority import LocalCredential
from tests.test_local_acceptance import effects


@pytest.fixture
def manual_host(tmp_path, chromium):
    sock = socket.socket()
    sock.bind(('127.0.0.1', 0))
    origin = f'http://127.0.0.1:{sock.getsockname()[1]}'
    app = create_app(tmp_path, origin)
    server = uvicorn.Server(uvicorn.Config(app, log_level='critical', access_log=False, proxy_headers=False))
    thread = threading.Thread(target=server.run, kwargs={'sockets': [sock]}, daemon=True)
    thread.start()
    context = None
    try:
        deadline = time.monotonic() + 10
        while not server.started:
            assert thread.is_alive() and time.monotonic() < deadline
            time.sleep(.01)
        context = chromium.new_context(viewport={'width': 1440, 'height': 1000}, reduced_motion='reduce')
        page = context.new_page()
        errors = []
        page.on('pageerror', lambda error: errors.append(type(error).__name__))
        page.goto(origin)
        secret = read_credential_file(tmp_path / 'operator.secret')
        env = SimpleNamespace(operator=LocalCredential(secret.split('.', 1)[0], secret))
        yield app, env, page
        assert not errors
    finally:
        if context:
            context.close()
        server.should_exit = True
        thread.join(timeout=10)
        sock.close()
    assert not thread.is_alive() and stopped_cleanly(app)


@pytest.mark.parametrize('mobile', [False, True])
def test_file_entry_explains_startup_and_cannot_submit_credentials(chromium, mobile):
    context = chromium.new_context(viewport={'width': 390 if mobile else 1440, 'height': 900})
    try:
        page = context.new_page()
        requests = []
        page.on('request', lambda request: requests.append(request))
        page.goto((Path(__file__).resolve().parents[1] / 'control/static/index.html').as_uri())
        expect(page.get_by_role('heading', name='Start the console server')).to_be_visible()
        expect(page.locator('#credential')).to_be_disabled()
        expect(page.locator('#connect')).to_be_hidden()
        assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
        page.locator('#connect-form').evaluate('form => form.requestSubmit()')
        assert not [r for r in requests if r.method == 'POST' or '/api/' in r.url]
        assert page.locator('#file-entry pre').inner_text() == '.\\scripts\\start_review_walkthrough.ps1'
    finally:
        context.close()


def test_manual_review_admission_and_two_separate_browser_approvals(manual_host):
    app, env, page = manual_host
    expect(page.locator('#file-entry')).to_be_hidden()
    expect(page.get_by_role('note', name='Simulation mode')).to_be_visible()
    # Actual CSS and module controls load through the protected HTTP host.
    assert page.locator('#connect').evaluate('el => getComputedStyle(el).display') == 'grid'
    connect(env, page)
    page.get_by_role('button', name='Review current proposal', exact=True).click()
    expect(page.locator('#proposal-transcript')).to_have_text('SIMULATED cargo')
    expect(page.locator('#proposal-admit')).to_be_disabled()
    assert effects(app) == [] and not hasattr(app.state, 'acceptance_task_id')
    artifacts = Path('results/console-visual')
    artifacts.mkdir(parents=True, exist_ok=True)
    page.screenshot(path=str(artifacts / 'manual-review-desktop.png'), full_page=True)
    page.get_by_label('I reviewed this transcript and these exact arguments.', exact=True).check()
    page.locator('#proposal-admit').click()
    expect(page.locator('#proposal-message')).to_contain_text('Task admitted')
    page.locator('#proposal-finish').click()
    deadline = time.monotonic() + 8
    while not hasattr(app.state, 'acceptance_task_id'):
        assert time.monotonic() < deadline
        time.sleep(.01)
    first = acceptance_pending(app)
    for i in range(2):
        action = first if i == 0 else acceptance_pending(app, first)
        assert effects(app) == ([] if i == 0 else [(1,)])
        inspect(page, 'actions', action)
        page.get_by_role('button', name='Approve forward operation', exact=True).click()
        page.get_by_label('Execution principal').fill(WORKER)
        confirm(page)
    deadline = time.monotonic() + 8
    while not app.state.acceptance_operation.done():
        assert time.monotonic() < deadline
        time.sleep(.01)
    assert app.state.acceptance_operation.result() == 'COMPLETED'
    page.get_by_role('button', name='Refresh', exact=True).click()
    inspect(page, 'tasks', app.state.acceptance_task_id)
    expect(page.locator('#detail')).to_contain_text('COMPLETED')
    assert effects(app) == [(1,), (2,)]
    page.screenshot(path=str(artifacts / 'manual-review-completed.png'), full_page=True)
