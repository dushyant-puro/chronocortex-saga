"""Authenticated recovery operations in Chromium; fixture effects SIMULATED."""
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
import socket
import threading
import time

import pytest
import uvicorn
from playwright.sync_api import expect

from browser.console_cases import chromium, connect
from operations.recovery import create_app, stopped_cleanly
from runtime.credential_input import read_credential_file
from runtime.security_authority import LocalCredential
from tests.test_recovery_host import RecoveryConfig, records, setup


@pytest.fixture
def recovery_console(tmp_path, chromium):
    # Chromium's synchronous API owns this thread's event loop.
    with ThreadPoolExecutor(1) as pool:
        seed = pool.submit(setup, tmp_path, seeded=True).result()
    sock = socket.socket()
    sock.bind(('127.0.0.1', 0))
    config = RecoveryConfig(seed.journal, seed.worker_credential_file, sock.getsockname()[1])
    before = records(config.journal)
    app = create_app(config)
    server = uvicorn.Server(uvicorn.Config(app, log_level='critical', access_log=False, proxy_headers=False))
    app.state.request_exit = lambda: setattr(server, 'should_exit', True)
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
        page.goto(config.origin)
        secret = read_credential_file(tmp_path / 'viewer.secret')
        env = SimpleNamespace(operator=LocalCredential(secret.split('.')[0], secret))
        yield app, env, page
        assert not errors
    finally:
        if context:
            context.close()
        server.should_exit = True
        thread.join(timeout=10)
        sock.close()
    assert not thread.is_alive() and stopped_cleanly(app)
    assert records(config.journal) == before


@pytest.mark.parametrize('mobile', [False, True])
def test_protected_recovery_check_is_an_observation_and_never_replays_work(recovery_console, mobile):
    app, env, page = recovery_console
    if mobile:
        page.set_viewport_size({'width': 390, 'height': 844})
    expect(page.get_by_role('note', name='Recovery mode')).to_be_visible()
    expect(page.locator('#host-status-panel')).to_be_hidden()
    connect(env, page)
    expect(page.locator('#host-status-panel')).to_be_visible()
    expect(page.locator('#proposal-start')).to_be_hidden()
    page.get_by_role('button', name='Check recovery host', exact=True).click()
    expect(page.locator('#host-status-message')).to_contain_text('passed the last check')
    expect(page.locator('#host-status-message')).to_contain_text('cannot authorize dispatch')
    assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
    artifacts = Path('results/console-visual')
    artifacts.mkdir(parents=True, exist_ok=True)
    page.screenshot(path=str(artifacts / ('recovery-mobile.png' if mobile else 'recovery-desktop.png')), full_page=True)


@pytest.mark.parametrize('failure', ['network', 'wrong_mode', 'unsafe_authority'])
def test_unavailable_or_invalid_host_observation_cannot_show_a_pass(recovery_console, failure):
    _, env, page = recovery_console
    connect(env, page)
    def fail(route):
        if failure == 'network':
            route.abort()
            return
        result = route.fetch().json()
        if failure == 'wrong_mode': result['mode'] = 'EXECUTE'
        else: result['dispatch_authorized'] = True
        route.fulfill(status=200, json=result)
    page.route('**/api/host-status', fail)
    page.get_by_role('button', name='Check recovery host', exact=True).click()
    expect(page.locator('#host-status-message')).to_contain_text('check unavailable')
    page.unroute('**/api/host-status')
    page.get_by_role('button', name='Check recovery host', exact=True).click()
    expect(page.locator('#host-status-message')).to_contain_text('passed the last check')


def test_late_readiness_response_cannot_repopulate_another_login(recovery_console):
    _, env, page = recovery_console
    connect(env, page)
    held = []
    page.route('**/api/host-status', lambda route: held.append(route))
    page.get_by_role('button', name='Check recovery host', exact=True).click()
    expect(page.locator('#host-status-message')).to_contain_text('Checking current local lifecycle')
    assert len(held) == 1
    page.get_by_role('button', name='Disconnect', exact=True).click()
    expect(page.locator('#connect')).to_be_visible()
    connect(env, page)
    expect(page.locator('#host-status-message')).to_contain_text('No current host check')
    held[0].fulfill(status=200, json={'schema': 1, 'mode': 'RECOVERY_ONLY', 'ready': True, 'state': 'READY',
        'provider': 'NOT_CONFIGURED', 'voice': 'NOT_CONFIGURED', 'dispatch_authorized': False,
        'restore_authorized': False, 'provider_reconciled': False, 'freshness_verified': False})
    page.unroute('**/api/host-status')
    expect(page.locator('#host-status-message')).to_contain_text('No current host check')
