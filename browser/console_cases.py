"""Real Chromium + loopback HTTP + SQLite. Provider effects are SIMULATED.

Run explicitly: python -m pytest browser/console_cases.py -q
The ordinary runtime suite does not require a browser installation.
"""
from contextlib import closing
from concurrent.futures import ThreadPoolExecutor
import asyncio
from pathlib import Path
import socket
import sqlite3
import threading
import time

import pytest
from playwright.sync_api import sync_playwright, expect
import uvicorn

from control.app import ControlConfig, create_app
from tests.control_test_support import seed_control
from runtime.action_journal import SQLiteActionJournal
from runtime.worker_supervisor import WorkerSupervisor
from worker.__main__ import recovery_runtime


@pytest.fixture(scope="module")
def chromium():
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        yield browser
        browser.close()


@pytest.fixture
def console(tmp_path, chromium):
    # Playwright's synchronous API owns an event loop on this thread. Seed
    # simulated async worker activity in a separate thread without nesting it.
    with ThreadPoolExecutor(1) as pool:
        env = pool.submit(seed_control, tmp_path / "browser.db").result()
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    origin = f"http://127.0.0.1:{sock.getsockname()[1]}"
    server = uvicorn.Server(uvicorn.Config(create_app(env.journal, ControlConfig(origin)),
                                          log_level="critical", access_log=False, proxy_headers=False))
    thread = threading.Thread(target=server.run, kwargs={"sockets": [sock]}, daemon=True)
    thread.start()
    deadline = time.monotonic() + 10
    while not server.started:
        if not thread.is_alive() or time.monotonic() >= deadline:
            raise RuntimeError("Loopback test server failed to start")
        time.sleep(.01)
    context = chromium.new_context(viewport={"width": 1440, "height": 1000}, reduced_motion="reduce")
    page = context.new_page()
    errors = []
    page.on("pageerror", lambda error: errors.append(str(error)))
    page.goto(origin)
    yield env, page, context, origin
    context.close()
    server.should_exit = True
    thread.join(timeout=5)
    sock.close()
    assert not thread.is_alive()
    assert not errors, errors


def connect(env, page, credential=None):
    page.get_by_label("Operator credential").fill((credential or env.operator).token)
    page.locator("#connect-button").click()
    expect(page.locator("#console")).to_be_visible()
    expect(page.locator("#sync-state")).to_have_text("Journal read current")


def inspect(page, kind, record_id):
    page.locator("#lookup-kind").select_option(kind)
    page.locator("#lookup-id").fill(record_id)
    page.locator("#lookup").get_by_role("button", name="Inspect").click()
    expect(page.locator("#detail")).to_be_visible()
    expect(page.locator("#detail > .record-id")).to_have_text(record_id)


def confirm(page):
    page.get_by_label("I reviewed this exact evidence", exact=False).check()
    page.get_by_role("button", name="Record decision", exact=True).click()
    expect(page.locator("#decision-dialog")).not_to_be_visible()


def test_login_journal_evidence_approval_review_and_logout(console):
    env, page, context, _ = console
    connect(env, page)
    assert page.locator("#credential").input_value() == ""
    storage = page.evaluate("JSON.stringify([Object.entries(localStorage), Object.entries(sessionStorage), document.cookie])")
    assert env.operator.token not in storage
    assert not context.storage_state()["origins"]
    assert all(cookie["httpOnly"] for cookie in context.cookies())
    expect(page.get_by_text("Provider not configured", exact=True)).to_be_visible()
    inspect(page, "actions", env.pending.action_id)
    page.get_by_role("button", name="Approve forward operation", exact=True).click()
    expect(page.locator("#decision-evidence")).to_contain_text('"destination": "Pune"')
    expect(page.locator("#decision-evidence")).to_contain_text("test:write")
    page.get_by_label("Execution principal").fill("executor")
    confirm(page)
    expect(page.locator("#notice")).to_contain_text("No executor was dispatched")
    assert env.journal.records()[0][1]["state"] == "PENDING"
    page.get_by_role("button", name="Disconnect", exact=True).click()
    expect(page.locator("#connect")).to_be_visible()
    assert not page.locator("#metrics").text_content()
    assert not page.locator("#detail").text_content()
    assert not page.locator("#principal").text_content()


def test_initial_session_lookup_finishes_before_login_can_be_submitted(console):
    env, _, context, origin = console
    page = context.new_page()
    pending = []
    logins = []
    page.on("request", lambda request: logins.append(request) if request.url.endswith("/api/login") else None)
    page.route("**/api/session", lambda route: pending.append(route))
    with page.expect_request("**/api/session"):
        page.goto(origin, wait_until="commit")
    expect(page.locator("#credential")).to_be_visible()
    expect(page.locator("#connect-button")).to_be_disabled()
    page.get_by_label("Operator credential").fill(env.operator.token)
    page.locator("#connect-form").evaluate("form => form.requestSubmit()")
    page.evaluate("() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve)))")
    assert not logins
    assert len(pending) == 1
    pending[0].fulfill(status=401, content_type="application/json", body='{"error":{"code":"session_required","message":"Connect with a valid operator credential."}}')
    page.unroute("**/api/session")
    expect(page.locator("#connect-button")).to_be_enabled()
    connect(env, page)
    assert len(logins) == 1


def test_native_form_fallback_never_places_credential_in_navigation_url(console):
    env, _, context, origin = console
    page = context.new_page()
    page.route("**/assets/console.js", lambda route: route.fulfill(status=503, content_type="text/javascript", body=""))
    page.goto(origin)
    page.get_by_label("Operator credential").fill(env.operator.token)
    with page.expect_request(lambda request: request.is_navigation_request()) as navigation:
        page.locator("#connect-form").evaluate("form => form.submit()")
    request = navigation.value
    assert request.method == "POST" and request.url == origin + "/api/login"
    assert env.operator.token not in request.url
    page.wait_for_load_state("load")
    # Browsers may omit Origin under no-referrer policy. Either the origin
    # boundary or the JSON-only boundary must reject this native submission.
    response = request.response()
    assert response.status in {403, 415}
    assert response.json()["error"]["code"] in {"origin_rejected", "json_required"}
    assert env.operator.token not in page.locator("body").text_content()


def test_cancellation_retains_effect_evidence_and_records_history(console):
    env, page, _, _ = console
    connect(env, page)
    inspect(page, "tasks", env.task.task_id)
    page.get_by_role("button", name="Record cancellation intent", exact=True).click()
    expect(page.locator("#decision-description")).to_contain_text("does not stop an in-flight request")
    confirm(page)
    expect(page.locator("#detail")).to_contain_text("CANCELLED")
    assert env.pending.chain_id in env.journal.closed_chains()
    assert env.journal.records()[0][1]["state"] == "PENDING"
    page.locator("#navigation").get_by_role("button", name="Decision history").click()
    expect(page.locator("#record-content")).to_contain_text("UNCHANGED")


def test_offline_freezes_decisions_and_reconnect_reads_evidence(console):
    env, page, context, _ = console
    connect(env, page)
    inspect(page, "actions", env.pending.action_id)
    button = page.get_by_role("button", name="Approve forward operation", exact=True)
    expect(button).to_be_enabled()
    context.set_offline(True)
    page.get_by_role("button", name="Refresh", exact=True).click()
    expect(page.locator("#stale-banner")).to_be_visible()
    expect(button).to_be_disabled()
    expect(page.locator("#detail")).to_contain_text("Pune")
    context.set_offline(False)
    page.get_by_role("button", name="Refresh", exact=True).click()
    expect(page.locator("#stale-banner")).not_to_be_visible()
    expect(button).to_be_enabled()


def test_stale_review_is_rejected_without_optimistic_cancellation(console):
    env, page, _, _ = console
    connect(env, page)
    inspect(page, "tasks", env.task.task_id)
    page.get_by_role("button", name="Record cancellation intent", exact=True).click()
    version, cp = env.journal.load_task(env.task.task_id)
    cp["error"] = "SIMULATED concurrent update"
    env.journal.save_task(env.task.task_id, cp, version)
    confirm(page)
    expect(page.locator("#notice")).to_contain_text("Evidence changed")
    assert env.journal.load_task(env.task.task_id)[1]["state"] != "CANCELLED"


def test_unavailable_post_response_is_never_automatically_retried(console):
    env, page, _, _ = console
    connect(env, page)
    inspect(page, "actions", env.unknown.action_id)
    attempts = []
    def lose_response(route):
        attempts.append(route.request.post_data)
        response = route.fetch()
        assert response.status == 202
        route.abort("failed")
    page.route("**/api/commands/reconcile/*", lose_response)
    page.get_by_role("button", name="Request reconciliation", exact=True).click()
    confirm(page)
    expect(page.locator("#notice")).to_contain_text("No automatic retry was sent")
    page.unroute("**/api/commands/reconcile/*", lose_response)
    page.get_by_role("button", name="Refresh", exact=True).click()
    page.locator("#navigation").get_by_role("button", name="Decision history").click()
    expect(page.locator("#record-content")).to_contain_text("REQUESTED")
    assert len(attempts) == 1
    assert env.journal.records()[1][1]["state"] == "IN_FLIGHT_UNKNOWN"


def test_untrusted_evidence_is_text_and_viewer_is_read_only(console):
    env, page, _, _ = console
    version, cp = env.journal.load_task(env.task.task_id)
    cp["name"] = '<img src=x onerror="window.__xss=true">'
    env.journal.save_task(env.task.task_id, cp, version)
    connect(env, page, env.viewer)
    expect(page.get_by_text(cp["name"], exact=True)).to_be_visible()
    assert page.evaluate("window.__xss") is None
    assert page.locator("#record-content img").count() == 0
    inspect(page, "tasks", env.task.task_id)
    expect(page.get_by_role("button", name="Record cancellation intent", exact=True)).to_be_disabled()


def test_mobile_and_keyboard_review_and_visual_artifacts(console):
    env, page, _, _ = console
    connect(env, page)
    artifacts = Path("results/console-visual")
    artifacts.mkdir(parents=True, exist_ok=True)
    page.screenshot(path=str(artifacts / "desktop.png"), full_page=True)
    inspect(page, "actions", env.unknown.action_id)
    page.screenshot(path=str(artifacts / "desktop-evidence.png"), full_page=True)
    page.set_viewport_size({"width": 390, "height": 844})
    page.screenshot(path=str(artifacts / "mobile.png"), full_page=True)
    overflow = page.evaluate("Array.from(document.querySelectorAll('body *')).filter(e => e.getBoundingClientRect().right > innerWidth + 1).map(e => ({tag:e.tagName, id:e.id,cls:e.className,right:e.getBoundingClientRect().right}))")
    assert page.evaluate("document.documentElement.scrollWidth <= innerWidth"), overflow
    button = page.get_by_role("button", name="Request reconciliation", exact=True)
    button.focus()
    page.keyboard.press("Enter")
    expect(page.locator("#decision-dialog")).to_be_visible()
    assert page.evaluate("document.querySelector('#decision-dialog').contains(document.activeElement)")
    page.keyboard.press("Escape")
    expect(page.locator("#decision-dialog")).not_to_be_visible()
    expect(button).to_be_focused()


def test_supervision_view_distinguishes_manual_lease_live_reports_and_stopped_host(console):
    env, page, _, _ = console
    connect(env, page)
    page.locator(".worker-inspector summary").click()
    expect(page.locator("#worker-details")).to_contain_text("NOT_OBSERVED")
    expect(page.locator("#worker-state")).to_have_text("LEASE CURRENT")
    expect(page.locator("#worker-health")).to_contain_text("NOT READY")
    env.journal.release_lease()
    env.journal.security.update_principal("executor", permissions={"ccs:work", "test:write", "ccs:repair", "ccs:reconcile"})
    ready, stop = threading.Event(), threading.Event()
    failures = []
    def run_host():
        async def run():
            host = WorkerSupervisor(SQLiteActionJournal(env.journal.path), env.worker.token)
            try:
                await host.start(recovery_runtime)
                ready.set()
                await asyncio.to_thread(stop.wait)
            finally:
                await host.stop()
        try:
            asyncio.run(run())
        except Exception as exc:
            failures.append(type(exc).__name__)
            ready.set()
    thread = threading.Thread(target=run_host, daemon=True)
    thread.start()
    try:
        assert ready.wait(10) and not failures
        page.get_by_role("button", name="Refresh", exact=True).click()
        expect(page.locator("#worker-health")).to_contain_text("READY REPORTED")
        expect(page.locator("#worker-details")).to_contain_text("FRESH")
        expect(page.locator(".worker-inspector")).to_contain_text("not proof of process liveness")
        artifacts = Path("results/console-visual")
        artifacts.mkdir(parents=True, exist_ok=True)
        page.screenshot(path=str(artifacts / "supervised-worker.png"), full_page=True)
    finally:
        stop.set()
        thread.join(timeout=10)
    assert not thread.is_alive() and not failures
    page.get_by_role("button", name="Refresh", exact=True).click()
    expect(page.locator("#worker-state")).to_have_text("LEASE EXPIRED")
    expect(page.locator("#worker-health")).to_contain_text("NOT READY")
    expect(page.locator("#worker-details")).to_contain_text("STOPPED")
