"""Chromium read-only admitted history; voice/tools and injected faults SIMULATED."""
import asyncio
import json
from pathlib import Path

import pytest
from playwright.sync_api import expect

from browser.console_cases import chromium, connect
from browser.proposal_cases import review_console, review, counts


def admitted(page):
    review(page)
    page.locator('#proposal-admit').click()
    expect(page.locator('#proposal-message')).to_contain_text('Task admitted')


def load(page):
    page.locator('#inbox-load').click()
    expect(page.locator('#inbox-message')).to_contain_text('Loaded admitted history')


def test_new_tab_discovers_receipt_without_replaying_or_clearing_old_uncertainty(review_console, chromium):
    env, old = review_console
    review(old)
    def lose(route):
        assert route.fetch().status == 200
        route.abort('failed')
    old.route('**/api/proposals/admit', lose)
    old.locator('#proposal-admit').click()
    expect(old.locator('#proposal-message')).to_contain_text('outcome is unconfirmed')
    task_id = old.locator('#proposal-task-id').input_value()
    async def forget(): env.service._pending.clear()
    asyncio.run_coroutine_threadsafe(forget(), env.loop).result(timeout=5)
    context = chromium.new_context(viewport={'width': 1440, 'height': 1100})
    try:
        page = context.new_page()
        attempts = []
        page.on('request', lambda r: attempts.append(r.url.rsplit('/', 1)[-1]) if '/api/proposals/' in r.url else None)
        page.goto(old.url)
        connect(env, page)
        assert page.evaluate('sessionStorage.length') == 0
        load(page)
        expect(page.locator('#inbox-items')).to_contain_text(task_id)
        expect(page.locator('#proposal-panel')).to_be_hidden()
        page.locator('#inbox-items button').click()
        expect(page.locator('#inbox-message')).to_contain_text(f'Journal confirms admission for {task_id}')
        assert attempts == ['inbox', 'receipt']
        assert page.evaluate('sessionStorage.length') == 0
        Path('results/inbox-browser').mkdir(parents=True, exist_ok=True)
        page.screenshot(path='results/inbox-browser/desktop.png', full_page=True)
        page.set_viewport_size({'width': 390, 'height': 844})
        assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
        page.screenshot(path='results/inbox-browser/mobile.png', full_page=True)
    finally:
        context.close()
    expect(old.locator('#proposal-finish')).to_be_disabled()
    expect(old.locator('#proposal-task-id')).to_have_value(task_id)
    assert counts(env) == dict(task_checkpoints=1, task_events=1, actions=0, approvals=0)


@pytest.mark.parametrize('operation', ['inbox', 'receipt'])
def test_delayed_inbox_or_receipt_cannot_populate_another_principal(review_console, operation):
    env, page = review_console
    admitted(page)
    if operation == 'receipt': load(page)
    held = []
    pattern = f'**/api/proposals/{operation}'
    page.route(pattern, lambda route: held.append((route, route.fetch())))
    with page.expect_request(pattern):
        page.locator('#inbox-load' if operation == 'inbox' else '#inbox-items button').click()
    page.get_by_role('button', name='Disconnect', exact=True).click()
    connect(env, page, env.viewer)
    assert len(held) == 1
    with page.expect_response(pattern): held[0][0].fulfill(response=held[0][1])
    page.evaluate('() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve)))')
    expect(page.locator('#inbox-items')).to_be_empty()
    expect(page.locator('#inbox-message')).not_to_contain_text('Journal confirms')
    page.unroute(pattern)
    page.locator('#inbox-load').click()
    expect(page.locator('#inbox-message')).to_contain_text('No visible admissions')


@pytest.mark.parametrize('damage', ['network', 'scope', 'cursor', 'duplicate', 'authority'])
def test_failed_or_malformed_inbox_does_not_leave_actionable_old_rows(review_console, damage):
    env, page = review_console
    admitted(page)
    load(page)
    def fail(route):
        if damage == 'network': return route.abort('failed')
        value = route.fetch().json()
        if damage == 'scope': value['scope'] = 'PENDING'
        elif damage == 'cursor': value['next_before'] = '9223372036854775808'
        elif damage == 'duplicate': value['items'] += value['items']
        else: value['items'][0]['execution_authorized'] = True
        route.fulfill(status=200, content_type='application/json', body=json.dumps(value))
    page.route('**/api/proposals/inbox', fail)
    page.locator('#inbox-load').click()
    expect(page.locator('#inbox-message')).to_have_attribute('role', 'alert')
    expect(page.locator('#inbox-items')).to_be_empty()
    expect(page.locator('#inbox-older')).to_be_disabled()
    assert counts(env)['task_events'] == 1


def test_empty_filtered_page_requires_explicit_older_read_and_renders_names_as_text(review_console):
    _, page = review_console
    admitted(page)
    payloads = []
    items = []
    def pages(route):
        body = route.request.post_data_json
        payloads.append(body)
        if body['before'] is None:
            value = route.fetch().json()
            items[:] = value['items']
            items[0]['name'] = '<img src=x onerror="window.__inboxXss=true">'
            value['items'], value['next_before'] = [], '1'
        else:
            value = dict(items=items, next_before=None, scan_limit=200, scope='ADMITTED_HISTORY_ONLY', execution_authorized=False)
        route.fulfill(status=200, content_type='application/json', body=json.dumps(value))
    page.route('**/api/proposals/inbox', pages)
    page.locator('#inbox-load').click()
    expect(page.locator('#inbox-message')).to_contain_text('Older records may contain matches')
    assert payloads == [{'before': None}]
    page.locator('#inbox-older').click()
    expect(page.locator('#inbox-items')).to_contain_text('<img')
    assert payloads == [{'before': None}, {'before': '1'}]
    assert page.locator('#inbox-items img').count() == 0 and page.evaluate('window.__inboxXss') is None
    expect(page.locator('#inbox-older')).to_be_disabled()


def test_receipt_identity_mismatch_does_not_confirm_history_row(review_console):
    _, page = review_console
    admitted(page)
    load(page)
    def mismatch(route):
        value = route.fetch().json()
        value['review_id'] = 'SIMULATED-other-review'
        route.fulfill(status=200, content_type='application/json', body=json.dumps(value))
    page.route('**/api/proposals/receipt', mismatch)
    page.locator('#inbox-items button').click()
    expect(page.locator('#inbox-message')).to_contain_text('receipt is unavailable')
    expect(page.locator('#inbox-items')).to_be_empty()
