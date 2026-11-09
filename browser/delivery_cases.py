"""Real Chromium/SQLite delivery, SIMULATED voice/tools and protocol faults."""
import asyncio
import json
from pathlib import Path

import pytest
from playwright.sync_api import expect

from browser.console_cases import chromium, connect
from browser.proposal_cases import review_console, review, counts


@pytest.fixture
def retained_console(review_console):
    env, page = review_console
    async def enable(): env.service.retain_reviews = True
    asyncio.run_coroutine_threadsafe(enable(), env.loop).result(timeout=5)
    review(page)
    return env, page


def load(page):
    page.locator('#delivery-load').click()
    expect(page.locator('#delivery-message')).to_contain_text('Retained unadmitted reviews')


def test_new_browser_context_reopens_exact_evidence_without_admission(retained_console, chromium):
    env, old = retained_console
    task = old.locator('#proposal-task-id').input_value()
    async def restart():
        env.service._pending.clear()
        env.revise('SIMULATED changed objective')
    asyncio.run_coroutine_threadsafe(restart(), env.loop).result(timeout=5)
    context = chromium.new_context(viewport={'width': 1440, 'height': 1100})
    try:
        page = context.new_page()
        requests = []
        page.on('request', lambda r: requests.append(r.url.rsplit('/',1)[-1]) if '/api/proposals/' in r.url else None)
        page.goto(old.url); connect(env, page)
        load(page)
        page.locator('#delivery-items button').click()
        expect(page.locator('#delivery-transcript')).to_have_text('SIMULATED move cargo')
        expect(page.locator('#delivery-identity')).to_contain_text(task)
        expect(page.locator('#delivery-message')).to_contain_text('Inspection only')
        expect(page.locator('#delivery-arguments')).to_contain_text('<img')
        assert page.locator('#review-deliveries img').count() == 0
        assert page.locator('#review-deliveries input').count() == 0
        assert page.locator('#review-deliveries').get_by_role('button', name='Admit this task').count() == 0
        assert requests == ['deliveries', 'deliveries'] and page.evaluate('sessionStorage.length') == 0
        Path('results/delivery-browser').mkdir(parents=True, exist_ok=True)
        page.screenshot(path='results/delivery-browser/desktop.png', full_page=True)
        page.set_viewport_size({'width': 390, 'height': 844})
        assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
        page.screenshot(path='results/delivery-browser/mobile.png', full_page=True)
    finally:
        context.close()
    assert counts(env) == dict(task_checkpoints=0, task_events=0, actions=0, approvals=0)


@pytest.mark.parametrize('detail', [False, True])
def test_delayed_retained_evidence_never_populates_a_different_principal(retained_console, detail):
    env, page = retained_console
    if detail: load(page)
    held = []; pattern = '**/api/proposals/deliveries'
    page.route(pattern, lambda route: held.append((route, route.fetch())))
    with page.expect_request(pattern): page.locator('#delivery-items button' if detail else '#delivery-load').click()
    page.get_by_role('button', name='Disconnect', exact=True).click()
    connect(env, page, env.viewer)
    assert len(held) == 1
    with page.expect_response(pattern): held[0][0].fulfill(response=held[0][1])
    page.evaluate('() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve)))')
    expect(page.locator('#delivery-transcript')).to_be_empty()
    expect(page.locator('#delivery-items')).to_be_empty()


def test_expired_detail_clears_display_without_admission_replay(retained_console):
    env, page = retained_console
    load(page)
    async def expire():
        observed = env.journal._now() + 60001
        env.journal._clock_ms = lambda: observed
    asyncio.run_coroutine_threadsafe(expire(), env.loop).result(timeout=5)
    page.locator('#delivery-items button').click()
    expect(page.locator('#delivery-message')).to_contain_text('unavailable, expired or changed')
    expect(page.locator('#delivery-transcript')).to_be_empty()
    expect(page.locator('#delivery-detail')).to_be_hidden()
    assert counts(env)['task_checkpoints'] == 0


@pytest.mark.parametrize('detail', [False, True])
def test_forged_delivery_authority_is_rejected_and_private_evidence_cleared(retained_console, detail):
    _, page = retained_console
    if detail: load(page)
    def forge(route):
        value = route.fetch().json(); value['admission_authorized'] = True
        route.fulfill(status=200, content_type='application/json', body=json.dumps(value))
    page.route('**/api/proposals/deliveries', forge)
    page.locator('#delivery-items button' if detail else '#delivery-load').click()
    expect(page.locator('#delivery-message')).to_have_attribute('role', 'alert')
    expect(page.locator('#delivery-transcript')).to_be_empty()
    expect(page.locator('#delivery-items')).to_be_empty()
