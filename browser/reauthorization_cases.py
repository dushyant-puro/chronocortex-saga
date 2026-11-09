"""Explicit fresh correction review; real UI/SQLite, SIMULATED voice and faults."""
import asyncio
import json
from pathlib import Path

from playwright.sync_api import expect
from browser.console_cases import chromium, connect
from browser.proposal_cases import review_console, counts
from browser.delivery_cases import retained_console, load


def inspect(page):
    load(page)
    page.locator('#delivery-items button').click()
    expect(page.locator('#delivery-transcript')).to_have_text('SIMULATED move cargo')


def test_current_correction_keeps_task_identity_and_requires_new_acceptance(retained_console):
    env,page=retained_console
    task_id=page.locator('#proposal-task-id').input_value()
    async def correct():env.revise('SIMULATED corrected cargo')
    asyncio.run_coroutine_threadsafe(correct(),env.loop).result(timeout=5)
    inspect(page)
    with page.expect_request('**/api/proposals/reauthorize') as request:
        page.locator('#delivery-reauthorize').click()
    assert set(request.value.post_data_json)=={'review_id','binding_digest'}
    expect(page.locator('#proposal-transcript')).to_have_text('SIMULATED corrected cargo')
    expect(page.locator('#proposal-task-id')).to_have_value(task_id)
    expect(page.locator('#proposal-confirmed')).not_to_be_checked()
    expect(page.locator('#proposal-admit')).to_be_disabled()
    expect(page.locator('#delivery-reauthorize')).to_be_disabled()
    assert counts(env)==dict(task_checkpoints=0,task_events=0,actions=0,approvals=0)
    Path('results/reauthorization-browser').mkdir(parents=True,exist_ok=True)
    page.evaluate('scrollTo(0,0)')
    page.screenshot(path='results/reauthorization-browser/desktop.png',full_page=True)
    page.set_viewport_size({'width':390,'height':844})
    assert page.evaluate('document.documentElement.scrollWidth<=innerWidth')
    page.evaluate('scrollTo(0,0)')
    page.screenshot(path='results/reauthorization-browser/mobile.png',full_page=True)
    page.locator('#proposal-confirmed').check();page.locator('#proposal-admit').click()
    expect(page.locator('#proposal-message')).to_contain_text('Task admitted')
    assert counts(env)==dict(task_checkpoints=1,task_events=1,actions=0,approvals=0)


def test_lost_reauthorization_response_never_admits_or_automatically_retries(retained_console):
    env,page=retained_console;inspect(page);attempts=[]
    def lose(route):
        attempts.append(1);assert route.fetch().status==200;route.abort('failed')
    page.route('**/api/proposals/reauthorize',lose)
    page.locator('#delivery-reauthorize').click()
    expect(page.locator('#proposal-message')).to_contain_text('Reauthorization outcome is unconfirmed')
    expect(page.locator('#proposal-admit')).to_be_disabled()
    assert attempts==[1] and counts(env)['task_checkpoints']==0


def test_uncertain_admission_cannot_be_cleared_by_preparing_a_correction(retained_console):
    env,page=retained_console
    page.route('**/api/proposals/admit',lambda route:route.abort('failed'))
    page.locator('#proposal-admit').click()
    expect(page.locator('#proposal-message')).to_contain_text('outcome is unconfirmed')
    inspect(page)
    expect(page.locator('#delivery-reauthorize')).to_be_disabled()
    expect(page.locator('#proposal-finish')).to_be_disabled()
    assert counts(env)['task_checkpoints']==0


def test_wrong_task_in_successor_response_is_not_presented_for_admission(retained_console):
    env,page=retained_console;inspect(page)
    def mismatch(route):
        value=route.fetch().json();value['binding']['task_id']='task-SIMULATED-wrong'
        route.fulfill(status=200,content_type='application/json',body=json.dumps(value))
    page.route('**/api/proposals/reauthorize',mismatch)
    page.locator('#delivery-reauthorize').click()
    expect(page.locator('#proposal-message')).to_contain_text('Reauthorization outcome is unconfirmed')
    expect(page.locator('#proposal-admit')).to_be_disabled()
    expect(page.locator('#proposal-transcript')).to_be_empty()
    assert counts(env)['task_checkpoints']==0


def test_delayed_successor_response_never_populates_another_principal(retained_console):
    env,page=retained_console;inspect(page);held=[];pattern='**/api/proposals/reauthorize'
    page.route(pattern,lambda route:held.append((route,route.fetch())))
    with page.expect_request(pattern):page.locator('#delivery-reauthorize').click()
    page.get_by_role('button',name='Disconnect',exact=True).click();connect(env,page,env.viewer)
    assert len(held)==1
    with page.expect_response(pattern):held[0][0].fulfill(response=held[0][1])
    page.evaluate('() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve)))')
    expect(page.locator('#proposal-panel')).to_be_hidden()
    expect(page.locator('#proposal-transcript')).to_be_empty()


def test_admitted_history_has_no_correction_preparation_action(retained_console):
    env,page=retained_console;inspect(page)
    page.locator('#proposal-admit').click()
    expect(page.locator('#proposal-message')).to_contain_text('Task admitted')
    # Refresh this already selected detail directly: the retained list now omits it.
    page.locator('#delivery-items button').click()
    expect(page.locator('#delivery-message')).to_contain_text('admission evidence')
    expect(page.locator('#delivery-reauthorize')).to_be_hidden()
    assert counts(env)['task_checkpoints']==1
