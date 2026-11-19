"""Real console cancellation/replacement; providers/voice/faults are SIMULATED."""
import asyncio
from contextlib import closing
import json
from pathlib import Path
import sqlite3

import pytest
from playwright.sync_api import expect

from browser.console_cases import chromium,connect,inspect,confirm
from browser.proposal_cases import review_console,review,counts


@pytest.fixture
def replacement_console(review_console,request):
    env,page=review_console
    async def enable():
        env.service.retain_reviews=True
        env.journal.security.update_principal('reviewer',permissions={'ccs:observe','ccs:task:create','ccs:cancel','review:write'})
    asyncio.run_coroutine_threadsafe(enable(),env.loop).result(timeout=5)
    page.context.clear_cookies();page.reload();connect(env,page)
    review(page);env.original_id=page.locator('#proposal-task-id').input_value()
    page.locator('#proposal-admit').click();expect(page.locator('#proposal-message')).to_contain_text('Task admitted')
    if getattr(request,'param',False):
        async def stage():
            env.service.manager._saga.stage_write('SIMULATED.move',{'SIMULATED':'staged only'},owner_task_id=env.original_id,owner_step_index=0,owner_generation=0)
        asyncio.run_coroutine_threadsafe(stage(),env.loop).result(timeout=5)
    inspect(page,'tasks',env.original_id)
    page.get_by_role('button',name='Record cancellation intent',exact=True).click()
    with page.expect_response('**/api/commands/cancel/*') as result:confirm(page)
    assert result.value.status==200
    page.locator('#proposal-finish').click();expect(page.locator('#proposal-panel')).to_be_hidden()
    page.locator('#task-replacement summary').click()
    page.get_by_label('Original cancelled task identity',exact=True).fill(env.original_id)
    return env,page


def eligible(page):
    page.locator('#replacement-inspect').click()
    expect(page.locator('#replacement-message')).to_contain_text('Eligible checkpoint')
    expect(page.locator('#replacement-prepare')).to_be_enabled()


def prepare(page):
    eligible(page);page.locator('#replacement-prepare').click()
    expect(page.locator('#proposal-transcript')).to_have_text('SIMULATED move cargo')


def claim_count(env):
    with closing(sqlite3.connect(env.journal.path)) as db:return db.execute('SELECT count(*) FROM task_replacements').fetchone()[0]


def test_explicit_cancel_inspect_review_accept_creates_one_linked_task(replacement_console):
    env,page=replacement_console
    eligible(page)
    with page.expect_request('**/api/proposals/replacement') as request:page.locator('#replacement-prepare').click()
    assert set(request.value.post_data_json)=={'task_id','checkpoint_digest'}
    expect(page.locator('#proposal-transcript')).to_have_text('SIMULATED move cargo')
    assert page.locator('#proposal-task-id').input_value()!=env.original_id
    expect(page.locator('#proposal-metadata')).to_contain_text(env.original_id)
    expect(page.locator('#proposal-confirmed')).not_to_be_checked()
    expect(page.locator('#proposal-admit')).to_be_disabled()
    expect(page.locator('#replacement-prepare')).to_be_disabled()
    assert counts(env)['task_checkpoints']==1 and claim_count(env)==0
    Path('results/replacement-browser').mkdir(parents=True,exist_ok=True)
    page.evaluate('scrollTo(0,0)');page.screenshot(path='results/replacement-browser/desktop.png',full_page=True)
    page.set_viewport_size({'width':390,'height':844});page.evaluate('scrollTo(0,0)')
    assert page.evaluate('document.documentElement.scrollWidth<=innerWidth')
    page.screenshot(path='results/replacement-browser/mobile.png',full_page=True)
    page.locator('#proposal-confirmed').check();page.locator('#proposal-admit').click()
    expect(page.locator('#proposal-message')).to_contain_text('Task admitted')
    assert counts(env)==dict(task_checkpoints=2,task_events=3,actions=0,approvals=0) and claim_count(env)==1
    with closing(sqlite3.connect(env.journal.path)) as db:
        assert json.loads(db.execute('SELECT checkpoint FROM task_checkpoints WHERE task_id=?',(env.original_id,)).fetchone()[0])['state']=='CANCELLED'


def test_replacement_controls_follow_review_state_without_waiting_for_snapshot(replacement_console):
    env,page=replacement_console
    eligible(page)
    held=[]
    page.route('**/api/snapshot',lambda route:held.append(route))
    with page.expect_request('**/api/snapshot'):
        page.get_by_role('button',name='Refresh',exact=True).click()
    assert len(held)==1
    try:
        page.locator('#replacement-prepare').click()
        expect(page.locator('#proposal-transcript')).to_have_text('SIMULATED move cargo')
        expect(page.locator('#replacement-prepare')).to_be_disabled()
        page.locator('#proposal-finish').click()
        expect(page.locator('#replacement-prepare')).to_be_enabled()
        assert counts(env)['task_checkpoints']==1 and claim_count(env)==0
    finally:
        for route in held:route.abort('failed')
        page.unroute('**/api/snapshot')


@pytest.mark.parametrize('replacement_console',[True],indirect=True)
def test_recorded_pending_action_blocks_replacement_and_does_not_imply_rollback(replacement_console):
    env,page=replacement_console
    page.locator('#replacement-inspect').click()
    expect(page.locator('#replacement-message')).to_contain_text('Recorded activity blocks')
    expect(page.locator('#replacement-prepare')).to_be_disabled()
    assert counts(env)['actions']==1 and claim_count(env)==0


def test_lost_preparation_ack_does_not_admit_claim_or_retry(replacement_console):
    env,page=replacement_console;eligible(page);attempts=[]
    def lose(route):attempts.append(1);assert route.fetch().status==200;route.abort('failed')
    page.route('**/api/proposals/replacement',lose);page.locator('#replacement-prepare').click()
    expect(page.locator('#proposal-message')).to_contain_text('Replacement review outcome is unconfirmed')
    expect(page.locator('#proposal-admit')).to_be_disabled()
    assert attempts==[1] and counts(env)['task_checkpoints']==1 and claim_count(env)==0


def test_wrong_original_in_preparation_response_cannot_enable_admission(replacement_console):
    env,page=replacement_console;eligible(page)
    def wrong(route):
        v=route.fetch().json();v['binding']['replaces_task']['task_id']='task-wrong'
        route.fulfill(status=200,content_type='application/json',body=json.dumps(v))
    page.route('**/api/proposals/replacement',wrong);page.locator('#replacement-prepare').click()
    expect(page.locator('#proposal-message')).to_contain_text('outcome is unconfirmed')
    expect(page.locator('#proposal-transcript')).to_be_empty();expect(page.locator('#proposal-admit')).to_be_disabled()
    assert counts(env)['task_checkpoints']==1 and claim_count(env)==0


def test_original_change_after_review_blocks_admission_at_sql_boundary(replacement_console):
    env,page=replacement_console;prepare(page)
    async def change():
        v,cp=env.journal.load_task(env.original_id);cp['error']='SIMULATED original changed'
        env.journal.save_task(env.original_id,cp,v)
    asyncio.run_coroutine_threadsafe(change(),env.loop).result(timeout=5)
    page.locator('#proposal-confirmed').check();page.locator('#proposal-admit').click()
    expect(page.locator('#proposal-message')).to_contain_text('outcome is unconfirmed')
    assert counts(env)['task_checkpoints']==1 and claim_count(env)==0


def test_uncertain_admission_keeps_replacement_preparation_disabled(replacement_console):
    env,page=replacement_console;prepare(page)
    page.route('**/api/proposals/admit',lambda route:route.abort('failed'))
    page.locator('#proposal-confirmed').check();page.locator('#proposal-admit').click()
    expect(page.locator('#proposal-message')).to_contain_text('outcome is unconfirmed')
    expect(page.locator('#replacement-prepare')).to_be_disabled();expect(page.locator('#proposal-finish')).to_be_disabled()
    assert claim_count(env)==0


def test_lost_applied_admission_ack_recovers_exact_replacement_and_one_claim(replacement_console):
    env,page=replacement_console;prepare(page);attempts=[]
    replacement_id=page.locator('#proposal-task-id').input_value()
    def lose(route):attempts.append(1);assert route.fetch().status==200;route.abort('failed')
    page.route('**/api/proposals/admit',lose)
    page.locator('#proposal-confirmed').check();page.locator('#proposal-admit').click()
    expect(page.locator('#proposal-message')).to_contain_text('outcome is unconfirmed')
    page.reload();expect(page.locator('#proposal-task-id')).to_have_value(replacement_id)
    page.locator('#proposal-status').click();expect(page.locator('#proposal-message')).to_contain_text('Journal confirms')
    assert attempts==[1] and claim_count(env)==1
    assert counts(env)==dict(task_checkpoints=2,task_events=3,actions=0,approvals=0)


@pytest.mark.parametrize('operation',['replacement-status','replacement'])
def test_delayed_replacement_reply_cannot_populate_another_principal(replacement_console,operation):
    env,page=replacement_console
    if operation=='replacement':eligible(page)
    held=[];pattern='**/api/proposals/'+operation
    page.route(pattern,lambda route:held.append((route,route.fetch())))
    with page.expect_request(pattern):page.locator('#replacement-prepare' if operation=='replacement' else '#replacement-inspect').click()
    page.get_by_role('button',name='Disconnect',exact=True).click();connect(env,page,env.viewer)
    assert len(held)==1
    with page.expect_response(pattern):held[0][0].fulfill(response=held[0][1])
    page.evaluate('() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve)))')
    expect(page.locator('#replacement-origin')).to_have_value('');expect(page.locator('#proposal-panel')).to_be_hidden()
    assert claim_count(env)==0
