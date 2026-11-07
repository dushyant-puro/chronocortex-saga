"""Bounded admitted history; real SQLite/auth, SIMULATED voice/tool fixtures."""
import asyncio
import json

import httpx2 as httpx
import pytest

from control.app import create_app, ControlConfig
from runtime.action_journal import SQLiteActionJournal
from runtime.control_store import ControlStore, ControlError
from runtime.proposal_contract import digest
from tests.test_session_proposal_review import browser, prepare, admit, ORIGIN
from tests.test_proposal_admission import env, counts


def seed(e):
    view = prepare(e)
    admit(e, view)
    return view['binding']


def inbox(e, before=None):
    return e.store.admission_inbox(e.session, e.csrf, before)


def test_pending_review_is_not_durable_delivery_and_read_never_admits(browser):
    e = browser
    prepare(e)
    before = counts(e)
    assert inbox(e) == dict(items=[], next_before=None, scan_limit=200,
                           scope='ADMITTED_HISTORY_ONLY', execution_authorized=False)
    assert counts(e) == before == dict(task_checkpoints=0, task_events=0, actions=0, approvals=0)


def test_reopened_store_discovers_exact_identity_without_tab_cache_or_pending_reviews(browser):
    e = browser
    binding = seed(e)
    before = counts(e)
    e.service._pending.clear()
    e.store.logout(e.session, e.csrf)
    e.store = ControlStore(SQLiteActionJournal(e.journal.path, clock_ms=lambda: e.clock.now))
    e.session, identity = e.store.login(e.browser_credential.token)
    e.csrf = identity['csrf_token']
    page = inbox(e)
    assert page['items'] == [dict(task_id=binding['task_id'], review_id=binding['review_id'],
                                binding_digest=binding['digest'], name=binding['task_name'],
                                state='ADMITTED', execution_authorized=False)]
    assert not any(value in json.dumps(page) for value in (e.proposal.text, 'destination', e.session, e.browser_credential.token))
    assert counts(e) == before and e.effects == []


def test_keyset_pages_are_bounded_and_new_insert_does_not_repeat_old_records(browser, monkeypatch):
    e = browser
    bindings = [seed(e) for _ in range(5)]
    monkeypatch.setattr(e.store, 'WINDOW', 2)
    first = inbox(e)
    assert [v['task_id'] for v in first['items']] == [b['task_id'] for b in bindings[-1:-3:-1]]
    assert first['scan_limit'] == 2 and first['next_before'] is not None
    newest = seed(e)
    second = inbox(e, first['next_before'])
    third = inbox(e, second['next_before'])
    all_ids = [v['task_id'] for page in (first, second, third) for v in page['items']]
    assert all_ids == [b['task_id'] for b in reversed(bindings)]
    assert len(set(all_ids)) == 5 and third['next_before'] is None
    assert inbox(e)['items'][0]['task_id'] == newest['task_id']


def test_filtered_empty_pages_preserve_scan_cursor(browser, monkeypatch):
    e = browser
    for _ in range(3): seed(e)
    e.authority.update_principal('browser', permissions={'ccs:observe'})
    e.session, identity = e.store.login(e.browser_credential.token)
    e.csrf = identity['csrf_token']
    monkeypatch.setattr(e.store, 'WINDOW', 2)
    first = inbox(e)
    assert first['items'] == [] and first['next_before'] is not None
    last = inbox(e, first['next_before'])
    assert last['items'] == [] and last['next_before'] is None


@pytest.mark.parametrize('change', ['principal', 'domain', 'create'])
def test_current_principal_and_permissions_filter_each_page(browser, change):
    e = browser
    seed(e)
    grants = {'ccs:observe', 'ccs:task:create', 'test:write'}
    principal = 'other-reader' if change == 'principal' else 'browser'
    if change == 'domain': grants.remove('test:write')
    if change == 'create': grants.remove('ccs:task:create')
    if change == 'principal': e.authority.provision_principal(principal, grants)
    else: e.authority.update_principal(principal, permissions=grants)
    credential = e.authority.issue_credential(principal)
    e.session, identity = e.store.login(credential.token)
    e.csrf = identity['csrf_token']
    assert inbox(e)['items'] == []


@pytest.mark.parametrize('damage', ['session', 'csrf', 'missing_csrf', 'revoked', 'expired', 'version'])
def test_each_inbox_read_requires_current_authority(browser, damage):
    e = browser
    seed(e)
    if damage == 'session': e.session = 'invalid'
    elif damage == 'csrf': e.csrf = '0'*64
    elif damage == 'missing_csrf': e.csrf = None
    elif damage == 'revoked': e.authority.revoke_credential(e.browser_credential.id)
    elif damage == 'expired': e.clock.now += 600001
    else: e.authority.update_principal('browser', permissions={'ccs:observe'})
    with pytest.raises(ControlError) as error:
        inbox(e)
    assert error.value.status in (401, 403)


@pytest.mark.parametrize('cursor', [True, 1, '', '0', '-1', '01', '1.0', '9223372036854775808', '9'*100, '1;DROP TABLE actions'])
def test_cursor_is_bounded_data_never_authority_or_sql(browser, cursor):
    e = browser
    seed(e)
    with pytest.raises(ControlError) as error:
        inbox(e, cursor)
    assert error.value.status == 400
    assert counts(e) == dict(task_checkpoints=1, task_events=1, actions=0, approvals=0)


@pytest.mark.parametrize('damage', ['json', 'shape', 'binding', 'row_identity'])
def test_corrupt_page_fails_closed_without_partial_or_private_content(browser, damage):
    e = browser
    seed(e)
    with e.journal._transaction() as db:
        cp = json.loads(db.execute('SELECT checkpoint FROM task_checkpoints').fetchone()[0])
        if damage == 'row_identity':
            cp['task_id'] = cp['proposal_binding']['task_id'] = 'task-SIMULATED-misbound'
            binding = cp['proposal_binding']
            binding['digest'] = digest({k: v for k, v in binding.items() if k != 'digest'})
        else:
            cp['proposal_binding']['digest'] = '0'*64
        payload = 'INVALID' if damage == 'json' else '[]' if damage == 'shape' else json.dumps(cp)
        db.execute('UPDATE task_checkpoints SET checkpoint=?', (payload,))
    with pytest.raises(ControlError) as error:
        inbox(e)
    assert (error.value.status, error.value.code) == (503, 'invalid_evidence')
    assert e.proposal.text not in str(error.value)


def test_legacy_task_is_not_fabricated_as_a_proposal_admission(browser):
    e = browser
    seed(e)
    with e.journal._transaction() as db:
        cp = json.loads(db.execute('SELECT checkpoint FROM task_checkpoints').fetchone()[0])
        del cp['proposal_binding']
        db.execute('UPDATE task_checkpoints SET checkpoint=?', (json.dumps(cp),))
    assert inbox(e)['items'] == []


def test_default_host_http_no_cache_csrf_origin_and_exact_body(browser):
    e = browser
    seed(e)
    app = create_app(e.journal, ControlConfig(ORIGIN))
    async def scenario():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=ORIGIN) as client:
            headers = {'Origin': ORIGIN, 'X-CCS-CSRF': e.csrf}
            assert (await client.post('/api/proposals/inbox', json={'before': None}, headers=headers)).status_code == 401
            client.cookies.set('ccs_operator', e.session)
            for altered, status in (({'Origin': 'https://evil.example'}, 403), ({'X-CCS-CSRF': ''}, 403), ({'Host': 'evil.example'}, 400)):
                assert (await client.post('/api/proposals/inbox', json={'before': None}, headers={**headers, **altered})).status_code == status
            for data in ({}, {'before': None, 'principal_id': 'injected'}, {'before': []}):
                assert (await client.post('/api/proposals/inbox', json=data, headers=headers)).status_code == 400
            result = await client.post('/api/proposals/inbox', json={'before': None}, headers=headers)
            assert result.status_code == 200 and result.headers['cache-control'] == 'no-store'
            assert len(result.json()['items']) == 1 and e.proposal.text not in result.text
    asyncio.run(scenario())
