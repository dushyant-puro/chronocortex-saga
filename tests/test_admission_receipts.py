"""Durable positive receipts across sessions; fixtures use SIMULATED voice/tools."""
import asyncio
import json

import httpx2 as httpx
import pytest

from control.app import ControlConfig, create_app
from runtime.action_journal import SQLiteActionJournal
from runtime.control_store import ControlStore, ControlError
from tests.test_session_proposal_review import browser, prepare, admit, ORIGIN
from tests.test_proposal_admission import env, counts, complete


def lookup(e, binding, store=None, session=None, csrf=None):
    return (store or e.store).admission_receipt(session or e.session, csrf or e.csrf,
                binding['task_id'], binding['review_id'], binding['digest'])


def test_reopened_journal_new_session_expired_corrected_review_never_replays(browser, monkeypatch):
    e = browser
    binding = prepare(e)['binding']
    admit(e, {'binding': binding})
    before = counts(e)
    checkpoint = e.journal.load_task(binding['task_id'])
    e.store.logout(e.session, e.csrf)
    e.clock.now += 60001
    complete(e.voice, 'SIMULATED corrected objective')
    e.service._pending.clear()
    reopened = SQLiteActionJournal(e.journal.path, clock_ms=lambda: e.clock.now)
    store = ControlStore(reopened)
    # Credential rotation is allowed for a read by the same principal. Historical
    # evidence does not restore an old session, credential or execution authority.
    credential = e.authority.issue_credential('browser', ttl_ms=60000)
    e.authority.revoke_credential(e.browser_credential.id)
    session, identity = store.login(credential.token)
    def forbidden(*args, **kwargs):
        pytest.fail('Receipt lookup must not admit or dispatch')
    monkeypatch.setattr(reopened, '_admit_task_authenticated', forbidden)
    monkeypatch.setattr(e.manager, 'restore_task', forbidden)
    receipt = lookup(e, binding, store, session, identity['csrf_token'])
    assert receipt == dict(task_id=binding['task_id'], review_id=binding['review_id'],
                          binding_digest=binding['digest'], state='ADMITTED', execution_authorized=False)
    assert lookup(e, binding, store, session, identity['csrf_token']) == receipt
    assert counts(e) == before and e.journal.load_task(binding['task_id']) == checkpoint and e.effects == []
    assert not any(value in json.dumps(receipt) for value in (e.proposal.text, 'destination', credential.token, session))


@pytest.mark.parametrize('change', ['task', 'review', 'digest', 'principal', 'create', 'domain', 'missing', 'legacy'])
def test_missing_mismatched_or_unscoped_receipts_are_indistinguishable(browser, change):
    e = browser
    binding = prepare(e)['binding']
    if change != 'missing':
        admit(e, {'binding': binding})
    before = counts(e)
    if change in ('task', 'review', 'digest'):
        binding = {**binding, {'task': 'task_id', 'review': 'review_id', 'digest': 'digest'}[change]:
                   '0' * 64 if change == 'digest' else 'SIMULATED-mismatch'}
    elif change in ('principal', 'create', 'domain'):
        principal = 'other-reader' if change == 'principal' else 'browser'
        grants = {'ccs:observe', 'ccs:task:create', 'test:write'}
        if change == 'create': grants.remove('ccs:task:create')
        if change == 'domain': grants.remove('test:write')
        if principal == 'browser': e.authority.update_principal(principal, permissions=grants)
        else: e.authority.provision_principal(principal, grants)
        credential = e.authority.issue_credential(principal, ttl_ms=60000)
        e.session, identity = e.store.login(credential.token)
        e.csrf = identity['csrf_token']
    elif change == 'legacy':
        with e.journal._transaction() as db:
            cp = json.loads(db.execute('SELECT checkpoint FROM task_checkpoints').fetchone()[0])
            del cp['proposal_binding']
            db.execute('UPDATE task_checkpoints SET checkpoint=?', (json.dumps(cp),))
    with pytest.raises(ControlError) as error:
        lookup(e, binding)
    assert (error.value.status, error.value.code) == (404, 'receipt_unavailable')
    assert counts(e) == before and e.effects == []


@pytest.mark.parametrize('damage', ['session', 'csrf', 'missing_csrf', 'revoked', 'expired', 'version'])
def test_current_authority_is_always_rechecked(browser, damage):
    e = browser
    binding = prepare(e)['binding']
    admit(e, {'binding': binding})
    if damage == 'session': e.session = 'invalid'
    elif damage == 'csrf': e.csrf = '0' * 64
    elif damage == 'missing_csrf':
        with pytest.raises(ControlError) as error:
            e.store.admission_receipt(e.session, None, binding['task_id'], binding['review_id'], binding['digest'])
        assert error.value.status == 403
        return
    elif damage == 'revoked': e.authority.revoke_credential(e.browser_credential.id)
    elif damage == 'expired': e.clock.now += 600001
    else: e.authority.update_principal('browser', permissions={'ccs:observe'})
    with pytest.raises(ControlError) as error:
        lookup(e, binding)
    assert error.value.status in (401, 403)
    assert counts(e) == dict(task_checkpoints=1, task_events=1, actions=0, approvals=0)


@pytest.mark.parametrize('damage', ['json', 'shape', 'digest', 'workflow', 'arguments'])
def test_corrupt_durable_provenance_cannot_confirm_admission(browser, damage):
    e = browser
    binding = prepare(e)['binding']
    admit(e, {'binding': binding})
    with e.journal._transaction() as db:
        cp = json.loads(db.execute('SELECT checkpoint FROM task_checkpoints').fetchone()[0])
        if damage == 'digest': cp['proposal_binding']['digest'] = '0' * 64
        elif damage == 'workflow': cp['proposal_binding']['workflow_digest'] = '0' * 64
        elif damage == 'arguments': cp['proposal_binding']['arguments'] = [{'changed': True}]
        payload = 'INVALID' if damage == 'json' else '[]' if damage == 'shape' else json.dumps(cp)
        db.execute('UPDATE task_checkpoints SET checkpoint=?', (payload,))
    with pytest.raises(ControlError) as error:
        lookup(e, binding)
    assert (error.value.status, error.value.code) == (503, 'invalid_evidence')
    assert e.proposal.text not in str(error.value) and e.effects == []


def test_default_host_receipt_http_security_and_exact_payload(browser):
    e = browser
    binding = prepare(e)['binding']
    admit(e, {'binding': binding})
    e.service._pending.clear()
    app = create_app(e.journal, ControlConfig(ORIGIN))  # No admission service/ingress.
    payload = dict(task_id=binding['task_id'], review_id=binding['review_id'], binding_digest=binding['digest'])
    async def scenario():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=ORIGIN) as client:
            headers = {'Origin': ORIGIN, 'X-CCS-CSRF': e.csrf}
            assert (await client.post('/api/proposals/receipt', json=payload, headers=headers)).status_code == 401
            client.cookies.set('ccs_operator', e.session)
            for changed, status in (({'Origin': 'https://evil.example'}, 403),
                                    ({'X-CCS-CSRF': ''}, 403), ({'Host': 'evil.example'}, 400)):
                assert (await client.post('/api/proposals/receipt', json=payload, headers={**headers, **changed})).status_code == status
            for bad in ({**payload, 'arguments': e.args}, {'review_id': binding['review_id']},
                        {**payload, 'task_id': []}, {**payload, 'binding_digest': 'G'*64}):
                response = await client.post('/api/proposals/receipt', json=bad, headers=headers)
                assert response.status_code == 400
            response = await client.post('/api/proposals/receipt', json=payload, headers=headers)
            assert response.status_code == 200 and response.json()['state'] == 'ADMITTED'
            assert response.headers['cache-control'] == 'no-store'
            assert not response.json()['execution_authorized']
            assert e.proposal.text not in response.text and 'arguments' not in response.json()
    asyncio.run(scenario())
    assert counts(e) == dict(task_checkpoints=1, task_events=1, actions=0, approvals=0)
