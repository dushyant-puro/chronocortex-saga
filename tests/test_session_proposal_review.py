"""Protected HTTP admission; all voice/provider fixtures remain SIMULATED."""
import asyncio
import json

import httpx2 as httpx
import pytest

from control.app import ControlConfig, create_app
from control.proposal_reviews import ProposalReviewController
from runtime.control_store import ControlStore
from runtime.policy_engine import PermissionDeniedError, PolicyError
from runtime.proposal_contract import ProposalAdmissionError
from tests.test_proposal_admission import env, complete, counts


ORIGIN = 'http://127.0.0.1:8877'


@pytest.fixture
def browser(env):
    env.authority.provision_principal('browser', {'ccs:observe', 'ccs:task:create', 'test:write'})
    env.browser_credential = env.authority.issue_credential('browser', ttl_ms=600000)
    env.store = ControlStore(env.journal)
    env.session, identity = env.store.login(env.browser_credential.token)
    env.csrf = identity['csrf_token']
    env.controller = ProposalReviewController(env.service, env.template.workflow.workflow_id, env.args)
    env.app = create_app(env.journal, ControlConfig(ORIGIN), proposal_reviews=env.controller)
    return env


def prepare(e):
    return e.controller.review(e.session, e.csrf)


def admit(e, reviewed, **overrides):
    binding = reviewed['binding']
    return e.service.admit_session(binding['review_id'], overrides.get('digest', binding['digest']),
                                   session=overrides.get('session', e.session), csrf=overrides.get('csrf', e.csrf))


def test_review_displays_exact_protected_mapping_and_admission_never_dispatches(browser):
    e = browser
    reviewed = prepare(e)
    assert reviewed['transcript'] == e.proposal.text
    assert reviewed['binding']['arguments'] == e.args
    assert reviewed['current'] and not reviewed['expired'] and not reviewed['admitted']
    assert not reviewed['execution_authorized'] and counts(e)['task_checkpoints'] == 0
    receipt = admit(e, reviewed)
    assert counts(e) == {'task_checkpoints': 1, 'task_events': 1, 'actions': 0, 'approvals': 0}
    assert e.effects == [] and e.manager.get_task(receipt.task_id) is None
    assert admit(e, reviewed) == receipt
    assert counts(e)['task_events'] == 1
    view = e.service.view_session(reviewed['binding']['review_id'], session=e.session, csrf=e.csrf)
    assert view['admitted'] and not view['execution_authorized']
    assert e.browser_credential.token not in json.dumps(view) and e.session not in json.dumps(view)


def test_same_credential_other_session_cannot_read_or_admit_review(browser):
    e = browser
    reviewed = prepare(e)
    other, identity = e.store.login(e.browser_credential.token)
    with pytest.raises(ProposalAdmissionError):
        admit(e, reviewed, session=other, csrf=identity['csrf_token'])
    with pytest.raises(ProposalAdmissionError):
        e.service.view_session(reviewed['binding']['review_id'], session=other, csrf=identity['csrf_token'])
    # Bearer API cannot bypass the extra browser-session binding.
    with pytest.raises(ProposalAdmissionError):
        e.service.admit(reviewed['binding']['review_id'], token=e.browser_credential.token)
    assert counts(e)['task_checkpoints'] == 0


@pytest.mark.parametrize('damage', ['logout', 'credential', 'permissions', 'expired', 'csrf', 'digest', 'correction'])
def test_stale_or_changed_authority_never_admits(browser, damage):
    e = browser
    reviewed = prepare(e)
    kwargs = {}
    if damage == 'logout':
        e.store.logout(e.session, e.csrf)
    elif damage == 'credential':
        e.authority.revoke_credential(e.browser_credential.id)
    elif damage == 'permissions':
        e.authority.update_principal('browser', permissions={'ccs:observe'})
    elif damage == 'expired':
        e.clock.now += 60001
    elif damage == 'csrf':
        kwargs['csrf'] = None
    elif damage == 'digest':
        kwargs['digest'] = '0' * 64
    elif damage == 'correction':
        complete(e.voice, 'corrected load beta')
    with pytest.raises((PolicyError, ProposalAdmissionError)):
        admit(e, reviewed, **kwargs)
    assert counts(e)['task_checkpoints'] == 0 and e.effects == []


def test_session_expiry_bounds_review_even_with_longer_credential(browser):
    e = browser
    e.store.SESSION_MS = 100
    session, identity = e.store.login(e.browser_credential.token)
    review = e.service.review_session(e.proposal, e.template.workflow.workflow_id, e.args,
                                     session=session, csrf=identity['csrf_token'])
    assert review.details()['expires_ms'] == 1100


def test_logout_at_final_sql_boundary_is_rechecked_not_cached(browser, monkeypatch):
    e = browser
    reviewed = prepare(e)
    original = e.journal._admit_task_authenticated
    def revoke_then_insert(checkpoint, authenticate):
        e.store.logout(e.session, e.csrf)
        return original(checkpoint, authenticate)
    monkeypatch.setattr(e.journal, '_admit_task_authenticated', revoke_then_insert)
    with pytest.raises(PolicyError):
        admit(e, reviewed)
    assert counts(e)['task_checkpoints'] == 0


@pytest.mark.parametrize('grants', [{'ccs:observe'}, {'ccs:observe', 'ccs:task:create'}, {'ccs:observe', 'test:write'}])
def test_review_requires_create_and_every_workflow_domain(browser, grants):
    e = browser
    e.authority.provision_principal('limited', grants)
    credential = e.authority.issue_credential('limited', ttl_ms=60000)
    session, identity = e.store.login(credential.token)
    with pytest.raises(PermissionDeniedError):
        e.controller.review(session, identity['csrf_token'])
    assert counts(e)['task_checkpoints'] == 0


def test_http_origin_csrf_host_and_exact_body_boundaries(browser):
    e = browser
    async def scenario():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=e.app), base_url=ORIGIN,
                                    cookies={'ccs_operator': e.session}) as client:
            headers = {'Origin': ORIGIN, 'X-CCS-CSRF': e.csrf}
            response = await client.post('/api/proposals/review', json={}, headers={'Origin': 'https://evil.example'})
            assert response.status_code == 403
            response = await client.post('/api/proposals/review', json={}, headers={'Origin': ORIGIN})
            assert response.status_code == 403
            for payload in ({'text': e.proposal.text}, {'confidence': 1}, {'workflow': 'injected'}, {'credential': 'PRIVATE'}):
                response = await client.post('/api/proposals/review', json=payload, headers=headers)
                assert response.status_code == 400 and 'PRIVATE' not in response.text
            response = await client.post('/api/proposals/review', json={}, headers={**headers, 'Host': 'evil.example'})
            assert response.status_code == 400
            response = await client.post('/api/proposals/review', json={}, headers=headers)
            assert response.status_code == 200
            assert response.headers['cache-control'] == 'no-store'
            assert 'frame-ancestors' in response.headers['content-security-policy']
            binding = response.json()['binding']
            response = await client.post('/api/proposals/admit', json={'review_id': binding['review_id'], 'binding_digest': '0'*64}, headers=headers)
            assert response.status_code == 409
            payload = {'review_id': binding['review_id'], 'binding_digest': binding['digest']}
            response = await client.post('/api/proposals/admit', json={**payload, 'arguments': e.args}, headers=headers)
            assert response.status_code == 400
            response = await client.post('/api/proposals/admit', json=payload, headers=headers)
            assert response.status_code == 200 and response.json()['state'] == 'ADMITTED'
            assert not response.json()['execution_authorized']
            assert (await client.post('/api/proposals/admit', json=payload, headers=headers)).json() == response.json()
    asyncio.run(scenario())
    assert counts(e) == {'task_checkpoints': 1, 'task_events': 1, 'actions': 0, 'approvals': 0}


def test_http_feature_is_disabled_by_default_and_transcript_is_not_logged(browser, caplog):
    e = browser
    app = create_app(e.journal)
    async def scenario():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=ORIGIN) as client:
            result = await client.post('/api/proposals/review', json={}, headers={'Origin': ORIGIN, 'X-CCS-CSRF': 'not-a-secret'})
            assert result.status_code == 404 and e.proposal.text not in result.text
    asyncio.run(scenario())
    assert e.proposal.text not in caplog.text


def test_anonymous_review_cannot_observe_whether_grounded_proposal_exists(browser):
    e = browser
    async def scenario():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=e.app), base_url=ORIGIN) as client:
            headers = {'Origin': ORIGIN, 'X-CCS-CSRF': '0' * 64}
            first = await client.post('/api/proposals/review', json={}, headers=headers)
            e.voice.latest = None
            second = await client.post('/api/proposals/review', json={}, headers=headers)
            assert first.status_code == second.status_code == 401
            assert first.json()['error']['code'] == second.json()['error']['code'] == 'session_required'
            assert e.proposal.text not in first.text + second.text
    asyncio.run(scenario())


def test_proposal_capability_discovery_is_authenticated_and_opt_in(browser):
    e = browser
    async def scenario():
        for app, configured in ((create_app(e.journal), False), (e.app, True)):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=ORIGIN) as client:
                anonymous = await client.get('/api/snapshot')
                assert anonymous.status_code == 401 and 'capabilities' not in anonymous.json()
                client.cookies.set('ccs_operator', e.session)
                response = await client.get('/api/snapshot')
                assert response.status_code == 200
                assert response.json()['capabilities'] == {'proposal_review': configured}
    asyncio.run(scenario())


def test_lost_http_admission_ack_is_read_back_without_replay(browser, monkeypatch):
    e = browser
    original = e.journal._admit_task_authenticated
    calls = []
    def lose_ack(checkpoint, authenticate):
        calls.append(1)
        original(checkpoint, authenticate)
        raise OSError('SIMULATED private acknowledgment loss')
    monkeypatch.setattr(e.journal, '_admit_task_authenticated', lose_ack)
    async def scenario():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=e.app), base_url=ORIGIN,
                                    cookies={'ccs_operator': e.session}) as client:
            headers = {'Origin': ORIGIN, 'X-CCS-CSRF': e.csrf}
            review = (await client.post('/api/proposals/review', json={}, headers=headers)).json()
            binding = review['binding']
            payload = {'review_id': binding['review_id'], 'binding_digest': binding['digest']}
            result = await client.post('/api/proposals/admit', json=payload, headers=headers)
            assert result.status_code == 500 and 'private' not in result.text
            status = await client.post('/api/proposals/status', json=payload, headers=headers)
            assert status.status_code == 200 and status.json()['admitted']
            assert status.json()['binding']['task_id'] == binding['task_id']
            assert not status.json()['execution_authorized']
    asyncio.run(scenario())
    assert calls == [1] and counts(e)['task_events'] == 1 and counts(e)['actions'] == 0
