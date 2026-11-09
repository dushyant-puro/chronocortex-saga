"""Manual host integration: real authority/journal, SIMULATED effects only."""
import asyncio
import pytest
from starlette.testclient import TestClient

from acceptance import local, review
from runtime.action_journal import SQLiteActionJournal
from tests.test_local_acceptance import ORIGIN, approve, command, connect, effects, pending, wait_for


def post(client, csrf, path, data):
    return client.post('/api/proposals/' + path, json=data,
                       headers={'Origin': ORIGIN, 'X-CCS-CSRF': csrf})


def admit(client, csrf):
    response = post(client, csrf, 'review', {})
    assert response.status_code == 200, response.text
    view = response.json()
    response = post(client, csrf, 'admit', {'review_id': view['binding']['review_id'],
                                         'binding_digest': view['binding']['digest']})
    assert response.status_code == 200, response.text
    return response.json()['task_id']


def test_no_seeded_task_review_is_not_admission_and_each_effect_needs_approval(tmp_path):
    app = review.create_app(tmp_path)
    with TestClient(app, base_url=ORIGIN) as client:
        assert 'SIMULATED MANUAL REVIEW' in client.get('/').text
        journal = SQLiteActionJournal(app.state.acceptance_files.journal)
        assert not journal.records() and not hasattr(app.state, 'acceptance_task_id')
        csrf = connect(app, client)
        assert client.get('/api/snapshot').json()['capabilities']['proposal_review']
        task = admit(client, csrf)
        first = wait_for(lambda: pending(app))
        assert effects(app) == []
        approve(client, csrf, first)
        second = wait_for(lambda: (p if (p := pending(app)) and p != first else None))
        assert effects(app) == [(1,)]
        approve(client, csrf, second)
        wait_for(lambda: journal.load_task(task)[1]['state'] == 'COMPLETED')
        assert effects(app) == [(1,), (2,)]
    assert local.stopped_cleanly(app)


@pytest.mark.parametrize('attack', ['unauthenticated', 'csrf', 'origin', 'browser_evidence', 'digest', 'revoked'])
def test_untrusted_or_stale_requests_create_no_task_or_effect(tmp_path, attack):
    app = review.create_app(tmp_path)
    with TestClient(app, base_url=ORIGIN) as client:
        csrf = connect(app, client)
        if attack == 'unauthenticated':
            client.cookies.clear()
        if attack == 'csrf':
            csrf = 'wrong'
        if attack == 'origin':
            response = client.post('/api/proposals/review', json={},
                headers={'Origin': 'http://evil.test', 'X-CCS-CSRF': csrf})
        elif attack == 'digest':
            view = post(client, csrf, 'review', {}).json()
            response = post(client, csrf, 'admit', {'review_id': view['binding']['review_id'], 'binding_digest': '0' * 64})
        else:
            if attack == 'revoked':
                SQLiteActionJournal(app.state.acceptance_files.journal).security.update_principal(
                    'SIMULATED-local-operator', active=False)
            response = post(client, csrf, 'review', {'transcript': 'forged'} if attack == 'browser_evidence' else {})
        assert response.status_code in (400, 401, 403, 409)
        assert not hasattr(app.state, 'acceptance_task_id') and effects(app) == []
    assert local.stopped_cleanly(app)


def test_one_selection_per_run_cancel_does_not_dispatch_later_admission(tmp_path):
    app = review.create_app(tmp_path)
    with TestClient(app, base_url=ORIGIN) as client:
        csrf = connect(app, client)
        original = admit(client, csrf)
        wait_for(lambda: pending(app))
        command(client, csrf, 'cancel', original)
        wait_for(lambda: app.state.acceptance_operation.done())
        second = admit(client, csrf)
        assert original != second and app.state.acceptance_task_id == original
        assert SQLiteActionJournal(app.state.acceptance_files.journal).load_task(second)[1]['state'] == 'PENDING'
        assert effects(app) == []
    assert local.stopped_cleanly(app)


def test_lost_apply_response_parks_without_retry_or_descendant(tmp_path, monkeypatch):
    original = local.LocalTasks._get_executor
    def lost(self, step):
        execute = original(self, step)
        async def run(args):
            await execute(args)
            raise TimeoutError('SIMULATED lost acknowledgement')
        return run
    monkeypatch.setattr(local.LocalTasks, '_get_executor', lost)
    app = review.create_app(tmp_path)
    with TestClient(app, base_url=ORIGIN) as client:
        csrf = connect(app, client)
        task = admit(client, csrf)
        approve(client, csrf, wait_for(lambda: pending(app)))
        wait_for(lambda: app.state.acceptance_operation.done())
        journal = SQLiteActionJournal(app.state.acceptance_files.journal)
        assert journal.load_task(task)[1]['state'] == 'NEEDS_RECONCILIATION'
        assert [r['state'] for _, r in journal.records()] == ['IN_FLIGHT_UNKNOWN']
        assert effects(app) == [(1,)]
    assert not local.stopped_cleanly(app)


def test_requests_without_lifespan_fail_closed(tmp_path):
    with TestClient(review.create_app(tmp_path), base_url=ORIGIN) as client:
        assert client.get('/file-entry.js').status_code == 200
        assert client.get('/file-entry.css').status_code == 200
        assert client.get('/review.py').status_code == 404
    # No context manager means no owning-loop admission or worker startup.
    other = tmp_path.parent / (tmp_path.name + '-other')
    other.mkdir(mode=0o700)
    client = TestClient(review.create_app(other), base_url=ORIGIN)
    assert client.get('/').status_code == 503
