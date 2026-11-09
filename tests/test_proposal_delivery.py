"""Durable evidence is inspection-only; all voice/tools are SIMULATED."""
import asyncio
from contextlib import closing
import json
import sqlite3
import subprocess
import sys

import httpx2 as httpx
import pytest

from control.app import create_app, ControlConfig
from runtime.action_journal import SQLiteActionJournal, JournalError
from runtime.control_store import ControlStore, ControlError
from runtime import proposal_delivery as delivery
from runtime.proposal_admission import ProposalAdmissionService
from runtime.proposal_contract import ProposalAdmissionError
from runtime.policy_engine import PolicyError
from runtime.voice_ingress import VoiceIngress
from scripts import journal_backup, recovery_preflight
from tests.test_session_proposal_review import browser, prepare, admit, ORIGIN
from tests.test_proposal_admission import env, counts, complete


@pytest.fixture
def retained(browser):
    browser.service.retain_reviews = True
    return browser


def read(e, review_id=None):
    return delivery.read(e.journal, e.session, e.csrf, review_id)


def rows(e):
    with closing(sqlite3.connect(e.journal.path)) as db:
        return db.execute('SELECT * FROM proposal_deliveries').fetchall()


def test_retention_is_opt_in_and_rejects_coercion(browser):
    prepare(browser)
    assert rows(browser) == []
    with pytest.raises(ProposalAdmissionError):
        ProposalAdmissionService(browser.manager, browser.voice, [browser.template], source_id='SIMULATED', retain_reviews=1)


def test_new_host_session_and_identical_voice_counters_cannot_resurrect_admission(retained):
    e = retained
    view = prepare(e)
    binding = view['binding']
    original = rows(e)
    e.store.logout(e.session, e.csrf)
    reopened = SQLiteActionJournal(e.journal.path, clock_ms=lambda: e.clock.now)
    store = ControlStore(reopened)
    session, identity = store.login(e.browser_credential.token)
    restored = delivery.read(reopened, session, identity['csrf_token'], binding['review_id'])
    assert restored['binding'] == binding and restored['transcript'] == e.proposal.text
    assert restored['scope'] == 'INSPECTION_ONLY'
    assert all(restored[k] is False for k in ('current_evidence_verified', 'admission_authorized', 'execution_authorized'))
    voice = VoiceIngress(); voice.open_stream(); complete(voice, e.proposal.text)
    service = ProposalAdmissionService(e.manager, voice, [e.template], source_id=e.service.source_id, retain_reviews=True)
    assert voice.latest.handle == e.proposal.handle  # Counter reuse is not identity.
    with pytest.raises(ProposalAdmissionError):
        service.admit_session(binding['review_id'], binding['digest'], session=session, csrf=identity['csrf_token'])
    assert rows(e) == original and counts(e) == dict(task_checkpoints=0, task_events=0, actions=0, approvals=0)
    assert e.effects == []


def test_correction_does_not_relabel_old_evidence_as_current_or_cancel_tasks(retained):
    e = retained
    view = prepare(e)
    complete(e.voice, 'SIMULATED corrected objective')
    archived = read(e, view['binding']['review_id'])
    assert archived['transcript'] == e.proposal.text and not archived['current_evidence_verified']
    with pytest.raises(ProposalAdmissionError): admit(e, view)
    assert counts(e)['task_checkpoints'] == 0


def test_admitted_reviews_leave_pending_list_but_detail_preserves_exact_history(retained):
    e = retained
    view = prepare(e)
    assert len(read(e)['items']) == 1
    admit(e, view)
    assert read(e)['items'] == []
    assert read(e, view['binding']['review_id'])['admitted']
    assert counts(e) == dict(task_checkpoints=1, task_events=1, actions=0, approvals=0)


def test_corrupt_task_projection_cannot_be_presented_as_admitted_delivery(retained):
    e = retained
    view = prepare(e); admit(e, view)
    with e.journal._transaction() as db:
        db.execute('UPDATE task_checkpoints SET checkpoint=?', (json.dumps({'proposal_binding': view['binding']}),))
    with pytest.raises(ControlError) as error: read(e, view['binding']['review_id'])
    assert error.value.status == 503


def test_global_capacity_is_atomic_and_expired_evidence_is_reclaimed(retained, monkeypatch):
    e = retained
    monkeypatch.setattr(delivery, 'CAPACITY', 2)
    for _ in range(2): prepare(e)
    with pytest.raises(ProposalAdmissionError): prepare(e)
    assert len(rows(e)) == 2 and len(e.service._pending) == 2
    e.clock.now += 60001
    prepare(e)
    assert len(rows(e)) == 1 and counts(e)['task_checkpoints'] == 0


def test_payload_limit_fails_before_pending_or_durable_publication(retained, monkeypatch):
    monkeypatch.setattr(delivery, 'MAX_BYTES', 64)
    with pytest.raises(ProposalAdmissionError): prepare(retained)
    assert rows(retained) == [] and retained.service._pending == {}


def test_revocation_at_publication_sql_boundary_prevents_storage(retained, monkeypatch):
    e = retained
    original = e.service._session_authenticator
    calls = []
    def factory(session, csrf):
        authenticate = original(session, csrf)
        def check(db, now):
            calls.append(1)
            if len(calls) == 3:  # preliminary auth, prepare, final publication
                db.execute('UPDATE operator_sessions SET revoked=1')
            return authenticate(db, now)
        return check
    monkeypatch.setattr(e.service, '_session_authenticator', factory)
    with pytest.raises(ControlError): prepare(e)
    assert rows(e) == [] and e.service._pending == {}


@pytest.mark.parametrize('damage', ['logout', 'csrf', 'missing_csrf', 'credential', 'version', 'expired_session'])
def test_delivery_requires_current_authority(retained, damage):
    e = retained
    view = prepare(e)
    if damage == 'logout': e.store.logout(e.session, e.csrf)
    elif damage == 'csrf': e.csrf = '0'*64
    elif damage == 'missing_csrf': e.csrf = None
    elif damage == 'credential': e.authority.revoke_credential(e.browser_credential.id)
    elif damage == 'version': e.authority.update_principal('browser', permissions={'ccs:observe'})
    else: e.clock.now += 600001
    with pytest.raises(ControlError) as error: read(e, view['binding']['review_id'])
    assert error.value.status in (401, 403)


@pytest.mark.parametrize('scope', ['principal', 'create', 'domain'])
def test_cross_session_reads_recheck_original_principal_and_current_grants(retained, scope):
    e = retained
    view = prepare(e)
    grants = {'ccs:observe', 'ccs:task:create', 'test:write'}
    principal = 'other-reader' if scope == 'principal' else 'browser'
    if scope == 'create': grants.remove('ccs:task:create')
    if scope == 'domain': grants.remove('test:write')
    if principal != 'browser': e.authority.provision_principal(principal, grants)
    else: e.authority.update_principal(principal, permissions=grants)
    credential = e.authority.issue_credential(principal)
    e.session, identity = e.store.login(credential.token); e.csrf = identity['csrf_token']
    assert read(e)['items'] == []
    with pytest.raises(ControlError) as error: read(e, view['binding']['review_id'])
    assert error.value.status == 404


def test_expiry_read_deletes_logical_rows_without_admission(retained):
    e = retained
    view = prepare(e)
    e.clock.now = view['binding']['expires_ms']
    assert read(e)['items'] == [] and rows(e) == []
    with pytest.raises(ControlError) as error: read(e, view['binding']['review_id'])
    assert error.value.status == 404 and counts(e)['task_checkpoints'] == 0


def test_expired_detail_commits_cleanup_even_when_lookup_returns_unavailable(retained):
    e = retained
    view = prepare(e)
    e.clock.now = view['binding']['expires_ms']
    with pytest.raises(ControlError) as error: read(e, view['binding']['review_id'])
    assert error.value.status == 404 and rows(e) == []


def test_expiry_cannot_be_resurrected_by_clock_rollback(retained):
    e = retained
    view = prepare(e)
    e.clock.now = view['binding']['expires_ms']
    assert read(e)['items'] == []
    e.clock.now -= 1
    with pytest.raises(PolicyError): read(e, view['binding']['review_id'])
    assert rows(e) == []


def test_fresh_process_reads_retained_identity_without_voice_or_admission_capability(retained):
    e = retained
    view = prepare(e)
    code = '''import json,sys
from runtime.action_journal import SQLiteActionJournal
from runtime.control_store import ControlStore
from runtime.proposal_delivery import read
v=json.load(sys.stdin)
j=SQLiteActionJournal(v['path'],clock_ms=lambda:v['now'])
s,i=ControlStore(j).login(v['credential'])
r=read(j,s,i['csrf_token'],v['review_id'])
print(json.dumps({'digest':r['binding']['digest'],'scope':r['scope'],'admission_authorized':r['admission_authorized']}))
'''
    result = subprocess.run([sys.executable, '-c', code], input=json.dumps(dict(path=str(e.journal.path), now=e.clock.now,
                            credential=e.browser_credential.token, review_id=view['binding']['review_id'])),
                            text=True, capture_output=True, timeout=15)
    assert result.returncode == 0  # Raw child diagnostics/credentials are not printed.
    assert json.loads(result.stdout) == dict(digest=view['binding']['digest'], scope='INSPECTION_ONLY', admission_authorized=False)
    assert counts(e) == dict(task_checkpoints=0, task_events=0, actions=0, approvals=0)


@pytest.mark.parametrize('damage', ['json', 'text', 'arguments', 'expiry', 'review_id', 'tokens'])
def test_corrupt_delivery_fails_live_read_and_readonly_recovery_inspection(retained, damage):
    e = retained
    view = prepare(e)
    with e.journal._transaction() as db:
        payload = json.loads(rows(e)[0][4])
        if damage == 'text': payload['evidence']['text'] = 'PRIVATE-CORRUPTED'
        elif damage == 'arguments': payload['checkpoint']['proposal_binding']['arguments'] = [{'wrong': 1}]
        elif damage == 'tokens': payload['evidence']['tokens'] = []
        db.execute('UPDATE proposal_deliveries SET payload=?', ('INVALID' if damage == 'json' else json.dumps(payload),))
        if damage == 'expiry': db.execute('UPDATE proposal_deliveries SET expires_ms=expires_ms+1')
        if damage == 'review_id': db.execute("UPDATE proposal_deliveries SET review_id='changed'")
    with pytest.raises(ControlError) as error: read(e)
    assert error.value.status == 503 and 'PRIVATE-CORRUPTED' not in str(error.value)
    before = e.journal.path.read_bytes()
    report = recovery_preflight.inspect(e.journal.path)
    assert not report['passed'] and 'PRIVATE-CORRUPTED' not in json.dumps(report)
    assert e.journal.path.read_bytes() == before


def test_backup_contains_delivery_and_never_authorizes_its_activation(retained, tmp_path):
    e = retained
    prepare(e)
    before = rows(e)
    target = tmp_path / 'retained.ccsb'
    assert journal_backup.capture(e.journal.path, target)['passed']
    report = journal_backup.drill(target, tmp_path)
    assert report['passed'] and not report['restore_authorized'] and not report['dispatch_authorized']
    assert report['counts']['proposal_deliveries'] == 1 and rows(e) == before


def test_schema_header_cannot_hide_new_delivery_semantics_from_backup_validation(retained):
    e = retained
    prepare(e)
    with closing(sqlite3.connect(e.journal.path)) as db:
        db.execute('PRAGMA user_version=7'); db.commit()
    report = recovery_preflight.inspect(e.journal.path)
    assert not report['passed'] and not report['restore_authorized']


def test_schema7_migration_is_offline_explicit_atomic_and_preserves_task_evidence(browser, monkeypatch):
    e = browser
    view = prepare(e); admit(e, view)
    before = e.journal.load_task(view['binding']['task_id'])
    with closing(sqlite3.connect(e.journal.path)) as db:
        db.execute('DROP TABLE task_replacements'); db.execute('DROP TABLE proposal_review_fences'); db.execute('DROP TABLE proposal_deliveries'); db.execute('PRAGMA user_version=7'); db.commit()
    with pytest.raises(JournalError): SQLiteActionJournal(e.journal.path, clock_ms=lambda: e.clock.now)
    with pytest.raises(JournalError): SQLiteActionJournal(e.journal.path, clock_ms=lambda: e.clock.now, migrate=True)
    with pytest.raises(JournalError): e.journal.load_task(view['binding']['task_id'])
    e.clock.now += 3600000
    original = delivery.create_schema
    def fail(db):
        original(db); raise RuntimeError('SIMULATED migration interruption')
    monkeypatch.setattr(delivery, 'create_schema', fail)
    with pytest.raises(RuntimeError): SQLiteActionJournal(e.journal.path, clock_ms=lambda: e.clock.now, migrate=True)
    with closing(sqlite3.connect(e.journal.path)) as db:
        assert db.execute('PRAGMA user_version').fetchone()[0] == 7
        assert db.execute("SELECT 1 FROM sqlite_master WHERE name='proposal_deliveries'").fetchone() is None
    monkeypatch.setattr(delivery, 'create_schema', original)
    migrated = SQLiteActionJournal(e.journal.path, clock_ms=lambda: e.clock.now, migrate=True)
    assert migrated.load_task(view['binding']['task_id']) == before
    with migrated._transaction() as db:
        assert db.execute('PRAGMA user_version').fetchone()[0] == 10


def test_default_host_delivers_protected_readonly_evidence_over_exact_http_body(retained):
    e = retained
    view = prepare(e)
    app = create_app(e.journal, ControlConfig(ORIGIN))
    async def scenario():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=ORIGIN) as client:
            headers = {'Origin': ORIGIN, 'X-CCS-CSRF': e.csrf}
            assert (await client.post('/api/proposals/deliveries', json={'review_id': None}, headers=headers)).status_code == 401
            client.cookies.set('ccs_operator', e.session)
            for altered in ({'Origin': 'https://evil.example'}, {'X-CCS-CSRF': ''}):
                assert (await client.post('/api/proposals/deliveries', json={'review_id': None}, headers={**headers, **altered})).status_code == 403
            for data in ({}, {'review_id': []}, {'review_id': None, 'arguments': e.args}):
                assert (await client.post('/api/proposals/deliveries', json=data, headers=headers)).status_code == 400
            result = await client.post('/api/proposals/deliveries', json={'review_id': view['binding']['review_id']}, headers=headers)
            assert result.status_code == 200 and result.headers['cache-control'] == 'no-store'
            assert result.json()['transcript'] == e.proposal.text and not result.json()['admission_authorized']
    asyncio.run(scenario())
    assert counts(e) == dict(task_checkpoints=0, task_events=0, actions=0, approvals=0)
