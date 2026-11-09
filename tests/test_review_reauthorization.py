"""Stable unadmitted task identity, deterministic SQL races; SIMULATED evidence."""
import asyncio
from contextlib import closing
import json
import sqlite3

import httpx2 as httpx
import pytest

from runtime.action_journal import SQLiteActionJournal, JournalError
from runtime.control_store import ControlStore
from runtime.policy_engine import PolicyError, PermissionDeniedError
from runtime.proposal_contract import ProposalAdmissionError, validate_binding, digest
from runtime.workflow_contract import canonical
from runtime import proposal_delivery, review_fence
from scripts import recovery_preflight
from tests.test_proposal_delivery import retained
from tests.test_session_proposal_review import browser, prepare, admit, ORIGIN
from tests.test_proposal_admission import env, counts, complete


def fresh(e, old, **kwargs):
    b = old['binding']
    r = e.service.reauthorize_session(b['review_id'], b['digest'], e.template.workflow.workflow_id, e.args,
                                     session=e.session, csrf=e.csrf, **kwargs)
    return e.service.view_session(r.review_id, session=e.session, csrf=e.csrf)


def stored(e):
    with closing(sqlite3.connect(e.journal.path)) as db:
        return (db.execute('SELECT * FROM proposal_deliveries').fetchall(),
                db.execute('SELECT * FROM proposal_review_fences').fetchall())


def test_current_correction_has_fresh_review_and_immutable_predecessor_for_same_task(retained):
    e = retained; old = prepare(e); before = json.dumps(old, sort_keys=True)
    complete(e.voice, 'SIMULATED corrected destination')
    e.args = [{'destination': 'corrected-zone'}]
    new = fresh(e, old)
    assert new['binding']['task_id'] == old['binding']['task_id']
    assert new['binding']['review_id'] != old['binding']['review_id']
    assert new['binding']['supersedes'] == dict(review_id=old['binding']['review_id'], binding_digest=old['binding']['digest'])
    assert new['binding']['arguments'] == e.args and new['transcript'] == 'SIMULATED corrected destination'
    assert new['current'] and not new['admitted'] and not new['execution_authorized']
    assert json.dumps(old, sort_keys=True) == before
    assert stored(e)[0][0][0] == new['binding']['review_id']
    assert counts(e) == dict(task_checkpoints=0, task_events=0, actions=0, approvals=0)


def test_fence_rejects_old_connection_and_preserves_one_admission_and_progress(retained):
    e = retained; old = prepare(e)
    old_cp = json.loads(e.service._pending[old['binding']['review_id']].checkpoint)
    new = fresh(e, old)  # Same VoiceProposal object: SQL must fence it anyway.
    assert not e.service.view_session(old['binding']['review_id'], session=e.session, csrf=e.csrf)['current']
    other = SQLiteActionJournal(e.journal.path, clock_ms=lambda:e.clock.now)
    other.bind_lease(e.journal._lease)
    with pytest.raises(PolicyError): other.admit_task(old_cp, e.browser_credential.token)
    receipt = admit(e, new)
    version, cp = e.journal.load_task(receipt.task_id)
    cp['state'] = 'CANCELLED'; e.journal.save_task(receipt.task_id, cp, version)
    assert admit(e, new) == receipt
    assert e.journal.load_task(receipt.task_id) == (1, cp)
    with pytest.raises((PolicyError, JournalError)): other.admit_task(old_cp, e.browser_credential.token)
    with e.journal._transaction() as db:
        assert db.execute("SELECT count(*) FROM security_events WHERE kind='TASK_ADMITTED'").fetchone()[0] == 1
        assert db.execute("SELECT count(*) FROM security_events WHERE kind='PROPOSAL_REAUTHORIZED'").fetchone()[0] == 1
    assert counts(e)['actions'] == 0 and e.effects == []


@pytest.mark.parametrize('state', ['PENDING','CANCELLED','COMPLETED','FAILED','NEEDS_RECONCILIATION'])
def test_any_admitted_task_blocks_reauthorization_without_rewriting_evidence(retained, state):
    e=retained; old=prepare(e); receipt=admit(e,old)
    version, cp=e.journal.load_task(receipt.task_id)
    cp['state']=state; e.journal.save_task(receipt.task_id,cp,version)
    before=stored(e)
    with pytest.raises(PolicyError): fresh(e,old)
    assert stored(e)==before and e.journal.load_task(receipt.task_id)==(1,cp)
    assert e.effects==[]


def test_original_admission_wins_before_successor_publication(retained, monkeypatch):
    e=retained; old=prepare(e); original=e.manager.prepare_checkpoint
    def admit_first(*args,**kwargs):
        admit(e,old)
        return original(*args,**kwargs)
    monkeypatch.setattr(e.manager,'prepare_checkpoint',admit_first)
    before=stored(e)
    with pytest.raises(PolicyError): fresh(e,old)
    assert stored(e)==before and counts(e)==dict(task_checkpoints=1,task_events=1,actions=0,approvals=0)
    assert e.journal.load_task(old['binding']['task_id'])[1]['proposal_binding']==old['binding']


def test_competing_reauthorization_wins_before_second_publication(retained, monkeypatch):
    e=retained; old=prepare(e); original=e.manager.prepare_checkpoint; winner=[]
    def race(*args,**kwargs):
        monkeypatch.setattr(e.manager,'prepare_checkpoint',original)
        winner.append(fresh(e,old))
        return original(*args,**kwargs)
    monkeypatch.setattr(e.manager,'prepare_checkpoint',race)
    with pytest.raises(PolicyError): fresh(e,old)
    assert stored(e)[0][0][0]==winner[0]['binding']['review_id']
    assert len(e.service._pending)==2 and counts(e)['task_checkpoints']==0


def test_failed_audit_rolls_back_successor_payload_and_fence(retained, monkeypatch):
    e=retained; old=prepare(e); before=stored(e); original=e.service.authority._event
    def fail(db,kind,*args,**kwargs):
        if kind=='PROPOSAL_REAUTHORIZED': raise PermissionDeniedError('SIMULATED audit refusal')
        return original(db,kind,*args,**kwargs)
    monkeypatch.setattr(e.service.authority,'_event',fail)
    with pytest.raises(PolicyError): fresh(e,old)
    assert stored(e)==before and len(e.service._pending)==1
    assert admit(e,old).task_id==old['binding']['task_id']


def test_shorter_successor_expiry_does_not_unlock_predecessor(retained):
    e=retained; old=prepare(e); new=fresh(e,old,ttl_ms=10)
    e.clock.now+=11
    assert proposal_delivery.read(e.journal,e.session,e.csrf)['items']==[]
    assert stored(e)[1][0][3]==old['binding']['expires_ms']
    with pytest.raises(PolicyError): admit(e,old)
    assert counts(e)['task_checkpoints']==0


def test_rotated_credential_new_session_requires_fresh_explicit_acceptance(retained):
    e=retained; old=prepare(e)
    e.authority.revoke_credential(e.browser_credential.id)
    credential=e.authority.issue_credential('browser')
    e.session,identity=e.store.login(credential.token);e.csrf=identity['csrf_token']
    new=fresh(e,old)
    assert new['binding']['reviewer']['credential_id']==credential.id
    assert counts(e)['task_checkpoints']==0
    assert admit(e,new).task_id==old['binding']['task_id']


@pytest.mark.parametrize('damage',['principal','domain','create','csrf','logout','expired','voice','retention'])
def test_stale_or_insufficient_authority_never_reauthorizes(retained,damage):
    e=retained;old=prepare(e);before=stored(e)
    if damage in {'principal','domain','create'}:
        grants={'ccs:observe','ccs:task:create','test:write'};p='other-reader' if damage=='principal' else 'browser'
        if damage=='domain':grants.remove('test:write')
        if damage=='create':grants.remove('ccs:task:create')
        if damage=='principal':e.authority.provision_principal(p,grants)
        else:e.authority.update_principal(p,permissions=grants)
        c=e.authority.issue_credential(p);e.session,i=e.store.login(c.token);e.csrf=i['csrf_token']
    elif damage=='csrf':e.csrf='0'*64
    elif damage=='logout':e.store.logout(e.session,e.csrf)
    elif damage=='expired':e.clock.now+=60001
    elif damage=='voice':e.voice.invalidate()
    else:e.service.retain_reviews=False
    with pytest.raises((PolicyError,ProposalAdmissionError)):fresh(e,old)
    assert stored(e)==before and counts(e)['task_checkpoints']==0


def test_missing_fence_blocks_new_format_admission_and_snapshot_acceptance(retained):
    e=retained;old=prepare(e)
    with e.journal._transaction() as db:db.execute('DELETE FROM proposal_review_fences')
    with pytest.raises(PolicyError):fresh(e,old)
    with pytest.raises(PolicyError):admit(e,old)
    assert not recovery_preflight.inspect(e.journal.path)['passed']


@pytest.mark.parametrize('damage',['digest','retire','task'])
def test_corrupt_fence_fails_readonly_recovery_validation(retained,damage):
    e=retained;prepare(e)
    with e.journal._transaction() as db:
        if damage=='digest':db.execute("UPDATE proposal_review_fences SET binding_digest=?",('0'*64,))
        elif damage=='retire':db.execute('UPDATE proposal_review_fences SET retire_after_ms=1')
        else:db.execute("UPDATE proposal_review_fences SET task_id='task-wrong'")
    assert not recovery_preflight.inspect(e.journal.path)['passed']


def test_successor_reuses_capacity_without_evicting_other_task_identities(retained,monkeypatch):
    e=retained;monkeypatch.setattr(review_fence,'CAPACITY',1)
    old=prepare(e);new=fresh(e,old)
    assert len(stored(e)[1])==1 and new['binding']['task_id']==old['binding']['task_id']
    with pytest.raises(ProposalAdmissionError):prepare(e)
    assert stored(e)[1][0][1]==new['binding']['review_id']


@pytest.mark.parametrize('damage',['self','missing','extra','digest'])
def test_lineage_contract_rejects_invalid_predecessor_identity(retained,damage):
    e=retained;old=prepare(e);new=fresh(e,old)
    cp=json.loads(e.service._pending[new['binding']['review_id']].checkpoint)
    prior=cp['proposal_binding']['supersedes']
    if damage=='self':prior['review_id']=new['binding']['review_id']
    elif damage=='missing':prior.pop('binding_digest')
    elif damage=='extra':prior['authority']=True
    else:prior['binding_digest']='G'*64
    with pytest.raises(ProposalAdmissionError):validate_binding(cp['proposal_binding'],cp)


def test_successor_checkpoint_restores_without_operation_approval_or_execution(retained):
    e=retained;new=fresh(e,prepare(e));receipt=admit(e,new)
    task=e.manager.restore_task(receipt.task_id,list(e.template.steps),workflow=e.template.workflow)
    assert task.task_id==receipt.task_id and e.effects==[]
    assert e.journal.load_task(task.task_id)[1]['proposal_binding']==new['binding']
    assert counts(e)['actions']==counts(e)['approvals']==0


def test_unadmitted_chain_evidence_blocks_reauthorization(retained):
    e=retained;old=prepare(e)
    with e.journal._transaction() as db:
        db.execute("INSERT INTO chains VALUES (?,?,'OPEN')",('task:'+old['binding']['task_id'],old['binding']['task_id']))
    with pytest.raises(PolicyError):fresh(e,old)
    assert counts(e)['task_checkpoints']==0


def test_schema8_migration_seeds_legacy_retained_fence_and_rolls_back_atomically(retained,monkeypatch):
    e=retained;old=prepare(e)
    with e.journal._transaction() as db:
        row=db.execute('SELECT payload FROM proposal_deliveries').fetchone();payload=json.loads(row[0])
        b=payload['checkpoint']['proposal_binding'];b['schema']=1;b.pop('supersedes');b['digest']=digest({k:v for k,v in b.items() if k!='digest'})
        db.execute('UPDATE proposal_deliveries SET payload=?',(canonical(payload),))
    e.journal.release_lease()
    with closing(sqlite3.connect(e.journal.path)) as db:db.execute('DROP TABLE task_replacements');db.execute('DROP TABLE proposal_review_fences');db.execute('PRAGMA user_version=8');db.commit()
    with pytest.raises(JournalError):SQLiteActionJournal(e.journal.path,clock_ms=lambda:e.clock.now)
    original=review_fence.create_schema
    def fail(db):original(db);raise RuntimeError('SIMULATED migration interruption')
    monkeypatch.setattr(review_fence,'create_schema',fail)
    with pytest.raises(RuntimeError):SQLiteActionJournal(e.journal.path,clock_ms=lambda:e.clock.now,migrate=True)
    with closing(sqlite3.connect(e.journal.path)) as db:
        assert db.execute('PRAGMA user_version').fetchone()[0]==8
        assert db.execute("SELECT 1 FROM sqlite_master WHERE name='proposal_review_fences'").fetchone() is None
    monkeypatch.setattr(review_fence,'create_schema',original)
    migrated=SQLiteActionJournal(e.journal.path,clock_ms=lambda:e.clock.now,migrate=True)
    with migrated._transaction() as db:
        assert db.execute('SELECT review_id,binding_digest FROM proposal_review_fences').fetchone()==(b['review_id'],b['digest'])
    assert recovery_preflight.inspect(migrated.path)['passed']


def test_http_current_correction_accepts_only_original_identity_and_protects_boundaries(retained):
    e=retained;old=prepare(e);payload=dict(review_id=old['binding']['review_id'],binding_digest=old['binding']['digest'])
    async def scenario():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=e.app),base_url=ORIGIN) as client:
            headers={'Origin':ORIGIN,'X-CCS-CSRF':e.csrf}
            assert (await client.post('/api/proposals/reauthorize',json=payload,headers=headers)).status_code==401
            client.cookies.set('ccs_operator',e.session)
            for h in ({'Origin':'https://evil.example'},{'X-CCS-CSRF':''}):
                assert (await client.post('/api/proposals/reauthorize',json=payload,headers={**headers,**h})).status_code==403
            for p in ({**payload,'arguments':e.args},{**payload,'workflow':'injected'},{'review_id':[]}):
                assert (await client.post('/api/proposals/reauthorize',json=p,headers=headers)).status_code==400
            result=await client.post('/api/proposals/reauthorize',json=payload,headers=headers)
            assert result.status_code==200 and result.headers['cache-control']=='no-store'
            assert result.json()['binding']['task_id']==old['binding']['task_id'] and not result.json()['admitted']
    asyncio.run(scenario())


@pytest.mark.parametrize('malformed',[[],42,'invalid',True])
def test_legacy_snapshot_malformed_binding_returns_sanitized_failure(browser,malformed):
    e=browser;old=prepare(e);receipt=admit(e,old)
    cp=e.journal.load_task(receipt.task_id)[1]
    with e.journal._transaction() as db:
        cp['proposal_binding']=malformed
        payload=canonical(cp)
        db.execute('UPDATE task_checkpoints SET checkpoint=?',(payload,))
        db.execute('UPDATE task_events SET checkpoint=?',(payload,))
    with closing(sqlite3.connect(e.journal.path)) as db:
        db.execute('DROP TABLE task_replacements');db.execute('DROP TABLE proposal_review_fences');db.execute('DROP TABLE proposal_deliveries')
        db.execute('PRAGMA user_version=7');db.commit()
    before=e.journal.path.read_bytes()
    report=recovery_preflight.inspect(e.journal.path)
    assert not report['passed'] and report['failure']=='JOURNAL_EVIDENCE_UNVERIFIED'
    assert e.proposal.text not in json.dumps(report)
    assert e.journal.path.read_bytes()==before
