"""Cancelled zero-activity replacement; real auth/SQL, SIMULATED tools/voice."""
import asyncio
from contextlib import closing
import json
import sqlite3
import uuid
from concurrent.futures import ThreadPoolExecutor
import threading

import httpx2 as httpx
import pytest

from runtime import task_replacement as policy
from runtime.action_journal import SQLiteActionJournal,JournalError
from runtime.policy_engine import PolicyError
from runtime.proposal_contract import ProposalAdmissionError,validate_binding,digest
from runtime.workflow_contract import canonical
from scripts import recovery_preflight,journal_backup
from tests.test_proposal_delivery import retained
from tests.test_session_proposal_review import browser,prepare,admit,ORIGIN
from tests.test_proposal_admission import env,counts


@pytest.fixture
def original(retained):
    e=retained
    e.authority.update_principal('browser',permissions={'ccs:observe','ccs:task:create','ccs:cancel','test:write'})
    e.session,i=e.store.login(e.browser_credential.token);e.csrf=i['csrf_token']
    e.original=prepare(e);e.original_id=admit(e,e.original).task_id
    return e


def cancel(e):
    view=e.store.detail(e.session,'tasks',e.original_id)
    e.store.command(e.session,e.csrf,'CANCEL',e.original_id,
                    {'operation_id':str(uuid.uuid4()),'review_digest':view['review_digest']})


@pytest.fixture
def cancelled(original):
    cancel(original)
    return original


def inspect(e):return policy.inspect(e.journal,e.session,e.csrf,e.original_id)


def replacement(e,descriptor=None):
    descriptor=descriptor or inspect(e)['original']
    r=e.service.replacement_session(e.original_id,descriptor['digest'],e.template.workflow.workflow_id,e.args,session=e.session,csrf=e.csrf)
    return e.service.view_session(r.review_id,session=e.session,csrf=e.csrf)


def claims(e):
    with closing(sqlite3.connect(e.journal.path)) as db:return db.execute('SELECT * FROM task_replacements').fetchall()


def test_explicit_cancel_then_new_review_then_accept_preserves_original_and_claim(cancelled):
    e=cancelled;before=e.journal.load_task(e.original_id);status=inspect(e)
    assert status['eligible'] and not status['execution_authorized']
    new=replacement(e)
    assert new['binding']['task_id']!=e.original_id and new['binding']['replaces_task']==status['original']
    assert not new['admitted'] and not new['execution_authorized'] and claims(e)==[]
    receipt=admit(e,new)
    assert e.journal.load_task(e.original_id)==before
    assert claims(e)==[(e.original_id,receipt.task_id,status['original']['version'],status['original']['digest'])]
    assert admit(e,new)==receipt and counts(e)==dict(task_checkpoints=2,task_events=3,actions=0,approvals=0)
    with pytest.raises(JournalError):e.journal.save_task(e.original_id,before[1],before[0])
    assert inspect(e)['reason']=='ALREADY_REPLACED'
    assert recovery_preflight.inspect(e.journal.path)['passed'] and e.effects==[]


def test_two_prepared_candidates_can_admit_only_one_replacement(cancelled):
    e=cancelled;a=replacement(e);b=replacement(e)
    admit(e,a)
    with pytest.raises(PolicyError):admit(e,b)
    assert len(claims(e))==1 and counts(e)['task_checkpoints']==2


def test_origin_change_at_admission_boundary_blocks_stale_review(cancelled,monkeypatch):
    e=cancelled;new=replacement(e);original=e.journal._admit_task_authenticated
    def change(cp,authenticate):
        version,old=e.journal.load_task(e.original_id);old['error']='SIMULATED changed cancellation evidence'
        e.journal.save_task(e.original_id,old,version)
        return original(cp,authenticate)
    monkeypatch.setattr(e.journal,'_admit_task_authenticated',change)
    with pytest.raises(PolicyError):admit(e,new)
    assert claims(e)==[] and counts(e)['task_checkpoints']==1


def test_failed_audit_rolls_back_new_task_and_claim(cancelled,monkeypatch):
    e=cancelled;new=replacement(e)
    # Admission creates its own SecurityAuthority: patch the class method.
    from runtime.security_authority import SecurityAuthority
    actual=SecurityAuthority._event
    def fail(db,kind,now,**metadata):
        if kind=='TASK_REPLACEMENT_ADMITTED':raise PolicyError('SIMULATED audit refusal')
        return actual(db,kind,now,**metadata)
    monkeypatch.setattr(SecurityAuthority,'_event',staticmethod(fail))
    with pytest.raises(PolicyError):admit(e,new)
    assert claims(e)==[] and counts(e)==dict(task_checkpoints=1,task_events=2,actions=0,approvals=0)


@pytest.mark.parametrize('state',['PENDING','RUNNING','NEEDS_RECONCILIATION','COMPLETED','FAILED'])
def test_non_cancelled_state_never_allows_replacement(original,state):
    e=original;v,cp=e.journal.load_task(e.original_id);cp['state']=state;e.journal.save_task(e.original_id,cp,v)
    assert inspect(e)['eligible'] is False and inspect(e)['reason']=='TASK_NOT_CANCELLED'


@pytest.mark.parametrize('field,value',[('current_step_index',1),('current_step_index',False),('step_states',['SKIPPED']),('step_states',['IN_FLIGHT_UNKNOWN']),('step_results',[{'SIMULATED':'result'}]),('step_action_ids',['action']),('step_dispatched_args',[{}]),('step_generations',[1]),('step_generations',[False]),('replan_from',0)])
def test_any_step_activity_blocks_replacement(cancelled,field,value):
    e=cancelled;v,cp=e.journal.load_task(e.original_id);cp[field]=value;e.journal.save_task(e.original_id,cp,v)
    assert inspect(e)['eligible'] is False and inspect(e)['reason']=='RECORDED_ACTIVITY'


def test_pending_action_before_cancel_is_not_proof_of_zero_effects(original):
    e=original
    e.manager._saga.stage_write('move',e.args[0],owner_task_id=e.original_id,owner_step_index=0,owner_generation=0)
    cancel(e)
    assert inspect(e)['reason']=='RECORDED_ACTIVITY' and claims(e)==[]


@pytest.mark.parametrize('damage',['absent','open','ownership'])
def test_missing_or_unclosed_chain_blocks_replacement(cancelled,damage):
    e=cancelled
    with e.journal._transaction() as db:
        if damage=='absent':
            db.execute('DELETE FROM chain_events WHERE chain_id=?',('task:'+e.original_id,))
            db.execute('DELETE FROM chains WHERE task_id=?',(e.original_id,))
        elif damage=='open':db.execute("UPDATE chains SET status='OPEN' WHERE task_id=?",(e.original_id,))
        else:db.execute("UPDATE chains SET task_id='wrong' WHERE chain_id=?",('task:'+e.original_id,))
    assert inspect(e)['eligible'] is False and inspect(e)['reason']=='CHAIN_NOT_CLOSED'


@pytest.mark.parametrize('damage',['principal','cancel','create','domain','csrf','session','voice','retention'])
def test_current_authority_and_evidence_are_required(cancelled,damage):
    e=cancelled;descriptor=inspect(e)['original']
    if damage in {'principal','cancel','create','domain'}:
        grants={'ccs:observe','ccs:task:create','ccs:cancel','test:write'};p='other-reader' if damage=='principal' else 'browser'
        if damage=='cancel':grants.remove('ccs:cancel')
        if damage=='create':grants.remove('ccs:task:create')
        if damage=='domain':grants.remove('test:write')
        if damage=='principal':e.authority.provision_principal(p,grants)
        else:e.authority.update_principal(p,permissions=grants)
        c=e.authority.issue_credential(p);e.session,i=e.store.login(c.token);e.csrf=i['csrf_token']
    elif damage=='csrf':e.csrf='0'*64
    elif damage=='session':e.store.logout(e.session,e.csrf)
    elif damage=='voice':e.voice.invalidate()
    else:e.service.retain_reviews=False
    with pytest.raises((PolicyError,ProposalAdmissionError)):replacement(e,descriptor)
    assert claims(e)==[] and counts(e)['task_checkpoints']==1


def test_reauthorization_of_candidate_preserves_original_replacement_lineage(cancelled):
    e=cancelled;new=replacement(e);b=new['binding']
    r=e.service.reauthorize_session(b['review_id'],b['digest'],e.template.workflow.workflow_id,e.args,session=e.session,csrf=e.csrf)
    current=e.service.view_session(r.review_id,session=e.session,csrf=e.csrf)
    assert current['binding']['replaces_task']==b['replaces_task']
    with pytest.raises(PolicyError):admit(e,new)
    admit(e,current)
    assert len(claims(e))==1


def test_checkpoint_restores_with_linked_lineage_without_execution(cancelled):
    e=cancelled;new=replacement(e);receipt=admit(e,new)
    task=e.manager.restore_task(receipt.task_id,list(e.template.steps),workflow=e.template.workflow)
    assert task.task_id==receipt.task_id and e.effects==[] and counts(e)['actions']==0


def test_new_credential_can_review_cancelled_origin_without_restoring_old_authority(cancelled):
    e=cancelled;before=e.journal.load_task(e.original_id)
    e.authority.revoke_credential(e.browser_credential.id)
    current=e.authority.issue_credential('browser')
    e.session,i=e.store.login(current.token);e.csrf=i['csrf_token']
    new=replacement(e)
    assert new['binding']['reviewer']['credential_id']==current.id
    assert counts(e)['task_checkpoints']==1 and claims(e)==[]
    admit(e,new)
    assert e.journal.load_task(e.original_id)==before and len(claims(e))==1


@pytest.mark.parametrize('damage',['claim','digest','original','lineage'])
def test_corrupt_replacement_claim_or_origin_fails_readonly_preflight(cancelled,damage):
    e=cancelled;new=replacement(e);admit(e,new)
    with e.journal._transaction() as db:
        if damage=='claim':db.execute('DELETE FROM task_replacements')
        elif damage=='digest':db.execute('UPDATE task_replacements SET original_digest=?',('0'*64,))
        elif damage=='original':db.execute("UPDATE task_checkpoints SET version=version+1 WHERE task_id=?",(e.original_id,))
        else:
            v,raw=db.execute('SELECT version,checkpoint FROM task_checkpoints WHERE task_id=?',(new['binding']['task_id'],)).fetchone();cp=json.loads(raw)
            cp['proposal_binding']['replaces_task']['task_id']='wrong'
            db.execute('UPDATE task_checkpoints SET checkpoint=? WHERE task_id=?',(canonical(cp),new['binding']['task_id']))
    before=e.journal.path.read_bytes();report=recovery_preflight.inspect(e.journal.path)
    assert not report['passed'] and e.journal.path.read_bytes()==before


def test_replacement_backup_preserves_original_claim_without_activation(cancelled,tmp_path):
    e=cancelled;admit(e,replacement(e));target=tmp_path/'replacement.ccsb'
    assert journal_backup.capture(e.journal.path,target)['passed']
    report=journal_backup.drill(target,tmp_path)
    assert report['passed'] and report['counts']['task_replacements']==1
    assert not report['restore_authorized'] and not report['dispatch_authorized']


def test_http_payloads_and_origin_cannot_inject_replacement_arguments(cancelled):
    e=cancelled;descriptor=inspect(e)['original'];payload={'task_id':e.original_id,'checkpoint_digest':descriptor['digest']}
    async def scenario():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=e.app),base_url=ORIGIN) as client:
            headers={'Origin':ORIGIN,'X-CCS-CSRF':e.csrf}
            assert (await client.post('/api/proposals/replacement',json=payload,headers=headers)).status_code==401
            client.cookies.set('ccs_operator',e.session)
            for h in ({'Origin':'https://evil.example'},{'X-CCS-CSRF':''}):
                assert (await client.post('/api/proposals/replacement',json=payload,headers={**headers,**h})).status_code==403
            for bad in ({**payload,'arguments':e.args},{**payload,'workflow':'injected'},{'task_id':[]}):
                assert (await client.post('/api/proposals/replacement',json=bad,headers=headers)).status_code==400
            result=await client.post('/api/proposals/replacement',json=payload,headers=headers)
            assert result.status_code==200 and not result.json()['admitted'] and result.headers['cache-control']=='no-store'
    asyncio.run(scenario())


def test_two_journal_connections_race_for_one_original_claim(cancelled):
    e=cancelled;candidates=[replacement(e),replacement(e)]
    checkpoints=[json.loads(e.service._pending[v['binding']['review_id']].checkpoint) for v in candidates]
    start=threading.Barrier(2)
    def accept(cp):
        journal=SQLiteActionJournal(e.journal.path,clock_ms=lambda:e.clock.now);journal.bind_lease(e.journal._lease)
        start.wait(timeout=10)
        try:journal.admit_task(cp,e.browser_credential.token);return 'admitted'
        except PolicyError:return 'rejected'
    with ThreadPoolExecutor(max_workers=2) as pool:
        results=list(pool.map(accept,checkpoints))
    assert sorted(results)==['admitted','rejected'] and len(claims(e))==1
    assert counts(e)==dict(task_checkpoints=2,task_events=3,actions=0,approvals=0)


def test_origin_change_before_publication_prevents_even_new_review_delivery(cancelled,monkeypatch):
    e=cancelled;before=counts(e);descriptor=inspect(e)['original'];original=e.manager.prepare_checkpoint
    def change(*args,**kwargs):
        v,cp=e.journal.load_task(e.original_id);cp['error']='SIMULATED origin changed'
        e.journal.save_task(e.original_id,cp,v)
        return original(*args,**kwargs)
    monkeypatch.setattr(e.manager,'prepare_checkpoint',change)
    with pytest.raises(PolicyError):replacement(e,descriptor)
    assert claims(e)==[] and counts(e)['task_checkpoints']==before['task_checkpoints']
    assert len(e.service._pending)==1


def test_missing_claim_blocks_repeated_acceptance_without_recreating_task(cancelled):
    e=cancelled;new=replacement(e);admit(e,new);before=counts(e)
    with e.journal._transaction() as db:db.execute('DELETE FROM task_replacements')
    with pytest.raises(PolicyError):admit(e,new)
    assert counts(e)==before


@pytest.mark.parametrize('state',['IN_FLIGHT_UNKNOWN','COMPENSATED'])
def test_action_record_without_slot_still_blocks_replacement(original,state):
    e=original;e.manager._saga.stage_write('move',e.args[0],owner_task_id=e.original_id,owner_step_index=0,owner_generation=0)
    cancel(e)
    # Adversarial corrupt metadata: removing a slot cannot hide its action.
    with e.journal._transaction() as db:
        db.execute('DELETE FROM operation_slots WHERE task_id=?',(e.original_id,))
        db.execute("UPDATE actions SET record=json_set(record,'$.state',?)",(state,))
    assert inspect(e)['reason']=='RECORDED_ACTIVITY'


def test_schema9_migration_is_explicit_offline_and_atomic(cancelled,monkeypatch):
    e=cancelled;before=e.journal.load_task(e.original_id)
    with closing(sqlite3.connect(e.journal.path)) as db:db.execute('DROP TABLE task_replacements');db.execute('PRAGMA user_version=9');db.commit()
    with pytest.raises(JournalError):SQLiteActionJournal(e.journal.path,clock_ms=lambda:e.clock.now)
    with pytest.raises(JournalError):SQLiteActionJournal(e.journal.path,clock_ms=lambda:e.clock.now,migrate=True)
    e.clock.now+=3600000
    actual=policy.create_schema
    def fail(db):actual(db);raise RuntimeError('SIMULATED migration interruption')
    monkeypatch.setattr(policy,'create_schema',fail)
    with pytest.raises(RuntimeError):SQLiteActionJournal(e.journal.path,clock_ms=lambda:e.clock.now,migrate=True)
    with closing(sqlite3.connect(e.journal.path)) as db:
        assert db.execute('PRAGMA user_version').fetchone()[0]==9
        assert db.execute("SELECT 1 FROM sqlite_master WHERE name='task_replacements'").fetchone() is None
    monkeypatch.setattr(policy,'create_schema',actual)
    migrated=SQLiteActionJournal(e.journal.path,clock_ms=lambda:e.clock.now,migrate=True)
    assert migrated.load_task(e.original_id)==before and recovery_preflight.inspect(e.journal.path)['passed']


def test_legacy_header_cannot_hide_replacement_binding_semantics(cancelled):
    e=cancelled;admit(e,replacement(e))
    with closing(sqlite3.connect(e.journal.path)) as db:db.execute('DROP TABLE task_replacements');db.execute('PRAGMA user_version=9');db.commit()
    assert not recovery_preflight.inspect(e.journal.path)['passed']
