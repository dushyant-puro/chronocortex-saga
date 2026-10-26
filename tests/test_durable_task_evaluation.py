"""Real task/saga/supervisor boundary with independent SIMULATED effect evidence."""
import asyncio
import copy
from dataclasses import replace
from pathlib import Path
import subprocess
import sys

import pytest

from eval import durable_tasks as evaluation


@pytest.mark.asyncio
async def test_frozen_multistep_matrix_preserves_progress_and_separates_outcomes():
    report=await evaluation.evaluate()
    assert report==await evaluation.evaluate()
    assert report['passed'] and report['case_count']==8 and report['full_runtime_cases']==7
    assert (report['completed_tasks'],report['parked_tasks'],report['cancelled_tasks'])==(3,3,1)
    assert not any(report['violations'].values()) and report['negative_control_detected']
    assert not report['real_provider'] and not report['real_audio'] and not report['latency_measured']
    assert all(r['identity_preserved'] and r['prior_receipt_preserved'] and r['workers_drained_and_released'] for r in report['observations'])


@pytest.mark.asyncio
async def test_voice_coupling_ablation_exposes_task_independence_defect(tmp_path):
    isolated=await evaluation.run_case(tmp_path/'isolated','voice_churn')
    coupled=await evaluation.run_case(tmp_path/'coupled','voice_epoch_ablation')
    assert isolated['task_state']=='COMPLETED' and isolated['effects']==[1,1,1]
    assert coupled['task_state']=='NEEDS_RECONCILIATION' and coupled['effects']==[1,1,0]
    assert isolated['voice_independence_violations']==0 and coupled['voice_independence_violations']==1


@pytest.mark.asyncio
async def test_restart_preserves_receipt_but_does_not_restore_prior_dispatch_authority(tmp_path):
    result=await evaluation.run_case(tmp_path/'restart','restart_unknown_second')
    assert result['effects']==[1,1,0] and result['index']==0
    assert result['action_states']==['COMMITTED_STALE','IN_FLIGHT_UNKNOWN',None]
    assert result['step_states']==['NEEDS_RECONCILIATION','NEEDS_RECONCILIATION','PENDING']
    assert result['identity_preserved'] and result['prior_receipt_preserved']


@pytest.mark.asyncio
async def test_cancelled_task_is_intent_not_a_claim_of_remote_rollback(tmp_path):
    result=await evaluation.run_case(tmp_path/'cancel','cancel_second')
    assert result['task_state']=='CANCELLED' and result['effects']==[1,1,0]
    assert result['action_states'][1]=='COMMITTED_STALE' and result['false_abort_claims']==0


@pytest.mark.asyncio
async def test_cancellation_of_an_active_consumer_wait_does_not_stop_owned_work(tmp_path):
    result=await evaluation.run_case(tmp_path/'consumer','consumer_wait_cancelled')
    assert result['consumer_wait_cancelled'] and result['task_state']=='COMPLETED'
    assert result['effects']==[1,1,1] and result['workers_drained_and_released']


@pytest.mark.asyncio
async def test_external_effect_survives_executor_cancellation(tmp_path):
    effects=evaluation.Effects(tmp_path/'effects.db')
    execute=asyncio.create_task(effects.execute({'step':1}))
    await asyncio.wait_for(effects.applied.wait(),5)
    execute.cancel()
    with pytest.raises(asyncio.CancelledError): await execute
    assert effects.counts()==[0,1,0]


@pytest.mark.parametrize('mutation,metric',[
    ({'effects':[1,2,0]},'duplicate_effects'),
    ({'effects':[1,1,1]},'blocked_descendant_effects'),
    ({'task_state':'COMPLETED'},'false_task_completions'),
    ({'action_states':['COMMITTED','ABORTED',None]},'false_abort_claims'),
])
def test_independent_oracle_exposes_false_local_outcomes(mutation,metric):
    observation=copy.deepcopy(evaluation.EXPECTED['lost_second']); observation.update(mutation)
    assert evaluation.judge('lost_second',observation,True,True)[metric]>0


@pytest.mark.parametrize('identity,receipt',[(False,True),(True,False),(False,False)])
def test_lost_identity_or_prior_receipt_fails_progress_invariant(identity,receipt):
    assert evaluation.judge('lost_second',evaluation.EXPECTED['lost_second'],identity,receipt)['lost_prior_evidence']==1


@pytest.mark.parametrize('counts',[[True,1,0],[1,-1,0],[1,1],[1,1,0,0]])
def test_malformed_effect_counts_are_not_scored(counts):
    observation=copy.deepcopy(evaluation.EXPECTED['normal']); observation['effects']=counts
    with pytest.raises(ValueError): evaluation.judge('normal',observation,True,True)


@pytest.mark.asyncio
async def test_frozen_expectation_mismatch_fails_evaluation(monkeypatch):
    expected=copy.deepcopy(evaluation.EXPECTED)
    expected['normal']['effects']=[0,0,0]
    monkeypatch.setattr(evaluation,'EXPECTED',expected)
    assert not (await evaluation.evaluate())['passed']


@pytest.mark.asyncio
async def test_dirty_provenance_cannot_yield_passing_experiment(monkeypatch):
    monkeypatch.setattr(evaluation,'provenance',lambda:{'revision':'a'*40,'clean_checkout_verified':False})
    report=await evaluation.experiment()
    assert not report['passed'] and not report['source_stable']


@pytest.mark.asyncio
@pytest.mark.parametrize('failure',['exception','unverified_drain'])
async def test_unverified_worker_lifetime_preserves_private_evidence(tmp_path,monkeypatch,failure):
    roots=[]
    temporary=evaluation.tempfile.TemporaryDirectory
    original=evaluation.run_case
    def create(*args,**kwargs):
        kwargs['dir']=tmp_path
        result=temporary(*args,**kwargs)
        roots.append(Path(result.name))
        return result
    async def fail(directory,case):
        if failure=='exception': raise RuntimeError('SIMULATED harness failure')
        row=await original(directory,case)
        row['workers_drained_and_released']=False
        return row
    monkeypatch.setattr(evaluation.tempfile,'TemporaryDirectory',create)
    monkeypatch.setattr(evaluation,'run_case',fail)
    if failure=='exception':
        with pytest.raises(RuntimeError): await evaluation.evaluate()
    else: assert not (await evaluation.evaluate())['passed']
    assert len(roots)==1 and roots[0].is_dir()


@pytest.mark.asyncio
async def test_reconstruction_does_not_start_a_second_host_after_unverified_stop(tmp_path,monkeypatch):
    started=[]
    start=evaluation.WorkerSupervisor.start
    stop=evaluation.WorkerSupervisor.stop
    async def observe(self,*args,**kwargs):
        started.append(self)
        return await start(self,*args,**kwargs)
    async def unverified(self,*args,**kwargs):
        report=await stop(self,*args,**kwargs)
        return replace(report,drained=False,lease_released=False)
    monkeypatch.setattr(evaluation.WorkerSupervisor,'start',observe)
    monkeypatch.setattr(evaluation.WorkerSupervisor,'stop',unverified)
    directory=tmp_path/'reconstruction'
    with pytest.raises(ValueError,match='reconstruction denied'):
        await evaluation.run_case(directory,'restart_unknown_second')
    assert len(started)==1
    assert (directory/'journal.db').is_file() and (directory/'effects.db').is_file()
    with evaluation.closing(evaluation.sqlite3.connect(directory/'effects.db')) as db:
        assert db.execute('SELECT step FROM effects ORDER BY seq').fetchall()==[(0,),(1,)]


def test_cli_never_overwrites_an_existing_report(tmp_path):
    prior=tmp_path/'prior.json'; prior.write_text('PRIOR EVIDENCE')
    result=subprocess.run([sys.executable,'-m','eval.durable_tasks','--output',str(prior)],capture_output=True,text=True,timeout=10)
    assert result.returncode==2 and not result.stdout and prior.read_text()=='PRIOR EVIDENCE'
    assert 'Traceback' not in result.stderr
