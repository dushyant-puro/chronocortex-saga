"""Independent oracle/adversarial checks for the SIMULATED comparison harness."""
import asyncio
import copy
import json
from pathlib import Path
import subprocess
import sys

import pytest

from eval import mutation_comparison as comparison


@pytest.mark.asyncio
async def test_complete_matrix_is_deterministic_and_separates_parking_from_completion():
    first=await comparison.evaluate()
    second=await comparison.evaluate()
    assert first==second and first['passed'] and first['cell_count']==20
    assert first['negative_controls_detected'] and first['ccs_safety_checks_passed']
    assert not first['task_completion_measured'] and not first['latency_measured']
    assert not first['real_provider'] and not first['real_audio']
    rows=[r for r in first['observations'] if r['policy']=='ccs']
    assert [r['state'] for r in rows]==['COMMITTED','ABORTED','COMMITTED_STALE','IN_FLIGHT_UNKNOWN','IN_FLIGHT_UNKNOWN']
    assert [r['effects'] for r in rows]==[1,0,1,1,1]
    assert sum(r['safely_parked'] for r in rows)==3
    assert first['summaries']['ccs_without_epoch_signal']['false_current_commitments']==2
    assert first['summaries']['restart_on_interruption']['duplicate_effects']==3
    assert first['summaries']['cancel_means_abort']['false_aborts']==3


@pytest.mark.asyncio
async def test_missing_epoch_notification_fails_current_validity_with_same_real_kernel(tmp_path):
    ablated=await comparison.run_case(tmp_path/'ablated','correction_after_effect','ccs_without_epoch_signal')
    full=await comparison.run_case(tmp_path/'full','correction_after_effect','ccs')
    assert ablated['effects']==full['effects']==1
    assert ablated['false_current_commitments']==1 and full['false_current_commitments']==0
    assert full['safely_parked'] and not ablated['safely_parked']


@pytest.mark.asyncio
async def test_effect_oracle_is_independent_of_local_cancellation(tmp_path):
    endpoint=comparison.EffectLedger(tmp_path/'effects.db')
    task=asyncio.create_task(endpoint.execute({}))
    await asyncio.wait_for(endpoint.applied.wait(),5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError): await task
    assert endpoint.evidence()=={'effects':1,'stale_dispatches':0}
    judged=comparison.judge('cancel_after_effect','ABORTED',endpoint.evidence(),False)
    assert judged['false_aborts']==1


@pytest.mark.parametrize('case,state,evidence,metric',[
    ('correction_before_dispatch','COMMITTED',{'effects':1,'stale_dispatches':1},'stale_dispatches'),
    ('correction_after_effect','COMMITTED',{'effects':1,'stale_dispatches':0},'false_current_commitments'),
    ('lost_receipt_restart','ABORTED',{'effects':1,'stale_dispatches':0},'false_aborts'),
    ('lost_receipt_restart','COMMITTED',{'effects':2,'stale_dispatches':0},'duplicate_effects'),
    ('normal','COMMITTED',{'effects':0,'stale_dispatches':0},'false_current_commitments'),
])
def test_oracle_rejects_false_local_success_and_nonexecution_claims(case,state,evidence,metric):
    assert comparison.judge(case,state,evidence,False)[metric]>0


@pytest.mark.parametrize('evidence',[
    {'effects':True,'stale_dispatches':0}, {'effects':-1,'stale_dispatches':0},
    {'effects':0,'stale_dispatches':1}, {'effects':1,'stale_dispatches':0,'private':'payload'},
])
def test_malformed_external_evidence_is_not_scored(evidence):
    with pytest.raises(ValueError): comparison.judge('normal','COMMITTED',evidence,False)


@pytest.mark.asyncio
@pytest.mark.parametrize('count',[0,11,True,1.5])
async def test_repetition_capacity_is_bounded(count):
    with pytest.raises(ValueError): await comparison.evaluate(count)


@pytest.mark.asyncio
async def test_unexpected_control_outcomes_fail_the_matrix(monkeypatch):
    expected=copy.deepcopy(comparison.EXPECTED)
    expected['normal'][0]=('ABORTED',0)
    monkeypatch.setattr(comparison,'EXPECTED',expected)
    assert not (await comparison.evaluate())['passed']


@pytest.mark.asyncio
async def test_lost_receipt_reopens_and_blocks_replay_through_production_manager(tmp_path):
    result=await comparison.run_case(tmp_path/'case','lost_receipt_restart','ccs')
    assert result['replay_blocked'] and result['effects']==1 and result['state']=='IN_FLIGHT_UNKNOWN'
    assert not result['false_aborts'] and result['safely_parked']


def test_unavailable_source_provenance_fails_closed(monkeypatch):
    def unavailable(*a,**k): raise OSError('PRIVATE git detail')
    monkeypatch.setattr(comparison.subprocess,'run',unavailable)
    assert comparison.provenance()=={'revision':None,'clean_checkout_verified':False}


@pytest.mark.asyncio
@pytest.mark.parametrize('condition',['dirty','changed','unavailable'])
async def test_unstable_or_unverified_checkout_cannot_produce_passing_experiment(monkeypatch,condition):
    first={'revision':'a'*40,'clean_checkout_verified':condition!='dirty'}
    second=dict(first)
    if condition=='changed': second['revision']='b'*40
    if condition=='unavailable': first=second={'revision':None,'clean_checkout_verified':False}
    calls=iter((first,second))
    monkeypatch.setattr(comparison,'provenance',lambda: next(calls))
    report=await comparison.experiment()
    assert not report['passed'] and not report['source_stable']
    assert report['ccs_safety_checks_passed'] and report['source']==first


def test_cli_refuses_overwriting_evidence_and_invalid_parameters(tmp_path):
    report=tmp_path/'prior.json'; report.write_text('PRIOR EVIDENCE')
    result=subprocess.run([sys.executable,'-m','eval.mutation_comparison','--output',str(report)],capture_output=True,text=True,timeout=90)
    assert result.returncode==2 and report.read_text()=='PRIOR EVIDENCE'
    assert not result.stdout and 'Traceback' not in result.stderr
    invalid=subprocess.run([sys.executable,'-m','eval.mutation_comparison','--output',str(tmp_path/'absent'),'--repetitions','0'],capture_output=True,text=True)
    assert invalid.returncode==2 and not (tmp_path/'absent').exists()
