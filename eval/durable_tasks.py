"""SIMULATED three-step task/voice isolation drills using the production runtime."""
import argparse
import asyncio
from contextlib import closing
import copy
import hashlib
import json
import logging
import os
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace

from eval.mutation_comparison import canonical, provenance
from runtime.action_journal import SQLiteActionJournal
from runtime.grounding_guard import TokenEvidence
from runtime.speculative_saga import SpeculativeSagaManager, TurnEpochClock
from runtime.task_engine import TaskManager, TaskStep
from runtime.tool_contract import ToolManifest, ToolRegistry
from runtime.voice_ingress import VoiceIngress
from runtime.worker_supervisor import WorkerSupervisor
from runtime.workflow_contract import StepVersions, WorkflowVersion


CASES = ('normal', 'voice_churn', 'consumer_wait_cancelled', 'correction_second',
         'lost_second', 'cancel_second', 'restart_unknown_second', 'voice_epoch_ablation')
FAULTS = {'correction_second','lost_second','cancel_second','restart_unknown_second'}
COMMITTED, PARKED, PENDING = 'COMMITTED', 'NEEDS_RECONCILIATION', 'PENDING'
# Frozen protocol expectations, not inferred from each run's observed results.
EXPECTED = {
    **{case: {'task_state':'COMPLETED','step_states':[COMMITTED]*3,'index':3,
              'effects':[1,1,1],'action_states':[COMMITTED]*3,'epoch':0}
       for case in ('normal','voice_churn','consumer_wait_cancelled')},
    **{case: {'task_state':PARKED,'step_states':[COMMITTED,PARKED,PENDING],'index':1,
              'effects':[1,1,0],'action_states':[COMMITTED,'COMMITTED_STALE',None],'epoch':1}
       for case in ('correction_second','voice_epoch_ablation')},
    'lost_second': {'task_state':PARKED,'step_states':[COMMITTED,PARKED,PENDING],'index':1,
                    'effects':[1,1,0],'action_states':[COMMITTED,'IN_FLIGHT_UNKNOWN',None],'epoch':0},
    'cancel_second': {'task_state':'CANCELLED','step_states':[COMMITTED,PARKED,PENDING],'index':1,
                      'effects':[1,1,0],'action_states':[COMMITTED,'COMMITTED_STALE',None],'epoch':0},
    'restart_unknown_second': {'task_state':PARKED,'step_states':[PARKED,PARKED,PENDING],'index':0,
                               'effects':[1,1,0],'action_states':['COMMITTED_STALE','IN_FLIGHT_UNKNOWN',None],'epoch':0},
}
METRICS = ('duplicate_effects','blocked_descendant_effects','false_task_completions',
           'false_abort_claims','lost_prior_evidence','voice_independence_violations')
WORKFLOW = WorkflowVersion('SIMULATED:durable-task-evaluation','v1')


def arguments(_task, index):
    return {'step':index}


def definitions():
    return [TaskStep('SIMULATED_change',build_args=arguments,
                     versions=StepVersions(f'step-{i}','v1','SIMULATED:v1','SIMULATED:v1','local-effect-ledger'))
            for i in range(3)]


class Effects:
    """Separate persisted effects; every invocation applies with no deduplication."""
    def __init__(self,path,lose_receipt=False):
        self.path, self.lose_receipt = path, lose_receipt
        self.applied, self.release = asyncio.Event(), asyncio.Event()
        with closing(sqlite3.connect(path)) as db, db:
            db.execute('PRAGMA synchronous=EXTRA')
            db.execute('CREATE TABLE effects(seq INTEGER PRIMARY KEY, step INTEGER NOT NULL CHECK(step BETWEEN 0 AND 2))')

    async def execute(self,args):
        if (type(args) is not dict or set(args)!={'step'} or
                type(args['step']) is not int or not 0<=args['step']<=2):
            raise ValueError('Invalid simulated operation')
        step=args['step']
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('PRAGMA synchronous=EXTRA')
            db.execute('INSERT INTO effects(step) VALUES (?)',(step,))
        if step==1:
            self.applied.set()  # Effect is durable before the response await.
            await self.release.wait()
            if self.lose_receipt: raise TimeoutError('SIMULATED lost receipt')
        return {'SIMULATED_receipt':step}

    def counts(self):
        with closing(sqlite3.connect(self.path)) as db:
            rows=db.execute('SELECT step FROM effects ORDER BY seq').fetchall()
        if any(type(r[0]) is not int or not 0<=r[0]<=2 for r in rows): raise ValueError('Invalid effect ledger')
        return [sum(r[0]==i for r in rows) for i in range(3)]


class Bindings(TaskManager):
    """Only trusted executor binding; production run/resume/cancel logic inherited."""
    def __init__(self,saga,effects):
        super().__init__(saga,lambda _:None)
        self.effects=effects

    def _get_executor(self,step):
        return self.effects.execute


def voice_churn():
    ingress=VoiceIngress()
    stream=ingress.open_stream()
    handle=ingress.begin_turn(stream)
    ingress.replace(handle,1,'yes cancel',
                    (TokenEvidence('yes',.99,0.,.4),TokenEvidence('cancel',.99,.5,.9)),final=False)
    ingress.replace(handle,2,'continue', (TokenEvidence('continue',.99,0.,.4),),final=True)
    ingress.complete(handle,'continue')
    ingress.begin_turn(stream)
    ingress.close_stream(stream)


def judge(case,observation,identity_preserved,prior_receipt_preserved):
    if case not in CASES or type(observation) is not dict: raise ValueError('Unknown task observation')
    counts=observation.get('effects')
    states=observation.get('action_states')
    if (type(counts) is not list or len(counts)!=3 or any(type(v) is not int or v<0 for v in counts) or
            type(states) is not list or len(states)!=3 or
            any(v is not None and (type(v) is not str or v not in {'COMMITTED','COMMITTED_STALE','IN_FLIGHT_UNKNOWN','ABORTED'}) for v in states) or
            type(observation.get('epoch')) is not int or observation['epoch']<0 or
            type(observation.get('task_state')) is not str or observation['task_state'] not in {'COMPLETED',PARKED,'CANCELLED'} or
            type(identity_preserved) is not bool or type(prior_receipt_preserved) is not bool):
        raise ValueError('Invalid task evidence')
    return {'duplicate_effects':sum(max(0,v-1) for v in counts),
            'blocked_descendant_effects':counts[2] if case in FAULTS else 0,
            'false_task_completions':int(observation['task_state']=='COMPLETED' and (case in FAULTS or counts!=[1,1,1])),
            'false_abort_claims':sum(s=='ABORTED' and counts[i]>0 for i,s in enumerate(states)),
            'lost_prior_evidence':int(not identity_preserved or not prior_receipt_preserved or counts[0]!=1),
            'voice_independence_violations':int(case in {'voice_churn','voice_epoch_ablation'} and observation['epoch']!=0)}


async def run_case(directory,case):
    if case not in CASES: raise ValueError('Unknown task schedule')
    directory.mkdir(mode=0o700)
    effects=Effects(directory/'effects.db',case in {'lost_second','restart_unknown_second'})
    path=directory/'journal.db'
    def journal(): return SQLiteActionJournal(path,clock_ms=lambda:1_000_000)
    initial=journal()
    initial.security.provision_principal('SIMULATED-task-worker',{'ccs:work','eval:write'})
    credential=initial.security.issue_credential('SIMULATED-task-worker')
    def factory(owned,auth):
        registry=ToolRegistry()
        registry.register(ToolManifest('SIMULATED_change','write',required_permissions=['eval:write']))
        saga=SpeculativeSagaManager(TurnEpochClock(),registry=registry,journal=owned,auth_context=auth)
        return SimpleNamespace(saga=saga,tasks=Bindings(saga,effects))
    host=WorkerSupervisor(initial,credential.token)
    operation=None
    completed=None
    consumer_cancelled=False
    stop_reports=[]
    try:
        runtime=await host.start(factory)
        task=runtime.tasks.create_task('SIMULATED three-step objective',definitions(),workflow=WORKFLOW)
        operation=host.submit(lambda _:runtime.tasks.run_task(task.task_id))
        async with asyncio.timeout(20):
            # Surface dispatch failures without waiting forever at a barrier.
            reached=asyncio.create_task(effects.applied.wait())
            consumer_entered=asyncio.Event()
            async def consume():
                consumer_entered.set()
                return await operation.wait()
            completed=asyncio.create_task(consume())
            await consumer_entered.wait()
            try:
                done,_=await asyncio.wait((reached,completed),return_when=asyncio.FIRST_COMPLETED)
                if completed in done: await completed; raise ValueError('Task did not reach second-effect barrier')
            finally:
                reached.cancel()
                await asyncio.gather(reached,return_exceptions=True)
            original_id=task.task_id
            original_actions=tuple(task.step_action_ids[:2])
            original_generations=tuple(task.step_generations[:2])
            prior_result=copy.deepcopy(task.step_results[0])
            if case in {'voice_churn','voice_epoch_ablation'}:
                voice_churn()
                if case=='voice_epoch_ablation': await runtime.saga.epoch_clock.advance('SIMULATED incorrect voice coupling')
            elif case=='correction_second': await runtime.saga.epoch_clock.advance('SIMULATED trusted task invalidation')
            elif case=='cancel_second':
                await host.submit(lambda _:runtime.tasks.cancel_task(task.task_id)).wait()
            elif case=='consumer_wait_cancelled':
                completed.cancel()
                try: await completed
                except asyncio.CancelledError: consumer_cancelled=True
                if operation.done(): raise ValueError('Consumer cancellation ended durable operation')
            effects.release.set()
            await operation.wait()
            if case=='restart_unknown_second':
                stopped=await host.stop()
                stop_reports.append(stopped.drained and stopped.lease_released)
                if not stop_reports[-1]:
                    raise ValueError('Original worker lifetime unverified; reconstruction denied')
                host=WorkerSupervisor(journal(),credential.token)
                runtime=await host.start(factory)
                task=runtime.tasks.restore_task(original_id,steps=definitions(),workflow=WORKFLOW)
                if task is None: raise ValueError('Durable task missing')
            # A second run/resume cannot reset uncertain action identity or replay.
            await host.submit(lambda _:runtime.tasks.resume_task(task.task_id)).wait()
            observation={'task_state':task.state.name,'step_states':[s.name for s in task.step_states],
                         'index':task.current_step_index,'effects':effects.counts(),
                         'action_states':[runtime.saga._actions[a].state.name if a else None for a in task.step_action_ids],
                         'epoch':runtime.saga.epoch_clock.current}
            identity=(task.task_id==original_id and tuple(task.step_action_ids[:2])==original_actions and
                      tuple(task.step_generations[:2])==original_generations)
            prior=task.step_results[0]==prior_result=={'SIMULATED_receipt':0}
            metrics=judge(case,observation,identity,prior)
        stopped=await host.stop()
        stop_reports.append(stopped.drained and stopped.lease_released)
        return {'case':case,**observation,**metrics,'identity_preserved':identity,
                'prior_receipt_preserved':prior,'workers_drained_and_released':all(stop_reports),
                'consumer_wait_cancelled':consumer_cancelled,
                'expectation_matched':observation==EXPECTED[case] and (case!='consumer_wait_cancelled' or consumer_cancelled)}
    finally:
        effects.release.set()
        # Stop drains owned operations before temporary databases can disappear.
        await host.stop()
        if completed is not None:
            if not completed.done(): completed.cancel()
            await asyncio.gather(completed,return_exceptions=True)


async def evaluate():
    rows=[]
    # A failed drain or exception cannot authorize deletion beneath owned work.
    # Keep the private synthetic evidence on abnormal exit for investigation.
    temporary=tempfile.TemporaryDirectory(prefix='ccs-task-evaluation-',delete=False)
    with temporary as directory:
        for case in CASES: rows.append(await run_case(Path(directory)/case,case))
    measured=[r for r in rows if r['case']!='voice_epoch_ablation']
    totals={m:sum(r[m] for r in measured) for m in METRICS}
    negative=rows[-1]['voice_independence_violations']==1
    passed=(not any(totals.values()) and negative and
            all(r['expectation_matched'] and r['workers_drained_and_released'] for r in rows))
    if all(r['workers_drained_and_released'] for r in rows): temporary.cleanup()
    return {'schema':1,'label':'SIMULATED','scope':'three_step_durable_task','passed':passed,
            'plan_sha256':hashlib.sha256(canonical({'cases':CASES,'expected':EXPECTED,'metrics':METRICS})).hexdigest(),
            'case_count':len(rows),'full_runtime_cases':len(measured),'negative_control_detected':negative,
            'completed_tasks':sum(r['task_state']=='COMPLETED' for r in measured),
            'parked_tasks':sum(r['task_state']==PARKED for r in measured),
            'cancelled_tasks':sum(r['task_state']=='CANCELLED' for r in measured),
            'violations':totals,'observations':rows,'real_provider':False,'real_audio':False,'latency_measured':False}


async def experiment():
    before=provenance()
    report=await evaluate()
    stable=before==provenance() and before['clean_checkout_verified']
    report.update(source=before,source_stable=stable)
    report['passed']=report['passed'] and stable
    return report


def main():
    parser=argparse.ArgumentParser(description=__doc__,allow_abbrev=False)
    parser.add_argument('--output',required=True)
    args=parser.parse_args()
    logging.disable(logging.CRITICAL)
    try:
        if os.path.lexists(args.output): raise ValueError('Existing evidence')
        report=asyncio.run(experiment())
        with Path(args.output).open('x',encoding='utf-8') as stream:
            stream.write(json.dumps(report,sort_keys=True,indent=2,allow_nan=False)+'\n')
        print(json.dumps({k:report[k] for k in ('label','passed','case_count','completed_tasks','parked_tasks','cancelled_tasks','negative_control_detected')}))
    except (OSError,ValueError,RuntimeError,sqlite3.Error):
        parser.exit(2,'Durable-task evaluation unverified; preserve evidence.\n')
    raise SystemExit(0 if report['passed'] else 1)


if __name__=='__main__': main()
