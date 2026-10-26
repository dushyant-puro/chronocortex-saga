"""Bounded SIMULATED policy comparison; no providers, audio or latency claims."""
import argparse
import asyncio
from contextlib import closing
import hashlib
import json
import logging
import os
from pathlib import Path
import platform
import sqlite3
import subprocess
import tempfile

from runtime.action_journal import JournalError, SQLiteActionJournal
from runtime.policy_engine import AuthorizationContext
from runtime.speculative_saga import (SagaAbortedError, SpeculativeSagaManager,
                                      StaleEpochError, TurnEpochClock)
from runtime.tool_contract import ToolManifest, ToolRegistry


POLICIES = ('restart_on_interruption', 'cancel_means_abort', 'ccs_without_epoch_signal', 'ccs')
CASES = ('normal', 'correction_before_dispatch', 'correction_after_effect',
         'cancel_after_effect', 'lost_receipt_restart')
# Frozen before execution, independent of runtime states returned by each policy.
# Columns follow POLICIES; each cell is (classification, external effect count).
EXPECTED = {
    'normal': [('COMMITTED', 1)] * 4,
    'correction_before_dispatch': [('COMMITTED', 1), ('ABORTED', 0), ('COMMITTED', 1), ('ABORTED', 0)],
    'correction_after_effect': [('COMMITTED', 2), ('ABORTED', 1), ('COMMITTED', 1), ('COMMITTED_STALE', 1)],
    'cancel_after_effect': [('COMMITTED', 2), ('ABORTED', 1), ('IN_FLIGHT_UNKNOWN', 1), ('IN_FLIGHT_UNKNOWN', 1)],
    'lost_receipt_restart': [('COMMITTED', 2), ('ABORTED', 1), ('IN_FLIGHT_UNKNOWN', 1), ('IN_FLIGHT_UNKNOWN', 1)],
}
METRICS = ('duplicate_effects', 'stale_dispatches', 'false_current_commitments', 'false_aborts')
ROOT = Path(__file__).resolve().parents[1]


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


class EffectLedger:
    """Independent SQLite truth: every attempt applies; intentionally no dedup."""
    def __init__(self, path, lose_receipt=False):
        self.path, self.lose_receipt = path, lose_receipt
        self.valid = True
        self.applied, self.release = asyncio.Event(), asyncio.Event()
        with closing(sqlite3.connect(path)) as db, db:
            db.execute('PRAGMA synchronous=EXTRA')
            db.execute('CREATE TABLE effects(seq INTEGER PRIMARY KEY, valid_at_dispatch INTEGER NOT NULL CHECK(valid_at_dispatch IN (0,1)))')

    async def execute(self, _args):
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('PRAGMA synchronous=EXTRA')
            attempt = db.execute('INSERT INTO effects(valid_at_dispatch) VALUES (?)', (int(self.valid),)).lastrowid
        # The effect commits independently BEFORE the local await/cancellation.
        self.applied.set()
        await self.release.wait()
        if self.lose_receipt and attempt == 1:
            raise TimeoutError('SIMULATED lost receipt')
        return {'receipt': 'SIMULATED', 'attempt': attempt}

    def evidence(self):
        # Reopen a separate connection. Never infer effects from local outcomes.
        with closing(sqlite3.connect(self.path)) as db:
            rows = db.execute('SELECT seq,valid_at_dispatch FROM effects ORDER BY seq').fetchall()
        if ([r[0] for r in rows] != list(range(1, len(rows)+1)) or
                any(type(r[1]) is not int or r[1] not in (0,1) for r in rows)):
            raise ValueError('Invalid effect ledger')
        return {'effects': len(rows), 'stale_dispatches': sum(r[1] == 0 for r in rows)}


class ToyPolicy:
    """Declared negative controls, never production adapters or vendor models."""
    def __init__(self, path, restart):
        self.path, self.restart = path, restart
        with closing(sqlite3.connect(path)) as db, db:
            db.execute('CREATE TABLE IF NOT EXISTS outcome(state TEXT NOT NULL)')
            if db.execute('SELECT count(*) FROM outcome').fetchone()[0] == 0:
                db.execute("INSERT INTO outcome VALUES ('PENDING')")

    @property
    def state(self):
        with closing(sqlite3.connect(self.path)) as db:
            return db.execute('SELECT state FROM outcome').fetchone()[0]

    def record(self, state):
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('UPDATE outcome SET state=?', (state,))

    async def commit(self, execute):
        if self.state != 'PENDING': return
        self.record('IN_FLIGHT')
        try:
            await execute({'target': 'SIMULATED immutable intent'})
        except (TimeoutError, asyncio.CancelledError):
            # Deliberately unsafe: local cancellation/error becomes nonexecution.
            self.record('ABORTED')
            raise
        self.record('COMMITTED')

    async def correction(self):
        if not self.restart and self.state == 'PENDING': self.record('ABORTED')

    async def recover(self, execute):
        reopened = ToyPolicy(self.path, self.restart)
        if self.restart:
            reopened.record('PENDING')
            await reopened.commit(execute)  # Intentionally unsafe blind replay.
        return False


class CCS:
    """Production manager, journal, lease and credentials; no method overrides."""
    def __init__(self, path, observe_epoch):
        self.path, self.observe_epoch = path, observe_epoch
        self.journal = self.open_journal()
        self.journal.security.provision_principal('SIMULATED-evaluator', {'eval:write'})
        self.context = AuthorizationContext(credential=self.journal.security.issue_credential('SIMULATED-evaluator').token)
        self.registry = ToolRegistry()
        self.registry.register(ToolManifest('simulated_change', 'write', required_permissions=['eval:write']))
        self.clock = TurnEpochClock()
        self.saga = self.manager()
        self.action_id = self.saga.stage_write('simulated_change', {'target': 'SIMULATED immutable intent'}).action_id

    def open_journal(self):
        journal = SQLiteActionJournal(self.path, clock_ms=lambda: 1_000_000)
        journal.acquire_lease('SIMULATED-evaluator', ttl_ms=60_000)
        return journal

    def manager(self):
        return SpeculativeSagaManager(self.clock, registry=self.registry,
                                      auth_context=self.context, journal=self.journal)

    @property
    def state(self):
        return self.journal.records()[0][1]['state']

    async def commit(self, execute):
        try:
            await self.saga.commit_write(self.action_id, execute, timeout=10)
        except (SagaAbortedError, StaleEpochError):
            if self.state != 'ABORTED': raise

    async def correction(self):
        # Controlled integration ablation: drop ONLY the epoch notification.
        if self.observe_epoch: await self.clock.advance('SIMULATED correction')

    async def recover(self, execute):
        # Reopen the journal and acquire a new lease generation after release.
        # This is controlled reconstruction, not an OS-crash/provider experiment.
        self.journal.release_lease()
        self.journal = self.open_journal()
        self.clock = TurnEpochClock()
        self.saga = self.manager()
        before = self.state
        try:
            await self.saga.commit_write(self.action_id, execute)
        except JournalError:
            return before == self.state == 'IN_FLIGHT_UNKNOWN'
        raise ValueError('Recovered intent unexpectedly dispatched')


def judge(case, state, evidence, replay_blocked):
    """Oracle inputs come from scenario intent and external durable effects."""
    if case not in CASES or state not in {'COMMITTED','ABORTED','COMMITTED_STALE','IN_FLIGHT_UNKNOWN'}:
        raise ValueError('Unsupported observation')
    if (set(evidence) != {'effects','stale_dispatches'} or
            any(type(v) is not int or v < 0 for v in evidence.values()) or
            evidence['stale_dispatches'] > evidence['effects'] or type(replay_blocked) is not bool):
        raise ValueError('Invalid evidence')
    effects = evidence['effects']
    invalidated = case in {'correction_before_dispatch','correction_after_effect','cancel_after_effect'}
    return {**evidence, 'duplicate_effects': max(0, effects-1),
            'false_current_commitments': int(state == 'COMMITTED' and (invalidated or effects == 0)),
            'false_aborts': int(state == 'ABORTED' and effects > 0),
            'safely_parked': state in {'COMMITTED_STALE','IN_FLIGHT_UNKNOWN'},
            'replay_blocked': replay_blocked}


async def run_case(directory, case, policy):
    if case not in CASES or policy not in POLICIES: raise ValueError('Unknown comparison cell')
    directory.mkdir(mode=0o700)
    endpoint = EffectLedger(directory/'effects.db', case == 'lost_receipt_restart')
    coordinator = (CCS(directory/'coordinator.db', policy == 'ccs') if policy.startswith('ccs')
                   else ToyPolicy(directory/'coordinator.db', policy == 'restart_on_interruption'))
    task = None
    replay_blocked = False
    try:
        async with asyncio.timeout(15):
            if case == 'correction_before_dispatch':
                endpoint.valid = False
                await coordinator.correction()
                endpoint.release.set()
                await coordinator.commit(endpoint.execute)
            else:
                task = asyncio.create_task(coordinator.commit(endpoint.execute))
                barrier = asyncio.create_task(endpoint.applied.wait())
                try:
                    done, _ = await asyncio.wait((task,barrier), return_when=asyncio.FIRST_COMPLETED)
                    if task in done: await task; raise ValueError('Dispatch did not reach effect barrier')
                finally:
                    barrier.cancel()
                    await asyncio.gather(barrier, return_exceptions=True)
                if case in {'correction_after_effect','cancel_after_effect'}:
                    endpoint.valid = False
                    if case == 'correction_after_effect': await coordinator.correction()
                    if case == 'cancel_after_effect' or isinstance(coordinator,ToyPolicy): task.cancel()
                endpoint.release.set()
                try:
                    await task
                except asyncio.CancelledError:
                    if case not in {'correction_after_effect','cancel_after_effect'}: raise
                except TimeoutError:
                    if case != 'lost_receipt_restart': raise
                if case == 'lost_receipt_restart' or (isinstance(coordinator,ToyPolicy) and coordinator.restart and case in {'correction_after_effect','cancel_after_effect'}):
                    replay_blocked = await coordinator.recover(endpoint.execute)
        evidence = endpoint.evidence()
        state = coordinator.state
        metrics = judge(case, state, evidence, replay_blocked)
        expected_state, expected_effects = EXPECTED[case][POLICIES.index(policy)]
        matches = state == expected_state and evidence['effects'] == expected_effects
        if policy.startswith('ccs') and case == 'lost_receipt_restart': matches = matches and replay_blocked
        return {'case': case, 'policy': policy, 'state': state, **metrics, 'expectation_matched': matches}
    finally:
        endpoint.release.set()
        if task is not None:
            if not task.done(): task.cancel()
            await asyncio.gather(task, return_exceptions=True)


async def evaluate(repetitions=1):
    if type(repetitions) is not int or not 1 <= repetitions <= 10: raise ValueError('Repetitions must be 1..10')
    rows = []
    with tempfile.TemporaryDirectory(prefix='ccs-comparison-') as directory:
        for repetition in range(repetitions):
            for case in CASES:
                for policy in POLICIES:
                    row = await run_case(Path(directory)/f'{repetition}-{case}-{policy}',case,policy)
                    rows.append({'repetition':repetition, **row})
    summaries = {}
    for policy in POLICIES:
        selected = [r for r in rows if r['policy']==policy]
        summaries[policy] = {metric: sum(r[metric] for r in selected) for metric in METRICS}
        summaries[policy]['safely_parked_cases'] = sum(r['safely_parked'] for r in selected)
    ccs_safe = all(summaries['ccs'][m] == 0 for m in METRICS)
    controls = all(any(summaries[p][m] > 0 for m in METRICS) for p in POLICIES[:-1])
    return {'schema':1, 'label':'SIMULATED', 'scope':'mutation_boundary', 'repetitions':repetitions,
            'plan_sha256':hashlib.sha256(canonical({'policies':POLICIES,'cases':CASES,'expected':EXPECTED,'metrics':METRICS})).hexdigest(),
            'cell_count':len(rows), 'passed':ccs_safe and controls and all(r['expectation_matched'] for r in rows),
            'ccs_safety_checks_passed':ccs_safe, 'negative_controls_detected':controls,
            'summaries':summaries, 'observations':rows, 'real_provider':False, 'real_audio':False,
            'latency_measured':False, 'task_completion_measured':False}


def provenance():
    try:
        revision = subprocess.run(['git','rev-parse','HEAD'],cwd=ROOT,capture_output=True,text=True,timeout=5,check=True).stdout.strip()
        dirty = subprocess.run(['git','status','--porcelain','--untracked-files=normal'],cwd=ROOT,capture_output=True,text=True,timeout=5,check=True).stdout
        verified = len(revision)==40 and all(c in '0123456789abcdef' for c in revision) and not dirty
        return {'revision':revision if len(revision)==40 else None,'clean_checkout_verified':verified,
                'runtime_lock_sha256':hashlib.sha256((ROOT/'requirements.txt').read_bytes()).hexdigest(),
                'python':platform.python_version(),'platform':platform.system()}
    except (OSError, subprocess.SubprocessError):
        return {'revision':None,'clean_checkout_verified':False}


async def experiment(repetitions=1):
    before = provenance()
    report = await evaluate(repetitions)
    after = provenance()
    stable = before == after and before['clean_checkout_verified']
    report.update(source=before, source_stable=stable)
    report['passed'] = report['passed'] and stable
    return report


def main():
    parser=argparse.ArgumentParser(description=__doc__,allow_abbrev=False)
    parser.add_argument('--output',required=True)
    parser.add_argument('--repetitions',type=int,default=1,choices=range(1,11))
    args=parser.parse_args()
    logging.disable(logging.CRITICAL)
    try:
        if os.path.lexists(args.output): raise ValueError('Existing evidence')
        report=asyncio.run(experiment(args.repetitions))
        # Exclusive report creation avoids replacing a prior experiment's evidence.
        with Path(args.output).open('x',encoding='utf-8') as stream:
            stream.write(json.dumps(report,sort_keys=True,indent=2,allow_nan=False)+'\n')
        print(json.dumps({k:report[k] for k in ('label','passed','cell_count','ccs_safety_checks_passed','negative_controls_detected')}))
    except (OSError, ValueError, RuntimeError, sqlite3.Error):
        parser.exit(2,'Comparison unverified; preserve existing evidence.\n')
    raise SystemExit(0 if report['passed'] else 1)


if __name__=='__main__': main()
