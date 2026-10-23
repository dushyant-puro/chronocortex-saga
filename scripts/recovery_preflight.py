"""Read-only local journal evidence checks; never authorize restore or dispatch."""
import argparse
from contextlib import closing
import json
from pathlib import Path
import sqlite3
import time


TABLES = frozenset('actions key_claims events task_checkpoints task_events chains operation_slots chain_events worker_lease worker_events principals credentials approvals security_clock security_events operator_sessions control_commands control_events'.split())
STATES = frozenset('PENDING IN_FLIGHT IN_FLIGHT_UNKNOWN COMMITTED COMMITTED_STALE ABORTED COMPENSATED'.split())
COMPENSATION = frozenset('NOT_STARTED IN_FLIGHT UNKNOWN NOT_APPLIED SUCCEEDED'.split())


class EvidenceError(ValueError):
    pass


def require(condition):
    if not condition:
        raise EvidenceError('inconsistent evidence')


def pairs(items):
    result = {}
    for key, value in items:
        require(key not in result)
        result[key] = value
    return result


def reject_constant(value):
    raise EvidenceError('nonfinite evidence')


def inspect(path):
    """One bounded read transaction. Reports contain counts, never stored payloads.

    A consistent old copy can pass: freshness, external effects, authorization
    semantics and host/storage integrity are outside this inspection boundary.
    """
    report = {'schema': 1, 'scope': 'LOCAL_JOURNAL_EVIDENCE', 'passed': False,
              'restore_authorized': False, 'dispatch_authorized': False,
              'freshness_verified': False, 'provider_reconciled': False}
    deadline = time.monotonic() + 10
    try:
        source = Path(path).resolve(strict=True)
        require(source.is_file())
        with closing(sqlite3.connect(source.as_uri() + '?mode=ro', uri=True,
                                     timeout=0.25, isolation_level=None)) as db:
            db.execute('PRAGMA query_only=ON')
            db.execute('PRAGMA trusted_schema=OFF')
            db.set_progress_handler(lambda: int(time.monotonic() >= deadline), 1000)
            db.execute('BEGIN')
            require(db.execute('PRAGMA application_id').fetchone()[0] == 0x43435341)
            require(db.execute('PRAGMA user_version').fetchone()[0] == 6)
            require(db.execute('PRAGMA journal_mode').fetchone()[0] == 'delete')
            present = {r[0] for r in db.execute("SELECT name FROM sqlite_schema WHERE type='table'")}
            require(TABLES <= present)
            require(db.execute('PRAGMA integrity_check(1)').fetchall() == [('ok',)])
            require(db.execute('PRAGMA foreign_key_check').fetchone() is None)
            # SQL identifiers below are fixed application constants, never input.
            counts = {}
            for table in sorted(TABLES):
                counts[table] = db.execute('SELECT count(*) FROM ' + table).fetchone()[0]
                require(counts[table] <= 100_000)
            for table, column in [('actions', 'record'), ('task_checkpoints', 'checkpoint')]:
                require(db.execute(f'SELECT 1 FROM {table} WHERE length(cast({column} AS BLOB))>1048576 LIMIT 1').fetchone() is None)
            for table, history, identity, payload in [('actions', 'events', 'action_id', 'record'),
                                                       ('task_checkpoints', 'task_events', 'task_id', 'checkpoint')]:
                # Every projection has a complete version sequence and matching head.
                require(db.execute(f'''SELECT 1 FROM {table} p LEFT JOIN
                    (SELECT {identity} id, min(version) lo, max(version) hi, count(*) n,
                     count(DISTINCT version) distinct_n FROM {history} GROUP BY {identity}) h
                    ON h.id=p.{identity} WHERE h.id IS NULL OR h.lo!=0 OR h.hi!=p.version
                    OR h.n!=p.version+1 OR h.distinct_n!=h.n LIMIT 1''').fetchone() is None)
                require(db.execute(f'''SELECT 1 FROM {table} p JOIN {history} h
                    ON h.{identity}=p.{identity} AND h.version=p.version
                    WHERE h.{payload}!=p.{payload} LIMIT 1''').fetchone() is None)
            uncertain = 0
            for task_id, payload in db.execute('SELECT task_id,checkpoint FROM task_checkpoints'):
                require(time.monotonic() < deadline)
                checkpoint = json.loads(payload, object_pairs_hook=pairs, parse_constant=reject_constant)
                require(type(checkpoint) is dict and checkpoint['task_id'] == task_id)
            for action_id, payload in db.execute('SELECT action_id,record FROM actions'):
                require(time.monotonic() < deadline)
                record = json.loads(payload, object_pairs_hook=pairs, parse_constant=reject_constant)
                require(type(record) is dict and record['action_id'] == action_id)
                require(type(record['chain_id']) is str and bool(record['chain_id']))
                require(record['state'] in STATES and record['compensation_state'] in COMPENSATION)
                require(type(record['idempotency_key']) is str and bool(record['idempotency_key']))
                released = record['state'] in {'ABORTED', 'COMPENSATED'} and record['compensation_state'] in {'NOT_STARTED', 'SUCCEEDED'}
                claim = db.execute('SELECT operation_key FROM key_claims WHERE action_id=?', (action_id,)).fetchall()
                require(claim == ([] if released else [(record['idempotency_key'],)]))
                chain = db.execute('SELECT task_id,status FROM chains WHERE chain_id=?', (record['chain_id'],)).fetchone()
                require(chain is not None and chain[0] == record['task_id'] and chain[1] in {'OPEN', 'CLOSED'})
                slots = db.execute('SELECT task_id,step_index,generation FROM operation_slots WHERE action_id=?', (action_id,)).fetchall()
                if record['task_id'] is None:
                    require(record['step_index'] is None and record['generation'] is None and not slots)
                    require(not record['chain_id'].startswith('task:'))
                else:
                    require(type(record['task_id']) is str and bool(record['task_id']))
                    require(all(type(record[k]) is int and record[k] >= 0 for k in ('step_index', 'generation')))
                    require(record['chain_id'] == 'task:' + record['task_id'])
                    require(slots == [(record['task_id'], record['step_index'], record['generation'])])
                uncertain += int(record['state'] in {'IN_FLIGHT', 'IN_FLIGHT_UNKNOWN'} or
                                 record['compensation_state'] in {'IN_FLIGHT', 'UNKNOWN'})
            db.rollback()
        report.update(passed=True, counts=counts, uncertain_actions=uncertain)
    except (OSError, sqlite3.Error, ValueError, TypeError, KeyError, RecursionError):
        report['failure'] = 'JOURNAL_EVIDENCE_UNVERIFIED'
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument('--journal', required=True)
    args = parser.parse_args()
    report = inspect(args.journal)
    print(json.dumps(report, sort_keys=True, allow_nan=False))
    raise SystemExit(0 if report['passed'] else 2)


if __name__ == '__main__':
    main()
