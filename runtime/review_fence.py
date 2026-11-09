"""Journal-local review succession. Never cancels work or reconciles a provider."""
from runtime.policy_engine import PermissionDeniedError
import json
import re

CAPACITY = 128


def create_schema(db):
    db.execute('CREATE TABLE proposal_review_fences (task_id TEXT PRIMARY KEY, review_id TEXT NOT NULL, binding_digest TEXT NOT NULL, retire_after_ms INTEGER NOT NULL)')
    from runtime.proposal_delivery import validate
    rows = db.execute('SELECT review_id,task_id,principal_id,expires_ms,payload FROM proposal_deliveries LIMIT ?', (CAPACITY+1,)).fetchall()
    if len(rows) > CAPACITY:
        raise ValueError('Legacy retained review capacity exceeded')
    for row in rows:
        binding, _ = validate(*row)
        db.execute('INSERT INTO proposal_review_fences VALUES (?,?,?,?)',
                   (row[1], row[0], binding['digest'], row[3]))
    validate_snapshot(db)


def cleanup(db, now):
    # Keep the fence through ALL predecessors' deadlines, even if a successor
    # has a shorter session/expiry and its raw delivery has already disappeared.
    db.execute('DELETE FROM proposal_review_fences WHERE retire_after_ms<=?', (now,))


def matches(db, binding):
    row = db.execute('SELECT review_id,binding_digest FROM proposal_review_fences WHERE task_id=?', (binding['task_id'],)).fetchone()
    if row is None:
        return binding['schema'] == 1
    return row == (binding['review_id'], binding['digest'])


def source(db, actor, review_id, binding_digest, now):
    from runtime.proposal_delivery import validate
    row = db.execute('SELECT review_id,task_id,principal_id,expires_ms,payload FROM proposal_deliveries WHERE review_id=?', (review_id,)).fetchone()
    if row is None:
        raise PermissionDeniedError('Retained review is unavailable')
    binding, _ = validate(*row)
    head = db.execute('SELECT review_id,binding_digest,retire_after_ms FROM proposal_review_fences WHERE task_id=?', (row[1],)).fetchone()
    if (row[2] != actor.principal_id or binding['digest'] != binding_digest or now >= row[3] or
            not set(binding['required_permissions']) <= actor.permissions or head is None or
            head[:2] != (review_id, binding_digest) or head[2] < row[3]):
        raise PermissionDeniedError('Retained review authority changed')
    # Even a terminal/cancelled task is already admitted. This bounded flow has
    # no authority to replace it or decide whether a remote effect was undone.
    for table in ('task_checkpoints', 'operation_slots', 'chains'):
        if db.execute('SELECT 1 FROM ' + table + ' WHERE task_id=? LIMIT 1', (row[1],)).fetchone():
            raise PermissionDeniedError('Existing task/effect evidence requires separate handling')
    return binding


def validate_snapshot(db):
    from runtime.proposal_contract import identity, validate_binding
    rows = db.execute('SELECT task_id,review_id,binding_digest,retire_after_ms FROM proposal_review_fences').fetchall()
    if len(rows) > CAPACITY:
        raise ValueError('Fence capacity changed')
    for task_id, review_id, digest, retire in rows:
        identity(task_id); identity(review_id)
        if type(digest) is not str or re.fullmatch('[0-9a-f]{64}', digest) is None or type(retire) is not int or not 0 < retire <= 2**63-1:
            raise ValueError('Invalid review fence')
        task = db.execute('SELECT checkpoint FROM task_checkpoints WHERE task_id=?', (task_id,)).fetchone()
        if task is not None:
            cp = json.loads(task[0])
            if type(cp) is not dict:
                raise ValueError('Invalid admitted checkpoint')
            binding = cp.get('proposal_binding')
            validate_binding(binding, cp)
            if binding['task_id'] != task_id or (binding['review_id'], binding['digest']) != (review_id, digest) or retire < binding['expires_ms']:
                raise ValueError('Admitted review fence changed')
    from runtime.proposal_delivery import validate
    for row in db.execute('SELECT review_id,task_id,principal_id,expires_ms,payload FROM proposal_deliveries'):
        binding, _ = validate(*row)
        head = db.execute('SELECT review_id,binding_digest,retire_after_ms FROM proposal_review_fences WHERE task_id=?', (row[1],)).fetchone()
        if head is None or head[:2] != (row[0], binding['digest']) or head[2] < row[3]:
            raise ValueError('Retained review fence changed')
