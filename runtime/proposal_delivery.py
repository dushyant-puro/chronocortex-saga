"""Bounded durable inspection evidence. No admission, voice or executor capability."""
from dataclasses import asdict
import json

from runtime.proposal_contract import digest, validate_binding
from runtime.voice_ingress import bounded_text
from runtime.workflow_contract import canonical
from runtime.policy_engine import PolicyError

CAPACITY = 128
MAX_BYTES = 262144


class DeliveryLimitError(PolicyError):
    """Expected publication refusal; preserves the journal's clock high-water mark."""


def create_schema(db):
    db.execute("CREATE TABLE proposal_deliveries (review_id TEXT PRIMARY KEY, task_id TEXT NOT NULL UNIQUE, principal_id TEXT NOT NULL REFERENCES principals(id), expires_ms INTEGER NOT NULL, payload TEXT NOT NULL)")
    db.execute("CREATE INDEX proposal_delivery_expiry ON proposal_deliveries(expires_ms)")


def validate(review_id, task_id, principal_id, expires_ms, payload):
    if type(payload) is not str or len(payload.encode('utf-8')) > MAX_BYTES:
        raise ValueError('Invalid delivery capacity')
    record = json.loads(payload)
    if type(record) is not dict or set(record) != {'schema', 'checkpoint', 'evidence'} or type(record['schema']) is not int or record['schema'] != 1:
        raise ValueError('Invalid delivery schema')
    cp, evidence = record['checkpoint'], record['evidence']
    if type(cp) is not dict:
        raise ValueError('Invalid delivery checkpoint')
    binding = cp.get('proposal_binding')
    validate_binding(binding, cp)
    if (binding['review_id'] != review_id or binding['task_id'] != task_id or
            binding['reviewer']['principal_id'] != principal_id or
            type(expires_ms) is not int or binding['expires_ms'] != expires_ms):
        raise ValueError('Delivery identity changed')
    if (type(evidence) is not dict or set(evidence) != {'handle', 'revision', 'text', 'tokens', 'grounded', 'reason'} or
            evidence['grounded'] is not True or type(evidence['reason']) is not str or
            len(evidence['reason']) > 64 or digest(evidence) != binding['source']['evidence_digest'] or
            type(evidence['handle']) is not dict or set(evidence['handle']) != {'stream', 'turn'} or
            any(type(v) is not int for v in evidence['handle'].values()) or
            evidence['handle'] != {'stream': binding['source']['stream'], 'turn': binding['source']['turn']} or
            type(evidence['revision']) is not int or evidence['revision'] != binding['source']['revision'] or
            bounded_text(evidence['text']) != evidence['text'] or type(evidence['tokens']) is not list or
            not 1 <= len(evidence['tokens']) <= 4096):
        raise ValueError('Invalid delivery evidence')
    from runtime.grounding_guard import TokenEvidence
    tokens = []
    for token in evidence['tokens']:
        if type(token) is not dict or set(token) != {'text', 'confidence', 'start_ts', 'end_ts'}:
            raise ValueError('Invalid delivery tokens')
        item = TokenEvidence(**token)
        item.validate()
        tokens.append(item.text)
    if evidence['text'].split() != tokens:
        raise ValueError('Unaligned delivery evidence')
    return binding, evidence['text']


def publish(db, checkpoint, proposal, now):
    binding = checkpoint['proposal_binding']
    payload = canonical({'schema': 1, 'checkpoint': checkpoint, 'evidence': asdict(proposal)})
    if len(payload.encode('utf-8')) > MAX_BYTES:
        raise DeliveryLimitError('Delivery payload capacity exhausted')
    row = (binding['review_id'], binding['task_id'], binding['reviewer']['principal_id'], binding['expires_ms'], payload)
    validate(*row)
    if now >= binding['expires_ms']:
        raise DeliveryLimitError('Delivery already expired')
    db.execute('DELETE FROM proposal_deliveries WHERE expires_ms<=?', (now,))
    if db.execute('SELECT count(*) FROM proposal_deliveries').fetchone()[0] >= CAPACITY:
        raise DeliveryLimitError('Delivery capacity exhausted')
    db.execute('INSERT INTO proposal_deliveries VALUES (?,?,?,?,?)', row)


def read(journal, token, csrf, review_id=None):
    """Authenticate on every read; lazy expiry deletion is not secure erasure."""
    from runtime.control_store import ControlStore, ControlError
    from runtime.proposal_contract import identity
    store = ControlStore(journal)
    store._session_token(token)
    if type(csrf) is not str or not csrf:
        raise ControlError(403, 'csrf_required', 'Refresh the session before a delivery read.')
    if review_id is not None:
        identity(review_id)
    with journal._transaction() as db:
        actor, now, _ = store._session(db, token, csrf)
        db.execute('DELETE FROM proposal_deliveries WHERE expires_ms<=?', (now,))
        rows = db.execute('SELECT review_id,task_id,principal_id,expires_ms,payload FROM proposal_deliveries WHERE principal_id=? ORDER BY rowid DESC LIMIT ?',
                          (actor.principal_id, CAPACITY)).fetchall()
        items = []
        for row in rows:
            if review_id is not None and row[0] != review_id:
                continue
            try:
                binding, text = validate(*row)
                task = db.execute('SELECT checkpoint FROM task_checkpoints WHERE task_id=?', (row[1],)).fetchone()
                admitted = task is not None
                if admitted:
                    _, admitted_binding = store._admission_checkpoint(task[0], row[1])
                    if admitted_binding != binding:
                        raise ValueError('Admitted identity changed')
            except (ValueError, TypeError, KeyError, RecursionError):
                raise ControlError(503, 'invalid_evidence', 'Stored review evidence needs operator inspection.') from None
            if not set(binding['required_permissions']) <= actor.granted_permissions:
                continue
            if review_id is None:
                if not admitted:
                    items.append({'review_id': row[0], 'task_id': row[1], 'name': binding['task_name'], 'expires_ms': row[3]})
            else:
                return {'binding': binding, 'transcript': text, 'admitted': admitted,
                        'scope': 'INSPECTION_ONLY', 'current_evidence_verified': False,
                        'admission_authorized': False, 'execution_authorized': False}
        if review_id is None:
            return {'items': items, 'capacity': CAPACITY, 'scope': 'INSPECTION_ONLY',
                    'admission_authorized': False, 'execution_authorized': False}
    # Commit lazy expiry cleanup even when the requested row is unavailable.
    raise ControlError(404, 'delivery_unavailable', 'Review delivery is unavailable or expired. No admission was replayed.')
