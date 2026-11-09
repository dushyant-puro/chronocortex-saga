"""One replacement for cancelled, zero-activity work. No provider rollback claims."""
import json

from runtime.policy_engine import PermissionDeniedError
from runtime.proposal_contract import digest, validate_binding, identity


class ReplacementDenied(PermissionDeniedError):
    def __init__(self, reason):
        self.reason = reason
        super().__init__('Task replacement is unavailable: ' + reason)


def create_schema(db):
    db.execute('CREATE TABLE task_replacements (original_task_id TEXT PRIMARY KEY REFERENCES task_checkpoints(task_id), replacement_task_id TEXT NOT NULL UNIQUE REFERENCES task_checkpoints(task_id), original_version INTEGER NOT NULL CHECK(original_version>=0), original_digest TEXT NOT NULL)')


def inactive(db, task_id, cp):
    count = len(cp['proposal_binding']['arguments'])
    if cp.get('state') != 'CANCELLED':
        raise ReplacementDenied('TASK_NOT_CANCELLED')
    if (type(cp.get('current_step_index')) is not int or cp.get('current_step_index') != 0 or cp.get('step_states') != ['PENDING']*count or
            type(cp.get('step_generations')) is not list or len(cp['step_generations']) != count or
            any(type(g) is not int or g!=0 for g in cp['step_generations']) or cp.get('replan_from') is not None or
            any(cp.get(k) != [None]*count for k in ('step_results','step_action_ids','step_dispatched_args'))):
        raise ReplacementDenied('RECORDED_ACTIVITY')
    if db.execute('SELECT 1 FROM operation_slots WHERE task_id=? LIMIT 1',(task_id,)).fetchone():
        raise ReplacementDenied('RECORDED_ACTIVITY')
    # Check actual action records too; never classify even a PENDING/compensated
    # action as proof that replaying its objective is safe.
    if db.execute("SELECT 1 FROM actions WHERE json_extract(record,'$.task_id')=? OR json_extract(record,'$.chain_id')=? LIMIT 1",(task_id,'task:'+task_id)).fetchone():
        raise ReplacementDenied('RECORDED_ACTIVITY')
    chain=db.execute('SELECT task_id,status FROM chains WHERE chain_id=?',('task:'+task_id,)).fetchone()
    if chain != (task_id,'CLOSED') or db.execute('SELECT 1 FROM chains WHERE task_id=? AND chain_id<>? LIMIT 1',(task_id,'task:'+task_id)).fetchone():
        raise ReplacementDenied('CHAIN_NOT_CLOSED')


def source(db, actor, task_id, expected=None):
    from runtime.control_store import ControlError
    identity(task_id)
    if not {'ccs:task:create','ccs:cancel'} <= actor.permissions:
        raise PermissionDeniedError('Current task creation and cancellation roles required')
    row=db.execute('SELECT version,checkpoint FROM task_checkpoints WHERE task_id=?',(task_id,)).fetchone()
    if row is None:
        raise ControlError(404,'not_found','The requested task is unavailable.')
    cp=json.loads(row[1])
    if type(cp) is not dict:
        raise ValueError('Invalid original checkpoint')
    binding=cp.get('proposal_binding');validate_binding(binding,cp)
    if cp.get('task_id') != task_id:
        raise ValueError('Original task identity changed')
    if binding['reviewer']['principal_id'] != actor.principal_id or not set(binding['required_permissions']) <= actor.permissions:
        raise ControlError(404,'not_found','The requested task is unavailable.')
    descriptor={'task_id':task_id,'version':row[0],'digest':digest({'version':row[0],'checkpoint':cp})}
    if expected is not None and descriptor != expected:
        raise ReplacementDenied('EVIDENCE_CHANGED')
    if db.execute('SELECT 1 FROM task_replacements WHERE original_task_id=?',(task_id,)).fetchone():
        raise ReplacementDenied('ALREADY_REPLACED')
    inactive(db,task_id,cp)
    return descriptor,cp['name']


def claim(db,binding):
    original=binding.get('replaces_task')
    if original is not None:
        db.execute('INSERT INTO task_replacements VALUES (?,?,?,?)',
                   (original['task_id'],binding['task_id'],original['version'],original['digest']))


def check_claim(db,binding):
    original=binding.get('replaces_task')
    if original is not None:
        row=db.execute('SELECT replacement_task_id,original_version,original_digest FROM task_replacements WHERE original_task_id=?',(original['task_id'],)).fetchone()
        if row != (binding['task_id'],original['version'],original['digest']):
            raise ReplacementDenied('REPLACEMENT_CLAIM_CHANGED')


def inspect(journal,session,csrf,task_id):
    from runtime.control_store import ControlStore,ControlError
    if type(csrf) is not str or not csrf:
        raise ControlError(403,'csrf_required','Refresh the operator session before task inspection.')
    with journal._transaction() as db:
        actor,_,_=ControlStore(journal)._session(db,session,csrf)
        try:
            descriptor,name=source(db,actor,task_id)
        except ReplacementDenied as error:
            return {'eligible':False,'reason':error.reason,'scope':'JOURNAL_ZERO_ACTIVITY_ONLY','execution_authorized':False}
        return {'eligible':True,'original':descriptor,'name':name,'scope':'JOURNAL_ZERO_ACTIVITY_ONLY','execution_authorized':False}


def validate_snapshot(db):
    for original,replacement,version,expected in db.execute('SELECT * FROM task_replacements'):
        history=db.execute('SELECT seq,checkpoint FROM task_events WHERE task_id=? AND version=?',(original,version)).fetchone()
        destination=db.execute('SELECT checkpoint FROM task_checkpoints WHERE task_id=?',(replacement,)).fetchone()
        first=db.execute('SELECT seq FROM task_events WHERE task_id=? AND version=0',(replacement,)).fetchone()
        current=db.execute('SELECT version,checkpoint FROM task_checkpoints WHERE task_id=?',(original,)).fetchone()
        if history is None or destination is None or first is None or history[0]>=first[0]:
            raise ValueError('Replacement history changed')
        if current != (version,history[1]):
            raise ValueError('Original checkpoint changed after replacement')
        old=json.loads(history[1]);new=json.loads(destination[0])
        if type(old) is not dict or type(new) is not dict:
            raise ValueError('Invalid replacement evidence')
        validate_binding(old.get('proposal_binding'),old);validate_binding(new.get('proposal_binding'),new)
        if old.get('task_id') != original or new.get('task_id') != replacement or digest({'version':version,'checkpoint':old}) != expected:
            raise ValueError('Replacement snapshot identity changed')
        try:
            inactive(db,original,old)
        except ReplacementDenied:
            raise ValueError('Replacement origin activity changed') from None
        if new['proposal_binding'].get('replaces_task') != {'task_id':original,'version':version,'digest':expected}:
            raise ValueError('Replacement lineage changed')
    for task_id,payload in db.execute('SELECT task_id,checkpoint FROM task_checkpoints'):
        cp=json.loads(payload)
        if type(cp) is not dict:
            raise ValueError('Invalid checkpoint')
        if type(cp.get('proposal_binding')) is dict and cp['proposal_binding'].get('replaces_task') is not None:
            try:
                check_claim(db,cp['proposal_binding'])
            except ReplacementDenied:
                raise ValueError('Replacement claim changed') from None
