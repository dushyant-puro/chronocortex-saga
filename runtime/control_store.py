"""Authenticated local operator decisions. Never executes provider callbacks."""
import hashlib
import hmac
import json
import re
import secrets
import uuid

from runtime.policy_engine import PolicyError, PermissionDeniedError
from runtime.workflow_contract import canonical, validate_record, WorkflowContractError
from runtime.worker_health import worker_view


class ControlError(PolicyError):
    def __init__(self, status, code, message):
        self.status, self.code, self.message = status, code, message
        super().__init__(message)


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def _require(actor, permissions):
    if not set(permissions) <= actor.granted_permissions:
        raise ControlError(403, "forbidden", "Current operation permissions are required.")


def _grants(value):
    if type(value) is not list or any(type(p) is not str or not p for p in value):
        raise ControlError(503, "invalid_evidence", "Stored policy evidence needs operator review.")
    return set(value)


class ControlStore:
    WINDOW = 200
    SESSION_MS = 900_000

    def __init__(self, journal):
        self.journal = journal
        self.authority = journal.security

    @staticmethod
    def _event(db, actor, kind, now, target=None, operation=None):
        db.execute("INSERT INTO control_events(principal_id,kind,target_id,operation_id,observed_ms) VALUES (?,?,?,?,?)",
                   (actor.principal_id, kind, target, operation, now))

    @staticmethod
    def _csrf(token, salt):
        return hmac.new(bytes.fromhex(salt), token.encode(), "sha256").hexdigest()

    def login(self, credential, previous=None):
        with self.journal._transaction() as db:
            now = self.authority._now(db)
            actor = self.authority._authenticate(db, credential, now)
            _require(actor, {"ccs:observe"})
            if previous:
                db.execute("UPDATE operator_sessions SET revoked=1 WHERE session_hash=?", (digest(previous),))
            db.execute("DELETE FROM operator_sessions WHERE revoked=1 OR expires_ms<=?", (now,))
            # Bound active sessions per credential. Oldest sessions lose authority.
            rows = db.execute("SELECT session_hash FROM operator_sessions WHERE credential_id=? ORDER BY expires_ms DESC", (actor.credential_id,)).fetchall()
            for row in rows[7:]:
                db.execute("UPDATE operator_sessions SET revoked=1 WHERE session_hash=?", row)
            token, salt = secrets.token_urlsafe(32), secrets.token_hex(32)
            expires = min(actor.expires_ms, now + self.SESSION_MS)
            db.execute("INSERT INTO operator_sessions VALUES (?,?,?,?,?,0)",
                       (digest(token), salt, actor.credential_id, actor.version, expires))
            self._event(db, actor, "SESSION_STARTED", now)
            return token, self._identity(actor, now, expires, self._csrf(token, salt))

    @staticmethod
    def _identity(actor, now, expires, csrf):
        return {"principal_id": actor.principal_id, "permissions": sorted(actor.granted_permissions),
                "server_time_ms": now, "expires_ms": expires, "csrf_token": csrf}

    @staticmethod
    def _session_token(token):
        if type(token) is not str or re.fullmatch(r"[A-Za-z0-9_-]{43}", token) is None:
            raise ControlError(401, "session_required", "Connect with a valid operator credential.")

    def _session(self, db, token, csrf=None):
        self._session_token(token)
        now = self.authority._now(db)
        row = db.execute("SELECT csrf_salt,credential_id,principal_version,expires_ms,revoked FROM operator_sessions WHERE session_hash=?", (digest(token),)).fetchone()
        if row is None or row[4] or now >= row[3]:
            raise ControlError(401, "session_expired", "The operator session ended. Connect again.")
        try:
            _, actor = self.authority._credential(db, row[1], now)
        except PermissionDeniedError:
            raise ControlError(401, "session_revoked", "The operator session is no longer authorized.") from None
        if actor.version != row[2]:
            raise ControlError(401, "session_changed", "Operator permissions changed. Connect again.")
        _require(actor, {"ccs:observe"})
        expected = self._csrf(token, row[0])
        if csrf is not None and (type(csrf) is not str or re.fullmatch(r"[0-9a-f]{64}", csrf) is None or not hmac.compare_digest(expected, csrf)):
            raise ControlError(403, "csrf_failed", "The control request could not be verified.")
        return actor, now, self._identity(actor, now, row[3], expected)

    def session(self, token):
        self._session_token(token)
        with self.journal._transaction() as db:
            return self._session(db, token)[2]

    def logout(self, token, csrf):
        self._session_token(token)
        with self.journal._transaction() as db:
            actor, now, _ = self._session(db, token, csrf)
            db.execute("UPDATE operator_sessions SET revoked=1 WHERE session_hash=?", (digest(token),))
            self._event(db, actor, "SESSION_ENDED", now)

    @staticmethod
    def _action_scope(record):
        policy = record.get("authorization_policy")
        return _grants(policy["required_permissions"]) if type(policy) is dict and "required_permissions" in policy else {"ccs:legacy-control"}

    @staticmethod
    def _task_scope(cp):
        try:
            contract = validate_record(cp.get("workflow_contract"))
            return set().union(*(_grants(step["manifest"]["required_permissions"]) for step in contract["steps"]))
        except (WorkflowContractError, KeyError, TypeError):
            return {"ccs:legacy-control"}

    def _action(self, db, actor, action_id):
        row = db.execute("SELECT version,record FROM actions WHERE action_id=?", (action_id,)).fetchone()
        if row:
            record = json.loads(row[1])
            if self._action_scope(record) <= actor.granted_permissions:
                return row[0], record
        raise ControlError(404, "not_found", "The requested record is unavailable.")

    def _task(self, db, actor, task_id):
        row = db.execute("SELECT version,checkpoint FROM task_checkpoints WHERE task_id=?", (task_id,)).fetchone()
        if row:
            cp = json.loads(row[1])
            if self._task_permissions(db, cp) <= actor.granted_permissions:
                return row[0], cp
        raise ControlError(404, "not_found", "The requested record is unavailable.")

    def _task_permissions(self, db, cp):
        required = self._task_scope(cp)
        for payload, in db.execute("SELECT a.record FROM actions a JOIN operation_slots s ON s.action_id=a.action_id WHERE s.task_id=?", (cp["task_id"],)):
            required |= self._action_scope(json.loads(payload))
        return required

    @staticmethod
    def _action_brief(version, record):
        return {"id": record["action_id"], "version": version, "tool": record["tool_name"],
                "state": record["state"], "compensation_state": record["compensation_state"],
                "epoch": record["capture_epoch"], "task_id": record["task_id"],
                "generation": record["generation"], "chain_id": record["chain_id"]}

    def _action_review(self, db, version, record):
        chain = db.execute("SELECT status FROM chains WHERE chain_id=?", (record["chain_id"],)).fetchone()
        task = db.execute("SELECT version,checkpoint FROM task_checkpoints WHERE task_id=?", (record["task_id"],)).fetchone()
        task_state = json.loads(task[1])["state"] if task else None
        return {"digest": digest({"version": version, "record": record, "chain": chain, "task": task}),
                "chain_open": bool(chain and chain[0] == "OPEN"),
                "task_active": record["task_id"] is None or (task is not None and task_state not in {"CANCELLED", "COMPLETED", "FAILED"})}

    def _task_view(self, db, version, cp):
        effects = []
        for v, payload in db.execute("SELECT a.version,a.record FROM actions a JOIN operation_slots s ON s.action_id=a.action_id WHERE s.task_id=? ORDER BY s.step_index DESC,s.generation DESC LIMIT ?", (cp["task_id"], self.WINDOW)):
            effects.append(self._action_brief(v, json.loads(payload)))
        aggregate = db.execute("SELECT count(*),coalesce(sum(a.version),0),coalesce(max(a.rowid),0) FROM actions a JOIN operation_slots s ON s.action_id=a.action_id WHERE s.task_id=?", (cp["task_id"],)).fetchone()
        unresolved = [a for a in effects if a["state"] in {"IN_FLIGHT", "IN_FLIGHT_UNKNOWN", "COMMITTED_STALE"} or a["compensation_state"] in {"IN_FLIGHT", "UNKNOWN", "NOT_APPLIED"}]
        return {"id": cp["task_id"], "version": version, "name": cp["name"], "state": cp["state"],
                "current_step": cp["current_step_index"], "total_steps": len(cp["step_states"]),
                "workflow": cp.get("workflow_contract"), "error": cp.get("error"),
                "effects": effects, "unresolved_effects": len(unresolved),
                "effect_window_limit": self.WINDOW, "total_effect_records": aggregate[0],
                "review_digest": digest({"version": version, "checkpoint": cp, "effect_versions": aggregate})}

    def _approval_status(self, db, approval, revoked, used, now):
        if used:
            return "CONSUMED"
        if revoked:
            return "REVOKED"
        if now >= approval["expires_ms"]:
            return "EXPIRED"
        try:
            _, issuer = self.authority._credential(db, approval["issuer_credential_id"], now)
            executor_version, executor_grants = self.authority._principal(db, approval["executor_principal_id"])
            required = set(approval["required_permissions"]) | {"ccs:approve"}
            if issuer.version != approval["issuer_version"] or executor_version != approval["executor_version"] or not required <= issuer.granted_permissions:
                return "INVALIDATED"
            if not set(approval["required_permissions"]) <= executor_grants:
                return "INVALIDATED"
            action = db.execute("SELECT record FROM actions WHERE action_id=?", (approval["binding"]["action_id"],)).fetchone()
            if action is None:
                return "INVALIDATED"
            record = json.loads(action[0])
            if approval["binding"]["direction"] == "FORWARD":
                review = self._action_review(db, 0, record)
                if not (review["chain_open"] and review["task_active"]):
                    return "INVALIDATED"
            self.authority._operation(record, approval["binding"]["direction"])
            if canonical(self.authority._binding(db, record, approval["binding"]["direction"])) != canonical(approval["binding"]):
                return "INVALIDATED"
        except PolicyError:
            return "INVALIDATED"
        return "AVAILABLE"

    def snapshot(self, token):
        self._session_token(token)
        with self.journal._transaction() as db:
            actor, now, _ = self._session(db, token)
            actions, tasks, approvals = [], [], []
            for version, payload in db.execute("SELECT version,record FROM actions ORDER BY rowid DESC LIMIT ?", (self.WINDOW,)):
                record = json.loads(payload)
                if self._action_scope(record) <= actor.granted_permissions:
                    actions.append(self._action_brief(version, record))
            for version, payload in db.execute("SELECT version,checkpoint FROM task_checkpoints ORDER BY rowid DESC LIMIT ?", (self.WINDOW,)):
                cp = json.loads(payload)
                if self._task_permissions(db, cp) <= actor.granted_permissions:
                    tasks.append({"id": cp["task_id"], "version": version, "name": cp["name"], "state": cp["state"],
                                  "current_step": cp["current_step_index"], "total_steps": len(cp["step_states"])})
            for approval_id, payload, revoked, used in db.execute("SELECT id,record,revoked,consumed FROM approvals ORDER BY rowid DESC LIMIT ?", (self.WINDOW,)):
                approval = json.loads(payload)
                if set(approval["required_permissions"]) <= actor.granted_permissions:
                    approvals.append({"id": approval_id, "action_id": approval["binding"]["action_id"],
                                      "direction": approval["binding"]["direction"], "executor": approval["executor_principal_id"],
                                      "issuer": approval["issuer_principal_id"], "expires_ms": approval["expires_ms"],
                                      "status": self._approval_status(db, approval, revoked, used, now),
                                      "review_digest": digest({"record": approval, "revoked": revoked, "used": used})})
            commands = [dict(zip(("operation_id", "kind", "target_id", "result", "created_ms"), row)) for row in db.execute("SELECT operation_id,kind,target_id,result,created_ms FROM control_commands WHERE principal_id=? ORDER BY created_ms DESC LIMIT 20", (actor.principal_id,))]
            for command in commands:
                command["result"] = json.loads(command["result"])
            commands = [command for command in commands if self._command_visible(db, actor, command["kind"], command["target_id"])]
            return {"server_time_ms": now, "window_limit": self.WINDOW, "provider": "NOT_CONFIGURED",
                    "reconciler": "NOT_CONNECTED", "evidence_mode": "JOURNAL_RECORDS",
                    "worker": worker_view(db, now),
                    "actions": actions, "tasks": tasks, "approvals": approvals, "commands": commands}

    def detail(self, token, kind, record_id):
        self._session_token(token)
        with self.journal._transaction() as db:
            actor, now, _ = self._session(db, token)
            if kind == "tasks":
                version, cp = self._task(db, actor, record_id)
                view = self._task_view(db, version, cp)
                view["can_cancel"] = "ccs:cancel" in actor.granted_permissions and cp["state"] not in {"CANCELLED", "COMPLETED", "FAILED"}
                return view
            version, record = self._action(db, actor, record_id)
            review = self._action_review(db, version, record)
            requested = bool(db.execute("SELECT 1 FROM control_commands WHERE kind='RECONCILE' AND target_id=? LIMIT 1", (record_id,)).fetchone())
            can_approve = "ccs:approve" in actor.granted_permissions
            try:
                binding = self.authority._binding(db, record, "FORWARD")
            except PolicyError:
                binding = {"workflow_digest": None, "provider_scopes": []}
                can_approve = False
            can_approve = can_approve and record.get("authorization_policy") is not None
            events = [{"version": v, "kind": name, "created_utc": created} for v, name, created in db.execute("SELECT version,kind,created_utc FROM events WHERE action_id=? ORDER BY seq DESC LIMIT 100", (record_id,))]
            return {**self._action_brief(version, record), "args": record["args"], "receipt": record["result"],
                    "review_digest": review["digest"], "policy": record.get("authorization_policy"),
                    "workflow_digest": binding["workflow_digest"], "provider_scopes": binding["provider_scopes"],
                    "idempotency_key": record["idempotency_key"],
                    "events": events, "reconciliation_requested": requested,
                    "can_approve_forward": can_approve and review["chain_open"] and review["task_active"] and record["state"] == "PENDING" and record["compensation_state"] == "NOT_STARTED",
                    "can_approve_compensation": can_approve and "ccs:repair" in actor.granted_permissions and record["state"] in {"COMMITTED", "COMMITTED_STALE"} and record["compensation_state"] == "NOT_STARTED",
                    "can_request_reconciliation": not requested and "ccs:reconcile" in actor.granted_permissions and (record["state"] in {"IN_FLIGHT", "IN_FLIGHT_UNKNOWN", "COMMITTED_STALE"} or record["compensation_state"] in {"IN_FLIGHT", "UNKNOWN", "NOT_APPLIED"})}

    def _command_visible(self, db, actor, kind, target):
        try:
            if kind == "CANCEL":
                self._task(db, actor, target)
            elif kind == "REVOKE":
                row = db.execute("SELECT record FROM approvals WHERE id=?", (target,)).fetchone()
                if row is None or not set(json.loads(row[0])["required_permissions"]) <= actor.granted_permissions:
                    return False
            else:
                self._action(db, actor, target)
            return True
        except ControlError as exc:
            if exc.status != 404:
                raise
            return False

    def command(self, token, csrf, kind, target, body):
        self._session_token(token)
        required_keys = {"operation_id", "review_digest"} | ({"direction", "executor", "ttl_ms"} if kind == "APPROVE" else set())
        if type(body) is not dict or set(body) != required_keys:
            raise ControlError(400, "invalid_request", "The control request has invalid fields.")
        operation = body["operation_id"]
        try:
            if type(operation) is not str or str(uuid.UUID(operation)) != operation or uuid.UUID(operation).version != 4:
                raise ValueError()
        except (ValueError, AttributeError):
            raise ControlError(400, "invalid_operation", "A unique version-4 operation ID is required.") from None
        request_digest = digest({"kind": kind, "target": target, "body": body})
        role = {"APPROVE": "ccs:approve", "REVOKE": "ccs:approve", "CANCEL": "ccs:cancel", "RECONCILE": "ccs:reconcile"}[kind]
        with self.journal._transaction() as db:
            actor, now, _ = self._session(db, token, csrf)
            _require(actor, {role})
            if not self._command_visible(db, actor, kind, target):
                raise ControlError(404, "not_found", "The requested record is unavailable.")
            previous = db.execute("SELECT request_digest,result FROM control_commands WHERE principal_id=? AND operation_id=?", (actor.principal_id, operation)).fetchone()
            if previous:
                if previous[0] != request_digest:
                    raise ControlError(409, "operation_conflict", "This operation ID already identifies a different request.")
                return {**json.loads(previous[1]), "replayed": True}
            if kind == "CANCEL":
                version, cp = self._task(db, actor, target)
                view = self._task_view(db, version, cp)
                self._review(body, view["review_digest"])
                if cp["state"] in {"CANCELLED", "COMPLETED", "FAILED"}:
                    raise ControlError(409, "ineligible", "This checkpoint is already terminal.")
                cp["state"] = "CANCELLED"
                cp["error"] = "Operator cancellation recorded; remote effects are not rolled back."
                chain_id = f"task:{target}"
                chain = db.execute("SELECT task_id FROM chains WHERE chain_id=?", (chain_id,)).fetchone()
                if chain is not None and chain[0] != target:
                    raise ControlError(503, "invalid_evidence", "Chain ownership needs operator review.")
                db.execute("INSERT INTO chains VALUES (?,?,'CLOSED') ON CONFLICT(chain_id) DO UPDATE SET status='CLOSED'", (chain_id, target))
                db.execute("INSERT INTO chain_events(chain_id,kind) VALUES (?,'OPERATOR_CANCELLED')", (chain_id,))
                db.execute("UPDATE task_checkpoints SET version=?,checkpoint=? WHERE task_id=? AND version=?", (version + 1, canonical(cp), target, version))
                db.execute("INSERT INTO task_events(task_id,version,checkpoint) VALUES (?,?,?)", (target, version + 1, canonical(cp)))
                result = {"state": "RECORDED", "task_id": target, "remote_effects": "UNCHANGED", "checkpoint_version": version + 1}
            elif kind == "REVOKE":
                row = db.execute("SELECT record,revoked,consumed FROM approvals WHERE id=?", (target,)).fetchone()
                if row is None:
                    raise ControlError(404, "not_found", "The requested record is unavailable.")
                record = json.loads(row[0])
                _require(actor, record["required_permissions"])
                self._review(body, digest({"record": record, "revoked": row[1], "used": row[2]}))
                self.authority._revoke_approval(db, actor, target, now)
                result = {"state": "RECORDED", "approval_id": target, "remote_effects": "UNCHANGED"}
            else:
                version, record = self._action(db, actor, target)
                review = self._action_review(db, version, record)
                self._review(body, review["digest"])
                if kind == "APPROVE":
                    direction = body["direction"]
                    required, _ = self.authority._operation(record, direction)
                    _, executor_grants = self.authority._principal(db, body["executor"])
                    if not required <= executor_grants:
                        raise ControlError(403, "executor_denied", "The execution principal lacks current operation permissions.")
                    if direction == "FORWARD" and not (review["chain_open"] and review["task_active"]):
                        raise ControlError(409, "ineligible", "The action chain is closed or its task is inactive.")
                    approval = self.authority._issue_approval(db, actor, body["executor"], target, direction, body["ttl_ms"], now)
                    result = {"state": "RECORDED", "approval_id": approval, "execution": "NOT_DISPATCHED"}
                else:
                    if db.execute("SELECT 1 FROM control_commands WHERE kind='RECONCILE' AND target_id=? LIMIT 1", (target,)).fetchone():
                        raise ControlError(409, "already_requested", "A reconciliation request is already recorded.")
                    if record["state"] not in {"IN_FLIGHT", "IN_FLIGHT_UNKNOWN", "COMMITTED_STALE"} and record["compensation_state"] not in {"IN_FLIGHT", "UNKNOWN", "NOT_APPLIED"}:
                        raise ControlError(409, "ineligible", "This action has no unresolved effect to investigate.")
                    result = {"state": "REQUESTED", "action_id": target, "execution": "NOT_CONNECTED"}
            db.execute("INSERT INTO control_commands VALUES (?,?,?,?,?,?,?)", (actor.principal_id, operation, request_digest, kind, target, canonical(result), now))
            self._event(db, actor, kind, now, target, operation)
            return {**result, "replayed": False}

    @staticmethod
    def _review(body, current):
        if type(body["review_digest"]) is not str or re.fullmatch(r"[0-9a-f]{64}", body["review_digest"]) is None:
            raise ControlError(400, "invalid_review", "A valid evidence review digest is required.")
        if not hmac.compare_digest(body["review_digest"], current):
            raise ControlError(409, "stale_review", "Evidence changed. Refresh and review the current record.")
