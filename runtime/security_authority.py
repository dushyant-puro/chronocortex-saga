"""Local host credential authentication and durable operation approvals.

Provisioning APIs are trusted offline bootstrap/control-plane APIs. This is not
human login, OIDC, an HTTP authentication service or protection from hostile
Python code/SQL access. Bearer secrets are returned once and never persisted.
"""
from dataclasses import dataclass, field
import hashlib
import json
import re
import secrets

from runtime.policy_engine import PermissionDeniedError, ConfirmationRequiredError, payload_digest
from runtime.workflow_contract import validate_record, WorkflowContractError


def _json(value):
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (ValueError, TypeError, RecursionError) as exc:
        raise PermissionDeniedError("Security data must be finite JSON") from exc


def _identity(value):
    if type(value) is not str or not value or value.strip() != value or len(value) > 256:
        raise PermissionDeniedError("Invalid security identity")
    return value


def _permissions(value):
    if type(value) not in (list, tuple, set, frozenset):
        raise PermissionDeniedError("Invalid security permissions")
    for permission in value:
        _identity(permission)
    return frozenset(value)


def _ttl(value, maximum):
    if type(value) is not int or not 1 <= value <= maximum:
        raise PermissionDeniedError("Invalid security lifetime")


def create_schema(db):
    db.execute("CREATE TABLE principals (id TEXT PRIMARY KEY, version INTEGER NOT NULL CHECK(version>0), active INTEGER NOT NULL CHECK(active IN (0,1)), permissions TEXT NOT NULL)")
    db.execute("CREATE TABLE credentials (id TEXT PRIMARY KEY, principal_id TEXT NOT NULL REFERENCES principals(id), verifier TEXT NOT NULL, expires_ms INTEGER NOT NULL, revoked INTEGER NOT NULL CHECK(revoked IN (0,1)))")
    db.execute("CREATE TABLE approvals (id TEXT PRIMARY KEY, record TEXT NOT NULL, revoked INTEGER NOT NULL DEFAULT 0 CHECK(revoked IN (0,1)), consumed INTEGER NOT NULL DEFAULT 0 CHECK(consumed IN (0,1)))")
    db.execute("CREATE TABLE security_clock (singleton INTEGER PRIMARY KEY CHECK(singleton=1), last_seen_ms INTEGER NOT NULL)")
    db.execute("INSERT INTO security_clock VALUES (1,0)")
    db.execute("CREATE TABLE security_events (seq INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT NOT NULL, record TEXT NOT NULL)")


@dataclass(frozen=True)
class LocalCredential:
    id: str
    token: str = field(repr=False)


@dataclass(frozen=True)
class AuthenticatedPrincipal:
    principal_id: str
    version: int
    credential_id: str
    expires_ms: int
    granted_permissions: frozenset[str]

    @property
    def permissions(self):
        return self.granted_permissions


@dataclass(frozen=True)
class AuthRequest:
    token: str = field(repr=False)
    approval_id: str | None = None
    required_permissions: tuple[str, ...] = ()
    require_approval: bool = False
    direction: str = "FORWARD"


class SecurityAuthority:
    create_schema = staticmethod(create_schema)

    def __init__(self, journal):
        self.journal = journal

    def _now(self, db):
        now = self.journal._now()
        if type(now) is not int or now < 0:
            raise PermissionDeniedError("Invalid security clock")
        row = db.execute("SELECT last_seen_ms FROM security_clock WHERE singleton=1").fetchone()
        if row is None or now < row[0]:
            raise PermissionDeniedError("Security clock moved backward; stop authorization")
        db.execute("UPDATE security_clock SET last_seen_ms=? WHERE singleton=1", (now,))
        return now

    @staticmethod
    def _event(db, kind, now, **metadata):
        db.execute("INSERT INTO security_events(kind,record) VALUES (?,?)",
                   (kind, _json({"observed_ms": now, **metadata})))

    @staticmethod
    def _principal(db, principal_id):
        _identity(principal_id)
        row = db.execute("SELECT version,active,permissions FROM principals WHERE id=?", (principal_id,)).fetchone()
        if row is None or row[1] != 1 or type(row[0]) is not int or row[0] < 1:
            raise PermissionDeniedError("Principal unavailable")
        try:
            permissions = _permissions(json.loads(row[2]))
        except (ValueError, TypeError) as exc:
            raise PermissionDeniedError("Invalid stored principal permissions") from exc
        return row[0], permissions

    @classmethod
    def _credential(cls, db, credential_id, now):
        row = db.execute("SELECT principal_id,verifier,expires_ms,revoked FROM credentials WHERE id=?",
                         (credential_id,)).fetchone()
        if row is None or row[3] != 0 or type(row[2]) is not int or now >= row[2]:
            raise PermissionDeniedError("Authentication failed")
        version, permissions = cls._principal(db, row[0])
        return row[1], AuthenticatedPrincipal(row[0], version, credential_id, row[2], permissions)

    def _authenticate(self, db, token, now):
        if type(token) is not str or re.fullmatch(r"[0-9a-f]{32}\.[A-Za-z0-9_-]{43}", token) is None:
            raise PermissionDeniedError("Authentication failed")
        credential_id = token.split(".", 1)[0]
        verifier = hashlib.sha256(token.encode("ascii")).hexdigest()
        try:
            expected, snapshot = self._credential(db, credential_id, now)
        except PermissionDeniedError:
            secrets.compare_digest(verifier, "0" * 64)
            raise PermissionDeniedError("Authentication failed") from None
        if type(expected) is not str or not secrets.compare_digest(verifier, expected):
            raise PermissionDeniedError("Authentication failed")
        return snapshot

    def provision_principal(self, principal_id, permissions):
        """Trusted offline provisioning; never expose directly as a model tool."""
        _identity(principal_id)
        grants = _permissions(permissions)
        with self.journal._transaction() as db:
            now = self._now(db)
            if db.execute("SELECT 1 FROM principals WHERE id=?", (principal_id,)).fetchone():
                raise PermissionDeniedError("Principal already exists")
            db.execute("INSERT INTO principals VALUES (?,1,1,?)", (principal_id, _json(sorted(grants))))
            self._event(db, "PRINCIPAL_PROVISIONED", now, principal_id=principal_id,
                        version=1, permissions=sorted(grants))

    def update_principal(self, principal_id, *, permissions=None, active=None):
        """Trusted offline update; every accepted update invalidates old approvals."""
        _identity(principal_id)
        if active is not None and type(active) is not bool:
            raise PermissionDeniedError("Principal active flag must be an exact bool")
        grants = _permissions(permissions) if permissions is not None else None
        with self.journal._transaction() as db:
            now = self._now(db)
            row = db.execute("SELECT version,active,permissions FROM principals WHERE id=?", (principal_id,)).fetchone()
            if row is None:
                raise PermissionDeniedError("Principal unavailable")
            version = row[0] + 1
            db.execute("UPDATE principals SET version=?,active=?,permissions=? WHERE id=?",
                       (version, row[1] if active is None else int(active),
                        row[2] if grants is None else _json(sorted(grants)), principal_id))
            if active is False:
                db.execute("UPDATE credentials SET revoked=1 WHERE principal_id=?", (principal_id,))
            self._event(db, "PRINCIPAL_UPDATED", now, principal_id=principal_id, version=version,
                        active=bool(row[1]) if active is None else active,
                        permissions=json.loads(row[2]) if grants is None else sorted(grants))

    def issue_credential(self, principal_id, *, ttl_ms=86_400_000):
        _ttl(ttl_ms, 86_400_000)
        with self.journal._transaction() as db:
            now = self._now(db)
            version, _ = self._principal(db, principal_id)
            credential_id = secrets.token_hex(16)
            token = credential_id + "." + secrets.token_urlsafe(32)
            db.execute("INSERT INTO credentials VALUES (?,?,?,?,0)",
                       (credential_id, principal_id, hashlib.sha256(token.encode("ascii")).hexdigest(), now + ttl_ms))
            self._event(db, "CREDENTIAL_ISSUED", now, principal_id=principal_id,
                        version=version, credential_id=credential_id, expires_ms=now + ttl_ms)
        return LocalCredential(credential_id, token)

    def revoke_credential(self, credential_id):
        _identity(credential_id)
        with self.journal._transaction() as db:
            now = self._now(db)
            row = db.execute("SELECT principal_id FROM credentials WHERE id=?", (credential_id,)).fetchone()
            if row is None:
                raise PermissionDeniedError("Credential unavailable")
            db.execute("UPDATE credentials SET revoked=1 WHERE id=?", (credential_id,))
            self._event(db, "CREDENTIAL_REVOKED", now, credential_id=credential_id, principal_id=row[0])

    def authenticate(self, token):
        with self.journal._transaction() as db:
            now = self._now(db)
            snapshot = self._authenticate(db, token, now)
            self._event(db, "AUTHENTICATED", now, principal_id=snapshot.principal_id,
                        version=snapshot.version, credential_id=snapshot.credential_id)
            return snapshot

    @staticmethod
    def _operation(old, direction):
        if direction not in ("FORWARD", "COMPENSATION") or type(direction) is not str:
            raise PermissionDeniedError("Invalid authorization direction")
        policy = old.get("authorization_policy")
        if type(policy) is not dict or set(policy) != {"required_permissions", "requires_confirmation"}:
            raise PermissionDeniedError("Recorded authorization policy required")
        permissions = _permissions(policy["required_permissions"])
        confirmation = policy["requires_confirmation"]
        if type(confirmation) is not bool:
            raise PermissionDeniedError("Invalid recorded confirmation policy")
        if old.get("compensation_state") != "NOT_STARTED":
            raise PermissionDeniedError("Operation already has a repair attempt")
        if direction == "FORWARD":
            if old.get("state") != "PENDING":
                raise PermissionDeniedError("Forward approval requires a pending operation")
        else:
            if old.get("state") not in ("COMMITTED", "COMMITTED_STALE"):
                raise PermissionDeniedError("Repair approval requires a known applied effect")
            permissions |= {"ccs:repair"}
            confirmation = True
        return permissions, confirmation

    @staticmethod
    def _binding(db, old, direction):
        keys = ("action_id", "tool_name", "digest", "capture_epoch", "session_id", "task_id",
                "step_index", "generation", "chain_id", "idempotency_key")
        try:
            binding = {key: old[key] for key in keys}
            if type(old["capture_epoch"]) is not int or old["capture_epoch"] < 0:
                raise PermissionDeniedError("Invalid operation epoch")
            if payload_digest(old["tool_name"], old["args"]) != old["digest"]:
                raise PermissionDeniedError("Operation payload differs from its durable identity")
            binding.update(direction=direction, workflow_digest=None, provider_scopes=[])
            if old["task_id"] is not None:
                row = db.execute("SELECT checkpoint FROM task_checkpoints WHERE task_id=?", (old["task_id"],)).fetchone()
                if row is not None:
                    contract = json.loads(row[0]).get("workflow_contract")
                    validate_record(contract)
                    idx = old["step_index"]
                    if type(idx) is not int or not 0 <= idx < len(contract["steps"]):
                        raise PermissionDeniedError("Invalid workflow operation index")
                    scopes = [step["versions"]["provider_scope"] for step in contract["steps"]]
                    for scope in scopes:
                        _identity(scope)
                    binding.update(workflow_digest=contract["digest"], provider_scopes=scopes)
            binding["policy_digest"] = hashlib.sha256(_json(old["authorization_policy"]).encode()).hexdigest()
            if direction == "COMPENSATION":
                binding["receipt_digest"] = hashlib.sha256(_json(old.get("result")).encode()).hexdigest()
            _json(binding)
            return binding
        except (KeyError, TypeError, ValueError, WorkflowContractError) as exc:
            raise PermissionDeniedError("Invalid durable approval binding") from exc

    def issue_approval(self, issuer_token, executor_principal_id, action_id, *, direction="FORWARD", ttl_ms=300_000):
        _ttl(ttl_ms, 300_000)
        _identity(action_id)
        with self.journal._transaction() as db:
            now = self._now(db)
            issuer = self._authenticate(db, issuer_token, now)
            return self._issue_approval(db, issuer, executor_principal_id, action_id, direction, ttl_ms, now)

    def _issue_approval(self, db, issuer, executor_principal_id, action_id, direction, ttl_ms, now):
        """Internal transaction API; caller must authenticate issuer in this transaction."""
        _ttl(ttl_ms, 300_000)
        _identity(action_id)
        executor_version, _ = self._principal(db, executor_principal_id)
        row = db.execute("SELECT record FROM actions WHERE action_id=?", (action_id,)).fetchone()
        if row is None:
            raise PermissionDeniedError("Operation unavailable")
        old = json.loads(row[0])
        required, _ = self._operation(old, direction)
        if not (required | {"ccs:approve"}) <= issuer.granted_permissions:
            raise PermissionDeniedError("Issuer lacks operation approval permissions")
        binding = self._binding(db, old, direction)
        approval_id = secrets.token_hex(16)
        record = {"binding": binding, "issuer_principal_id": issuer.principal_id,
                  "issuer_version": issuer.version, "issuer_credential_id": issuer.credential_id,
                  "executor_principal_id": executor_principal_id, "executor_version": executor_version,
                  "issued_ms": now, "expires_ms": now + ttl_ms, "required_permissions": sorted(required)}
        db.execute("INSERT INTO approvals(id,record) VALUES (?,?)", (approval_id, _json(record)))
        self._event(db, "APPROVAL_ISSUED", now, approval_id=approval_id, action_id=action_id,
                    direction=direction, issuer_principal_id=issuer.principal_id,
                    executor_principal_id=executor_principal_id, expires_ms=now + ttl_ms)
        return approval_id

    def revoke_approval(self, issuer_token, approval_id):
        _identity(approval_id)
        with self.journal._transaction() as db:
            now = self._now(db)
            issuer = self._authenticate(db, issuer_token, now)
            self._revoke_approval(db, issuer, approval_id, now)

    def _revoke_approval(self, db, issuer, approval_id, now):
        if "ccs:approve" not in issuer.granted_permissions:
            raise PermissionDeniedError("Approval revocation permission required")
        row = db.execute("SELECT record FROM approvals WHERE id=?", (approval_id,)).fetchone()
        if row is None:
            raise PermissionDeniedError("Approval unavailable")
        required = _permissions(json.loads(row[0])["required_permissions"])
        if not required <= issuer.granted_permissions:
            raise PermissionDeniedError("Approval revocation requires current operation permissions")
        db.execute("UPDATE approvals SET revoked=1 WHERE id=?", (approval_id,))
        self._event(db, "APPROVAL_REVOKED", now, approval_id=approval_id,
                    issuer_principal_id=issuer.principal_id)

    def _validate_approval(self, db, approval_id, binding, actor, required, now):
        """Check current authority without consuming; intent must recheck it."""
        _identity(approval_id)
        row = db.execute("SELECT record,revoked,consumed FROM approvals WHERE id=?", (approval_id,)).fetchone()
        if row is None or row[1] != 0 or row[2] != 0:
            raise ConfirmationRequiredError("Approval unavailable, revoked or consumed")
        try:
            approval = json.loads(row[0])
            if (type(approval["expires_ms"]) is not int or now >= approval["expires_ms"] or
                    _json(approval["binding"]) != _json(binding) or
                    approval["executor_principal_id"] != actor.principal_id or
                    type(approval["executor_version"]) is not int or approval["executor_version"] != actor.version):
                raise ConfirmationRequiredError("Approval does not match the current operation or executor")
            _, issuer = self._credential(db, approval["issuer_credential_id"], now)
            if (issuer.principal_id != approval["issuer_principal_id"] or
                    type(approval["issuer_version"]) is not int or issuer.version != approval["issuer_version"] or
                    not (required | {"ccs:approve"}) <= issuer.granted_permissions):
                raise ConfirmationRequiredError("Approval issuer authority changed")
        except (KeyError, TypeError, ValueError) as exc:
            raise ConfirmationRequiredError("Invalid stored approval") from exc

    def find_forward_approval(self, executor_token, action_id):
        """Find the latest console decision for this worker's pending action.

        A returned lookup ID is not dispatch authorization. The journal rechecks
        and consumes approval atomically with DISPATCH_INTENT. No fallback to an
        older approval, event insertion, mutation callback or approval issuance.
        """
        _identity(action_id)
        with self.journal._transaction() as db:
            now = self._now(db)
            actor = self._authenticate(db, executor_token, now)
            if "ccs:work" not in actor.granted_permissions:
                raise PermissionDeniedError("Current worker permission required")
            row = db.execute("SELECT record FROM actions WHERE action_id=?", (action_id,)).fetchone()
            if row is None:
                raise PermissionDeniedError("Operation unavailable")
            old = json.loads(row[0])
            required, _ = self._operation(old, "FORWARD")
            if not required <= actor.granted_permissions:
                raise PermissionDeniedError("Principal lacks current operation permissions")
            chain = db.execute("SELECT status FROM chains WHERE chain_id=?", (old["chain_id"],)).fetchone()
            if chain is None or chain[0] != "OPEN":
                raise PermissionDeniedError("Action chain is closed")
            self.journal._task_projection_guard(db, old)
            binding = self._binding(db, old, "FORWARD")
            # Existing (kind,target_id) index bounds lookup to this action's
            # decisions; rowid breaks same-clock ties by committed insert order.
            decision = db.execute("SELECT result FROM control_commands WHERE kind='APPROVE' AND target_id=? ORDER BY rowid DESC LIMIT 1",
                                  (action_id,)).fetchone()
            if decision is None:
                return None
            result = json.loads(decision[0])
            if (type(result) is not dict or set(result) != {"state", "approval_id", "execution"} or
                    result["state"] != "RECORDED" or result["execution"] != "NOT_DISPATCHED"):
                raise PermissionDeniedError("Invalid recorded approval decision")
            try:
                self._validate_approval(db, result["approval_id"], binding, actor, required, now)
            except (ConfirmationRequiredError, PermissionDeniedError):
                return None
            return result["approval_id"]

    def authorize(self, db, record, old, request):
        """Called only inside the journal's dispatch/repair intent transaction."""
        if (type(request) is not AuthRequest or type(request.required_permissions) is not tuple or
                type(request.require_approval) is not bool):
            raise PermissionDeniedError("Authenticated execution request required")
        now = self._now(db)
        actor = self._authenticate(db, request.token, now)
        required, confirmation = self._operation(old, request.direction)
        if request.direction == "COMPENSATION" and _json(record.get("result")) != _json(old.get("result")):
            raise PermissionDeniedError("Repair receipt differs from the durable approved effect")
        required |= _permissions(request.required_permissions)
        if not required <= actor.granted_permissions:
            raise PermissionDeniedError("Principal lacks current operation permissions")
        binding = self._binding(db, old, request.direction)
        approval_id = request.approval_id
        if approval_id is None and (confirmation or request.require_approval):
            raise ConfirmationRequiredError("Durable action-bound approval required")
        if approval_id is not None:
            self._validate_approval(db, approval_id, binding, actor, required, now)
            changed = db.execute("UPDATE approvals SET consumed=1 WHERE id=? AND revoked=0 AND consumed=0", (approval_id,))
            if changed.rowcount != 1:
                raise ConfirmationRequiredError("Approval already consumed")
            self._event(db, "APPROVAL_CONSUMED", now, approval_id=approval_id,
                        action_id=old["action_id"], direction=request.direction,
                        executor_principal_id=actor.principal_id)
        metadata = {"principal_id": actor.principal_id, "principal_version": actor.version,
                    "credential_id": actor.credential_id, "direction": request.direction,
                    "approval_id": approval_id, "required_permissions": sorted(required),
                    "binding_digest": hashlib.sha256(_json(binding).encode()).hexdigest()}
        self._event(db, "INTENT_AUTHORIZED", now, action_id=old["action_id"], **metadata)
        return metadata
