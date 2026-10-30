from contextlib import closing
from concurrent.futures import ThreadPoolExecutor
import asyncio
import json
import sqlite3
import uuid

import pytest
from starlette.testclient import TestClient

from control.app import ControlConfig, create_app, COOKIE
from runtime.action_journal import JournalError
from runtime.action_journal import SQLiteActionJournal, JournalConflict
from runtime.control_store import ControlStore, ControlError
from runtime.speculative_saga import ActionState
from tests.security_test_support import drop_control_tables
from tests.control_test_support import Clock, seed_control


ORIGIN = "http://127.0.0.1:8877"


@pytest.fixture
def console(tmp_path):
    env = seed_control(tmp_path / "console.db", Clock())
    app = create_app(env.journal, ControlConfig(ORIGIN))
    with TestClient(app, base_url=ORIGIN) as client:
        yield env, client


def login(env, client, credential=None):
    response = client.post("/api/login", json={"credential": (credential or env.operator).token}, headers={"Origin": ORIGIN})
    assert response.status_code == 200, response.text
    return response.json()["csrf_token"]


def command(client, csrf, kind, target, review, **extra):
    return client.post(f"/api/commands/{kind}/{target}", json={"operation_id": str(uuid.uuid4()), "review_digest": review, **extra}, headers={"Origin": ORIGIN, "X-CCS-CSRF": csrf})


def test_console_is_public_but_journal_requires_authenticated_session(console):
    env, client = console
    assert client.get("/").status_code == 200
    response = client.get("/api/snapshot")
    assert response.status_code == 401, response.text
    login(env, client)
    snapshot = client.get("/api/snapshot").json()
    assert len(snapshot["actions"]) == 3
    assert all(a["id"] != env.foreign.action_id for a in snapshot["actions"])
    assert snapshot["provider"] == "NOT_CONFIGURED"


@pytest.mark.parametrize("path", ["/api/session", "/api/snapshot", "/api/records/actions/missing"])
@pytest.mark.parametrize("cookie", [None, "malformed"])
def test_anonymous_console_reads_do_not_contend_for_a_journal_write_lock(console, path, cookie):
    env, client = console
    if cookie:
        client.cookies.set(COOKIE, cookie)
    with closing(sqlite3.connect(env.journal.path)) as blocker:
        blocker.execute("BEGIN IMMEDIATE")
        response = client.get(path)
    assert response.status_code == 401
    assert response.json()["error"]["code"] == "session_required"


def test_authenticated_reads_still_fail_closed_when_journal_is_busy(console):
    env, client = console
    login(env, client)
    with closing(sqlite3.connect(env.journal.path)) as blocker:
        blocker.execute("BEGIN IMMEDIATE")
        response = client.get("/api/snapshot")
    assert response.status_code == 503 and "actions" not in response.json()


def test_login_cookie_and_headers_are_hardened_without_exposing_credentials(console):
    env, client = console
    response = client.post("/api/login", json={"credential": env.operator.token}, headers={"Origin": ORIGIN})
    assert response.status_code == 200
    cookie = response.headers["set-cookie"].lower()
    assert "httponly" in cookie and "samesite=strict" in cookie
    assert env.operator.token not in response.text and env.operator.token not in cookie
    assert response.headers["cache-control"] == "no-store"
    assert "frame-ancestors 'none'" in response.headers["content-security-policy"]


def test_approval_decision_is_durable_and_does_not_dispatch(console):
    env, client = console
    csrf = login(env, client)
    detail = client.get(f"/api/records/actions/{env.pending.action_id}").json()
    response = command(client, csrf, "approve", env.pending.action_id, detail["review_digest"],
                       direction="FORWARD", executor="executor", ttl_ms=60000)
    assert response.status_code == 200, response.text
    assert response.json()["execution"] == "NOT_DISPATCHED"
    assert env.journal.records()[0][1]["state"] == "PENDING"


def test_cancellation_records_intent_without_undoing_effects(console):
    env, client = console
    csrf = login(env, client)
    detail = client.get(f"/api/records/tasks/{env.task.task_id}").json()
    response = command(client, csrf, "cancel", env.task.task_id, detail["review_digest"])
    assert response.status_code == 200, response.text
    assert env.journal.load_task(env.task.task_id)[1]["state"] == "CANCELLED"
    assert env.pending.chain_id in env.journal.closed_chains()
    assert env.journal.records()[0][1]["state"] == "PENDING"


def test_reconciliation_request_preserves_unknown_and_reports_no_worker(console):
    env, client = console
    csrf = login(env, client)
    detail = client.get(f"/api/records/actions/{env.unknown.action_id}").json()
    response = command(client, csrf, "reconcile", env.unknown.action_id, detail["review_digest"])
    assert response.status_code == 202, response.text
    assert response.json()["execution"] == "NOT_CONNECTED"
    assert env.journal.records()[1][1]["state"] == "IN_FLIGHT_UNKNOWN"


def test_missing_csrf_and_cross_origin_requests_cannot_record_decisions(console):
    env, client = console
    login(env, client)
    response = client.post(f"/api/commands/cancel/{env.task.task_id}", json={}, headers={"Origin": ORIGIN})
    assert response.status_code == 403
    response = client.post("/api/login", json={"credential": env.operator.token}, headers={"Origin": "https://attacker.invalid"})
    assert response.status_code == 403


@pytest.mark.parametrize("headers,status", [
    ({"Host": "attacker.invalid:8877"}, 400),
    ({"Origin": "https://attacker.invalid"}, 403),
    ({"Origin": "null"}, 403),
    ({"Sec-Fetch-Site": "cross-site"}, 403),
])
def test_host_and_browser_origin_boundary(console, headers, status):
    env, client = console
    login(env, client)
    response = client.get("/api/snapshot", headers=headers)
    assert response.status_code == status
    assert "actions" not in response.json()
    assert response.headers["cache-control"] == "no-store"


@pytest.mark.parametrize("origin", ["http://0.0.0.0:8877", "https://console.example:8877", "http://127.0.0.1", "http://user@localhost:8877", "http://localhost:8877/"])
def test_console_refuses_nonexact_or_remote_origin(origin):
    with pytest.raises(ValueError):
        ControlConfig(origin)


def test_tls_session_cookie_is_secure(console):
    env, _ = console
    origin = "https://127.0.0.1:8877"
    with TestClient(create_app(env.journal, ControlConfig(origin)), base_url=origin) as client:
        response = client.post("/api/login", json={"credential": env.operator.token}, headers={"Origin": origin})
        assert response.status_code == 200
        assert "; secure" in response.headers["set-cookie"].lower()
        assert client.get("/api/session").status_code == 200


def test_login_rate_limit_and_no_default_console_authority(console):
    env, client = console
    for _ in range(5):
        assert client.post("/api/login", json={"credential": "bad"}, headers={"Origin": ORIGIN}).status_code == 401
    assert client.post("/api/login", json={"credential": env.operator.token}, headers={"Origin": ORIGIN}).status_code == 429
    env.clock.now += 60_001
    assert client.post("/api/login", json={"credential": env.worker.token}, headers={"Origin": ORIGIN}).status_code == 401


def test_session_rotation_logout_and_restart_use_durable_revocation(console):
    env, client = console
    csrf = login(env, client)
    old_cookie = client.cookies.get(COOKIE)
    csrf = login(env, client)
    new_cookie = client.cookies.get(COOKIE)
    assert old_cookie != new_cookie
    store = ControlStore(SQLiteActionJournal(env.journal.path, clock_ms=env.clock))
    with pytest.raises(ControlError, match="ended"):
        store.session(old_cookie)
    assert store.session(new_cookie)["csrf_token"] == csrf
    assert client.post("/api/logout", json={}, headers={"Origin": ORIGIN, "X-CCS-CSRF": csrf}).status_code == 204
    with pytest.raises(ControlError, match="ended"):
        store.session(new_cookie)


@pytest.mark.parametrize("change", ["credential", "principal", "permissions", "expiry"])
def test_current_authority_invalidates_existing_sessions(console, change):
    env, client = console
    login(env, client)
    if change == "credential":
        env.journal.security.revoke_credential(env.operator.id)
    elif change == "principal":
        env.journal.security.update_principal("operator", active=False)
    elif change == "permissions":
        env.journal.security.update_principal("operator", permissions={"ccs:observe"})
    else:
        env.clock.now += ControlStore.SESSION_MS
    assert client.get("/api/session").status_code == 401
    assert client.get("/api/snapshot").status_code == 401


def test_expired_session_cannot_be_resurrected_by_clock_rollback(console):
    env, client = console
    login(env, client)
    env.clock.now += ControlStore.SESSION_MS
    assert client.get("/api/session").status_code == 401
    env.clock.now -= 1000
    assert client.get("/api/session").status_code == 403


def test_session_csrf_restores_on_reload_and_secrets_are_not_persisted(console):
    env, client = console
    csrf = login(env, client)
    token = client.cookies.get(COOKIE)
    assert client.get("/api/session").json()["csrf_token"] == csrf
    with closing(sqlite3.connect(env.journal.path)) as db:
        dump = "\n".join(db.iterdump())
    for secret in (env.operator.token, env.operator.token.split(".")[1], token, csrf):
        assert secret not in dump


def test_viewer_cannot_control_and_foreign_records_are_hidden(console):
    env, client = console
    csrf = login(env, client, env.viewer)
    detail = client.get(f"/api/records/tasks/{env.task.task_id}").json()
    assert not detail["can_cancel"]
    response = command(client, csrf, "cancel", env.task.task_id, detail["review_digest"])
    assert response.status_code == 403
    assert client.get(f"/api/records/actions/{env.foreign.action_id}").status_code == 404
    assert client.get("/api/records/actions/missing").status_code == 404
    assert env.journal.load_task(env.task.task_id)[1]["state"] != "CANCELLED"


@pytest.mark.parametrize("raw,status", [
    ('{"credential":"a","credential":"b"}', 400),
    ('{"credential":NaN}', 400), ('[]', 400), ('null', 400),
    ('{"credential":"' + 'x' * 17000 + '"}', 413),
])
def test_invalid_or_oversized_json_fails_closed(console, raw, status):
    _, client = console
    response = client.post("/api/login", content=raw, headers={"Origin": ORIGIN, "Content-Type": "application/json"})
    assert response.status_code == status


def test_content_type_and_csrf_are_required_even_with_cookie(console):
    env, client = console
    login(env, client)
    response = client.post("/api/login", content="credential=bad", headers={"Origin": ORIGIN})
    assert response.status_code == 415
    assert client.post("/api/logout", json={}, headers={"Origin": ORIGIN, "X-CCS-CSRF": "0" * 64}).status_code == 403
    assert client.post("/api/logout", json={}, headers={"X-CCS-CSRF": "0" * 64}).status_code == 403


def test_exact_id_review_rejects_stale_action_and_task_checkpoint(console):
    env, client = console
    csrf = login(env, client)
    action_review = client.get(f"/api/records/actions/{env.pending.action_id}").json()["review_digest"]
    task_review = client.get(f"/api/records/tasks/{env.task.task_id}").json()["review_digest"]
    version, cp = env.journal.load_task(env.task.task_id)
    cp["error"] = "SIMULATED concurrent checkpoint update"
    env.journal.save_task(env.task.task_id, cp, version)
    response = command(client, csrf, "approve", env.pending.action_id, action_review, direction="FORWARD", executor="executor", ttl_ms=60000)
    assert response.status_code == 409 and response.json()["error"]["code"] == "stale_review"
    response = command(client, csrf, "cancel", env.task.task_id, task_review)
    assert response.status_code == 409


def test_task_review_includes_owned_effect_updates(console):
    env, client = console
    csrf = login(env, client)
    review = client.get(f"/api/records/tasks/{env.task.task_id}").json()["review_digest"]
    env.pending.state = ActionState.ABORTED
    env.saga._persist_action(env.pending, "SIMULATED_KNOWN_NO_EFFECT")
    response = command(client, csrf, "cancel", env.task.task_id, review)
    assert response.status_code == 409


def test_operation_id_replay_is_exact_and_never_creates_a_second_decision(console):
    env, client = console
    csrf = login(env, client)
    review = client.get(f"/api/records/tasks/{env.task.task_id}").json()["review_digest"]
    body = {"operation_id": str(uuid.uuid4()), "review_digest": review}
    headers = {"Origin": ORIGIN, "X-CCS-CSRF": csrf}
    path = f"/api/commands/cancel/{env.task.task_id}"
    first = client.post(path, json=body, headers=headers)
    repeated = client.post(path, json=body, headers=headers)
    assert first.status_code == repeated.status_code == 200
    assert repeated.json()["replayed"] is True
    assert first.json()["checkpoint_version"] == repeated.json()["checkpoint_version"]
    conflict = client.post(path, json={**body, "review_digest": "0" * 64}, headers=headers)
    assert conflict.status_code == 409 and conflict.json()["error"]["code"] == "operation_conflict"
    with closing(sqlite3.connect(env.journal.path)) as db:
        assert db.execute("SELECT count(*) FROM control_commands").fetchone()[0] == 1


def test_two_connections_serialize_duplicate_operator_decisions(console):
    env, _ = console
    stores = [ControlStore(SQLiteActionJournal(env.journal.path, clock_ms=env.clock, timeout=2)) for _ in range(2)]
    token, identity = stores[0].login(env.operator.token)
    review = stores[0].detail(token, "tasks", env.task.task_id)["review_digest"]
    body = {"operation_id": str(uuid.uuid4()), "review_digest": review}
    def send(store):
        return store.command(token, identity["csrf_token"], "CANCEL", env.task.task_id, body)
    with ThreadPoolExecutor(2) as pool:
        results = list(pool.map(send, stores))
    assert sorted(result["replayed"] for result in results) == [False, True]
    with closing(sqlite3.connect(env.journal.path)) as db:
        assert db.execute("SELECT count(*) FROM chain_events WHERE kind='OPERATOR_CANCELLED'").fetchone()[0] == 1


@pytest.mark.parametrize("kind", ["cancel", "approve"])
def test_control_audit_fault_rolls_back_entire_decision(console, kind):
    env, client = console
    csrf = login(env, client)
    task_before = env.journal.load_task(env.task.task_id)
    with closing(sqlite3.connect(env.journal.path)) as db:
        approvals_before = db.execute("SELECT count(*) FROM approvals").fetchone()[0]
        db.execute("CREATE TRIGGER reject_control_event BEFORE INSERT ON control_events WHEN NEW.operation_id IS NOT NULL BEGIN SELECT RAISE(ABORT,'SIMULATED audit failure'); END")
        db.commit()
    target = env.task.task_id if kind == "cancel" else env.pending.action_id
    record_kind = "tasks" if kind == "cancel" else "actions"
    detail = client.get(f"/api/records/{record_kind}/{target}").json()
    extra = {} if kind == "cancel" else {"direction": "FORWARD", "executor": "executor", "ttl_ms": 60000}
    response = command(client, csrf, kind, target, detail["review_digest"], **extra)
    assert response.status_code == 503
    assert env.journal.load_task(env.task.task_id) == task_before
    assert env.pending.chain_id not in env.journal.closed_chains()
    with closing(sqlite3.connect(env.journal.path)) as db:
        assert db.execute("SELECT count(*) FROM control_commands").fetchone()[0] == 0
        assert db.execute("SELECT count(*) FROM approvals").fetchone()[0] == approvals_before


def test_cancel_fences_real_dispatch_boundary_and_stale_checkpoint_writer(console):
    env, client = console
    csrf = login(env, client)
    previous = env.journal.load_task(env.task.task_id)
    detail = client.get(f"/api/records/tasks/{env.task.task_id}").json()
    response = command(client, csrf, "cancel", env.task.task_id, detail["review_digest"])
    assert response.status_code == 200
    env.saga.auth_context.approval_id = env.approval
    invoked = []
    async def execute(args):
        invoked.append(args)
    with pytest.raises(JournalError):
        asyncio.run(env.saga.commit_write(env.pending.action_id, execute))
    assert invoked == []
    with pytest.raises(JournalConflict):
        env.journal.save_task(env.task.task_id, previous[1], previous[0])


@pytest.mark.parametrize("intent_change", ["operator_cancel", "terminal_checkpoint", "superseded_generation"])
def test_cancel_after_intent_preserves_simulated_returned_effect_as_stale(console, intent_change):
    env, client = console
    csrf = login(env, client)
    env.saga.auth_context.approval_id = env.approval
    async def scenario():
        started, finish = asyncio.Event(), asyncio.Event()
        async def execute(args):
            started.set()
            await finish.wait()
            return {"provider": "SIMULATED", "receipt": "applied-before-cancel"}
        operation = asyncio.create_task(env.saga.commit_write(env.pending.action_id, execute))
        await started.wait()
        if intent_change == "operator_cancel":
            detail = client.get(f"/api/records/tasks/{env.task.task_id}").json()
            response = command(client, csrf, "cancel", env.task.task_id, detail["review_digest"])
            assert response.status_code == 200
        else:
            # The durable checkpoint can independently invalidate the result,
            # even if a trusted host has not closed the action chain yet.
            version, checkpoint = env.journal.load_task(env.task.task_id)
            if intent_change == "terminal_checkpoint":
                checkpoint["state"] = "CANCELLED"
            else:
                checkpoint["step_generations"][0] += 1
            env.journal.save_task(env.task.task_id, checkpoint, version)
            assert env.pending.chain_id not in env.journal.closed_chains()
        finish.set()
        await operation
    asyncio.run(scenario())
    record = dict((r["action_id"], r) for _, r in env.journal.records())[env.pending.action_id]
    assert record["state"] == "COMMITTED_STALE"
    assert record["result"]["receipt"] == "applied-before-cancel"
    checkpoint = env.journal.load_task(env.task.task_id)[1]
    if intent_change != "superseded_generation":
        assert checkpoint["state"] == "CANCELLED"
    else:
        assert checkpoint["step_generations"] == [1]
    assert env.pending not in env.saga._chain


def test_reconciliation_deduplicates_across_new_operation_ids_without_replay(console):
    env, client = console
    csrf = login(env, client)
    detail = client.get(f"/api/records/actions/{env.unknown.action_id}").json()
    before = env.journal.records()
    assert command(client, csrf, "reconcile", env.unknown.action_id, detail["review_digest"]).status_code == 202
    assert command(client, csrf, "reconcile", env.unknown.action_id, detail["review_digest"]).status_code == 409
    assert env.journal.records() == before


def test_status_query_return_after_operator_cancel_is_stale_evidence(console):
    env, client = console
    csrf = login(env, client)
    env.saga.auth_context.approval_id = env.approval
    async def scenario():
        async def unknown(_):
            raise ConnectionError("SIMULATED lost acknowledgement")
        with pytest.raises(ConnectionError):
            await env.saga.commit_write(env.pending.action_id, unknown)
        async def status(action):
            detail = client.get(f"/api/records/tasks/{env.task.task_id}").json()
            assert command(client, csrf, "cancel", env.task.task_id, detail["review_digest"]).status_code == 200
            return True  # SIMULATED authoritative evidence of an applied effect
        await env.saga.reconcile_write(env.pending.action_id, status)
    asyncio.run(scenario())
    assert env.pending.state == ActionState.COMMITTED_STALE
    assert env.pending not in env.saga._chain
    assert env.journal.events(env.pending.action_id)[-1]["kind"] == "RECONCILED_PRESENT_STALE"


def test_distinct_simultaneous_cancel_requests_accept_only_one_review(console):
    env, _ = console
    stores = [ControlStore(SQLiteActionJournal(env.journal.path, clock_ms=env.clock, timeout=2)) for _ in range(2)]
    token, identity = stores[0].login(env.operator.token)
    review = stores[0].detail(token, "tasks", env.task.task_id)["review_digest"]
    def send(store):
        try:
            return store.command(token, identity["csrf_token"], "CANCEL", env.task.task_id,
                                 {"operation_id": str(uuid.uuid4()), "review_digest": review})["state"]
        except ControlError as exc:
            return exc.code
    with ThreadPoolExecutor(2) as pool:
        assert sorted(pool.map(send, stores)) == ["RECORDED", "stale_review"]


def test_task_visibility_requires_all_owned_effect_permissions(console):
    env, client = console
    login(env, client)
    # A mismatched/legacy host can have stronger owned action policy than its
    # stored workflow manifest. Observation/cancellation must not ignore it.
    with closing(sqlite3.connect(env.journal.path)) as db:
        record = json.loads(db.execute("SELECT record FROM actions WHERE action_id=?", (env.pending.action_id,)).fetchone()[0])
        record["authorization_policy"]["required_permissions"] = ["private:write"]
        db.execute("UPDATE actions SET record=? WHERE action_id=?", (json.dumps(record), env.pending.action_id))
        db.commit()
    assert client.get(f"/api/records/tasks/{env.task.task_id}").status_code == 404
    assert not client.get("/api/snapshot").json()["tasks"]


def test_revocation_and_principal_change_update_approval_status(console):
    env, client = console
    csrf = login(env, client)
    approval = next(a for a in client.get("/api/snapshot").json()["approvals"] if a["id"] == env.approval)
    assert approval["status"] == "AVAILABLE"
    response = command(client, csrf, "revoke", approval["id"], approval["review_digest"])
    assert response.status_code == 200
    assert next(a for a in client.get("/api/snapshot").json()["approvals"] if a["id"] == env.approval)["status"] == "REVOKED"
    other = env.journal.security.issue_approval(env.operator.token, "executor", env.pending.action_id)
    env.journal.security.update_principal("executor", permissions={"test:write"})
    assert next(a for a in client.get("/api/snapshot").json()["approvals"] if a["id"] == other)["status"] == "INVALIDATED"


@pytest.mark.parametrize("field,value", [("operation_id", "invalid"), ("review_digest", "bad"), ("ttl_ms", True), ("direction", "RETRY"), ("executor", "viewer")])
def test_malformed_approval_requests_do_not_create_authority(console, field, value):
    env, client = console
    csrf = login(env, client)
    detail = client.get(f"/api/records/actions/{env.pending.action_id}").json()
    with closing(sqlite3.connect(env.journal.path)) as db:
        before = db.execute("SELECT count(*) FROM approvals").fetchone()[0]
    body = {"operation_id": str(uuid.uuid4()), "review_digest": detail["review_digest"], "ttl_ms": 60000, "direction": "FORWARD", "executor": "executor"}
    # Viewer has domain grants, so use a missing principal to exercise a denied executor.
    body[field] = "missing" if field == "executor" else value
    response = client.post(f"/api/commands/approve/{env.pending.action_id}", json=body, headers={"Origin": ORIGIN, "X-CCS-CSRF": csrf})
    assert response.status_code in {400, 403}
    with closing(sqlite3.connect(env.journal.path)) as db:
        assert db.execute("SELECT count(*) FROM approvals").fetchone()[0] == before
        assert db.execute("SELECT count(*) FROM control_commands").fetchone()[0] == 0


def test_schema5_migration_is_offline_explicit_and_preserves_security(console):
    env, _ = console
    records = env.journal.records()
    with closing(sqlite3.connect(env.journal.path)) as db:
        credentials = db.execute("SELECT * FROM credentials").fetchall()
        drop_control_tables(db)
        db.execute("PRAGMA user_version=5")
        db.commit()
    with pytest.raises(JournalError, match="migration"):
        SQLiteActionJournal(env.journal.path, clock_ms=env.clock)
    with pytest.raises(JournalError, match="expired/released"):
        SQLiteActionJournal(env.journal.path, clock_ms=env.clock, migrate=True)
    # The fixture now has the old schema, so its old worker is stopped and its
    # lease expires naturally. A schema-6 handle must refuse writes to it.
    env.clock.now += 3_600_000
    migrated = SQLiteActionJournal(env.journal.path, clock_ms=env.clock, migrate=True)
    assert migrated.records() == records
    assert migrated.security.authenticate(env.operator.token).principal_id == "operator"
    with closing(sqlite3.connect(env.journal.path)) as db:
        assert db.execute("PRAGMA user_version").fetchone()[0] == 6
        assert db.execute("SELECT * FROM credentials").fetchall() == credentials


def test_contended_console_read_logs_safe_sqlite_category_and_recovers(console, caplog):
    env, client = console
    login(env, client)
    before = env.journal.records()
    env.journal.timeout = .01  # Fixture only; real journal policy is unchanged.
    with closing(sqlite3.connect(env.journal.path, isolation_level=None)) as holder:
        holder.execute("BEGIN EXCLUSIVE")
        try:
            response = client.get("/api/snapshot")
        finally:
            holder.rollback()
    assert response.status_code == 503 and response.json()["error"]["code"] == "journal_unavailable"
    logs = "\n".join(r.getMessage() for r in caplog.records if r.name == "ccs.control")
    assert "category=SQLITE_BUSY" in logs
    assert env.operator.token not in logs and str(env.journal.path) not in logs
    assert "SQLITE_BUSY" not in response.text  # Internal diagnostics do not expand the API contract.
    assert client.get("/api/snapshot").status_code == 200
    assert env.journal.records() == before


def test_console_unknown_exception_name_and_payload_are_not_logged(console, monkeypatch, caplog):
    env, client = console
    login(env, client)
    private_error = type("PRIVATE_EXCEPTION_NAME", (RuntimeError,), {})("PRIVATE CREDENTIAL AND TRANSCRIPT")
    def fail(*_): raise private_error
    monkeypatch.setattr(client.app.state.store, "snapshot", fail)
    response = client.get("/api/snapshot")
    assert response.status_code == 500 and response.json()["error"]["code"] == "server_error"
    logs = "\n".join(r.getMessage() for r in caplog.records if r.name == "ccs.control")
    assert "category=UNCLASSIFIED" in logs
    assert "PRIVATE" not in logs + response.text
