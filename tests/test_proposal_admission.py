"""Real local auth/SQLite/task loop; voice and external tool effects are SIMULATED."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import replace
import json
import sqlite3
import struct
from types import SimpleNamespace

import pytest

from runtime.action_journal import SQLiteActionJournal, JournalError, JournalConflict, JournalLeaseError, WorkerLease
from runtime.grounding_guard import TokenEvidence
from runtime.policy_engine import AuthorizationContext, ConfirmationRequiredError, PermissionDeniedError
from runtime.proposal_admission import AdmissionTemplate, ProposalAdmissionService
from runtime.proposal_contract import ProposalAdmissionError, bounded_arguments, digest
from runtime.speculative_saga import SpeculativeSagaManager, TurnEpochClock
from runtime.task_engine import TaskManager, TaskStep, TaskState, WorkflowMismatchError
from runtime.tool_contract import ToolManifest, ToolRegistry
from runtime.voice_ingress import VoiceIngress, VoiceEvidenceError
from runtime.workflow_contract import WorkflowVersion, StepVersions, canonical
from runtime.security_authority import SecurityAuthority
from scripts import journal_backup, recovery_preflight


class Tasks(TaskManager):
    def __init__(self, saga, effects):
        self.effects = effects
        super().__init__(saga, lambda _: None)

    def _get_executor(self, step):
        async def execute(args):
            self.effects.append(json.loads(canonical(args)))
            args["nested"]["items"].append("executor-mutated-copy")
            return {"SIMULATED": "applied"}
        return execute


def complete(voice, text="move load alpha", confidence=.95):
    turn = voice.begin_turn(voice._stream)
    tokens = tuple(TokenEvidence(w, confidence, i * .5, i * .5 + .4) for i, w in enumerate(text.split()))
    voice.replace(turn, 1, text, tokens, final=True)
    return voice.complete(turn, text)


@pytest.fixture
def env(tmp_path):
    clock = SimpleNamespace(now=1000)
    journal = SQLiteActionJournal(tmp_path / "journal.db", clock_ms=lambda: clock.now)
    journal.acquire_lease("SIMULATED-host", ttl_ms=3600000)
    authority = journal.security
    for name, grants in (("reviewer", {"ccs:task:create", "test:write"}),
                         ("worker", {"test:write"}), ("other", {"ccs:task:create", "test:write"}),
                         ("viewer", {"test:write"}), ("no-domain", {"ccs:task:create"})):
        authority.provision_principal(name, grants)
    credentials = {name: authority.issue_credential(name, ttl_ms=600000)
                   for name in ("reviewer", "worker", "other", "viewer", "no-domain")}
    registry = ToolRegistry()
    registry.register(ToolManifest("move", "write", cancellable=False,
                                   requires_authoritative_commit=True,
                                   required_permissions=["test:write"], requires_confirmation=True))
    saga = SpeculativeSagaManager(TurnEpochClock(), registry=registry, journal=journal,
                                 auth_context=AuthorizationContext(credential=credentials["worker"].token))
    effects = []
    manager = Tasks(saga, effects)
    voice = VoiceIngress()
    voice.open_stream()
    proposal = complete(voice)
    step = TaskStep("move", versions=StepVersions("move", "reviewed-json:v1", "SIMULATED:executor:v1",
                                                "SIMULATED:provider:v1", "SIMULATED:local"))
    template = AdmissionTemplate("SIMULATED reviewed move", WorkflowVersion("reviewed-move", "v1"), (step,))
    service = ProposalAdmissionService(manager, voice, [template], source_id="SIMULATED:adapter")
    return SimpleNamespace(**locals(), args=[{"destination": "zone-b", "nested": {"items": [1]}}])


def review(env, **kwargs):
    return env.service.review(env.proposal, env.template.workflow.workflow_id, env.args,
                              token=env.credentials["reviewer"].token, **kwargs)


def cp(env, prepared):
    return env.journal.load_task(prepared.task_id)[1]


def counts(env):
    with closing(sqlite3.connect(env.journal.path)) as db:
        return {t: db.execute(f"SELECT count(*) FROM {t}").fetchone()[0] for t in
                ("task_checkpoints", "task_events", "actions", "approvals")}


def admit(env, prepared):
    return env.service.admit(prepared.review_id, token=env.credentials["reviewer"].token)


def test_review_is_ephemeral_explicit_admission_is_durable_but_has_no_execution_authority(env):
    prepared = review(env)
    assert counts(env) == {"task_checkpoints": 0, "task_events": 0, "actions": 0, "approvals": 0}
    receipt = admit(env, prepared)
    assert receipt.task_id == prepared.task_id and receipt.binding_digest == prepared.binding_digest
    assert counts(env) == {"task_checkpoints": 1, "task_events": 1, "actions": 0, "approvals": 0}
    assert env.effects == [] and env.manager.get_task(receipt.task_id) is None
    binding = cp(env, prepared)["proposal_binding"]
    assert binding == prepared.details()
    assert binding["reviewer"]["principal_id"] == "reviewer"
    assert binding["source"]["revision"] == 1 and binding["arguments"] == env.args
    assert env.proposal.text not in canonical(binding)
    with closing(sqlite3.connect(env.journal.path)) as db:
        event = db.execute("SELECT record FROM security_events WHERE kind='TASK_ADMITTED'").fetchall()
        assert len(event) == 1 and json.loads(event[0][0])["binding_digest"] == prepared.binding_digest
        assert env.proposal.text not in event[0][0] and env.credentials["reviewer"].token not in event[0][0]
    assert "zone-b" not in repr(prepared)


@pytest.mark.parametrize("actor", ["viewer", "no-domain", "other", None])
def test_default_deny_and_cross_principal_acceptance(env, actor):
    prepared = review(env)
    token = env.credentials[actor].token if actor else None
    with pytest.raises(PermissionDeniedError):
        env.service.admit(prepared.review_id, token=token)
    assert counts(env)["task_checkpoints"] == 0


@pytest.mark.parametrize("actor", ["viewer", "no-domain", None])
def test_review_also_requires_authenticated_creation_and_domain_grants(env, actor):
    with pytest.raises(PermissionDeniedError):
        env.service.review(env.proposal, "reviewed-move", env.args,
                           token=env.credentials[actor].token if actor else None)
    assert not env.service._pending


@pytest.mark.parametrize("change", ["revoke", "disable", "permission-version", "expiry", "rollback", "credential-swap"])
def test_authority_is_rechecked_at_atomic_admission(env, change):
    prepared = review(env, ttl_ms=100)
    token = env.credentials["reviewer"].token
    if change == "revoke":
        env.authority.revoke_credential(env.credentials["reviewer"].id)
    elif change == "disable":
        env.authority.update_principal("reviewer", active=False)
    elif change == "permission-version":
        env.authority.update_principal("reviewer", permissions={"ccs:task:create", "test:write"})
    elif change == "expiry":
        env.clock.now += 100
    elif change == "rollback":
        env.clock.now -= 1
    else:
        token = env.authority.issue_credential("reviewer").token
    with pytest.raises(JournalLeaseError if change == "rollback" else PermissionDeniedError):
        env.service.admit(prepared.review_id, token=token)
    assert counts(env)["task_checkpoints"] == 0


def test_revocation_between_service_validation_and_transaction_is_not_a_toctou_grant(env, monkeypatch):
    prepared = review(env)
    original = env.journal.admit_task
    def interleave(checkpoint, token):
        env.authority.revoke_credential(env.credentials["reviewer"].id)
        return original(checkpoint, token)
    monkeypatch.setattr(env.journal, "admit_task", interleave)
    with pytest.raises(PermissionDeniedError):
        admit(env, prepared)
    assert counts(env)["task_checkpoints"] == 0


@pytest.mark.parametrize("change", ["turn", "stream", "close", "invalidate", "forged-copy"])
def test_stale_or_forged_voice_evidence_cannot_be_admitted(env, change):
    prepared = review(env)
    if change == "turn":
        complete(env.voice, "new instructions")
    elif change == "stream":
        env.voice.open_stream()
    elif change == "close":
        env.voice.close_stream(env.voice._stream)
    elif change == "invalidate":
        env.voice.invalidate()
    else:
        with pytest.raises(ProposalAdmissionError):
            env.service.review(replace(env.proposal), "reviewed-move", env.args,
                               token=env.credentials["reviewer"].token)
        return
    with pytest.raises(ProposalAdmissionError):
        admit(env, prepared)
    assert counts(env)["task_checkpoints"] == 0


@pytest.mark.parametrize("confidence", [None, .1])
def test_unavailable_or_weak_acoustics_do_not_become_authority(env, confidence):
    env.proposal = complete(env.voice, confidence=confidence)
    with pytest.raises(ProposalAdmissionError):
        review(env)
    assert counts(env)["task_checkpoints"] == 0


def test_wrong_thread_and_unknown_review_fail_closed(env):
    prepared = review(env)
    with ThreadPoolExecutor(max_workers=1) as pool:
        with pytest.raises(VoiceEvidenceError):
            pool.submit(admit, env, prepared).result(timeout=5)
    with pytest.raises(ProposalAdmissionError):
        env.service.admit("unknown", token=env.credentials["reviewer"].token)


@pytest.mark.parametrize("change", ["revision", "manifest", "builder", "fields", "callback-binding"])
def test_deployment_and_handler_drift_require_new_review(env, change):
    if change == "callback-binding":
        async def original_status(action):
            return None
        env.step.status_check = original_status
        env.step.versions = replace(env.step.versions, reconciliation="SIMULATED:status:v1")
    prepared = review(env)
    if change == "revision":
        env.step.versions = replace(env.step.versions, executor="v2")
    elif change == "manifest":
        # Registry replacement is trusted deployment activity, never model input.
        env.registry._manifests["move"] = replace(env.registry.get("move"), timeout=8)
    elif change == "builder":
        env.step.build_args = lambda t, i: {"unreviewed": True}
    elif change == "fields":
        env.step.required_fields = ["mutable-voice-value"]
    else:
        # Same revision strings do not authorize replacement runtime handlers.
        async def status(action):
            return None
        env.step.status_check = status
    with pytest.raises((ProposalAdmissionError, WorkflowMismatchError)):
        admit(env, prepared)
    assert counts(env)["task_checkpoints"] == 0


def test_review_cannot_outlive_its_authenticated_credential(env):
    short = env.authority.issue_credential("reviewer", ttl_ms=10)
    prepared = env.service.review(env.proposal, "reviewed-move", env.args, token=short.token)
    assert prepared.details()["expires_ms"] == env.clock.now + 10
    env.clock.now += 10
    with pytest.raises(PermissionDeniedError):
        env.service.admit(prepared.review_id, token=short.token)
    assert counts(env)["task_checkpoints"] == 0


def test_request_and_review_view_mutations_cannot_change_sealed_arguments(env):
    prepared = review(env)
    expected = json.loads(canonical(env.args))
    env.args[0]["nested"]["items"].append("caller mutation")
    prepared.details()["arguments"][0]["destination"] = "elsewhere"
    admit(env, prepared)
    assert cp(env, prepared)["proposal_binding"]["arguments"] == expected


def test_replayed_acceptance_preserves_current_progress_and_one_admission_event(env):
    prepared = review(env)
    receipt = admit(env, prepared)
    version, checkpoint = env.journal.load_task(prepared.task_id)
    checkpoint["state"] = "CANCELLED"
    env.journal.save_task(prepared.task_id, checkpoint, version)
    assert admit(env, prepared) == receipt
    assert env.journal.load_task(prepared.task_id) == (1, checkpoint)
    assert counts(env)["task_events"] == 2
    with closing(sqlite3.connect(env.journal.path)) as db:
        assert db.execute("SELECT count(*) FROM security_events WHERE kind='TASK_ADMITTED'").fetchone()[0] == 1


def test_concurrent_sqlite_admissions_share_one_task_identity(env):
    prepared = review(env)
    checkpoint = json.loads(env.service._pending[prepared.review_id].checkpoint)
    peer = SQLiteActionJournal(env.journal.path, clock_ms=lambda: env.clock.now)
    peer.bind_lease(WorkerLease("SIMULATED-host", 1))
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(j.admit_task, checkpoint, env.credentials["reviewer"].token)
                   for j in (env.journal, peer)]
        for future in futures:
            future.result(timeout=5)
    assert counts(env) == {"task_checkpoints": 1, "task_events": 1, "actions": 0, "approvals": 0}


def test_transaction_failure_rolls_back_task_and_admission_event(env, monkeypatch):
    prepared = review(env)
    original = env.journal._save_task
    def fail(*args, **kwargs):
        original(*args, **kwargs)
        raise JournalError("SIMULATED transaction failure")
    monkeypatch.setattr(env.journal, "_save_task", fail)
    with pytest.raises(JournalError):
        admit(env, prepared)
    assert counts(env)["task_checkpoints"] == counts(env)["task_events"] == 0
    with closing(sqlite3.connect(env.journal.path)) as db:
        assert db.execute("SELECT count(*) FROM security_events WHERE kind='TASK_ADMITTED'").fetchone()[0] == 0
    monkeypatch.setattr(env.journal, "_save_task", original)
    admit(env, prepared)
    assert counts(env)["task_events"] == 1


def test_failure_after_security_event_still_rolls_back_entire_admission(env, monkeypatch):
    prepared = review(env)
    original = SecurityAuthority._event
    def fail(db, kind, now, **metadata):
        original(db, kind, now, **metadata)
        if kind == "TASK_ADMITTED":
            raise JournalError("SIMULATED failure after admission audit insert")
    monkeypatch.setattr(SecurityAuthority, "_event", staticmethod(fail))
    with pytest.raises(JournalError):
        admit(env, prepared)
    assert counts(env)["task_checkpoints"] == counts(env)["task_events"] == 0
    with closing(sqlite3.connect(env.journal.path)) as db:
        assert db.execute("SELECT count(*) FROM security_events WHERE kind='TASK_ADMITTED'").fetchone()[0] == 0


def test_lost_admission_ack_is_inspectable_and_explicit_replay_cannot_duplicate(env, monkeypatch):
    prepared = review(env)
    original = env.journal.admit_task
    def lost_ack(*args):
        original(*args)
        raise OSError("SIMULATED lost local acknowledgement")
    monkeypatch.setattr(env.journal, "admit_task", lost_ack)
    with pytest.raises(OSError):
        admit(env, prepared)
    assert cp(env, prepared)["proposal_binding"]["digest"] == prepared.binding_digest
    assert env.effects == []
    monkeypatch.setattr(env.journal, "admit_task", original)
    admit(env, prepared)
    assert counts(env)["task_events"] == 1


@pytest.mark.parametrize("change", ["remove", "arguments", "principal", "task-name"])
def test_admitted_provenance_cannot_be_rewritten_by_checkpoint_updates(env, change):
    prepared = review(env)
    admit(env, prepared)
    version, checkpoint = env.journal.load_task(prepared.task_id)
    if change == "remove":
        checkpoint.pop("proposal_binding")
    else:
        binding = checkpoint["proposal_binding"]
        if change == "arguments":
            binding["arguments"][0]["destination"] = "elsewhere"
        elif change == "principal":
            binding["reviewer"]["principal_id"] = "other"
        else:
            binding["task_name"] = checkpoint["name"] = "renamed"
        binding["digest"] = digest({k: v for k, v in binding.items() if k != "digest"})
    with pytest.raises(JournalError):
        env.journal.save_task(prepared.task_id, checkpoint, version)
    assert cp(env, prepared)["proposal_binding"]["digest"] == prepared.binding_digest


def test_pending_review_is_not_restored_as_authority_after_service_restart(env):
    prepared = review(env)
    fresh = ProposalAdmissionService(env.manager, env.voice, [env.template], source_id="SIMULATED:adapter")
    with pytest.raises(ProposalAdmissionError):
        fresh.admit(prepared.review_id, token=env.credentials["reviewer"].token)
    assert counts(env)["task_checkpoints"] == 0


@pytest.mark.asyncio
async def test_restore_executes_reviewed_arguments_only_after_separate_operation_approval(env):
    prepared = review(env)
    admit(env, prepared)
    # Restore through the real TaskManager, not a second execution path.
    task = env.manager.restore_task(prepared.task_id, steps=[env.step], workflow=env.template.workflow)
    complete(env.voice, "cancel that and move elsewhere")
    with pytest.raises(ConfirmationRequiredError):
        await env.manager.run_task(task.task_id)
    assert env.effects == [] and task.state == TaskState.WAITING_FOR_AUTHORIZATION
    env.authority.provision_principal("approver", {"ccs:approve", "test:write"})
    approver = env.authority.issue_credential("approver")
    action_id = task.step_action_ids[0]
    approval = env.authority.issue_approval(approver.token, "worker", action_id)
    env.saga.auth_context = AuthorizationContext(credential=env.credentials["worker"].token, approval_id=approval)
    await env.manager.run_task(task.task_id)
    assert task.state == TaskState.COMPLETED and env.effects == env.args
    env.manager.on_field_evicted("destination", "zone-b")
    await asyncio.sleep(0)
    assert task.state == TaskState.COMPLETED and len(env.effects) == 1
    assert env.manager.checkpoint(task.task_id)["proposal_binding"] == prepared.details()
    restarted = Tasks(env.saga, env.effects)
    restored = restarted.restore_task(task.task_id, steps=[env.step], workflow=env.template.workflow)
    await restarted.run_task(restored.task_id)
    assert len(env.effects) == 1


@pytest.mark.asyncio
async def test_admitted_task_preserves_unknown_effect_identity_without_retry(env):
    prepared = review(env)
    admit(env, prepared)
    task = env.manager.restore_task(prepared.task_id, steps=[env.step], workflow=env.template.workflow)
    with pytest.raises(ConfirmationRequiredError):
        await env.manager.run_task(task.task_id)
    env.authority.provision_principal("approver", {"ccs:approve", "test:write"})
    approver = env.authority.issue_credential("approver")
    action_id = task.step_action_ids[0]
    approval = env.authority.issue_approval(approver.token, "worker", action_id)
    env.saga.auth_context = AuthorizationContext(credential=env.credentials["worker"].token, approval_id=approval)
    async def lost(args):
        env.effects.append(json.loads(canonical(args)))
        raise TimeoutError("SIMULATED applied, response lost")
    env.manager._get_executor = lambda _: lost
    await env.manager.run_task(task.task_id)
    complete(env.voice, "correct the request")
    env.manager.on_field_evicted("destination", "zone-b")
    await env.manager.resume_task(task.task_id)
    assert task.state == TaskState.NEEDS_RECONCILIATION
    assert task.step_action_ids[0] == action_id and env.effects == env.args


@pytest.mark.asyncio
async def test_multistep_admission_keeps_each_argument_and_operation_approval_separate(env):
    second = replace(env.step, versions=replace(env.step.versions, step_id="second-move"))
    template = replace(env.template, steps=(env.step, second))
    service = ProposalAdmissionService(env.manager, env.voice, [template], source_id="SIMULATED:adapter")
    arguments = [env.args[0], {"destination": "zone-c", "nested": {"items": [2]}}]
    prepared = service.review(env.proposal, "reviewed-move", arguments,
                              token=env.credentials["reviewer"].token)
    service.admit(prepared.review_id, token=env.credentials["reviewer"].token)
    task = env.manager.restore_task(prepared.task_id, steps=list(template.steps), workflow=template.workflow)
    env.authority.provision_principal("approver", {"ccs:approve", "test:write"})
    approver = env.authority.issue_credential("approver")
    with pytest.raises(ConfirmationRequiredError):
        await env.manager.run_task(task.task_id)
    first_id = task.step_action_ids[0]
    first_approval = env.authority.issue_approval(approver.token, "worker", first_id)
    env.saga.auth_context = AuthorizationContext(credential=env.credentials["worker"].token,
                                               approval_id=first_approval)
    with pytest.raises(ConfirmationRequiredError):
        await env.manager.run_task(task.task_id)
    assert env.effects == arguments[:1] and task.current_step_index == 1
    second_id = task.step_action_ids[1]
    assert second_id != first_id
    complete(env.voice, "a new unrelated conversation")
    env.manager.on_field_evicted("destination", "zone-b")
    second_approval = env.authority.issue_approval(approver.token, "worker", second_id)
    env.saga.auth_context = AuthorizationContext(credential=env.credentials["worker"].token,
                                               approval_id=second_approval)
    await env.manager.run_task(task.task_id)
    assert task.state == TaskState.COMPLETED and env.effects == arguments
    assert task.step_action_ids == [first_id, second_id]
    assert env.manager.checkpoint(task.task_id)["proposal_binding"]["arguments"] == arguments


def test_allowlist_rejects_lazy_or_voice_dependent_workflows(env):
    for step in (replace(env.step, build_args=lambda t, i: {}), replace(env.step, required_fields=["voice"])):
        with pytest.raises(ProposalAdmissionError, match="STATIC_ARGUMENTS"):
            ProposalAdmissionService(env.manager, env.voice, [replace(env.template, steps=(step,))],
                                     source_id="SIMULATED:adapter")


def test_review_rejects_argument_count_different_from_workflow(env):
    with pytest.raises(ProposalAdmissionError, match="BINDING_MISMATCH"):
        env.service.review(env.proposal, "reviewed-move", [{}, {}], token=env.credentials["reviewer"].token)
    assert not env.service._pending and counts(env)["task_checkpoints"] == 0


def test_unlisted_workflow_cannot_be_reviewed(env):
    with pytest.raises(ProposalAdmissionError, match="NOT_ALLOWLISTED"):
        env.service.review(env.proposal, "unlisted", env.args, token=env.credentials["reviewer"].token)


@pytest.mark.parametrize("value", [[], [{}] * 33, [{1: "coerced"}], [{"x": (1, 2)}],
                                   [{"x": float("nan")}], [{"x": float("inf")}],
                                   [{"x": "x" * 65537}], [{"x": object()}], [{"x": 1 << 4096}]])
def test_arguments_have_explicit_json_and_capacity_bounds(value):
    with pytest.raises(ProposalAdmissionError):
        bounded_arguments(value)


def test_argument_depth_cycles_and_node_count_are_bounded():
    cycle = {}
    cycle["self"] = cycle
    deep = {}
    for _ in range(33):
        deep = {"nested": deep}
    for value in ([cycle], [deep], [{"nodes": [None] * 4096}]):
        with pytest.raises(ProposalAdmissionError, match="CAPACITY"):
            bounded_arguments(value)


def test_pending_capacity_and_expiry_are_bounded(env):
    env.service.max_pending = 1
    first = review(env, ttl_ms=10)
    with pytest.raises(ProposalAdmissionError, match="CAPACITY"):
        review(env)
    env.clock.now += 10
    second = review(env)
    assert first.review_id not in env.service._pending and second.review_id in env.service._pending


def test_new_checkpoint_identity_cannot_overwrite_an_existing_task(env):
    prepared = review(env)
    admit(env, prepared)
    checkpoint = cp(env, prepared)
    checkpoint["proposal_binding"]["review_id"] = "different-review"
    binding = checkpoint["proposal_binding"]
    binding["digest"] = digest({k: v for k, v in binding.items() if k != "digest"})
    with pytest.raises(JournalConflict):
        env.journal.admit_task(checkpoint, env.credentials["reviewer"].token)


def test_generic_checkpoint_creation_cannot_bypass_authenticated_admission(env):
    prepared = review(env)
    checkpoint = json.loads(env.service._pending[prepared.review_id].checkpoint)
    with pytest.raises(PermissionDeniedError, match="authenticated admission"):
        env.journal.save_task(prepared.task_id, checkpoint, None)
    assert counts(env)["task_checkpoints"] == 0


def test_schema6_requires_offline_migration_preserves_records_and_fences_old_handle(env):
    prepared = review(env)
    admit(env, prepared)
    before = cp(env, prepared)
    with closing(sqlite3.connect(env.journal.path)) as db:
        # A synthetic v6 fixture has no admitted record; retain ordinary task data.
        before.pop("proposal_binding")
        payload = canonical(before)
        db.execute("UPDATE task_checkpoints SET checkpoint=?", (payload,))
        db.execute("UPDATE task_events SET checkpoint=?", (payload,))
        db.execute("PRAGMA user_version=6")
        db.commit()
    with pytest.raises(JournalError, match="migration"):
        SQLiteActionJournal(env.journal.path, clock_ms=lambda: env.clock.now)
    with pytest.raises(JournalError, match="expired/released"):
        SQLiteActionJournal(env.journal.path, clock_ms=lambda: env.clock.now, migrate=True)
    with pytest.raises(JournalError, match="schema changed"):
        env.journal.save_task(prepared.task_id, before, 0)
    env.clock.now += 3600000
    migrated = SQLiteActionJournal(env.journal.path, clock_ms=lambda: env.clock.now, migrate=True)
    assert migrated.load_task(prepared.task_id) == (0, before)
    with closing(sqlite3.connect(migrated.path)) as db:
        assert db.execute("PRAGMA user_version").fetchone()[0] == 7


@pytest.mark.parametrize("schema", [6, 7])
def test_backup_reads_legacy_and_new_evidence_without_authorizing_activation(env, tmp_path, schema):
    if schema == 7:
        prepared = review(env)
        admit(env, prepared)
    else:
        with closing(sqlite3.connect(env.journal.path)) as db:
            db.execute("PRAGMA user_version=6")
    inspected = recovery_preflight.inspect(env.journal.path)
    assert inspected["passed"] and inspected["journal_schema"] == schema
    target = tmp_path / "snapshot.ccsb"
    assert journal_backup.capture(env.journal.path, target)["passed"]
    result = journal_backup.drill(target, tmp_path)
    assert result["passed"] and not result["dispatch_authorized"] and not result["restore_authorized"]
    # Even an otherwise valid bundle cannot lie about its checkpoint semantics.
    raw = target.read_bytes()
    start = len(journal_backup.MAGIC)
    size = struct.unpack(">I", raw[start:start + 4])[0]
    manifest = json.loads(raw[start + 4:start + 4 + size])
    manifest["journal_schema"] = 7 if schema == 6 else 6
    encoded = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode("ascii")
    target.write_bytes(journal_backup.MAGIC + struct.pack(">I", len(encoded)) + encoded + raw[start + 4 + size:])
    assert not journal_backup.drill(target, tmp_path)["passed"]
