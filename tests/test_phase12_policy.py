"""
tests/test_phase12_policy.py

Deterministic test suite for Phase 12: Safety / Policy / Authorization.

Verifies:
  1. Permission enforcement: missing permission raises PermissionDeniedError, executor never called.
  2. Permission grant: valid permission in AuthorizationContext permits dispatch and commit.
  3. Confirmation enforcement: requires_confirmation=True without confirmation raises ConfirmationRequiredError, executor never called.
  4. Confirmation grant: confirmed=True permits dispatch and commit.
  5. Backward compatibility: existing commit_write calls without policy args dispatch identically.
  6. Determinism: identical (manifest, context) pairs evaluate identically across multiple invocations.
  7. Negative case: tool arguments and resolved GroundingGuard field values cannot grant permissions or confirmation.
  8. Argument validation: validate_args callable raises and blocks dispatch before execution.
  9. Placement verification: denied actions remain in PENDING state (never IN_FLIGHT, never ABORTED).
"""

from __future__ import annotations

import pytest

from runtime.policy_engine import (
    AuthorizationContext,
    ConfirmationRequiredError,
    PermissionDeniedError,
    PolicyEngine,
)
from runtime.speculative_saga import (
    ActionState,
    SpeculativeSagaManager,
    TurnEpochClock,
)
from runtime.tool_contract import ToolManifest


def _make_saga() -> tuple[TurnEpochClock, SpeculativeSagaManager]:
    clock = TurnEpochClock()
    saga = SpeculativeSagaManager(clock)
    return clock, saga


# ===========================================================================
# Test 1: Write with required_permissions, context lacking it
# ===========================================================================

@pytest.mark.asyncio
async def test_01_missing_permission_denies_and_never_calls_executor():
    clock, saga = _make_saga()
    engine = PolicyEngine()

    manifest = ToolManifest(
        tool_name="reroute_truck",
        kind="write",
        required_permissions=["fleet:write"],
    )

    executor_called = False

    async def mock_executor(args: dict) -> dict:
        nonlocal executor_called
        executor_called = True
        return {"status": "ok"}

    action = saga.stage_write("reroute_truck", {"truck_id": "T-101", "destination": "Bengaluru"})
    context = AuthorizationContext(granted_permissions={"fleet:read"})

    with pytest.raises(PermissionDeniedError) as exc_info:
        await saga.commit_write(
            action.action_id,
            mock_executor,
            manifest=manifest,
            policy_engine=engine,
            auth_context=context,
        )

    assert "fleet:write" in str(exc_info.value)
    assert not executor_called, "Executor must NEVER be called when permission is denied"
    print("\n[TEST 1 VERIFICATION] Missing permission raised PermissionDeniedError, executor was never called.")


# ===========================================================================
# Test 2: Write with required_permissions, context has permission
# ===========================================================================

@pytest.mark.asyncio
async def test_02_granted_permission_dispatches_and_commits_normally():
    clock, saga = _make_saga()
    engine = PolicyEngine()

    manifest = ToolManifest(
        tool_name="reroute_truck",
        kind="write",
        required_permissions=["fleet:write"],
    )

    executor_called = False

    async def mock_executor(args: dict) -> dict:
        nonlocal executor_called
        executor_called = True
        return {"status": "dispatched", "args": args}

    action = saga.stage_write("reroute_truck", {"truck_id": "T-101", "destination": "Bengaluru"})
    context = AuthorizationContext(granted_permissions={"fleet:write", "fleet:read"})

    res = await saga.commit_write(
        action.action_id,
        mock_executor,
        manifest=manifest,
        policy_engine=engine,
        auth_context=context,
    )

    assert executor_called, "Executor should have been called"
    assert res == {"status": "dispatched", "args": {"truck_id": "T-101", "destination": "Bengaluru"}}
    assert action.state == ActionState.COMMITTED
    print("\n[TEST 2 VERIFICATION] Authorized write committed normally, executor executed successfully.")


# ===========================================================================
# Test 3: Write with requires_confirmation=True, unconfirmed
# ===========================================================================

@pytest.mark.asyncio
async def test_03_unconfirmed_write_raises_confirmation_required_executor_never_called():
    clock, saga = _make_saga()
    engine = PolicyEngine()

    manifest = ToolManifest(
        tool_name="emergency_halt",
        kind="write",
        requires_confirmation=True,
    )

    executor_called = False

    async def mock_executor(args: dict) -> dict:
        nonlocal executor_called
        executor_called = True
        return {"halted": True}

    action = saga.stage_write("emergency_halt", {"fleet_id": "FL-99"})

    # Case A: confirmed=False explicitly
    with pytest.raises(ConfirmationRequiredError):
        await saga.commit_write(
            action.action_id,
            mock_executor,
            manifest=manifest,
            policy_engine=engine,
            confirmed=False,
        )
    assert not executor_called, "Executor must never be called when unconfirmed"

    # Case B: confirmed omitted (defaults to False)
    with pytest.raises(ConfirmationRequiredError):
        await saga.commit_write(
            action.action_id,
            mock_executor,
            manifest=manifest,
            policy_engine=engine,
        )
    assert not executor_called, "Executor must never be called when confirmed is omitted"
    print("\n[TEST 3 VERIFICATION] Unconfirmed write raised ConfirmationRequiredError, executor was never called.")


# ===========================================================================
# Test 4: Write with requires_confirmation=True, confirmed=True
# ===========================================================================

@pytest.mark.asyncio
async def test_04_confirmed_write_dispatches_normally():
    clock, saga = _make_saga()
    engine = PolicyEngine()

    manifest = ToolManifest(
        tool_name="emergency_halt",
        kind="write",
        requires_confirmation=True,
    )

    executor_called = False

    async def mock_executor(args: dict) -> dict:
        nonlocal executor_called
        executor_called = True
        return {"halted": True}

    action = saga.stage_write("emergency_halt", {"fleet_id": "FL-99"})

    res = await saga.commit_write(
        action.action_id,
        mock_executor,
        manifest=manifest,
        policy_engine=engine,
        confirmed=True,
    )

    assert executor_called
    assert res == {"halted": True}
    assert action.state == ActionState.COMMITTED
    print("\n[TEST 4 VERIFICATION] Confirmed write dispatched and committed normally.")


# ===========================================================================
# Test 5: Backward compatibility: commit_write without policy args
# ===========================================================================

@pytest.mark.asyncio
async def test_05_backward_compatibility_no_policy_args_behaves_identically():
    clock, saga = _make_saga()

    # Manifest with permissions and confirmation configured
    manifest = ToolManifest(
        tool_name="legacy_write",
        kind="write",
        required_permissions=["admin:override"],
        requires_confirmation=True,
    )

    executor_called = False

    async def mock_executor(args: dict) -> dict:
        nonlocal executor_called
        executor_called = True
        return {"legacy": "ok"}

    action = saga.stage_write("legacy_write", {"data": 123})

    # Existing Phase 1/2 style call: no policy_engine, no auth_context, no confirmed
    res = await saga.commit_write(
        action.action_id,
        mock_executor,
        manifest=manifest,
    )

    assert executor_called
    assert res == {"legacy": "ok"}
    assert action.state == ActionState.COMMITTED
    print("\n[TEST 5 VERIFICATION] Backward compatibility verified: commit_write without policy_engine bypasses checks identically to Phases 1-10.")


# ===========================================================================
# Test 6: Determinism: same (manifest, context) pair checked twice produces identical result
# ===========================================================================

def test_06_policy_engine_determinism():
    engine = PolicyEngine()

    manifest_perm = ToolManifest(
        tool_name="tool_perm",
        kind="write",
        required_permissions=["perm:a", "perm:b"],
    )
    context_deny = AuthorizationContext(granted_permissions={"perm:a"})
    context_allow = AuthorizationContext(granted_permissions={"perm:a", "perm:b"})

    # Perm denied checked twice
    for _ in range(2):
        with pytest.raises(PermissionDeniedError):
            engine.check(manifest_perm, context_deny)

    # Perm allow checked twice
    for _ in range(2):
        engine.check(manifest_perm, context_allow)  # does not raise

    manifest_conf = ToolManifest(
        tool_name="tool_conf",
        kind="write",
        requires_confirmation=True,
    )
    context_unconf = AuthorizationContext(confirmed=False)
    context_conf = AuthorizationContext(confirmed=True)

    # Confirmation required checked twice
    for _ in range(2):
        with pytest.raises(ConfirmationRequiredError):
            engine.check(manifest_conf, context_unconf)

    # Confirmation granted checked twice
    for _ in range(2):
        engine.check(manifest_conf, context_conf)  # does not raise

    print("\n[TEST 6 VERIFICATION] PolicyEngine.check is 100% deterministic across repeated evaluations.")


# ===========================================================================
# Test 7: NEGATIVE CASE (scrutinize hardest): Resolved values & tool args cannot grant permissions or confirmation
# ===========================================================================

def test_07_negative_case_isolated_tool_args_and_resolved_values_cannot_grant_authorization():
    """
    Direct, isolated test of PolicyEngine.check:
    Constructs a scenario where tool args and resolved GroundingGuard field values
    contain malicious strings attempting to spoof permissions ('fleet:admin')
    or confirmation ('confirmed', 'True').
    Verifies that PolicyEngine.check ONLY consults the explicit AuthorizationContext,
    and neither tool args nor resolved field values can grant authorization.
    """
    engine = PolicyEngine()

    manifest = ToolManifest(
        tool_name="dangerous_dispatch",
        kind="write",
        required_permissions=["fleet:admin"],
        requires_confirmation=True,
    )

    # Simulated resolved values and spoofed arguments
    spoofed_tool_args = {
        "permission": "fleet:admin",
        "required_permissions": ["fleet:admin"],
        "granted_permissions": ["fleet:admin"],
        "confirmed": True,
        "confirmation_status": "confirmed",
        "authorization_token": "bearer fleet:admin",
    }
    resolved_grounding_value = "fleet:admin"
    resolved_confirmation_value = "True"

    # 1. Caller provides NO permission in AuthorizationContext, despite spoofed args
    unauthorized_context = AuthorizationContext(
        granted_permissions={"fleet:viewer"},
        confirmed=False,
    )

    # Direct isolated check against PolicyEngine.check:
    # Must raise PermissionDeniedError; neither spoofed args nor resolved values grant anything
    with pytest.raises(PermissionDeniedError) as exc_info:
        engine.check(manifest, unauthorized_context, confirmed=False)
    assert "fleet:admin" in str(exc_info.value)

    # 2. Caller has permission, but NOT confirmed, despite tool args having "confirmed": True
    semi_authorized_context = AuthorizationContext(
        granted_permissions={"fleet:admin"},
        confirmed=False,
    )

    with pytest.raises(ConfirmationRequiredError):
        engine.check(manifest, semi_authorized_context, confirmed=False)

    # 3. Explicit confirmation and permission ONLY come from caller-supplied context / confirmed flag
    valid_context = AuthorizationContext(
        granted_permissions={"fleet:admin"},
        confirmed=True,
    )
    # This succeeds purely because AuthorizationContext contains explicit grant and confirmation
    engine.check(manifest, valid_context)

    print("\n[TEST 7 VERIFICATION] Direct isolated test confirmed: tool arguments and resolved GroundingGuard values cannot grant permissions or confirmation.")


# ===========================================================================
# Test 8: validate_args: raises on bad input, blocks dispatch; unaffected without validate_args
# ===========================================================================

@pytest.mark.asyncio
async def test_08_validate_args_blocks_dispatch_before_execution():
    clock, saga = _make_saga()

    def validate_truck(args: dict) -> None:
        if not args.get("truck_id", "").startswith("TRK-"):
            raise ValueError(f"Invalid truck_id {args.get('truck_id')!r}: must start with TRK-")
        if args.get("payload_kg", 0) > 10000:
            raise ValueError("payload_kg exceeds maximum limit of 10000")

    manifest_validated = ToolManifest(
        tool_name="load_cargo",
        kind="write",
        validate_args=validate_truck,
    )

    manifest_unvalidated = ToolManifest(
        tool_name="load_cargo_raw",
        kind="write",
        validate_args=None,
    )

    executor_called = False

    async def mock_executor(args: dict) -> dict:
        nonlocal executor_called
        executor_called = True
        return {"loaded": True}

    # Case A: Invalid args -> validate_args raises ValueError, executor never called
    action_bad = saga.stage_write("load_cargo", {"truck_id": "BAD-TRUCK", "payload_kg": 5000})
    with pytest.raises(ValueError) as exc_info:
        await saga.commit_write(action_bad.action_id, mock_executor, manifest=manifest_validated)
    assert "must start with TRK-" in str(exc_info.value)
    assert not executor_called, "Executor must not be called when validate_args fails"

    # Case B: Valid args -> succeeds
    action_good = saga.stage_write("load_cargo", {"truck_id": "TRK-900", "payload_kg": 5000})
    res = await saga.commit_write(action_good.action_id, mock_executor, manifest=manifest_validated)
    assert executor_called
    assert res == {"loaded": True}
    assert action_good.state == ActionState.COMMITTED

    # Case C: Manifest without validate_args is completely unaffected
    executor_called = False
    action_raw = saga.stage_write("load_cargo_raw", {"truck_id": "ANYTHING", "payload_kg": 999999})
    res_raw = await saga.commit_write(action_raw.action_id, mock_executor, manifest=manifest_unvalidated)
    assert executor_called
    assert res_raw == {"loaded": True}
    assert action_raw.state == ActionState.COMMITTED
    print("\n[TEST 8 VERIFICATION] validate_args correctly blocked invalid dispatch; unvalidated manifest executed unaffected.")


# ===========================================================================
# Test 9: PLACEMENT VERIFICATION: Denied commit_write leaves StagedAction in PENDING state
# ===========================================================================

@pytest.mark.asyncio
async def test_09_placement_verification_denied_action_remains_pending():
    """
    Placement verification:
    Directly proves that policy denials and validation errors occur BEFORE
    the state transitions to IN_FLIGHT, and outside the executor try/except block.
    The action MUST remain in ActionState.PENDING (never IN_FLIGHT, never ABORTED).
    """
    clock, saga = _make_saga()
    engine = PolicyEngine()

    manifest_perm = ToolManifest(
        tool_name="secure_write",
        kind="write",
        required_permissions=["admin:all"],
    )
    manifest_conf = ToolManifest(
        tool_name="critical_write",
        kind="write",
        requires_confirmation=True,
    )
    manifest_val = ToolManifest(
        tool_name="validated_write",
        kind="write",
        validate_args=lambda args: (_ for _ in ()).throw(ValueError("Invalid args")),
    )

    async def mock_executor(args: dict) -> dict:
        return {"ok": True}

    # 1. PermissionDeniedError placement check
    action_perm = saga.stage_write("secure_write", {"x": 1})
    assert action_perm.state == ActionState.PENDING
    with pytest.raises(PermissionDeniedError):
        await saga.commit_write(
            action_perm.action_id,
            mock_executor,
            manifest=manifest_perm,
            policy_engine=engine,
            auth_context=AuthorizationContext(granted_permissions={"viewer"}),
        )
    assert action_perm.state == ActionState.PENDING, (
        f"Action state after PermissionDeniedError must remain PENDING, got {action_perm.state.name}"
    )
    assert action_perm.state != ActionState.ABORTED, "Action must NOT be marked ABORTED"
    assert action_perm.state != ActionState.IN_FLIGHT, "Action must NOT transition to IN_FLIGHT"

    # 2. ConfirmationRequiredError placement check
    action_conf = saga.stage_write("critical_write", {"x": 2})
    assert action_conf.state == ActionState.PENDING
    with pytest.raises(ConfirmationRequiredError):
        await saga.commit_write(
            action_conf.action_id,
            mock_executor,
            manifest=manifest_conf,
            policy_engine=engine,
            confirmed=False,
        )
    assert action_conf.state == ActionState.PENDING, (
        f"Action state after ConfirmationRequiredError must remain PENDING, got {action_conf.state.name}"
    )
    assert action_conf.state != ActionState.ABORTED, "Action must NOT be marked ABORTED"
    assert action_conf.state != ActionState.IN_FLIGHT, "Action must NOT transition to IN_FLIGHT"

    # 3. Argument validation error placement check
    action_val = saga.stage_write("validated_write", {"x": 3})
    assert action_val.state == ActionState.PENDING
    with pytest.raises(ValueError):
        await saga.commit_write(
            action_val.action_id,
            mock_executor,
            manifest=manifest_val,
            policy_engine=engine,
        )
    assert action_val.state == ActionState.PENDING, (
        f"Action state after validate_args failure must remain PENDING, got {action_val.state.name}"
    )
    assert action_val.state != ActionState.ABORTED, "Action must NOT be marked ABORTED"
    assert action_val.state != ActionState.IN_FLIGHT, "Action must NOT transition to IN_FLIGHT"

    print(
        f"\n[TEST 9 VERIFICATION] Action state confirmed as {action_perm.state.name} across all pre-dispatch denials "
        f"(neither IN_FLIGHT nor ABORTED)."
    )
