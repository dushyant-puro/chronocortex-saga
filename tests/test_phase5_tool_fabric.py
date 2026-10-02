"""
tests/test_phase5_tool_fabric.py

New Phase 5 (PRISM roadmap) — Tool/Action Fabric:
1. commit_write rejects tools manifested as kind="read" with InvalidToolKindError
2. fire_speculative_read rejects tools manifested as kind="write" with InvalidToolKindError
3. Regression: all 5 existing fleet tools pass kind check correctly (no false rejections)
4. query_telemetry -> query_traffic dependency registers and topological_order() places telemetry before traffic
5. Constructed cycle test: register A depends_on B, B depends_on A -> raises CyclicDependencyError
6. Deterministic topological_order() returns identical order across multiple calls
7. required_permissions field is inert: declaring permissions does not alter or block callability
8. Phase 4 prediction rule regression with TOOL_REGISTRY
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from runtime.grounding_guard import GroundingGuard
from runtime.speculative_saga import (
    ActionState,
    SpeculativeSagaManager,
    TurnEpochClock,
)
from runtime.tool_contract import (
    CyclicDependencyError,
    InvalidToolKindError,
    ToolContractError,
    ToolManifest,
    ToolRegistry,
)
from tools.fleet_tools import (
    READ_TOOLS,
    TOOL_MANIFEST,
    TOOL_REGISTRY,
    WRITE_TOOLS,
)


def _make_env() -> tuple[TurnEpochClock, SpeculativeSagaManager, GroundingGuard]:
    clock = TurnEpochClock()
    saga = SpeculativeSagaManager(clock, registry=TOOL_REGISTRY)
    guard = GroundingGuard(epoch_clock=clock)
    return clock, saga, guard


# ============================================================================
# 1. COMMIT_WRITE REJECTS READ TOOLS
# ============================================================================

@pytest.mark.asyncio
async def test_01_commit_write_rejects_read_tool():
    """commit_write must verify, before dispatching, that the tool being
    invoked is manifested with kind='write'. If a caller attempts to
    commit_write a tool manifested as 'read', raise InvalidToolKindError."""
    clock, saga, _ = _make_env()

    async def mock_executor(args: dict[str, Any]) -> dict[str, Any]:
        return {"data": args}

    # query_telemetry is manifested as kind="read" in TOOL_REGISTRY
    action = saga.stage_write("query_telemetry", {"truck_id": "truck-17"})

    with pytest.raises(InvalidToolKindError) as exc_info:
        await saga.commit_write(action.action_id, mock_executor)

    assert "commit_write requires a tool with kind='write'" in str(exc_info.value)
    assert "query_telemetry" in str(exc_info.value)
    assert "read" in str(exc_info.value)
    # Ensure it did not transition to IN_FLIGHT
    assert action.state == ActionState.PENDING


# ============================================================================
# 2. FIRE_SPECULATIVE_READ REJECTS WRITE TOOLS
# ============================================================================

@pytest.mark.asyncio
async def test_02_fire_speculative_read_rejects_write_tool():
    """fire_speculative_read must verify, before dispatching, that the tool
    being invoked is manifested with kind='read'. If a caller attempts to
    fire_speculative_read a tool manifested as 'write', raise InvalidToolKindError."""
    clock, saga, _ = _make_env()

    async def mock_executor(args: dict[str, Any]) -> dict[str, Any]:
        return {"data": args}

    # reroute_truck is manifested as kind="write" in TOOL_REGISTRY
    with pytest.raises(InvalidToolKindError) as exc_info:
        saga.fire_speculative_read(
            tool_name="reroute_truck",
            args={"truck_id": "truck-17", "destination": "Chennai"},
            executor=mock_executor,
            entity_hash="reroute:truck-17",
        )

    assert "fire_speculative_read requires a tool with kind='read'" in str(exc_info.value)
    assert "reroute_truck" in str(exc_info.value)
    assert "write" in str(exc_info.value)


# ============================================================================
# 3. REGRESSION: ALL 5 FLEET TOOLS PASS KIND CHECKS
# ============================================================================

@pytest.mark.asyncio
async def test_03_all_fleet_tools_pass_kind_checks():
    """All 5 fleet tools in TOOL_REGISTRY must pass kind enforcement with zero
    false rejections when used via their proper dispatch path."""
    clock, saga, _ = _make_env()

    # 1. query_telemetry (read)
    a_telem = saga.fire_speculative_read(
        tool_name="query_telemetry",
        args={"truck_id": "truck-17"},
        executor=READ_TOOLS["query_telemetry"],
        entity_hash="truck_id:truck-17",
    )
    await a_telem._task
    assert a_telem.state == ActionState.COMMITTED

    # 2. query_traffic (read)
    a_traffic = saga.fire_speculative_read(
        tool_name="query_traffic",
        args={"route": "Bengaluru"},
        executor=READ_TOOLS["query_traffic"],
        entity_hash="route:Bengaluru",
    )
    await a_traffic._task
    assert a_traffic.state == ActionState.COMMITTED

    # 3. query_dock_availability (read)
    a_dock = saga.fire_speculative_read(
        tool_name="query_dock_availability",
        args={"dock_id": "D-1"},
        executor=READ_TOOLS["query_dock_availability"],
        entity_hash="dock_id:D-1",
    )
    await a_dock._task
    assert a_dock.state == ActionState.COMMITTED

    # 4. reroute_truck (write)
    fw_reroute, comp_reroute = WRITE_TOOLS["reroute_truck"]
    a_reroute = saga.stage_write(
        tool_name="reroute_truck",
        args={"truck_id": "truck-17", "destination": "Bengaluru"},
        compensate=comp_reroute,
    )
    res_reroute = await saga.commit_write(a_reroute.action_id, fw_reroute)
    assert a_reroute.state == ActionState.COMMITTED
    assert res_reroute.new_route == "Bengaluru"

    # 5. reserve_dock (write)
    fw_dock, comp_dock = WRITE_TOOLS["reserve_dock"]
    a_res_dock = saga.stage_write(
        tool_name="reserve_dock",
        args={"dock_id": "D-99", "truck_id": "truck-17"},
        compensate=comp_dock,
    )
    res_dock = await saga.commit_write(a_res_dock.action_id, fw_dock)
    assert a_res_dock.state == ActionState.COMMITTED
    assert res_dock.dock_id == "D-99"


# ============================================================================
# 4. DEPENDENCY REGISTRATION & TOPOLOGICAL ORDERING
# ============================================================================

def test_04_dependency_topological_ordering():
    """query_telemetry -> query_traffic dependency registers and
    topological_order() places telemetry before traffic."""
    traffic_mf = TOOL_REGISTRY.get("query_traffic")
    assert traffic_mf is not None
    assert "query_telemetry" in traffic_mf.depends_on

    order = TOOL_REGISTRY.topological_order()
    assert "query_telemetry" in order
    assert "query_traffic" in order

    idx_telemetry = order.index("query_telemetry")
    idx_traffic = order.index("query_traffic")
    assert idx_telemetry < idx_traffic, (
        f"Expected query_telemetry (index {idx_telemetry}) to precede "
        f"query_traffic (index {idx_traffic}) in topological order {order}"
    )


# ============================================================================
# 5. CONSTRUCTED CYCLE TEST
# ============================================================================

def test_05_constructed_cycle_raises_error():
    """Register two manifests where A depends_on B and B depends_on A.
    Assert registration raises CyclicDependencyError rather than succeeding or hanging."""
    reg = ToolRegistry()

    manifest_a = ToolManifest(
        tool_name="tool_a",
        kind="read",
        idempotent=True,
        cancellable=True,
        requires_authoritative_commit=False,
        depends_on=["tool_b"],
    )
    manifest_b = ToolManifest(
        tool_name="tool_b",
        kind="read",
        idempotent=True,
        cancellable=True,
        requires_authoritative_commit=False,
        depends_on=["tool_a"],
    )

    # First registration succeeds (tool_b not yet registered)
    reg.register(manifest_a)
    assert "tool_a" in reg

    # Second registration creates cycle: tool_a -> tool_b -> tool_a
    with pytest.raises(CyclicDependencyError) as exc_info:
        reg.register(manifest_b)

    error_msg = str(exc_info.value)
    assert "Cyclic dependency detected" in error_msg
    # Ensure registry was not polluted with tool_b
    assert "tool_b" not in reg
    assert len(reg) == 1


# ============================================================================
# 6. DETERMINISTIC TOPOLOGICAL ORDER
# ============================================================================

def test_06_topological_order_is_deterministic():
    """topological_order() called multiple times on the same registered set
    must return identical order every time."""
    order1 = TOOL_REGISTRY.topological_order()
    order2 = TOOL_REGISTRY.topological_order()
    order3 = TOOL_REGISTRY.topological_order()

    assert order1 == order2 == order3
    assert len(order1) == len(TOOL_REGISTRY)


# ============================================================================
# 7. REQUIRED_PERMISSIONS FIELD IS INERT (METADATA ONLY)
# ============================================================================

@pytest.mark.asyncio
async def test_07_required_permissions_inert():
    """Declaring required_permissions on a tool does NOT block, filter, or alter
    its dispatch capability. Calls succeed identically before and after permissions
    are attached."""
    clock = TurnEpochClock()
    reg = ToolRegistry()
    saga = SpeculativeSagaManager(clock, registry=reg)

    async def echo_executor(args: dict[str, Any]) -> dict[str, Any]:
        return {"result": "ok", "args": args}

    # 1. Unrestricted manifests (no permissions declared)
    mf_read_plain = ToolManifest(
        tool_name="echo_read",
        kind="read",
        idempotent=True,
        cancellable=True,
        requires_authoritative_commit=False,
        required_permissions=[],
    )
    mf_write_plain = ToolManifest(
        tool_name="echo_write",
        kind="write",
        idempotent=False,
        cancellable=False,
        requires_authoritative_commit=True,
        required_permissions=[],
    )
    reg.register(mf_read_plain)
    reg.register(mf_write_plain)

    a_r1 = saga.fire_speculative_read("echo_read", {"q": 1}, echo_executor, "echo_read:1")
    await a_r1._task
    assert a_r1.state == ActionState.COMMITTED

    a_w1 = saga.stage_write("echo_write", {"w": 1})
    res_w1 = await saga.commit_write(a_w1.action_id, echo_executor)
    assert a_w1.state == ActionState.COMMITTED
    assert res_w1 == {"result": "ok", "args": {"w": 1}}

    # 2. Restricted manifests (high-privilege permissions declared as metadata)
    reg_restricted = ToolRegistry()
    saga_restricted = SpeculativeSagaManager(clock, registry=reg_restricted)

    mf_read_perm = ToolManifest(
        tool_name="secure_telematics",
        kind="read",
        idempotent=True,
        cancellable=True,
        requires_authoritative_commit=False,
        required_permissions=["fleet:telematics:read", "audit:pii:access"],
    )
    mf_write_perm = ToolManifest(
        tool_name="secure_dispatch",
        kind="write",
        idempotent=False,
        cancellable=False,
        requires_authoritative_commit=True,
        required_permissions=["fleet:routing:write", "admin:super_override"],
    )
    reg_restricted.register(mf_read_perm)
    reg_restricted.register(mf_write_perm)

    # Calling secure_telematics must execute successfully without being blocked
    a_r2 = saga_restricted.fire_speculative_read(
        "secure_telematics", {"truck": "t-8"}, echo_executor, "secure_telematics:t-8"
    )
    await a_r2._task
    assert a_r2.state == ActionState.COMMITTED
    assert a_r2.result == {"result": "ok", "args": {"truck": "t-8"}}

    # Calling secure_dispatch must commit successfully without being blocked
    a_w2 = saga_restricted.stage_write("secure_dispatch", {"route": "R-9"})
    res_w2 = await saga_restricted.commit_write(a_w2.action_id, echo_executor)
    assert a_w2.state == ActionState.COMMITTED
    assert res_w2 == {"result": "ok", "args": {"route": "R-9"}}


# ============================================================================
# 8. PREDICTION RULE REGRESSION WITH REGISTRY
# ============================================================================

@pytest.mark.asyncio
async def test_08_phase4_prediction_rule_regression():
    """Verify Phase 4 prediction rule still functions correctly with TOOL_REGISTRY."""
    clock, saga, guard = _make_env()

    traffic_calls: list[dict[str, Any]] = []

    async def mock_telemetry(args: dict[str, Any]) -> dict[str, Any]:
        return {"speed_kph": 92, "truck_id": args["truck_id"]}

    async def mock_traffic(args: dict[str, Any]) -> dict[str, Any]:
        traffic_calls.append(dict(args))
        return {"congestion": "low", "route": args["route"]}

    def prediction_hook(action, result):
        if action.tool_name == "query_telemetry":
            destination = guard.resolve_current_value("destination")
            if destination is not None:
                saga.fire_speculative_read(
                    tool_name="query_traffic",
                    args={"route": destination},
                    executor=mock_traffic,
                    entity_hash=f"route:{destination}",
                )

    saga.set_prediction_hook(prediction_hook)

    # Sub-case A: destination is None -> MUST NOT fire
    assert guard.resolve_current_value("destination") is None
    a1 = saga.fire_speculative_read(
        tool_name="query_telemetry",
        args={"truck_id": "truck-17"},
        executor=mock_telemetry,
        entity_hash="truck_id:truck-17",
    )
    await a1._task
    assert len(traffic_calls) == 0

    # Sub-case B: destination grounded -> fires with grounded destination
    idx = guard.ingest_token("Chennai", 0.95, 0.0, 0.5)
    guard.stage_candidate("destination", "Chennai", (idx, idx + 1))
    assert guard.resolve_current_value("destination") == "Chennai"

    a2 = saga.fire_speculative_read(
        tool_name="query_telemetry",
        args={"truck_id": "truck-99"},
        executor=mock_telemetry,
        entity_hash="truck_id:truck-99",
    )
    await a2._task
    assert len(traffic_calls) == 1
    assert traffic_calls[0] == {"route": "Chennai"}
