"""
tools/fleet_tools.py

Domain extension: Mission-Critical In-Cabin Fleet Logistics & Telematics
Copilot. Each mutating tool exposes a forward call and a compensate()
handler for saga rollback. Reads are cheap/idempotent and safe to fire
speculatively. All external calls are mocked with realistic latency —
swap the bodies for real fleet-management API calls without touching
the saga/grounding layers.
"""

from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass
from typing import Any

from runtime.tool_contract import ToolManifest, ToolRegistry


# ---- mock backing store (stands in for a real fleet telematics API) -------

_ROUTES: dict[str, dict[str, Any]] = {}
_DOCK_RESERVATIONS: dict[str, dict[str, Any]] = {}
_TELEMETRY = {
    "truck-17": {"speed_kph": 88, "engine_temp_c": 91, "fuel_pct": 42, "tire_pressure_ok": True},
}


async def _simulated_network(min_ms: int = 60, max_ms: int = 220) -> None:
    await asyncio.sleep(random.uniform(min_ms, max_ms) / 1000)


# ---- idempotent reads (safe to fire speculatively) -------------------------

async def query_telemetry(args: dict[str, Any]) -> dict[str, Any]:
    """Read-only: current vehicle telemetry. Safe to speculate on entity
    recognition (e.g. as soon as 'truck 17' is heard)."""
    await _simulated_network(20, 90)  # telemetry fabric target: sub-20ms in prod
    truck_id = args["truck_id"]
    return _TELEMETRY.get(truck_id, {"error": f"no telemetry for {truck_id}"})


async def query_traffic(args: dict[str, Any]) -> dict[str, Any]:
    await _simulated_network()
    return {
        "route": args.get("route", "unknown"),
        "congestion_level": random.choice(["low", "moderate", "heavy"]),
        "eta_delta_minutes": random.randint(-5, 25),
    }


async def query_dock_availability(args: dict[str, Any]) -> dict[str, Any]:
    await _simulated_network()
    dock_id = args["dock_id"]
    reserved = any(r["dock_id"] == dock_id for r in _DOCK_RESERVATIONS.values())
    return {"dock_id": dock_id, "available": not reserved}


# ---- mutating writes (staged; dispatched only on confirmed turn boundary) --

@dataclass
class RerouteResult:
    route_id: str
    truck_id: str
    new_route: str


async def reroute_truck(args: dict[str, Any]) -> RerouteResult:
    """Forward: commit a new route for a truck."""
    await _simulated_network()
    route_id = f"route-{args['truck_id']}-{random.randint(1000, 9999)}"
    _ROUTES[route_id] = {"truck_id": args["truck_id"], "route": args["destination"]}
    return RerouteResult(route_id=route_id, truck_id=args["truck_id"], new_route=args["destination"])


async def compensate_reroute(result: RerouteResult) -> None:
    """Rollback: revert the truck to its previous route (or a safe default)."""
    await _simulated_network()
    _ROUTES.pop(result.route_id, None)


@dataclass
class DockReservationResult:
    reservation_id: str
    dock_id: str
    truck_id: str


async def reserve_dock(args: dict[str, Any]) -> DockReservationResult:
    """Forward: reserve a loading dock slot. Rejects if already taken —
    caller (saga manager) treats a raised exception as abort-worthy."""
    await _simulated_network()
    dock_id = args["dock_id"]
    if any(r["dock_id"] == dock_id for r in _DOCK_RESERVATIONS.values()):
        raise RuntimeError(f"dock {dock_id} already reserved")
    reservation_id = f"dock-{dock_id}-{random.randint(1000, 9999)}"
    _DOCK_RESERVATIONS[reservation_id] = {"dock_id": dock_id, "truck_id": args["truck_id"]}
    return DockReservationResult(reservation_id=reservation_id, dock_id=dock_id, truck_id=args["truck_id"])


async def compensate_reserve_dock(result: DockReservationResult) -> None:
    """Rollback: release the dock reservation."""
    await _simulated_network()
    _DOCK_RESERVATIONS.pop(result.reservation_id, None)


# ---- tool registry consumed by agent.py ------------------------------------

READ_TOOLS = {
    "query_telemetry": query_telemetry,
    "query_traffic": query_traffic,
    "query_dock_availability": query_dock_availability,
}

WRITE_TOOLS = {
    "reroute_truck": (reroute_truck, compensate_reroute),
    "reserve_dock": (reserve_dock, compensate_reserve_dock),
}

# ---- tool manifests (additive metadata; does not replace READ_TOOLS/WRITE_TOOLS) ----
#
# Each ToolManifest describes the behavioural contract of one tool so that
# the saga integration layer can decide dispatch strategy without hard-coding
# per-tool logic. All actual dispatch still goes through SpeculativeSagaManager.
#
# Timeout rationale:
#   - Reads: 2.0 s — network P99 for telematics fabric is well under 500 ms;
#     2 s gives headroom for degraded conditions without blocking speculative
#     prefetch for too long.
#   - reroute_truck: 4.0 s — fleet routing API has higher tail latency.
#   - reserve_dock: 3.0 s — dock management API; shorter than routing because
#     the dock call either succeeds quickly or raises immediately on conflict.

TOOL_MANIFEST: dict[str, ToolManifest] = {
    "query_telemetry": ToolManifest(
        tool_name="query_telemetry",
        kind="read",
        idempotent=True,           # same truck_id always returns current snapshot
        cancellable=True,          # read; no remote side-effect to worry about
        requires_authoritative_commit=False,
        compensate=None,           # reads have no compensation path
        reconciliation=None,       # reads need no reconciliation (idempotent re-read)
        timeout=2.0,
        telemetry_category="fleet.telematics",
    ),
    "query_traffic": ToolManifest(
        tool_name="query_traffic",
        kind="read",
        idempotent=True,           # snapshot read; repeated calls yield fresh snapshots
        cancellable=True,
        requires_authoritative_commit=False,
        compensate=None,
        reconciliation=None,
        timeout=2.0,
        telemetry_category="fleet.traffic",
        depends_on=["query_telemetry"],
    ),
    "query_dock_availability": ToolManifest(
        tool_name="query_dock_availability",
        kind="read",
        idempotent=True,           # availability is a snapshot; re-reading is safe
        cancellable=True,
        requires_authoritative_commit=False,
        compensate=None,
        reconciliation=None,
        timeout=2.0,
        telemetry_category="fleet.dock",
    ),
    "reroute_truck": ToolManifest(
        tool_name="reroute_truck",
        kind="write",
        idempotent=False,          # each call creates a new route record
        cancellable=False,         # write; cancellation after dispatch ≠ non-execution
        requires_authoritative_commit=True,
        compensate=compensate_reroute,
        reconciliation=None,       # no reconciliation endpoint modelled in mock
        timeout=4.0,
        telemetry_category="fleet.routing",
    ),
    "reserve_dock": ToolManifest(
        tool_name="reserve_dock",
        kind="write",
        idempotent=False,          # reserving an already-reserved dock raises
        cancellable=False,
        requires_authoritative_commit=True,
        compensate=compensate_reserve_dock,
        reconciliation=None,
        timeout=3.0,
        telemetry_category="fleet.dock",
    ),
}

# ---- tool registry (with acyclicity enforcement and topological ordering) --

TOOL_REGISTRY: ToolRegistry = ToolRegistry()
for _mf in TOOL_MANIFEST.values():
    TOOL_REGISTRY.register(_mf)

FLEET_TOOL_REGISTRY = TOOL_REGISTRY


# ---- task step definitions for the Uninterruptible Task Engine (Phase 6) ----

from runtime.task_engine import TaskStep


def _build_telemetry_args(task: Any, step_idx: int) -> dict[str, Any]:
    """Build args for query_telemetry: requires truck_id."""
    from runtime.task_engine import Task
    # truck_id is resolved by the task engine's resolve_field
    return {"truck_id": task._resolved_fields.get("truck_id", "unknown")}


def _build_traffic_args(task: Any, step_idx: int) -> dict[str, Any]:
    """Build args for query_traffic: requires destination."""
    return {"route": task._resolved_fields.get("destination", "unknown")}


def _build_dock_args(task: Any, step_idx: int) -> dict[str, Any]:
    """Build args for query_dock_availability: requires dock_id."""
    return {"dock_id": task._resolved_fields.get("dock_id", "unknown")}


def _build_reroute_args(task: Any, step_idx: int) -> dict[str, Any]:
    """Build args for reroute_truck: requires truck_id + destination."""
    return {
        "truck_id": task._resolved_fields.get("truck_id", "unknown"),
        "destination": task._resolved_fields.get("destination", "unknown"),
    }


def _build_reserve_dock_args(task: Any, step_idx: int) -> dict[str, Any]:
    """Build args for reserve_dock: requires truck_id + dock_id."""
    return {
        "truck_id": task._resolved_fields.get("truck_id", "unknown"),
        "dock_id": task._resolved_fields.get("dock_id", "unknown"),
    }


FLEET_REROUTE_LOGISTICS_TASK_STEPS: list[TaskStep] = [
    TaskStep(
        tool_name="query_telemetry",
        required_fields=["truck_id"],
        build_args=_build_telemetry_args,
        kind="read",
    ),
    TaskStep(
        tool_name="query_traffic",
        required_fields=["destination"],
        build_args=_build_traffic_args,
        kind="read",
    ),
    TaskStep(
        tool_name="query_dock_availability",
        required_fields=["dock_id"],
        build_args=_build_dock_args,
        kind="read",
    ),
    TaskStep(
        tool_name="reroute_truck",
        required_fields=["truck_id", "destination"],
        build_args=_build_reroute_args,
        kind="write",
        compensate=compensate_reroute,
    ),
    TaskStep(
        tool_name="reserve_dock",
        required_fields=["truck_id", "dock_id"],
        build_args=_build_reserve_dock_args,
        kind="write",
        compensate=compensate_reserve_dock,
    ),
]
