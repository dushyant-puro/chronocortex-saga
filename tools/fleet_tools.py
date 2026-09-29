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
