"""
runtime/tool_contract.py

Minimal ToolManifest dataclass describing the behavioural contract of a
single tool in the ChronoCortex-Saga fleet domain.

Design intent:
  - No schema-validation library; no over-engineering.
  - ToolManifest is pure metadata consumed by the saga integration layer to
    drive decisions (e.g. whether to fire speculatively, whether to
    require authoritative commit, what timeout to apply).
  - All dispatch still goes through SpeculativeSagaManager; this manifest
    does NOT introduce a second transaction system.

Fields:
  tool_name    : str
      Canonical name used as the key in READ_TOOLS / WRITE_TOOLS registries.

  kind         : Literal["read", "write"]
      "read"  — idempotent lookup; safe to fire speculatively before turn
                boundary is confirmed.
      "write" — mutating call; must only be dispatched after turn boundary
                confirmation via commit_write().

  idempotent   : bool
      True iff re-running the same call with the same args produces the
      same observable outcome. All reads must be idempotent. Writes may
      or may not be idempotent depending on the backend (e.g., reserve_dock
      is NOT idempotent — a second call for an already-reserved dock raises).

  cancellable  : bool
      True iff an in-flight call can be safely cancelled (task.cancel())
      without risk of partial remote side-effects. Reads are cancellable;
      writes are generally NOT (see speculative_saga.py module docstring).

  requires_authoritative_commit : bool
      True iff the call must go through commit_write() (i.e. is a
      STAGED_WRITE). False for speculative reads.

  compensate   : Optional[Callable]
      The saga compensation (rollback) coroutine function, or None if the
      tool has no compensation path (reads, or writes with no rollback).

  reconciliation : Optional[Callable]
      A coroutine function (status_check signature) that can confirm or
      deny whether a remote side-effect persisted, used by reconcile_write()
      to resolve IN_FLIGHT_UNKNOWN states. None if the tool has no
      reconciliation path.

  timeout      : float
      Maximum number of seconds to wait for a response before classifying
      the outcome as IN_FLIGHT_UNKNOWN (for writes) or ABORTED (for reads).

  telemetry_category : str
      Free-form label for observability routing (e.g. dashboard bucketing,
      alerting thresholds). Not validated here.

  depends_on   : list[str]
      Tool names this tool's execution pattern depends on (e.g. query_traffic
      depends_on=["query_telemetry"]).

  required_permissions : list[str]
      Inert permission metadata for future authorization layers (New Phase 12).
      Does not block or alter tool dispatch in this phase.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Literal, Optional


class ToolContractError(ValueError):
    """Base exception for tool contract violations."""


class InvalidToolKindError(ToolContractError):
    """Raised when a tool's kind (read vs write) does not match the operation being performed."""


class CyclicDependencyError(ToolContractError):
    """Raised when registering a tool would create a cyclic dependency in the tool graph."""


@dataclass(frozen=True)
class ToolManifest:
    """
    Describes the behavioural contract of one tool in the fleet domain.
    Consumed by agent/cortex code to decide dispatch strategy; does not
    replace or duplicate the SpeculativeSagaManager's state machinery.
    """
    tool_name: str
    kind: Literal["read", "write"]
    idempotent: bool = True
    cancellable: bool = True
    requires_authoritative_commit: bool = False
    compensate: Optional[Callable[[Any], Awaitable[None]]] = None
    reconciliation: Optional[Callable[[Any], Awaitable[Optional[bool]]]] = None
    timeout: float = 4.0
    telemetry_category: str = "default"
    depends_on: list[str] = field(default_factory=list)
    required_permissions: list[str] = field(default_factory=list)


class ToolRegistry:
    """
    Registry for ToolManifest objects enforcing graph acyclicity at registration time
    and offering deterministic topological ordering.
    """

    def __init__(self) -> None:
        self._manifests: dict[str, ToolManifest] = {}

    def register(self, manifest: ToolManifest) -> None:
        """
        Register a ToolManifest.
        Detects cycles in the depends_on graph across all currently registered manifests
        at registration time and raises CyclicDependencyError if adding this manifest
        would create a cycle.
        """
        candidate = dict(self._manifests)
        candidate[manifest.tool_name] = manifest

        # 0 = unvisited, 1 = visiting (in active recursion stack), 2 = visited
        state: dict[str, int] = {}

        def dfs(node: str) -> None:
            state[node] = 1
            mf = candidate[node]
            for dep in mf.depends_on:
                if dep in candidate:
                    dep_state = state.get(dep, 0)
                    if dep_state == 1:
                        raise CyclicDependencyError(
                            f"Cyclic dependency detected: '{node}' depends on '{dep}', "
                            f"which is currently active in the dependency chain."
                        )
                    if dep_state == 0:
                        dfs(dep)
            state[node] = 2

        for tool_name in candidate:
            if state.get(tool_name, 0) == 0:
                dfs(tool_name)

        # Committed only if cycle check passes
        self._manifests[manifest.tool_name] = manifest

    def get(self, tool_name: str, default: Optional[ToolManifest] = None) -> Optional[ToolManifest]:
        return self._manifests.get(tool_name, default)

    def __getitem__(self, tool_name: str) -> ToolManifest:
        return self._manifests[tool_name]

    def __contains__(self, tool_name: str) -> bool:
        return tool_name in self._manifests

    def __len__(self) -> int:
        return len(self._manifests)

    def __iter__(self):
        return iter(self._manifests)

    @property
    def manifests(self) -> dict[str, ToolManifest]:
        return dict(self._manifests)

    def topological_order(self) -> list[str]:
        """
        Returns registered tool names in dependency-respecting order
        (prerequisites before dependents). Deterministic tie-breaking
        using alphabetical order for equal-priority nodes.
        """
        if not self._manifests:
            return []

        in_degree: dict[str, int] = {name: 0 for name in self._manifests}
        dependents: dict[str, list[str]] = {name: [] for name in self._manifests}

        for name, mf in self._manifests.items():
            for dep in mf.depends_on:
                if dep in self._manifests:
                    in_degree[name] += 1
                    dependents[dep].append(name)

        ready = sorted([name for name, deg in in_degree.items() if deg == 0])
        order: list[str] = []

        while ready:
            curr = ready.pop(0)
            order.append(curr)
            for nxt in dependents[curr]:
                in_degree[nxt] -= 1
                if in_degree[nxt] == 0:
                    ready.append(nxt)
            ready.sort()

        if len(order) < len(self._manifests):
            raise CyclicDependencyError("Dependency graph contains an unresolved cycle.")

        return order

