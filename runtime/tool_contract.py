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
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Literal, Optional


@dataclass(frozen=True)
class ToolManifest:
    """
    Describes the behavioural contract of one tool in the fleet domain.
    Consumed by agent/cortex code to decide dispatch strategy; does not
    replace or duplicate the SpeculativeSagaManager's state machinery.
    """
    tool_name: str
    kind: Literal["read", "write"]
    idempotent: bool
    cancellable: bool
    requires_authoritative_commit: bool
    compensate: Optional[Callable[[Any], Awaitable[None]]]
    reconciliation: Optional[Callable[[Any], Awaitable[Optional[bool]]]]
    timeout: float
    telemetry_category: str
