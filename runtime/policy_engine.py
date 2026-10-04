"""
runtime/policy_engine.py

Deterministic authorization and confirmation policy engine for tool execution.

Core safety principle:
  "The LLM must never be the final authority on whether an unsafe mutation
   is permitted." Every permission grant and confirmation token must come
   from an explicit AuthorizationContext object supplied by the caller
   independently of the conversation — NEVER from LLM-generated text,
   tool arguments, or anything resolved via GroundingGuard.resolve_current_value.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, Optional

from runtime.tool_contract import ToolManifest


class PolicyError(Exception):
    """Base exception for policy engine violations."""


class PermissionDeniedError(PolicyError):
    """
    Raised when an operation requires permissions that are not present
    in the caller's explicit AuthorizationContext.
    """


class ConfirmationRequiredError(PolicyError):
    """
    Raised when an operation requires explicit operator/caller confirmation
    and neither the call-time confirmed flag nor context.confirmed is True.
    """


@dataclass
class AuthorizationContext:
    """
    Caller-supplied authorization credentials.

    Attributes:
        granted_permissions: Set of permissions granted to this execution context.
        confirmed: Whether caller/operator confirmation has been explicitly provided.
    """
    granted_permissions: set[str] = field(default_factory=set)
    confirmed: bool = False

    def __init__(
        self,
        granted_permissions: Optional[Iterable[str]] = None,
        confirmed: bool = False,
    ) -> None:
        self.granted_permissions = set(granted_permissions) if granted_permissions is not None else set()
        self.confirmed = bool(confirmed)


class PolicyEngine:
    """
    Evaluates execution requests against tool manifests and authorization contexts.
    """

    def __init__(self, *, default_deny: bool = False) -> None:
        """
        Args:
            default_deny: If False (default), absent context (context=None)
                is treated as 'no enforcement' of permissions (opt-in-via-absence,
                consistent with Phase 5 tool contract checks). If True, absent
                context denies any tool that requires permissions.
        """
        self.default_deny = default_deny

    def check(
        self,
        manifest: Optional[ToolManifest],
        context: Optional[AuthorizationContext] = None,
        confirmed: bool = False,
    ) -> None:
        """
        Evaluate policy for a tool manifest against caller credentials.

        Raises:
            PermissionDeniedError: If any required permission is missing.
            ConfirmationRequiredError: If the tool requires confirmation and
                neither the call-time argument nor context.confirmed is True.
        """
        if manifest is None:
            return

        # 1. Permission check
        if manifest.required_permissions:
            if context is None:
                if self.default_deny:
                    raise PermissionDeniedError(
                        f"Permission denied for tool '{manifest.tool_name}': "
                        f"No AuthorizationContext supplied and default_deny is True."
                    )
                # Opt-in-via-absence pattern: unauthenticated callers without
                # context bypass permission checks for backward compatibility.
            else:
                missing = [p for p in manifest.required_permissions if p not in context.granted_permissions]
                if missing:
                    raise PermissionDeniedError(
                        f"Permission denied for tool '{manifest.tool_name}'. "
                        f"Missing required permissions: {sorted(missing)}"
                    )

        # 2. Confirmation check
        is_confirmed = confirmed or (context is not None and context.confirmed)
        if manifest.requires_confirmation and not is_confirmed:
            raise ConfirmationRequiredError(
                f"Tool '{manifest.tool_name}' requires explicit confirmation before execution."
            )
