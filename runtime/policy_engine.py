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
import hashlib
import json
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
    """Explicit caller confirmation is missing or does not match the operation."""


def payload_digest(tool_name: str, args: dict) -> str:
    """Canonical JSON only: reject NaN/non-JSON objects rather than signing ambiguity."""
    if type(tool_name) is not str or not tool_name or type(args) is not dict:
        raise ValueError("Mutation payload requires a tool name and JSON object")

    def validate(value):
        if type(value) is dict:
            for key, child in value.items():
                if type(key) is not str:
                    raise ValueError("JSON object keys must be strings")
                validate(child)
        elif type(value) is list:
            for child in value:
                validate(child)
        elif value is not None and type(value) not in (str, int, float, bool):
            raise ValueError("Mutation arguments must contain only JSON values")

    validate(args)
    payload = json.dumps({"tool": tool_name, "args": args}, sort_keys=True,
                         separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class MutationApproval:
    """Trusted caller's in-process approval, bound to one staged action and payload.

    This is not authentication or a signed network credential. Only a trusted
    operator boundary may construct it; never derive it from model/ASR text.
    """
    action_id: str
    capture_epoch: int
    digest: str

    @classmethod
    def for_action(cls, action) -> MutationApproval:
        return cls(action.action_id, action.capture_epoch,
                   payload_digest(action.tool_name, action.args))

    def matches(self, action) -> bool:
        return (self.action_id == action.action_id and
                self.capture_epoch == action.capture_epoch and
                self.digest == payload_digest(action.tool_name, action.args))


@dataclass
class AuthorizationContext:
    """
    Caller-supplied authorization credentials.

    Attributes:
        granted_permissions: Set of permissions granted to this execution context.
        confirmed: Legacy exact-bool confirmation signal. Insufficient for dispatch
            of a manifest requiring confirmation; an action-bound approval is required.
        approval: Trusted approval bound to one staged action, epoch and payload.
    """
    granted_permissions: set[str] = field(default_factory=set)
    confirmed: bool = False
    approval: Optional[MutationApproval] = None

    def __init__(
        self,
        granted_permissions: Optional[Iterable[str]] = None,
        confirmed: bool = False,
        approval: Optional[MutationApproval] = None,
    ) -> None:
        if type(confirmed) is not bool:
            raise TypeError("confirmed must be an exact bool")
        if isinstance(granted_permissions, str):
            raise TypeError("granted_permissions must be a collection, not a string")
        self.granted_permissions = set(granted_permissions) if granted_permissions is not None else set()
        if any(not isinstance(p, str) or not p.strip() for p in self.granted_permissions):
            raise TypeError("permissions must be non-empty strings")
        if approval is not None and not isinstance(approval, MutationApproval):
            raise TypeError("approval must be a MutationApproval")
        self.confirmed = confirmed
        self.approval = approval


class PolicyEngine:
    """
    Evaluates execution requests against tool manifests and authorization contexts.
    """

    def __init__(self, *, default_deny: bool = True) -> None:
        """
        Args:
            default_deny: Legacy argument retained for source compatibility.
                False cannot disable mandatory authorization.
        """
        self.default_deny = True

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
            raise PermissionDeniedError("No tool manifest supplied")
        if not isinstance(context, AuthorizationContext):
            raise PermissionDeniedError("Explicit AuthorizationContext required")
        if type(confirmed) is not bool or type(context.confirmed) is not bool:
            raise PermissionDeniedError("Confirmation must be an exact bool")
        if context.approval is not None and not isinstance(context.approval, MutationApproval):
            raise PermissionDeniedError("Invalid approval object")
        if (type(context.granted_permissions) not in (set, frozenset) or
                any(type(p) is not str or not p.strip() for p in context.granted_permissions)):
            raise PermissionDeniedError("Invalid permission collection")

        # 1. Permission check
        if manifest.required_permissions:
            missing = [p for p in manifest.required_permissions if p not in context.granted_permissions]
            if missing:
                raise PermissionDeniedError(
                    f"Permission denied for tool '{manifest.tool_name}'. "
                    f"Missing required permissions: {sorted(missing)}"
                )

        # 2. Confirmation check
        is_confirmed = confirmed or context.confirmed or context.approval is not None
        if manifest.requires_confirmation and not is_confirmed:
            raise ConfirmationRequiredError(
                f"Tool '{manifest.tool_name}' requires explicit confirmation before execution."
            )
