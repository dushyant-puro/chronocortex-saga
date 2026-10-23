"""Positive, operation-bound evidence from a trusted provider status adapter.

This is a claim made by adapter code, not a cryptographic provider attestation.
Only redacted receipts belong here; raw HTTP bodies and credentials do not.
"""
from dataclasses import dataclass
import json

from runtime.policy_engine import payload_digest, PermissionDeniedError


@dataclass(frozen=True)
class ReconciledEffect:
    action_id: str
    digest: str
    receipt: dict

    def validated_receipt(self, action):
        if (self.action_id != action.action_id or self.digest != action._staged_digest or
                self.digest != payload_digest(action.tool_name, action.args)):
            raise PermissionDeniedError("Reconciliation evidence does not match the staged operation")
        if type(self.receipt) is not dict:
            raise ValueError("Reconciliation receipt must be a JSON object")
        # Reuse exact JSON-type validation (reject custom objects, nonstring keys,
        # bool-like substitutes and nonfinite numbers), then detach ownership.
        payload_digest("reconciliation-receipt", self.receipt)
        encoded = json.dumps(self.receipt, sort_keys=True, separators=(",", ":"), allow_nan=False)
        if len(encoded.encode("utf-8")) > 16_384:
            raise ValueError("Reconciliation receipt exceeds 16 KiB")
        return json.loads(encoded)
