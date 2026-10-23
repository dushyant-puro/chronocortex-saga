"""Known-resource cancellation only. See docs/engineering/provider-reconciliation.md.

No creation, confirmation, capture, refund, compensation or POST retry. A status
query proves only the terminal cancellation goal, not who performed it. Transport
injection is for SIMULATED tests; the default transport uses the real HTTPS API.
"""
import asyncio
from dataclasses import dataclass, field
import json
import re

import httpx2 as httpx

from runtime.credential_input import read_stripe_key_file
from runtime.policy_engine import payload_digest, PermissionDeniedError
from runtime.reconciliation import ReconciledEffect
from runtime.tool_contract import ToolManifest


API_VERSION = "2026-09-30.endive"
CONTRACT = "stripe.pi.cancel.v1"
TOOL = "stripe_sandbox_cancel_payment_intent"
PERMISSION = "payments:cancel_sandbox"
ORIGIN = "https://api.stripe.com"
MAX_RESPONSE_BYTES = 65_536
_ACCOUNT = re.compile(r"acct_[A-Za-z0-9]{8,64}\Z")
_INTENT = re.compile(r"pi_[A-Za-z0-9]{8,128}\Z")
_REQUEST = re.compile(r"req_[A-Za-z0-9]{8,128}\Z")
_KEY = re.compile(r"rk_test_[A-Za-z0-9]{16,256}\Z")
_STATUSES = {"requires_payment_method", "requires_confirmation", "requires_action",
             "processing", "requires_capture", "canceled", "succeeded"}


class ProviderUnavailable(RuntimeError):
    """Sanitized failure; says nothing about nonexecution of a mutation."""


class ProviderProtocolError(ProviderUnavailable):
    pass


@dataclass(frozen=True)
class StripeSandboxConfig:
    account_id: str
    key: str = field(repr=False, compare=False)

    def __post_init__(self):
        if type(self.account_id) is not str or _ACCOUNT.fullmatch(self.account_id) is None:
            raise ValueError("Invalid expected Stripe account identity")
        if type(self.key) is not str or _KEY.fullmatch(self.key) is None:
            raise ValueError("A restricted Stripe test key is required")

    @classmethod
    def from_file(cls, account_id, path):
        return cls(account_id, read_stripe_key_file(path))


def _pairs(values):
    result = {}
    for key, value in values:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _constant(_):
    raise ValueError("nonfinite JSON value")


class StripeSandboxCancellation:
    def __init__(self, config, *, transport=None):
        if type(config) is not StripeSandboxConfig:
            raise ValueError("Explicit sandbox configuration required")
        self._config = config
        # Deliberately no configurable URL, proxy, redirect, SDK retry, env key
        # discovery or Connect account header. /v1/account attests the key's own
        # effective account on every operation before the resource request.
        self._client = httpx.AsyncClient(
            base_url=ORIGIN, trust_env=False, follow_redirects=False,
            timeout=httpx.Timeout(5.0),
            limits=httpx.Limits(max_connections=4, max_keepalive_connections=2),
            transport=transport if transport is not None else httpx.AsyncHTTPTransport(retries=0, trust_env=False),
            headers={"Authorization": "Bearer " + config.key, "Stripe-Version": API_VERSION,
                     "Accept": "application/json", "Accept-Encoding": "identity",
                     "User-Agent": "ChronoCortex-Saga/stripe-cancel-v1"})

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        await self.aclose()

    async def aclose(self):
        await self._client.aclose()

    def arguments(self, payment_intent_id):
        args = {"provider_contract": CONTRACT, "api_version": API_VERSION,
                "account_id": self._config.account_id, "payment_intent_id": payment_intent_id}
        self.validate_args(args)
        return args

    def validate_args(self, args):
        if (type(args) is not dict or set(args) != {"provider_contract", "api_version", "account_id", "payment_intent_id"} or
                args["provider_contract"] != CONTRACT or args["api_version"] != API_VERSION or
                args["account_id"] != self._config.account_id or
                type(args["payment_intent_id"]) is not str or _INTENT.fullmatch(args["payment_intent_id"]) is None):
            raise ValueError("Invalid or incompatible sandbox cancellation scope")

    def operation_key(self, args):
        self.validate_args(args)
        return "ccs-stripe-cancel-" + payload_digest(TOOL, args)

    def manifest(self):
        return ToolManifest(TOOL, "write", idempotent=False, cancellable=False,
                            requires_authoritative_commit=True, requires_confirmation=True,
                            required_permissions=[PERMISSION], validate_args=self.validate_args,
                            reconciliation=self.status_check, timeout=20.0,
                            telemetry_category="stripe_test_cancellation")

    def stage(self, saga, payment_intent_id):
        if saga.journal is None:
            raise PermissionDeniedError("Provider staging requires a durable journal")
        args = self.arguments(payment_intent_id)
        return saga.stage_write(TOOL, args, manifest=self.manifest(), idempotency_key=self.operation_key(args))

    async def _request(self, method, path, *, idempotency_key=None):
        headers = {}
        if idempotency_key is not None:
            headers = {"Idempotency-Key": idempotency_key, "Content-Type": "application/x-www-form-urlencoded"}
        try:
            async with self._client.stream(method, path, headers=headers) as response:
                if response.status_code != 200:
                    # Never retain body, URL, provider error message or secret.
                    raise ProviderUnavailable("STRIPE_HTTP_" + str(response.status_code))
                if (response.headers.get("content-type", "").split(";", 1)[0].strip().lower() != "application/json" or
                        response.headers.get("content-encoding", "identity").lower() != "identity" or
                        response.headers.get("stripe-version", API_VERSION) != API_VERSION):
                    raise ProviderProtocolError("STRIPE_RESPONSE_HEADERS_INVALID")
                request_id = response.headers.get("request-id", "")
                if _REQUEST.fullmatch(request_id) is None:
                    raise ProviderProtocolError("STRIPE_REQUEST_REFERENCE_INVALID")
                length = response.headers.get("content-length")
                if length is not None and (not length.isascii() or not length.isdecimal() or int(length) > MAX_RESPONSE_BYTES):
                    raise ProviderProtocolError("STRIPE_RESPONSE_TOO_LARGE")
                raw = bytearray()
                async for chunk in response.aiter_raw():
                    if len(raw) + len(chunk) > MAX_RESPONSE_BYTES:
                        raise ProviderProtocolError("STRIPE_RESPONSE_TOO_LARGE")
                    raw.extend(chunk)
                data = json.loads(raw.decode("utf-8"), object_pairs_hook=_pairs, parse_constant=_constant)
                if type(data) is not dict:
                    raise ValueError("object required")
                return data, request_id
        except ProviderUnavailable:
            raise
        except (httpx.HTTPError, OSError, ValueError, RecursionError):
            raise ProviderProtocolError("STRIPE_RESPONSE_UNAVAILABLE_OR_INVALID") from None

    async def _account(self):
        data, _ = await self._request("GET", "/v1/account")
        if data.get("object") != "account" or data.get("id") != self._config.account_id:
            raise ProviderProtocolError("STRIPE_ACCOUNT_MISMATCH")

    def _receipt(self, data, request_id, args, method):
        if (data.get("object") != "payment_intent" or data.get("id") != args["payment_intent_id"] or
                data.get("livemode") is not False or type(data.get("status")) is not str or data["status"] not in _STATUSES):
            raise ProviderProtocolError("STRIPE_PAYMENT_INTENT_MISMATCH")
        if data["status"] != "canceled":
            return None
        if (type(data.get("canceled_at")) is not int or data["canceled_at"] <= 0 or
                type(data.get("amount_capturable")) is not int or data["amount_capturable"] != 0):
            raise ProviderProtocolError("STRIPE_TERMINAL_EVIDENCE_INVALID")
        return {"provider": "stripe", "mode": "test", **args,
                "idempotency_key": self.operation_key(args), "status": "canceled",
                "canceled_at": data["canceled_at"], "request_id": request_id,
                "observed_via": method, "effect": "terminal_cancellation", "actor_attribution": "not_claimed"}

    async def execute(self, args):
        """Trusted saga executor. One POST attempt; every failure remains unknown."""
        self.validate_args(args)
        args = dict(args)
        try:
            async with asyncio.timeout(15):
                await self._account()
                data, reference = await self._request("POST", "/v1/payment_intents/" + args["payment_intent_id"] + "/cancel",
                                                      idempotency_key=self.operation_key(args))
                receipt = self._receipt(data, reference, args, "POST")
                if receipt is None:
                    raise ProviderProtocolError("STRIPE_CANCELLATION_NOT_TERMINAL")
                return receipt
        except TimeoutError:
            raise ProviderUnavailable("STRIPE_DEADLINE_EXCEEDED") from None

    async def status_check(self, action):
        """Read only, positive evidence or unknown. Never return False or POST."""
        self.validate_args(action.args)
        args = dict(action.args)
        digest = payload_digest(TOOL, args)
        if (action.tool_name != TOOL or action._staged_digest != digest or
                action.idempotency_key != self.operation_key(args)):
            raise PermissionDeniedError("Sandbox status scope does not match the staged action")
        try:
            async with asyncio.timeout(15):
                await self._account()
                data, reference = await self._request("GET", "/v1/payment_intents/" + args["payment_intent_id"])
                receipt = self._receipt(data, reference, args, "GET")
                return ReconciledEffect(action.action_id, digest, receipt) if receipt is not None else None
        except TimeoutError:
            raise ProviderUnavailable("STRIPE_DEADLINE_EXCEEDED") from None
