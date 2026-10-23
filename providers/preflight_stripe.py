"""Inspect a Stripe sandbox with GET only; no journal, creation or cancellation."""
import argparse
import asyncio
import json
import re

from providers.stripe_sandbox import StripeSandboxConfig, StripeSandboxCancellation, ProviderUnavailable
from runtime.credential_input import CredentialInputError


class _SafeParser(argparse.ArgumentParser):
    def error(self, message):
        # Argparse's default error echoes unknown argv, potentially including a
        # key mistakenly pasted as an argument. Only file paths belong in argv.
        self.exit(2, "Invalid preflight arguments. Use --help; supply secrets only through --stripe-key-file.\n")


async def run(args, *, provider_factory=None):
    """The factory is a SIMULATED test seam, never a CLI transport override."""
    config = StripeSandboxConfig.from_file(args.account_id, args.stripe_key_file)
    make_provider = StripeSandboxCancellation if provider_factory is None else provider_factory
    async with make_provider(config) as provider:
        summary = await provider.preflight(args.payment_intent_id)
    return {"event": "STRIPE_SANDBOX_PREFLIGHT_COMPLETED",
            "source": "REAL_PROVIDER_TEST_MODE" if provider_factory is None else "SIMULATED",
            **summary, "mutation_attempted": False, "write_permission_verified": False,
            "journal_updated": False}


def _failure_code(error):
    if isinstance(error, CredentialInputError):
        return "CREDENTIAL_INPUT_INVALID"
    if isinstance(error, ValueError):
        return "SCOPE_INVALID"
    if isinstance(error, ProviderUnavailable):
        value = str(error)
        # Provider bodies, arbitrary exception text and paths never reach output.
        if re.fullmatch(r"STRIPE_HTTP_[1-5][0-9]{2}", value):
            return value
        if value in {"STRIPE_RESPONSE_HEADERS_INVALID", "STRIPE_REQUEST_REFERENCE_INVALID",
                     "STRIPE_RESPONSE_TOO_LARGE", "STRIPE_RESPONSE_UNAVAILABLE_OR_INVALID",
                     "STRIPE_ACCOUNT_MISMATCH", "STRIPE_PAYMENT_INTENT_MISMATCH",
                     "STRIPE_TERMINAL_EVIDENCE_INVALID", "STRIPE_DEADLINE_EXCEEDED"}:
            return value
    return "PREFLIGHT_UNAVAILABLE"


def main():
    parser = _SafeParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--stripe-key-file", required=True, help="Protected file containing the exact rk_test_ key, with no newline")
    parser.add_argument("--account-id", required=True, help="Expected sandbox acct_ identity")
    parser.add_argument("--payment-intent-id", help="Optional existing pi_ identity; never created by this command")
    args = parser.parse_args()
    try:
        outcome = asyncio.run(run(args))
    except Exception as error:
        print(json.dumps({"event": "STRIPE_SANDBOX_PREFLIGHT_FAILED", "code": _failure_code(error),
                          "mutation_attempted": False, "journal_updated": False}), flush=True)
        raise SystemExit(1) from None
    print(json.dumps(outcome), flush=True)
    raise SystemExit(0)


if __name__ == "__main__":
    main()
