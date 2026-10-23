# Dependency advisory gate and telemetry remediation

Status: **LOCAL BRANCH VERIFIED** implementation with real, read-only public
advisory queries labeled `ONLINE_PYPI`. Deterministic scanner fixtures remain
**SIMULATED**. Online advisory queries are separate from **REAL PROVIDER** Stripe
acceptance and real audio, neither of which is run by this phase. PUBLIC MAIN
VERIFIED remains Phase 0; the safety stack is unmerged. Exact-head clean-checkout,
runtime/browser and Windows/Linux CI results are retained in the phase PR.

## Verified finding and compatible remediation

The first complete online scan reported `CVE-2026-114603` /
`GHSA-xcq7-p3pf-p88h` in locked `opentelemetry-api==1.44.0`. It appeared in both
runtime and browser graphs; those are two scope occurrences of one advisory.
The [reviewed advisory](https://github.com/advisories/GHSA-xcq7-p3pf-p88h) identifies
unbounded tracestate regex parsing and lists `1.45.1` as patched.

The preceding `livekit-agents==1.8.3` declares OpenTelemetry `<1.45`, so changing
the API pin alone produces an actual resolver conflict. The new locks use the
matching agent/plugin `1.8.8` pair and `opentelemetry-api==1.45.1`, within the
SDK's declared range. The resolver updates the matching telemetry components
and SDK-required dependencies, including the OpenAI client. No dependency-bound
override or finding suppression is used. See the official
[LiveKit release](https://github.com/livekit/agents/releases/tag/livekit-agents%401.8.8)
and [OpenTelemetry release](https://github.com/open-telemetry/opentelemetry-python/releases/tag/v1.45.1).

The voice observer retains its version guard, now qualified against `1.8.8`.
Older or arbitrary newer SDK versions cannot connect. Real SDK-object tests
retain proposal-only behavior, absent LLM/TTS/tools/recording, and no durable
task authority. Long-space/tab tracestate tests trap the vulnerable regex-split
path without CPU timing thresholds; valid parsing is also exercised against
the installed dependency. This does not certify every upstream parser/logging
behavior or HTTP server header limit. Live audio/provider behavior still needs
its separate acceptance gate after this necessary SDK upgrade.

## Scope, coverage and evidence

The scanner is [PyPA pip-audit](https://github.com/pypa/pip-audit), pinned to
`2.10.1` with its entire tool graph in `requirements-security.txt`. Install it
in a dedicated environment. The exact security-profile metadata audit and pip
compatibility check must pass before a network query is admitted. Tool-version
or lock-bundle mismatch cannot produce a successful advisory verdict.

The wrapper evaluates all runtime, browser and security locks for Windows AMD64
and Linux x86_64, at the running CPython 3.13 patch version. Target environment
markers are explicit. Unknown kernel/version/extra marker scopes fail for review;
they cannot silently remove coverage. Identical graphs share a query, with all
six profile/target scopes retained. Graphs with different package versions are
queried separately rather than forcing conflicting pins into one resolver input.

Each tool invocation receives a generated list of exact pins from the verified
manifest, `--disable-pip --no-deps --strict`, the PyPI advisory service and JSON
output. It runs under isolated Python in a temporary working directory, with
only OS/locale environment settings retained, a private home/cache and empty
netrc. Provider key, pip/uv option and proxy environment variables are excluded.
It receives no provider credential, journal,
custom service URL, ignore list, fix flag or dry-run flag. It never installs or
automatically upgrades dependencies. Scanner input is the complete locked graph;
dependency collection is not inferred from whichever app packages happen to be
installed in the tool environment.

Every expected name/version must occur exactly once. Missing, extra, duplicated,
skipped or substituted dependencies, malformed/bounded JSON, inconsistent exit
codes, nonempty fix output and unavailable/timed-out queries fail closed. One
failed graph prevents a complete verdict. A source/manifest change during network
queries prevents success for the earlier graph. The wrapper cannot guarantee
protection against a hostile build host or an undetectable source ABA change.

The HTTP cache is new for each invocation of the wrapper, so a previous run's
clean status cannot substitute for new queries. Within-run HTTP reuse is allowed.
Reports record local start/end timestamps, manifest/inventory identities, tool
version, exact scoped dependency coverage, advisory IDs/aliases/fix versions and
the raw-report hash. Descriptions, arbitrary URLs, raw stderr and exception text
are excluded. The raw hash is an evidence identifier, not a signed attestation.
The report retains sanitized dependency rows, not the untrusted raw body.

| Verdict / exit | Meaning |
| --- | --- |
| `NO_KNOWN_ADVISORIES` / 0 | Complete coverage and no known findings reported by the selected service during this run. |
| `ADVISORIES_FOUND` / 1 | Complete coverage with at least one finding. Review and remediate; no automatic fix or ignore. |
| Incomplete coverage / 2 | `UNKNOWN` or partially observed findings with unverified tool/environment/source/service evidence. No clearance. Also used if the report cannot be preserved. |

A partially observed finding can remain in group evidence while overall coverage
is incomplete; the exit remains 2. `advisory_count` and `finding_packages` count
scope-group occurrences. `unique_advisories` and `unique_vulnerable_packages`
deduplicate repeated findings across graphs. A PyPI outage or malformed scanner
response is not equivalent to finding zero vulnerabilities.

## Windows commands

From a clean checkout of this reviewed safety branch, with CPython 3.13:

```powershell
py -3.13 -m venv .venv-security
New-Item -ItemType Directory -Path results -Force | Out-Null
.\.venv-security\Scripts\python.exe -m pip --isolated --disable-pip-version-check install `
  --require-hashes --only-binary=:all: --report results/install-security.json `
  -r requirements-security.txt
if ($LASTEXITCODE -ne 0) { throw 'Security tool installation failed' }
.\.venv-security\Scripts\python.exe -m scripts.dependency_audit --profile security `
  --output results/environment-security.json
if ($LASTEXITCODE -ne 0) { throw 'Security tool environment is unverified' }
.\.venv-security\Scripts\python.exe -m scripts.advisory_audit `
  --output results/dependency-advisories.json
$LASTEXITCODE
Get-Content results/dependency-advisories.json
```

This needs public package/advisory network access and no service account. Regular
runtime tests use deterministic fixtures and do not contact the advisory service:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_advisory_audit.py tests/test_telemetry_security.py tests/test_livekit_voice_observer.py -q -p no:cacheprovider
```

CI adds an isolated advisory job alongside the existing Windows/Linux runtime
and browser matrix. Its report covers both target graphs; it is not a claim
that every advisory service implementation was run on both OSes. The
`dependency-advisories` artifact retains installation, environment and advisory
reports for seven days. A failed query remains a failed CI check. Archive the
evidence before retention expires; obtain a new online result for a release.

## Limits, response and rollback

The feed is mutable. Local timestamps do not establish a globally consistent
database snapshot, upstream freshness or absence of unpublished vulnerabilities.
No package-security, malicious-package, publisher-authenticity, host-integrity,
native/browser/C++ dependency, interpreter or universal novelty claim follows.
Application reachability and exploitability require separate investigation.
The release gate also needs the normal fresh installs, exact-set audits, full
runtime/browser regressions and both explicitly SIMULATED scorecards.

For a new finding, preserve its IDs, affected scope and source evidence. Verify
the authoritative advisory and compatible upstream metadata, review the entire
dependency/hash diff, then regenerate, reinstall and retest. For service failure,
investigate access and retry the read-only audit; do not reuse an old passing
report as current clearance. There is no CLI override to suppress findings or
reinterpret an incomplete scan as passed.

Stop/drain the affected hosts before changing their environment. Do not roll
back to the known-vulnerable telemetry lock merely to regain a green functional
test. On an SDK regression, keep voice connection disabled, retain the fixed
dependency bundle and journal evidence, and qualify a compatible implementation.
Recovery-only operation must still respect current ownership and authority.
No journal schema, operation key, approval, retry or compensation policy changes
in this phase. Preserve schema-6 actions/claims/receipts; never restore an old
action database or replay an unknown mutation as security rollback.
