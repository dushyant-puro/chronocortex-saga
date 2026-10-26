# Comparative mutation-boundary evaluation

This is a bounded **SIMULATED** mechanism experiment. It compares five frozen
synthetic schedules across four policies using a separate file-backed effect
ledger. It does not measure ASR, audio interruption, full task completion,
provider idempotency, production latency or universal novelty. PUBLIC MAIN
remains the unmerged Phase 0 baseline; use the reviewed safety branch.

The existing voice replay and provider process-fault scorecards answer different
questions. This experiment adds explicit negative controls and one integration
ablation without copying the CCS write loop or bypassing its authorization.

## Policies and the fairness boundary

| Policy | Implementation and intended contrast |
| --- | --- |
| `restart_on_interruption` | An explicitly unsafe toy coordinator replays the same immutable intent after interruption or a lost receipt. Its local SQLite outcome is retained, but it deliberately ignores that evidence when restarting. |
| `cancel_means_abort` | An explicitly unsafe toy coordinator treats coroutine cancellation or receipt loss as proof of nonexecution. It aborts an undispatched retracted intent correctly. |
| `ccs_without_epoch_signal` | The real `SpeculativeSagaManager`, SQLite journal, worker lease and credential checks, with only the harness's correction-to-epoch notification omitted. This is a controlled integration ablation. |
| `ccs` | The same production manager and journal with correction notifications delivered. No runtime dispatch/state method is overridden. |

The toy policies are explanatory negative controls, not reproductions of Temporal,
LangGraph, LiveKit or another system. No result establishes superiority over a
vendor workflow engine. The ablation isolates the epoch signal for these traces;
it does not establish the contribution of every grounding, policy or task layer.
Both CCS variants use public provisioning APIs to create temporary, scoped
synthetic principals/credentials and acquire actual local worker leases. They do
not use `simulation=True` or import test helper implementations.

Every cell starts with one immutable simulated intent and a fresh coordinator
database. The endpoint deliberately applies every invocation: no deduplication,
compensation or provider-enforced idempotency can conceal a replay. It persists
an effect in its **separate SQLite file** before notifying the harness that the
executor has reached its await. Its ledger is reopened independently to count
effects. This is a local coroutine/SQLite experiment, not remote process or TCP
fault evidence; use [provider process faults](provider-process-faults.md) for that.

## Frozen schedules and expectations

No timing sleeps select the interleavings. An `asyncio.Event` barrier follows
the committed effect, then the harness injects the named correction/cancellation
before releasing the response. Timeout limits are execution guards, not latency
measurements. Each cell has an independent fresh database and endpoint.

| Schedule | Expected full CCS outcome | External effects |
| --- | --- | --- |
| Normal response | `COMMITTED` | 1 |
| Correction before dispatch | `ABORTED` | 0 |
| Correction after effect, before response | `COMMITTED_STALE` | 1 |
| Coroutine cancellation after effect | `IN_FLIGHT_UNKNOWN` | 1 |
| Lost receipt, coordinator reconstruction, attempted replay | `IN_FLIGHT_UNKNOWN`; recovered intent rejects dispatch | 1 |

The reconstruction schedule releases the lease after the call has stopped,
reopens the real journal under a new lease generation and attempts dispatch of
the recovered action. It is not an OS crash, lease-expiry takeover or live
provider-reconciliation test. The separate endpoint ledger survives this reopen.

`CASES`, `POLICIES`, `EXPECTED` and `METRICS` are declared before execution in
`eval/mutation_comparison.py`. Their canonical JSON hash is recorded as
`plan_sha256`. Every cell must match its frozen classification/effect count.
The matrix fails if full CCS violates a metric, an expected outcome changes,
or a negative control ceases to expose its intended safety defect. Known-bad
controls are expected to violate safety; their violations do not become CCS
successes or completed tasks.

## Metrics and interpretation

| Count | Independent definition |
| --- | --- |
| `duplicate_effects` | `max(0, external_effects - 1)` for this one-intent schedule. |
| `stale_dispatches` | Endpoint effects whose invocation began after the harness had retracted the intent. |
| `false_current_commitments` | Local `COMMITTED` despite a correction/cancellation, or without an external effect. |
| `false_aborts` | Local `ABORTED` despite a persisted external effect. |
| `safely_parked_cases` | Classifications `COMMITTED_STALE` or `IN_FLIGHT_UNKNOWN`; this is not recovery or task completion. |

The oracle reads scenario intent and the independent effect ledger, not runtime
logs or self-reported executor counts. Tests feed fabricated false local outcomes
to the oracle, remove the epoch signal, corrupt expectation controls, and verify
that dirty, changed or unavailable source provenance cannot produce a passing
experiment. Fixture agreement alone is insufficient: the full CCS safety counts
must also be zero, and every negative-control policy must expose a violation.

Five schedules times four policies produce **20 cells per repetition**. Repetition
is limited to 1..10 and increases repeated observations, not scenario diversity.
This small, intentionally selected corpus is not a statistical estimate of
deployment safety. No p-values, throughput, p95 latency or independent paper
replication are claimed. Grounding-quality and cross-task/tenant/worker races
remain separate evaluation dimensions.

## Reproduce on Windows and CI

Use a clean checkout and a [fresh hash-locked runtime](locked-environments.md).
Run the environment audit and complete tests, then choose a new output filename:

```powershell
.\.venv\Scripts\python.exe -m scripts.dependency_audit --profile runtime --output results/environment-runtime.json
if ($LASTEXITCODE -ne 0) { throw 'Runtime environment unverified.' }
.\.venv\Scripts\python.exe -m pytest -q -p no:cacheprovider
if ($LASTEXITCODE -ne 0) { throw 'Regression suite failed.' }
.\.venv\Scripts\python.exe -m eval.mutation_comparison --output results/mutation-comparison-001.json
if ($LASTEXITCODE -ne 0) { throw 'Comparison or source provenance unverified.' }
```

The report contains every normalized cell, policy totals, plan hash, Git revision,
runtime-lock hash, Python/platform and source-stability verdict. Git revision,
clean tracked/untracked status and provenance are checked before and after the
experiment. These are reproducibility checks, not signed host attestation or a
substitute for the separate installed-metadata audit. A dirty/unavailable/changed
checkout cannot yield a passing CLI result. Random action IDs, credentials,
paths, payloads and timestamps are omitted from observations; the normalized
core matrix is deterministic. Source/environment fields vary between checkouts.

CI runs the command after runtime regressions on Windows and Linux and uploads
`simulated-mutation-comparison-<OS>` artifacts for seven days. Preserve/download
these before expiry for an independent study; CI retention is not archival
publication. All jobs explicitly check out the triggering commit (the PR's head
for a PR event), so recorded Git provenance identifies the published source
instead of a temporary GitHub merge commit. Merge-integration testing remains a
separate concern. Dependencies and deployment code are unchanged by this phase.

Exit 0 means all declared controls and provenance checks passed. Exit 1 means a
completed report failed those checks. Exit 2 means invalid use, unavailable
execution or report-creation failure. Existing outputs are never overwritten;
choose a new filename. An interrupted report may be partial and is unverified.
Temporary databases contain synthetic credentials/effects only and are cleaned
on normal exit; abrupt process death can retain them in the OS temporary area.

Rollback removes the evaluator/CI step independently of the runtime. It does not
alter a live journal, approvals or provider state. Retain prior evaluation
artifacts and their matching commit/lock/plan hashes for comparison.
