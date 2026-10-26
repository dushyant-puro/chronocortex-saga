# Durable-task and voice-isolation evaluation

This **SIMULATED** experiment exercises an actual three-step `TaskManager`
workflow under `WorkerSupervisor`, `SpeculativeSagaManager`, scoped local
credentials, SQLite checkpoints and worker leases. Executor binding is the only
task-manager override. No production dispatch/lifecycle method is copied or
replaced, and no `simulation=True` authorization bypass or test helper is used.
PUBLIC MAIN remains the unmerged Phase 0 baseline.

The [mutation comparison](mutation-comparison.md) tests one mutation at a time.
This evaluator additionally checks historical progress, dependent-step admission,
consumer-wait cancellation, explicit task cancellation, and conservative task
reconstruction. All effects and voice inputs are synthetic. It does not measure
microphones, ASR, a real provider, remote process death, latency or deployment
task-success rates.

## Schedules and the independent oracle

Each schedule starts one fresh three-step workflow with explicit deployment
revisions. A separate SQLite file records each effect, with no deduplication.
Step zero commits normally. Step one's external effect commits **before** an
`asyncio.Event` announces the response-wait barrier. The harness then injects
the declared event and releases the response. No timing sleeps select the race.
Step two must run only when its predecessor remains valid and committed.

| Schedule | Expected task outcome | Effect counts by step |
| --- | --- | --- |
| Normal | `COMPLETED` | `[1,1,1]` |
| Voice revisions, new turn and disconnect | `COMPLETED`; task epoch unchanged | `[1,1,1]` |
| Cancel an active consumer's `WorkerOperation.wait()` | `COMPLETED`; owned work continues | `[1,1,1]` |
| Trusted task invalidation while step one awaits | `NEEDS_RECONCILIATION`; second action stale | `[1,1,0]` |
| Lose step one's receipt | `NEEDS_RECONCILIATION`; second action UNKNOWN | `[1,1,0]` |
| Explicit trusted task cancellation while step one awaits | `CANCELLED` intent; second action stale | `[1,1,0]` |
| Reconstruct the task after the lost receipt | `NEEDS_RECONCILIATION`; historical authority remains parked | `[1,1,0]` |
| Negative control: incorrectly connect voice churn to the task epoch | `NEEDS_RECONCILIATION`; voice-independence violation detected | `[1,1,0]` |

The voice schedule sends same-turn revisions through real `VoiceIngress` objects,
then completes a synthetic proposal, opens another turn and disconnects. Words
including "yes" and "cancel" carry no task command or approval authority. This
is an ingress contract experiment, not SDK streaming or live speech acceptance.
The negative control uses the same real runtime but deliberately injects a task
epoch advance after voice churn; it demonstrates a host-integration defect.

Task invalidation/cancellation are trusted harness injections. This evaluator
does not infer task corrections from speech, test operator HTTP authorization,
or claim that a spoken cancellation is an authenticated task command. The
synthetic tool requires a scoped worker permission; real-provider exact approval
acceptance remains a separate gate.

For every schedule the harness explicitly calls resume again. That call must
preserve the original task identity, the first two operation IDs/generations,
the first receipt, and the independent effect counts. It must not reset an
uncertain step or dispatch a dependent mutation. A cancelled consumer wait is
already active inside `WorkerOperation.wait()` when cancelled; cancelling the
wait is distinct from cancelling the durable objective.

Reconstruction occurs only after the original call stops and the first host
drains/releases its lease. A new journal and supervisor acquire a new lease
generation, hydrate the matching workflow and resume conservatively. The
evaluator refuses to start that second host if the original stop is unverified.
The
first historical receipt remains available, but its loaded action is stale;
both prior steps park and the task index returns to zero. Retained evidence
does not restore current dispatch authority. This is controlled reconstruction,
not OS-crash, lease-expiry takeover, provider reconciliation or automatic repair.

## Predeclared checks and counts

`CASES`, `EXPECTED` and `METRICS` are declared before execution and their canonical
JSON hash appears as `plan_sha256`. Every task/step/action classification, task
index, epoch and external effect vector must match its frozen expectation.
Worker drain/release must also be verified before the experiment can pass.

| Violation count | Definition |
| --- | --- |
| `duplicate_effects` | Sum of `max(0, count-1)` over the three distinct step operations. |
| `blocked_descendant_effects` | Third-step effects after a stale, unknown or explicitly cancelled predecessor. |
| `false_task_completions` | `COMPLETED` for a fault schedule, or without exactly one effect per step. |
| `false_abort_claims` | An action classified `ABORTED` despite that step having an external effect. Task `CANCELLED` denotes intent and is not a rollback claim. |
| `lost_prior_evidence` | Original task/action identity, generation, first receipt or single first effect is lost. |
| `voice_independence_violations` | Voice-only input advances the durable task's epoch. |

The seven ordinary bindings must have zero measured violations. The eighth,
deliberately coupled binding must expose the voice-independence defect. Its
results remain visible in raw observations and are excluded from ordinary task
outcome totals. A passing run expects **three completed simulated tasks, three
parked tasks and one cancelled objective**. Do not combine them into a recovery
or success percentage. Parking, preserved evidence and completed business work
answer different questions.

Tests deliberately supply false completion/abort claims, duplicate/descendant
effects, lost identities/receipts and malformed counts to the oracle. They also
check the real restart classifications, active-wait cancellation, deterministic
reports, dirty source, changed frozen expectations and unverified drain cleanup.
This selected eight-schedule corpus is not a statistical safety estimate,
third-party system comparison, independent replication or universal novelty proof.

## Windows reproduction and artifacts

Use a clean checkout and the [locked runtime setup](locked-environments.md):

```powershell
.\.venv\Scripts\python.exe -m scripts.dependency_audit --profile runtime --output results/environment-runtime.json
if ($LASTEXITCODE -ne 0) { throw 'Runtime environment unverified.' }
.\.venv\Scripts\python.exe -m pytest -q -p no:cacheprovider
if ($LASTEXITCODE -ne 0) { throw 'Regression suite failed.' }
.\.venv\Scripts\python.exe -m eval.durable_tasks --output results/durable-tasks-001.json
if ($LASTEXITCODE -ne 0) { throw 'Task evaluation or source provenance unverified.' }
```

The report retains every normalized observation, all violation counts, separate
task outcomes, plan hash, exact Git revision, runtime-lock hash, Python/platform
and source-stability verdict. It uses the mutation evaluator's before/after
clean-source checks; these are reproducibility checks, not signed attestation.
Raw transcript text, credentials, random IDs, request/result payloads, paths and
timestamps are omitted. The installed environment audit is still independently
required. Reports are exclusively created and never overwrite earlier evidence.

CI runs the evaluator after runtime regressions on Windows/Linux at the explicit
published commit and retains `simulated-durable-tasks-<OS>` artifacts for seven
days. Preserve the artifacts and matching commit/lock/plan before expiry for an
independent study. Passing CI is not archival publication or live acceptance.

Exit 0 means all declared expectations and source checks passed. Exit 1 means
a completed report failed those checks. Exit 2 means invalid use, unavailable
execution or report-creation failure. Partial reports are unverified. Private
synthetic databases are removed only after every host drain/release is verified;
an exception during the schedules or an unverified drain retains
`ccs-task-evaluation-*` evidence in the
OS temporary directory for investigation. Abrupt process death can also retain
it. Do not sweep an evaluator directory beneath work with an unverified lifetime.

This phase changes no production task/dispatch behavior, journal schema,
dependency, provider configuration or UI authority. Rollback removes the
evaluator/CI integration independently and retains earlier artifacts; it never
restores older journal history, clears UNKNOWN evidence or retries a mutation.
