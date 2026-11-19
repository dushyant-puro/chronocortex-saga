# ChronoCortex-Saga (CCS-Agent)

ChronoCortex-Saga is a research system for safe, recoverable, multi-step work
that can outlive the conversation that started it.

> **VOICE TURN ≠ TASK.** A spoken turn can be interrupted or corrected. A task
> must retain its identity, authority, history, and uncertain external effects.

## Repository status

The code baseline on the default `main` branch is Phase 0 at
[`4162ec1`](https://github.com/dushyant-puro/chronocortex-saga/commit/4162ec17f4bda15c13de3fa1f0e1e33e3fe35591).
Only README documentation has been added on top of that baseline. The
accumulated safety implementation is being developed in stacked pull requests
and is **not merged into `main`**.

| Evidence label | Status |
| --- | --- |
| **PUBLIC MAIN VERIFIED** | Main's code remains at the Phase 0 baseline above; only README documentation has been added since. It does not contain the current safety stack. |
| **LOCAL BRANCH VERIFIED** | The current implementation is on [`codex/ci-correctness-recovery`](https://github.com/dushyant-puro/chronocortex-saga/tree/codex/ci-correctness-recovery), at `d53285698a2908d10bded76d2af9e7290481baed`. Its latest phase is draft [PR #45](https://github.com/dushyant-puro/chronocortex-saga/pull/45), based on the preceding recovery-host phase. |
| **SIMULATED** | Provider fixtures, synthetic voice evidence, and process-fault drills. They do not prove live provider or live audio behavior. |
| **REAL PROVIDER** | No real provider acceptance has been completed. |

GitHub's branch page compares each branch to `main`, so the current branch's
“ahead” count includes the preceding stacked phases. PR #45 itself is based on
`codex/configured-recovery-host`. The stack is intentionally kept separate
from the Phase 0 default branch while its phases are reviewed.

## Current implementation

The safety branch includes a durable SQLite task journal, server-checked
operator authority, supervised worker ownership, conservative handling of
unknown outcomes, an operator console, reviewed task admission, recovery and
backup tooling, deterministic voice and mutation evaluations, and restricted
provider reconciliation interfaces. The UI is an observability and control
surface; it is not the source of truth.

Speech is proposal evidence. It does not authorize, cancel, or complete a task.
Coroutine cancellation and local fencing do not undo a remote provider effect.
Unknown mutations are not blindly retried. Automated provider tests and voice
replays are **SIMULATED**; no production-readiness, live voice, or real provider
acceptance is claimed.

The latest branch head, `d53285698a2908d10bded76d2af9e7290481baed`, passed all
five hosted runtime, browser, and advisory jobs in
[GitHub Actions run 44557121015](https://github.com/dushyant-puro/chronocortex-saga/actions/runs/44557121015).
Earlier CI failures and the exact source they apply to remain in the branch's
[implementation ledger](https://github.com/dushyant-puro/chronocortex-saga/blob/codex/ci-correctness-recovery/implementation.md)
and [production-readiness ledger](https://github.com/dushyant-puro/chronocortex-saga/blob/codex/ci-correctness-recovery/docs/engineering/production-readiness.md).
These checks qualify that branch snapshot only; they do not qualify `main` or a
later commit automatically.

## Try the current safety implementation on Windows

Use the branch that contains the code and its matching documentation:

```powershell
git clone --branch codex/ci-correctness-recovery https://github.com/dushyant-puro/chronocortex-saga.git
Set-Location chronocortex-saga
```

Then follow that checkout's [README](https://github.com/dushyant-puro/chronocortex-saga/blob/codex/ci-correctness-recovery/README.md)
and [locked environment guide](https://github.com/dushyant-puro/chronocortex-saga/blob/codex/ci-correctness-recovery/docs/engineering/locked-environments.md)
for Python 3.13 setup, hash-locked dependencies, tests, and the local operator
walkthrough. The walkthrough uses **SIMULATED** local effects and needs no
provider key or microphone.

For the full design, implementation history, current evidence, and remaining
acceptance gates, see the branch's
[implementation record](https://github.com/dushyant-puro/chronocortex-saga/blob/codex/ci-correctness-recovery/implementation.md).
