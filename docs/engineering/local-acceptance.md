# Test ChronoCortex locally: simulated two-step acceptance

You can test the **local console and durable task controls now**, without a
sandbox account, provider key, microphone or real payment. This walkthrough uses
the production authentication, approval, task, journal and worker boundaries.
Its two effects are **SIMULATED** inserts into a separate local SQLite ledger.
It is not live voice/provider acceptance or a production deployment.

## Start on Windows

Use `codex/local-acceptance-walkthrough`, not public `main` (which remains Phase 0).
First follow the [locked environment setup](locked-environments.md#clean-windows-installation)
if `.venv` is unavailable. From the repository root in PowerShell:

```powershell
.\scripts\start_local_acceptance.ps1
```

If PowerShell execution policy blocks the script, invoke it only for this
process; no machine/user policy change is needed:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\start_local_acceptance.ps1
```

The launcher creates a unique `CCS-Acceptance-...` directory under your local
application-data folder. It sets a protected current-user/SYSTEM ACL before
Python creates the journal or credentials, prints the directory and loopback
URL, then starts the app. Existing files/runs are never reused or overwritten.
An optional `-Python` selects a different qualified interpreter; `-Port 8878`
selects another loopback port if 8877 is occupied. No host/provider/key override
exists. Do not choose shared/synced storage or a Git worktree.

Open **http://127.0.0.1:8877** after `LOCAL_ACCEPTANCE_READY` appears (use the
printed URL if you selected another port). In a second
PowerShell window, use the **exact new directory printed by the launcher**:

```powershell
$acceptanceRun = 'C:\Users\YOUR_USER\AppData\Local\CCS-Acceptance-REPLACE_WITH_PRINTED_ID'
Set-Clipboard -Value (Get-Content -Raw -LiteralPath (Join-Path $acceptanceRun 'operator.secret'))
```

Paste that credential into **Operator credential**, then choose **Connect**.
Clear the clipboard afterward:

```powershell
Set-Clipboard -Value ''
```

Never paste this bearer credential into chat, screenshots, an issue or a URL.
The login field clears after connection; authentication/session/CSRF and domain
permissions still apply. This is same-machine HTTP, not verified human login,
SSO, TLS/remote hosting or an Internet-facing service.

## Walkthrough A: two approvals, one task

1. Confirm the **SIMULATED LOCAL ACCEPTANCE** banner. Open the one task named
   **SIMULATED local acceptance: two approvals**. It starts waiting for authority;
   there should be no completed effect before your decision.
2. Open the PENDING **SIMULATED.reserve** action. Choose **Approve forward
   operation**, review its exact arguments, and enter execution principal
   **SIMULATED-local-worker**. Check **I reviewed this exact evidence** and choose
   **Record decision**.
3. Refresh journal evidence. Step one commits in the fake ledger. The task
   reaches **1 / 2**, with a different PENDING **SIMULATED.confirm** action.
   The first approval cannot authorize that second operation.
4. Review and approve the second action for the same execution principal.
   Refresh the overview and selected evidence panel. The task should become
   **COMPLETED**, with two COMMITTED actions and **2 / 2** progress.
5. Inspect **Approvals** and **Decision history** for consumed approvals and
   durable decisions. Disconnect/reconnect: task identity and evidence remain.

Overview snapshots and selected evidence are separate reads. The worker can
advance between them; use **Refresh** and **Refresh evidence** before comparing
or deciding. A transient status notice is not a durable receipt.

## Walkthrough B: explicit cancellation

Stop the first run with **Ctrl+C** and start a new run with the same launcher.
It allocates a new directory and task, preserving the earlier evidence.

- Cancel the task before the first approval: **Record cancellation intent**
  should leave zero fake effects and no step execution.
- Or approve step one, wait for step two, then cancel the task: the task becomes
  CANCELLED, step two does not execute, and step one's effect remains recorded.
  Cancellation does not silently reverse it.

There is no automatic compensation, automatic approval issuance or mutation
retry. Browser logout closes the consumer session, not the owned task. Ctrl+C
requests supervised shutdown; it does not delete evidence or establish rollback.
An OS/storage hang or forced process kill is not a guaranteed graceful drain.
The command reports a clean interruption only after owned drain and lease
release are verified; parked uncertainty or an incomplete stop returns failure.

## Optional independent check of the fake effects

With `$acceptanceRun` still set to the exact run directory, this reads the
separate ledger without creating a missing database or changing task state:

```powershell
$env:CCS_ACCEPTANCE_LEDGER = Join-Path $acceptanceRun 'simulated-effects.db'
@'
import os, sqlite3
from contextlib import closing
from pathlib import Path
path = Path(os.environ['CCS_ACCEPTANCE_LEDGER']).resolve(strict=True)
with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True)) as db:
    print('SIMULATED committed steps:', db.execute('SELECT step FROM effects ORDER BY step').fetchall())
'@ | .\.venv\Scripts\python.exe -
```

Expect no rows before approval, step 1 after the first effect and steps 1/2
after completion. A temporary read failure is unavailable evidence, not zero
effects. This ledger models only this fake executor, not Stripe idempotency.

## Data, scope and failure handling

Each run keeps `acceptance.db`, `simulated-effects.db`, `operator.secret` and
`worker.secret` in its protected directory. Credentials expire within 24 hours;
the UI's session has its own shorter lifetime. Do not copy the directory into
Git. The worker can only execute two fixed SIMULATED tools and exact static
arguments; no external provider adapter or microphone is imported/configured.

Startup pre-admits a canned task using a separately identified fixture seeder,
then revokes that seeder credential. This is **not an interactive human review
of a voice proposal**. The walkthrough tests execution approvals and task
controls; protected proposal-review UI, semantic mapping and live participant
identity remain separate product work.

On an error, inspect the journal/ledger and preserve the directory. The worker
stops admission; it does not manufacture a new operation or replay UNKNOWN.
The launcher refuses to resume an existing directory. A new independent fake
run is not a recovery mechanism for an unresolved operation in the old run.
No existing user journal is migrated or adopted by this entrypoint.

## Automated verification and remaining building

```powershell
.\.venv\Scripts\python.exe -m pytest -q tests/test_local_acceptance.py tests/test_windows_local_acceptance.py
.\.venv\Scripts\python.exe -m pytest -q --junitxml=results/runtime-tests.xml
```

The separate browser profile runs real Chromium desktop/mobile journeys; see
[locked browser qualification](locked-environments.md). Tests cover separate
approvals, disconnect, cancellation before/after an effect, revoked worker
authority, no-overwrite/refused resume, loopback-only CLI and native ACL setup.

Testing levels are different gates:

| Level | When / remaining requirement |
| --- | --- |
| Local console/task walkthrough and automated safety tests | Available with this branch and qualified environment now. All effects are SIMULATED. |
| Connected research alpha | Requires protected proposal-review product flow plus chosen/configured voice deployment, participant/privacy boundaries and real sandbox acceptance assets. |
| Production release candidate | Requires all seven [acceptance workstreams](production-readiness.md), including diagnosed reliability findings, operations, recovery activation/custody, independent evaluation and consolidated security/release review. |

There is substantial integration/acceptance work left. Seven workstreams are
not seven small commits, and no defensible calendar date or completion
percentage follows from the test count. Real-provider testing requires the
dedicated sandbox/restricted credential; live voice requires real audio assets
and a reviewed configuration. Neither is necessary for this local walkthrough.
