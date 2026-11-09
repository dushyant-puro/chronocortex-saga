# Configured local recovery host

**LOCAL BRANCH VERIFIED.** The existing recovery-only worker and protected
operator console now run in one foreground process. Provider and voice are
**NOT_CONFIGURED**. Tests use **SIMULATED** historical effects. No REAL PROVIDER,
microphone, native service installation, deployment or release is qualified.

The invariant is: **starting, checking or reconnecting a recovery host must not
activate or replay a recorded mutation**. The host has an empty tool registry
and no executor, compensator, provider-query adapter, task rebinder, proposal
ingress or automatic task selector. It can inspect evidence and record only
operator decisions allowed by current permissions. Those decisions do not invoke
a provider through this host.

## Start on Windows

Use `codex/configured-recovery-host` with the [locked runtime](locked-environments.md).
Start with an existing, stopped, current-schema journal in protected local
storage. Confirm the previous process is stopped; lease expiry alone is not proof.
Do not migrate, provision or rotate credentials against a live host.

For an isolated trial, stop the [SIMULATED manual walkthrough](manual-review-walkthrough.md)
with Ctrl+C and confirm clean shutdown. Preserve its exact private run directory.
Its worker/operator files can inspect that same SIMULATED journal while valid.
Do not rerun the acceptance launcher against that directory. For a separate
installation use the [protected Windows setup](operator-control-console.md#windows-setup-new-local-installation)
and [separate worker provisioning](worker-supervision.md#recovery-host-and-managed-credential-input).
The worker needs `ccs:work`; keep its bearer file separate from the operator's.
Use an observer-only operator when decision controls are not needed.

Replace `$recoveryRun` below with the **exact existing protected directory**:

```powershell
Set-Location 'C:\Users\dushi\Desktop\ChronoCortex Saga'
$recoveryRun = 'C:\Users\YOUR_USER\AppData\Local\CCS-Acceptance-PRINTED_ID'
$recoveryConfig = Join-Path $recoveryRun 'recovery-host.json'
if (Test-Path -LiteralPath $recoveryConfig) { throw 'Preserve and inspect the existing configuration.' }
$recoverySettings = @{
    schema = 1
    mode = 'RECOVERY_ONLY'
    journal = Join-Path $recoveryRun 'acceptance.db'
    worker_credential_file = Join-Path $recoveryRun 'worker.secret'
    port = 8877
}
[IO.File]::WriteAllText($recoveryConfig, ($recoverySettings | ConvertTo-Json), [Text.UTF8Encoding]::new($false))
.\.venv\Scripts\python.exe -m operations.recovery --config $recoveryConfig --check
if ($LASTEXITCODE -ne 0) { throw 'Startup check failed; preserve evidence and inspect before starting.' }
.\.venv\Scripts\python.exe -m operations.recovery --config $recoveryConfig
```

For a separate installation change `journal` to its actual existing database.
No journal, credential or schema is created/migrated by these commands. The
sample is a same-machine SIMULATED trial, not production provisioning. Config
inherits its protected parent's ACL; the Python reader does not certify Windows
ACL custody. Administrators must prevent parent-path replacement and use durable
local storage suitable for SQLite locking/fsync. UNC paths and symlinks are
rejected; mapped/network mounts are not certified by a pathname check.
Unsupported relative/UNC paths are rejected before any file probe, so a rejected
UNC configuration cannot initiate a network filesystem lookup during validation.

`--check` is an **active ownership check**, not read-only preflight: it binds
the port, authenticates, acquires a lease, hydrates recovery evidence, drains and
releases. It records worker/security/clock observations but serves no HTTP and
changes no action/task/effect history. It never force-takes or retries ownership.
A busy port is rejected before journal construction/acquisition. Use the separate
[read-only preflight](recovery-preflight.md) when acquisition is inappropriate.

After `RECOVERY_HOST_READY`, open its printed HTTP URL (default
`http://127.0.0.1:8877`). In another PowerShell window, copy the operator file
locally, connect in the browser, then clear the clipboard:

```powershell
$recoveryRun = 'C:\Users\YOUR_USER\AppData\Local\CCS-Acceptance-PRINTED_ID'
Set-Clipboard -Value (Get-Content -Raw -LiteralPath (Join-Path $recoveryRun 'operator.secret'))
# Paste into Operator credential and connect, then:
Set-Clipboard -Value ''
```

Never put credentials in chat, URLs, issues, screenshots or commits. Check the
**RECOVERY ONLY** banner. The authenticated **Current recovery host** panel offers
**Check recovery host**: an observation, never activation, restart or execution.

## Configuration and readiness contracts

Schema-1 JSON accepts exactly `schema`, `mode`, `journal`,
`worker_credential_file`, `port`, plus optional `worker_settings`. The config
must be a regular single-link file, bounded to 16 KiB; POSIX additionally requires
owner-only permissions. UTF-8 with/without a BOM supports Windows tooling.
Duplicate fields, nonfinite numbers, booleans in integer fields, unknown fields
and malformed input fail with bounded messages. Paths must be absolute existing
files: there is no working-directory/environment expansion. No raw secrets,
provider, remote bind, TLS, proxy, auto-restart or mutation setting is accepted.

Optional timings/capacity use the existing validated `WorkerSettings`. Defaults
remain lease TTL 30,000 ms, renewal interval 5,000 ms, safety margin 1,000 ms,
drain budget 10,000 ms and capacity 32. These are policy, not measured SLOs;
this phase changes no lease, watchdog, retry or timing policy.

`GET /api/host-status` uses the console's exact-origin boundary, current
`ccs:observe` session, no-store headers and CSP. Unauthenticated/cross-origin
requests cannot trigger a privileged probe. It checks local monotonic lifecycle,
journal lease ownership/clock, security clock and current worker credential/
`ccs:work`, then rechecks lifecycle after SQL and on return to the owning event
loop. It may update existing clock high-water marks. Successful schema-1 status
reports `RECOVERY_ONLY`, `ready=true`, `state=READY`; dispatch/restore/freshness/
provider-reconciled flags all remain false. No path, secret or payload is exposed.

Success is a **point-in-time observation**, not future liveness, snapshot
freshness, custody, provider reconciliation or remote rollback. The UI says
**passed the last check**. Login changes clear it; late old-session responses
cannot repopulate another login. Invalid/unavailable responses cannot display
success. Unconfigured consoles keep their previous capability shape and hide
the panel.

A failed privileged probe requests supervised stop without retry/reacquisition
and returns 503. A watcher also exits the server when the worker independently
starts stopping, including heartbeat failure. Shutdown logs
`RECOVERY_HOST_STOPPED`, the actual drain/release report and `healthcheck_failed`.
A failed probe yields exit 1 even after successful cleanup. Denied operator
requests do not stop the worker. There is no unauthenticated health endpoint.

## Shutdown and rollback

Use Ctrl+C and await the final report. Exit 0 requires drained operations,
released lease, no worker failure and no failed probe. Otherwise preserve the
configuration, journal, claims, receipts, approvals and redacted logs for
inspection. Do not automatically restart or equate process termination with
remote rollback. An external watchdog/native service still needs qualification.

To roll back this packaging change, stop it and use the prior separate
`python -m worker` / `python -m control` tools from a compatible checkout, following
their runbooks. There is no schema change. Never lower headers, delete UNKNOWN,
overwrite newer evidence with a snapshot, or enable callbacks to make recovery
look completed. Windows Service/systemd installation, service-account custody,
external watchdog, off-host backups, TLS/remote identity, real provider/audio and
measured operational acceptance remain open.

## Qualification

Tests cover preserved seeded history, empty registry, current operator/worker
authority, origin/method boundary, competing owners, changed lease, monotonic/
security-clock failure, stop during probe, bounded strict config, busy-port
ordering, active check drain/release, and a real server process exiting 1 after
revocation. Chromium covers desktop/mobile, invalid/unavailable observations and
late replies across logout/login. Exact-source results belong to the phase PR.

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_recovery_host.py -q -p no:cacheprovider
.\.venv-browser\Scripts\python.exe -m pytest browser/recovery_host_cases.py -q -p no:cacheprovider
```

Inherited intermittent CI findings remain deferred release gates. Passing
offline checks do not establish their causes fixed or close any of the seven
readiness workstreams.

### Cancellation race found during compatibility qualification

The first fresh-source run at `4bee1cf` recorded **1,671 passed, 1 failed,
2 skipped** in the existing manual-host test for cancellation followed by a
parked admission. Retained evidence showed PENDING -> WAITING_FOR_AUTHORIZATION
-> CANCELLED, one PENDING action and no dispatched effect. Cancellation closed
the chain between checkpoint inspection and the read-only approval lookup;
the driver treated its PermissionDenied result as a host failure, released the
lease, and the later admission correctly failed closed. This was not evidence
of a remote mutation, rollback, or the older watchdog/lease-takeover failures.

New barriers reproduced the lookup race both before any effect and after the
first known effect. Both normal-cancellation cases failed before the correction;
both worker-revocation negative controls already stopped safely. The correction
finishes only this read-only lookup when a fresh durable CANCELLED checkpoint
and current worker/lifecycle authority hold. Earlier effects remain; no mutation
is resumed, repeated or compensated. Revocation still stops the host. All 68
focused manual/legacy/recovery-host cases then passed. Final fresh-source full
qualification follows this correction and is recorded separately below.
