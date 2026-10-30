# Console read recovery and uncertain decisions

Base: `codex/verified-offline-wheels`, `1d303b3`. The previous phase's PR Windows
browser run exposed a persistent error notice after a transient journal failure.
Its push browser run passed. This follow-up retains that failure as evidence;
successful archive checks did not qualify the whole integration.

The console remains an authenticated control/observation surface over the
journal. A failed journal read pauses controls. A later successful snapshot can
clear a transient read-unavailable notice; reading a single record alone cannot
restore global freshness. Changed selected evidence still needs a new detail
read before controls can be used. The backend independently rechecks authority
and exact evidence when recording every command.

Command-response uncertainty has a different lifetime. A failed transport or
unparseable JSON receipt may occur after the command was recorded. The console
closes its review, pauses controls and preserves the operation ID and explicit
no-automatic-retry warning through later failed or successful reads. Neither a
snapshot nor a detail read proves that a remote effect was rolled back. Read
errors and changed-evidence notifications cannot overwrite this warning.
Explicit stale-review rejections remain evidence-change notices rather than
being mislabeled unknown command outcomes.

A delayed failed detail response is checked against both the inspection number
and session identity, as successful responses already were. It cannot poison a
new operator's view after disconnect/reconnect. This does not claim cancellation
of the old HTTP request, universal response-schema validation or rollback of any
recorded command. Database contention/storage diagnosis and measured availability
remain operations work; the journal's locking and fail-closed behavior are not
weakened by this UI fix.

`browser/console_cases.py` uses real Chromium, loopback HTTP and SQLite. New
controlled schedules cover repeated snapshot failures, successful detail-only
reads while stale, successful snapshot recovery, an old session's delayed detail
failure, and malformed/lost command receipts. The command tests first let the
actual endpoint record the request, then replace/drop its response, inject a GET
failure and recover reads. They assert exactly one command attempt, the retained
warning and unchanged UNKNOWN effect evidence. Effects remain **SIMULATED**.

Run the exact locked browser profile in a fresh environment as described in
[locked environments](locked-environments.md), then:

```powershell
.\.venv\Scripts\python.exe -m scripts.dependency_audit --profile browser --output results/environment-browser.json
if ($LASTEXITCODE -ne 0) { throw 'Browser environment unverified.' }
.\.venv\Scripts\python.exe -m playwright install chromium
if ($LASTEXITCODE -ne 0) { throw 'Chromium assets unavailable.' }
.\.venv\Scripts\python.exe -m pytest browser/console_cases.py -q -p no:cacheprovider
```

The phase PR owns its final clean-checkout and CI results. Real audio/provider
acceptance and the [seven readiness workstreams](production-readiness.md) remain
open. For rollback, retain the current journal and decision history, close
admission when needed, and revert only the UI code. Never clear approvals,
operation IDs or uncertain effects to make a view appear healthy.

## Qualification findings

Review date: **2026-10-30**; the cited CI evidence was recorded on 2026-10-28.

At `b3d44f0`, all five PR CI jobs passed, but the push run was not green. Its
Linux browser run showed two distinct test assumptions: the integration test
required a valid success acknowledgement to disappear, and the read-recovery
protocol test depended on a subsequent real disk read succeeding. The first now
asserts committed evidence with no error notice, allowing a legitimate approval
acknowledgement. The second explicitly controls failing/successful GET responses
using an initially authenticated snapshot of an unchanged fixture. That tests
browser state handling; it does not certify backend availability. Actual HTTP,
journal, authority and command integration remain covered separately.

The same push run's Windows voice/task test reported `RENEWAL_FAILED` during
shutdown after its task assertions passed. The preceding archive phase also
reported a hidden-effect peer exit timeout and an incomplete lost-response drill
on Windows. Passing other runs does not explain these failures. Their causes
remain an operations/evaluation qualification finding; no lease deadline,
database timeout, safety assertion or UNKNOWN classification is relaxed here.
Before release, reproduce them with bounded safe diagnostics and distinguish
host/storage delays from a lifecycle defect. Do not automatically retry effects.
