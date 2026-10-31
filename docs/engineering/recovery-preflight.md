# Read-only recovery evidence preflight

Status: **LOCAL BRANCH VERIFIED** implementation; clean-checkout and CI evidence
belongs in the phase PR. Provider effects in the tests are **SIMULATED**. This
command performs no REAL PROVIDER calls and does not certify recovery readiness.

The protected invariant is that inspection must not initialize, migrate, repair,
acquire authority over, or dispatch from the journal being investigated. The
command uses a separate standard-library SQLite reader instead of constructing
the runtime journal. Missing files are never created. Reports contain fixed
verdicts and aggregate counts, without paths, action identifiers, payloads,
credentials, raw SQL errors or provider receipts.

## Run on Windows

Use the reviewed safety branch and its verified runtime environment. Stop and
drain hosts before an operational recovery investigation. Retain the original
database and all sidecars on protected storage; do not copy just a live SQLite
database file or delete a journal sidecar to make this command pass.

```powershell
.\.venv\Scripts\python.exe -m scripts.recovery_preflight `
  --journal 'C:\CCS\state\journal.db'
if ($LASTEXITCODE -ne 0) { throw 'Journal evidence is unverified; preserve it for investigation' }
```

The command emits one JSON report to stdout. Exit 0 means only that the specified
local evidence checks completed successfully. Exit 2 means unavailable, locked,
unsupported, inconsistent, over-budget or malformed evidence (or invalid CLI
arguments). There is no repair, restore, force, ignore or provider option. Preserve
the report separately from the journal if needed; never redirect stdout onto the
database. The tool cannot enforce shell redirection safety.

## Checks and bounds

- Existing file, SQLite header, rollback format, CCS application ID, schema 6 or 7,
  DELETE journal mode and required tables. WAL headers are rejected before SQLite
  opens the file, avoiding creation of WAL shared-memory sidecars.
- SQLite integrity check and a separate foreign-key check; integrity checking
  alone does not establish foreign-key consistency.
- One read transaction for all SQL observations. Each action/task projection must
  have contiguous event versions starting at zero and a matching latest event.
- Finite JSON with unique keys for current action/task projections; projection
  identity, action state, compensation state, chain ownership, task slot ownership
  and retained/released operation-key claims must agree.
- At most 100,000 rows per required table and 1 MiB per current projection. A
  ten-second cooperative SQL/Python budget and 250 ms SQLite lock timeout fail
  closed; these are resource limits, not measured latency guarantees or a hard
  OS I/O deadline. Large journals need a separately qualified inspection path.

See SQLite's [integrity and foreign-key pragmas](https://www.sqlite.org/pragma.html)
and [transaction isolation](https://www.sqlite.org/isolation.html) for the database
semantics. A read transaction can briefly delay writer commits. Do not use the
inspection as a continuous production health poll.

## Meaning of a successful report

`uncertain_actions` counts current forward IN_FLIGHT/IN_FLIGHT_UNKNOWN or
compensation IN_FLIGHT/UNKNOWN evidence. Zero does not prove absence of remote
effects, outstanding external requests, repair obligations or lost later history.
Nonzero uncertainty does not make the local database structurally inconsistent.

Every report, including successful ones, explicitly says:

```json
{"restore_authorized": false, "dispatch_authorized": false,
 "freshness_verified": false, "provider_reconciled": false}
```

A consistent old copy can pass all these checks while omitting later remote
effects, consumed approvals or revoked credentials. The stale-copy regression
demonstrates exactly this limitation. No comparison with an authoritative backup
catalog, external effect ledger or newer host is performed. Historical payload
semantics, all authorization/workflow contracts, tamper evidence, hostile SQLite
files/hosts and storage durability are outside this bounded check set.

## Response and rollback

On failure, preserve the evidence and keep dispatch stopped. Do not clear claims,
unconsume approvals, edit event history or replay an unknown mutation to obtain
a passing result. A database requiring crash recovery needs a separately
controlled recovery procedure; this reader will not repair it.

On success, continue the independent recovery investigation: establish freshness,
fence all old workers, reconcile provider evidence and review current security
authority before considering admission. Restoring an old journal as the live
source of truth remains unsupported. Backup capture, isolated restore drills,
credential rotation and a reviewed activation protocol are subsequent gates.

This phase changes no journal schema or dispatch behavior. Rolling back the
inspection command means using the preceding reviewed application code while
retaining the current journal and evidence. Never roll back database history.
