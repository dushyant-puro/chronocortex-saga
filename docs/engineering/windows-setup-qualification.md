# Windows setup timeout qualification

Base: `codex/failure-diagnostics`, `04197c0`. Review date: 2026-10-31.
The prior phase's push run failed in the real Windows PowerShell key-setup test
after its existing 20-second subprocess budget. Its PR run passed. The recorded
failure did not identify whether startup, path inspection, ACL handling, prompting,
file I/O or process teardown was delayed. The historical cause remains unconfirmed.

The unchanged local baseline passed all 21 existing Windows setup cases. A passing
local run does not replace or explain that CI failure. This phase adds evidence;
it does not claim to fix an unobserved root cause or qualify real Stripe access.

## Diagnostic contract

`scripts/save_stripe_sandbox_key.ps1 -Diagnostics` optionally writes fixed stage
markers to stderr. Diagnostics are off by default. Markers contain no key,
directory, account identity, exception text or arbitrary caller data. Stages cover
path validation, directory creation, ACL application/verification, masked input,
key validation, exclusive file creation, writing, flushing, cleanup and completion.
The same native validation and failure codes still govern the operation.

Diagnostic writes are best effort. A closed diagnostic writer cannot bypass
secret cleanup, relax ACL checks or turn a failed operation into success. The
file is still exclusively created and flushed; existing paths are refused.
No key argument or environment-key fallback, retry or provider request is added.

The test-only runner emits `PROCESS_STARTED` before test source execution. It
retains the Windows PowerShell 5.1 executable, standard-module isolation and the
same **20-second** budget. On a timeout it reports only a fixed reason and last
recognized stage, omitting the command and captured partial payload. Unknown,
missing or malformed markers become `UNOBSERVED`; absence is not proof that the
process never started. Its parser bounds both the inspected tail and marker count.
Success still returns the command's actual exit code/stdout/stderr, with the
simulated command source omitted from the returned argument metadata.

Only use this runner for controlled test code with SIMULATED keys. It is not a
production secret-input API. Python subprocess cleanup remains the parent-process
mechanism; the budget is not a measured wall-clock SLO, process-tree guarantee or
an external watchdog. Native creation, scheduling or cleanup can require separate
host investigation.

## Reading the evidence

| Last reported stage | Evidence available; no stronger inference is permitted |
| --- | --- |
| `UNOBSERVED` | No recognized marker was retained. Startup, early exit, capture loss or another condition remain possible. |
| `PROCESS_STARTED` | The wrapper executed. A later script/security stage was not observed. |
| `APPLY_ACL` / `VERIFY_ACL` | The named stage was entered; successful protection must still be verified independently. |
| `READ_SECRET` / `VALIDATE_SECRET` | Input/validation was entered; key validity or successful persistence is not established. |
| `CREATE_FILE` / `WRITE_FILE` / `FLUSH_FILE` | File state may have changed. Preserve the folder and inspect it; do not retry based on a missing receipt. |
| `CLEANUP` / `COMPLETE` | The marker is not proof of process exit, durable custody or provider authority. |

Markers are diagnostic observations, never authorization or rollback evidence.
The tests check actual protected ACLs before their simulated input and inspect the
resulting file independently. A blocked native process uses an unreleased event
before script invocation, not a timing sleep; the unchanged timeout must fail
without setup creation or an automatic retry. Other cases verify default-off
behavior, a broken diagnostic sink, redacted failures, malformed/oversized markers,
environment isolation and missing executables. The earlier security cases remain.

## Reproduce and retain

Use the exact [locked runtime environment](locked-environments.md):

```powershell
New-Item -ItemType Directory -Path results -Force | Out-Null
.\.venv\Scripts\python.exe -m pytest tests/test_windows_shell_support.py `
  tests/test_windows_stripe_key_setup.py -q -p no:cacheprovider `
  --junitxml=results/windows-setup-tests.xml
if ($LASTEXITCODE -ne 0) { throw 'Setup qualification failed; preserve its evidence.' }
```

The native ACL/process cases require Windows PowerShell and are explicitly
skipped on other systems. Parser/runner-control unit cases run on both platforms.
No real key, sandbox account, microphone, SDK session or provider call is used.

Runtime CI now retains JUnit output alongside its existing installation,
environment and wheel inventory evidence, including when pytest fails before
evaluation scorecards run. Review the exact commit, platform/Python identity,
test reason and last reported stage; correlate operator-controlled host data
before attributing startup or storage as the cause. Do not publish real secret
input in command arguments, test source, raw tracing or arbitrary stderr.

For an explicitly authorized real local setup, the existing masked script accepts
`-Diagnostics` as an additional switch. Choose a new protected directory as in the
[sandbox setup guide](stripe-sandbox-setup.md); a timeout or partial folder calls
for inspection, not rerunning the command on the same path.

## Rollback and remaining gate

Diagnostics are additive and need no migration. Revert tracing/runner changes
independently while retaining partial folders and failure evidence. No credential
or action journal should be reset, and no remote effect should be retried.
The next actual failure must establish the delayed stage/cause before a corrective
change is justified. These schedules do not explain the older worker renewal or
peer-exit failures. The seven [production acceptance areas](production-readiness.md)
and REAL PROVIDER/live audio acceptance remain open.
