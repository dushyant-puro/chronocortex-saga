# Hash-locked Python environments

Status: **LOCAL BRANCH VERIFIED** implementation; exact-head clean-install and
Windows/Linux CI evidence are recorded in the phase PR. This closes the Python
dependency-selection and installation-integrity gate for the declared targets.
It is not a production-readiness, package-vulnerability or provider certification.
Provider/voice effects remain **SIMULATED**; no REAL PROVIDER acceptance was run.
PUBLIC MAIN VERIFIED remains the unmerged Phase 0 baseline, not this safety stack.

## Contract and boundaries

The supported audit targets are CPython 3.13 on Windows AMD64 and Linux x86_64.
The exact interpreter patch, pip version and selected graph are recorded in
evidence. OS images, Python installers, the venv bootstrap pip and browser/native
system libraries are not locked by Python requirements. CI follows the Python
3.13 patch series; this is not a bit-for-bit OS/interpreter build claim.

| File | Purpose |
| --- | --- |
| `requirements/runtime.in` | Reviewed exact direct pins, preserving the preceding phase's direct dependencies. |
| `requirements/browser.in` | Runtime input plus the Playwright direct pin. |
| `requirements.txt` | Complete generated runtime graph, markers and SHA-256 archive hashes. |
| `requirements-browser.txt` | Complete generated browser graph, with every shared runtime version identical. |
| `requirements/security.in` / `requirements-security.txt` | Isolated, hash-locked advisory tool graph; never mixed into the application environment. |
| `requirements-dev.txt` | Compatibility alias for the locked runtime test environment. |
| `requirements/locking.txt` | Separately hash-pinned maintenance resolver/parser; never an application dependency. |
| `requirements/manifest.json` | Resolver/index/cutoff, input identities and normalized output hashes. |

Manifest schema 2 binds all three profiles. Checkout the matching source and
manifest together; no implicit source/lock migration or runtime-journal migration
is performed. The [advisory gate](dependency-advisories.md) records known-advisory
queries and the compatible security update that followed its first finding.

The initial resolution uses versions from the preceding verified Windows
environment. The universal resolver includes platform markers; the audit still
rejects unverified OS/architecture/interpreter targets. A universal output is not
evidence that every platform was tested. Browser-only packages are deliberate
profile differences; an environment containing them cannot pass a runtime-only
audit. Bootstrap pip is the sole additional permitted distribution, with its
actual version reported. An unexpected setuptools, wheel, uv or local project
installation also fails this exact-set gate.

Every generated requirements file enables `--require-hashes`, `--only-binary
:all:` and the fixed PyPI index. Normal installation never resolves new versions
or runs a source build. There is no unhashed fallback, automatic lock refresh,
automatic downgrade or environment repair. pip documents the combination in
[Secure installs](https://pip.pypa.io/en/stable/topics/secure-installs/).
Hashes bind allowed archive bytes; they do not prove that a publisher or package
is trustworthy or free of vulnerabilities. Accepting the first hashes from PyPI
is part of the reviewed dependency update, not independent publisher attestation.

## Clean Windows installation

Use a fresh environment from a reviewed safety branch. Keep worker/provider
credentials outside the checkout; none are needed for this validation.

```powershell
py -3.13 -m venv .venv
New-Item -ItemType Directory -Path results -Force | Out-Null
.\.venv\Scripts\python.exe -m pip --isolated --disable-pip-version-check install `
  --require-hashes --only-binary=:all: --report results/install-runtime.json `
  -r requirements.txt
if ($LASTEXITCODE -ne 0) { throw 'Locked installation failed' }
.\.venv\Scripts\python.exe -m pip --isolated check
if ($LASTEXITCODE -ne 0) { throw 'Dependency compatibility check failed' }
.\.venv\Scripts\python.exe -m scripts.dependency_audit --profile runtime `
  --output results/environment-runtime.json
if ($LASTEXITCODE -ne 0) { throw 'Environment audit failed; preserve its report' }
.\.venv\Scripts\python.exe -m pytest -q -p no:cacheprovider
.\.venv\Scripts\python.exe -m eval.voice_replay
.\.venv\Scripts\python.exe -m eval.provider_faults
```

For browser development, install `requirements-browser.txt` in a fresh browser
environment, or deliberately extend the matching runtime environment. Every
shared version must remain identical. Then audit the **browser** profile:

```powershell
.\.venv\Scripts\python.exe -m pip --isolated --disable-pip-version-check install `
  --require-hashes --only-binary=:all: --report results/install-browser.json `
  -r requirements-browser.txt
if ($LASTEXITCODE -ne 0) { throw 'Locked browser installation failed' }
.\.venv\Scripts\python.exe -m scripts.dependency_audit --profile browser `
  --output results/environment-browser.json
if ($LASTEXITCODE -ne 0) { throw 'Browser environment audit failed' }
.\.venv\Scripts\python.exe -m playwright install chromium
.\.venv\Scripts\python.exe -m pytest browser/console_cases.py -q -p no:cacheprovider
```

On Linux use `python3 -m venv .venv` and `.venv/bin/python` with the same flags.
CI creates a new venv for each runtime/browser job rather than trusting the
runner's preinstalled packages. Public package-index and browser-download access
are needed for a new installation; the provider/voice evaluations remain offline
with respect to their actual services. A missing wheel is an unmet gate, not
permission to compile or use a different version silently.

## Environment evidence and failures

`python -m scripts.dependency_audit --profile runtime` is read-only. It validates
the manifest, exact direct pins, strict hash-lock syntax, marker selection and
runtime/browser consistency before comparing installed distribution metadata.
It rejects missing/extra/mismatched packages, duplicate package records and any
direct URL/editable/local installation metadata. pip compatibility checking must
also pass. Unsupported targets or unavailable/malformed facts cannot pass.

Exit 0 means that metadata check passed. Exit 1 preserves a failed report; exit 2
means invalid CLI arguments or inability to preserve the report. Nothing is
automatically installed, uninstalled or repaired. Reports contain only normalized
distribution names/versions, lock/manifest identities and verdicts. Direct URLs,
credential-bearing metadata and raw exception payloads are not serialized.

The auditor does not hash installed module bytes. A tampered installed file can
retain unchanged metadata. The pip install report records downloaded archive
identities, while pip's hash check enforces the requirements during installation;
an already-installed matching package is not rehashed by pip. This is why the
acceptance path requires a fresh venv. A hostile host or modified dependency
metadata is outside this attestation boundary.

CI retains installation and audit reports for seven days as
`dependency-evidence-runtime-<OS>` and `dependency-evidence-browser-<OS>`, alongside
the simulated scorecards and console artifacts. Archive evidence for a release
before CI retention expires. Raw CRLF/LF differences are normalized for lock
identity, so Windows Git line-ending conversion cannot manufacture drift.

The legacy `eval/reproduce_eval.sh` FDB wrapper is not this validated acceptance
path: its external benchmark assets and launch/scoring integration remain
unverified. Use the documented runtime/browser suites and explicitly SIMULATED
Python scorecards; do not report absent benchmark or real-audio results as passes.

## Reviewed lock updates

Do not edit generated requirements or use `pip freeze` as the resolver. Edit the
direct `.in` inputs and review the full dependency/marker/hash diff. The pinned
uv resolver supports [compiled, hashed requirements](https://docs.astral.sh/uv/pip/compile/)
and [universal resolution](https://docs.astral.sh/uv/concepts/resolution/).

```powershell
py -3.13 -m venv .venv-lock-tools
.\.venv-lock-tools\Scripts\python.exe -m pip --isolated install `
  --require-hashes --only-binary=:all: -r requirements/locking.txt
if ($LASTEXITCODE -ne 0) { throw 'Maintenance tool installation failed' }
.\.venv-lock-tools\Scripts\python.exe -m scripts.lock_dependencies
```

Normal regeneration preserves the recorded cutoff and uses committed pins as
resolver preferences. It strips ambient uv/pip configuration and uses the fixed
PyPI index. All candidates are validated before publishing any. The explicit
`--upgrade` option advances the cutoff and deliberately re-resolves the graph:

```powershell
.\.venv-lock-tools\Scripts\python.exe -m scripts.lock_dependencies --upgrade
git diff -- requirements.txt requirements-browser.txt requirements
```

Run the adversarial tests, fresh locked installs, environment audits, complete
runtime suite, browser suite and both simulated evaluations before committing.
Updates require the same Windows/Linux CI gates. New archive hashes or marker
branches require review even if package version strings do not change. The
cutoff and available index metadata constrain reproduction; hashes do not keep
deleted package archives available forever. An archived, verified wheelhouse or
artifact mirror is a separate release/disaster-recovery gate.

Do not run concurrent generators or update dependencies inside a live worker.
Publication replaces each file through a flushed same-directory temporary file,
then publishes the manifest last. This is not a multi-file atomic transaction.
A failure during publication leaves an audit-detectable mismatch and cannot
count as success. Preserve the diff, regenerate/review the complete bundle and
rerun validation before installing it. A failed candidate resolution preserves
all published files. Process termination and real disk faults are not certified
by the injected-failure tests.

## Rollback and remaining release gates

Stop/drain workers before changing their environment. Restore the preceding
reviewed dependency bundle and compatible source in a fresh venv only after
advisory review; never restore a known-vulnerable version merely to recover green
tests. Follow the [advisory response runbook](dependency-advisories.md) for the
telemetry remediation. Preserve install/audit evidence. Removing this installer gate does not resolve unknown
remote effects. Keep journal schema 6, claims, approvals and receipts unchanged;
never restore an older action database or replay a mutation as dependency rollback.
Do not return to unsafe public main.

Release work remains for ongoing advisory review, publisher attestations,
archived wheels, pinned deployment images/native dependencies, backup/restore
drills, authenticated remote identity/TLS, real provider acceptance and real
audio evaluation. No dependency-safety, full-host reproducibility, financial
correctness, performance or universal novelty claim is made by this phase.
