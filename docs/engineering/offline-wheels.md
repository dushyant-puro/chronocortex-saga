# Verified offline wheel archives

This release tool captures and verifies complete, host-compatible wheel sets
against a matching repository lock bundle. It makes no provider call, downloads
nothing, extracts nothing and installs nothing. **LOCAL BRANCH VERIFIED** evidence
belongs in its phase PR. Negative tests use synthetic wheels; CI additionally
uses real locked archives. Provider effects elsewhere remain **SIMULATED**.

Hash locks alone cannot keep an index or a deleted download available. Earlier
local clean installs encountered index-resolution failures despite successful CI
installs. Preserving reviewed wheel bytes independently closes that availability
gap for the selected Python dependency graph. Interpreter/bootstrap pip, native
OS libraries, browsers and deployment images remain separate release assets.

## Capture, verify and install on Windows

Use the [locked runtime setup](locked-environments.md). Choose a new empty source
directory and a new archive path; do not modify the source while capture runs.
This example prepares the runtime profile for the current Windows x64 CPython
3.13 host. Use separate archives for the browser/security profiles and Linux.

```powershell
$ccsSource = Join-Path $env:LOCALAPPDATA 'CCS-WheelDownloads-runtime-001'
$ccsArchive = Join-Path $env:LOCALAPPDATA 'CCS-Wheels-runtime-win64-001'
if ((Test-Path -LiteralPath $ccsSource) -or (Test-Path -LiteralPath $ccsArchive)) {
    throw 'Choose new paths; preserve existing evidence.'
}
New-Item -ItemType Directory -Path $ccsSource -ErrorAction Stop | Out-Null
.\.venv\Scripts\python.exe -m pip --isolated --disable-pip-version-check download `
  --no-deps --require-hashes --only-binary=:all: --dest $ccsSource -r requirements.txt
if ($LASTEXITCODE -ne 0) { throw 'Downloads incomplete; do not label them an archive.' }
.\.venv\Scripts\python.exe -m scripts.wheelhouse capture --profile runtime `
  --source $ccsSource --output $ccsArchive
if ($LASTEXITCODE -ne 0) { throw 'Archive capture unverified; preserve partial output.' }
.\.venv\Scripts\python.exe -m scripts.wheelhouse verify --profile runtime --directory $ccsArchive
if ($LASTEXITCODE -ne 0) { throw 'Archive unverified; do not install.' }
```

`--no-deps` applies only to the preparatory download: the committed lock already
enumerates the complete graph. Capture requires exactly one compatible locked
wheel per active package. It rejects extra/duplicate/incompatible wheels and
sdist-only or missing packages. Non-wheel source catalogs/files are ignored and
never copied. A source directory containing multiple platform alternatives must
be reduced explicitly with pip's host selection first; the tool does not guess.

For an offline install into a **new** environment from that same matching source
checkout, use a trusted preinstalled Python and bootstrap pip:

```powershell
if (Test-Path -LiteralPath '.venv-offline') { throw 'Choose a fresh environment path.' }
py -3.13 -m venv .venv-offline
if ($LASTEXITCODE -ne 0) { throw 'Environment creation failed.' }
New-Item -ItemType Directory -Path results -Force | Out-Null
.\.venv-offline\Scripts\python.exe -m pip --isolated --disable-pip-version-check install `
  --no-index --find-links $ccsArchive --require-hashes --only-binary=:all: `
  --report results/install-offline.json -r requirements.txt
if ($LASTEXITCODE -ne 0) { throw 'Offline installation failed; retain the archive and report.' }
.\.venv-offline\Scripts\python.exe -m scripts.dependency_audit `
  --profile runtime --output results/environment-offline.json
if ($LASTEXITCODE -ne 0) { throw 'Installed graph unverified.' }
.\.venv-offline\Scripts\python.exe -m pytest -q -p no:cacheprovider
```

The final install rechecks the lock hashes with indexes disabled; archive
verification is not a replacement for that check or the installed-metadata audit.
Bootstrap tools and the source checkout must already exist. The offline archive
is not a standalone application installer. For source-provenance scorecards,
create the environment outside the checkout or use an ignored `.venv` path so
the experimental source remains clean.

## Integrity and failure boundaries

The existing strict lock parser now exposes each requirement's own archive
hashes while preserving the previous requirement-only API. Marker selection and
all lock/input manifest checks still apply. Hashes from another package cannot
authorize a wheel even when both packages share the same version number.

Each wheel is bounded and hashed before ZIP/header parsing. Package name/version,
single metadata directory, supported wheel format, declared tags, host-compatible
filename tags and `Requires-Python` must agree. Duplicate ZIP members, traversal
paths, symlink entries, encrypted entries and oversized metadata are rejected.
No ZIP member is extracted. These checks use the
[PyPA wheel format](https://packaging.python.org/en/latest/specifications/binary-distribution-format/)
and [packaging's filename parser](https://packaging.pypa.io/en/stable/utils.html#packaging.utils.parse_wheel_filename).

Limits are 256 packages, 256 MiB per wheel, 1 GiB total archive bytes, 1 MiB per
metadata member and 256 KiB for the manifest. Reviewed hashes narrow which ZIP
bytes reach parsing; these limits are not certification of arbitrary adversarial
ZIP memory usage. Ordinary single-link regular files are required. Keep source
and retained archives under operator-controlled, immutable custody during use;
path/file-shape checks are not protection against a compromised host or an owner
changing files during verification/install.

Capture exclusively creates a new directory and files, flushes copied bytes,
and independently verifies the copies before writing `wheelhouse.json` last.
The manifest binds the profile, supported host target, full dependency-manifest
hash, selected lock hash, exact filenames, package versions, byte lengths and
SHA-256 values. Final readback must pass before capture reports success. POSIX
also flushes the output/parent directory; Windows storage durability remains a
host validation concern. A readback pass cannot certify a prior flush or power
loss behavior.

The verifier is read-only. It rejects an incomplete inventory, any extra archive
file, malformed/duplicate/nonfinite JSON, changed metadata, mismatched profile,
target or lock bundle. Existing destinations are never overwritten. A failure
can retain a partial directory; even a complete readable directory may remain
unverified after a flush failure. Preserve it for investigation and choose a
new path rather than deleting or relabeling it based on an exit status.

The repository lock bundle is a trusted input, not a signature. Matching local
hashes does not prove that the commit was reviewed, identify a publisher, certify
package safety or reproduce a whole host. Preserve a trusted source revision and
review/advisory evidence separately. A changed lock bundle requires recapture or
verification with its matching reviewed source; do not edit archive metadata to
silence a mismatch.

## Retention, CI and rollback

Keep at least the chosen recovery copies outside the checkout and pip cache,
with explicit ownership, access, integrity checks and off-host retention. CI
builds/verifies actual wheel sets for its runtime/browser targets and Linux
security profile. It retains only the small inventories with dependency evidence;
the wheel bytes are not automatically uploaded or made into a public release.
Creating an archive does not prove durable redundant storage or a recovery drill.

Exit 0 means the named checks completed; exit 2 means invalid use or an unverified
operation. Reports contain aggregate counts and lock identity, not source paths,
URLs, credentials or raw exceptions. `installed` and `publisher_authenticated`
remain false. Verification does not grant runtime task or mutation authority.

Rollback the tooling independently of journals. Keep old archives with their
matching reviewed lock/source and advisory evidence. Never use an archive as
permission to restore an older action database, revive consumed approvals, weaken
dependency hashes or retry an UNKNOWN operation.
