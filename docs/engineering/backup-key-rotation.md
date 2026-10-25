# Offline backup-key rotation

Status: **LOCAL BRANCH VERIFIED** implementation with real local cryptography,
SQLite and process-death tests over **SIMULATED** journal effects. Published-head
clean-checkout and Windows/Linux CI results belong in the phase PR. No REAL
PROVIDER or real audio acceptance is implied.

`scripts.encrypted_backup rotate` migrates one existing encrypted bundle to an
explicitly named new key and a new destination. It retains the original bundle
and both keys. It neither captures a newer journal nor activates a restored
database. This is an offline operation with no provider or voice connection.

## Preconditions and Windows commands

Use the [encrypted-backup setup](encrypted-backups.md#protected-keys-and-storage)
to provision administrator-protected directories outside Git/synced/shared
folders. Both keys must be single-link private regular files outside **all**
source-storage, destination-storage and plaintext-workspace directory trees.
POSIX ownership/modes are checked; Windows ACL protection remains the host
administrator's responsibility. Do not rotate while another program may replace
these paths. The command does not discover keys, accept key bytes in arguments,
load an environment key or generate a replacement key implicitly.

After provisioning those directories, use PowerShell from a compatible checkout:

```powershell
$ccsKeys = Join-Path $env:LOCALAPPDATA 'CCS-BackupKeys'
$ccsEncrypted = Join-Path $env:LOCALAPPDATA 'CCS-EncryptedBackups'
$ccsOldKey = Join-Path $ccsKeys 'key-01.fernet'
$ccsNewKey = Join-Path $ccsKeys 'key-02.fernet'
$ccsSource = Join-Path $ccsEncrypted 'journal-key01-001.ccse'
$ccsRotated = Join-Path $ccsEncrypted 'journal-key02-001.ccse'

# Generate the new generation once. An existing key is never replaced.
.\.venv\Scripts\python.exe -m scripts.encrypted_backup create-key --key-file $ccsNewKey
if ($LASTEXITCODE -ne 0) { throw 'New key creation unverified; inspect without overwriting.' }

.\.venv\Scripts\python.exe -m scripts.encrypted_backup rotate `
  --bundle $ccsSource --destination $ccsRotated --workspace $ccsEncrypted `
  --old-key-file $ccsOldKey --new-key-file $ccsNewKey
if ($LASTEXITCODE -ne 0) { throw 'Rotation unverified; preserve originals and inspect any destination.' }

.\.venv\Scripts\python.exe -m scripts.encrypted_backup drill `
  --bundle $ccsRotated --workspace $ccsEncrypted --key-file $ccsNewKey
if ($LASTEXITCODE -ne 0) { throw 'New-key drill unverified; do not retire any key or original bundle.' }

.\.venv\Scripts\python.exe -m scripts.encrypted_backup drill `
  --bundle $ccsSource --workspace $ccsEncrypted --key-file $ccsOldKey
if ($LASTEXITCODE -ne 0) { throw 'Original evidence unverified; retain all generations for investigation.' }
```

For later bundles, reuse the separately retained new key; do not rerun its
generation command. Always choose a new destination. Repeating rotation against
an existing destination fails, even if its contents appear identical. There is
no overwrite, in-place update, bulk sweep or automatic resume option.

## Protected invariants

1. The explicitly supplied old key authenticates the strict bounded envelope and
   protocol domain before any plaintext file or SQLite connection is created.
   A source readable only by the new key is rejected. Equal decoded old/new key
   bytes are rejected, including base64 aliases of the same key.
2. The authenticated inner manifest and complete SQLite snapshot pass the same
   preflight, integrity, claim-retention and in-memory restore checks as an
   ordinary encrypted drill. A key holder's valid encryption of invalid journal
   evidence cannot become a passing rotation.
3. PyCA [MultiFernet.rotate](https://cryptography.io/en/50.0.2/fernet/#cryptography.fernet.MultiFernet.rotate)
   re-encrypts under the new key while preserving the token's original timestamp.
   The command independently checks new-key decryption against the exact original
   payload and checks timestamp equality. The inner manifest, captured time,
   journal bytes, claims, receipts and authority clocks remain unchanged.
4. Only encrypted bytes enter the publication temporary file. After flush/fsync,
   strict file readback must match the verified token before atomic no-overwrite
   hard-link publication. An unsupported filesystem, competing destination,
   write/flush failure, backend failure or cleanup failure returns an unverified
   verdict. There is no copy/replace/plaintext fallback.
5. Rotation changes key custody, not recovery authority. Every result leaves
   `freshness_verified`, `restore_authorized`, `dispatch_authorized`,
   `provider_reconciled` and `authenticity_verified` false. Valid old evidence
   remains old; UNKNOWN effects retain their claims and require authoritative
   reconciliation. No mutation is dispatched or retried.

`payload_preserved` and `token_timestamp_preserved` describe a successful
rotation's checks. They do not prove who supplied the symmetric keys or that the
source is the newest journal. No token TTL grants recovery authority. The
64 MiB snapshot limit and bounded whole-message memory costs remain unchanged.
Plaintext still exists transiently in process memory and the explicitly protected
workspace; cleanup does not promise secure erasure, crash-dump or swap protection.

## Inventory, interruption and key retirement

Keep a protected inventory mapping every original and migrated bundle to its key
generation, compatible reader and completed drill report. Review off-host copies,
archives, retention requirements and recovery custodians separately. Passing one
rotation says nothing about unlisted copies or whether a custodian has retained
the new key. The tool does not authorize key retirement, delete old keys/bundles,
revoke access to old ciphertext or provide KMS/HSM custody. A compromised old key
can still decrypt previously copied old bundles; rotation cannot undo exposure.

A crash before publication can leave a private `.ccs-rotation-*` directory with
encrypted partial evidence. A crash after publication can leave both a complete
destination and that temporary file. No success receipt may exist in either
case. A crash during the preceding drill can also retain private plaintext in
the workspace. Protect residual files and inspect each known artifact with the
matching key and drill command. Never sweep directories, replace a destination,
or delete the last original/key based on a failed exit status. The runtime test
suite kills a real child process immediately before and after publication and
checks that the original and published evidence remain recoverable.

Exit 0 means this command completed its checks; exit 2 means invalid use or an
unverified result. Reports omit raw exceptions, key material, paths and journal
payloads. Host/storage power-loss durability beyond the documented filesystem
contract remains an operational validation gate.

## Validation and rollback

From a [fresh locked environment](locked-environments.md), run:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_backup_key_rotation.py tests/test_encrypted_backup.py tests/test_journal_backup.py tests/test_recovery_preflight.py -q -p no:cacheprovider
.\.venv\Scripts\python.exe -m pytest -q -p no:cacheprovider
```

Regression/adversarial coverage includes wrong/unchanged/colocated keys, corrupt
and cross-protocol input, authenticated invalid evidence, exact payload/age
preservation, stale snapshots, UNKNOWN receipt retention, publication races,
write/readback/flush/link/cleanup/backend failures, CLI use and real process
death. This phase changes no dependency lock, bundle format or journal schema.

Rollback the application code independently of data. Retain the original and
migrated bundles, their respective keys and a compatible encrypted reader. Both
generations use the existing encrypted format. Returning to the previous reader
does not require re-encryption or a journal restore. Do not replace live state,
re-enable older approvals or retry unknown mutations as rollback.
