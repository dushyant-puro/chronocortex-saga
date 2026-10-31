# Journal backup bundles and restore drills

For new operational captures use the [authenticated encrypted backup path](encrypted-backups.md).
The plaintext format below remains an explicit compatibility/forensic tool. An
encrypted bundle is never automatically downgraded to this format.

Status: **LOCAL BRANCH VERIFIED** implementation. Tests use real local SQLite
files and **SIMULATED** external effects. Published-commit clean-checkout and
Windows/Linux CI evidence belongs in the phase PR. No REAL PROVIDER or live-audio
acceptance is performed by these commands.

## Safety contract

`scripts.journal_backup` captures one schema-6 or schema-7 journal snapshot through SQLite's
backup API, checks it with [recovery preflight](recovery-preflight.md), and publishes
a single new bundle. It never replaces an existing destination or the source's
database/sidecar paths. The drill verifies the bundle and restores its snapshot
to an in-memory SQLite database. There is no live-journal destination or activation
command, and no runtime host, worker or provider adapter is constructed.

The snapshot preserves operation keys, uncertain states, event history, receipts,
worker generations, security clocks, credentials, approvals, revoked sessions
and control history as stored. Capture never clears uncertainty, expires leases,
unconsumes approvals or resets security authority. A successful drill does not
make that captured authority current or suitable for use.

## Protected Windows storage

Bundles contain unencrypted sensitive journal data, including request/result
payloads and credential/session verifiers. Use a protected local NTFS directory
outside Git, synced folders and shared directories. The Python tool rejects Git
ancestors and symlink/reparse ancestors. On POSIX it also enforces owner-only
parent permissions. **Windows ACLs must be established and reviewed by the host
administrator; the Python tool does not certify them.**

Create a new directory before writing any backup data. This example grants only
the current account and SYSTEM access. Apply your organization's backup/storage
policy; the tool does not provide encryption or retention management.

```powershell
$ccsBackupRoot = Join-Path $env:LOCALAPPDATA 'CCS-PrivateBackups'
if (Test-Path -LiteralPath $ccsBackupRoot) { throw 'Existing directory: inspect its ACL before reuse.' }
New-Item -ItemType Directory -Path $ccsBackupRoot | Out-Null
$ccsUser = [System.Security.Principal.WindowsIdentity]::GetCurrent().User
$ccsSystem = [System.Security.Principal.SecurityIdentifier]::new('S-1-5-18')
$ccsAcl = [System.Security.AccessControl.DirectorySecurity]::new()
$ccsAcl.SetOwner($ccsUser)
$ccsAcl.SetAccessRuleProtection($true, $false)
foreach ($ccsIdentity in @($ccsUser, $ccsSystem)) {
    $ccsRule = [System.Security.AccessControl.FileSystemAccessRule]::new(
        $ccsIdentity, 'FullControl', 'ContainerInherit,ObjectInherit', 'None', 'Allow')
    $ccsAcl.AddAccessRule($ccsRule)
}
Set-Acl -LiteralPath $ccsBackupRoot -AclObject $ccsAcl
Get-Acl -LiteralPath $ccsBackupRoot | Format-List Owner,AccessToString
```

Use a new bundle name for each capture. A failed command must be investigated;
never delete evidence or repeatedly overwrite a destination to obtain success.

```powershell
$ccsBundle = Join-Path $ccsBackupRoot 'journal-001.ccsb'
.\.venv\Scripts\python.exe -m scripts.journal_backup capture `
  --journal 'C:\CCS\state\journal.db' --bundle $ccsBundle
if ($LASTEXITCODE -ne 0) { throw 'Backup unverified; inspect protected storage and preserve evidence.' }
.\.venv\Scripts\python.exe -m scripts.journal_backup drill `
  --bundle $ccsBundle --workspace $ccsBackupRoot
if ($LASTEXITCODE -ne 0) { throw 'Restore drill unverified; retain bundle for investigation.' }
```

Exit 0 means only that the named operation completed its checks. Exit 2 means
unverified evidence, failure or invalid CLI input. JSON output contains aggregate
counts, a snapshot hash and fixed verdicts; it omits paths, raw exceptions and
stored payloads. Never redirect output over a source or bundle.

## Snapshot and publication behavior

A pinned read transaction keeps all copied tables at one committed SQLite view.
Concurrent writer commits wait while that transaction is held. Prefer draining
workers before operational capture to avoid heartbeat delay or lease loss; a
backup cannot fence a remote request already in flight. The concurrency test
uses two real SQLite connections to verify that a writer's later commit cannot
mix with the captured generation.

Only rollback-format, schema-6 or schema-7 CCS journals are accepted. Bundles
record the actual schema and drills reject manifest/snapshot disagreement.
Neither schema's evidence pass authorizes runtime adoption or activation.
See [schema-7 migration and rollback](reviewed-task-admission.md#schema-7-migration-and-rollback).
WAL is rejected before
opening SQLite. The snapshot is limited to 64 MiB and preflight's existing row
and projection bounds. Backup progress has a ten-second cooperative budget and
250 ms lock timeout. Logical-dump verification is separately bounded to ten
seconds and 256 MiB. These are limits, not measured performance or hard OS I/O
deadlines. Larger journals require a separately qualified path.

The bundle has a fixed magic/version, bounded JSON manifest, declared length,
SHA-256 and SQLite bytes. Truncation, trailing bytes, duplicate keys, unsupported
versions and hash mismatches fail before SQLite reads the extracted snapshot.
The drill checks local evidence, copies into memory, and compares streaming
logical digests of schema, rows and sequences. Temporary extraction occurs only
under the supplied private workspace and is removed on normal completion.

Publication writes and flushes a temporary file, then creates the destination
with an atomic no-overwrite hard link. Unsupported filesystems fail without a
replace/copy fallback. POSIX parent-directory fsync is attempted; Windows does
not have an equivalent certified power-loss guarantee here. Failure after
publication can leave a complete destination with a failed report. Preserve it
and run a separate drill; never reinterpret the failed capture as success.
Abrupt process/host loss can leave private temporary files requiring inspection.
Cleanup is not secure erasure, and disk/controller failure has not been certified.

## Acceptance limits and recovery decision

Every result leaves restore authority, dispatch authority, freshness, provider
reconciliation and authenticity **false**, including successful capture/drills.
SHA-256 detects accidental change relative to the manifest; an attacker can
replace both. A successful stale-bundle drill is explicitly tested. No external
catalog or newer journal is consulted, and timestamps are informational.

Do not extract a bundle as a live journal. Subsequent recovery work must establish
freshness against independently protected evidence, fence old workers, reconcile
remote effects, rotate/review security authority and authorize activation. Do not
infer safe mutation retry from cancellation, an expired lease or a backup hash.
Encryption, independent/offsite retention, signed provenance, storage-loss drills
and a reviewed activation protocol remain release gates.

Rollback of this tool preserves the live journal and all published evidence.
The phase changes no schema or dispatch semantics. Restore the preceding reviewed
application code if needed; never restore older journal history as code rollback.

Database behavior follows the [SQLite backup API](https://www.sqlite.org/backup.html)
and [Python sqlite3 backup interface](https://docs.python.org/3.13/library/sqlite3.html#sqlite3.Connection.backup).
These sources describe the API, not certification of this implementation.
