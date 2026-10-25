# Authenticated encrypted journal backups

Status: **LOCAL BRANCH VERIFIED** implementation using real local cryptography
and SQLite with **SIMULATED** journal effects. Clean-checkout and Windows/Linux
acceptance evidence is recorded in the phase PR. No REAL PROVIDER or real audio
acceptance is implied.

New operational backups should use `scripts.encrypted_backup`. It wraps the
[verified snapshot and memory-drill pipeline](journal-backup-drills.md) with
PyCA Fernet from hash-locked `cryptography==50.0.2`. The old plaintext commands
remain explicit compatibility tools for existing bundles and isolated forensic
work; the encrypted reader never silently falls back to them.

## Protected keys and storage

Use separate, administrator-protected local directories for keys and bundles,
outside Git/synced/shared folders. The encrypted tool refuses a key inside the
bundle or drill-workspace directory tree. Key files must be single-link regular
files containing exactly one 44-byte Fernet key. Passwords, whitespace, raw
command-line key material, environment variables and key discovery are unsupported.
POSIX ownership/modes are enforced; Windows inherited ACLs remain the host
administrator's responsibility.

Create two new protected directories in PowerShell before generating a key or
capturing evidence. Existing paths require ACL inspection, not automatic changes.

```powershell
$ccsKeys = Join-Path $env:LOCALAPPDATA 'CCS-BackupKeys'
$ccsEncrypted = Join-Path $env:LOCALAPPDATA 'CCS-EncryptedBackups'
$ccsUser = [System.Security.Principal.WindowsIdentity]::GetCurrent().User
$ccsSystem = [System.Security.Principal.SecurityIdentifier]::new('S-1-5-18')
foreach ($ccsDirectory in @($ccsKeys, $ccsEncrypted)) {
    if (Test-Path -LiteralPath $ccsDirectory) { throw 'Existing directory: inspect its ACL before reuse.' }
    New-Item -ItemType Directory -Path $ccsDirectory -ErrorAction Stop | Out-Null
    $ccsAcl = [System.Security.AccessControl.DirectorySecurity]::new()
    $ccsAcl.SetOwner($ccsUser)
    $ccsAcl.SetAccessRuleProtection($true, $false)
    foreach ($ccsIdentity in @($ccsUser, $ccsSystem)) {
        $ccsRule = [System.Security.AccessControl.FileSystemAccessRule]::new(
            $ccsIdentity, 'FullControl', 'ContainerInherit,ObjectInherit', 'None', 'Allow')
        $ccsAcl.AddAccessRule($ccsRule)
    }
    Set-Acl -LiteralPath $ccsDirectory -AclObject $ccsAcl -ErrorAction Stop
    Get-Acl -LiteralPath $ccsDirectory | Format-List Owner,AccessToString
}
```

Generate once, then retain the key securely and independently from backup media.
Generation uses Fernet's random-key API and exclusive creation; it never prints
the key or replaces an existing file. Failure can leave an unverified key file;
preserve/review it rather than overwriting it or assuming generation succeeded.

```powershell
$ccsKey = Join-Path $ccsKeys 'key-01.fernet'
$ccsBundle = Join-Path $ccsEncrypted 'journal-key01-001.ccse'
.\.venv\Scripts\python.exe -m scripts.encrypted_backup create-key --key-file $ccsKey
if ($LASTEXITCODE -ne 0) { throw 'Key creation unverified.' }
.\.venv\Scripts\python.exe -m scripts.encrypted_backup capture `
  --journal 'C:\CCS\state\journal.db' --bundle $ccsBundle --key-file $ccsKey
if ($LASTEXITCODE -ne 0) { throw 'Encrypted capture unverified; retain existing evidence.' }
.\.venv\Scripts\python.exe -m scripts.encrypted_backup drill `
  --bundle $ccsBundle --workspace $ccsEncrypted --key-file $ccsKey
if ($LASTEXITCODE -ne 0) { throw 'Encrypted restore drill unverified.' }
```

The filenames identify a key generation without embedding key bytes. Keep that
mapping in protected operational records. Losing the key makes retained bundles
unreadable. Generating another key does not rotate existing bundles; keep old
keys until every dependent bundle has been inventoried and migrated. The
[offline rotation command](backup-key-rotation.md) validates and migrates one
bundle to a separately named key/destination while preserving its original age
and evidence. Key retirement, KMS/HSM custody and bulk rotation remain separate
operational gates.

## Authentication and extraction boundary

The complete inner bundle, including its manifest/hash and journal bytes, is
encrypted together with a versioned protocol-domain prefix. The outer envelope
accepts only its fixed magic and canonical bounded Fernet token. Wrong keys,
modified/truncated ciphertext, trailing junk, unsupported versions and wrong
protocol domains fail before any plaintext file or SQLite connection is created.
There is no plaintext output or decryption-to-live-path option.

After authentication, plaintext exists only in process memory and transient
files under the explicitly supplied private workspace. The existing drill then
validates evidence, restores into memory and compares logical contents. Creation,
encryption, publication or cleanup failure cannot yield a passing report. Capture
uses the same no-overwrite publication contract as plaintext bundles. No fallback
publishes plaintext when encryption fails.

`ciphertext_authenticated: true` on a successful drill means the supplied key
authenticated the ciphertext. `authenticity_verified` remains false: any holder
of that symmetric key can create a valid bundle, so this is not a signature or
proof of a particular operator's identity. Freshness, restore authority,
dispatch authority and provider reconciliation also remain false. Token age is
not used as a recovery authorization decision; old evidence must remain readable.

PyCA's [Fernet documentation](https://cryptography.io/en/50.0.2/fernet/) describes
the reviewed high-level construction: AES-128-CBC encryption, HMAC-SHA256
authentication and library-generated random IVs. This implementation does not
invent a cipher, password KDF or nonce protocol. Fernet reveals its token creation
timestamp; file size and filesystem metadata remain visible. It processes the
whole message in memory. The existing 64 MiB snapshot limit remains, and the
encrypted envelope is bounded below 129 MiB. Temporary plaintext, swap, crash
dumps and a compromised host/key holder remain outside ciphertext-at-rest
protection. Cleanup is not secure erasure. No FIPS or independent cryptographic
audit certification is claimed.

## Validation, failures and rollback

Run the full runtime suite plus focused encryption/backup/preflight tests from a
fresh locked environment. The advisory gate covers the new package in runtime
and browser profiles. Native OpenSSL provenance and host memory/storage behavior
remain separate release concerns.

Exit 0 means the named operation's checks completed. Exit 2 means failure or
invalid CLI use. Reports omit key material, raw exceptions, paths and journal
payloads. Do not place keys in chat, source control or shell arguments. No real
production key is needed for the synthetic acceptance tests.

On failure preserve the encrypted bundle and protect any residual private
temporary data. Do not weaken authentication, swap in an older database, reset
approvals or retry a remote mutation. On application rollback retain the bundle,
key generation and matching compatible reader: the earlier plaintext reader
cannot read encrypted envelopes. Rollback never means exporting plaintext into
ordinary backup storage. Live restoration still requires independent freshness,
worker fencing, current provider reconciliation and reviewed security authority.
