# Windows local-development input only. No Stripe requests or environment keys.
[CmdletBinding()]
param(
    [string]$Directory = (Join-Path $env:LOCALAPPDATA 'ChronoCortexSaga-StripeSandbox'),
    [switch]$Diagnostics
)

function Write-CcsSetupStage {
    param([ValidateSet('VALIDATE_PATH', 'CREATE_DIRECTORY', 'APPLY_ACL', 'VERIFY_ACL',
                      'READ_SECRET', 'VALIDATE_SECRET', 'CREATE_FILE', 'WRITE_FILE',
                      'FLUSH_FILE', 'CLEANUP', 'COMPLETE')][string]$Stage)
    # Diagnostic I/O must not skip secret cleanup or change credential authority.
    if ($Diagnostics) {
        try { [Console]::Error.WriteLine("CCS_KEY_SETUP_STAGE:$Stage") } catch {}
    }
}

$ErrorActionPreference = 'Stop'
$ccsFailure = 'UNSAFE_DIRECTORY'
$ccsSecret = $null
$ccsPlain = $null
$ccsPointer = [IntPtr]::Zero
$ccsBytes = $null
$ccsFile = $null

try {
    Write-CcsSetupStage 'VALIDATE_PATH'
    if ($Directory -notmatch '\A[A-Za-z]:[\\/]') { throw 'Fully qualified local directory required' }
    $ccsDirectory = [System.IO.Path]::GetFullPath($Directory)
    $ccsDriveRoot = [System.IO.Path]::GetPathRoot($ccsDirectory)
    if ($ccsDriveRoot -notmatch '^[A-Za-z]:\\$' -or
        [System.IO.DriveInfo]::new($ccsDriveRoot).DriveType -ne 'Fixed') {
        throw 'Local fixed disk required'
    }
    $ccsParent = Get-Item -Force -LiteralPath (Split-Path -Parent $ccsDirectory)
    if (-not $ccsParent.PSIsContainer) { throw 'Existing parent directory required' }
    for ($ccsAncestor = $ccsParent; $null -ne $ccsAncestor; $ccsAncestor = $ccsAncestor.Parent) {
        if ($ccsAncestor.Attributes -band [System.IO.FileAttributes]::ReparsePoint) {
            throw 'Reparse-point ancestor refused'
        }
        if (Test-Path -LiteralPath (Join-Path $ccsAncestor.FullName '.git')) {
            throw 'Git checkout ancestor refused'
        }
    }
    if (Test-Path -LiteralPath $ccsDirectory) {
        $ccsFailure = 'DIRECTORY_EXISTS'
        throw 'Existing path refused'
    }
    Write-CcsSetupStage 'CREATE_DIRECTORY'
    New-Item -ItemType Directory -Path $ccsDirectory | Out-Null

    $ccsFailure = 'ACL_NOT_VERIFIED'
    Write-CcsSetupStage 'APPLY_ACL'
    $ccsOwner = [System.Security.Principal.WindowsIdentity]::GetCurrent().User
    $ccsSystem = [System.Security.Principal.SecurityIdentifier]::new('S-1-5-18')
    $ccsAcl = [System.Security.AccessControl.DirectorySecurity]::new()
    $ccsAcl.SetOwner($ccsOwner)
    $ccsAcl.SetAccessRuleProtection($true, $false)
    foreach ($ccsIdentity in @($ccsOwner, $ccsSystem)) {
        $ccsRule = [System.Security.AccessControl.FileSystemAccessRule]::new(
            $ccsIdentity, 'FullControl', 'ContainerInherit,ObjectInherit', 'None', 'Allow')
        $ccsAcl.AddAccessRule($ccsRule)
    }
    Set-Acl -LiteralPath $ccsDirectory -AclObject $ccsAcl
    Write-CcsSetupStage 'VERIFY_ACL'
    $ccsActual = Get-Acl -LiteralPath $ccsDirectory
    $ccsRules = @($ccsActual.GetAccessRules($true, $true, [System.Security.Principal.SecurityIdentifier]))
    if (-not $ccsActual.AreAccessRulesProtected -or $ccsRules.Count -ne 2 -or
        $ccsActual.GetOwner([System.Security.Principal.SecurityIdentifier]).Value -ne $ccsOwner.Value) {
        throw 'Directory protection mismatch'
    }
    foreach ($ccsRule in $ccsRules) {
        if ($ccsRule.IdentityReference.Value -notin @($ccsOwner.Value, $ccsSystem.Value) -or
            $ccsRule.AccessControlType -ne 'Allow' -or $ccsRule.IsInherited -or
            $ccsRule.FileSystemRights -ne [System.Security.AccessControl.FileSystemRights]::FullControl -or
            $ccsRule.InheritanceFlags -ne 'ContainerInherit,ObjectInherit') {
            throw 'Directory access mismatch'
        }
    }

    $ccsFailure = 'KEY_INPUT_INVALID'
    Write-CcsSetupStage 'READ_SECRET'
    $ccsSecret = Read-Host -Prompt 'Paste the restricted sandbox key (rk_test_)' -AsSecureString
    Write-CcsSetupStage 'VALIDATE_SECRET'
    if ($ccsSecret -isnot [System.Security.SecureString]) { throw 'Masked input required' }
    $ccsPointer = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($ccsSecret)
    $ccsPlain = [System.Runtime.InteropServices.Marshal]::PtrToStringBSTR($ccsPointer)
    if ($ccsPlain -cnotmatch '\Ark_test_[A-Za-z0-9]{16,256}\z') { throw 'Restricted test key required' }

    $ccsFailure = 'KEY_SAVE_FAILED'
    $ccsKeyPath = Join-Path $ccsDirectory 'stripe-test.key'
    $ccsBytes = [System.Text.Encoding]::ASCII.GetBytes($ccsPlain)
    Write-CcsSetupStage 'CREATE_FILE'
    $ccsFile = [System.IO.File]::Open($ccsKeyPath, 'CreateNew', 'Write', 'None')
    Write-CcsSetupStage 'WRITE_FILE'
    $ccsFile.Write($ccsBytes, 0, $ccsBytes.Length)
    Write-CcsSetupStage 'FLUSH_FILE'
    $ccsFile.Flush($true)
}
catch {
    [Console]::Error.WriteLine("Stripe sandbox key setup failed: $ccsFailure. Inspect the folder before another attempt; existing paths are never replaced.")
    exit 1
}
finally {
    Write-CcsSetupStage 'CLEANUP'
    if ($null -ne $ccsFile) { $ccsFile.Dispose() }
    if ($null -ne $ccsBytes) { [Array]::Clear($ccsBytes, 0, $ccsBytes.Length) }
    if ($ccsPointer -ne [IntPtr]::Zero) { [System.Runtime.InteropServices.Marshal]::ZeroFreeBSTR($ccsPointer) }
    if ($ccsSecret -is [System.Security.SecureString]) { $ccsSecret.Dispose() }
    $ccsPlain = $null
}

Write-CcsSetupStage 'COMPLETE'
Write-Output "Stripe sandbox key saved: $ccsKeyPath"
