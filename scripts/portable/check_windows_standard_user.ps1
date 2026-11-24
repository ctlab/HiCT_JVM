param(
  [Parameter(Mandatory)][string]$Workspace,
  [Parameter(Mandatory)][string]$ZipLauncher,
  [Parameter(Mandatory)][string]$Exe,
  [Parameter(Mandatory)][string]$Version
)

$ErrorActionPreference = "Stop"
if (-not $env:GITHUB_ACTIONS -or $env:RUNNER_OS -ne "Windows") {
  throw "Run the disposable-user check on a dedicated Windows CI runner."
}

$stamp = [guid]::NewGuid().ToString("N").Substring(0, 12)
$name = "hictqa-$stamp"
$password = ConvertTo-SecureString ("HictQA!9" + [guid]::NewGuid().ToString("N")) -AsPlainText -Force
$account = New-LocalUser -Name $name -Password $password -Description "Disposable HiCT Windows qualification user"

function Set-TestAccess($path, $permissions, $mode) {
  $acl = Get-Acl -LiteralPath $path
  $rule = [System.Security.AccessControl.FileSystemAccessRule]::new(
    $account.SID, $permissions, "ContainerInherit, ObjectInherit", "None", $mode
  )
  $acl.AddAccessRule($rule)
  Set-Acl -LiteralPath $path -AclObject $acl
}

try {
  $users = [System.Security.Principal.SecurityIdentifier]::new("S-1-5-32-545")
  if (-not (Get-LocalGroupMember -SID $users | Where-Object { $_.SID -eq $account.SID })) {
    Add-LocalGroupMember -SID $users -Member $account
  }

  $work = Join-Path $Workspace "standard user output"
  $localExe = Join-Path $Workspace "HiCT standard-user check.exe"
  Copy-Item -LiteralPath $Exe -Destination $localExe
  New-Item -ItemType Directory -Path $work | Out-Null
  Set-TestAccess $Workspace "ReadAndExecute" "Allow"
  Set-TestAccess $work "Modify" "Allow"
  Set-TestAccess (Join-Path $Workspace "data") "WriteData, AppendData, DeleteSubdirectoriesAndFiles" "Deny"

  $run = Join-Path $Workspace "standard-user.ps1"
  $configuration = Join-Path $Workspace "standard-user.json"
  @{ Sid = $account.SID.Value; Work = $work; Data = (Join-Path $Workspace "data")
    Exe = $localExe; Zip = $ZipLauncher; Version = $Version } |
    ConvertTo-Json | Set-Content -LiteralPath $configuration -Encoding utf8

  @'
param([string]$Configuration)
$ErrorActionPreference = "Stop"
$config = Get-Content -LiteralPath $Configuration -Raw | ConvertFrom-Json
Start-Transcript -Path (Join-Path $config.Work "qualification.log")
try {
  $identity = [System.Security.Principal.WindowsIdentity]::GetCurrent()
  $principal = [System.Security.Principal.WindowsPrincipal]::new($identity)
  if ($identity.User.Value -ne $config.Sid -or $principal.IsInRole([System.Security.Principal.WindowsBuiltInRole]::Administrator)) {
    throw "The test process must run under the disposable account without admin privileges."
  }
  $env:DATA_DIR = $config.Data
  $env:PROCESSED_DIR = Join-Path $config.Work "processed"
  $env:HICT_PORTABLE_CACHE_ROOT = Join-Path $config.Work "portable cache"
  $env:USERPROFILE = [Environment]::GetFolderPath("UserProfile")
  if (-not $env:USERPROFILE) { throw "The standard user's Windows profile was not loaded." }
  $blocked = Join-Path $config.Work "unavailable redirected folder"
  Set-Content -LiteralPath $blocked -Value "The application must choose another folder."
  $env:APPDATA = $env:LOCALAPPDATA = $env:TEMP = $env:TMP = $env:HICT_TEMP_DIR = $blocked

  $writeProbe = Join-Path $config.Data "write-probe"
  try { [System.IO.File]::WriteAllText($writeProbe, "test") } catch [UnauthorizedAccessException] {}
  if (Test-Path -LiteralPath $writeProbe) { throw "The input data directory must be read-only for this test user." }
  $inputMap = Join-Path $config.Data "tiny-primary.mcool"
  $startHash = (Get-FileHash -LiteralPath $inputMap -Algorithm SHA256).Hash
  foreach ($package in @(@{ Name = "EXE"; Path = $config.Exe }, @{ Name = "ZIP"; Path = $config.Zip })) {
    $launcher = $package.Path
    $version = & $launcher --version
    if ($LASTEXITCODE -ne 0 -or "HiCT $($config.Version)" -notin $version) { throw "Unexpected $($package.Name) version." }
    & $launcher check-toolchains --require-hdf5-native
    if ($LASTEXITCODE -ne 0) { throw "HDF5 failed under the standard account." }
    $outputMap = Join-Path $config.Work "$($package.Name) matrix Δ.hict.hdf5"
    & $launcher convert mcool-to-hict --input $inputMap --output $outputMap --chunk-size 64 --parallelism 1
    if ($LASTEXITCODE -ne 0) { throw "Conversion failed under the standard account." }
    $reopened = Join-Path $config.Work "$($package.Name) reopened.mcool"
    & $launcher convert hict-to-mcool --input $outputMap --output $reopened --chunk-size 64 --parallelism 1
    if ($LASTEXITCODE -ne 0 -or -not (Test-Path -LiteralPath $reopened)) { throw "Cannot reopen the converted matrix." }
    if ((Get-FileHash -LiteralPath $inputMap -Algorithm SHA256).Hash -ne $startHash) { throw "Input matrix changed." }
  }
}
finally {
  Stop-Transcript
}
'@ | Set-Content -LiteralPath $run -Encoding utf8

  $start = [System.Diagnostics.ProcessStartInfo]::new()
  $start.FileName = (Get-Process -Id $PID).Path
  $start.UseShellExecute = $false
  $start.Domain = $env:COMPUTERNAME
  $start.UserName = $account.Name
  $start.Password = $password
  $start.LoadUserProfile = $true
  $start.WorkingDirectory = $Workspace
  foreach ($argument in @("-NoProfile", "-NonInteractive", "-ExecutionPolicy", "RemoteSigned", "-File", $run, $configuration)) {
    $start.ArgumentList.Add($argument)
  }
  $process = [System.Diagnostics.Process]::Start($start)
  try {
    if (-not $process.WaitForExit(480000)) {
      $process.Kill($true)
      $process.WaitForExit()
      throw "The standard-user qualification did not finish before its deadline."
    }
    if ($process.ExitCode -ne 0) { throw "The standard-user qualification failed with exit $($process.ExitCode)." }
  }
  finally {
    $process.Dispose()
  }
}
finally {
  $log = Join-Path $Workspace "standard user output\qualification.log"
  if (Test-Path -LiteralPath $log) { Get-Content -LiteralPath $log | Write-Host }
  Remove-LocalUser -SID $account.SID
  $password.Dispose()
}
