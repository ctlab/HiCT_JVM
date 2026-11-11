Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "download_verified.ps1")
$root = Join-Path ([IO.Path]::GetTempPath()) ("hict-sdk-tests-" + [guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory -Path $root | Out-Null
$fixture = Join-Path $root "fixture"
[IO.File]::WriteAllText($fixture, "SDK test bytes, not an executable")
$hash = (Get-FileHash $fixture -Algorithm SHA256).Hash
$script:responses = @()
$script:seen = [Collections.Generic.List[string]]::new()
function Invoke-WebRequest {
  param($Uri, $OutFile, $TimeoutSec, $ErrorAction)
  $script:seen.Add($Uri)
  $index = $script:seen.Count - 1
  $response = if ($index -lt $script:responses.Count) { $script:responses[$index] } else { "dns" }
  if ($response -eq "good") { Copy-Item $fixture $OutFile; return }
  [IO.File]::WriteAllText($OutFile, "incomplete or incorrect bytes")
  if ($response -eq "dns") { throw "No such host is known (simulated DNS failure)" }
}
function Assert($Condition, $Message) { if (-not $Condition) { throw $Message } }
function Fetch($Name) {
  Get-VerifiedDownload -Urls @("https://primary.invalid/sdk", "https://backup.invalid/sdk") -Sha256 $hash -CacheDirectory (Join-Path $root $Name) -RetryDelaySeconds 0
}
try {
  $script:responses = @("dns", "good"); $script:seen.Clear()
  $result = Fetch "fallback"
  Assert ($script:seen.Count -eq 2) "DNS failure must fall back to a second host"
  Assert ((Get-FileHash $result).Hash -eq $hash) "Published cache must match digest"
  $script:seen.Clear(); $again = Fetch "fallback"
  Assert ($again -eq $result -and $script:seen.Count -eq 0) "Valid cache must work fully offline"

  $script:responses = @("wrong", "good"); $script:seen.Clear()
  $result = Fetch "checksum"
  Assert ($script:seen.Count -eq 2 -and (Get-FileHash $result).Hash -eq $hash) "Wrong bytes must not be accepted"
  Assert (@(Get-ChildItem (Join-Path $root "checksum") -Filter '*.pending').Count -eq 0) "Partial downloads must not remain usable"

  [IO.File]::WriteAllText($result, "corrupt cache")
  $script:responses = @("good"); $script:seen.Clear()
  $result = Fetch "checksum"
  Assert ((Get-FileHash $result).Hash -eq $hash -and $script:seen.Count -eq 1) "Invalid cache must be re-fetched"
  Assert (@(Get-ChildItem (Join-Path $root "checksum") -Filter '*.rejected-*').Count -eq 1) "Rejected cache must be preserved"

  $script:responses = @("dns", "dns", "good"); $script:seen.Clear()
  $result = Fetch "retry"
  Assert ($script:seen.Count -eq 3) "Transient failure must get a bounded second attempt"
  Assert ($script:seen[0] -eq $script:seen[2]) "Retry must return to the primary host"

  $script:responses = @(); $script:seen.Clear(); $message = ""
  try { Fetch "failure" | Out-Null } catch { $message = $_.Exception.Message }
  Assert ($script:seen.Count -eq 4) "Total failures must stop after two attempts per host"
  Assert ($message.Contains("checksum-verified LZMA SDK") -and $message.Contains("No such host")) "Failure must explain cause and recovery"
  Assert (@(Get-ChildItem (Join-Path $root "failure") -File).Count -eq 0) "Failed downloads must never populate the cache"

  $parseErrors = $null; $tokens = $null
  [void][Management.Automation.Language.Parser]::ParseFile((Join-Path $PSScriptRoot "build_portable_windows.ps1"), [ref]$tokens, [ref]$parseErrors)
  Assert ($parseErrors.Count -eq 0) "Windows packaging script must parse successfully"
  Write-Host "SDK acquisition regressions passed: DNS fallback, offline cache, checksum rejection, corrupt-cache preservation, bounded retries, partial downloads, diagnostic errors and packaging syntax."
} finally {
  # This unique temporary fixture tree was created by this test only.
  Remove-Item -LiteralPath $root -Recurse -Force
}
