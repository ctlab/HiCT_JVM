# Shared by Windows packaging and offline regression tests. No build side effects.
function Get-VerifiedDownload {
  [CmdletBinding()]
  param(
    [Parameter(Mandatory = $true)][string[]]$Urls,
    [Parameter(Mandatory = $true)][ValidatePattern('^[a-fA-F0-9]{64}$')][string]$Sha256,
    [Parameter(Mandatory = $true)][string]$CacheDirectory,
    [ValidateRange(1, 5)][int]$Attempts = 2,
    [ValidateRange(1, 300)][int]$TimeoutSeconds = 60,
    [ValidateRange(0, 30)][int]$RetryDelaySeconds = 2
  )
  $digest = $Sha256.ToLowerInvariant()
  New-Item -ItemType Directory -Force -Path $CacheDirectory | Out-Null
  $cached = Join-Path $CacheDirectory "$digest.7z"
  if (Test-Path -LiteralPath $cached -PathType Leaf) {
    if ((Get-FileHash -LiteralPath $cached -Algorithm SHA256).Hash -eq $digest) {
      Write-Host "Using checksum-verified cached LZMA SDK: $cached"
      return $cached
    }
    # Preserve a corrupt/stale cache for inspection instead of overwriting it.
    Move-Item -LiteralPath $cached -Destination "$cached.rejected-$([guid]::NewGuid().ToString('N'))"
    Write-Warning "Cached LZMA SDK checksum mismatch; fetching a verified copy."
  }
  $failures = [System.Collections.Generic.List[string]]::new()
  for ($attempt = 1; $attempt -le $Attempts; $attempt++) {
    foreach ($url in ($Urls | Select-Object -Unique)) {
      $pending = Join-Path $CacheDirectory "$digest.$([guid]::NewGuid().ToString('N')).pending"
      try {
        Write-Host "Downloading LZMA SDK (attempt $attempt/$Attempts): $url"
        Invoke-WebRequest -Uri $url -OutFile $pending -TimeoutSec $TimeoutSeconds -ErrorAction Stop
        $actual = (Get-FileHash -LiteralPath $pending -Algorithm SHA256).Hash
        if ($actual -ne $digest) { throw "SHA256 mismatch: expected $digest; received $actual" }
        # Another build may have populated the same immutable cache meanwhile.
        if (Test-Path -LiteralPath $cached) {
          if ((Get-FileHash -LiteralPath $cached -Algorithm SHA256).Hash -ne $digest) {
            throw "Concurrent cache entry failed checksum validation: $cached"
          }
        } else {
          Move-Item -LiteralPath $pending -Destination $cached
        }
        return $cached
      } catch {
        $reason = "${url}: $($_.Exception.Message)"
        $failures.Add($reason)
        Write-Warning "SDK download failed; trying another verified source. $reason"
      } finally {
        # Only this call's temporary download is removed; existing caches remain.
        if (Test-Path -LiteralPath $pending) { Remove-Item -LiteralPath $pending -Force }
      }
    }
    if ($attempt -lt $Attempts -and $RetryDelaySeconds -gt 0) { Start-Sleep -Seconds ($RetryDelaySeconds * $attempt) }
  }
  throw "Unable to obtain checksum-verified LZMA SDK after $Attempts attempts per source. Check network/DNS or prepopulate $cached with the matching SDK. For a different SDK, set SEVENZIP_SDK_URL and SEVENZIP_SDK_SHA256 together. Failures: $($failures -join '; ')"
}
