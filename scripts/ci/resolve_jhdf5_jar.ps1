$ErrorActionPreference = 'Stop'

$repo = if ($env:HICT_JHDF5_REPO) { $env:HICT_JHDF5_REPO } else { 'AxisAlexNT/jhdf5-with-plugins-configuration-snapshot' }
$ref = if ($env:HICT_JHDF5_REF) { $env:HICT_JHDF5_REF } elseif ($env:GITHUB_REF_NAME) { $env:GITHUB_REF_NAME } else { 'jhdf5-with-plugins-configuration-snapshot' }
$jarName = if ($env:HICT_JHDF5_JAR_NAME) { $env:HICT_JHDF5_JAR_NAME } else { 'sis-jhdf5-19.04.1.jar' }
$out = if ($env:HICT_JHDF5_LOCAL_JAR) { $env:HICT_JHDF5_LOCAL_JAR } else { Join-Path 'src/main/resources/libs' $jarName }
$mode = if ($env:HICT_JHDF5_SOURCE_MODE) { $env:HICT_JHDF5_SOURCE_MODE } else { 'artifact' }
$releaseTag = if ($env:HICT_JHDF5_RELEASE_TAG) { $env:HICT_JHDF5_RELEASE_TAG } else { 'latest' }
$artifactName = if ($env:HICT_JHDF5_ARTIFACT_NAME) { $env:HICT_JHDF5_ARTIFACT_NAME } else { 'jhdf5-packaged-jar' }
$strictSnapshot = $env:HICT_REQUIRE_SNAPSHOT_JHDF5 -eq '1'

function Write-GitHubEnvLines([string[]] $Lines) {
  if ($env:GITHUB_ENV) {
    foreach ($line in $Lines) {
      $line | Out-File -Append -FilePath $env:GITHUB_ENV -Encoding utf8
    }
  }
}

function Use-MavenFallback([string] $Reason) {
  Write-Host "::warning::Falling back to Maven-provided cisd:jhdf5:19.04.1 because $Reason"
  Write-GitHubEnvLines @(
    'HICT_JHDF5_SOURCE_MODE=maven',
    'HICT_USE_MAVEN_JHDF5=1',
    'HICT_REQUIRE_BUNDLED_JHDF5=0'
  )
  exit 0
}

function Get-RequiredPatternsForRunner() {
  $runnerOs = if ($env:RUNNER_OS) { $env:RUNNER_OS } else { [System.Runtime.InteropServices.RuntimeInformation]::OSDescription }
  $runnerArch = if ($env:RUNNER_ARCH) { $env:RUNNER_ARCH } else { [System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture.ToString() }
  if ($runnerOs -eq 'Windows') {
    return @('^native/jhdf5/amd64-Windows/jhdf5\.dll$')
  }
  if ($runnerOs -eq 'macOS' -and $runnerArch -eq 'ARM64') {
    return @('^resources/libs/(osx_arm64|macos_arm64|darwin_arm64)/libhdf5\.dylib$')
  }
  if ($runnerOs -eq 'macOS') {
    return @('^resources/libs/(osx_64|macos_64|darwin_x86_64)/libhdf5\.dylib$')
  }
  if ($runnerOs -eq 'Linux' -and $runnerArch -eq 'ARM64') {
    return @('^native/jhdf5/(aarch64|arm64)-Linux/libjhdf5\.so$')
  }
  return @('^native/jhdf5/amd64-Linux/libjhdf5\.so$')
}

if ($mode -in @('maven', 'maven-central', 'published')) {
  Write-Host "HICT_JHDF5_SOURCE_MODE=$mode; using Maven-provided cisd:jhdf5:19.04.1."
  Write-GitHubEnvLines @(
    'HICT_JHDF5_SOURCE_MODE=maven',
    'HICT_USE_MAVEN_JHDF5=1',
    'HICT_REQUIRE_BUNDLED_JHDF5=0'
  )
  exit 0
}

New-Item -ItemType Directory -Force -Path (Split-Path -Parent $out) | Out-Null

if (Test-Path $out) {
  Write-Host "Using existing JHDF5 jar: $out"
} else {
  if (-not (Get-Command gh -ErrorAction SilentlyContinue)) {
    if ($strictSnapshot -or $mode -eq 'local') {
      throw "GitHub CLI (gh) is required to resolve $repo JHDF5 jar."
    }
    Use-MavenFallback 'GitHub CLI (gh) is unavailable'
  }
  $tmp = Join-Path ([System.IO.Path]::GetTempPath()) ("hict-jhdf5-" + [guid]::NewGuid().ToString('N'))
  New-Item -ItemType Directory -Force -Path $tmp | Out-Null
  try {
    $resolveError = $null
    try {
      switch ($mode) {
        'local' { $resolveError = "HICT_JHDF5_SOURCE_MODE=local but $out does not exist." }
        'release' {
          if ($releaseTag -eq 'latest') {
            gh release download --repo $repo --pattern $jarName --dir $tmp
          } else {
            gh release download $releaseTag --repo $repo --pattern $jarName --dir $tmp
          }
          if ($LASTEXITCODE -ne 0) { $resolveError = "release $releaseTag in $repo has no downloadable $jarName payload" }
        }
        { $_ -in @('artifact', 'branch', 'workflow') } {
          $runId = gh run list --repo $repo --branch $ref --workflow build-native.yml --status success --limit 1 --json databaseId --jq '.[0].databaseId'
          if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($runId) -or $runId -eq 'null') {
            $resolveError = "no successful build-native.yml run exists in $repo on branch/ref $ref"
          } else {
            gh run download $runId --repo $repo --name $artifactName --dir $tmp
            if ($LASTEXITCODE -ne 0) { $resolveError = "artifact $artifactName is missing from successful run $runId" }
          }
        }
        default { throw "Unsupported HICT_JHDF5_SOURCE_MODE=$mode; use artifact, release, local, or maven." }
      }
    } catch {
      $resolveError = $_.Exception.Message
    }
    if ($resolveError) {
      if ($strictSnapshot -or $mode -eq 'local') { throw $resolveError }
      Use-MavenFallback $resolveError
    }
    $found = Get-ChildItem -Path $tmp -Recurse -Filter $jarName | Sort-Object FullName | Select-Object -First 1
    if (-not $found) {
      if ($strictSnapshot) {
        Get-ChildItem -Path $tmp -Recurse | Select-Object -ExpandProperty FullName | Write-Host
        throw "Downloaded payload does not contain $jarName."
      }
      Use-MavenFallback "downloaded $repo artifact does not contain $jarName"
    }
    Copy-Item -Force $found.FullName $out
  } finally {
    Remove-Item -Recurse -Force $tmp -ErrorAction SilentlyContinue
  }
}

$entries = (& jar tf $out)
if ($LASTEXITCODE -ne 0) { throw "jar tf failed for $out" }
$missing = $false
foreach ($pattern in Get-RequiredPatternsForRunner) {
  if (-not ($entries | Select-String -Quiet -Pattern $pattern)) {
    Write-Host "::warning::JHDF5 snapshot jar $out lacks required native entry matching $pattern for $($env:RUNNER_OS)/$($env:RUNNER_ARCH)."
    $missing = $true
  }
}
if ($missing) {
  if ($strictSnapshot) {
    throw 'Required snapshot JHDF5 native payload is missing and HICT_REQUIRE_SNAPSHOT_JHDF5=1.'
  }
  Remove-Item -Force $out -ErrorAction SilentlyContinue
  Use-MavenFallback "snapshot JHDF5 artifact has no ready native payload for $($env:RUNNER_OS)/$($env:RUNNER_ARCH)"
}

Write-GitHubEnvLines @(
  "HICT_JHDF5_SOURCE_MODE=$mode",
  "HICT_JHDF5_LOCAL_JAR=$out",
  'HICT_REQUIRE_BUNDLED_JHDF5=1'
)
Write-Host "Resolved JHDF5 jar: $out"
