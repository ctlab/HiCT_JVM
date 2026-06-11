$ErrorActionPreference = 'Stop'

$repo = if ($env:HICT_JHDF5_REPO) { $env:HICT_JHDF5_REPO } else { 'AxisAlexNT/jhdf5-with-plugins-configuration-snapshot' }
$ref = if ($env:HICT_JHDF5_REF) { $env:HICT_JHDF5_REF } elseif ($env:GITHUB_REF_NAME) { $env:GITHUB_REF_NAME } else { 'master' }
$jarName = if ($env:HICT_JHDF5_JAR_NAME) { $env:HICT_JHDF5_JAR_NAME } else { 'sis-jhdf5-19.04.1.jar' }
$out = if ($env:HICT_JHDF5_LOCAL_JAR) { $env:HICT_JHDF5_LOCAL_JAR } else { Join-Path 'src/main/resources/libs' $jarName }
$mode = if ($env:HICT_JHDF5_SOURCE_MODE) { $env:HICT_JHDF5_SOURCE_MODE } else { 'artifact' }
$releaseTag = if ($env:HICT_JHDF5_RELEASE_TAG) { $env:HICT_JHDF5_RELEASE_TAG } else { 'latest' }
$artifactName = if ($env:HICT_JHDF5_ARTIFACT_NAME) { $env:HICT_JHDF5_ARTIFACT_NAME } else { 'jhdf5-packaged-jar' }

New-Item -ItemType Directory -Force -Path (Split-Path -Parent $out) | Out-Null

if (Test-Path $out) {
  Write-Host "Using existing JHDF5 jar: $out"
} else {
  if (-not (Get-Command gh -ErrorAction SilentlyContinue)) {
    throw "GitHub CLI (gh) is required to resolve $repo JHDF5 jar."
  }
  $tmp = Join-Path ([System.IO.Path]::GetTempPath()) ("hict-jhdf5-" + [guid]::NewGuid().ToString('N'))
  New-Item -ItemType Directory -Force -Path $tmp | Out-Null
  try {
    switch ($mode) {
      'local' { throw "HICT_JHDF5_SOURCE_MODE=local but $out does not exist." }
      'release' {
        if ($releaseTag -eq 'latest') {
          gh release download --repo $repo --pattern $jarName --dir $tmp
        } else {
          gh release download $releaseTag --repo $repo --pattern $jarName --dir $tmp
        }
      }
      { $_ -in @('artifact', 'branch', 'workflow') } {
        $runId = gh run list --repo $repo --branch $ref --workflow build-native.yml --status success --limit 1 --json databaseId --jq '.[0].databaseId'
        if ([string]::IsNullOrWhiteSpace($runId) -or $runId -eq 'null') {
          throw "No successful build-native.yml run found in $repo on branch/ref $ref."
        }
        gh run download $runId --repo $repo --name $artifactName --dir $tmp
      }
      default { throw "Unsupported HICT_JHDF5_SOURCE_MODE=$mode; use artifact, release, or local." }
    }
    $found = Get-ChildItem -Path $tmp -Recurse -Filter $jarName | Sort-Object FullName | Select-Object -First 1
    if (-not $found) {
      Get-ChildItem -Path $tmp -Recurse | Select-Object -ExpandProperty FullName | Write-Host
      throw "Downloaded payload does not contain $jarName."
    }
    Copy-Item -Force $found.FullName $out
  } finally {
    Remove-Item -Recurse -Force $tmp -ErrorAction SilentlyContinue
  }
}

$entries = (& jar tf $out)
if ($LASTEXITCODE -ne 0) { throw "jar tf failed for $out" }
function Assert-JarEntryAny([string]$Label, [string[]]$Patterns) {
  foreach ($pattern in $Patterns) {
    if ($entries | Select-String -Quiet -Pattern $pattern) { return }
  }
  throw "JHDF5 jar $out is missing $Label."
}
Assert-JarEntryAny 'Windows amd64 jhdf5.dll' @('^native/jhdf5/amd64-Windows/jhdf5\.dll$')
Assert-JarEntryAny 'macOS arm64 libhdf5.dylib' @('^resources/libs/(osx_arm64|macos_arm64|darwin_arm64)/libhdf5\.dylib$')
Assert-JarEntryAny 'macOS x86_64 libhdf5.dylib' @('^resources/libs/(osx_64|macos_64|darwin_x86_64)/libhdf5\.dylib$')
if ($env:GITHUB_ENV) { "HICT_JHDF5_LOCAL_JAR=$out" | Out-File -Append -FilePath $env:GITHUB_ENV -Encoding utf8 }
Write-Host "Resolved JHDF5 jar: $out"
