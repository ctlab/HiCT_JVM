param(
  [string]$HictkRef = $(if ($env:HICTK_REF) { $env:HICTK_REF } else { "latest" }),
  [string]$OutputDir = $(if ($env:OUTPUT_DIR) { $env:OUTPUT_DIR } else { (Join-Path $PSScriptRoot "..\..\toolchains-dist\windows_x86_64") }),
  [string]$WorkDir = $(if ($env:WORK_DIR) { $env:WORK_DIR } else { (Join-Path $env:TEMP "hictk-build-windows-x86_64") }),
  [switch]$RunTests,
  [switch]$MostlyStaticRuntime
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
if ($PSVersionTable.PSVersion.Major -ge 7) {
  $PSNativeCommandUseErrorActionPreference = $true
}

function Require-Command {
  param([Parameter(Mandatory = $true)][string]$Name)
  if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
    throw "Missing required command: $Name"
  }
}

function Invoke-Native {
  param(
    [Parameter(Mandatory = $true)][string]$FilePath,
    [string[]]$Arguments = @()
  )
  & $FilePath @Arguments
  if ($LASTEXITCODE -ne 0) {
    throw "Native command failed with exit code ${LASTEXITCODE}: $FilePath $($Arguments -join ' ')"
  }
}

function Resolve-LatestRef {
  $refs = & git ls-remote --refs --tags https://github.com/paulsengroup/hictk.git "v*"
  if ($LASTEXITCODE -ne 0) {
    throw "Failed to query official hictk tags."
  }
  if (-not $refs) {
    throw "Failed to resolve official hictk tags."
  }
  $tags = $refs `
    | ForEach-Object { ($_ -split "`t")[-1] } `
    | ForEach-Object { $_ -replace "^refs/tags/", "" } `
    | Sort-Object {
      [Version](($_ -replace "^v", "") -replace "-.*$", "")
    }
  return $tags[-1]
}

Require-Command git
Require-Command python
Require-Command cmake

if ($HictkRef -eq "latest") {
  $HictkRef = Resolve-LatestRef
}

Write-Host "[hictk/windows] Building $HictkRef into $OutputDir"

$repoUrl = "https://github.com/paulsengroup/hictk.git"
$sourceDir = Join-Path $WorkDir "src"
$venvDir = Join-Path $WorkDir "venv"
$buildDir = Join-Path $sourceDir "build"
$stageDir = Join-Path $WorkDir "stage"

if (Test-Path $WorkDir) {
  Remove-Item -Recurse -Force $WorkDir
}

New-Item -ItemType Directory -Force -Path $WorkDir | Out-Null
Invoke-Native -FilePath "python" -Arguments @("-m", "venv", $venvDir)

$pythonExe = Join-Path $venvDir "Scripts\python.exe"
$pipExe = $pythonExe
$conanExe = Join-Path $venvDir "Scripts\conan.exe"
$ninjaExe = Join-Path $venvDir "Scripts\ninja.exe"

Invoke-Native -FilePath $pipExe -Arguments @("-m", "pip", "install", "--upgrade", "pip", "setuptools", "wheel")
Invoke-Native -FilePath $pipExe -Arguments @("-m", "pip", "install", "conan>=2", "cmake>=3.25", "ninja")

Invoke-Native -FilePath "git" -Arguments @("clone", "--depth", "1", "--branch", $HictkRef, $repoUrl, $sourceDir)

$env:PATH = "$(Join-Path $venvDir 'Scripts');$env:PATH"
$env:CONAN_HOME = Join-Path $WorkDir "conan-home"
Invoke-Native -FilePath $conanExe -Arguments @("profile", "detect", "--force")

Push-Location $sourceDir
try {
  $conanInstallArgs = @(
    "install",
    "--build=missing",
    "-pr:h", "default",
    "-pr:b", "default",
    "-s:h", "build_type=Release",
    "-s:b", "build_type=Release",
    "-s:h", "compiler.cppstd=17",
    "-s:b", "compiler.cppstd=17",
    "--output-folder=$buildDir",
    "."
  )

  if ($MostlyStaticRuntime) {
    $conanInstallArgs += @(
      "-s:h", "compiler.runtime=static",
      "-s:h", "compiler.runtime_type=Release"
    )
  }

  Invoke-Native -FilePath $conanExe -Arguments $conanInstallArgs

  $cmakeArgs = @(
    "-DCMAKE_BUILD_TYPE=Release",
    "-DCMAKE_PREFIX_PATH=$buildDir",
    "-DHICTK_ENABLE_TESTING=$(if ($RunTests) { 'ON' } else { 'OFF' })",
    "-DHICTK_ENABLE_FUZZY_TESTING=OFF",
    "-DHICTK_BUILD_BENCHMARKS=OFF",
    "-DHICTK_BUILD_EXAMPLES=OFF",
    "-DHICTK_BUILD_TOOLS=ON",
    "-DHICTK_DOWNLOAD_TEST_DATASET=OFF",
    "-DHICTK_WITH_ARROW=OFF",
    "-DHICTK_WITH_EIGEN=OFF",
    "-DBUILD_SHARED_LIBS=OFF",
    "-G", "Ninja",
    "-S", $sourceDir,
    "-B", $buildDir
  )

  $conanToolchainFile = Join-Path $buildDir "conan_toolchain.cmake"
  if (Test-Path $conanToolchainFile) {
    $cmakeArgs += "-DCMAKE_TOOLCHAIN_FILE=$conanToolchainFile"
  }

  if ($MostlyStaticRuntime) {
    $cmakeArgs += "-DCMAKE_MSVC_RUNTIME_LIBRARY=MultiThreaded"
  }

  Invoke-Native -FilePath "cmake" -Arguments $cmakeArgs
  Invoke-Native -FilePath "cmake" -Arguments @("--build", $buildDir, "--config", "Release")

  if ($RunTests) {
    Invoke-Native -FilePath "ctest" -Arguments @("--test-dir", $buildDir, "--output-on-failure", "-C", "Release")
  }

  if (Test-Path $stageDir) {
    Remove-Item -Recurse -Force $stageDir
  }

  Invoke-Native -FilePath "cmake" -Arguments @("--install", $buildDir, "--config", "Release", "--prefix", $stageDir, "--component", "Runtime")

  New-Item -ItemType Directory -Force -Path (Join-Path $stageDir "share\doc\hictk") | Out-Null
  Copy-Item -Force (Join-Path $sourceDir "CITATION.cff") (Join-Path $stageDir "share\doc\hictk\CITATION.cff")

  @"
project=hictk
repository=$repoUrl
ref=$HictkRef
platform=windows_x86_64
build_shared_libs=OFF
msvc_static_runtime=$(if ($MostlyStaticRuntime) { 'ON' } else { 'OFF' })
timestamp_utc=$([DateTime]::UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ"))
"@ | Set-Content -Encoding UTF8 (Join-Path $stageDir "share\doc\hictk\build-info.txt")

  $manifest = @{
    id = "hictk-$HictkRef-windows-x86_64"
    commands = @{
      hictk = "bin/hictk.exe"
    }
    files = @(
      "bin/hictk.exe",
      "share/licenses/hictk/LICENSE",
      "share/doc/hictk/CITATION.cff",
      "share/doc/hictk/build-info.txt"
    )
    notices = @(
      "This HiCT build bundles an official hictk source build produced from $HictkRef.",
      "HiCT performs .hic conversion by invoking the bundled hictk executable; no Python runtime is required for this path.",
      "hictk is redistributed under its MIT license. Keep the bundled license and citation files with released artifacts."
    )
    citations = @(
      "hictk: Rossini R, Paulsen J. hictk: blazing fast toolkit to work with .hic and .cool files. Bioinformatics. 2024;40(7):btae408. doi:10.1093/bioinformatics/btae408."
    )
    limitations = @(
      "Windows builds should be produced on Windows and bundled only into Windows fat-JAR releases.",
      "Even with /MT, system DLL compatibility still depends on the target Windows environment."
    )
  } | ConvertTo-Json -Depth 6

  $manifest | Set-Content -Encoding UTF8 (Join-Path $stageDir "manifest.json")

  if (Test-Path $OutputDir) {
    Remove-Item -Recurse -Force $OutputDir
  }

  New-Item -ItemType Directory -Force -Path $OutputDir | Out-Null
  Copy-Item -Recurse -Force (Join-Path $stageDir "*") $OutputDir
}
finally {
  Pop-Location
}

Write-Host "[hictk/windows] Bundled payload prepared at $OutputDir"
Write-Host "[hictk/windows] Run .\gradlew.bat shadowJar from HiCT_JVM to embed it into the fat JAR."
