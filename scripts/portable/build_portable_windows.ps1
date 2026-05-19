param(
  [switch]$SkipGradle,
  [switch]$CreateSelfExtractingExe,
  [string]$RuntimeModules = $(if ($env:HICT_RUNTIME_MODULES) { $env:HICT_RUNTIME_MODULES } else { "java.se,jdk.charsets,jdk.crypto.ec,jdk.localedata,jdk.unsupported,jdk.zipfs" }),
  [string]$DistRoot = $(if ($env:HICT_PORTABLE_DIST_DIR) { $env:HICT_PORTABLE_DIST_DIR } else { (Join-Path $PSScriptRoot "..\..\build\portable") }),
  [string]$SevenZipRoot = $(if ($env:SEVENZIP_ROOT) { $env:SEVENZIP_ROOT } else { "C:\Program Files\7-Zip" }),
  [string]$SevenZipExtraUrl = $(if ($env:SEVENZIP_EXTRA_URL) { $env:SEVENZIP_EXTRA_URL } else { "https://www.7-zip.org/a/7z2601-extra.7z" })
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

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

function Get-JdkTool {
  param([Parameter(Mandatory = $true)][string]$Name)
  if ($env:JAVA_HOME) {
    $candidate = Join-Path $env:JAVA_HOME "bin\$Name"
    if (Test-Path $candidate) {
      return $candidate
    }
  }
  $cmd = Get-Command $Name -ErrorAction SilentlyContinue
  if ($cmd) {
    return $cmd.Source
  }
  throw "Missing $Name. Use a full JDK, not a JRE."
}

$projectDir = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$version = (Get-Content (Join-Path $projectDir "version.txt") -Raw).Trim()
$platform = "windows-x86_64"
$appName = "HiCT"
$appDir = Join-Path $DistRoot "$appName-$version-$platform"
$artifactDir = Join-Path $projectDir "build\distributions"
$jlink = Get-JdkTool "jlink.exe"
$jarTool = Get-JdkTool "jar.exe"

Require-Command "powershell"

if (-not $SkipGradle) {
  Push-Location $projectDir
  try {
    & .\gradlew.bat -PrequireBundledWebUI=true shadowJar
    if ($LASTEXITCODE -ne 0) {
      throw "Gradle shadowJar failed with exit code $LASTEXITCODE"
    }
  }
  finally {
    Pop-Location
  }
}

$fatJar = Get-ChildItem -Path (Join-Path $projectDir "build\libs") -Filter "*-fat.jar" |
  Sort-Object LastWriteTime |
  Select-Object -Last 1
if (-not $fatJar) {
  throw "Fat JAR was not found under $projectDir\build\libs"
}
$jarEntries = & $jarTool tf $fatJar.FullName
if ($LASTEXITCODE -ne 0) {
  throw "Failed to inspect $($fatJar.FullName)"
}
if (-not ($jarEntries -contains "webui/index.html")) {
  throw "Fat JAR does not contain webui/index.html; portable packages require a baked-in HiCT_WebUI build."
}

if (Test-Path $appDir) {
  Remove-Item -Recurse -Force $appDir
}
New-Item -ItemType Directory -Force -Path $appDir | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $appDir "bin") | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $appDir "lib") | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $appDir "licenses") | Out-Null
New-Item -ItemType Directory -Force -Path $artifactDir | Out-Null

Copy-Item -Force $fatJar.FullName (Join-Path $appDir "lib\hict.jar")
Copy-Item -Force (Join-Path $projectDir "LICENSE") (Join-Path $appDir "licenses\HiCT_JVM_LICENSE")
$webUiLicense = Join-Path $projectDir "..\HiCT_WebUI\LICENSE"
if (Test-Path $webUiLicense) {
  Copy-Item -Force $webUiLicense (Join-Path $appDir "licenses\HiCT_WebUI_LICENSE")
}

Push-Location $appDir
try {
  Invoke-Native -FilePath $jarTool -Arguments @("xf", (Join-Path $appDir "lib\hict.jar"), "webui")
}
finally {
  Pop-Location
}

@'
HiCT portable distribution notice
=================================

This package is assembled from:

  - HiCT_JVM and HiCT_WebUI, redistributed under their bundled MIT licenses.
  - The HiCT_JVM fat JAR, which contains Java dependencies and preserves the
    upstream META-INF license/notice files included in those dependency JARs.
  - A jlink runtime image created from the release JDK. The runtime\legal
    directory is intentionally kept intact and must remain with redistributed
    packages. For Eclipse Temurin/OpenJDK runtimes this includes GPLv2 with
    Classpath Exception notices and third-party runtime notices.
  - Optional bundled hictk resources inside hict.jar when toolchains-dist was
    prepared before packaging. hictk is redistributed under its MIT license and
    should be cited when .hic conversion is used.
  - Optional single-file Windows EXE packaging built with official 7-Zip SFX
    modules when -CreateSelfExtractingExe is used. Keep the 7-Zip license notice
    with redistributed artifacts.

The portable Windows ZIP remains the most transparent artifact. The optional
EXE is an official 7-Zip self-extracting launcher for users who need a single
double-clickable file without MSI installation.
'@ | Set-Content -Encoding UTF8 (Join-Path $appDir "licenses\PORTABLE_DISTRIBUTION_NOTICE.txt")

@'
7-Zip SFX notice
================

Single-file Windows EXE artifacts are assembled with official 7-Zip SFX modules
when requested by the release workflow. 7-Zip is distributed under LGPL terms
with additional components noted by the upstream project. Keep this notice with
redistributed portable packages and refer to the official 7-Zip license page for
the exact license text of the module used by the build runner.
'@ | Set-Content -Encoding UTF8 (Join-Path $appDir "licenses\SevenZip_SFX_NOTICE.txt")

& $jlink `
  --add-modules $RuntimeModules `
  --strip-debug `
  --no-header-files `
  --no-man-pages `
  --compress=zip-6 `
  --output (Join-Path $appDir "runtime")

@'
@echo off
setlocal
set "SCRIPT_DIR=%~dp0"
for %%I in ("%SCRIPT_DIR%..") do set "APP_HOME=%%~fI"

if "%DATA_DIR%"=="" (
  set "DATA_DIR=%APP_HOME%"
)
if "%WEBUI_ROOT%"=="" (
  if exist "%APP_HOME%\webui\index.html" set "WEBUI_ROOT=%APP_HOME%\webui"
)

if not exist "%DATA_DIR%" mkdir "%DATA_DIR%" >nul 2>nul

set "JAVA_EXE=%APP_HOME%\runtime\bin\java.exe"
if "%~1"=="" (
  start "HiCT WebUI opener" /min "%APP_HOME%\bin\open-browser-when-ready.cmd" "http://localhost:8080/"
  "%JAVA_EXE%" -DAUTO_OPEN_BROWSER=false %HICT_JAVA_OPTS% -jar "%APP_HOME%\lib\hict.jar"
) else (
  "%JAVA_EXE%" %HICT_JAVA_OPTS% -jar "%APP_HOME%\lib\hict.jar" %*
)
exit /b %ERRORLEVEL%
'@ | Set-Content -Encoding ASCII (Join-Path $appDir "bin\hict.cmd")

@'
@echo off
setlocal
set "HICT_URL=%~1"
if "%HICT_URL%"=="" set "HICT_URL=http://localhost:8080/"
for /l %%i in (1,1,90) do (
  powershell -NoProfile -ExecutionPolicy Bypass -Command "try { $r = Invoke-WebRequest -UseBasicParsing -TimeoutSec 1 '%HICT_URL%'; if ($r.StatusCode -ge 200) { exit 0 } } catch { exit 1 }" >nul 2>nul
  if not errorlevel 1 (
    start "" "%HICT_URL%"
    exit /b 0
  )
  timeout /t 1 /nobreak >nul
)
exit /b 1
'@ | Set-Content -Encoding ASCII (Join-Path $appDir "bin\open-browser-when-ready.cmd")

@'
@echo off
call "%~dp0bin\hict.cmd" %*
'@ | Set-Content -Encoding ASCII (Join-Path $appDir "HiCT.cmd")

@'
@echo off
setlocal
if "%DATA_DIR%"=="" (
  for /f "usebackq delims=" %%I in (`powershell -NoProfile -ExecutionPolicy Bypass -Command "$self = Get-CimInstance Win32_Process -Filter ('ProcessId=' + $PID); $parent = if ($self) { Get-CimInstance Win32_Process -Filter ('ProcessId=' + $self.ParentProcessId) } else { $null }; $grandParent = if ($parent) { Get-CimInstance Win32_Process -Filter ('ProcessId=' + $parent.ParentProcessId) } else { $null }; if ($grandParent -and $grandParent.ExecutablePath) { Split-Path -Parent $grandParent.ExecutablePath }" 2^>nul`) do set "DATA_DIR=%%I"
)
call "%~dp0HiCT.cmd"
'@ | Set-Content -Encoding ASCII (Join-Path $appDir "HiCT-SFX.cmd")

@"
HiCT portable Windows package
=============================

Run:
  HiCT.cmd
  HiCT.cmd --help
  HiCT.cmd start-server
  HiCT.cmd convert --help

The package includes:
  - HiCT_JVM fat JAR, including the built HiCT_WebUI resources
  - extracted HiCT_WebUI assets used as WEBUI_ROOT for robust portable serving
  - a jlink runtime built from the JDK used by the release runner
  - HiCT license files
  - the runtime\legal directory generated by jlink

DATA_DIR defaults to this extracted portable directory. Explicit DATA_DIR always wins.

Java runtime notices:
  The embedded runtime keeps its jlink-generated legal\ directory intact. For
  Temurin/OpenJDK builds this includes the OpenJDK GPLv2 + Classpath Exception
  notices and third-party notices shipped with the runtime.

Windows single-file note:
  The ZIP is the transparent portable artifact. If the release also contains a
  .exe, that EXE is an official 7-Zip SFX wrapper around the same portable app,
  not an MSI installer.
"@ | Set-Content -Encoding UTF8 (Join-Path $appDir "README_PORTABLE.txt")

$zipPath = Join-Path $artifactDir "$appName-$version-$platform-portable.zip"
if (Test-Path $zipPath) {
  Remove-Item -Force $zipPath
}
Compress-Archive -Path $appDir -DestinationPath $zipPath -CompressionLevel Optimal

$shaPath = Join-Path $artifactDir "$appName-$version-$platform.sha256"
$hashLines = @()
$hashLines += "$((Get-FileHash -Algorithm SHA256 $zipPath).Hash.ToLowerInvariant())  $(Split-Path -Leaf $zipPath)"

if ($CreateSelfExtractingExe) {
  $sevenZipExe = Join-Path $SevenZipRoot "7z.exe"
  if (-not (Test-Path $sevenZipExe)) {
    $sevenZipCommand = Get-Command "7z.exe" -ErrorAction SilentlyContinue
    if ($sevenZipCommand) {
      $sevenZipExe = $sevenZipCommand.Source
    }
  }
  if (-not (Test-Path $sevenZipExe)) {
    throw "7z.exe is required for -CreateSelfExtractingExe. Install official 7-Zip or set SEVENZIP_ROOT."
  }

  $sfxCandidates = @(
    (Join-Path $SevenZipRoot "7zS.sfx"),
    (Join-Path $SevenZipRoot "7zSD.sfx")
  ) | Where-Object { Test-Path $_ }

  $sfxBuildDir = Join-Path $DistRoot "windows-sfx"
  if (Test-Path $sfxBuildDir) {
    Remove-Item -Recurse -Force $sfxBuildDir
  }
  New-Item -ItemType Directory -Force -Path $sfxBuildDir | Out-Null

  if (-not $sfxCandidates) {
    $extraArchive = Join-Path $sfxBuildDir "7zip-extra.7z"
    $extraDir = Join-Path $sfxBuildDir "7zip-extra"
    Invoke-WebRequest -Uri $SevenZipExtraUrl -OutFile $extraArchive
    Invoke-Native -FilePath $sevenZipExe -Arguments @("x", $extraArchive, "-o$extraDir", "-y")
    $sfxCandidates = Get-ChildItem -Path $extraDir -Recurse -Include "7zS.sfx", "7zSD.sfx" |
      Sort-Object FullName |
      Select-Object -ExpandProperty FullName
  }
  if (-not $sfxCandidates) {
    throw "No installer-capable official 7-Zip SFX module (7zS.sfx/7zSD.sfx) was found."
  }
  $sfxModule = $sfxCandidates[0]

  $archivePath = Join-Path $sfxBuildDir "payload.7z"
  $configPath = Join-Path $sfxBuildDir "config.txt"
  $exePath = Join-Path $artifactDir "$appName-$version-$platform.exe"

  Push-Location $appDir
  try {
    Invoke-Native -FilePath $sevenZipExe -Arguments @("a", "-t7z", "-mx=7", "-mmt=on", $archivePath, ".\*")
  }
  finally {
    Pop-Location
  }

  @'
;!@Install@!UTF-8!
Title="HiCT Portable"
GUIMode="2"
RunProgram="cmd /d /k HiCT-SFX.cmd"
;!@InstallEnd@!
'@ | Set-Content -Encoding UTF8 $configPath

  if (Test-Path $exePath) {
    Remove-Item -Force $exePath
  }
  $out = [System.IO.File]::Create($exePath)
  try {
    foreach ($part in @($sfxModule, $configPath, $archivePath)) {
      $bytes = [System.IO.File]::ReadAllBytes($part)
      $out.Write($bytes, 0, $bytes.Length)
    }
  }
  finally {
    $out.Dispose()
  }
  $hashLines += "$((Get-FileHash -Algorithm SHA256 $exePath).Hash.ToLowerInvariant())  $(Split-Path -Leaf $exePath)"
}

$hashLines | Set-Content -Encoding ASCII $shaPath

Write-Host "Built $zipPath"
if ($CreateSelfExtractingExe) {
  Write-Host "Built optional portable SFX EXE in $artifactDir"
}
Write-Host "Wrote $shaPath"
