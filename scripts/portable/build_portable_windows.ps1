param(
  [switch]$SkipGradle,
  [switch]$CreateSelfExtractingExe,
  [string]$RuntimeModules = $(if ($env:HICT_RUNTIME_MODULES) { $env:HICT_RUNTIME_MODULES } else { "java.se,jdk.charsets,jdk.crypto.ec,jdk.localedata,jdk.unsupported,jdk.zipfs" }),
  [string]$DistRoot = $(if ($env:HICT_PORTABLE_DIST_DIR) { $env:HICT_PORTABLE_DIST_DIR } else { (Join-Path $PSScriptRoot "..\..\build\portable") }),
  [string]$SevenZipRoot = $(if ($env:SEVENZIP_ROOT) { $env:SEVENZIP_ROOT } else { "C:\Program Files\7-Zip" }),
  [string]$SevenZipSdkUrl = $(if ($env:SEVENZIP_SDK_URL) { $env:SEVENZIP_SDK_URL } else { "https://www.7-zip.org/a/lzma2601.7z" })
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

function Find-SfxModules {
  param([Parameter(Mandatory = $true)][string[]]$Roots)

  # Prefer the official console small SFX module for the portable EXE. Unlike
  # the installer SFX path, it does not present itself to Windows as an
  # installer and should not request elevation.
  $preferredNames = @("7zS2con.sfx", "7zS2.sfx", "7zSD.sfx", "7zS.sfx")
  $found = New-Object 'System.Collections.Generic.List[string]'

  foreach ($root in $Roots) {
    if (-not (Test-Path $root)) {
      continue
    }

    Get-ChildItem -Path $root -Recurse -File -ErrorAction SilentlyContinue |
      Where-Object { $preferredNames -contains $_.Name } |
      ForEach-Object { $found.Add($_.FullName) }
  }

  return $found |
    Sort-Object {
      $name = Split-Path -Leaf $_
      $preferredNames.IndexOf($name)
    }, { $_ }
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
  - Optional single-file Windows EXE packaging built with official 7-Zip/LZMA
    SDK SFX modules when -CreateSelfExtractingExe is used. The build prefers
    the non-installer console SFX module to avoid unnecessary UAC elevation.
    Keep the 7-Zip SFX notice with redistributed artifacts.

The portable Windows ZIP remains the most transparent artifact. The optional
EXE is an official 7-Zip self-extracting launcher for users who need a single
double-clickable file without MSI installation.
'@ | Set-Content -Encoding UTF8 (Join-Path $appDir "licenses\PORTABLE_DISTRIBUTION_NOTICE.txt")

@'
7-Zip SFX notice
================

Single-file Windows EXE artifacts are assembled with official 7-Zip/LZMA SDK
SFX modules when requested by the release workflow. The build first checks the
local 7-Zip installation and then downloads the official LZMA SDK when needed.
7-Zip is distributed under LGPL terms with additional components noted by the
upstream project. LZMA SDK is public domain. Keep this notice with redistributed
portable packages and refer to the official 7-Zip license and SDK pages for the
exact license text of the module used by the build runner.
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

if not defined DATA_DIR (
  set "DATA_DIR=%APP_HOME%"
)
if not defined WEBUI_ROOT (
  if exist "%APP_HOME%\webui\index.html" set "WEBUI_ROOT=%APP_HOME%\webui"
)
if not defined HICT_BIND_HOST (
  set "HICT_BIND_HOST=127.0.0.1"
)

if not exist "%DATA_DIR%" mkdir "%DATA_DIR%" >nul 2>nul
pushd "%DATA_DIR%" >nul 2>nul
if errorlevel 1 (
  echo Failed to enter DATA_DIR "%DATA_DIR%".
  exit /b 1
)

set "JAVA_EXE=%APP_HOME%\runtime\bin\java.exe"
if "%~1"=="" (
  start "" /min "%ComSpec%" /c ""%APP_HOME%\bin\open-browser-when-ready.cmd" "http://localhost:8080/""
  "%JAVA_EXE%" -DAUTO_OPEN_BROWSER=false %HICT_JAVA_OPTS% -jar "%APP_HOME%\lib\hict.jar"
) else (
  "%JAVA_EXE%" %HICT_JAVA_OPTS% -jar "%APP_HOME%\lib\hict.jar" %*
)
set "HICT_EXIT_CODE=%ERRORLEVEL%"
popd >nul 2>nul
exit /b %HICT_EXIT_CODE%
'@ | Set-Content -Encoding ASCII (Join-Path $appDir "bin\hict.cmd")

@'
@echo off
setlocal
set "HICT_URL=%~1"
if "%HICT_URL%"=="" set "HICT_URL=http://localhost:8080/"
for /l %%i in (1,1,90) do (
  powershell -NoProfile -ExecutionPolicy Bypass -Command "try { $r = Invoke-WebRequest -UseBasicParsing -TimeoutSec 1 '%HICT_URL%'; if ($r.StatusCode -ge 200) { exit 0 } } catch { exit 1 }" >nul 2>nul
  if not errorlevel 1 (
    powershell -NoProfile -ExecutionPolicy Bypass -Command "try { Start-Process -FilePath '%HICT_URL%'; exit 0 } catch { exit 1 }" >nul 2>nul
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
call "%~dp0HiCT-SFX.cmd" %*
set "HICT_EXIT_CODE=%ERRORLEVEL%"
if "%~1"=="" (
  echo.
  echo HiCT exited with code %HICT_EXIT_CODE%.
  echo Press any key to close this window.
  pause >nul
)
exit /b %HICT_EXIT_CODE%
'@ | Set-Content -Encoding ASCII (Join-Path $appDir "run.cmd")

@'
@echo off
setlocal
set "SCRIPT_DIR=%~dp0"
for %%I in ("%SCRIPT_DIR%.") do set "APP_HOME=%%~fI"
if not defined DATA_DIR (
  for /f "usebackq delims=" %%I in (`powershell -NoProfile -ExecutionPolicy Bypass -Command "$skip = @('cmd.exe','powershell.exe','pwsh.exe','conhost.exe'); $current = $PID; for ($i = 0; $i -lt 10; $i++) { $self = Get-CimInstance Win32_Process -Filter ('ProcessId=' + $current); if (-not $self -or -not $self.ParentProcessId) { break }; $parent = Get-CimInstance Win32_Process -Filter ('ProcessId=' + $self.ParentProcessId); if (-not $parent) { break }; $path = $parent.ExecutablePath; if ($path) { $name = [IO.Path]::GetFileName($path).ToLowerInvariant(); if (($skip -notcontains $name) -and $name.StartsWith('hict') -and $name.EndsWith('.exe')) { Split-Path -Parent $path; break } }; $current = $parent.ProcessId }" 2^>nul`) do set "DATA_DIR=%%I"
)
if defined DATA_DIR (
  if not exist "%DATA_DIR%\" set "DATA_DIR="
)
if not defined DATA_DIR (
  set "DATA_DIR=%APP_HOME%"
)
if not exist "%APP_HOME%\HiCT.cmd" (
  echo HiCT portable payload is incomplete: "%APP_HOME%\HiCT.cmd" was not found.
  exit /b 1
)
call "%APP_HOME%\HiCT.cmd" %*
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

DATA_DIR defaults to this extracted portable directory when running HiCT.cmd
directly. The optional SFX EXE wrapper sets DATA_DIR to the directory containing
the EXE when it can infer that location from the parent process. The launcher
enters DATA_DIR before Java starts so file dialogs and relative paths begin from
the portable data location. Explicit DATA_DIR always wins.

HICT_BIND_HOST defaults to 127.0.0.1 in this portable launcher. Set
HICT_BIND_HOST=0.0.0.0 explicitly if remote machines must connect to this HiCT
server.

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

  $sfxBuildDir = Join-Path $DistRoot "windows-sfx"
  if (Test-Path $sfxBuildDir) {
    Remove-Item -Recurse -Force $sfxBuildDir
  }
  New-Item -ItemType Directory -Force -Path $sfxBuildDir | Out-Null

  $sfxCandidates = @(Find-SfxModules -Roots @($SevenZipRoot))

  $hasNonInstallerSfx = @(($sfxCandidates | Where-Object {
    $name = Split-Path -Leaf $_
    $name -in @("7zS2con.sfx", "7zS2.sfx")
  })).Count -gt 0

  if (-not $sfxCandidates -or -not $hasNonInstallerSfx) {
    $sdkArchive = Join-Path $sfxBuildDir "lzma-sdk.7z"
    $sdkDir = Join-Path $sfxBuildDir "lzma-sdk"
    try {
      Write-Host "Downloading official LZMA SDK from $SevenZipSdkUrl"
      Invoke-WebRequest -Uri $SevenZipSdkUrl -OutFile $sdkArchive
      Invoke-Native -FilePath $sevenZipExe -Arguments @("x", $sdkArchive, "-o$sdkDir", "-y")
      $sdkNotice = Join-Path $sdkDir "DOC\lzma-sdk.txt"
      if (Test-Path $sdkNotice) {
        Copy-Item -Force $sdkNotice (Join-Path $appDir "licenses\LZMA_SDK_NOTICE.txt")
      }
      $sdkSfxCandidates = @(Find-SfxModules -Roots @($sdkDir))
      $sfxCandidates = @($sdkSfxCandidates + $sfxCandidates)
    } catch {
      if (-not $sfxCandidates) {
        throw
      }
      Write-Warning "Could not obtain the official non-installer LZMA SFX module; falling back to the locally available installer SFX module, which may trigger Windows UAC."
    }
  }
  if (-not $sfxCandidates) {
    throw "No official 7-Zip/LZMA SDK SFX module (7zS2con.sfx, 7zS2.sfx, 7zSD.sfx, or 7zS.sfx) was found."
  }
  $sfxModule = $sfxCandidates[0]
  $sfxModuleName = (Split-Path -Leaf $sfxModule)
  $sfxUsesInstallerConfig = $sfxModuleName -notin @("7zS2con.sfx", "7zS2.sfx")
  Write-Host "Using official SFX module $sfxModule"

  $archivePath = Join-Path $sfxBuildDir "payload.7z"
  $configPath = Join-Path $sfxBuildDir "config.txt"
  $exePath = Join-Path $artifactDir "$appName-$version-$platform.exe"

  Push-Location $appDir
  try {
    Invoke-Native -FilePath $sevenZipExe -Arguments @("a", "-t7z", "-mx=7", "-mmt=on", "-ms=off", $archivePath, ".\*")
  }
  finally {
    Pop-Location
  }

  if ($sfxUsesInstallerConfig) {
    @'
;!@Install@!UTF-8!
Title="HiCT Portable"
Directory=""
RunProgram="cmd.exe /d /k call run.cmd"
;!@InstallEnd@!
'@ | Set-Content -Encoding UTF8 $configPath
  }

  if (Test-Path $exePath) {
    Remove-Item -Force $exePath
  }
  $out = [System.IO.File]::Create($exePath)
  try {
    $sfxParts = if ($sfxUsesInstallerConfig) {
      @($sfxModule, $configPath, $archivePath)
    } else {
      @($sfxModule, $archivePath)
    }
    foreach ($part in $sfxParts) {
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
