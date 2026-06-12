param(
  [switch]$SkipGradle,
  [switch]$CreateSelfExtractingExe,
  [ValidateSet("custom", "7zip-sfx")]
  [string]$WindowsExeMode = $(if ($env:HICT_WINDOWS_EXE_MODE) { $env:HICT_WINDOWS_EXE_MODE } else { "custom" }),
  [string]$RuntimeModules = $(if ($env:HICT_RUNTIME_MODULES) { $env:HICT_RUNTIME_MODULES } else { "java.se,jdk.charsets,jdk.crypto.ec,jdk.localedata,jdk.management,jdk.unsupported,jdk.zipfs" }),
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
  & $FilePath @Arguments 2>&1 | ForEach-Object { Write-Host $_ }
  if ($LASTEXITCODE -ne 0) {
    throw "Native command failed with exit code ${LASTEXITCODE}: $FilePath $($Arguments -join ' ')"
  }
}

function Invoke-PortableSmoke {
  param(
    [Parameter(Mandatory = $true)][string]$Label,
    [Parameter(Mandatory = $true)][string]$FilePath,
    [string[]]$Arguments = @()
  )

  if (-not (Test-Path $FilePath)) {
    throw "Portable smoke test target is missing: $FilePath"
  }

  $root = Join-Path ([System.IO.Path]::GetTempPath()) ("hict-portable-smoke-" + [System.Guid]::NewGuid().ToString("N"))
  $dataDir = Join-Path $root "data"
  $cacheDir = Join-Path $root "cache"
  $tmpDir = Join-Path $root "tmp"
  New-Item -ItemType Directory -Force -Path $dataDir, $cacheDir, $tmpDir | Out-Null

  try {
    Write-Host "Testing ${Label}: $FilePath $($Arguments -join ' ')"
    $psi = [System.Diagnostics.ProcessStartInfo]::new()
    if ($FilePath.EndsWith(".cmd", [System.StringComparison]::OrdinalIgnoreCase) -or
        $FilePath.EndsWith(".bat", [System.StringComparison]::OrdinalIgnoreCase)) {
      $psi.FileName = "$env:ComSpec"
      [void]$psi.ArgumentList.Add("/d")
      [void]$psi.ArgumentList.Add("/c")
      [void]$psi.ArgumentList.Add("call")
      [void]$psi.ArgumentList.Add($FilePath)
    } else {
      $psi.FileName = $FilePath
    }
    foreach ($argument in $Arguments) {
      [void]$psi.ArgumentList.Add($argument)
    }
    $psi.UseShellExecute = $false
    $psi.RedirectStandardOutput = $true
    $psi.RedirectStandardError = $true
    $psi.Environment["DATA_DIR"] = $dataDir
    $psi.Environment["HICT_PORTABLE_DATA_DIR"] = $dataDir
    $psi.Environment["XDG_CACHE_HOME"] = $cacheDir
    $psi.Environment["TEMP"] = $tmpDir
    $psi.Environment["TMP"] = $tmpDir
    $psi.Environment["HICT_LAUNCHER_MODE"] = "cli"

    $process = [System.Diagnostics.Process]::Start($psi)
    if (-not $process.WaitForExit(600000)) {
      try {
        $process.Kill($true)
      } catch {
        $process.Kill()
      }
      throw "Portable smoke test timed out after 600 seconds: $Label"
    }
    $stdout = $process.StandardOutput.ReadToEnd()
    $stderr = $process.StandardError.ReadToEnd()
    if ($stdout) {
      Write-Host $stdout
    }
    if ($stderr) {
      Write-Host $stderr
    }
    if ($process.ExitCode -ne 0) {
      throw "Portable smoke test failed with exit code $($process.ExitCode): $Label"
    }
  }
  finally {
    Remove-Item -Recurse -Force $root -ErrorAction SilentlyContinue
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
  param([Parameter(Mandatory = $true)][AllowEmptyString()][string[]]$Roots)

  # Prefer the progress-capable official installer SFX modules. The small
  # console modules are kept as a fallback for environments where the installer
  # SFX is unavailable.
  $preferredNames = @("7zSD.sfx", "7zS.sfx", "7zS2con.sfx", "7zS2.sfx")
  $found = New-Object 'System.Collections.Generic.List[string]'

  foreach ($root in $Roots) {
    if ([string]::IsNullOrWhiteSpace($root)) {
      continue
    }
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

function Find-StandaloneSevenZipExtractors {
  param([Parameter(Mandatory = $true)][AllowEmptyString()][string[]]$Roots)

  $found = New-Object 'System.Collections.Generic.List[string]'
  foreach ($root in $Roots) {
    if ([string]::IsNullOrWhiteSpace($root)) {
      continue
    }
    if (-not (Test-Path $root)) {
      continue
    }
    Get-ChildItem -Path $root -Recurse -File -Filter "7zr.exe" -ErrorAction SilentlyContinue |
      ForEach-Object { $found.Add($_.FullName) }
  }
  return $found | Sort-Object { $_ }
}

function Expand-LzmaSdk {
  param(
    [Parameter(Mandatory = $true)][string]$SevenZipExe,
    [Parameter(Mandatory = $true)][string]$BuildDir,
    [Parameter(Mandatory = $true)][string]$NoticeTarget
  )

  $sdkArchive = Join-Path $BuildDir "lzma-sdk.7z"
  $sdkDir = Join-Path $BuildDir "lzma-sdk"
  if (Test-Path $sdkDir) {
    return $sdkDir
  }

  Write-Host "Downloading official LZMA SDK from $SevenZipSdkUrl"
  Invoke-WebRequest -Uri $SevenZipSdkUrl -OutFile $sdkArchive
  Invoke-Native -FilePath $SevenZipExe -Arguments @("x", $sdkArchive, "-o$sdkDir", "-y")
  $sdkNotice = Join-Path $sdkDir "DOC\lzma-sdk.txt"
  if (Test-Path $sdkNotice) {
    Copy-Item -Force $sdkNotice $NoticeTarget
  }
  return $sdkDir
}

function Get-LittleEndianUInt64Bytes {
  param([Parameter(Mandatory = $true)][UInt64]$Value)
  return [BitConverter]::GetBytes($Value)
}

function Build-CustomPortableExe {
  param(
    [Parameter(Mandatory = $true)][string]$SevenZipExe,
    [Parameter(Mandatory = $true)][string]$BuildDir,
    [Parameter(Mandatory = $true)][string]$ExePath,
    [Parameter(Mandatory = $true)][string]$AppDirectory,
    [Parameter(Mandatory = $true)][string]$DistDirectory,
    [Parameter(Mandatory = $true)][string]$AppDirectoryName
  )

  $archivePath = Join-Path $BuildDir "payload.7z"
  $launcherStub = Join-Path $BuildDir "HiCTPortableLauncher.stub.exe"
  $launcherSource = Join-Path $PSScriptRoot "windows_launcher\HiCTPortableLauncher.cpp"
  $lzmaNoticeTarget = Join-Path $AppDirectory "licenses\LZMA_SDK_NOTICE.txt"

  $extractorCandidates = @(Find-StandaloneSevenZipExtractors -Roots @($SevenZipRoot, $BuildDir))
  if (-not $extractorCandidates) {
    $sdkDir = Expand-LzmaSdk -SevenZipExe $SevenZipExe -BuildDir $BuildDir -NoticeTarget $lzmaNoticeTarget
    $extractorCandidates = @(Find-StandaloneSevenZipExtractors -Roots @($sdkDir))
  }
  if (-not $extractorCandidates) {
    throw "No official standalone 7zr.exe was found in 7-Zip or the LZMA SDK."
  }
  $extractorPath = $extractorCandidates[0]

  Push-Location $DistDirectory
  try {
    Invoke-Native -FilePath $SevenZipExe -Arguments @("a", "-t7z", "-mx=9", "-mmt=on", "-ms=off", $archivePath, ".\$AppDirectoryName")
  }
  finally {
    Pop-Location
  }

  $clExe = Get-Command "cl.exe" -ErrorAction SilentlyContinue
  if (-not $clExe) {
    throw "cl.exe is required for the custom Windows portable EXE. Run from a Visual Studio Developer shell or configure MSVC in CI."
  }
  Invoke-Native -FilePath $clExe.Source -Arguments @(
    "/nologo",
    "/std:c++17",
    "/O2",
    "/MT",
    "/EHsc",
    "/DUNICODE",
    "/D_UNICODE",
    "/D_WIN32_WINNT=0x0601",
    "/Fe:$launcherStub",
    $launcherSource,
    "shell32.lib",
    "/link",
    "/SUBSYSTEM:CONSOLE,6.01"
  )

  $launcherManifest = Join-Path $BuildDir "HiCTPortableLauncher.manifest"
  @'
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<assembly xmlns="urn:schemas-microsoft-com:asm.v1" manifestVersion="1.0">
  <assemblyIdentity version="1.0.0.0" processorArchitecture="amd64" name="HiCT.PortableLauncher" type="win32"/>
  <description>HiCT portable launcher</description>
  <trustInfo xmlns="urn:schemas-microsoft-com:asm.v3">
    <security>
      <requestedPrivileges>
        <requestedExecutionLevel level="asInvoker" uiAccess="false"/>
      </requestedPrivileges>
    </security>
  </trustInfo>
</assembly>
'@ | Set-Content -Encoding UTF8 $launcherManifest
  $mtExe = Get-Command "mt.exe" -ErrorAction SilentlyContinue
  if ($mtExe) {
    Invoke-Native -FilePath $mtExe.Source -Arguments @("-nologo", "-manifest", $launcherManifest, "-outputresource:$launcherStub;1")
  } else {
    Write-Warning "mt.exe was not found; custom launcher will be built without an embedded asInvoker manifest."
  }

  $stubBytes = [System.IO.File]::ReadAllBytes($launcherStub)
  $extractorBytes = [System.IO.File]::ReadAllBytes($extractorPath)
  $payloadBytes = [System.IO.File]::ReadAllBytes($archivePath)
  $extractorOffset = [UInt64]$stubBytes.Length
  $payloadOffset = [UInt64]($stubBytes.Length + $extractorBytes.Length)
  $payloadHash = (Get-FileHash -Algorithm SHA256 $archivePath).Hash.ToLowerInvariant()
  $extractorHash = (Get-FileHash -Algorithm SHA256 $extractorPath).Hash.ToLowerInvariant()
  $manifest = [ordered]@{
    format = "hict-portable-launcher-v1"
    appDirName = $AppDirectoryName
    payloadSha256 = $payloadHash
    extractorSha256 = $extractorHash
    extractorOffset = $extractorOffset
    extractorSize = [UInt64]$extractorBytes.Length
    payloadOffset = $payloadOffset
    payloadSize = [UInt64]$payloadBytes.Length
  }
  $manifestJson = $manifest | ConvertTo-Json -Depth 4 -Compress
  $manifestBytes = [System.Text.UTF8Encoding]::new($false).GetBytes($manifestJson)
  $manifestLengthBytes = Get-LittleEndianUInt64Bytes -Value ([UInt64]$manifestBytes.Length)
  $magicBytes = [System.Text.Encoding]::ASCII.GetBytes("HICT-PORTABLE-LAUNCHER-V1")

  if (Test-Path $ExePath) {
    Remove-Item -Force $ExePath
  }
  $out = [System.IO.File]::Create($ExePath)
  try {
    foreach ($bytes in @($stubBytes, $extractorBytes, $payloadBytes, $manifestBytes, $manifestLengthBytes, $magicBytes)) {
      $out.Write($bytes, 0, $bytes.Length)
    }
  }
  finally {
    $out.Dispose()
  }
  Write-Host "Built custom portable EXE with content-addressed cache support: $ExePath"
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
$hasPreparedWindowsToolchain = Test-Path (Join-Path $projectDir "toolchains-dist\windows_x86_64\manifest.json")
if ($hasPreparedWindowsToolchain -and -not ($jarEntries -contains "toolchains/windows_x86_64/manifest.json")) {
  throw "toolchains-dist\windows_x86_64 exists, but the fat JAR does not contain the bundled Windows hictk manifest."
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
$jhdf5NativesArchive = Get-ChildItem -Path (Join-Path $projectDir "build\libs") -Filter "sis-jhdf5-*-natives.tar.gz" -File -ErrorAction SilentlyContinue |
  Sort-Object Name |
  Select-Object -Last 1
if ($jhdf5NativesArchive) {
  Copy-Item -Force $jhdf5NativesArchive.FullName (Join-Path $appDir ("lib\" + $jhdf5NativesArchive.Name))
}
Copy-Item -Force (Join-Path $projectDir "LICENSE") (Join-Path $appDir "licenses\HiCT_JVM_LICENSE")
$webUiLicense = Join-Path $projectDir "..\HiCT_WebUI\LICENSE"
if (Test-Path $webUiLicense) {
  Copy-Item -Force $webUiLicense (Join-Path $appDir "licenses\HiCT_WebUI_LICENSE")
}

Push-Location $appDir
try {
  Invoke-Native -FilePath $jarTool -Arguments @("xf", (Join-Path $appDir "lib\hict.jar"), "webui", "toolchains")
}
finally {
  Pop-Location
}
if ($hasPreparedWindowsToolchain) {
  $toolchainManifest = Join-Path $projectDir "toolchains-dist\windows_x86_64\manifest.json"
  if ((Test-Path $toolchainManifest) -and ((Get-Content -Raw $toolchainManifest) -match '"hictk"')) {
    $bundledHictk = Join-Path $appDir "toolchains\windows_x86_64\bin\hictk.exe"
    if (-not (Test-Path $bundledHictk)) {
      throw "Portable package is missing bundled hictk at toolchains\windows_x86_64\bin\hictk.exe."
    }
    Invoke-Native -FilePath $bundledHictk -Arguments @("--version")
  }
  if ((Test-Path $toolchainManifest) -and ((Get-Content -Raw $toolchainManifest) -match '"minimap2"')) {
    $bundledMinimap2 = Join-Path $appDir "toolchains\windows_x86_64\bin\minimap2.exe"
    if (-not (Test-Path $bundledMinimap2)) {
      throw "Portable package is missing bundled minimap2 at toolchains\windows_x86_64\bin\minimap2.exe."
    }
    Invoke-Native -FilePath $bundledMinimap2 -Arguments @("--version")
  }
  if ((Test-Path $toolchainManifest) -and ((Get-Content -Raw $toolchainManifest) -match '"mm2plus_avx2"')) {
    $bundledMm2PlusAvx2 = Join-Path $appDir "toolchains\windows_x86_64\bin\mm2plus-avx2.exe"
    if (-not (Test-Path $bundledMm2PlusAvx2)) {
      throw "Portable package is missing bundled mm2-plus AVX2 at toolchains\windows_x86_64\bin\mm2plus-avx2.exe."
    }
    Invoke-Native -FilePath $bundledMm2PlusAvx2 -Arguments @("--version")
  }
  if ((Test-Path $toolchainManifest) -and ((Get-Content -Raw $toolchainManifest) -match '"mm2plus_avx512"')) {
    $bundledMm2PlusAvx512 = Join-Path $appDir "toolchains\windows_x86_64\bin\mm2plus-avx512.exe"
    if (-not (Test-Path $bundledMm2PlusAvx512)) {
      throw "Portable package is missing bundled mm2-plus AVX-512 at toolchains\windows_x86_64\bin\mm2plus-avx512.exe."
    }
  }
}

$browserSource = Join-Path $projectDir "browsers-dist\windows_x86_64"
$browserManifests = @()
if (Test-Path $browserSource) {
  $browserManifests = @(Get-ChildItem -Path $browserSource -Recurse -Filter "manifest.json" -File -ErrorAction SilentlyContinue)
}
if ($browserManifests.Count -gt 0) {
  $browserTargetRoot = Join-Path $appDir "browsers"
  $browserTarget = Join-Path $browserTargetRoot "windows_x86_64"
  New-Item -ItemType Directory -Force -Path $browserTargetRoot | Out-Null
  Copy-Item -Recurse -Force $browserSource $browserTargetRoot
  $rootBrowserManifest = Join-Path $browserTarget "manifest.json"
  $childBrowserManifests = @(Get-ChildItem -Path $browserTarget -Recurse -Filter "manifest.json" -File |
    Where-Object { $_.FullName -ne $rootBrowserManifest })
  if ((Test-Path $rootBrowserManifest) -and $childBrowserManifests.Count -gt 0) {
    Remove-Item -Force $rootBrowserManifest
    Remove-Item -Recurse -Force -ErrorAction SilentlyContinue (Join-Path $browserTarget "app")
  }
  foreach ($browserManifestPath in Get-ChildItem -Path $browserTarget -Recurse -Filter "manifest.json" -File) {
    $browserManifest = Get-Content -Raw $browserManifestPath.FullName | ConvertFrom-Json
    if (-not $browserManifest.command) {
      throw "$($browserManifestPath.FullName) must contain a command field."
    }
    $browserCommand = [string]$browserManifest.command
    $browserExecutable = Join-Path $browserManifestPath.DirectoryName ($browserCommand -replace '/', '\')
    if (-not (Test-Path $browserExecutable)) {
      throw "Bundled browser command path does not exist: $browserExecutable"
    }
  }
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
    prepared before packaging. Portable packages also extract the platform
    hictk payload under toolchains\ and set HICT_TOOLCHAIN_DIR at launch time.
    hictk is redistributed under its MIT license and should be cited when .hic
    conversion is used.
  - Optional bundled browser resources under browsers\ when browsers-dist was
    prepared before packaging. HiCT does not download browser binaries during
    packaging; browser payloads must be curated with their upstream license,
    trademark, and update requirements before redistribution.
  - Optional single-file Windows EXE packaging when -CreateSelfExtractingExe is
    used. The default EXE mode is a small HiCT launcher that embeds an official
    7-Zip runtime extractor and reuses a content-addressed cache next to the
    EXE.
    Legacy official 7-Zip/LZMA SDK SFX packaging can be selected explicitly.
    Keep the 7-Zip notice with redistributed artifacts.

The portable Windows ZIP remains the most transparent artifact. The optional
EXE is a single double-clickable launcher without MSI installation.
'@ | Set-Content -Encoding UTF8 (Join-Path $appDir "licenses\PORTABLE_DISTRIBUTION_NOTICE.txt")

@'
7-Zip/LZMA SDK notice
=====================

Single-file Windows EXE artifacts use official 7-Zip/LZMA SDK components when
requested by the release workflow. The default custom launcher embeds the
standalone 7zr.exe extractor and a .7z payload; the legacy mode assembles an
official 7-Zip/LZMA SDK SFX module. The build first checks the local 7-Zip
installation and then downloads the official LZMA SDK when needed.

7-Zip is distributed under LGPL terms with additional components noted by the
upstream project. LZMA SDK is public domain. Keep this notice with redistributed
portable packages and refer to the official 7-Zip license and SDK pages for the
exact license text of the component used by the build runner.
'@ | Set-Content -Encoding UTF8 (Join-Path $appDir "licenses\SevenZip_NOTICE.txt")

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
if not defined PROCESSED_DIR (
  set "PROCESSED_DIR=%DATA_DIR%\processed"
)
if not defined HICT_TEMP_DIR (
  set "HICT_TEMP_DIR=%DATA_DIR%\tmp"
)
set "HICT_APP_HOME=%APP_HOME%"
set "HICT_JAR_PATH=%APP_HOME%\lib\hict.jar"
if not defined HICT_JHDF5_NATIVES_ARCHIVE (
  for %%I in ("%APP_HOME%\lib\sis-jhdf5-*-natives.tar.gz") do if exist "%%~fI" set "HICT_JHDF5_NATIVES_ARCHIVE=%%~fI"
)
if not defined WEBUI_ROOT (
  if exist "%APP_HOME%\webui\index.html" set "WEBUI_ROOT=%APP_HOME%\webui"
)
if not defined HICT_TOOLCHAIN_DIR (
  if exist "%APP_HOME%\toolchains\windows_x86_64\manifest.json" set "HICT_TOOLCHAIN_DIR=%APP_HOME%\toolchains\windows_x86_64"
)
if not defined HICT_BROWSER_DIR (
  if exist "%APP_HOME%\browsers\windows_x86_64" set "HICT_BROWSER_DIR=%APP_HOME%\browsers\windows_x86_64"
)
if not defined HICT_BIND_HOST (
  set "HICT_BIND_HOST=127.0.0.1"
)

call :WarnWebView2IfNeeded
call :SelectRuntimeTempDir
if errorlevel 1 exit /b 1

if not exist "%DATA_DIR%" mkdir "%DATA_DIR%" >nul 2>nul
if not exist "%PROCESSED_DIR%" mkdir "%PROCESSED_DIR%" >nul 2>nul
if not exist "%HICT_TEMP_DIR%" mkdir "%HICT_TEMP_DIR%" >nul 2>nul
pushd "%DATA_DIR%" >nul 2>nul
if errorlevel 1 (
  echo Failed to enter DATA_DIR "%DATA_DIR%".
  exit /b 1
)

set "JAVA_EXE=%APP_HOME%\runtime\bin\java.exe"
if "%~1"=="" (
  if not defined HICT_LAUNCHER_MODE set "HICT_LAUNCHER_MODE=gui"
  "%JAVA_EXE%" "-Djava.io.tmpdir=%HICT_TEMP_DIR%" %HICT_JAVA_OPTS% -jar "%APP_HOME%\lib\hict.jar"
) else (
  "%JAVA_EXE%" "-Djava.io.tmpdir=%HICT_TEMP_DIR%" %HICT_JAVA_OPTS% -jar "%APP_HOME%\lib\hict.jar" %*
)
set "HICT_EXIT_CODE=%ERRORLEVEL%"
popd >nul 2>nul
exit /b %HICT_EXIT_CODE%

:SelectRuntimeTempDir
set "HICT_LOCAL_TEMP_DIR=%DATA_DIR%\tmp"
if defined HICT_TEMP_DIR (
  call :CanUseRuntimeTempDir "%HICT_TEMP_DIR%"
  if not errorlevel 1 exit /b 0
  echo WARNING: Runtime temp candidate is not usable, trying fallback: "%HICT_TEMP_DIR%"
)
call :CanUseRuntimeTempDir "%HICT_LOCAL_TEMP_DIR%"
if not errorlevel 1 (
  set "HICT_TEMP_DIR=%HICT_LOCAL_TEMP_DIR%"
  exit /b 0
)
echo WARNING: Runtime temp candidate is not usable, trying fallback: "%HICT_LOCAL_TEMP_DIR%"
if defined TEMP (
  call :CanUseRuntimeTempDir "%TEMP%\HiCT\runtime"
  if not errorlevel 1 (
    set "HICT_TEMP_DIR=%TEMP%\HiCT\runtime"
    set "HICT_PORTABLE_NOTICE=DATA_DIR\tmp could not be used as the runtime temp directory; using %TEMP%\HiCT\runtime."
    exit /b 0
  )
)
call :CanUseRuntimeTempDir "%APP_HOME%\tmp"
if not errorlevel 1 (
  set "HICT_TEMP_DIR=%APP_HOME%\tmp"
  set "HICT_PORTABLE_NOTICE=DATA_DIR\tmp could not be used as the runtime temp directory; using %APP_HOME%\tmp."
  exit /b 0
)
echo No usable runtime temp directory is available. Check DATA_DIR\tmp, HICT_TEMP_DIR, and %%TEMP%%.
exit /b 1

:CanUseRuntimeTempDir
set "HICT_PROBE_DIR=%~1"
if "%HICT_PROBE_DIR%"=="" exit /b 1
if not exist "%HICT_PROBE_DIR%" mkdir "%HICT_PROBE_DIR%" >nul 2>nul
if not exist "%HICT_PROBE_DIR%" exit /b 1
set "HICT_PROBE=%HICT_PROBE_DIR%\.hict-exec-test-%RANDOM%-%RANDOM%.cmd"
> "%HICT_PROBE%" echo @echo off
>> "%HICT_PROBE%" echo exit /b 0
cmd.exe /d /c "%HICT_PROBE%" >nul 2>nul
set "HICT_PROBE_STATUS=%ERRORLEVEL%"
del /f /q "%HICT_PROBE%" >nul 2>nul
exit /b %HICT_PROBE_STATUS%

:WarnWebView2IfNeeded
if not exist "%APP_HOME%\browsers\windows_x86_64" exit /b 0
findstr /S /I /C:"tauri-system-webview" "%APP_HOME%\browsers\windows_x86_64\manifest.json" "%APP_HOME%\browsers\windows_x86_64\*\manifest.json" >nul 2>nul
if errorlevel 1 exit /b 0
reg query "HKCU\Software\Microsoft\EdgeUpdate\Clients\{F3017226-FE2A-4295-8BDF-00C3A9A7E4C5}" /v pv >nul 2>nul
if not errorlevel 1 exit /b 0
reg query "HKLM\SOFTWARE\Microsoft\EdgeUpdate\Clients\{F3017226-FE2A-4295-8BDF-00C3A9A7E4C5}" /v pv >nul 2>nul
if not errorlevel 1 exit /b 0
reg query "HKLM\SOFTWARE\WOW6432Node\Microsoft\EdgeUpdate\Clients\{F3017226-FE2A-4295-8BDF-00C3A9A7E4C5}" /v pv >nul 2>nul
if not errorlevel 1 exit /b 0
if exist "%ProgramFiles%\Microsoft\EdgeWebView\Application\*\msedgewebview2.exe" exit /b 0
if exist "%ProgramFiles(x86)%\Microsoft\EdgeWebView\Application\*\msedgewebview2.exe" exit /b 0
echo WARNING: HiCT includes the small Tauri WebView browser, but Microsoft Edge WebView2 Runtime was not detected.
echo The launcher will try Tauri first and then fall back to Electron or the system browser if available.
echo Install Microsoft Edge WebView2 Runtime if the bundled Tauri browser does not open:
echo   https://developer.microsoft.com/microsoft-edge/webview2/
exit /b 0
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
  HiCT.cmd launcher
  HiCT.cmd --help
  HiCT.cmd start-server
  HiCT.cmd convert --help

The package includes:
  - HiCT_JVM fat JAR, including the built HiCT_WebUI resources
  - optional split JHDF5 native archive under lib\ when the release uses the
    slim JHDF5 jar packaging
  - extracted HiCT_WebUI assets used as WEBUI_ROOT for robust portable serving
  - extracted bundled hictk payload under toolchains\ when release packaging
    was built with .hic conversion support
  - optional bundled browser payload under browsers\ when browsers-dist was
    prepared before packaging
  - a jlink runtime built from the JDK used by the release runner
  - HiCT license files
  - the runtime\legal directory generated by jlink

With no arguments, HiCT.cmd opens the graphical launcher. Explicit CLI
subcommands keep the traditional command-line behavior.

DATA_DIR defaults to this extracted portable directory when running HiCT.cmd
directly. The optional single-file EXE wrapper sets DATA_DIR to the directory
containing the EXE when possible. The launcher enters DATA_DIR before Java
starts so file dialogs and relative paths begin from the portable data location.
Explicit DATA_DIR always wins.

HICT_BIND_HOST defaults to 127.0.0.1 in this portable launcher. Set
HICT_BIND_HOST=0.0.0.0 explicitly if remote machines must connect to this HiCT
server.

Java runtime notices:
  The embedded runtime keeps its jlink-generated legal\ directory intact. For
  Temurin/OpenJDK builds this includes the OpenJDK GPLv2 + Classpath Exception
  notices and third-party notices shipped with the runtime.

Windows single-file note:
  The ZIP is the transparent portable artifact. If the release also contains a
  .exe, that EXE is a portable launcher around the same app, not an MSI
installer. The default custom EXE reuses a content-addressed cache next to the EXE;
the legacy 7-Zip SFX mode can be selected at build time.
"@ | Set-Content -Encoding UTF8 (Join-Path $appDir "README_PORTABLE.txt")

$zipPath = Join-Path $artifactDir "$appName-$version-$platform-portable.zip"
if (Test-Path $zipPath) {
  Remove-Item -Force $zipPath
}
Compress-Archive -Path $appDir -DestinationPath $zipPath -CompressionLevel Optimal

Invoke-PortableSmoke `
  -Label "portable ZIP run.cmd" `
  -FilePath (Join-Path $appDir "run.cmd") `
  -Arguments @("check-toolchains", "--require-hdf5-native", "--check-available-natives", "--quiet")

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

  $sfxBuildDir = Join-Path $DistRoot "windows-exe"
  if (Test-Path $sfxBuildDir) {
    Remove-Item -Recurse -Force $sfxBuildDir
  }
  New-Item -ItemType Directory -Force -Path $sfxBuildDir | Out-Null

  $exePath = Join-Path $artifactDir "$appName-$version-$platform.exe"

  if ($WindowsExeMode -eq "custom") {
    Build-CustomPortableExe `
      -SevenZipExe $sevenZipExe `
      -BuildDir $sfxBuildDir `
      -ExePath $exePath `
      -AppDirectory $appDir `
      -DistDirectory $DistRoot `
      -AppDirectoryName "$appName-$version-$platform"
  } else {
    $sfxCandidates = @(Find-SfxModules -Roots @($SevenZipRoot))

    $hasProgressSfx = @(($sfxCandidates | Where-Object {
      $name = Split-Path -Leaf $_
      $name -in @("7zSD.sfx", "7zS.sfx")
    })).Count -gt 0

    if (-not $sfxCandidates -or -not $hasProgressSfx) {
      $sdkDir = Expand-LzmaSdk -SevenZipExe $sevenZipExe -BuildDir $sfxBuildDir -NoticeTarget (Join-Path $appDir "licenses\LZMA_SDK_NOTICE.txt")
      $sdkSfxCandidates = @(Find-SfxModules -Roots @($sdkDir))
      $sfxCandidates = @($sdkSfxCandidates + $sfxCandidates)
    }
    if (-not $sfxCandidates) {
      throw "No official 7-Zip/LZMA SDK SFX module (7zSD.sfx, 7zS.sfx, 7zS2con.sfx, or 7zS2.sfx) was found."
    }
    $sfxModule = $sfxCandidates[0]
    $sfxModuleName = (Split-Path -Leaf $sfxModule)
    $sfxUsesInstallerConfig = $sfxModuleName -notin @("7zS2con.sfx", "7zS2.sfx")
    Write-Host "Using official SFX module $sfxModule"

    $archivePath = Join-Path $sfxBuildDir "payload.7z"
    $configPath = Join-Path $sfxBuildDir "config.txt"

    Push-Location $appDir
    try {
      Invoke-Native -FilePath $sevenZipExe -Arguments @("a", "-t7z", "-mx=9", "-mmt=on", "-ms=off", $archivePath, ".\*")
    }
    finally {
      Pop-Location
    }

    if ($sfxUsesInstallerConfig) {
      @'
;!@Install@!UTF-8!
Title="HiCT Portable"
Progress="yes"
Directory=""
RunProgram="cmd.exe /d /c call run.cmd"
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
  }
  $hashLines += "$((Get-FileHash -Algorithm SHA256 $exePath).Hash.ToLowerInvariant())  $(Split-Path -Leaf $exePath)"
}

$hashLines | Set-Content -Encoding ASCII $shaPath

Write-Host "Built $zipPath"
if ($CreateSelfExtractingExe) {
  Write-Host "Built optional portable $WindowsExeMode EXE in $artifactDir"
}
if ($CreateSelfExtractingExe -and (Test-Path $exePath)) {
  if ($WindowsExeMode -eq "custom") {
    Invoke-PortableSmoke `
      -Label "portable EXE" `
      -FilePath $exePath `
      -Arguments @("check-toolchains", "--require-hdf5-native", "--check-available-natives", "--quiet")
  } else {
    Invoke-Native -FilePath $exePath -Arguments @("--help")
  }
}
Write-Host "Wrote $shaPath"
