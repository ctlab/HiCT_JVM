param(
  [string]$OutputDir = $(if ($env:OUTPUT_DIR) { $env:OUTPUT_DIR } else { (Join-Path $PSScriptRoot "..\..\toolchains-dist\windows_x86_64") }),
  [string]$WorkDir = $(if ($env:WORK_DIR) { $env:WORK_DIR } else { (Join-Path $env:TEMP "minimap2-build-windows-x86_64") }),
  [string]$Minimap2Ref = $(if ($env:MINIMAP2_REF) { $env:MINIMAP2_REF } else { "v2.31" })
)

$ErrorActionPreference = "Stop"

function Resolve-CommandPath([string[]]$Names) {
  foreach ($name in $Names) {
    $cmd = Get-Command $name -ErrorAction SilentlyContinue
    if ($cmd) {
      return $cmd.Source
    }
  }
  return $null
}

function Format-NativeArgumentForLog([string]$Argument) {
  if ($Argument -match "\s") {
    return '"' + $Argument.Replace('"', '\"') + '"'
  }
  return $Argument
}

function Invoke-Native([string]$FilePath, [string[]]$Arguments, [string]$WorkingDirectory = $PWD.Path) {
  $argumentText = ($Arguments | ForEach-Object { Format-NativeArgumentForLog $_ }) -join " "
  Write-Host "> $FilePath $argumentText"
  $startInfo = [System.Diagnostics.ProcessStartInfo]::new()
  $startInfo.FileName = $FilePath
  $startInfo.WorkingDirectory = $WorkingDirectory
  $startInfo.UseShellExecute = $false
  foreach ($argument in $Arguments) {
    [void]$startInfo.ArgumentList.Add($argument)
  }
  $process = [System.Diagnostics.Process]::Start($startInfo)
  $process.WaitForExit()
  if ($process.ExitCode -ne 0) {
    throw "Command failed with exit code $($process.ExitCode): $FilePath $($Arguments -join ' ')"
  }
}

function Resolve-MingwToolPrefix([string]$CompilerPath) {
  $leaf = Split-Path -Leaf $CompilerPath
  if ($leaf -match "^(.*-)gcc(?:\.exe)?$") {
    return $Matches[1]
  }
  return ""
}

function Find-MingwZlib([string]$CompilerPath) {
  $isNonWindowsPowerShell = (Get-Variable -Name IsWindows -ErrorAction SilentlyContinue) -and (-not $IsWindows)
  $candidates = New-Object System.Collections.Generic.List[string]
  foreach ($envName in @("MINGW_PREFIX", "MSYSTEM_PREFIX")) {
    $value = [Environment]::GetEnvironmentVariable($envName)
    if ($value) {
      [void]$candidates.Add($value)
    }
  }
  foreach ($prefix in @("C:\mingw64", "C:\msys64\mingw64", "C:\msys64\ucrt64", "/usr/x86_64-w64-mingw32")) {
    [void]$candidates.Add($prefix)
  }
  $compilerDir = Split-Path -Parent $CompilerPath
  if ($compilerDir) {
    [void]$candidates.Add((Join-Path $compilerDir "..\x86_64-w64-mingw32"))
    [void]$candidates.Add((Join-Path $compilerDir ".."))
  }

  foreach ($candidate in $candidates) {
    if (-not $candidate) {
      continue
    }
    if ($isNonWindowsPowerShell -and ($candidate -match "^[A-Za-z]:\\")) {
      continue
    }
    $include = Join-Path $candidate "include"
    $lib = Join-Path $candidate "lib"
    if ((Test-Path (Join-Path $include "zlib.h")) -and ((Test-Path (Join-Path $lib "libz.a")) -or (Test-Path (Join-Path $lib "libz.dll.a")))) {
      return [pscustomobject]@{
        RootDir = $candidate
        IncludeDir = $include
        LibDir = $lib
        Source = "system"
      }
    }
  }
  return $null
}

function Ensure-MingwZlib([string]$WorkDir, [string]$MakePath, [string]$CompilerPath) {
  $existing = Find-MingwZlib -CompilerPath $CompilerPath
  if ($existing) {
    Write-Host "[minimap2/windows] Using MinGW zlib from $($existing.RootDir)"
    return $existing
  }

  $zlibRef = if ($env:ZLIB_REF) { $env:ZLIB_REF } else { "v1.3.1" }
  $zlibRepo = if ($env:ZLIB_REPO_URL) { $env:ZLIB_REPO_URL } else { "https://github.com/madler/zlib.git" }
  $sourceDir = Join-Path $WorkDir "zlib-src"
  $installDir = Join-Path $WorkDir "zlib-mingw"
  if (-not (Test-Path (Join-Path $sourceDir ".git"))) {
    if (Test-Path $sourceDir) {
      Remove-Item -Recurse -Force $sourceDir
    }
    Invoke-Native -FilePath "git" -Arguments @("clone", "--depth", "1", "--branch", $zlibRef, $zlibRepo, $sourceDir)
  }
  $prefix = Resolve-MingwToolPrefix -CompilerPath $CompilerPath
  try {
    Invoke-Native -FilePath $MakePath -Arguments @("-C", $sourceDir, "-f", "win32/Makefile.gcc", "clean")
  } catch {
    Write-Warning "zlib clean failed, continuing with a fresh build attempt: $($_.Exception.Message)"
  }
  $jobs = if ($env:NUMBER_OF_PROCESSORS) { $env:NUMBER_OF_PROCESSORS } else { "2" }
  Invoke-Native -FilePath $MakePath -Arguments @("-C", $sourceDir, "-f", "win32/Makefile.gcc", "-j$jobs", "PREFIX=$prefix")
  New-Item -ItemType Directory -Force -Path (Join-Path $installDir "include"), (Join-Path $installDir "lib") | Out-Null
  Copy-Item -Force (Join-Path $sourceDir "zlib.h") (Join-Path $installDir "include\zlib.h")
  Copy-Item -Force (Join-Path $sourceDir "zconf.h") (Join-Path $installDir "include\zconf.h")
  Copy-Item -Force (Join-Path $sourceDir "libz.a") (Join-Path $installDir "lib\libz.a")
  Write-Host "[minimap2/windows] Built static MinGW zlib $zlibRef into $installDir"
  return [pscustomobject]@{
    RootDir = $installDir
    IncludeDir = Join-Path $installDir "include"
    LibDir = Join-Path $installDir "lib"
    Source = "built:$zlibRef"
  }
}

function Repair-Minimap2MingwGettimeofday([string]$SourceDir) {
  $miscPath = Join-Path $SourceDir "misc.c"
  if (-not (Test-Path $miscPath)) {
    return
  }

  $content = Get-Content -Raw -Path $miscPath
  $windowsHelper = @"
#if defined(_WIN32)
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
static int hict_mingw_gettimeofday(struct timeval *tp, void *tzp)
{
    (void)tzp;
    FILETIME ft;
    ULARGE_INTEGER uli;
    GetSystemTimeAsFileTime(&ft);
    uli.LowPart = ft.dwLowDateTime;
    uli.HighPart = ft.dwHighDateTime;
    const unsigned long long epoch = 116444736000000000ULL;
    const unsigned long long micros = (uli.QuadPart - epoch) / 10ULL;
    tp->tv_sec = (time_t)(micros / 1000000ULL);
    tp->tv_usec = (long)(micros % 1000000ULL);
    return 0;
}
#endif
"@

  function Replace-GettimeofdayCall([string]$Text) {
    return [regex]::Replace(
      $Text,
      "\bgettimeofday\s*\(\s*&tp\s*,\s*NULL\s*\)",
      "hict_mingw_gettimeofday(&tp, NULL)"
    )
  }

  if ($content -notmatch "struct timezone" -or $content -notmatch "int gettimeofday") {
    if ($content -match "hict_mingw_gettimeofday") {
      $withoutMacro = [regex]::Replace(
        $content,
        "(?m)^\s*#define\s+gettimeofday\s*\(\s*tp\s*,\s*tzp\s*\)\s+hict_mingw_gettimeofday\s*\(\s*\(tp\)\s*,\s*NULL\s*\)\s*\r?\n?",
        ""
      )
      $patchedExisting = Replace-GettimeofdayCall $withoutMacro
      if ($patchedExisting -ne $content) {
        Set-Content -Encoding UTF8 -NoNewline -Path $miscPath -Value $patchedExisting
        Write-Host "[minimap2/windows] Normalized existing private MinGW gettimeofday shim."
      }
      return
    }

    if ($content -match "\bgettimeofday\s*\(\s*&tp\s*,\s*NULL\s*\)") {
      if ($content -match '#include\s+"mmpriv\.h"[^\r\n]*(\r?\n)') {
        $content = [regex]::Replace($content, '(#include\s+"mmpriv\.h"[^\r\n]*\r?\n)', "`$1$windowsHelper`n", 1)
      } elseif ($content -match "#include[^\r\n]*(\r?\n)") {
        $content = [regex]::Replace($content, "(#include[^\r\n]*\r?\n)", "`$1$windowsHelper`n", 1)
      } else {
        $content = "$windowsHelper`n$content"
      }
      $patchedInjected = Replace-GettimeofdayCall $content
      Set-Content -Encoding UTF8 -NoNewline -Path $miscPath -Value $patchedInjected
      Write-Host "[minimap2/windows] Injected a private Win32 gettimeofday shim for MinGW."
    }
    return
  }

  $patched = $content -replace "\bstruct\s+timezone\b(?=\s*\{)", "struct hict_mingw_timezone"
  $patched = $patched -replace "int\s+gettimeofday\s*\(\s*struct\s+timeval\s*\*\s*tp\s*,\s*struct\s+timezone\s*\*\s*tzp\s*\)", "static int hict_mingw_gettimeofday(struct timeval * tp, struct hict_mingw_timezone *tzp)"
  $patched = Replace-GettimeofdayCall $patched
  if ($patched -ne $content) {
    Set-Content -Encoding UTF8 -NoNewline -Path $miscPath -Value $patched
    Write-Host "[minimap2/windows] Patched misc.c to call a private MinGW gettimeofday shim."
  } else {
    Write-Warning "Could not apply MinGW gettimeofday compatibility patch to misc.c; attempting build without patch."
  }
}

$repoUrl = if ($env:MINIMAP2_REPO_URL) { $env:MINIMAP2_REPO_URL } else { "https://github.com/lh3/minimap2.git" }
$make = Resolve-CommandPath @("mingw32-make.exe", "mingw32-make", "make.exe", "make")
$gcc = Resolve-CommandPath @("x86_64-w64-mingw32-gcc.exe", "x86_64-w64-mingw32-gcc", "gcc.exe", "gcc")
if (-not $make -or -not $gcc) {
  throw "Building minimap2 on Windows requires MinGW-w64 gcc and make/mingw32-make on PATH."
}

Write-Host "[minimap2/windows] Building $Minimap2Ref into $OutputDir"
New-Item -ItemType Directory -Force -Path $WorkDir, (Join-Path $OutputDir "bin"), (Join-Path $OutputDir "share\licenses\minimap2"), (Join-Path $OutputDir "share\doc\minimap2") | Out-Null

$sourceDir = Join-Path $WorkDir "src"
if (-not (Test-Path (Join-Path $sourceDir ".git"))) {
  if (Test-Path $sourceDir) {
    Remove-Item -Recurse -Force $sourceDir
  }
  Invoke-Native -FilePath "git" -Arguments @("clone", "--filter=blob:none", $repoUrl, $sourceDir)
}

Invoke-Native -FilePath "git" -Arguments @("-C", $sourceDir, "fetch", "--tags", "--force", "origin", $Minimap2Ref)
Invoke-Native -FilePath "git" -Arguments @("-C", $sourceDir, "checkout", "--force", $Minimap2Ref)
Repair-Minimap2MingwGettimeofday -SourceDir $sourceDir
try {
  Invoke-Native -FilePath $make -Arguments @("-C", $sourceDir, "clean")
}
catch {
  Write-Warning "minimap2 clean failed, continuing with a fresh build attempt: $($_.Exception.Message)"
}
$jobs = if ($env:NUMBER_OF_PROCESSORS) { $env:NUMBER_OF_PROCESSORS } else { "2" }
$zlib = Ensure-MingwZlib -WorkDir $WorkDir -MakePath $make -CompilerPath $gcc
Invoke-Native -FilePath $make -Arguments @(
  "-C",
  $sourceDir,
  "-j$jobs",
  "CC=$gcc",
  "CFLAGS=-O3 -DNDEBUG -I$($zlib.IncludeDir)",
  "LIBS=-L$($zlib.LibDir) -static -lm -lz -lpthread"
)

$builtExe = Join-Path $sourceDir "minimap2.exe"
if (-not (Test-Path $builtExe)) {
  throw "minimap2.exe was not produced by the MinGW build."
}
Copy-Item -Force $builtExe (Join-Path $OutputDir "bin\minimap2.exe")

foreach ($licenseName in @("LICENSE.txt", "LICENSE")) {
  $license = Join-Path $sourceDir $licenseName
  if (Test-Path $license) {
    Copy-Item -Force $license (Join-Path $OutputDir "share\licenses\minimap2\$licenseName")
    break
  }
}

@"
project=minimap2
repository=$repoUrl
ref=$Minimap2Ref
platform=windows_x86_64
compiler=$(& $gcc --version | Select-Object -First 1)
linker_flags=-static
cpu_flag_policy=generic official minimap2 build; upstream SSE2/SSE4.1 dispatch objects retain their fixed target flags.
zlib=$($zlib.Source) $($zlib.RootDir)
timestamp_utc=$([DateTime]::UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ"))
"@ | Set-Content -Encoding UTF8 (Join-Path $OutputDir "share\doc\minimap2\build-info.txt")

$commands = [ordered]@{}
$files = New-Object System.Collections.Generic.List[string]
$notices = New-Object System.Collections.Generic.List[string]
$citations = New-Object System.Collections.Generic.List[string]
$limitations = New-Object System.Collections.Generic.List[string]

function Add-FileIfPresent([string]$Relative) {
  if (Test-Path (Join-Path $OutputDir $Relative)) {
    [void]$files.Add($Relative.Replace("\", "/"))
  }
}

if (Test-Path (Join-Path $OutputDir "bin\hictk.exe")) {
  $commands["hictk"] = "bin/hictk.exe"
  foreach ($relative in @("bin\hictk.exe", "share\licenses\hictk\LICENSE", "share\doc\hictk\CITATION.cff", "share\doc\hictk\build-info.txt")) {
    Add-FileIfPresent $relative
  }
  [void]$notices.Add("This HiCT build bundles an official hictk source build.")
  [void]$notices.Add("HiCT performs .hic conversion and .cool/.mcool dotplot loading by invoking the bundled hictk executable; no Python runtime is required for these paths.")
  [void]$notices.Add("hictk is redistributed under its MIT license. Keep the bundled license and citation files with released artifacts.")
  [void]$citations.Add("hictk: Rossini R, Paulsen J. hictk: blazing fast toolkit to work with .hic and .cool files. Bioinformatics. 2024;40(7):btae408. doi:10.1093/bioinformatics/btae408.")
}

if (Test-Path (Join-Path $OutputDir "bin\minimap2.exe")) {
  $commands["minimap2"] = "bin/minimap2.exe"
  foreach ($relative in @("bin\minimap2.exe", "share\licenses\minimap2\LICENSE.txt", "share\licenses\minimap2\LICENSE", "share\doc\minimap2\build-info.txt")) {
    Add-FileIfPresent $relative
  }
  [void]$notices.Add("This HiCT build bundles an official minimap2 source build produced from $Minimap2Ref.")
  [void]$notices.Add("HiCT self-dotplot generation uses minimap2 for self-alignment and built-in Java/native post-processing instead of Python/Cooler.")
  [void]$notices.Add("minimap2 is redistributed under its MIT license. Keep the bundled license and citation files with released artifacts.")
  [void]$citations.Add("minimap2: Li H. Minimap2: pairwise alignment for nucleotide sequences. Bioinformatics. 2018;34(18):3094-3100.")
}

[void]$limitations.Add("Windows builds should be produced on Windows and bundled only into Windows fat-JAR releases.")
[void]$limitations.Add("The minimap2 Windows payload is built with MinGW-w64 and static linker flags when supported by the runner toolchain.")

$manifest = [ordered]@{
  id = "hict-toolchain-windows-x86_64-hictk-minimap2-$Minimap2Ref"
  commands = $commands
  files = @($files)
  notices = @($notices)
  citations = @($citations)
  limitations = @($limitations)
}
$manifest | ConvertTo-Json -Depth 6 | Set-Content -Encoding UTF8 (Join-Path $OutputDir "manifest.json")

Write-Host "[minimap2/windows] Bundled minimap2 payload prepared at $OutputDir"
