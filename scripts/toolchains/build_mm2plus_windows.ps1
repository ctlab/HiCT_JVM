param(
  [string]$OutputDir = $(if ($env:OUTPUT_DIR) { $env:OUTPUT_DIR } else { (Join-Path $PSScriptRoot "..\..\toolchains-dist\windows_x86_64") }),
  [string]$WorkDir = $(if ($env:WORK_DIR) { $env:WORK_DIR } else { (Join-Path $env:TEMP "mm2plus-build-windows-x86_64") }),
  [string]$Mm2PlusRef = $(if ($env:MM2PLUS_REF) { $env:MM2PLUS_REF } else { "v1.2" })
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

function Start-NativeProcess([string]$FilePath, [string[]]$Arguments, [string]$WorkingDirectory = $PWD.Path) {
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
  return $process
}

function Invoke-Native([string]$FilePath, [string[]]$Arguments, [string]$WorkingDirectory = $PWD.Path) {
  $process = Start-NativeProcess -FilePath $FilePath -Arguments $Arguments -WorkingDirectory $WorkingDirectory
  if ($process.ExitCode -ne 0) {
    throw "Command failed with exit code $($process.ExitCode): $FilePath $($Arguments -join ' ')"
  }
}

function Invoke-NativeAllowFailure([string]$FilePath, [string[]]$Arguments, [string]$WorkingDirectory = $PWD.Path) {
  $process = Start-NativeProcess -FilePath $FilePath -Arguments $Arguments -WorkingDirectory $WorkingDirectory
  return $process.ExitCode
}

function Repair-Mm2PlusMingwGettimeofday([string]$SourceDir) {
  $miscPath = Join-Path $SourceDir "src\misc.c"
  if (-not (Test-Path $miscPath)) {
    return
  }

  $content = Get-Content -Raw -Path $miscPath
  if ($content -match "__MINGW32__" -or $content -match "__MINGW64__") {
    return
  }
  if ($content -notmatch "struct timezone" -or $content -notmatch "int gettimeofday") {
    return
  }

  $patched = $content -replace "(#include <windows\.h>\r?\n\r?\n)(struct timezone)", "`$1#if !defined(__MINGW32__) && !defined(__MINGW64__)`n`$2"
  $patched = $patched -replace "(return 0;\r?\n}\r?\n)(\r?\n// taken from https://stackoverflow\.com/questions/5272470/c-get-cpu-usage-on-linux-and-windows)", "`$1#endif /* !MinGW gettimeofday */`$2"
  if ($patched -ne $content) {
    Set-Content -Encoding UTF8 -NoNewline -Path $miscPath -Value $patched
    Write-Host "[mm2plus/windows] Patched src/misc.c to use MinGW's gettimeofday/timezone definitions."
  } else {
    Write-Warning "Could not apply MinGW gettimeofday compatibility patch to src/misc.c; attempting build without patch."
  }
}

function Repair-Mm2PlusMakefileExtraFlags([string]$SourceDir) {
  $makefilePath = Join-Path $SourceDir "Makefile"
  if (-not (Test-Path $makefilePath)) {
    return
  }

  $content = Get-Content -Raw -Path $makefilePath
  $old = '$(CXX) -c $(CPPFLAGS) $(INCLUDES) $< -o $@'
  $new = '$(CXX) -c $(CPPFLAGS) $(EXTRAFLAGS) $(INCLUDES) $< -o $@'
  $count = ([regex]::Matches($content, [regex]::Escape($old))).Count
  if ($count -gt 0) {
    Set-Content -Encoding UTF8 -NoNewline -Path $makefilePath -Value $content.Replace($old, $new)
    Write-Host "[mm2plus/windows] Patched $count Makefile generic object rule(s) to include EXTRAFLAGS."
  } elseif ($content.Contains($new)) {
    Write-Host "[mm2plus/windows] Makefile generic object rules already include EXTRAFLAGS."
  } else {
    Write-Warning "Could not patch mm2-plus Makefile generic object rule; upstream Makefile layout may have changed."
  }
}

$repoUrl = if ($env:MM2PLUS_REPO_URL) { $env:MM2PLUS_REPO_URL } else { "https://github.com/at-cg/mm2-plus.git" }
$make = Resolve-CommandPath @("mingw32-make.exe", "mingw32-make", "make.exe", "make")
$cxx = Resolve-CommandPath @("x86_64-w64-mingw32-g++.exe", "x86_64-w64-mingw32-g++", "g++.exe", "g++")
if (-not $make -or -not $cxx) {
  throw "Building mm2-plus on Windows requires MinGW-w64 g++ and make/mingw32-make on PATH."
}

Write-Host "[mm2plus/windows] Building $Mm2PlusRef into $OutputDir"
New-Item -ItemType Directory -Force -Path $WorkDir, (Join-Path $OutputDir "bin"), (Join-Path $OutputDir "share\licenses\mm2plus"), (Join-Path $OutputDir "share\doc\mm2plus") | Out-Null
$buildInfoPath = Join-Path $OutputDir "share\doc\mm2plus\build-info.txt"

$sourceDir = Join-Path $WorkDir "src"
if (-not (Test-Path (Join-Path $sourceDir ".git"))) {
  if (Test-Path $sourceDir) {
    Remove-Item -Recurse -Force $sourceDir
  }
  Invoke-Native -FilePath "git" -Arguments @("clone", "--filter=blob:none", $repoUrl, $sourceDir)
}

try {
  Invoke-Native -FilePath "git" -Arguments @("-C", $sourceDir, "fetch", "--tags", "--force", "origin", $Mm2PlusRef)
} catch {
  Invoke-Native -FilePath "git" -Arguments @("-C", $sourceDir, "fetch", "--tags", "--force", "origin")
}
Invoke-Native -FilePath "git" -Arguments @("-C", $sourceDir, "checkout", "--force", $Mm2PlusRef)
Repair-Mm2PlusMingwGettimeofday -SourceDir $sourceDir
Repair-Mm2PlusMakefileExtraFlags -SourceDir $sourceDir

function Build-Variant([string]$Variant, [string]$ExtraFlags) {
  $output = Join-Path $OutputDir "bin\mm2plus-$Variant.exe"
  if ((Test-Path $output) -and ($env:HICT_REBUILD_MM2PLUS -ne "1")) {
    $sameRef = $false
    if (Test-Path $buildInfoPath) {
      $sameRef = (Select-String -Path $buildInfoPath -Pattern "^ref=$([regex]::Escape($Mm2PlusRef))$" -Quiet)
    }
    if ($sameRef) {
      Write-Host "[mm2plus/windows] Reusing existing $output"
      return $true
    }
    Write-Host "[mm2plus/windows] Existing $output was built for a different or unknown ref; rebuilding."
  }

  try {
    Invoke-Native -FilePath $make -Arguments @("-C", $sourceDir, "clean")
  } catch {
    Write-Warning "mm2-plus clean failed, continuing with a fresh build attempt: $($_.Exception.Message)"
  }

  $jobs = if ($env:NUMBER_OF_PROCESSORS) { $env:NUMBER_OF_PROCESSORS } else { "2" }
  Write-Host "[mm2plus/windows] Compiling $Variant with EXTRAFLAGS=$ExtraFlags"
  $exit = Invoke-NativeAllowFailure -FilePath $make -Arguments @(
    "-C",
    $sourceDir,
    "-j$jobs",
    "base=1",
    "avx=1",
    "CXX=$cxx",
    "EXTRAFLAGS=$ExtraFlags",
    "LDFLAGS=-static -static-libgcc -static-libstdc++",
    "LIBS=-lz -fopenmp -lm -lpthread"
  )
  if ($exit -ne 0 -or -not (Test-Path (Join-Path $sourceDir "mm2plus.exe"))) {
    Write-Host "::warning::mm2-plus $Variant build failed; dotplot generation can still use minimap2 or another mm2-plus variant."
    Remove-Item -Force $output -ErrorAction SilentlyContinue
    return $false
  }

  Copy-Item -Force (Join-Path $sourceDir "mm2plus.exe") $output
  return $true
}

$builtAvx2 = Build-Variant -Variant "avx2" -ExtraFlags "-mavx2"
$builtAvx512 = Build-Variant -Variant "avx512" -ExtraFlags "-mavx512f -mavx512dq -mavx512bw -mavx512vl -mavx2"

foreach ($licenseName in @("LICENSE.txt", "LICENSE")) {
  $license = Join-Path $sourceDir $licenseName
  if (Test-Path $license) {
    Copy-Item -Force $license (Join-Path $OutputDir "share\licenses\mm2plus\$licenseName")
    break
  }
}
if (Test-Path (Join-Path $sourceDir "README.md")) {
  Copy-Item -Force (Join-Path $sourceDir "README.md") (Join-Path $OutputDir "share\doc\mm2plus\README.md")
}

$buildInfo = @(
  "project=mm2-plus",
  "repository=$repoUrl",
  "ref=$Mm2PlusRef",
  "platform=windows_x86_64",
  "compiler=$(& $cxx --version | Select-Object -First 1)",
  "avx2_extraflags=-mavx2",
  "avx512_extraflags=-mavx512f -mavx512dq -mavx512bw -mavx512vl -mavx2",
  "cpu_flag_policy=EXTRAFLAGS are applied to generic and AVX/OpenMP objects; upstream SSE2/SSE4.1 dispatch objects intentionally retain their fixed target flags.",
  "timestamp_utc=$([DateTime]::UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ"))"
)
foreach ($variant in @("avx2", "avx512")) {
  $binary = Join-Path $OutputDir "bin\mm2plus-$variant.exe"
  if (Test-Path $binary) {
    $buildInfo += ""
    $buildInfo += "[$variant version]"
    if ($variant -eq "avx512") {
      $buildInfo += "version check skipped to avoid executing AVX-512 code on runners without AVX-512 support"
    } else {
      try {
        $buildInfo += (& $binary --version 2>&1)
      } catch {
        $buildInfo += "version check failed: $($_.Exception.Message)"
      }
    }
  }
}
$buildInfo | Set-Content -Encoding UTF8 $buildInfoPath

$commands = [ordered]@{}
$files = New-Object System.Collections.Generic.List[string]
$notices = New-Object System.Collections.Generic.List[string]
$citations = New-Object System.Collections.Generic.List[string]
$limitations = New-Object System.Collections.Generic.List[string]

function Add-FileIfPresent([string]$Relative) {
  if (Test-Path (Join-Path $OutputDir $Relative)) {
    $normalized = $Relative.Replace("\", "/")
    if (-not $files.Contains($normalized)) {
      [void]$files.Add($normalized)
    }
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
  [void]$notices.Add("This HiCT build bundles an official minimap2 source build.")
  [void]$notices.Add("HiCT self-dotplot generation can use minimap2 for self-alignment and integrated Java/native PAF/BG2 conversion; Python and Cooler are not required.")
  [void]$notices.Add("minimap2 is redistributed under its MIT license. Keep the bundled license and citation files with released artifacts.")
  [void]$citations.Add("minimap2: Li H. Minimap2: pairwise alignment for nucleotide sequences. Bioinformatics. 2018;34(18):3094-3100.")
}

if (Test-Path (Join-Path $OutputDir "bin\mm2plus-avx2.exe")) {
  $commands["mm2plus_avx2"] = "bin/mm2plus-avx2.exe"
  Add-FileIfPresent "bin\mm2plus-avx2.exe"
}
if (Test-Path (Join-Path $OutputDir "bin\mm2plus-avx512.exe")) {
  $commands["mm2plus_avx512"] = "bin/mm2plus-avx512.exe"
  Add-FileIfPresent "bin\mm2plus-avx512.exe"
}
if ($commands.Contains("mm2plus_avx2") -or $commands.Contains("mm2plus_avx512")) {
  foreach ($relative in @("share\licenses\mm2plus\LICENSE.txt", "share\licenses\mm2plus\LICENSE", "share\doc\mm2plus\README.md", "share\doc\mm2plus\build-info.txt")) {
    Add-FileIfPresent $relative
  }
  [void]$notices.Add("This HiCT build bundles mm2-plus source builds produced from $Mm2PlusRef.")
  [void]$notices.Add("HiCT self-dotplot generation can use mm2-plus as an accelerated minimap2-compatible aligner when selected.")
  [void]$notices.Add("mm2-plus is redistributed with its upstream license file. Keep the bundled license and citation files with released artifacts.")
  [void]$citations.Add("mm2-plus: Ghanshyam Chandra, Md Vasimuddin, Sanchit Misra and Chirag Jain. Accelerating whole-genome alignment in the age of complete genome assemblies. bioRxiv 2024. doi:10.1101/2024.11.25.625328.")
} else {
  [void]$limitations.Add("No mm2-plus executable was built; HiCT self-dotplot generation will fall back to minimap2 when available.")
}

[void]$limitations.Add("Windows builds should be produced on Windows and bundled only into Windows fat-JAR releases.")
[void]$limitations.Add("The mm2-plus Windows payload is built with MinGW-w64 and static linker flags when supported by the runner toolchain.")

$manifest = [ordered]@{
  id = "hict-toolchain-windows-x86_64-hictk-minimap2-mm2plus-$Mm2PlusRef"
  commands = $commands
  files = @($files)
  notices = @($notices)
  citations = @($citations)
  limitations = @($limitations)
}
$manifest | ConvertTo-Json -Depth 6 | Set-Content -Encoding UTF8 (Join-Path $OutputDir "manifest.json")

if (-not $builtAvx2 -and -not $builtAvx512) {
  throw "No mm2-plus variant was built."
}

Write-Host "[mm2plus/windows] Bundled mm2-plus payload prepared at $OutputDir"
