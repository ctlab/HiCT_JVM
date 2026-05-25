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

function Repair-Minimap2MingwGettimeofday([string]$SourceDir) {
  $miscPath = Join-Path $SourceDir "misc.c"
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
    Write-Host "[minimap2/windows] Patched misc.c to use MinGW's gettimeofday/timezone definitions."
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
Invoke-Native -FilePath $make -Arguments @("-C", $sourceDir, "-j$jobs", "CC=$gcc", "CFLAGS=-O3 -DNDEBUG", "LIBS=-static -lm -lz -lpthread")

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
