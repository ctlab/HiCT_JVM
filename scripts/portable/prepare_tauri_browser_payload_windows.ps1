param(
  [string]$WebUiDir = $env:HICT_WEBUI_DIR,
  [string]$WebUiRef = $(if ($env:HICT_WEBUI_REF) { $env:HICT_WEBUI_REF } else { "same-as-jvm" }),
  [string]$WebUiRepoUrl = $(if ($env:HICT_WEBUI_REPO_URL) { $env:HICT_WEBUI_REPO_URL } else { "https://github.com/ctlab/HiCT_WebUI.git" }),
  [string]$Platform = $(if ($env:HICT_BROWSER_PLATFORM) { $env:HICT_BROWSER_PLATFORM } else { "windows_x86_64" }),
  [string]$OutputDir = $env:HICT_BROWSER_OUTPUT_DIR,
  [string]$WorkRoot = $env:HICT_BROWSER_BUILD_ROOT,
  [bool]$SkipNpmInstall = ($env:HICT_SKIP_NPM_INSTALL -eq "1")
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectDir = Resolve-Path (Join-Path $scriptDir "..\..")
if (-not $OutputDir) {
  $OutputDir = Join-Path $projectDir "browsers-dist\$Platform\tauri"
}
if (-not $WorkRoot) {
  $WorkRoot = Join-Path $projectDir "build\tauri-browser"
}

function Require-Command {
  param([string]$Name)
  if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
    throw "Missing required command: $Name"
  }
}

function Get-CurrentJvmRef {
  if ($env:GITHUB_REF_NAME) {
    return $env:GITHUB_REF_NAME
  }
  $branch = (& git -C $projectDir branch --show-current 2>$null)
  if ($LASTEXITCODE -eq 0 -and $branch) {
    return $branch.Trim()
  }
  return "master"
}

function Resolve-WebUiSource {
  if ($WebUiDir) {
    return (Resolve-Path $WebUiDir).Path
  }

  $sibling = Join-Path $projectDir "..\HiCT_WebUI"
  if (Test-Path (Join-Path $sibling "package.json")) {
    return (Resolve-Path $sibling).Path
  }

  $requestedRef = $WebUiRef
  if ($requestedRef -eq "same-as-jvm") {
    $requestedRef = Get-CurrentJvmRef
  }

  $checkoutDir = Join-Path $WorkRoot "HiCT_WebUI"
  if (Test-Path $checkoutDir) {
    Remove-Item -Recurse -Force $checkoutDir
  }
  New-Item -ItemType Directory -Force -Path $WorkRoot | Out-Null
  & git clone --depth 1 --branch $requestedRef $WebUiRepoUrl $checkoutDir
  if ($LASTEXITCODE -eq 0) {
    return $checkoutDir
  }

  if ($requestedRef -ne "master") {
    Write-Warning "HiCT_WebUI ref '$requestedRef' was not found; falling back to master."
    if (Test-Path $checkoutDir) {
      Remove-Item -Recurse -Force $checkoutDir
    }
    & git clone --depth 1 --branch master $WebUiRepoUrl $checkoutDir
    if ($LASTEXITCODE -eq 0) {
      return $checkoutDir
    }
  }

  throw "Could not clone HiCT_WebUI ref '$requestedRef'."
}

Require-Command cargo
Require-Command git
Require-Command node
Require-Command npm.cmd

$resolvedWebUiDir = Resolve-WebUiSource
Write-Host "[tauri-browser] Using HiCT_WebUI source: $resolvedWebUiDir"
Write-Host "[tauri-browser] Writing payload to: $OutputDir"

Push-Location $resolvedWebUiDir
try {
  if ($SkipNpmInstall) {
    Write-Host "[tauri-browser] Reusing existing HiCT_WebUI node_modules."
  } else {
    if (Test-Path "package-lock.json") {
      & npm.cmd ci
    } else {
      & npm.cmd install
    }
    if ($LASTEXITCODE -ne 0) {
      throw "npm dependency installation failed."
    }
  }

  & npm.cmd run build:web
  if ($LASTEXITCODE -ne 0) {
    throw "HiCT_WebUI build failed."
  }
} finally {
  Pop-Location
}

& node (Join-Path $resolvedWebUiDir "scripts\build-tauri-browser-payload.mjs") --platform $Platform --output $OutputDir
if ($LASTEXITCODE -ne 0) {
  throw "Tauri browser payload build failed."
}
