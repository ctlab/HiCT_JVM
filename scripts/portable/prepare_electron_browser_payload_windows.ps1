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
  $OutputDir = Join-Path $projectDir "browsers-dist\$Platform\electron"
}
if (-not $WorkRoot) {
  $WorkRoot = Join-Path $projectDir "build\electron-browser"
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

Require-Command git
Require-Command node
Require-Command npm.cmd

$resolvedWebUiDir = Resolve-WebUiSource
Write-Host "[electron-browser] Using HiCT_WebUI source: $resolvedWebUiDir"
Write-Host "[electron-browser] Writing payload to: $OutputDir"

Push-Location $resolvedWebUiDir
try {
  Remove-Item Env:ELECTRON_SKIP_BINARY_DOWNLOAD -ErrorAction SilentlyContinue
  Remove-Item Env:ELECTRON_OVERRIDE_DIST_PATH -ErrorAction SilentlyContinue
  Remove-Item Env:npm_config_electron_skip_binary_download -ErrorAction SilentlyContinue
  Remove-Item Env:npm_config_ELECTRON_SKIP_BINARY_DOWNLOAD -ErrorAction SilentlyContinue
  Remove-Item Env:force_no_cache -ErrorAction SilentlyContinue
  if (-not $env:HICT_ELECTRON_CACHE_DIR) {
    $env:HICT_ELECTRON_CACHE_DIR = Join-Path $projectDir "build\electron-cache"
  }
  $env:electron_config_cache = Join-Path $env:HICT_ELECTRON_CACHE_DIR $Platform
  New-Item -ItemType Directory -Force -Path $env:electron_config_cache | Out-Null

  if ($SkipNpmInstall) {
    Write-Host "[electron-browser] Reusing existing HiCT_WebUI node_modules."
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

  & npm.cmd run build
  if ($LASTEXITCODE -ne 0) {
    throw "HiCT_WebUI build failed."
  }
} finally {
  Pop-Location
}

& node (Join-Path $resolvedWebUiDir "scripts\build-electron-browser-payload.mjs") --platform $Platform --output $OutputDir
if ($LASTEXITCODE -ne 0) {
  throw "Electron browser payload build failed."
}
