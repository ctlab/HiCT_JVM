#!/usr/bin/env node
/*
 Copyright (c) 2021-2026 Aleksandr Serdiukov, Anton Zamyatin, Aleksandr Sinitsyn, Vitalii Dravgelis and Computer Technologies Laboratory ITMO University team.

 Permission is hereby granted, free of charge, to any person obtaining a copy of
 this software and associated documentation files (the "Software"), to deal in
 the Software without restriction, including without limitation the rights to
 use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 the Software, and to permit persons to whom the Software is furnished to do so,
 subject to the following conditions:

 The above copyright notice and this permission notice shall be included in all
 copies or substantial portions of the Software.

 THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 SOFTWARE.
 */

import { createHash } from "node:crypto";
import { spawnSync } from "node:child_process";
import { chmodSync, cpSync, existsSync, mkdirSync, readFileSync, readdirSync, rmSync, statSync, writeFileSync } from "node:fs";
import { basename, dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const scriptDir = dirname(fileURLToPath(import.meta.url));
const args = parseArgs(process.argv.slice(2));
const repoDir = resolve(args.webuiDir ?? process.env.HICT_WEBUI_DIR ?? join(scriptDir, "..", "..", "..", "HiCT_WebUI"));
const tauriDir = resolve(repoDir, "src-tauri");
const tauriConfigPath = resolve(tauriDir, "tauri.conf.json");
const platform = args.platform ?? detectPlatform();
const outputDir = resolve(args.output ?? join(scriptDir, "..", "..", "browsers-dist", platform, "tauri"));
const executableName = platform === "windows_x86_64" ? "hict-tauri-browser.exe" : "hict-tauri-browser";
const cargoTargetDir = process.env.CARGO_TARGET_DIR ? resolve(process.env.CARGO_TARGET_DIR) : resolve(tauriDir, "target");
const builtExecutable = resolve(cargoTargetDir, "release", executableName);
const generatedIconPngBase64 = [
  "iVBORw0KGgoAAAANSUhEUgAAACAAAAAgCAYAAABzenr0AAAD6ElEQVR4nM2WQWhUVxSGv/vuezOTmcnMaMSaEBJFEUQSNKVoceWqs6oLSxcVunDRRTfqypWbum1JQ0u7sIiU0oWkENw0WagFQagBwXRjcROkVtq0kCBhJibvni7ezeS+926SSSi0PwyEe879//+ce+ZM4D+GOjt9vQZI+hRWZv7g9dwiKgrSoTBoy6pZnZ3+NnX+VvNDVBREsmZK7rmsGqLRBqV33siqJHTAp8AoYDaOFXq4jPr1FZjULS1GJsP+8menzl80P/9wE4BT5y8S1ArB2p+ty8B7QNyhKmrC4TIoBZLiCoC5ELgLvA/UN2wLeqiM7i8R/9YC5ZYkg/Ff7Z/EyOzbH30MwNqLZeR1+01ELgMDG7mg+0vooXJWHGAJGA+AKWAyFRJQlZBobA+qkH4ChAExcino0aV4oUW80CLo0SUxcglxxAFVCBKOSuhr/yQwFQArwBfA81TYCOFwGX24mr9s5Jxpx01pxUgrxrTjJkbOZYvQh6tJ+01O/bnVXFkv7wnwDVmpUFE42UBVcxVUMVxRJd2nSroPwxWg6oqrakjhZANCRQZitZ4ABPeb19YDN4HH2dRgf5HoeC09BwAiZ2TNXJA1cwGRM6mYguh4jWB/0df6x1aL+81ruA/8ApggeZI02UidYF+OTGPkKkauAjplel+RaKSeN51wT1gtIPkq4HRhCpjJdkH12nbqDKMwkB08tH22Xu/gzViNjmZmxHkFjAN/Z03oI1XCIe9AbcAI4VAZfcQzuAnnuNXooGPA6cID4PvsbVUMiMYaqB7tI0861aOTnGK2LrCcDzJa6Q7YQAx8BTzLCujBMuHRXh85AOHRXvRg2WfwmeWMXfGcAQdPga9xVup6dnSiQVCP0iICQT0iOtHwMcaW66lPKJfuOPwOeJgKCgR9BaLRzIQriEbrBH0FX/UPLRfZ6r0GHCwAnwPL2UB4rIY+UErEBPSBEuGxmo9j2XIsbCbiNeA4/RG4kwoKqIomGtuTbLlQ2X3vHc47lsNb/aYGHLRIFsfLrInwUIXwoP0cqvjEX9q7ra0ENjXgOH4E3MolRIrC6b0UTu+FKL/y7J1HGa4cvDddnJ2+DnCQpJ0j3tv56n8B3gXmtxKH7Z9gHfPAl8Bq6lS84qs2d74b4m0NOBXcBu51wXnP5m7Z+q4NOFgk+UotbZGzZHMWuyXtyoBTyV1sdZvgts3pqvquDTjY6n3n8c3Jv2XAqWgOuEHu14AbNtZ19TsykCG+Bcw6oVl7tiPxHRtw8DvJlmvbz4Q92zG2XUQ+2OVUYeMflw+A5Z1WDxDuxoDFMvCJ8/eusKsOQKcLHeym+v8F/gEnK0D2mS+qQgAAAABJRU5ErkJggg=="
].join("");

if (!["linux_x86_64", "windows_x86_64", "darwin_arm64", "darwin_x86_64"].includes(platform)) {
  throw new Error(`Unsupported Tauri browser payload platform: ${platform}`);
}
if (!existsSync(resolve(repoDir, "dist", "index.html"))) {
  throw new Error("HiCT_WebUI dist/index.html was not found. Run npm run build first.");
}
ensureTauriConventionalIcons();
validateTauriIconConfig(platform);
validateTauriCapabilities();
if (!args.skipBuild) {
  run("cargo", ["build", "--release", "--locked", "--manifest-path", resolve(tauriDir, "Cargo.toml")], repoDir);
}
if (!existsSync(builtExecutable)) {
  throw new Error(`Tauri browser executable was not found at ${builtExecutable}`);
}

rmSync(outputDir, { recursive: true, force: true });
mkdirSync(join(outputDir, "bin"), { recursive: true });
mkdirSync(join(outputDir, "licenses"), { recursive: true });

const command = platform === "windows_x86_64" ? "bin/hict-tauri-browser.exe" : "bin/hict-tauri-browser";
cpSync(builtExecutable, join(outputDir, command));
if (platform !== "windows_x86_64") {
  chmodExecutable(join(outputDir, command));
}

writeFileSync(
  join(outputDir, "licenses", "TAURI_BROWSER_NOTICE.txt"),
  [
    "HiCT Tauri browser payload",
    "===========================",
    "",
    "This payload contains a small Rust/Tauri wrapper written by the HiCT team.",
    "It does not bundle Chromium. It uses the operating system WebView runtime:",
    "Microsoft Edge WebView2 on Windows, WebKitGTK on Linux, and WKWebView",
    "from macOS.",
    "",
    "Tauri, WRY, TAO and their Rust dependencies are redistributed as compiled",
    "Rust code under their upstream licenses. See cargo-metadata.json for the",
    "exact dependency names, versions, license strings and repositories captured",
    "from the build graph.",
    ""
  ].join("\n"),
  "utf8"
);
writeFileSync(join(outputDir, "licenses", "cargo-metadata.json"), cargoMetadata(), "utf8");

const manifest = {
  name: "HiCT Tauri WebView",
  engine: "tauri-system-webview",
  priority: 10,
  command,
  arguments: [],
  license: "HiCT MIT; Tauri stack MIT/Apache-2.0 and dependency-specific licenses; system WebView runtime under OS/vendor terms",
  notices: [
    "licenses/TAURI_BROWSER_NOTICE.txt",
    "licenses/cargo-metadata.json"
  ],
  runtimeRequirements: {
    linux: {
      libraries: [
        "libwebkit2gtk-4.1.so.0",
        "libjavascriptcoregtk-4.1.so.0",
        "libgtk-3.so.0"
      ],
      installHints: {
        "Debian/Ubuntu": "sudo apt-get install libwebkit2gtk-4.1-0 libjavascriptcoregtk-4.1-0 libgtk-3-0",
        "Fedora/RHEL": "sudo dnf install webkit2gtk4.1 gtk3",
        "Arch Linux": "sudo pacman -S webkit2gtk-4.1 gtk3",
        "openSUSE": "sudo zypper install libwebkit2gtk-4_1-0 gtk3"
      }
    },
    windows: {
      runtime: "Microsoft Edge WebView2 Runtime",
      detection: "EdgeUpdate Clients {F3017226-FE2A-4295-8BDF-00C3A9A7E4C5} pv registry value or standard Microsoft/EdgeWebView installation directory"
    },
    macos: {
      runtime: "WKWebView",
      detection: "Built into macOS; no separate browser runtime is bundled or required."
    }
  },
  sizeBytes: directorySize(outputDir),
  sha256: sha256OfTree(outputDir)
};
writeFileSync(join(outputDir, "manifest.json"), `${JSON.stringify(manifest, null, 2)}\n`, "utf8");

console.log(`Prepared HiCT Tauri browser payload for ${platform}`);
console.log(`  output: ${outputDir}`);
console.log(`  size: ${(manifest.sizeBytes / (1024 * 1024)).toFixed(1)} MiB`);

function parseArgs(argv) {
  const parsed = {};
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === "--platform") {
      parsed.platform = argv[++i];
    } else if (arg.startsWith("--platform=")) {
      parsed.platform = arg.slice("--platform=".length);
    } else if (arg === "--output") {
      parsed.output = argv[++i];
    } else if (arg.startsWith("--output=")) {
      parsed.output = arg.slice("--output=".length);
    } else if (arg === "--webui-dir") {
      parsed.webuiDir = argv[++i];
    } else if (arg.startsWith("--webui-dir=")) {
      parsed.webuiDir = arg.slice("--webui-dir=".length);
    } else if (arg === "--skip-build") {
      parsed.skipBuild = true;
    } else {
      throw new Error(`Unknown argument: ${arg}`);
    }
  }
  return parsed;
}

function detectPlatform() {
  if (process.platform === "win32") {
    return "windows_x86_64";
  }
  if (process.platform === "linux") {
    return "linux_x86_64";
  }
  if (process.platform === "darwin") {
    return process.arch === "arm64" ? "darwin_arm64" : "darwin_x86_64";
  }
  throw new Error(`Unsupported platform for HiCT Tauri browser payload: ${process.platform}`);
}

function run(command, commandArgs, cwd) {
  const result = spawnSync(command, commandArgs, { cwd, stdio: "inherit" });
  if (result.error) {
    throw result.error;
  }
  if (result.status !== 0) {
    throw new Error(`${command} ${commandArgs.join(" ")} failed with exit code ${result.status}`);
  }
}

function ensureTauriConventionalIcons() {
  const iconsDir = resolve(tauriDir, "icons");
  const pngPath = resolve(iconsDir, "icon.png");
  const icoPath = resolve(iconsDir, "icon.ico");
  const sourcePngPath = resolve(repoDir, "public", "hict-logo-512.png");
  const faviconPath = resolve(repoDir, "public", "favicon.ico");

  mkdirSync(iconsDir, { recursive: true });
  if (existsSync(sourcePngPath)) {
    cpSync(sourcePngPath, pngPath);
  } else {
    writeFileSync(pngPath, Buffer.from(generatedIconPngBase64, "base64"));
  }
  if (existsSync(faviconPath)) {
    cpSync(faviconPath, icoPath);
  } else {
    const pngBuffer = readFileSync(pngPath);
    writeFileSync(icoPath, icoBufferFromPng(pngBuffer));
  }
}

function icoBufferFromPng(pngBuffer) {
  const header = Buffer.alloc(22);
  header.writeUInt16LE(0, 0);
  header.writeUInt16LE(1, 2);
  header.writeUInt16LE(1, 4);
  header.writeUInt8(32, 6);
  header.writeUInt8(32, 7);
  header.writeUInt8(0, 8);
  header.writeUInt8(0, 9);
  header.writeUInt16LE(1, 10);
  header.writeUInt16LE(32, 12);
  header.writeUInt32LE(pngBuffer.length, 14);
  header.writeUInt32LE(header.length, 18);
  return Buffer.concat([header, pngBuffer]);
}

function validateTauriIconConfig(currentPlatform) {
  const config = JSON.parse(readFileSync(tauriConfigPath, "utf8"));
  const icons = config.bundle?.icon ?? [];
  if (!Array.isArray(icons) || icons.length === 0) {
    throw new Error("src-tauri/tauri.conf.json must define bundle.icon; Windows Tauri builds require an .ico resource.");
  }
  const resolvedIcons = icons.map((icon) => resolve(tauriDir, icon));
  const missingIcons = resolvedIcons.filter((iconPath) => !existsSync(iconPath));
  if (missingIcons.length > 0) {
    throw new Error(`Tauri icon file(s) do not exist: ${missingIcons.join(", ")}`);
  }
  if (currentPlatform === "windows_x86_64" && !resolvedIcons.some((iconPath) => iconPath.toLowerCase().endsWith(".ico"))) {
    throw new Error("Windows Tauri payload builds require at least one .ico path in bundle.icon.");
  }
}

function validateTauriCapabilities() {
  const capabilitiesDir = resolve(tauriDir, "capabilities");
  if (!existsSync(capabilitiesDir)) {
    return;
  }

  for (const entry of readdirSync(capabilitiesDir)) {
    if (!entry.endsWith(".json")) {
      continue;
    }
    const capabilityPath = resolve(capabilitiesDir, entry);
    const capability = JSON.parse(readFileSync(capabilityPath, "utf8"));
    validateTauriPermissionIdentifiers(capabilityPath, capability.permissions ?? []);
    validateTauriRemoteUrlPatterns(capabilityPath, capability.remote?.urls ?? []);
  }
}

function validateTauriPermissionIdentifiers(capabilityPath, permissions) {
  if (!Array.isArray(permissions)) {
    throw new Error(`${capabilityPath}: permissions must be an array.`);
  }

  const identifierPattern = /^(?:[a-z][a-z0-9]*(?:-[a-z0-9]+)*:)?[a-z][a-z0-9]*(?:-[a-z0-9]+)*$/;
  for (const permission of permissions) {
    if (typeof permission === "string") {
      if (!identifierPattern.test(permission)) {
        throw new Error(
          `${capabilityPath}: invalid Tauri permission identifier '${permission}'. ` +
            "Use lowercase kebab-case, for example 'allow-quit-app'."
        );
      }
    } else if (permission && typeof permission === "object" && typeof permission.identifier === "string") {
      if (!identifierPattern.test(permission.identifier)) {
        throw new Error(
          `${capabilityPath}: invalid Tauri permission identifier '${permission.identifier}'. ` +
            "Use lowercase kebab-case, for example 'allow-quit-app'."
        );
      }
    } else {
      throw new Error(`${capabilityPath}: unsupported Tauri permission entry: ${JSON.stringify(permission)}`);
    }
  }
}

function validateTauriRemoteUrlPatterns(capabilityPath, urls) {
  if (!Array.isArray(urls)) {
    throw new Error(`${capabilityPath}: remote.urls must be an array.`);
  }

  for (const url of urls) {
    if (typeof url !== "string") {
      throw new Error(`${capabilityPath}: remote URL patterns must be strings.`);
    }
    if (/\[[^\]]+\]/.test(url)) {
      throw new Error(
        `${capabilityPath}: Tauri's URLPattern parser rejects bracketed IPv6 remote URL patterns ('${url}'). ` +
          "Use 'http://localhost:*' and 'http://127.0.0.1:*' for the bundled HiCT WebUI."
      );
    }
  }
}

function cargoMetadata() {
  const result = spawnSync("cargo", ["metadata", "--locked", "--format-version", "1", "--manifest-path", resolve(tauriDir, "Cargo.toml")], {
    cwd: repoDir,
    encoding: "utf8"
  });
  if (result.error || result.status !== 0) {
    return JSON.stringify({
      error: result.error?.message ?? result.stderr ?? "cargo metadata failed"
    }, null, 2);
  }
  const metadata = JSON.parse(result.stdout);
  const packages = metadata.packages
    .map((pkg) => ({
      name: pkg.name,
      version: pkg.version,
      license: pkg.license,
      repository: pkg.repository
    }))
    .sort((left, right) => `${left.name}@${left.version}`.localeCompare(`${right.name}@${right.version}`));
  return `${JSON.stringify({ packages }, null, 2)}\n`;
}

function chmodExecutable(path) {
  const mode = statSync(path).mode;
  chmodSync(path, mode | 0o755);
}

function directorySize(path) {
  const stats = statSync(path);
  if (stats.isFile()) {
    return stats.size;
  }
  return readdirSync(path)
    .map((entry) => directorySize(join(path, entry)))
    .reduce((left, right) => left + right, 0);
}

function sha256OfTree(path) {
  const hash = createHash("sha256");
  for (const filePath of listFiles(path).sort()) {
    if (basename(filePath) === "manifest.json") {
      continue;
    }
    hash.update(filePath.slice(path.length + 1));
    hash.update("\0");
    hash.update(readFileSync(filePath));
    hash.update("\0");
  }
  return hash.digest("hex");
}

function listFiles(path) {
  const stats = statSync(path);
  if (stats.isFile()) {
    return [path];
  }
  return readdirSync(path).flatMap((entry) => listFiles(join(path, entry)));
}
