# Electron runtime and native acceptance

Electron is a development dependency in npm, but its executable is delivered in
the desktop payload. An `npm audit --omit=dev` result cannot qualify that runtime.
The former 41.10.7 runtime is flagged by the full audit. The targeted update pins
43.8.10 and changes no other locked package version. The full audit no longer
flags Electron, while 28 other build/development advisories remain (17 high and
11 moderate); this is not a blanket dependency-security clearance.

At validation time the latest stable line is 45, so 43 is within Electron's
[three supported stable lines](https://www.electronjs.org/docs/latest/tutorial/electron-timelines).
It retains upstream macOS 12 compatibility; Electron 44+ requires macOS 13.
See the [breaking changes](https://www.electronjs.org/docs/latest/breaking-changes)
and [upstream certificate advisory](https://github.com/electron/electron/security/advisories/GHSA-jr5w-w6x2-84p9).
This is an upstream compatibility fact, not macOS native GUI qualification.

## Linux validation

The tested runtime reports Electron 43.8.10, Chromium 150.0.7871.250 and Node
24.22.1. `npm run build` and all 16 shared Rust save-writer tests pass. The
WebUI CI matrix now installs the stable minimal Rust toolchain and runs
`node scripts/check-tauri-export.mjs` on all four hosted platforms. This checks
the actual shared writer and command permissions without requiring a WebView
SDK; complete wrapper/package builds remain separate gates.

The development executable passes nine native export cases. The built payload
with production fuses passes the same nine cases, eight offline/reconnect/
storage cases and fourteen real-map histogram/export/restart/reset checks.
The 512-Mb FASTA has exact file/sequence identity, and no coordinate bytes enter
the renderer. The built-payload main-process RSS rises from about 156 to
234 MiB in this host run. These measurements are environment-specific.

Real GTK choosers, cancellation, permission failure, session readback and native
window close are exercised. SVG/PNG/PDF outputs are independently rasterized
and compared. The real map remains byte-identical. `check_electron_export.py`
now accepts `--payload` so the delivered executable/app can be tested directly.

The SDK is the Ubuntu 22.04 image documented in `tauri-native-validation.md`,
with this validation-only figure/runtime layer:

```dockerfile
FROM hict-tauri-validation:rust188-ubuntu22
RUN apt-get update && apt-get install -y --no-install-recommends python3-numpy python3-pil inkscape poppler-utils x11-utils && rm -rf /var/lib/apt/lists/*
```

The tested image is
`sha256:7c2c24e91ef73e76025c6a43c22039b1d093915a7c09af89232ac0aa30a96c93`.
The base SDK alone lacks NSS and cannot start Electron; the figure layer supplies
NSS transitively. Always check `ldd` before attributing an SDK startup failure to
the application. Nothing from this test SDK is added to the desktop payload.

Native runs use private Xvfb displays selected by `-displayfd`, a self-contained
Python 3.12/Playwright environment mounted read-only, JDK 21, read-only sources/
payload and fresh physical data/output directories. The exact Docker drivers,
SDK layer and small receipts are retained under
`build/release-evidence/electron-43.8.10`; the original runs are under
`/tmp/hict-electron-43-native-02`, `/tmp/hict-electron-43-payload-qa-01` and
`/tmp/hict-electron-43-figure-01`. These are browser-payload checks, not final
installer, AppImage, Windows or macOS GUI qualification.

## Package performance follow-up

Version-66 archive identity passes all 3,128 entries. Its full workflow gate
fails the 1.2-GB map's fresh-browser frame limit (33.3 ms versus 33 ms), while
repeat-open is 16.8 ms and acknowledgements remain below 47 ms. Other native
GUI export tests were running concurrently. A single targeted investigation
with those tests stopped passes unchanged limits: frames 16.7/16.8 ms and
acknowledgements 34.33/34.58 ms. This is consistent with resource contention,
not proof of its sole cause. Preserve the failed run and repeat the complete
final package gate and 30-minute soak with other agent GUI tests stopped.
