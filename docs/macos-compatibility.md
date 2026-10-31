# macOS build and compatibility policy

The complete HiCT distribution targets **macOS 12.3 Monterey or later** on
Apple Silicon and Intel. Apple Silicon builds use GitHub's `macos-latest`
runner; Intel builds explicitly use `macos-15-intel`. These are separate
architecture-specific downloads, not a single universal binary. M1 Macs can
run Monterey; the complete bundle does not target their original Big Sur OS.

The deployment target is independent of the runner/SDK version. This policy
was chosen after checking the actual dependencies, rather than assuming that
all components can follow HiCT's own target:

- The pinned SeqKit revision requires Go 1.25.5. Go 1.25 requires macOS 12.
  See [Go's release notes](https://go.dev/doc/go1.25#darwin).
- SciPy 1.14.1's `macosx_12_0_arm64` wheel contains extensions with a **12.3**
  Mach-O minimum. Wheel platform tags alone therefore cannot prove the
  minimum OS of the complete bundle.
- Current Electron requires macOS 12 or later. The portable macOS package
  uses Tauri/WKWebView instead; WebUI assets target Safari 15 syntax.
  See [Electron 38's compatibility change](https://www.electronjs.org/blog/electron-38-0).

## Building

The release jobs set `MACOSX_DEPLOYMENT_TARGET=12.3`. The same default applies
to local Darwin toolchain, JNI, browser and portable-package builders.
`MACOS_DEPLOYMENT_TARGET` remains a legacy alias; conflicting values fail
early. Overrides may raise the target, but cannot lower the supported minimum.

The shell entrypoints share `scripts/portable/macos_target.sh`. Gradle records
the target as a JNI compilation input. Both Tauri payload builders export the
target for their direct Cargo invocation, and `tauri.conf.json` specifies the
same default minimum. Release caches are partitioned by deployment target;
minimap2 and mm2-plus additionally check it before reusing compiled tools.

The sequence-tool builder first downloads binary wheels for the requested
macOS platform and then installs only from that wheelhouse. This prevents a
newer runner from selecting SciPy's macOS 14 wheel. ModDotPlot's source
extension inherits the compiler deployment target.

## Release gate and evidence

`scripts/ci/check_macos_compatibility.py` checks native deployment headers with
Apple's `lipo` and `otool`, filters by the package architecture, and rejects
minimum OS versions above the target. It also rejects non-system absolute
dylib dependencies that would bind a release to the build host. It inspects
native libraries inside JARs and compressed native archives without extracting
archive member paths into the filesystem.

The gate runs on sequence payloads and on the complete staged portable package
before either release archive is created. The latter includes the Java runtime,
JNI/HDF5, native tools and Tauri. Its report is bundled at
`share/doc/macos-compatibility.json` and uploaded as a CI artifact even when
the audit fails. Fix/rebuild an incompatible dependency; do not rewrite its
Mach-O version header to claim unsupported compatibility.

To rerun on an extracted release on a Mac with Xcode command-line tools:

```bash
python3 scripts/ci/check_macos_compatibility.py /path/to/extracted/HiCT \
  --arch arm64 --target 12.3 --report /tmp/hict-macos-compatibility.json
```

Use `--arch x86_64` for the Intel artifact.

## Validation performed on Linux (2026-10-31)

- Toolchain regression tests cover version parsing, SDK versus minimum OS,
  wrong-platform rejection, absolute dylib dependencies, nested JAR/native
  archives, architecture filtering, wheel selection and environment handling.
  The real Apple compiler test is skipped on Linux and runs in macOS CI.
- The pinned Python requirements resolve for `macosx_12_3_arm64`. Independent
  inspection with macholib found 258 ARM64 Mach-O headers in downloaded
  wheels; none require more than 12.3. Of these, 115 SciPy headers require
  12.3, explaining why a nominal 12.0 target would be inaccurate.
- The published `sis-jhdf5-19.04.1-natives.tar.gz` release asset was downloaded
  to `/tmp` and inspected: 18 ARM64 headers require 11.0 and 18 Intel headers
  require 10.11, both within the chosen target.
- Gradle configuration, WebUI production build, workflow YAML, shell and Node
  syntax, and whitespace checks are validated locally.

## Native acceptance still required

Header checks do not prove runtime/API compatibility. Build both architectures
in GitHub Actions, then test the **same downloaded artifacts** on macOS 12.3
(or record the exact later Monterey patch used), macOS 15 and the current OS.
Do not substitute a rebuild on the older OS for this test.

1. Extract/launch the matching architecture's `.run` and `.tar.gz`; check the
   Launcher, CLI help and bundled Java startup, without Homebrew in PATH.
2. Open an HDF5 map, navigate/zoom, edit a scaffold name, reverse and move a
   selection, and verify the viewport remains stable.
3. Load overlays and FASTA; run sequence search and ModDotPlot. Check completed
   jobs deliver tracks, then check strand colours and independent track hiding.
4. Exercise histogram zoom/log axes, rendering presets and both source sliders.
5. Export SVG/PNG/PDF using the native save dialog, including cancellation,
   and reopen the outputs. Save/restore a session and check visual state.
6. Smoke-test all bundled CLI tools (hictk, minimap2, mm2-plus, SeqKit,
   ModDotPlot and BLAST) and retain their versions and job logs.

Record OS version, CPU, artifact SHA-256, compatibility report and failures.
Native Monterey runtime acceptance has not been claimed from Linux checks.
