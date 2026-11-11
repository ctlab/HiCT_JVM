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

BLAST 2.16.0 is built from the checksum-pinned NCBI source archive on both
Mac architectures: the official 2.16.0 executables require macOS 14. The
builder passes the deployment target to C, C++ and the linker, uses static
toolkit libraries with Apple's system SQLite, and audits the resulting
executable before packaging. SQLite is a required BLAST dependency: NCBI's
build can report success while skipping `blastn` if it is disabled. The
builder therefore also checks explicitly that the executable was produced.
It follows [NCBI's macOS guidance](https://www.ncbi.nlm.nih.gov/books/NBK569861/)
to disable OpenMP for Apple Clang while retaining BLAST's multithreaded mode.
Smoke tests compare local gapped alignments with `-num_threads 1` and `2`.
Windows retains the checksum-pinned official executable and app-local C++
runtime. Neither architecture's compatibility audit is bypassed.

BLAST's bundled zlib 1.2.12 also needs the classic-Mac-OS guard backported
from [upstream zlib 1.3.1](https://github.com/madler/zlib/blob/v1.3.1/zutil.h).
`TARGET_OS_MAC` in recent Apple SDKs incorrectly activated a legacy `fdopen`
replacement, breaking the system `stdio.h` declaration and redefining the
gzip OS code. `patch_blast_source.py` applies the checked, repeatable guard
change after archive verification. The altered source is marked and header
hashes are retained in `blast-source-patches.json`.

NCBI builds zlib without running zlib's own configure script. The Darwin
builder supplies `-DHAVE_UNISTD_H=1` in CFLAGS so its gzip code includes the
POSIX declarations for `read`, `write`, `close`, and `lseek`. Linux's
`_LARGEFILE64_SOURCE` can incidentally enable this include, but Darwin does
not use that fallback. The builder checks **all** bundled zlib C files with
the triggering Apple macro, the runner's SDK and
`-Werror=implicit-function-declaration` before compiling the toolkit. These
checks include `gzlib.c`, `gzread.c` and `gzwrite.c`, not only `zutil.c`.

The pinned BitMagic `bm.h` also has two stale calls in bulk-insert iterator
assignment operators: `allocate_tempblock` and `free_tempblock` no longer
exist on `bvector`. Both Apple Clang jobs fail on these calls (also reported
in [NCBI issue 20](https://github.com/ncbi/ncbi-cxx-toolkit-public/issues/20)).
The source patch replaces them with the allocator API already used by the
same iterator's constructors and destructor. Both header patches are
validated before either file is changed; unexpected or partially patched
code is rejected. The provenance report now contains a `patches` array.
Before configuring BLAST, the Mac builder compiles and runs
`blast_bitmagic_smoke.cpp` with the job's C++ compiler, SDK and deployment
target. It exercises copy assignment, move assignment, buffer reuse and
resulting bit contents. Explicit instantiation also catches these errors
with older compilers that accept the unused template definitions.

Archive inspection stages each member in a temporary directory and closes
the file before reopening it, so the same archive tests run on Windows
without `NamedTemporaryFile` sharing violations. Files and directories are
removed even if inspection fails. Native CI retains reports from the work
directory as well as successfully published payloads.

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

## Packaging regression validation (2026-11-01)

On Linux, 31 toolchain tests passed and the Apple-compiler-only test was
skipped. Coverage includes reopening nested archive members, cleanup after
inspection failure, verified BLAST source routing, compiler/linker target
flags for both Mac architectures, and detection of a skipped executable.

The checksum-pinned BLAST 2.16.0 source was also compiled on Linux using the
Mac builder's dependency-selection options (with Linux compiler flags).
Its executable found the expected 225-base, one-gap alignment, with identical
results for `-num_threads 1` and `2`. This checks the dependency selection and
executable behavior; it does not substitute for compiling with Apple's SDK or
running on Windows. Native CI and Monterey runtime acceptance remain required.

After the Apple-SDK zlib fix, 34 toolchain tests passed on Linux (one
Apple-only test skipped). The original `zutil.c` failed with
`TARGET_OS_MAC=1` and `__APPLE__=1`; the patched translation unit passed.
All 15 bundled zlib C units compiled with those macros. Compression levels
1/5/9 and gzip file-descriptor I/O round-tripped exactly, with Apple gzip OS
code 19. The zlib library was rebuilt and `blastn` relinked for a repeated
gapped-alignment smoke test. These macro regressions do not simulate Apple's
entire SDK; a successful native Actions run is still required.

The subsequent gzip-header failure exposed a gap in those earlier checks:
they did not reject implicit function declarations, and the native preflight
checked only `zutil.c`. With strict declaration checking and Linux's
large-file include fallback explicitly disabled, `gzlib.c`, `gzread.c` and
`gzwrite.c` each reproduced the reported errors. Enabling `HAVE_UNISTD_H`
passed syntax checks on all 16 C sources (including the compression source
included by NCBI's wrapper). All 15 library units then compiled under those
strict flags and passed the compression/gzip round trips again. The 34 local
toolchain tests passed, with one Apple-only test skipped. Builder regressions
check propagation of the feature flag into the toolkit and coverage of every
gzip unit in the early compile check on both target architectures.

For the BitMagic follow-up, both failed macOS job logs in
[Actions run 41248229062](https://github.com/ctlab/HiCT_JVM/actions/runs/41248229062)
contained only the two missing-member compiler errors. Compiling the new
assignment regression against the original pinned header reproduced both
errors with Linux Clang 18.1.3; the patched header compiled and the program
passed. Address/undefined-behavior sanitizer checks also passed (leak
detection disabled because this sandbox uses ptrace). The 35 local toolchain
tests passed, with one Apple-only test skipped. Native Actions must still
validate Apple's SDK, Mach-O output and deployment audit for this change.

A fresh Linux Clang 18.1.3 build of the patched BLAST source completed the
full `all_r` target and produced `blastn` 2.16.0. Its gapped-alignment smoke
test passed with identical
225-base, one-gap results for one and two threads. This temporary Linux
validation used `CONF_APP_LDFLAGS=-no-pie` to correct NCBI configure's
unsupported Linux Clang `-nopie` spelling; the macOS builder is unchanged
in this respect. No Apple SDK or Monterey runtime was available locally.

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
