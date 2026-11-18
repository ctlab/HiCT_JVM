# Branch CI and release validation

The owner authorized pushes and cancellation of superseded Actions runs on
2026-11-17. This supersedes the earlier no-push instruction. Check current branch
CI before every commit/push, monitor replacement runs to completion, and inspect
failures rather than treating a successful local Linux build as platform proof.
Do not merge or manually create release tags. Normal gated release workflows may
run after an authorized push.

Before pushing, fetch both branch heads, verify that the push is a fast-forward,
and push WebUI before JVM when both changed. Record the exact resulting SHAs.
Only queued/running runs on the same branch with an older superseded SHA may be
cancelled. Keep useful failure logs/artifacts. Do not cancel the current required
validation or unrelated branches, weaken tests or enable early publication.

## Initial audit

The WebUI head `2eb9f29` passes its four-platform build:
[WebUI CI](https://github.com/ctlab/HiCT_WebUI/actions/runs/44132027047).
The JVM head `2f492034` fails the Windows/macOS regression matrix:
[Java regressions](https://github.com/ctlab/HiCT_JVM/actions/runs/44132052957).
The preceding portable run built all four platforms successfully, then failed
its regression gate and skipped release publication:
[Portable Packages](https://github.com/ctlab/HiCT_JVM/actions/runs/44072316658).
The publication gate is correctly enforcing the failing regressions.

The shared failure is `ExampleSourceProvenanceTest` expecting converted-source
attribution. `capture()` canonicalized the data root before looking up metadata
whose cache key retained its supplied spelling. macOS `/var` aliases and Windows
short paths can therefore miss a valid conversion cache. Lookup now checks the
supplied and real-root spellings while retaining real-path output containment
and exact cache/source validation. A synthetic POSIX alias regression fails on
Linux before the change and passes afterward. The original cross-platform test
remains enabled; Windows/macOS CI must confirm their native path behavior.

The Windows sequence-track test also fails while deleting its temporary folder.
HTSJDK maps BAM indexes by default and its mapped-buffer `close()` drops the Java
reference without immediate unmapping. Windows now uses ordinary index I/O so
track closure can release the file deterministically. The existing Windows
regression and package matrix must confirm that fix; Linux retains its existing
mapping behavior.

Evidence is under `/tmp/hict-actions-audit-01`, including the downloaded Windows
JUnit XML, and the following local receipts:

- `/tmp/hict-ci-path-alias-red-01.log`: added alias regression fails before fix.
- `/tmp/hict-ci-path-and-track-green-01.log`: targeted provenance/track tests pass.
- `/tmp/hict-ci-release-regressions-01.log` and `.exit.json`: 642 JVM tests,
  zero failures/errors, two optional skips, plus one passing native-baseline test.
- `/tmp/hict-ci-release-workflow-contracts-01.log`: all 25 workflow, checkout,
  artifact-selection and publication-safeguard contracts pass.

Remote confirmation of the new commits is pending. A completed older run or a
successful package-build job does not qualify a newer SHA or its publication gate.


The next coordinated source increment is WebUI `3804e45` and JVM patch
`2.0.64-2f49203-webui_3804e45`, with desktop lifecycle hardening and both
cross-platform regression fixes. Local validation passed before commit/push.
Remote run conclusions must be recorded against the resulting exact SHAs.

## First pushed replacement and macOS fixture correction

WebUI `3804e455` passes all four jobs in [WebUI CI](https://github.com/ctlab/HiCT_WebUI/actions/runs/44138402420).
JVM `2f923de3e1e7ba66408a2b31889df419e9b11c29` passes Linux and Windows
in both push/PR matrices, including the original conversion-provenance and BAM
cleanup regressions. Its ARM macOS job fails only the newly added alias fixture:
`@TempDir` itself uses `/var`, so its second conversion root was another alias,
not the canonical root the test intended to exercise.

The fixture now explicitly uses `root.toRealPath()` for that branch and reports
the tested conversion-root spelling on failure. Both supplied-alias and canonical
branches remain exercised; production lookup and cache validation are unchanged.
The original all-platform test remains enabled. Focused provenance/cache tests
pass locally in `/tmp/hict-ci-macos-alias-fixture-01.log`. A new macOS run must
confirm the correction. The prior four active older-SHA JVM runs were cancelled
after their replacements started, as authorized; receipt:
`/tmp/hict-ci-superseded-cancellations-01.json`.

## Windows portable staged-writer fixture

Both Java matrices for `55448708` finish successfully:
[push](https://github.com/ctlab/HiCT_JVM/actions/runs/44140457432) and
[PR](https://github.com/ctlab/HiCT_JVM/actions/runs/44140477889).
The [Windows portable job](https://github.com/ctlab/HiCT_JVM/actions/runs/44140457590/job/134245495808)
fails the shared Rust test `failed_rename_and_window_disposal_clean_temporary_files`
at its initial `begin`, with Windows error 87 while copying a directory's
permissions to the staged file. The test now creates the obstructing directory
after opening/writing a valid destination, then asserts failed commit, preserved
directory and removal of staging. This exercises the intended OS rename failure
on each platform. All 16 shared tests pass locally in
`/tmp/hict-tauri-windows-rename-fixture-01.log`; the full WebUI build also passes.
Windows confirmation and the portable publication gate remain required.


## Follow-up CI on the cancellation increment

JVM `bef9025c` passes both full four-platform Java matrices:
[push](https://github.com/ctlab/HiCT_JVM/actions/runs/44147044470) and
[PR](https://github.com/ctlab/HiCT_JVM/actions/runs/44147058709).
WebUI `e15f17d2` passes all four platforms in
[WebUI CI](https://github.com/ctlab/HiCT_WebUI/actions/runs/44146880847).
The old portable run was kept only until all eight native components and four
assemblies succeeded, then cancelled to release its fixed successor. Native
cache validation/source keys remain unchanged. The newer portable run and
Autobuild runs still require terminal confirmation.

The Electron runtime follow-up adds shared Rust save-writer checks directly to
the WebUI CI matrix. It installs the stable minimal Rust toolchain explicitly;
missing Rust or a failed test fails the job. This complements the complete
wrapper and portable gates and gives earlier Windows filesystem feedback.


## Early native checks confirmed and incremental artifact cleanup

WebUI `72a4d809` passes all four platforms in
[WebUI CI](https://github.com/ctlab/HiCT_WebUI/actions/runs/44153475239), including
its new native save tests. The Windows job reports 16 passed and zero ignored;
the rename-failure regression now passes on Windows itself.

The next JVM-only build fix makes `copyWebUI` synchronize generated files rather
than append them. A local incremental JAR previously retained 56 obsolete JS
chunks. Actual artifact comparison now verifies exactly 64 current frontend
files and matching bytes, and a missing optional frontend leaves its fallback
untouched. This fixes local incremental artifacts; clean CI checkouts still
require the complete frontend/package gates. Source/native compilation and
publication conclusions must remain tied to each replacement commit.

## Windows HTTP cancellation observation

JVM `bf6ee7a3` passes both current four-platform Java matrices. The Windows job
in [Portable Packages](https://github.com/ctlab/HiCT_JVM/actions/runs/44156848158/job/134307409296)
passes 22 full native Rust tests and fails
`cancellation_interrupts_waiting_headers_and_stalled_bodies` at the five-second
wait for staging size to become nonzero. This occurs before the cancellation
assertion. The sixteen shared save-writer tests in WebUI CI remain green on
Windows; they do not include these HTTP tests.

The observer uses `DirEntry::metadata()`, which Rust documents as using directory
enumeration data on Windows. Microsoft warns that NTFS enumeration attributes
can be stale and recommends querying an open file handle for current values.
The prepared test-only change opens each staging file and reads its handle
metadata. Production transfer/writer behavior, the two-second cancellation bound
and all assertions remain unchanged. All 23 full native Rust tests now pass locally, including the HTTP cancellation
case. Replacement Windows CI must still validate this diagnosis; it is not yet
a confirmed Windows fix. Both current JVM Autobuild Release runs are green.

References: [Rust directory metadata](https://doc.rust-lang.org/std/fs/struct.DirEntry.html#method.metadata)
and [Microsoft NTFS enumeration caveat](https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-findfirstfilea).
The failure excerpt is retained in
`build/release-evidence/2.0.68/windows-portable-native-http-failure.log`.


## Version-69 Windows portable confirmation

[Portable Packages run 44169303713](https://github.com/ctlab/HiCT_JVM/actions/runs/44169303713)
now completes its Windows portable ZIP/EXE job 134347933674 successfully. The
shared writer suite passes 16 cases and the complete native HTTP suite passes
23, zero ignored, including bounded cancellation while headers/bodies stall.
This confirms the live-handle metadata observer correction on Windows without
relaxing the two-second assertion. The retained excerpt is
`build/release-evidence/2.0.69/windows-portable-native-confirmation.log`.
Both version-69 Autobuild Release runs (44169303701, 44169319469) and Java
push/PR matrices (44169303698, 44169319389) are successful. Sequence tools and
other portable platforms remain running at this checkpoint; refresh their
conclusions before claiming the complete release workflow passes.


## Candidate-70 fast-response failure

[Java push run 44181839176](https://github.com/ctlab/HiCT_JVM/actions/runs/44181839176)
fails ExampleDownloaderTest on Windows (unpinned cache refresh) and Apple Silicon
(redirect repetition 4/20), both with `IllegalStateException: Response already
ended`. Linux and Intel macOS pass. The concurrent
[PR matrix 44181853301](https://github.com/ctlab/HiCT_JVM/actions/runs/44181853301)
and [WebUI CI 44181662816](https://github.com/ctlab/HiCT_WebUI/actions/runs/44181662816)
pass; they do not erase the push failures. Both exact failing JUnit XML files
are retained under `build/release-evidence/signed-bigwig/`.

The client correction uses `request.send(handler)` to register the callback
before ending the request, rather than attaching a map to `send()` afterward.
The actual pinned dependency bytecode confirms that ordering difference. The
existing downloader suite, including 20 redirect repetitions and every strict
cancellation/verification assertion, passes locally; the complete 659-test JVM
suite and native parity pass too. Replacement platform CI is still required.
Portable run 44169303713 is retained while its useful native caches/platform
jobs finish; candidate-70 Portable is pending behind it at this checkpoint.


## Candidate-71 response-race confirmation and runtime follow-up

Both candidate-71 Java matrices now pass all four platforms:
[push 44184897410](https://github.com/ctlab/HiCT_JVM/actions/runs/44184897410),
[PR 44184911620](https://github.com/ctlab/HiCT_JVM/actions/runs/44184911620).
[WebUI CI 44184641424](https://github.com/ctlab/HiCT_WebUI/actions/runs/44184641424)
also passes all four platforms. This confirms the Examples handler-order fix
without suppressing the previously failing cases. Full portable/sequence and
Autobuild results remain running/queued at this checkpoint.

The next dependency candidate updates five runtime libraries and adds native
Snappy/BZip2 compatibility checks. Its full 661-test local suite and native
parity pass; the resolved Maven OSV scan has zero flagged components, with two
local JARs explicitly uncovered. New platform CI must confirm the updated
codec natives and runtime graph before release qualification. Superseded
same-branch version-69/70 jobs were cancelled after replacement workflows existed;
their useful successful tool/assembly results and failure evidence are retained.


## Native dependency and download-only follow-up

Candidate 72's Java push/PR matrices both pass all four platforms. Complete
portable/sequence/Autobuild workflows remain active at this checkpoint.
The next coordinated source updates compatible Rust dependencies and carries
only the upstream two-line GLib VariantStrIter soundness backport. Its optimized
regression crashes with the registry original; all 24 Linux native tests pass
with the pinned patch, zero ignored. Other platforms retain the existing 23
native cases; the additional GLib regression is Linux-specific. The frontend
build verifies the complete upstream archive and all 121 source files, preserving
exact bytes across Windows checkouts. The full frontend build passes locally.

Playwright 1.65.2 / Firefox 159.0 passes eleven real download-only file/session/
figure cases with no writable-picker shim, exact scalar sequence comparisons
and unchanged native inputs. Source/audit receipts are retained in
`build/release-evidence/native-dependency-update/`. The new native graph retains
ten explicitly reviewed maintenance notices; see `native-dependency-audit.md`.
New platform Actions and final native/package qualification remain required.


## Portable CI cache stability and critical path — 2026-11-18

Prepared against pushed JVM `9b07395e079ee3073ff779f73e12465ce0a6a454`
in an isolated worktree. The current Portable Packages run
[44187130439](https://github.com/ctlab/HiCT_JVM/actions/runs/44187130439)
continues unchanged; these improvements have not yet run on GitHub.

Python unit tests ran before restoring native payload caches, creating bytecode
inside broad source globs. Its checkout-dependent bytes changed cache keys even
for identical source. All nine affected source fingerprints now exclude
`__pycache__`, `.pyc` and `.pyo`; actual sources, build scripts, runner identity
and upstream revisions remain fingerprinted. GitHub's official `@actions/glob`
0.7.0 reproduces the old key changing with identical source and proves the new
key stays stable while real Python/native source changes still invalidate it.

Linux previously forced Conan to rebuild every package after exact cache
restoration. A new source-built cache namespace includes ABI mode, upstream
source fingerprint and build/container scripts. Only an exact hit permits
`--build=missing`; cold, prefix and legacy hits retain forced source builds.
Existing component ABI validation, payload checksums and fresh tool smoke tests
remain required. Conan documents the distinction between these policies at
https://docs.conan.io/2/reference/commands/install.html#build-modes .

The ordinary source/JNI regression matrix now starts alongside packaging. It
consumes no package artifacts. Publication still requires all four packages and
the regression matrix to succeed; early publication remains disabled by default.
This removes a serial stage but does not establish a measured delivery-time
promise. The first run may still build cold caches; compare cache-hit receipts
and end-to-end time on subsequent unchanged-toolchain runs.

Local evidence under `build/release-evidence/portable-publication-priority/`:
`cache-fingerprint-repro.json`, `cache-contracts-green-02.log` (28 passing
contracts, no skips), and `cache-python-toolchains.log`. Remote validation and
warm-cache timing remain pending. These receipts are in the main JVM checkout,
not the isolated CI worktree.

The read-only cache inventory reports 77 entries totaling 10,723,702,747 bytes
(about 9.99 GiB), including 38 sequence payload entries using 3.889 GiB.
Generated-key churn therefore also creates storage pressure. No remote caches
were deleted, and PR checks continue to use the same build/verification path.
Inventory receipt: `cache-inventory.json` in the main evidence directory.

Autobuild logs show a separate BLAST compilation during the full Gradle build. The workflow now restores the complete Linux toolchain by exact runner, upstream revision and source fingerprint. Gradle verifies the restored seal and file checksums. The workflow checks upstream revisions again after building and saves verified payloads on branch runs.

## Hosted native cache measurements — 2026-11-18

The same PR workflow compiled its payloads in run 44202813810. Run 44212288935
then restored seven of eight payloads, verified their checksums and exercised
the restored binaries. Apple Silicon BLAST took the source-build path.

| Job | Compiled | Restored and verified |
|---|---:|---:|
| Linux BLAST | 34m 57s | 2m 20s |
| Windows BLAST | 7m 37s | 50s |
| Intel macOS BLAST | 52m 36s | 1m 50s |
| Intel macOS sequence tools | 9m 45s | 2m 41s |
| Apple Silicon sequence tools | 4m 55s | 1m 31s |

These durations use each job's start and completion timestamps. The package
build and push-to-publication timings are still pending.
[Compiled jobs](https://github.com/ctlab/HiCT_JVM/actions/runs/44202813810) ·
[Restored jobs](https://github.com/ctlab/HiCT_JVM/actions/runs/44212288935)

## WebUI revision shared by every portable package

A release now resolves the WebUI ref once, alongside native compilation. All
four packages build from that commit SHA, and the release provenance records
it. An explicit branch, tag or SHA must resolve successfully. Automatic
selection retains the existing fallback to the WebUI default release branch.
Cargo caches retain their requested-ref namespace so WebUI commits can reuse
compiled dependencies. This workflow edit changes the existing portable native
cache fingerprints once.

The checkout tests, workflow contracts and actionlint passed locally. The
resolver's exact shell step also passed against a disposable Git repository
for a matching branch, automatic fallback, annotated tag and rejection of an
invalid explicit ref. Hosted validation remains pending.

## Scope of sequence payload cache inputs

The pinned release changed the portable workflow's unit test, which was
enough to invalidate the older sequence payload keys. The sequence job now
hashes its build and smoke scripts, pinned configuration and bundled notices.
The portable workflow's independent tests, hictk build scripts and unrelated
license inventories no longer invalidate these payloads. The validation
suite still runs on every sequence job. The new namespace needs one cold
build per platform and component before it can serve exact hits.

An official Actions glob fixture verified that nine build and verification
inputs change the key while six unrelated inputs, including the portable
workflow unit test and generated Python bytecode, leave it unchanged.
