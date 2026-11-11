# HiCT 2.0.0 release validation

## Release decision

**NO-GO for publication while current-revision CI, the remaining control audit and owner platform evidence remain open.**
The last packaged application candidate is JVM `a1f7f0b8` and WebUI `d091321`.
Acceptance-only commit `861d9a59` adds reload, zoom-endpoint and caption checks.
The full Java suite passes 468 tests with seven optional skips. All forty browser scripts have passing recorded results across the final source
sweep and focused compatible-candidate repeats. The corrected full-duration
packaged memory soak passes its original limits. The control ledger retains its remaining
boundary and interaction assertions.

Both Linux packages pass fresh Borrelia journeys, offline use and exact metadata.
The self-extracting package also passes a fresh yeast journey. Existing GitHub
portable run 43010279649 passes all four targets at JVM fd46e1fd and WebUI
f0ef290. The later Java run at 2e48a9f0 passes Windows and Intel macOS and fails
the logging-test race on Linux and Apple Silicon. The race is fixed locally in
02391436. Current local fixes await the owner push required by the plan.
The Windows and macOS interactive checklist remains unverified. GitHub inspection
has used existing runs, logs and artifacts. Nothing has been pushed or merged.

## Implementation traceability

| Package | JVM commit | WebUI commit | Delivered behavior |
|---|---|---|---|
| U1 | `dc12b716` | `8c52b61` | Readable opening, coherent normalization and silent excluded-bin policy |
| U2 | `ea151ea2` | `507d487` | Split lineage and exact native session restore |
| U3 | `4359a880` | `5562552` | Three-click picker, keyboard opening, prerequisites, compact controls and focus |
| U4 | `13cbe62e` | `824a658` | Exact genomic rulers, split/reverse/scoped coordinates and exports |
| U5 | `e8fea2f4` | `40ee1e9` | Deduplicated failures, retained diagnostics and actionable notifications |
| U6 | `6a8819ba` | Unchanged | Bounded resources, cancellation, FASTA-only behavior and offline API assets |
| U7 | `8214fe36` | `be2a817f` | Lazy heavy modules and disposed map/ruler resources |
| U8 | `19faa062` | `7d6549e6` | Independently selected secondary pyramid levels and bounded projected tiles |
| U9 | `efbf5f3d` | `c251787a` | CIGAR-based alignment detail at letter zoom with consistent exports |
| U10 | `01c9dca2` | `807fa9b` | Unified background jobs, cancellation, atomic outputs and process-tree shutdown |
| U11 | `fb4f1863`, `c8ae7cce` | `33c39e0`, `7299669` | Verified DNA Zoo cards and owner-selected Borrelia/yeast quick starts |
| U12 dependencies | `59b1c666` | `1b2651a` | Sequential dependency upgrades and JVM dependency locks |
| U12 documents | `83fc05c2` | `b486877` | Installation guide, citations, attribution, notices, API guide and platform CI |
| U13 capabilities | `ddf14d6f` | `1865476` | Reactive resolution-dependent normalization availability |
| U13 shutdown | `173178f3` | Unchanged | Close published maps and remove primary/secondary gzip FASTA indexes on stop |
| U13 editor keyboard | `b8bd9985` | `dfb9063` | Focused editor Escape cancellation and retained map-tool cancellation |
| U13 Cooler controls | `89175319` | `d26144b` | Supported compression and balancing choices with exact exported-contact checks |
| U13 formatting | `bb44d3fe` | `13168ab` | Focused formatting correction with the original warning baseline retained |
| U13 bookmark names | `3ee2ee6e` | `3105102` | Named bookmark export/import, sidebar remount and native session recovery |
| U13 compression levels | `94a328ed` | `0ce0314` | Selected gzip level and optional balanced uncompressed interaction data |
| U13 menu and levels | `844cfb0d` | `a93412c` | Keyboard menu Escape and consistent final resolution switch |
| U13 conversion list | `d141adbf` | `f57012a` | Empty current-job list matches actual server state |
| U13 entity navigation | `b1d094c8` | `c1ab1bb` | Sidebar navigation reuses assembly-aware range projection |
| U13 name export | `d91f6bad` | `ee5f708` | Name mapping export uses its declared GET method |
| U13 atomic presets | `b674555e` | `f069f32` | Rejected preset batches preserve libraries and thresholds |
| U13 Move cancellation | `26268352` | `8163456` | Escape removes Move arrows immediately and preserves native assembly state |
| U13 source-independent graphs | `91581c50` | Unchanged | Constant, coordinate and track-only branches render once on the primary grid |
| U13 dialog controls | `dc40c18e` | `cb7ca73` | Reachable Close actions, contained keyboard focus and visible opening focus |
| U13 exact scaling | `524230f0` | Unchanged | Fractional and overflow-safe resolution scaling with independent numerical checks |
| U13 portable startup | `464eba3c` | Application UI `f069f32` | Atomic extraction and concurrent first-start cache publication |
| U13 track repaint | `cdbb8f06` | `3caeeba` | Immediate track backgrounds on both axes and matching exports |
| U13 source identity | `2e48a9f0` | Unchanged | Full-precision timestamps and bounded content identity across filesystem limits |
| U13 logging check | `02391436` | Unchanged | Shutdown interruption handled by the regression fake sink |
| U13 full graph presets | `992e999a` | `d091321` | Complete applied graphs, matching editor layouts, JSON/native libraries and atomic dependency rejection |
| U13 alpha compatibility | `a1f7f0b8` | Unchanged | Exact serialized alpha bytes with established fractional-input behavior |
| U13 navigation and captions | `861d9a59` | Unchanged | Exact reload pixels, both zoom endpoints and ten exported caption placements |

Supervisor items 1–7, 9 and 10 retain their earlier implementations with the
U1/U3/U4/U5/U6 corrections above. Item 8 is implemented by U8 and U9. Item 11 is
implemented by U10 and U11. Item 12 depends on completing the U13 audit and the
platform checks recorded here.

## Builds and regression checks

The latest full JVM suite has 468 tests, zero failures, zero errors and seven optional
skips. The compiled native-processing baseline also passes. The complete smoke
script passes through JVM compilation/tests and WebUI type checking/build.
The full WebUI build runs its existing source and numerical regression scripts.
Full source ESLint, using the repository ignore paths without fixes, reports zero
errors and 99 warnings on WebUI `0ce0314`. Focused navigation lint on `a93412c`
reports zero errors and five existing warnings across its two changed components.

The dependency upgrades passed their staged build and browser/export gates.
The production npm audit has zero findings. The development dependency graph
retains 15 findings, including three high and zero critical. No requested upgrade
was reverted. Detailed versions and chains are in `docs/reinforcement-validation.md`.

## Packaged Linux journeys

On JVM `173178f3` and WebUI `1865476`, both the `.run` and AppImage passed a fresh
profile journey through three clicks to the small example, navigation, Reverse,
Split, Undo, session Save/Close/Restore and AGP export. The saved and restored
assembly, linked FASTA and AGP SHA-256 agree. Each restored viewport contains
31,102 signal pixels and has no page or console errors.

| Package | Readable example | Complete journey |
|---|---:|---:|
| Linux `.run` | 20.35 s | 27.54 s |
| Linux AppImage | 19.73 s | 26.64 s |

The AppImage uses the documented `APPIMAGE_EXTRACT_AND_RUN=1` path on this host.
Both packages also pass local map opening, editing, unsaved Close cancellation,
Save and browser restart/session recovery with all WAN requests denied. PNG,
SVG and PDF export and the bundled Swagger operation list work in that state.
The evidence roots are named by `/tmp/hict-u13-run-first-journey-dir`,
`/tmp/hict-u13-appimage-first-journey-dir`, `/tmp/hict-u13-run-offline-dir` and
`/tmp/hict-u13-appimage-offline-dir`.

## Performance and retention

The packaged `.run` completed 232 mixed-workload cycles in 1,804.27 seconds.
The four inputs include mixed native pyramids and maps with 96, 1,765 and 7,790
contigs. Work includes opening, pan/zoom, secondary attachment, Reverse/Undo,
linked-sequence comparison, viewport PNG export and close. Browser exceptions
and unexpected console errors fail the gate.

| Measurement | Result |
|---|---:|
| Pan/zoom frame p95 | 16.8 ms |
| Reverse/Undo acknowledgement p95 | 58.78 ms |
| Post-warmup browser heap slope | 0.054 MiB/min |
| Browser heap early/late growth | 857,612 bytes |
| Post-warmup Java heap slope | 0.141 MiB/min |
| Java heap early/late growth | 2,556,928 bytes |
| Java RSS slope | -0.311 MiB/min |
| Post-warmup DOM nodes | 675–679 |
| Post-warmup listeners | 226–230 |

Measurements use normal JIT, one browser context, forced browser GC after close,
console-handle disposal and Java GC before sampling. The first five minutes are
warmup. The script checks early/late thirds and linear slopes against declared
limits. Backend RSS varies with map size and allocator retention. Evidence is
`/tmp/hict-u13-soak-qualified.CPPhbN/evidence/results.json` and its raw JSONL samples.

The fresh-browser and repeat-open measurements below retain OS and server caches.
Opening includes a declared raw-count graph configuration and settling.

| Contigs | Fresh browser open/configure | Repeat open/configure | Frame p95 fresh/repeat | Long tasks fresh/repeat |
|---|---:|---:|---:|---:|
| 96 | 1,775.92 ms | 567.24 ms | 16.75 / 16.70 ms | 0 / 0 |
| 1,765 | 966.53 ms | 398.16 ms | 16.70 / 16.70 ms | 0 / 0 |
| 7,790 | 2,775.92 ms | 1,324.42 ms | 16.80 / 16.80 ms | 1 × 52 ms / 0 |

The earlier controlled browser baseline had 16.7 ms frame p95 and zero long tasks
on the 1,765-contig map. Its opening sample was 412.1/335.3 ms and used an earlier
runtime and test setup. The timings above retain their explicit setup/cache
conditions. The packaged p95 frame and acknowledgement limits pass.

## Export and desktop evidence

The alignment fixture's PNG, independently rasterized SVG and PDF each contain
exactly 12 substitution pixels at centroid `(584.8333, 455.0)` in a 1,034 × 791
scene. The pixel centres agree across all three formats. The CIGAR gate also
verifies insertions, deletions, reverse complement, repeats, N/IUPAC handling,
no-alignment guidance, Split/Reverse/Undo and stale-response cancellation.

Electron and Tauri pass native Save/Cancel for viewport and whole-map PNG/SVG/PDF,
profile persistence and storage reset on private displays. Export comparisons use
matching raster scale and the actual asymmetric ruler margins. Electron's mean
absolute channel errors range from 0.18 to 2.83 on a 0–255 scale. Its heatmap red
recall ranges from 0.944 to 0.999. Full per-format metrics remain in
`/tmp/hict-u13-electron-native-ready.xKSYy1/evidence/rasterization-results.json` and
`/tmp/hict-u13-tauri-native-final.GbXFOI/evidence/rasterization-results.json`.

Launcher reset acceptance also starts a real packaged server, reads `/version`,
confirms Stop and verifies that the owned process exits. Evidence is
`/tmp/hict-launcher-reset-jy8l0lgr/results.txt`.

## Example data and resource decisions

The selected tardigrade matrix and matching FASTA total 45,250,776 source bytes.
The two larger DNA Zoo cards have recorded source hashes, successful conversion,
readable signal and linked sequence. Cheetah conversion/open took 664.72 seconds
with sampled heap 2.59 GB. Mosquito conversion/open took 348.48 seconds with sampled
heap 2.08 GB. The portable default uses `MaxRAMPercentage=50.0`, and an explicit
`-Xmx` overrides it. The tardigrade choice was made after presenting candidates
and receiving no alternative selection. Full sources, terms, citations and
measurements are in `docs/examples-catalog.md`.

Both CFF files pass the official 1.2.0 schema. The deterministic aggregate notice
manifest records 394 components and 248 unique normalized notice texts. Portable
packages retain the aggregate and the runtime/tool-specific legal files.

## Remaining qualification

Windows x86-64, macOS arm64 and macOS x86-64 CI build/start and owner manual results
remain pending. The written checklist records the required tester, revisions,
artifact checksum and evidence. Record each target’s CI build/start and completed owner checklist.

The historical control inventory has 232 signature contracts with 227 inspected
source bindings and five merged/ambiguous signatures. Static reconciliation
passes. Full functional outcomes need their scenario-specific runtime mapping in
`docs/reinforcement-control-coverage.md`. The final audit continues from those recorded expectations.


## Later candidate memory result

The 30-minute run on JVM `3ee2ee6e` and WebUI `3105102` completed 232 cycles
but failed the selected RSS slope limit. Java heap slope was 0.131876 MiB/min.
RSS slope was 1.101863 MiB/min against a 1.0 MiB/min limit, with early/late
median growth of 33,095,680 bytes. The earlier passing run above remains
revision-specific evidence. The longer repeat and later packaged run below pass the original limits.
Raw samples and the failure are retained in
`/tmp/hict-u13-publication-soak-ready.dfwKOb/`.

## Additional audited controls

The compression gate verifies eight actual exports, every contact, HDF5 filter
levels and nonuniform balancing weights. The navigation gate verifies all six
keyboard menus, exact reload graph/view preservation, sidebar restoration and
all eleven resolution switches with valid/reversed limit bounds. The expanded
workspace gate verifies seven scenarios including independent scope values, names,
presets, every OSD field, actual toast dismissal and zoom-thumb dragging. The
picker gate verifies four scenarios with independent loaded matrix values.

The runtime ledger now lists every historical control index alongside executed
assertions and remaining checks in `docs/reinforcement-control-coverage.md`. The
full interface audit remains open. The owner-selected full custom graph preset
extension now passes JSON and native-session round trips, independent signals
and rendered colors, complete editor layouts and dependency rejection checks.

Batch selection and real conversion produce independently exact contacts, and
overwrite cancellation preserves both the output hash and server job list. The
audit also reproduced an empty active conversion list displaying Running. The
corrected four-scenario gate passes with an actual empty server response.

## Longer memory diagnosis

The original 30-minute failure remains recorded above. A 60-minute repeat on JVM
`3ee2ee6e` and WebUI `3105102` passes the same limits with normal JIT and native
memory tracking. It completes 451 cycles in 3,603.851 s. Browser heap growth is
1,094,108 bytes with slope 0.033488 MiB/min. Java heap slope is 0.047528 MiB/min.
RSS slope is 0.204776 MiB/min, with median growth 30,212,096 bytes and observed
range 634,359,808–1,334,353,920 bytes. Frame p95 is 16.7 ms and Reverse/Undo
acknowledgement p95 is 58.752 ms. Evidence is under
`/tmp/hict-u13-native-memory-hour-ready-dir`.

A separate 30-minute allocator diagnostic uses MALLOC_ARENA_MAX=4. It completes
228 cycles and passes with RSS slope 0.454851 MiB/min, browser heap slope 0.056031
MiB/min, frame p95 16.8 ms and acknowledgement p95 58.530 ms. It is diagnostic
evidence. The later packaged candidate uses the default allocator and passes a 40-minute
qualification under `/tmp/hict-u13-final-release-soak-dir`. It completes 305 cycles
in 2,402.853 s. Browser heap slope is 0.048470 MiB/min and Java heap slope is
0.078346 MiB/min. RSS slope is -0.196678 MiB/min, with median growth 21,190,656
bytes. Frame p95 is 16.7 ms and acknowledgement p95 is 58.264488 ms. Captured
browser errors and unhandled promise rejections are empty. This run qualifies
JVM `b674555e` and WebUI `f069f32`. Later changes retain separate evidence.

## Qualified Linux artifact provenance

The application JAR is built from JVM `b674555e` with WebUI
`f069f3257bc15588b907b1234ab9cfd17e8ad979`. Its portable shell wrapper corresponds
to `464eba3c`. The full JAR build passes 433 tests with seven optional skips and
zero errors/failures. The complete smoke suite passes. Full source ESLint reports
zero errors and 99 warnings. Exact artifact hashes are retained in
`/tmp/hict-u13-safe-artifacts.json`.

Two simultaneous first .run starts both enter extraction for the same cold cache
and complete their journeys successfully. The corrected example opens in 20.277 s
and completes its edit/save/restore/AGP journey in 28.092 s. The matching AppImage
opens in 20.299 s and completes in 27.545 s. Each restores 31,102 signal pixels,
exact native AGP and linked FASTA with zero browser errors. Both artifacts pass
the offline edit/restart/session/PNG/SVG/PDF/Swagger journey. Evidence pointers
are `/tmp/hict-u13-safe-run-journey-dir`, `/tmp/hict-u13-safe-run-offline-dir`,
`/tmp/hict-u13-safe-appimage-journey-dir` and
`/tmp/hict-u13-safe-appimage-offline-dir`.

Four focused wrapper tests pass, including deterministic reproduction of the old
concurrent extraction failure, cache reuse, invalid-cache recovery and failed
extraction cleanup/retry. The final baked candidate also passes eight Cooler
export cases, including both multi-resolution level-0 writers and native level 9.
Every output contact, HDF5 interaction filter and requested balancing invariant is
checked independently.

## Latest committed application checks

JVM `dc40c18e` and WebUI `cb7ca73ad322f3fd609bb93439226b5f1f32d85a` pass
the full smoke suite and the normal bundled-JAR build. The JAR SHA-256 is
`5661a4e69004fee1fdc72419c99cb84e1a629fa5be09941e2c72b524525c25af`.
Thirty-one repeated suites pass with zero failed suites, including 151 opening
assertions, eight Cooler exports, all eighteen modal destinations, real jobs,
alignment, mixed pyramids, structural edits, history, picker, conversion, session
validation and concurrency. All fifteen sequence lifecycle cases and the exact
baked UI revision also pass. Group result files are
`/tmp/hict-u13-dialog-regate-{core,open-session,quad,extra}.json`.

The subsequent pipeline audit found that source-independent graph branches
produced transparent source tiles. JVM `91581c50` fixes their primary-grid routing.
All 440 JVM tests pass with seven optional skips, including four regressions
that fail on the preceding implementation. Twelve real-browser pipeline scenarios
pass, including exact constant values and rendered colors before and after Swap.
Mixed-pyramid and actual ModDotPlot identity gates also pass. Final packaged
qualification will use the corrected source revision.

## Rebuilt Linux qualification and advanced controls

JVM `91581c500d91776be91d2841a0723bfd90c77375` and WebUI
`cb7ca73ad322f3fd609bb93439226b5f1f32d85a` produce the following artifacts.
The portable staging JAR hash matches the standalone JAR exactly.

| Artifact | SHA-256 |
|---|---|
| Fat JAR | `a3265c29015ff93837b5b4c0a3a62b6b6ba87e3517ad74e7a75c9116d5799438` |
| Linux .run | `c75c66c3bf3091d5159cec6f2d3e978a647d39fcf73cf909660e46d4def99a08` |
| Linux AppImage | `98c0b00c6dddef3a61dbd7a7671ab23653ce870d9e31c9bc1cae66c6c3b797c3` |

Both actual packages pass the example journey and the complete WAN-denied
edit/restart/session/export/Swagger journey. The .run opens the example in
20.362 s and completes in 27.443 s. The AppImage takes 20.463 s and 27.750 s.
Each restores exactly 31,102 signal pixels, linked FASTA and matching native AGP,
with zero browser errors. Simultaneous cold .run starts also complete.

Fresh/repeat packaged navigation retains OS and server caches.

| Contigs | Open/configure fresh/repeat | Frame p95 fresh/repeat | Long tasks |
|---|---:|---:|---:|
| 96 | 1,695.71 / 678.51 ms | 16.755 / 16.700 ms | 0 / 0 |
| 1,765 | 1,103.99 / 347.82 ms | 16.700 / 16.700 ms | 0 / 0 |
| 7,790 | 2,737.55 / 1,238.76 ms | 16.800 / 16.800 ms | 0 / 0 |

Native Tauri, Electron and Launcher gates pass on this JAR. Electron's compiled
main and preload hashes match the shipped browser payload exactly. Evidence
pointers are `/tmp/hict-u13-source-independent-tauri-dir`,
`/tmp/hict-u13-source-independent-electron-dir` and
`/tmp/hict-u13-source-independent-launcher.log`.

All fifteen sequence lifecycle scenarios also pass with changed advanced
settings. Identity settings k=15, sketch=128, delta=0.2 and workers=2 appear in
artifact provenance. The native input step rejects delta=0.25 while preserving
the job list. Alignment k=7, window=1, chain=5, minimum length=20, sample=50 and
workers=2 agree with provenance. An independent scalar endpoint-sampling oracle
counts 49 owned samples and matches all 1,600 output matrix cells. Empty
resolution selections disable submission for both tools. Evidence is
`/tmp/hict-u13-advanced-sequence-empty-levels/`.

The 60-second packaged preflight passes eight complete mixed-workload cycles
with repeated dialog focus checks. The full 30-minute gate completed with a failed DOM-node assertion at
`/tmp/hict-u13-source-independent-release-soak-valid-port-dir`. The diagnosis and
corrected measurement preflight are recorded below. Its first attempt
used port 6566, which Chromium rejects as unsafe before any workload cycle. That
failed record remains under `/tmp/hict-u13-source-independent-release-soak-dir`.
The corrected run uses port 6572 and the original acceptance thresholds.

The later Move-arrow visibility correction is committed in JVM `26268352` and
WebUI `8163456`. It passes the full frontend build and all seven actual structural
scenarios. The packaged evidence above predates that one-line UI correction.
Final artifact qualification and the remaining control scenarios remain open.

## Modal-retention measurement correction

The 91581c50/cb7ca73 packaged run completes 226 cycles in 1,800.900 s. Java heap
slope is 0.128833 MiB/min and RSS slope is 0.342719 MiB/min. Browser heap growth
is 1,318,030 bytes with slope 0.078908 MiB/min. Frame p95 is 16.8 ms and
acknowledgement p95 is 58.140496 ms. Browser errors are empty. The recorded result
is failed because DOM-node and listener counts grew. The earlier harness's
qualifiesThirtyMinutes flag recorded requested duration, and its assertion text
was empty. The status field correctly records the failure.

Focused snapshots trace the detached dialog trees to inspector Global handles.
The same lifecycle with primitive-only waits and hit-tested mouse clicks plateaus
at 1,787 nodes and 702 listeners. The revised harness preserves all actual mouse
and focus assertions and the original acceptance thresholds. It saves DOM trend
context and marks qualification false on failure. Its 120-second packaged
preflight completes sixteen cycles with zero errors, node maxima 683/689 and
listener maxima 229/229 across the compared sample groups. Full qualification
remains pending. Detailed A/B evidence and the primary browser reference are in
`docs/reinforcement-validation.md`.


## Owner-selected examples and read-only platform evidence

Borrelia Hi-C and yeast Micro-C now have quick-start cards. Both pass independent
matrix/reference checks and full browser edit/session/export journeys. The
existing tardigrade card is retained. The local suite passes 445 Java tests with
seven optional skips. These source changes still need final package qualification
and the corrected thirty-minute workload.

Read-only inspection of existing GitHub Actions run 43010279561 at fd46e1fd
finds Linux and both macOS Java jobs successful. Windows has three stale-file
identity failures. The local correction is committed at 2e48a9f0 and passes the
full local suite and sequence lifecycle browser tests. Windows execution of that
correction remains unverified.
[Java regression run](https://github.com/ctlab/HiCT_JVM/actions/runs/43010279561)

Portable run 43010279649 uses WebUI f0ef29062cd37ec5f970c70975e79dcb646a67b2
and has successful Windows native-tool and Windows portable
ZIP/EXE jobs at this checkpoint. Its log verifies the packaged executable,
hictk, minimap2, AVX2 alignment, sequence tools and gapped BLAST. The optional
AVX-512 executable fails on that runner and is reported as a warning. macOS
arm64 native tools pass, with its portable package still building. Other package
jobs remain in progress.
[Windows portable job](https://github.com/ctlab/HiCT_JVM/actions/runs/43010279649/job/130490068024)

Existing successful run 42887896950 at 8ca65915 supplies macOS architecture and
minimum-version compatibility records for both architectures, plus Windows native
sequence-tool evidence. These downloaded records describe that earlier revision.
Their contents cover executable compatibility and automated tool fixtures. The
interactive platform checklist remains pending. Downloads are retained under
`/tmp/hict-u13-prior-platform-evidence/`. All GitHub operations in this inspection
read existing runs, jobs, logs or artifacts.
[Prior successful portable run](https://github.com/ctlab/HiCT_JVM/actions/runs/42887896950)


## Subsequent Java CI evidence at 2e48a9f0

Existing run 43014849030 passes Windows and Intel macOS Java jobs, verifying the
source identity correction on Windows. Linux and Apple Silicon macOS fail the
fake logging sink's shutdown-interrupt race. Both JUnit artifacts show the sink
throws before counting any of its 201 queued events. The corrected fake sink
passes twenty repetitions
with the original enqueue and complete-drain assertions. A workflow run containing
this correction is still required.
[Subsequent Java run](https://github.com/ctlab/HiCT_JVM/actions/runs/43014849030)

The earlier portable run 43010279649 now passes Windows and Apple Silicon macOS
packaging plus their native tool jobs. Linux native tools also pass. Linux
packaging and Intel macOS native tools remain in progress at this checkpoint.


## Complete rendering presets

The owner-selected preset extension passes six real-browser scenarios covering
complete branches and flags, custom editor layout and a disconnected inspector,
exact signal and pixel values, non-default border styles, saved background,
JSON and native-session libraries, stable IDs, 100 nested unary nodes and
expected-profile restoration against independent diagonal means. Missing-input
and malformed batch/style cases preserve the live visualization and library.
Full-graph import adds records without changing the active visualization.
Applied-state saving is explicit in the control. Wizard source-style choices
identify the options component used in the selected wizard presentation.

The normal UI build, native session parser checks and the current 466-test Java
suite pass, with seven optional skips. Existing built-in preset, pipeline-editor,
legacy-session and workspace browser gates also pass. These source changes still
require final package qualification and the corrected full-duration soak.


The final preset implementation also preserves every 8-bit alpha value through
the decimal API format. All 468 local Java tests pass with seven optional skips.
Final UI lint has zero errors and 99 warnings. Existing portable run 43010279649
now has successful native-tool jobs for all four targets and successful Windows,
Apple Silicon macOS and Linux packaging jobs. Intel macOS packaging is still
running at this checkpoint. Source revisions remain fd46e1fd and WebUI f0ef290.


## Compatible final Linux artifacts

The normal bundled build and smoke suite pass. These immutable artifacts contain
JVM a1f7f0b8 and WebUI d0913212670a9ddd1c2b2b0d766b96b9d559a655.
The later acceptance-only commit retains the same application sources.

| Format | Bytes | SHA-256 |
|---|---|---|
| Fat JAR | 352734941 | `6d77d2bcd909a3b08d20962a68c116ff7ac61dd2664ed482fbacde5f8e658cb1` |
| Linux .run | 580157215 | `8b886a2c3708c919462428d238a93a5309169e586de4dd386fae5fb37bb08888` |
| AppImage | 752708088 | `0e65e9753d6d07cab27e9218175526d097c0656e369d02d0018083aeebe824e2` |

The manifest is `/tmp/hict-u13-compatible-final-artifacts.json`. Normal build logs
are `/tmp/hict-u13-compatible-final-{normal,portable,appimage}-build.log`, and the
complete smoke result is `/tmp/hict-u13-compatible-final-smoke.log`.

| First-run journey | Clicks to readable map | Readable map, seconds | Full journey, seconds | Restored signal pixels |
|---|---|---|---|---|
| Linux .run Borrelia | ['Help', 'Examples...', 'Open example'] | 9.056 | 12.556 | 86371 |
| AppImage Borrelia | ['Help', 'Examples...', 'Open example'] | 9.883 | 13.366 | 86371 |
| Linux .run yeast | ['Help', 'Examples...', 'Open example'] | 140.956 | 146.542 | 7630 |

Each journey uses a fresh data folder and public downloads, then navigates,
reverses, splits, undoes, saves, closes, restores and checks exported AGP against
the saved assembly. Both formats also pass offline assets and native session
reopening. Metadata uses the complete UI commit. Full-graph preset, all fourteen
built-in preset and session snapshot repeats pass through the actual .run.
The first temporary Borrelia invocation used an invalid catalog ID and stopped
in argument validation. Corrected geo-borrelia invocations pass on both formats.

Electron and Tauri final repeats pass native save/cancel, storage reset and
actual raster/SVG/PDF comparisons against the final server. Evidence pointers
are `/tmp/hict-u13-compatible-final-electron-dir` and
`/tmp/hict-u13-compatible-final-tauri-retry-dir`. The Launcher starts the earlier
992e999a packaged server and confirms Stop terminates its owned process tree.

Read-only downloads from successful portable run 43010279649 confirm both macOS
architectures and minimum-version checks have empty error lists. Reports are
retained under `/tmp/hict-u13-fd46-platform-evidence/`. Their source revisions
precede this candidate and the reports contain automated executable qualification.
The owner-run interactive checklist remains in `release-manual-checklist.md`.


## Final packaged navigation and memory qualification

Fresh-browser and repeat-open measurements pass on 96-, 1,765- and 7,790-contig
maps. Frame p95 is 16.7–16.8 ms, with zero long tasks. Every measured
view-change acknowledgement is below 46 ms. These samples preserve normal JIT,
server and OS caches. The first 96-contig sample overlapped completion of native
wrapper validation. Its isolated repeat passes after the soak, export checks and final Launcher acceptance.

The actual Linux self-extracting artifact completes 1801.010 seconds and 228 mixed
cycles with no browser errors. The same original limits govern this run.

| Measure | Final value | Limit |
|---|---|---|
| Browser heap slope | 0.069459 MiB/min | 0.2 MiB/min |
| Browser heap growth | 1007012 bytes | 8 MiB |
| Java heap slope | 0.118403 MiB/min | 0.5 MiB/min |
| Server RSS slope | 0.359644 MiB/min | 1 MiB/min |
| DOM nodes, early/late maximum | 693/693 | Late maximum within 100 nodes |
| Listeners, early/late maximum | 233/233 | Late maximum within 20 listeners |
| Frame p95 | 16.800 ms | 33 ms |
| Acknowledgement p95 | 58.540 ms | 100 ms |

Evidence is under `/tmp/hict-u13-compatible-final-soak.o87ZIy`. The run records JVM native-memory
categories and uses the standard repeated close/GC measurement, full navigation,
actual modal mouse/focus interactions, edits/Undo, linked sequence, source
replacement and image export. The harness retains primitive state while its
inspector handles are discarded. All forty browser scripts now have passing
records across the 992e999a/d091321 sweep and a1f7f0b8 compatibility repeats.


## Subsequent custom graph control correction

The extended source/triangle audit found a generic transparency override changed
a custom colormap's alpha during a range edit and could touch unselected targets.
Hydration and publication now honor the configured colors of the selected
colormaps. A fractional-alpha graph passes exact target isolation, linked styling,
source switching and both swap round trips with pixel and contact checks. The
full UI build and lint pass. Existing interface regressions are running. This UI
correction requires a fresh package and full-duration soak before qualification.
The preceding artifact hashes and successful soak describe a1f7f0b8/d091321.


The custom-color correction is committed in WebUI b6e01ce. It also publishes
primary styles with their graph in the existing atomic transaction and consumes
the pending live-apply timer on Apply. The seven-case source-style gate verifies
a single forced503 error, exact preserved options/graph/pixels, explicit retry,
individual and linked targets, both swap controls and both actual color pickers.
All fourteen built-ins, six full-graph cases, twelve editor cases, five track
cases and sixteen sequence cases pass. Full lint has zero errors and99warnings.
The interface sweep also passes after an isolated resolution-checkbox repeat.
New packaged artifacts and a new full-duration soak remain required.
