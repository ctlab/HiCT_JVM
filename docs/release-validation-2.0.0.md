# HiCT 2.0.0 release validation

## Release decision

**NO-GO for publication while the required Windows and macOS evidence is pending.**
An earlier 30-minute run exceeded the selected RSS trend threshold. Its one-hour
repeat passes the original limits. A later packaged candidate also passes a 40-minute qualification.
Subsequent scaling and dialog changes require final artifact qualification. The full control audit and remaining platform qualification stay open.
This report records revision-specific local evidence. The manual platform record
is in `docs/release-manual-checklist.md`. Nothing has been pushed or merged.

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
| U11 | `fb4f1863` | `33c39e0` | Small tardigrade quick start and verified DNA Zoo cards |
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

Supervisor items 1–7, 9 and 10 retain their earlier implementations with the
U1/U3/U4/U5/U6 corrections above. Item 8 is implemented by U8 and U9. Item 11 is
implemented by U10 and U11. Item 12 depends on completing the U13 audit and the
platform checks recorded here.

## Builds and regression checks

The latest full JVM suite has 440 tests, zero failures, zero errors and seven optional
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
full interface audit remains open. Visualization presets retain their existing
option-only format. The separate pipeline graph export preserves complete custom
graphs. The optional request for an owner preference on extending presets remains
unanswered.

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
with repeated dialog focus checks. The full 30-minute gate is running at
`/tmp/hict-u13-source-independent-release-soak-valid-port-dir`. Its first attempt
used port 6566, which Chromium rejects as unsafe before any workload cycle. That
failed record remains under `/tmp/hict-u13-source-independent-release-soak-dir`.
The corrected run uses port 6572 and the original acceptance thresholds.

The later Move-arrow visibility correction is committed in JVM `26268352` and
WebUI `8163456`. It passes the full frontend build and all seven actual structural
scenarios. The packaged evidence above predates that one-line UI correction.
Final artifact qualification and the remaining control scenarios remain open.
