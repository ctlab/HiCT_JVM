# HiCT 2.0.0 release validation

## Release decision

**GO for 2.0.0 release readiness with the owner’s Windows/macOS GUI checks deferred.**
The qualified application source is JVM
`c9b15edb7b55836302762c8cb1a5247d9ea27c00` and WebUI
`95866b5d89a7946d44de2b5bee8ec09db70637b9`, on `jvm-sfx-native`, version 2.0.0.
The full Java suite passes 470 tests with zero failures, zero errors and seven
optional skips. Public `.hic` v8 and v9 conversions now match independent source
counts after the assembly-strand correction.

Java workflow 43058445089 passes all four targets at `c9b15edb`. Each target
reports 470 full-suite tests and a separate successful compiled-native baseline.
The full suite skips 12 optional fixtures on Linux/macOS and 14 on Windows.
Portable workflow 43058445223 passes all four native-tool jobs, all four
platform packages and publication. Independent WebUI CI 43076202090 also passes
all four targets at `95866b5`. Local Linux package qualification,
the full-duration soak and all three explicitly enforced performance fixtures
pass. Both Linux packages pass exact public `.hic` v8/v9 conversion and cached
reopening checks.

On 2026-11-11 the owner deferred Windows x86-64, macOS arm64 and macOS x86-64
interactive GUI checks for future internal qualification. The follow-up remains
in `docs/release-manual-checklist.md`. The current release decision uses automated
platform CI evidence and local Linux interaction. Superseded portable run
43045033635 was cancelled with owner approval after its Linux prerequisite step
spent over an hour retrying Ubuntu mirror DNS failures. That cancellation freed
the serialized current-source queue. The owner pushed both application candidates.

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
| U13 atomic styling | `ea0b44d9` | `b6e01ce` | Custom alpha, selected-target isolation and atomic primary styling with retry |
| U13 pending weights | `8bccf8af` | `95866b5` | Shared update state, guarded switches and committed-state recovery on failure |
| U13 import strands | `c9b15edb` | Unchanged | Correct AGP orientation and generation-3 conversion cache |

Supervisor items 1–7, 9 and 10 retain their earlier implementations with the
U1/U3/U4/U5/U6 corrections above. U8 and U9 implement item 8. U10 and U11 implement
item 11. The functional browser ledger and package gates cover item 12.
Engineering chronology and reproduction detail are in
`docs/reinforcement-validation.md` and `docs/reinforcement-control-coverage.md`.

## Platform automation

Committed and locally qualified stable packages use version `2.0.0`. Push CI
stamps its packages `2.0.252-ci1-c9b15ed` for workflow run 252 at the same JVM
source. The workflow preserves an explicit stable version for release-tag builds.
CI provenance and checksums below refer to the run-stamped packages.
The companion WebUI build-only workflow 43076202090 was dispatched explicitly
at `95866b5`, because its automatic branch filters exclude `jvm-sfx-native`.
It passes dependency installation, type checking and regression builds on all
four targets.

| Platform | Java job | Full-suite tests / skips | Native baseline | WebUI CI job | Portable package |
|---|---|---:|---|---|---|
| Linux x86-64 | `130654122087` | 470 / 12 | Pass | Pass, `130716556582` | Pass, job `130680978230` |
| Windows x86-64 | `130654122432` | 470 / 14 | Pass | Pass, `130716556476` | Pass, job `130669919490` |
| macOS arm64 | `130654122593` | 470 / 12 | Pass | Pass, `130716556431` | Pass, job `130685722626` |
| macOS x86-64 | `130654122689` | 470 / 12 | Pass | Pass, `130716556010` | Pass, job `130700908332` |

Each full suite has zero failures and zero errors. Optional tests depend on
private maps, source data or tools absent from Java-only CI. Windows additionally
skips POSIX-specific unreadable-directory and process-tree fixtures. Test XML,
artifact IDs and archive digests are retained under
`/tmp/hict-u13-strand-java-evidence` and
`/tmp/hict-u13-strand-java-artifacts.tsv`.
[Java CI](https://github.com/ctlab/HiCT_JVM/actions/runs/43058445089),
[portable CI](https://github.com/ctlab/HiCT_JVM/actions/runs/43058445223),
[WebUI CI](https://github.com/ctlab/HiCT_WebUI/actions/runs/43076202090).

## Published CI provenance and checksums

Publication job `130725909419` succeeds. The published build-origin file records
JVM `c9b15edb7b55836302762c8cb1a5247d9ea27c00`, branch `jvm-sfx-native`,
workflow `43058445223`, attempt 1 and version `2.0.252-ci1-c9b15ed`. Each platform's
actual build log independently confirms WebUI
`95866b5d89a7946d44de2b5bee8ec09db70637b9`.

All 16 entries in the published aggregate checksum file match the independent
SHA-256 digests returned by GitHub's release-asset API. The downloaded origin and
checksum files also match their API digests. Evidence is
`/tmp/hict-u13-published-ci252/verification.json`. The table lists the executable
packages. The aggregate also covers archives, the fat JAR and per-platform
checksum files.

| Run-stamped artifact | Bytes | SHA-256 |
|---|---:|---|
| Linux .run | 588171483 | `50a0c8c65d8ffb2b8b5309b09803b60fd2beabe47a5e1393399ce258919273cf` |
| Linux AppImage | 753261048 | `d23ec98dbde93f5cc7e19dbdd28091018e2e8b438109e87e19d340a3051f3711` |
| Windows EXE | 450147007 | `7b3a27e9de9525bfde18629c3c4f3aecbb95c695fda7ac2072b869c555108d3d` |
| macOS arm64 .run | 638471124 | `28efe06c11c4fa041105b5efdbfed53e1d062ece451725ca88d00948cc2ae2ff` |
| macOS x86-64 .run | 827264464 | `bdf7c4bcfd811a49bb7e234da9bb75b2a2e3d0242f5a08396366734dd6726988` |

The build-origin and aggregate checksums are published with the rolling CI
packages. These run-stamped assets qualify the same application source as the
locally tested stable 2.0.0 Linux packages below.
[Published CI packages](https://github.com/ctlab/HiCT_JVM/releases/tag/latest-jvm-sfx-native),
[build provenance](https://github.com/ctlab/HiCT_JVM/releases/download/latest-jvm-sfx-native/HiCT-2.0.252-ci1-c9b15ed-build-origin.txt),
[aggregate checksums](https://github.com/ctlab/HiCT_JVM/releases/download/latest-jvm-sfx-native/HiCT-2.0.252-ci1-c9b15ed-release-assets.sha256).

## Current source regressions

The 470-test suite includes AGP `+`, `-`, `?`, `0` and `na` metadata and reopened
matrix checks. The regression fails before the correction. The cache regression
preserves an existing output and generation-2 record while requiring a fresh
conversion. Generation 3 prevents automatic reuse of affected cached imports.
Affected standalone imports require regeneration from the original matrix and
layout for corrected strand orientation.

The public hictk v2.2.0 fuzzer fixtures exercise actual File/Open, batch conversion,
completion, converted-map opening, rendered signal and cached source reopening.
Each version matches all 625 first-chromosome RAW_COUNTS values at 10 Mb against
an independent hictk dump. Both original SHA-256 hashes remain unchanged and
cached reopening creates no conversion job. Fixture provenance, hashes and
commands are in the engineering report.

The complete smoke script and production WebUI build pass on the corrected
application candidate. WebUI `95866b5` full-source ESLint reports zero errors and
99 existing warnings using the repository ignore file without fixes. Its normal
build includes the source and numerical regression scripts.

The production npm audit has zero findings. The development graph retains 15
findings, including three high and zero critical. All five requested dependency
upgrades passed their staged checks. No requested upgrade was reverted.

## Browser and control coverage

Forty established browser scripts have passing records across the source sweep,
focused compatible-candidate repeats and actual packaged gates. The added public
`.hic` format script passes against both corrected Linux packages. The signal suite checks
seven cases including selected-target fractional alpha, both swaps, linked primary
nodes, secondary isolation, delayed debounce and exact state/pixel preservation
after a forced HTTP 503 followed by retry. The six track cases hold real create
requests while asserting shared busy state and restore the committed switch after
an injected failure. All 14 built-ins, six full-graph preset cases, 12 pipeline
editor cases and 16 real-tool sequence lifecycle cases have passing evidence.

Navigation verifies exact canvas bytes after reload and both toolbar/native zoom
endpoints. Caption exports verify ten placements using independently transformed
SVG coordinates. Matrix export verifies eight modes against independent contact
values, compression settings and balanced weights. Browse dismissal, custom AGP
selection, draft-preserving refresh and actual overwrite have separate assertions.

A parallel source sweep recorded one navigation timeout. The unchanged complete
navigation suite passed in isolation. Both records remain in the evidence.

The static inventory contains 232 historical signature contracts, with 227
source-inspected bindings and five explicitly ambiguous historical signatures.
Fresh reconciliation against WebUI `95866b5` passes and finds 162 literal source
candidates. Runtime claims are recorded in the scenario ledger. The static
contracts retain functional status `pending` because the reconciler validates
source bindings. Actual dialog/toast owners and the native zoom-slider instance
have separate runtime assertions.

## Preset decisions

The save control captures applied visualization options and the complete rendering
graph from one atomic snapshot. It includes compatible editor layout and restores
lossless colors. JSON and native session libraries retain those records. Legacy
option-only presets remain supported. Missing graph resources reject application
before state publication. The opening wizard labels complete-graph presets as
source styles and uses their source-style options for its chosen presentation.
Complete graph restoration is available from the visualization preset control.

## Packaged Linux evidence

The corrected JVM `c9b15edb` / WebUI `95866b5` immutable manifest is
`/tmp/hict-u13-strand-final-artifacts.json`. Its JAR, `.run` and AppImage hashes
are recorded below. Both package builds pass extraction, startup and tool smoke
checks. The 24-stage functional qualifier and three explicitly enforced
performance repetitions pass. Both larger DNA Zoo cards also pass fresh conversion and independent source/reference checks.

| Artifact | Bytes | SHA-256 |
|---|---:|---|
| Fat JAR | 352734872 | `51fbaae03ba8b1d4063c655d78613cdcff972af63e274ad4004e4e6e4e7a4c38` |
| Linux `.run` | 580156403 | `473cd60d7957ec0725f0b47d972de52ab7fa3f352343d254f0a76091f1e4e390` |
| Linux AppImage | 752712184 | `a21d5000a5150a35f1970e99907ee5aecdb98488930ac0789dec6eb2074ed5b2` |

All 24 corrected-candidate functional stages complete successfully. The separate
release-timing repetitions enforce the timing thresholds for all three maps. They cover packaged signal/presets/editor,
tracks, native sessions, independent matrix exports, exact version/UI identity,
fresh Borrelia journeys in both packages, a fresh yeast journey in `.run`, offline
operation, concurrent cold-cache startup, Launcher start/Stop, actual Tauri and
Electron native Save/Cancel, storage reset and PNG/rasterized SVG/PDF exports.
Automated Playwright and native-desktop checks run on the declared Linux host
with private displays. GitHub-hosted runners execute platform CI. The AppImage uses
`APPIMAGE_EXTRACT_AND_RUN=1` on this host. Disposable inputs,
fresh browser contexts and private native displays preserve the user's data and
desktop. Evidence pointers use `/tmp/hict-u13-strand-final-<stage>-dir`.

The corrected-package first-run journeys pass navigation, Reverse, Split, Undo,
Save/Close/Restore and AGP export. Each map opens in three clicks from a fresh
profile with its linked FASTA and no browser errors. Timing includes public
network downloads and preparation.

| Package and example | Readable map | Complete journey | Restored signal pixels |
|---|---:|---:|---:|
| `.run`, Borrelia | 38.35 s | 41.92 s | 86371 |
| `.run`, yeast | 163.37 s | 169.06 s | 7630 |
| AppImage, Borrelia | 10.03 s | 13.63 s | 86371 |

## Performance and retention

The first corrected-package 7790-contig diagnostic exceeds the 33 ms frame
limit, recording 33.30/33.355 ms in fresh/repeat samples with 1/9 long tasks.
At that time, a separate 32 GiB-limit JVM used approximately 33 GB RSS and host
swap was nearly full. All three repeated fixtures pass the original limits after
that workload was closed. The original measurements remain retained.

The corrected packaged `.run` completes 227 mixed-workload cycles in 1803.964
seconds on Linux 6.8.0-146-lowlatency, glibc 2.39, Ryzen 9 7900X with 24 visible
logical CPUs and 61.955 GiB host RAM. Java uses normal JIT, `-Xmx2g` and native
memory tracking. Inputs include mixed native pyramids and 96-, 1765- and
7790-contig maps. Work covers opening, pan/zoom, secondary attach, Reverse/Undo,
linked-sequence comparison, PNG export and close. The first five minutes warm up.
Browser and Java GC precede primitive-only retention samples. Original limits
and workload remain in force.

| Measurement | Result |
|---|---:|
| Frame p95 | 16.80 ms |
| Acknowledgement p95 | 58.47 ms |
| Browser heap slope | 0.05641 MiB/min |
| Browser heap early/late growth | 985832 bytes |
| Java heap slope | 0.12329 MiB/min |
| Java heap early/late growth | 2104320 bytes |
| RSS slope | -0.55494 MiB/min |
| RSS early/late growth | 18661376 bytes |
| DOM early/late maximum | 693 / 693 |
| Listener early/late maximum | 233 / 233 |

All original thresholds pass. RSS regression slope and early/late growth are
reported separately. The external workload was closed approximately halfway
through the soak. Evidence is
`/tmp/hict-u13-strand-final-soak.PXzZK3/evidence/results.json` and raw JSONL samples.
Earlier failed and interrupted records remain in version history and the
engineering chronology.

Fresh-browser and repeat-open benchmarks retain OS and server caches. These
repetitions run after the separate high-memory workload was closed. All six
samples explicitly enforce frame p95 at most 33 ms and input acknowledgement
p95 at most 100 ms.

| Contigs | Fresh/repeat open and configure | Frame p95 fresh/repeat | Input acknowledgement p95 fresh/repeat | Long tasks fresh/repeat |
|---|---:|---:|---:|---:|
| 96 | 1596.55 / 559.11 ms | 16.70 / 16.75 ms | 30.70 / 31.00 ms | 0 / 0 |
| 1765 | 889.72 / 381.57 ms | 16.70 / 16.80 ms | 29.87 / 30.25 ms | 0 / 0 |
| 7790 | 2785.85 / 1275.14 ms | 16.80 / 16.80 ms | 40.30 / 31.42 ms | 0 / 0 |

The original baseline records 16.7 ms frames and zero long tasks on the
1765-contig map. Its opening sample is 412.1/335.3 ms with an earlier runtime and
setup. Current timings include the declared raw-count graph and settling.
Evidence pointers are `/tmp/hict-u13-strand-final-release-timing-<count>-dir`.

## Examples, attribution and remaining scope

Both larger DNA Zoo examples pass fresh conversion on the corrected package.
Cheetah opens 19 contigs at 11 levels in 598.36 seconds. Aedes opens three
contigs at 11 levels in 302.04 seconds. Original 2.5 Mb source pixels independently
sum to all 576/1024 first-chromosome cells at the generated 10 Mb level. Both
first 1000 forward-reference bases match exactly, every catalog source checksum
matches, readable signal is present and unexpected browser errors are absent.
The first Cheetah oracle's unavailable-source-level assumption is retained with
its corrected continuation on the freshly converted output.

The retained tardigrade quick start reaches its map in 21.29 seconds and completes
the edit/session/export journey in 28.63 seconds. All five reference names,
lengths and forward directions and the first 1000 bases match independently.
All five catalog cards now have corrected-package evidence. One redundant-copy
disk-space preparation failure was recovered before server launch. Engineering
records retain that incident and the independent-oracle correction.

The owner selected both Borrelia Hi-C GSM7056007 and yeast Micro-C GSM9302472.
Borrelia's prepared FASTA preserves the original bases and case and matches all
19 replicon names and lengths. Yeast's public UCSC sacCer3 FASTA matches all
17 chromosomes directly. Both cards include public downloads and study citations.
The original tardigrade and both DNA Zoo validations retain conversion, readable
signal and linked-sequence evidence in `docs/examples-catalog.md`. The portable
runtime defaults to `MaxRAMPercentage=50.0`, with explicit `-Xmx` taking precedence.

Both CFF files pass the official 1.2.0 schema. The deterministic attribution
manifest contains 394 components and 248 normalized notice texts. Portable
packages include aggregate and runtime/tool legal files, including bundled BLAST.

All required current-source automated, local runtime and artifact checks pass.
Owner-run platform GUI checklists are deferred internal follow-up. Optional
private-data integration fixtures retain their declared skips. Current public `.hic` checks cover v8 and v9 source counts and
cache reuse. These records qualify the frozen application revisions above.
The final validation-record update includes acceptance helpers and release-note
consolidation after those artifacts were frozen.
