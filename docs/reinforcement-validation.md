# Reinforcement implementation evidence

## Publication qualification — 2026-11-13

This R1–R6 record supersedes the archived W/U progress below. Both repositories
remain on `jvm-sfx-native`. The execution plan prohibits push and merge and
leaves tagging to the owner. None occurred. All initially dirty paths remain
excluded from these commits.
Their original hashes are recorded in
`/tmp/hict-reinforcement-session/baseline.json`. The source plan was not edited.

**Verdict:** **GO for the qualified Linux publication/demo workflows and artifact.**
Cross-platform GUI and hosted CI qualification remain partial as detailed below.

Qualified runtime source and acceptance revisions:

- JVM `ccd98404a14265e376fb6f859f7ad3a2bf708314`.
- WebUI `cdbe49c50487882db7938983db38dc7911f7742c`.
- Frozen package `2.0.7-871ee09-webui_cdbe49c`,
  `/tmp/hict-ux-r6/final/hict-fat.jar`.
- JAR SHA-256: `7522516186ef87e7c5a3cdae68e2d9a2af24ad9bc92996338d1958b4ac5ad54a`.

The final qualification commit only updates this evidence. It does not change
runtime sources, version or the tested artifact. The bundled WebUI was extracted
and served explicitly; checkout development assets were not used for final gates.

### R1 — Zoom readability

WebUI `8cdbc65` makes the opening-owned range follow visible-tile histograms
across pyramid levels until the user edits the range, options or pipeline.
Panning within a level keeps contrast stable. Expected/O/E retain their own
ranges, and stale async fits cannot overwrite a newer options/pipeline snapshot.

The final main-map wheel gate passed **38 checks** on `combined_ind2_4DN` and
`arabiensis.0`, each with file weights and square-root coverage. Eight real wheel
steps retain diagonal signal above the stated 0.5 floor at steps 0/4/8. Screenshots
show meaningful slider positions, separate O/E bounds 0.5–2, exact Observed
restoration, and a manual Upper of 0.123 surviving further level changes.
Pan/zoom p95 was at most 16.8 ms; no page or console errors. The separate opening
gate passed **103 checks** across manual, wizard, attach and normalization paths.

Evidence: `/tmp/hict-ux-r6/final/rechecks/zoom_readability/` and
`/tmp/hict-ux-r6/final/opening_readable/`. The corrected gate targets the main map;
the earlier first-viewport locator selected the minimap. The unchanged baseline
also passed the diagonal pixel floor. The original off-diagonal washout audit's
root-cause hypothesis is therefore not conclusively established by that floor.
Do not claim otherwise. Zooming into a single empty/masked bin can legitimately
produce a white view; it is distinct from the eight-step gate on contact signal.

### R2 — Calm Visualization and Log2

WebUI `bb427cb` moves history controls to Journal while retaining keyboard
shortcuts, uses Log2 for enabling/resetting the transform, keeps custom log bases
in one Advanced section, and shows a neutral masked-bin count after automatic
masking. New-profile borders remain collapsed; existing preferences are retained.

Final packaged checks at 1440×900 and 1280×720 pass Log2, custom base 10,
masked-weight handling, collapsed borders and reachable Journal controls.
Visible sidebar buttons/inputs/selects fell from 20 to 18 on the same fixture.
A real small-window interaction failure from overlapping notifications was fixed
by WebUI `cdbe49c`, which moves toasts to bottom-center. The corrected gate uses
normal clicks without dismissing toasts. Evidence: final `publication-r2/` and
`/tmp/hict-ux-r6/toast-interaction/`.

### R3 — Quiet chrome

WebUI `12eb233` hides Dev unless `?dev=1` or the persisted About toggle is enabled,
keeps Reload tiles under Dev, renames OSD controls to Info, and defaults Info off
for new profiles. Saved preferences remain effective. Help → Connection… provides
the address dialog with modal focus and Escape handling.

Final empty/open screenshots show calm defaults. Both persisted toggle directions,
the query override, connection changes and Info refresh after wheel zoom without
further pointer movement pass. Evidence: final `publication-r3/`.

### R4 — Dense labels

WebUI `27f414c` suppresses on-map name paint when median contig span is below
roughly three pixels, including collapsed contigs. Density calculation is linear;
rulers and entity navigation remain available. Exports without a viewport keep
their requested label layout and collision/clipping rules.

Whole-map checks pass on combined_ind2, arabiensis and the 595-contig quad Orlando
assembly. Dense cases show zero painted names and working entity navigation.
Rotated PNG/SVG/PDF exports pass independently rasterized signal and viewport
checks at 45/90/180 degrees. Evidence: final `publication-r4/`, `density-extra/`
and the rotated-export suite in `functional-gates.json`.

### R5 — Neutral naming and FASTA link

WebUI `9c3e911` uses Apply .assembly file… and consistent menu ellipses. The FASTA
status chip opens the existing linker and exposes Link/Relink FASTA labels with
a link icon. Custom/legacy pipelines have one summary.

Final packaged checks verify neutral menus, the status-chip linker, Relink and
exact first-100-base equality against the physical reference. Evidence: final
`publication-r5/`. Native acceptance pages explicitly encode their Unicode text
as UTF-8; Electron and Tauri then pass the renamed menu paths.

### R6 — Packaged functional and endurance gates

All **37 distinct browser acceptance suites pass**. The authoritative merged
manifest `/tmp/hict-ux-r6/final/functional-gates.json` points to every successful
run and its source manifest. Earlier setup/stale-selector failures and rechecks
are retained rather than discarded. Coverage includes structural edits and exact
Undo/Redo, Group, journal paging, session save/restore and validation, multi-client
conflicts, normalization and O/E, presets and graphs, tracks/histograms, bookmarks,
scopes, errors, offline/cancelled tiles, mixed pyramids, native exports, metadata,
citation, examples, real search, BLAST, ModDotPlot and alignment detail.

The fresh-profile Borrelia journey opens in three clicks, verifies more than
1,000 main-heatmap signal pixels after each of two real toolbar zooms, and passes
Reverse, Split/Undo, save/close/restore and exact AGP export. Restored signal:
65,893 pixels. No page or console errors. See final `journey-audit/quickstart_example/`.

The real 221 MB .hic v8 conversion includes the full pyramid and balancing;
77,841 raw values match an independent hictk dump exactly. The v9 fixture matches
all 25 values. Both inputs retain their SHA-256, render at the queried resolution,
and reopen from conversion cache without another job. The format runner retains
its 120-second default and uses explicit `--job-timeout 600` for this large input.
See final `formats-final/matrix_input_formats/`. ModDotPlot triangle preservation
explicitly selects Below diagonal; the current Overlay selection default has its
own passing UX gate. Numerical/pixel assertions were not relaxed.

- Full smoke/build: **500 JVM tests, zero failures/errors, seven skips**;
  WebUI regression contracts, typecheck and production build pass.
- Native baseline parity passes; pinned-WebUI checkout CI checks pass **4/4**.
- Linux native Electron **14 checks**, Tauri **25 checks**, and real Swing Launcher
  reset/settings/minimum-window/start/Stop process-tree checks pass.
- **16 real-tool sequence lifecycle cases** pass, including BLAST, ModDotPlot,
  overlay accumulation, search/IUPAC/strands/exact BED, alignment and missing tools.

Evidence: final `electron-recheck/`, `tauri-recheck/`, `launcher.log`,
`sequence_lifecycle/summary.json` and the merged functional manifest. The packaged
build used `scripts/run_smoke_tests.sh` and
`./gradlew testNativeProcessingBaseline shadowJar`; checkout checks used
`node --test scripts/portable/check_webui_checkout.mjs`. No Java runtime source
changed during this R1–R6 pass. Builds used Java 21 and installed Chromium 156
with Playwright 1.65 on Linux; native GUI checks ran on private Xvfb.

The **full two-hour soak passed**: 7200.312 seconds, 933 completed
cycles, 933 samples, zero page/console errors and driver exit 0. It alternates
combined_ind2, arabiensis and mixed-primary with linked sequence/secondary data;
cycles exercise navigation, resolution menus, edits/Undo, sequence, exports and
close. A single browser context and normal JIT are retained throughout. Memory
samples follow close and explicit GC; these are retained-memory measurements,
not claims of maximum active workload memory.

| Final post-warmup metric | Measured | Gate |
|---|---:|---:|
| Browser retained-heap growth | 1.148 MiB | ≤8 MiB |
| Browser heap slope | 0.0212 MiB/min | ≤0.2 MiB/min |
| JVM heap slope | 0.0273 MiB/min | ≤0.5 MiB/min |
| JVM RSS slope | -3.6665 MiB/min | ≤1 MiB/min |
| DOM early/late maxima | 594 / 595 | increase ≤100 |
| Listener early/late maxima | 215 / 215 | increase ≤20 |
| Frame p95 | 16.8 ms | ≤33 ms |
| Action acknowledgement p95 | 56.46 ms | ≤100 ms |

Sampled JVM heap ranged from 33.15 to 38.79 MiB. Sampled process RSS ranged from
213.73 to 1444.96 MiB, including native allocations;
the falling RSS trend must not be read as negligible peak memory use. JVM native
categories were recorded using `-XX:NativeMemoryTracking=summary`.
All thresholds were retained. Evidence: final `soak/results.json`,
`soak/samples.jsonl`, `soak.log` and `run-soak.py`. The runner invokes
`scripts/check_release_soak_browser.py` with `--duration-seconds 7200`,
`--server-pid` and `--native-memory`; it uses disposable physical fixture copies
and the explicitly extracted bundled UI. The owned server and browser were
closed when the driver finished.

### Limits and owner release actions

This is Linux qualification of the identified artifact and exercised workflows,
not proof of every possible dataset, UI state or operating system. Windows/macOS
interactive checks in `docs/release-manual-checklist.md` remain owner-deferred by
the plan. Hosted GitHub Actions for these local commits remain unverified because
the plan prohibits pushing; both branch workflows include `jvm-sfx-native`, and
local build/regression/checkout checks passed. Do not describe hosted CI as green.
No release was published or tagged. Tag/version choice and cross-platform owner
checks remain external release actions. No comparative performance claim is made.

---

## Archived reinforcement progress

The sections below describe earlier W/U increments and their historical status;
they do not override the current qualification above.


## Scope and status

Started from `plan-reinforce-final.md`, in its prescribed W0-first order.
The first change was a **W0 foundation increment**, followed by real HDF5 fixtures
and source capability integration. The W0 foundation is now ready for W1;
the later package gates remain unqualified. W1 has started with primary-map
replacement failure protection; W2–W16 have not been started. In particular, session loss,
unfinished replacement transactions, stale sequence,
armed scissors after Escape, settings persistence and mixed-resolution failures
still block release.

The direct instruction to commit now supersedes the earlier review-only ban.
All pre-existing changed/untracked files and `plan*.md` are protected. Only paths
introduced or edited for this increment are eligible for staging. No push or merge.

Baseline: JVM `0dbcb5f42a7d06eab178c0197e57d5478e138105`, WebUI
`8107d95fa6234cc67bed31652e8ad7543e2eba4a`; both indexes empty, WebUI clean.
SHA-256 of every pre-existing untracked/modified file and all plan files was
recorded locally in `/tmp/hict-reinforce-w0/baseline.json` before editing.

## Implemented foundation

- Shared synthetic matrix, FASTA, projection and graph fixtures, with zero rows,
  literal Log2 values, reverse slices, gaps, ambiguous bases and a custom multiplier.
- Production-code regression tests for every nonempty sequence subwindow, plain
  and gzip input, source coordinates, graph serialization and numerical values.
- Explicit coordinate, source capability and job contracts in
  `reinforcement-contracts.md`, with future contracts distinguished from runtime.
- Browser helpers for actual FASTA selection and source buttons, corrected
  user-triggered range scanning and preset selection, and order-independent ruler
  lane intersection checks.
- Changed-content FASTA relink oracle checks both stationary axes. A mere request
  or increased revision no longer proves success.
- Missing dotplot levels must retain a rendered layer. The former assertion that
  no layer was visible encoded the defect as expected behavior and is removed.
- Rotated exports use a rounded content envelope allowing captions and rulers,
  retaining signal checks, independent SVG/PDF rasterization and viewport restoration.
  This envelope is not a complete text-clipping/pixel-parity oracle; W16 still needs it.
- Reusable fresh-browser/repeat-open navigation diagnostic records action round-trip
  and settled time, frame distribution, long tasks, resource timings, heap and DOM.
  It does not label Playwright click latency as human input acknowledgement.

## Reproducibility

Run tests with:

```sh
./gradlew test -x copyWebUI --tests '*ReinforcementFixtureTest'
./gradlew test -x copyWebUI
```

Both passed: 315 Java tests, zero failures/errors, seven skipped optional tests.
The two added tests passed. WebUI application source was not changed, so no new
WebUI build is claimed. Browser runs use the previously built JAR
`hict_server-1.0.216-50ae800-webui_8107d95-fat.jar` and existing WebUI dist.
This is a baseline of current runtime, not qualification of a new distributable.

Machine: AMD Ryzen 9 7900X, 64,964,784 kB RAM, Linux 6.8.0-146-lowlatency,
1440×1000 headless Chromium. Playwright runs from the existing validation Python
environment. API `127.0.0.1:5202`, WebUI `127.0.0.1:8203`; physical/reflink copies
under `/tmp/hict-reinforce-w0/data`. No symlink or hardlink to original inputs.
The local `runtime-manifest.json` records the following inputs:

| Input | Bytes | SHA-256 |
|---|---:|---|
| combined_ind2_4DN.hict.hdf5 | 154317488 | `8cce355723881effce56ea8da89e25ca3087ebb30159ef13c7e85af20ca5f317` |
| ind2.self.hict.hdf5 | 51572875 | `d6b75919eacb551f35d92b218a8a67e97114297ff0909c491ab14c58c59fb6ed` |
| quad_combined_ind2.fasta | 294768927 | `6fdc6272679e228e485a2c782dbb5ad07cb294c49029613ed1ac90be55cc8106` |

These user-provided data are referenced by checksum and not redistributed in git.
Source-publication provenance is not asserted for these local test inputs.

## Interactive baseline results

Evidence: `/tmp/hict-reinforce-w0/`, including commands and exit codes in
`browser-results.json`, per-suite logs, screenshots, exported files and JSON.
Initial helper failures are preserved as `*.initial`; they are harness mistakes,
not additional application defects. Current helpers select file rows explicitly
and expand the range disclosure before scanning.

| Check | Result | Outcome verified |
|---|---|---|
| Exact signal range | Pass, 6.52 s | User-triggered primary scan, nine blocks, pan does not rescan, independent secondary bounds, return reuses cache |
| Edited view | Fails at missing repaint | FASTA is linked by UI; reversal succeeds but the stationary sequence axis never repaints. Later assertions remain unexecuted |
| Large FASTA | Fails at changed-content relink | Bounded 64 MiB Java indexing, 100 independent sequence intervals, 30 rapid pans, 76 letters/colors, 76 SVG letters and delayed-response recovery pass first |
| ModDotPlot zoom | Fails at missing 10K layer | Two real 5K computations preserve 339,729 primary pixels each; secondary threshold changes preserve primary pixels. Preset assertions after the failure remain unexecuted |
| Rotated export | Current assertions pass, 16.49 s | Four rotation/scope combinations × SVG/PNG/PDF, rounded content envelope, signal present in independent rasterizations, exact viewport restoration |
| Navigation diagnostic | Recorded separately | Forty actual map zoom-button actions across two opens; see controlled raw-count baseline below |

No browser page errors or server errors were recorded in the dotplot run. Its
failure is the intended W8 regression, not a generation failure. The export pass
does not establish complete glyph clipping or cross-format pixel equivalence.

The first navigation run reproduced nearly blank default rendering (R3). Retained
under `reinforcement_baseline/`, it is not the representative signal-rendering
measurement. `reinforcement_baseline_raw/` explicitly selects PRIMARY raw counts,
linear red, 0–100, both triangles before measurement. The measured opening duration
includes this setup. Fresh browser does not mean cold disk/server cache. These are
short local diagnostics, not W16 latency, memory or platform qualification.

Controlled baseline, Chromium 156.0.8078.4:

| Measurement | Fresh browser | Repeat open |
|---|---:|---:|
| Open + controlled graph + settled map | 412.1 ms | 335.3 ms |
| Frame interval p50 / p95 | 16.7 / 16.7 ms | 16.7 / 16.7 ms |
| Recorded frames | 365 | 364 |
| Observed long tasks over 50 ms | 0 | 0 |
| Tile/matrix resource entries | 65 | 36 |
| Tile/matrix resource duration p95 | 46.82 ms | 17.60 ms |
| Playwright click round-trip p95 | 36.38 ms | 36.12 ms |
| Zoom action to settled map p95 | 306.76 ms | 304.35 ms |
| JS heap after forced GC | 19,857,504 bytes | 25,694,328 bytes |
| DOM nodes / listeners | 1,746 / 669 | 1,844 / 675 |

The heap increase is not a plateau result; W6's repeated-open retention diagnosis
remains required. Timing includes browser instrumentation and only the declared
zoom workload. Visible red contact signal was checked in the saved screenshots.

## W0 handoff to W1

The eight W0 foundation deliverables are present. This is readiness to begin the
ordered implementation packages, not certification of their target behavior.

| W0 requirement | Evidence |
|---|---|
| Repository baseline and protected changes | Original HEAD/status/index and SHA-256 manifests; verified before every commit |
| Deterministic and real fixtures | Numerical/sequence/alignment oracles, generated real HDF5 pyramids, complete synthetic catalog cases, legacy preset/session envelopes and custom graph; real input hashes above |
| Coordinate contract | `reinforcement-contracts.md`, production sequence projection and tiny HDF5 read/reversal tests |
| Source capabilities and applicability | Versioned Open/Attach/secondary DTOs, declared source metadata, resolution-specific imported weights and disabled controls with reasons; HTTP and browser tests |
| Common job contract | Target states, transitions, revision identity and publication rules in `reinforcement-contracts.md`; runtime implementation belongs to W13 |
| Control inventory | 18 menu contracts and 232 signature contracts with IDs, prerequisites, expected effects and explicit inspected/pending status; five merged signatures still need instance-specific audit |
| Stale harness repairs | Correct selectors, user-triggered scan, FASTA linking, ruler lane ordering, mixed-resolution failure oracle and rotated export envelope; known application failures retained |
| Performance baseline | Declared hardware/browser/input hashes, fresh/repeat runs, zoom and drag-pan outcomes, event-to-view/render timings, frame distribution, long tasks, tile timings/counts and heap |

Some earlier entries described later-package validation as unfinished W0 work.
Those are not dropped: source fingerprints and transactional sequence lifecycle
belong to W1/W3, complete catalog schema validation to W15, continuous independent
pyramid display to W8, retention diagnosis to W6, and full caption parity and
representative performance qualification to W16. Their tests must consume these
fixtures and contracts. No schema acceptance, migration correctness, exhaustive
control coverage, memory plateau or release readiness is claimed here.

## Second W0 increment: source metadata and real HDF5 fixtures

Baseline JVM `7f8740d0`, WebUI `8107d95`, unchanged protected files. Evidence root
`/tmp/hict-reinforce-w0-capabilities/`; API/WebUI ports 5204/8204, disposable data.
Runtime uses newly compiled JVM code and the rebuilt WebUI, not the earlier review
artifact. The development JAR was built with `shadowJar -x copyWebUI` and an explicit
WebUI dist override. Full packaged qualification remains W16.

Changes:

- Conversion now preserves declared source semantics and records whether weights
  actually came from each source resolution. Legacy metadata stays unknown.
- `/open`, Attach and secondary status carry versioned capabilities; the OpenAPI
  opening field names also now match the existing `pixelResolutions`, `tileSize`
  and `matrixSizesBins` wire fields.
- The selected source's inapplicable contact-scaling and imported-weight controls
  are disabled with one accessible explanation. A source switch and a change of
  resolution update applicability. Unknown legacy sources retain prior controls.
- Real tiny Cooler-to-HiCT fixtures exercise pixels, zero rows, partial bins,
  disjoint/fine/coarse pyramids and reversal through production I/O. Separate
  fixtures cover actual imported weights and generated identity metadata.

Validation:

- Full Java suite: 323 tests, 316 passed, seven optional skips, zero errors/failures.
  Includes HTTP Open/Attach/secondary/detach consistency and real conversion weights.
- `npm run build:web`: type checks, all configured regression groups, capability
  parser tests and production build pass. Existing large-chunk warnings remain.
- ESLint without fixes on touched WebUI files: no reported lint violations;
  the installed plugin emits existing directive-schema reference warnings.
- Browser capability scenario: 25 exact matrix cells; 4 bp imported weights
  available versus 10 bp absent; selected identity source blocks contact controls;
  returning to primary restores availability; fresh-window File Attach restores
  both capabilities; identity opened as primary remains non-contact; legacy map
  retains unknown metadata and usable controls. No page or HTTP errors.
- Initial browser setup failures were harness issues: the tiny map's fit constraint
  kept its finest level selected, and a secondary source absent from the graph did
  not show source selection. The final test selects a single enabled level and
  includes both sources in its graph. This does not certify W8 zoom interpolation.
- Existing exact-range, pipeline-editor (including stale cached graph and delayed
  threshold protection), and multi-client Attach/conflict browser regressions pass.
  A launcher attempt passed an unsupported `--data-dir` to the pipeline script;
  the corrected invocation passed. It was not an application failure.
- Generated fixture and runtime artifact hashes are recorded in the local
  `manifest.json`. No real or generated binary data are added to git.
- One development restart failed because the running JVM's JAR was rebuilt in
  place before it had exited. The old process logged missing Netty classes and
  retained its ports during shutdown; its thread dump was saved. This is a test
  orchestration error, not evidence of a capability regression. That owned process
  was terminated, the final artifact was started in a fresh JVM, and the capability
  journey passed again. Future builds must stop the JVM first or use a new JAR path.

The disabled controls do not rewrite existing/custom rendering graphs. Numerical
processing transactions and legacy migration remain W2. Contact applicability is
not a claim that square-root normalization or FASTA-only runtime is implemented.

## Third W0 increment: repeatable menu coverage

Added 18 source-backed menu contracts, a fresh-context browser runner, and the
preserved 232-signature historical fixture with deterministic IDs and source search
candidates. The source search finds candidates for 170 signatures; none is promoted
to functional coverage automatically. Detailed prerequisites and effects for the
232 signatures remain manual work; W0 remains incomplete.

All 18 destination opening assertions passed at 1280×720 on the same development
runtime, including the actual API documentation popup. Three sequence dialogs
settled to missing-FASTA guidance, disabled computation and opened the FASTA picker
from their recovery buttons. No page exceptions or HTTP errors were recorded.
Evidence: `/tmp/hict-reinforce-w0-capabilities/menu-surfaces-final/`.
The initial opening-only run is retained separately. Python syntax, fixture IDs,
source paths and protected-file hashes were checked. This increment changes only
diagnostics, fixtures and documentation; no additional application build is claimed.

## Fourth W0 increment: authored control expectations

All 232 historical signatures now have an explicit contract with prerequisites,
expected effect and coverage status. Source anchors were inspected for 227;
five ambiguous signatures remain pending and explain why their surface entry
point is not a confirmed element owner. Runtime functional outcomes remain
separate and pending. See `reinforcement-control-coverage.md` and
`src/test/resources/reinforcement/control-contracts.json`.

The reconciler validates source anchors and refuses stale references or static
claims of runtime success. New captures retain element HTML and dialog ownership.
They confirmed that unnamed close buttons and the map zoom-slider thumb collide
under the historical signature scheme. The contract distinguishes their outcomes.
All 18 menu surfaces and three missing-FASTA recovery paths passed again with no
page or HTTP errors, using the unchanged development application artifact.
Evidence: `/tmp/hict-reinforce-control-contracts/`. No Java or WebUI application
code changed in this increment; syntax and contract consistency were checked.

## Fifth W0 increment: complete fixture documents and input baseline

Added standalone complete synthetic catalog and nine negative catalog documents,
a legacy preset export, legacy v2 session and a custom processing graph. Their
structure and internal references were checked against the declared contract and
existing serializer. They remain inputs for the later production validators, not
proof that those validators exist or accept the documents. The fixture README
marks all catalog URLs and citations as synthetic and prohibits fetching them.

The browser baseline now verifies 20 zoom actions and six drag pans per opening,
for 52 actions across fresh and repeat opens. Timing starts in a DOM capture
listener. Center/resolution property events mark the first actual view update;
the subsequent map postrender marks a rendered frame, not screen presentation.
Event dispatch delay uses injected browser input and is not hardware latency.
An initial harness used generic OpenLayers change events, which can arrive at
animation completion; those measurements are retained separately and superseded
by `property-events/`. Another initial read preceded the postrender callback;
the runner now explicitly waits for that callback. Neither issue is an app defect.

Evidence: `/tmp/hict-reinforce-baseline-complete/property-events/`, same declared
machine, Chromium and development runtime as the prior capability runs, with
1440×1000 viewport and a controlled raw-count graph. No page or HTTP errors.

| Metric | Fresh browser | Repeat open |
|---|---:|---:|
| Frame p50 / p95 (ms) | 16.7 / 16.7 | 16.7 / 16.7 |
| Zoom-in first view change p95 (ms) | 12.67 | 14.04 |
| Zoom-in first rendered frame p95 (ms) | 29.87 | 31.11 |
| Zoom-out first view change p95 (ms) | 13.30 | 13.48 |
| Zoom-out first rendered frame p95 (ms) | 31.02 | 31.32 |
| Drag-pan first view change p95 (ms) | 32.43 | 32.28 |
| Drag-pan first rendered frame p95 (ms) | 33.48 | 33.30 |
| Long tasks over 50 ms | 0 | 0 |
| Heap after forced GC (bytes) | 23,258,040 | 32,569,880 |

These short samples are a reproducible baseline, not a responsiveness budget,
large-workload result or memory plateau. The heap increase remains a W6 concern.
This step changes diagnostics, fixtures and documentation; no new application
build is claimed.

## W1 first increment: preserve the live map on failed primary replacement

The primary `/open` handler now builds and validates the candidate map, response,
statistics and track manager before changing the published session. Failure closes
candidate resources and leaves the old wrappers, workspace, linked FASTA metadata
and secondary attachment intact. Successful publication then retires the old
resources; cleanup failures are logged without reporting a failed open after the
new map has already been installed. Stale secondary sequence metadata is cleared
on successful primary replacement.

The first browser run exposed a separate client defect: the manual file-selection
handler called `resetState()` before the request. The client now retains its map
manager, filenames, scope state and workspace baseline until the server accepts
the replacement. File-specific resets happen after success. The deep `ref` holding
the map manager remains unchanged.

Validation and evidence (`/tmp/hict-w1-primary-replacement/`):

- Real-HDF5 HTTP regression opens primary/secondary, links FASTA, reverses the
  assembly, then attempts a missing file and repeated malformed HDF5 opens.
  It compares resource identity, workspace/statistics identity, AGP and exact
  matrix values. A deliberately unusable processed-directory path forces failure
  after candidate HDF5 preparation; the prior map remains readable. Finally,
  reopening the same physical HDF5 path succeeds and returns its original pixels.
- Full Java suite: 324 tests, 317 passed, seven optional skips, no failures/errors.
  Development `shadowJar -x copyWebUI` also built successfully before launch.
- WebUI `npm run build:web` passed all configured checks and production build.
  ESLint without fixes reported zero errors and two existing benchmark-hook `any`
  warnings; the installed plugin still emits directive-schema reference warnings.
- Browser setup uses the benchmark opener, actual FASTA picker, secondary and
  reversal APIs, then actual File Attach to load the edited assembly. Two actual
  Manual Open failures preserve 25 exact reversed pixels, primary/secondary/FASTA
  metadata, workspace, edit journal and client manager. A fresh browser reload
  and File Attach preserve the same state. No page exceptions or unexpected HTTP
  errors. The two failed `/open` responses are expected.
- Existing multi-client Attach, stale-workspace conflict and same-path reopening
  regressions pass on a separate physical copy of the medium map.
- Harness development failures are retained: an initial run started before the
  WebUI listener was ready; an early Attach comparison mistook the first automatic
  workspace save for a replacement side effect. The final runner waits for the
  original window's initial save before attaching and for Attach completion. The
  observed client reset before its fix was an application defect, not a harness
  problem. Initial test compilation mistakes were corrected before passing runs.

This is a failure-protection increment, not the entire W1 transaction. The assembly
scheduler serializes assembly mutations; it is not a global read lock. Coherent
publication to concurrent readers, stale client responses, failures after server
publication/client installation, secondary replacement, FASTA/AGP steps, session
v3 validation/restoration and dirty-state prompts remain W1 work. Existing verbose
and duplicate error notifications remain visible in screenshots and belong to
error-presentation cleanup. No release-readiness claim is made.

## W1 second increment: secondary preparation and layout rollback

Secondary attachment now prepares alignment, compatibility and its response before
publishing the candidate or closing the old source. When secondary is the active
assembly source, a failed preparation restores the primary assembly snapshot.
Shared overlay synchronization also restores its target snapshot and raises an
error instead of swallowing a failed import and claiming success. Assembly-source
selection is published only after synchronization succeeds. Successful secondary
replacement clears the previous secondary FASTA path/name; rejected candidates and
cancelled compatibility warnings retain it.

Primary FASTA alias construction now precedes publication of the new FASTA paths.
The alias map is validated before it is handed to the track manager. This reduces
a partial-publication path; it does not solve changed-content indexing or stale
sequence responses, which remain in W3.

Evidence: `/tmp/hict-w1-secondary-replacement/`.

- A real-HDF5 HTTP regression injects an IOException **after an actual AGP import**
  has changed primary's layout while secondary is the active assembly source.
  Repeated failed replacements preserve both original AGPs, reversed matrix values,
  old secondary wrapper, status and FASTA link. A failed assembly-source switch
  keeps its prior selection. Removing the injected failure lets the replacement
  succeed, aligns the primary to the new source and clears the old FASTA link.
  Fault injection exists only in the test subclass, not in the runtime.
- Rejected missing-file and unconfirmed mismatching FASTA relinks retain the
  published primary and secondary path objects and filenames.
- Full Java suite and development shadow JAR build passed: 326 tests, 319 passed,
  seven optional skips, zero failures/errors. No WebUI source changed in this step.
- Actual Tracks and layers actions rejected an invalid secondary, cancelled a
  mismatching-resolution warning, and attached a valid replacement. Both 25-cell
  edited matrices and the journal were unchanged on failure/cancellation; the
  accepted candidate reproduced the reversed primary pixels exactly. No browser
  page exceptions or unexpected HTTP errors.
- The earlier edited-session primary-failure browser journey passed again,
  including fresh-client Attach, preserved workspace and edit journal.
- The source-capability browser regression passed all five checks, including
  per-resolution weights, generated identity sources and fresh-client Attach.
  Screenshot review still shows stacked low-level Axios error notifications after
  the rejected file; readable, deduplicated error presentation remains outstanding.

The same W1 limits remain: no global reader transaction, no complete stale-response
policy, and no session v3 or dirty-state workflow yet. The source-switch success
path and every AGP/FASTA error stage still need the complete W1 acceptance audit.
A successful secondary replacement when secondary drives the assembly also needs
client-model refresh verification. These results do not certify all of W1.

## W1 third increment: refresh the assembly after secondary replacement

A browser diagnostic with secondary driving the assembly reproduced a stale client
strand: the server's 25-cell matrix changed from reversed to forward, while the
client's contig descriptor remained reversed. Evidence is in
`/tmp/hict-w1-secondary-replacement/secondary-driven/` (diagnostic, not a passing
acceptance run).

Successful secondary opens now include the synchronized assembly. Both ordinary
attachment and compatibility-confirmed attachment apply that assembly to the
client after updating its secondary resolution model, refreshing contigs,
scaffolds, scopes, visuals, overview and track caches. The dialog now explains
that replacing a secondary which drives assembly adopts the replacement's layout.

The expanded browser check then exposed a second failure: returning secondary's
native resolution descriptors for a disjoint pyramid caused `Can't get prefix sum
for resolution 4`. The final open and assembly-source-switch responses use the
synchronized **primary map grid**, which the client's coordinate model requires.
The response schema documents this. Secondary rendering retains its separate
resolution model; this does not establish the complete W8 overlay gate.

The Java regression compares the returned assembly to the primary model for both
matching and disjoint pyramids, then switches both assembly sources. Its initial
assertion also exposed differing default scaffold spacer metadata after AGP import
(0 on the synchronized target versus 1000 on the original source). These defaults
do not alter the tested one-contig intervals, but lossless session serialization
must not assume an AGP round trip preserves every metadata field.

Final evidence lives under `/tmp/hict-w1-secondary-replacement/client-refresh-*`.
The full Java suite passed with 326 tests, 319 passed and seven optional skips.
The secondary-driven browser run passed failure retention, cancellation, exact
25-cell replacement values with updated strand, confirmed mismatched attachment,
and assembly-source switching with disjoint pyramids, without page exceptions or
unexpected HTTP errors. Screenshot review confirms the new source and source
selection are visible; the old stacked-notification UX issue remains. This is a
focused state/model test, not pixel-perfect validation of every overlay layer.

The primary-driven browser journey also passed all five checks. Final WebUI build,
changed-file ESLint (no file warnings/errors), and API DTO/source-capability checks
passed. ESLint still emits its existing directive-schema resolver notices, and the
build reports its existing bundle-size warning. W1 session v3, dirty-state prompts,
complete stale-response handling and concurrent-reader publication remain open.

## W1 fourth increment: reject malformed session containers before mutation

`SessionDocument.ts` now validates legacy imports before file listing, filename
resolution or opening. Only explicit versions 1 and 2 are supported at this stage;
missing and future versions fail with a field-specific message. Limits are 8 MiB
(measured as file bytes before reading and UTF-8 bytes in the parser), 32 nesting
levels and 100,000 visited values. Non-finite JSON numeric results and reserved
prototype property names are rejected. Validation covers filename/background
string types, object containers, finite two-coordinate views, positive resolution,
bookmark coordinates and IDs, preset IDs/names, and axis visibility ID arrays.

The UI explains legacy imports cannot restore the edited assembly, full processing
graph or undo history. This is a compatibility notice, not complete v3 migration.
Nested visualization/style/action semantics, graph grammar, cross-references,
fingerprints, full v3 save/restore, and backend-stage transactional restoration
remain outstanding. In particular a well-formed legacy document can still fail
later while opening referenced FASTA/AGP; this increment does not certify the W1
atomic-session gate.

Evidence: `/tmp/hict-w1-session-validation/browser-final/` plus
`/tmp/hict-session-*.log`. Eight actual file-chooser uploads (future version, null,
missing version, short center, zero resolution, wrong filename type, malformed
bookmark and visibility list) preserved manager identity, exact reversed matrix,
contig strand, view center/resolution and edit journal. No replacement/mutation
requests or browser exceptions were recorded. A newly downloaded v2 settings file
then restored successfully with the legacy notice. An initial extended-browser
harness run failed because Python Playwright has `remove_listener`, not
`remove_all_listeners`; the harness was corrected and the complete run passed.

`node scripts/check-session-documents.mjs` covers valid v1/v2 and malformed fields,
non-finite numbers, size/depth/value bounds and reserved keys. WebUI build and API
DTO regressions passed. Changed-file ESLint reports only the two existing `any`
warnings in MainUI and existing resolver notices. No Java runtime code changed;
the browser used the previously validated immutable 1.0.223 development JAR with
the newly built external WebUI. Packaged release qualification remains W16 work.

## W1 fifth increment: nested legacy session validation

The pre-mutation validator now checks visualization boolean flags, logarithm bases
(base one is invalid; non-positive values retain the existing disabled-stage
convention), quantiles, NaN counts, known signal/weight policies, gradient bounds
and colors. Both track style records must be complete when supplied; numeric
width/label fields, border/name-placement enums and label booleans/colors are
checked. Saved presets use the same validators and validate their action variants,
palette colors, signal thresholds and duplicate IDs. Bookmark IDs must also be
unique. Color parsing uses the application's existing ColorTranslator library.

Two compatibility cases were verified against actual saved documents. Legacy
`trackStyles: {}` means no saved style and is removed from the parsed document
before application (the previous loader passed it to a routine requiring both
contigs and scaffolds). Action presets may carry an `Unknown` colormap placeholder;
it is accepted only in a preset with a validated action, while ordinary unsupported
colormap types fail visibly. An initial overly strict check rejected this known
representation; fixture tests caught it before committing.

Evidence: `/tmp/hict-w1-session-nested/` and `/tmp/hict-session-nested-*.log`.
The browser's fourteen malformed chooser uploads now include string-valued weight
flags, base-one logs, bad colors, partial styles, reversed ranges and invalid
preset actions. All retained the edited matrix, manager, view and journal without
mutation requests. The current UI's downloaded v2 session restored successfully.
Node tests also load the repository legacy fixture and a real downloaded session.
WebUI build, changed-file ESLint and API DTO regressions passed; only existing
ESLint resolver notices and the build's bundle-size warning remain.

This completes more of the legacy field validation, not session format v3. Full
processing-graph validation/persistence, source fingerprints, assembly round-trip
verification, cross-references and transactional restoration across backend stages
remain W1 work. No Java runtime code changed in this increment.

A separate browser run also restored `legacy-session-v2.json` with its empty style
bundle and FASTA reference, after all fourteen rejection checks, with no browser
exceptions. Both successful restore paths therefore have runtime evidence.

## W1 sixth increment: server capture foundation for v3 sessions

The new `/session/snapshot` endpoint captures both open sources' AGP text and its
SHA-256, name overrides, detailed assembly descriptors, filenames, FASTA references
and capabilities. It includes the rendering graph's entire serialized expressions
(including arbitrary multipliers), track summaries/definitions and the persisted
workspace envelope. Snapshot version 1 identifies this internal capture contract;
it is **not** saved-session format v3. The existing Save session UI remains legacy.

Capture runs on the assembly scheduler and does not add a journal entry. A client
with displayed history state sends its session/revision; the history interceptor
rejects a stale capture with 409. The client capture method does not advance its
displayed assembly revision merely by reading a snapshot. Missing open maps return
404. The request, request DTO, client method, response DTO and OpenAPI contract were
added together.

Evidence: `/tmp/hict-w1-session-snapshot/` and `/tmp/hict-snapshot-*.log`.

- Full Java suite and development JAR build passed: 327 tests, 320 passed, seven
  optional skips. Real HDF5 HTTP tests verify the edited strand, renamed contig,
  exact custom graph, both sources, independently computed AGP digest, stable
  repeated capture and changed digest after a subsequent assembly edit.
- The browser opens two real fixture maps, links FASTA, opens a BED track, reverses
  the assembly and uses actual File Attach. It installs the custom graph, changes
  zoom and waits for the real workspace autosave response. The client capture then
  matches sources, edited strand, graph, track and persisted workspace. Capture
  leaves the journal unchanged. A second client's edit produces a 409 for the old
  displayed model; File Attach recovers and captures the new hash and strand.
- Browser page exceptions: zero. WebUI build and API DTO checks passed. ESLint has
  only the five existing unused-import warnings in request.ts/requestDTO.ts and
  existing resolver notices.
- Initial browser execution preceded server readiness and was rerun after HTTP
  readiness on the same JVM. The workspace check initially used a clamped center
  change that did not trigger autosave; the final test changes resolution and waits
  for its actual successful response. Neither failed harness attempt is counted
  as passing evidence.

Remaining: content fingerprints for all referenced files, generated-result
provenance validation, AGP round-trip representability and hash checks, strict v3
schema/graph/reference validation, atomic restore, UI Save/Open integration and
dirty-state handling. The snapshot is serialized with assembly mutations, but
concurrent visualization/workspace writes still need the complete session
transaction protocol. W1 is not complete and release qualification remains open.

## W1 seventh increment: isolated assembly reconstruction and fidelity checks

`SessionAssemblyRestorer` reconstructs an unpublished candidate from captured AGP,
checks its SHA-256, component identities, source intervals, strands and bin counts,
restores visibility and original/display names, and reconstructs scaffold metadata
that AGP alone does not retain (stable IDs and default spacer lengths). It then
requires the complete assembly DTO and regenerated AGP hash to match. Any failure
restores the candidate checkpoint. Name restoration changes in-memory maps only.
The helper assumes a structurally validated capture; complete external v3 document
validation is still required before it becomes part of the restore endpoint.

Snapshot requests can opt into `verifyAssemblyRoundTrip`. The server opens and
closes independent primary/secondary candidates, reconstructs them and compares
canonical native contact-bin mappings with the live sources, including stripe,
range and direction at each resolution. Only successful verification returns
`assemblyVerified: true`. Existing snapshot requests remain lightweight by default.
The client rejects missing/false verification when verification was requested,
including an older server that ignores the new option. No Save/Open UI migration
or live-source replacement is part of this increment.

Evidence: `/tmp/hict-w1-assembly-restorer/` and
`/tmp/hict-session-restorer-*.log`.

- Real-HDF5 tests reconstruct reversed and renamed assemblies with their scaffold
  defaults, and split/reversed components with an annotated gap. The latter test
  compares every matrix cell at both fixture resolutions, besides exact snapshot
  equality. Hash, strand and display-name corruption trigger candidate rollback.
  A deliberately altered native mapping is rejected even with matching lengths.
- Full Java suite and development JAR: 330 tests, 323 passed, seven optional skips.
- Five final browser checks pass with explicit verification of both sources,
  custom graph, BED track, FASTA and persisted workspace. The journal is unchanged,
  stale clients receive 409, fresh Attach recovers, and an injected older-server
  response without verification is refused. Browser page exceptions: zero.
- WebUI build, API DTO tests and changed-file lint passed with the five existing
  unused-import warnings and resolver notices. An initial build failed TypeScript
  narrowing in the request factory, leaving the previous bundle in dist; the
  browser assertion caught its unverified request. The cast was corrected and
  fail-fast build/check commands and the complete browser run subsequently passed.

Component IDs must currently survive AGP reconstruction; a layout requiring an ID
remap fails rather than publishing incompatible references. Fingerprints, generated
provenance, comprehensive v3 validation, cross-resource restoration, atomic
publication, dirty prompts and export progress/cancellation for large captures
remain. Explicit verification opens candidate files and can be expensive on large
maps; it is not enabled in the legacy Save UI. W1 and release qualification remain
incomplete.

## W1 eighth increment: content fingerprints for session resources

Snapshot requests can opt into `fingerprintFiles`. Matrix sources and linked FASTA
receive SHA-256/byte-length identities; physical track definitions receive their
own identity. Virtual Cooler-weight tracks reference PRIMARY or SECONDARY instead
of attempting to hash an internal pseudo-path. A requested fingerprint operation
must succeed for every referenced resource before `filesFingerprinted: true` is
returned. Clients reject older-server responses that omit that confirmation.

`SessionFileFingerprint` streams contents with a bounded buffer, reports byte
progress, accepts a cancellation predicate and observes thread interruption. It
checks size, modification time, file key and resolved path before/after hashing,
and refuses missing, non-regular or ordinarily changing resources. It deliberately
has no size/mtime cache: changed bytes with unchanged size and restored timestamp
produce a different identity. Timestamps are not part of content equality.

Evidence: `/tmp/hict-w1-session-fingerprints/` and `/tmp/hict-fingerprint-*.log`.

- Full Java suite and development JAR passed: 333 tests, 326 passed, seven optional
  skips. Tests cover a known SHA-256 vector, same-size/same-time replacement,
  progress, cancellation/recovery, changes during hashing, directories and missing
  files.
- Seven browser checks passed with no page exceptions. Python independently
  computes both matrix digests plus FASTA and BED digests and compares the complete
  returned fingerprints. A virtual weights track references its primary source.
  Temporarily renaming the disposable BED rejects capture, leaves the journal
  unchanged and recovers after the file is restored in a finally block; sources
  and track snapshots match the previous capture. Existing assembly verification,
  exact graph, workspace, stale-client and Attach recovery checks also passed.
- The older-server fixture checks rejection of both absent assembly verification
  and absent fingerprint confirmation. WebUI build and API DTO checks passed;
  changed-file lint retains only five existing unused-import warnings and resolver
  notices.

This remains opt-in and is not wired to legacy Save. Full hashing currently runs
within the serialized capture request; large-map export needs a progress/cancel
job workflow before UI integration. The identities describe on-disk bytes at
capture time, not proof that every pre-existing reader cache matches externally
modified files. Cross-resource consistency and restore-time identity checks,
auxiliary/index dependencies and generated-result provenance, strict v3 validation
and atomic restore remain open. No release-readiness claim is made.

## W1 ninth increment: stable track reconstruction and graph references

Track managers can now prepare saved definitions in an empty candidate manager,
preserving track IDs, ordering, names, colors, visibility, range/log settings,
render/aggregation modes and virtual Cooler-weight source references. Validation
rejects duplicate/malformed IDs, invalid appearance/range/mode values, unsupported
or mismatched file types, changed render styles and missing files. Failure closes
the staged data sources without publishing tracks or advancing the ID counter.
Successful preparation advances the counter beyond the largest restored ID, so
later user additions cannot overwrite saved graph references. Track1DManager now
implements AutoCloseable using its existing close lifecycle.

Snapshot requests opt into `verifyTrackDefinitions`; they construct and close an
independent track manager and require its complete summaries to match. Rendering
graph bindings must name existing tracks, and secondary weights need a secondary
source. Success sets `tracksVerified`; a requesting client rejects an older-server
response without it. This prepares the track part of the eventual atomic restore
transaction but does not replace the live manager or change legacy Save/Open.

Evidence: `/tmp/hict-w1-session-tracks/` and `/tmp/hict-session-tracks-*.log`.

- Full Java suite/development JAR passed: 335 tests, 328 passed, seven optional
  skips. Focused tests preserve sparse IDs and reordered mixed BED/virtual tracks
  with custom settings, prove later additions use a fresh ID, and reject duplicate
  IDs, missing resources and attempts to restore into a nonempty manager without
  partial publication or counter advancement.
- Eight browser checks passed. The saved custom graph now references a real BED
  track by ID, as well as retaining its multiplier. Assembly/track verification,
  independent content hashes, virtual weights, persisted workspace, missing-resource
  recovery, stale-client refusal and fresh Attach all pass. A deliberately missing
  graph track reference is rejected without changing assembly history. Older-server
  fixtures omit each verification flag in turn and are refused. Page exceptions:
  zero.
- WebUI build, API DTO checks and changed-file lint passed; only the five existing
  unused-import warnings/resolver notices remain. Initial Java compilation exposed
  the missing AutoCloseable declaration; it was corrected. The first extended
  browser run stopped on an extra quote in injected test JavaScript; the corrected
  full run passed. Those incomplete attempts are not passing evidence.

Remaining W1 work includes external v3 document validation and migration,
restore-time fingerprints/auxiliary resources, preparation of the full session,
atomic publication and client refresh, dirty-state prompts and large-export job
progress/cancellation. These checks cover track definitions and their graph
references, not every format's complete rendering/export fidelity; that requires
expanded round-trip and release gates. W1 remains incomplete.

## W1 tenth increment: graph validation at the session boundary

Optional snapshot track verification now validates the graph before preparing
tracks. The validator checks both expression roots, Boolean pipeline flags,
numeric versus color inputs, operation and dynamic-field enums, existing track
IDs (including supported built-in weight aliases), track axes, finite numeric
values, explicit log bases and supported color syntax. It bounds traversal to
32 nested levels and 10,000 expressions and reports the failing field path.
Supported RGB/HSL/HSV channel aliases, blend aliases and optional renderer
defaults remain accepted. Valid graphs are inspected without modification.
The ordinary rendering API keeps its existing compatibility behavior.

Evidence: `/tmp/hict-w1-session-graph/browser/results.json` and screenshot;
`/tmp/hict-session-graph-focused.log`, `/tmp/hict-session-graph-build.log`
and `/tmp/hict-session-graph-browser.log`.

- Four focused tests pass, including preserved JSON and compiler compatibility,
  supported aliases, missing references, invalid axes/operations/log bases/colors,
  invalid child types, nonfinite values, flags, depth and node-count limits.
  An initial test-fixture parenthesis error was corrected before passing runs.
- Full Java suite and development runtime build pass: 339 tests, 332 passed,
  seven optional skips, zero failures or errors.
- Nine real Chromium checks pass, with zero page exceptions. The extra case
  deliberately installs an axis that the ordinary renderer accepts by fallback;
  verified capture refuses it and names
  `renderPipeline.lowerExpression.left.right.axis`. Assembly history stays
  unchanged, and correcting the graph restores successful verified capture.
  Existing independent hashes, missing-file recovery, stale-client refusal,
  Attach, exact graph capture and old-server refusal checks also pass.
- The inspected screenshot still labels the primary source as absent from a
  custom pipeline that does reference it. This remains a W2 sidebar attribution
  defect, not evidence of a completed interface audit.

This is a session-boundary foundation, not the complete external v3 validator:
unknown extension properties, whole-document byte limits, source availability,
static threshold relationships and dynamically evaluated domains still need
the complete document contract. Preparation/publication, Save/Open v3, dirty
state handling and the full W1 gate remain outstanding. No release qualification
is claimed. No WebUI files changed in this increment.

## W1 eleventh increment: prepare the session resources together

`SessionRestoreCandidate` owns unpublished primary/secondary readers, a track
manager, linked FASTA paths/primary aliases and the compiled rendering pipeline.
It validates the active source, checks track summaries against their typed
definitions, verifies graph references and resolves each physical resource under
the real DATA_DIR. Lexical traversal and symlink escapes are rejected. Matrix,
FASTA and physical-track fingerprints are mandatory in this preparation layer.
Virtual weight references must agree with their saved source and availability.

All resource contents are checked before opening the candidate, and checked
again after assembly, track and FASTA preparation. Failure closes every resource
already prepared, preserving cleanup failures as suppressed exceptions. Explicit
close is idempotent. Source assembly reconstruction still checks complete
descriptors and AGP hashes; the snapshot route additionally compares the
candidate's native contact-bin mappings with the live source.

The existing snapshot route uses this combined preparation whenever all three
verification flags are true. Individual verification options keep their narrower
behavior. Candidates are closed after verification; no source/track publication
or client session replacement happens in this increment.

Evidence: `/tmp/hict-w1-session-candidate/`;
`/tmp/hict-session-candidate-focused.log`,
`/tmp/hict-session-candidate-build.log`,
`/tmp/hict-session-candidate-tests-final.log` and
`/tmp/hict-session-candidate-browser.log`.

- Five candidate tests cover two reversed sources with different resolution
  pyramids, BED and secondary-weight tracks, linked FASTA, primary-only sessions,
  invalid active-secondary references, same-size/same-timestamp track replacement,
  missing fingerprints, traversal/symlink escape, secondary AGP failure and retry.
  Live assembly/track state remains usable and unchanged.
- Full Java suite: 344 tests, 337 passed, seven optional skips, no failures or
  errors. Development JAR build passed.
- All nine existing real Chromium snapshot checks pass through combined
  preparation, including recovery, exact graph retention and stale-client guards.
  Zero page exceptions.
- Eight additional consecutive fully verified captures leave the process's
  data-file descriptor counts unchanged (one descriptor per live matrix) and
  preserve assembly history. Tiny-fixture capture times were about 10–16 ms;
  these are not large-file performance claims.

Remaining work is substantial: externally supplied v3 documents still need a
complete validator and typed decoder; workspace/source metadata, generated-result
provenance and auxiliary indices need their complete contract; candidate
publication and client refresh must form one transaction; Save/Open v3 and dirty
prompts remain unwired. Hashing is synchronous and requires cancellable job
integration for large resources. Double hashing detects changes during
preparation but does not lock external files against later replacement. FASTA
analysis retains existing partial-link behavior rather than claiming full
sequence coverage. W1 and the release gate remain incomplete.

## W1 twelfth increment: decode serialized resource snapshots before preparation

The public candidate preparation entry now accepts JSON and first runs the pure
`SessionSnapshotDecoder`. Combined snapshot verification uses this entry, so
resource reconstruction no longer depends on copying typed definitions directly
from the live track manager. The decoder owns a copy of the validated tree and
returns immutable typed track definitions.

Validation covers the internal snapshot version, required Boolean/string/object/
array fields, exact integers within the portable safe range, duplicate component
and track IDs, contig resolution maps, scaffold bounds/order/lengths, name override
references, AGP content hashes, fingerprint syntax, track settings and virtual
source references, graph grammar and track bindings. Verification flags are
type-checked but do not bypass reconstruction or fingerprint verification.
The full tree, including unknown extension fields, is bounded to 8 MiB encoded
UTF-8, 100,000 visited values and depth 64; nonfinite values and reserved
prototype keys are refused before copying or compiling.

Saved view fields require finite coordinates, a positive resolution and finite
rotation. Row and column scopes are validated independently against the active
assembly's IDs and intervals. Invalid values report their document field path.

Evidence: `/tmp/hict-w1-session-decoder/`,
`/tmp/hict-session-decoder-focused.log`,
`/tmp/hict-session-decoder-build.log` and
`/tmp/hict-session-decoder-browser.log`.

- Six decoder tests cover exact serialized round trips/input ownership, scalar
  coercion refusal, fractional and unsafe integers, missing/duplicate references,
  interval/hash/fingerprint corruption, malformed views/scopes, size/count/depth
  limits and rejection before any filesystem access. A real two-source candidate
  regression now uses the JSON entry point.
- Full Java suite and development JAR pass: 350 tests, 343 passed, seven optional
  skips, zero failures or errors.
- Eleven real Chromium checks pass with zero page exceptions. Two new cases
  deliberately install a zero-resolution workspace and then a missing contig
  scope via the existing workspace API. Verified capture rejects each with its
  precise field path, preserves assembly history and succeeds after restoring
  the original state. The previous graph/resource/recovery/stale-client checks
  continue to pass.

This decoder validates the internal resource snapshot, not a finished portable
v3 document. Workspace styling and matrix-view metadata still need their full
versioned contract, as do capabilities/provenance and generated auxiliary files.
Ordinary workspace saving still accepts these malformed test states; the
protection here is at verified session preparation. The portable envelope,
frontend Save/Open, atomic publication/client replacement, dirty prompts and
large-save jobs remain outstanding. The overall W1 and release gates remain open.

## Session restore and file identity (W1-A, W1-B)

`POST /session/restore` takes the body produced by `/session/snapshot`. The
snapshot is decoded, both sources, tracks, linked FASTA files, processing graph
and visualization options are prepared in an isolated candidate, and file
identity is checked before and after preparation. The candidate is published
in a single step. The previous resources are closed afterwards, and any failure
leaves the live session as it was (HTTP tests cover tampered, missing and
malformed snapshots).

File identity defaults to `HICT-QUICK-1`: the size, the first and last 4 MiB and
16 evenly spaced 64 KiB blocks. A 3 GiB file is identified in well under 2 s
(unit test). `fullFingerprints` selects the whole-file SHA-256. The modification
time is deliberately not part of the identity, so copying a data folder keeps
sessions valid. A changed file is refused with its name and the reason (size
change or content difference). Edits that avoid all sampled regions of a large
file are not detected by the default mode, so use `fullFingerprints` where that
matters. Restore reads the algorithm stored in the snapshot.

## Complete session Save and Open (W1-C)

Save writes a v3 document (`format hict-session`, `version 3`): the server
snapshot (both edited assemblies, linked FASTA, tracks, processing graph,
visualization options, file identities) plus the client state (a fresh workspace
capture with view, scopes, styles and track visibility, saved locations and
presets). Open validates the envelope, restores through `/session/restore`, and
replaces the displayed map only after the server accepted everything. Undo
history is not saved, and the Save confirmation says so. Versions 1 and 2 keep the
legacy path and its notice.

Restore failures answer 400 for a malformed snapshot and 409 for a changed or
missing file, with the file name and reason. Names must stay inside DATA_DIR as
for `/open`, and a symlink placed inside DATA_DIR is followed (the earlier rule
that the real path must also stay inside DATA_DIR rejected the common setup of
linking maps stored elsewhere).

Real Chromium gate (25 checks, zero failures): two edits through the context menu,
a BED and a weights track, a x7 graph, a contig row scope and a zoomed view; Save
through the File menu; Close; Open session. AGP hash, track list, graph, visualization
options, scopes, view and track styles are identical, and sidebar swatches match.
A session with a changed file or without file identities leaves the open assembly
untouched.

## Unsaved-edit prompts (W1-D)

An assembly is unsaved when the edit journal's current state differs from the state
at the last open, session restore or completed Save session. Undoing back to that
state counts as saved. Attaching or detaching a source starts a new journal, which
is not an edit, so a clean assembly stays clean and an edited one stays unsaved.
Close, Manual Open, the Open Wizard and Open session ask Save session, Discard edits
or Cancel (Esc cancels). A failed save keeps the dialog open and never continues.
`beforeunload` warns while unsaved. Attach does not ask: it joins the same server
session, so nothing is discarded. Edits made by another window count as unsaved too.

Real Chromium gate (27 checks, zero failures): clean close without a prompt, dialog
semantics and focus, Esc, undo back to clean, a forced failed save, save then
continue into Open session, discard, and source attach/detach. It also found and
fixed a defect: right-click menus stopped working after the map manager was replaced
without an unmount (opening another map or restoring a session).

## Signal controls (W2: items 1, 2, 7)

Causes found by tracing the real sidebar: the toggle negated the stored base, so the
default off value `-1` became base 1 when switched on. With a processing graph
active, Apply never rewrote the graph's signal subtree, so the sidebar reloaded the
old flags. When a graph held a single log, the reader assigned it to the legacy
pre-log, which made the post-log switch appear to reset.

`core/visualization/signalProfile.ts` is the single definition of the standard
signal subtree: divisor, post-log, Cooler weights, linear scaling, quadratic
scaling, pre-log, source, in that order, each optional, with a coefficient on either
side of a product. A log with nothing multiplied outside it is the signal transform.
Anything else is custom and is never rewritten (the sidebar says so and disables the
signal controls). Convention everywhere: `log(1 + x) / log(base)`, base finite,
positive and not 1. A pre-log with nothing multiplied after it builds the same graph
as a post-log and is reported as one, which is numerically identical.

Controls: Signal transform (Linear / Log2, or Log10 or Log for another base), and an
Advanced disclosure with Log base, Scaling across zoom (None, Linear, Quadratic, one
choice) and Cooler weights. Pre-log and Auto upper threshold are removed from the
ordinary controls, and a preset that uses them shows a one-line summary with a button
to remove it. Invalid bases (1, 0, negative, text, above 1000) are rejected visibly
and never sent. The slider's Linear/Log buttons are captioned Slider scale.

R3 (nearly blank or inconsistent map after the wizard): the opening threshold was
written into the graph while the sidebar and the stored options kept the preset's
old value, which the next Apply wrote back. The thresholds are now read back from the
graph. On the arabiensis map the sidebar and graph both show 0 to 0.6535 after the
wizard (before: sidebar 0.003, graph 0.6535).

Checks: `check-visualization-signal-profile.mjs` (144 build and read round trips,
19 custom graphs left untouched, the log convention), and a real Chromium gate of 36
checks with zero failures: numeric output from the server equals `log2(1 + x)` with no
pipeline, with a standard pipeline (evaluated on the graph's input branch) and a custom
graph stays `x * 7`; every control persists after Apply, a sidebar remount, a reload
and Attach; invalid bases are never sent; resetting a custom graph makes the controls
editable again. Still open for W2: a single opening transaction that includes the NaN
choice (the prompt after the wizard still recomputes thresholds as a second step), and
exact-scan ownership of the slider domain.

## Toolbar zoom and tool safety (W3)

The toolbar zoom buttons animated to the sentinel zooms `10e10` and `0` from pixel
indices. They now take one step (`getConstrainedZoom(zoom +/- 1)`, 250 ms), keep the
view center and report the limits. Escape disarms the armed tool (`deactivateScissors`
or `deactivateTranslocation`) before clearing the selection. The armed tool is backed
by a Vue ref, because async completions hold the raw manager and the toolbar buttons
stayed pressed after a finished split. Real Chromium gate (18 checks, zero failures):
one-step zoom with the center kept, the real limits reached at a realistic click pace and
explained, Escape then a double-click changes nothing, a positive control showing that an
armed double-click does split (contigs 96 to 97), the tool disarming itself and exact
Undo, and translocation mode following Escape. Not done for W3: a keyboard action for
Split (the double-click gesture remains the only way) and showing the affected interval
before the gesture.

## Map anchoring and selection contrast (W5: items 3 and 5)

`uiSettings.mapAnchorTopLeft` (default on, persisted) drives a lock at the ruler corner.
`core/mapmanagers/mapAnchor.ts` is the only place that reaches into OpenLayers: it wraps
`view.constraints_.center` after every `applyOptions_`, and `check-map-anchor.mjs` pins
OpenLayers 9.1.0 and the private names it uses. Rule: a map smaller than the area sits at
the top-left, a larger one pans across its whole extent but never past its top or left edge.
A rotated view and an export in progress are not anchored. Selection styles live in
`core/mapmanagers/selectionStyles.ts` (dark outline, light dashed outline, translucent fill).
Real Chromium gate (15 checks, zero failures) at 1900x950 and 1100x800: default on, whole-map
corner exactly at the origin, overscroll blocked, far corner reachable, unlock allows
overscroll, persistence, keyboard Enter and Space, re-lock applied at once, and screenshot
pixels at the selection edge contain both dark (luminance below 90) and light (above 200)
values over a dense red area and a pale area. Not done for W5: screenshots for every shipped
palette on a dark background, increased UI scale, and a dedicated export layout test.

## Dotplot accumulation (W7: item 6)

Checked in `scripts/sequence_analysis/overlay_store.py`: accumulating overlays does not
add values. `OverlayWriter.patch` replaces every cell of the updated rectangle (including
with zero) and keeps cells outside it, and each update is recorded separately. Added tests
(`tests/sequence_analysis/test_overlay.py`): four repeated identical updates give the same
matrix as the first, an overlapping second computation shows its own values in the overlap
(5 and 6, not 27 and 29) and keeps the rest, the same overlap computed again is idempotent,
alignment sample counts replace rather than accumulate across overlays, identity percentages
above 1 are stored as given (nothing is clamped to a presence flag), and fractional or
negative counts are rejected. The Java PAF sampling path, the Python kernels, zoomify,
tile composition and export were not audited in this pass. Within one computation the
per-bin value is a count of alignment samples (and zoomify sums them at coarser levels),
so the question for the owner stands: if the supervisor wants binary presence instead,
add it as a separately versioned mode built at each level, not by rewriting the stored
counts. `test_dotplot.py` and `test_moddotplot.py` need the `moddotplot` package, which this
machine's Python lacks, so they were not run here.

## Sequence strip repaint (W4)

Cause: the revision counter that tells the FASTA strip its content changed was a plain
field, read only as a cache key, so after an edit nothing scheduled a redraw. The strip now
subscribes to one notification (`RequestManager.bumpSequenceRevision`, also fired when the
FASTA linkage changes) and to the contig snapshot being replaced, drops its cache, cancels
the stale read and repaints at the same viewport. Letter visibility uses the measured width
of the widest base glyph with hysteresis. Checked in a real browser on a 96-contig map with
a generated FASTA at about 25 px per base: horizontal and vertical strips equal the FASTA,
after Reverse they equal the reverse complement without panning (viewport unchanged), Undo
and Redo restore each state, and linking a FASTA with different content under another name
repaints with the new bases (14 checks). Linking by the same name after overwriting the file
returns the new content from the server. Not done: Move, Split, Group and Ungroup were not
driven separately (they share the same notification path), delayed responses and gzip input
were not exercised, and the sequence detail dialog does not refresh while it is open.

## Track input validation (W12)

`Track1DManager.ParseReport` counts the records read, skipped and corrected for BED-family
files and keeps the first five examples with line numbers; `TrackSummary.importWarnings`
returns them from `/tracks/open` and the interface shows them in one message. Syntax errors
(a coordinate that is not an integer or overflows 64 bits) reject the file with the line, as
before. A file whose records are all unusable is refused and the track list is unchanged.
`updateTrack` refuses a range whose maximum is not above its minimum (the old
`normalizeTrackRange` reset it to 0..1 silently). The track log base uses the same validity
rule as the map's (positive, finite, not 1) and keeps the last valid base. Unit tests
(`Track1DManagerValidationTest`, 6 cases) and a browser run (open a partly valid file, refuse
an all-invalid and a comment-only file, base 4 accepted and 1, -3 refused, base 1.0000001 kept
as typed, range 2..8 accepted and minimum 9 refused) all passed. Not done: VCF and GFF/GTF
readers still skip short lines without counting them, and BigWig/BAM inputs are validated by
their libraries only. A `.bedgraph` extension is not accepted yet (only `.bed`).

## Square-root coverage normalization (W10: item 4)

Reference pinned to Juicebox 3c4936c0 (`NormalizationCalculations.computeVC` and
`NormalizationVectorUpdater.buildVCOrVCSQRT`), run by `scripts/normalization/GoldenVcSqrt.java`
on six matrices (small, empty bin, diagonal only, off-diagonal only, all zero, random 40 bins
with empty bins) to make `src/test/resources/normalization/vc_sqrt_golden.json`. Evidence:
`CoverageNormalizerTest` (7 tests, tolerance 2e-6 relative because the reference accumulates in
32-bit floats), `CoverageWeightsTest` (4 tests on a real converted HDF5: coverage equals the
recipe applied to the full symmetric matrix at every resolution, weights follow the bins through
a whole-assembly reversal, the file's weights are untouched, oversize and cancel leave nothing
published), and `scripts/normalization/check_vc_sqrt_parity.py`, which reads the contacts directly
from an `.hict.hdf5` and compares them with the reference: 11,145 bins, 10,969,252 contacts,
10,904 valid bins, worst |weight x reference divisor - 1| = 1.5e-7, no weight where the reference
has none. Preparing all ten resolutions of that 278,614-bin map takes about 16 s.

Domain decision: one vector for the whole opened map (not per chromosome), keyed to source bins,
so it is unchanged by reversal, move, regroup and (apart from the removed bin) split. Contigs
hidden at a resolution still contribute their contacts, which matters when comparing with a
visible-only matrix. Browser gate (18 checks): the control and its three options, progress with
Cancel, cancel publishes nothing and the control keeps showing None, full path in 15 s, values
change, switching between prepared modes is instant and exact, None restores the original
values, the mode survives a reload and re-attach, a saved session records the mode and the
restore recomputes it, and the file-weights NaN question is not asked while coverage is used.
Not done: a second source attached after coverage was chosen must be prepared again (the sidebar
says so and offers a button, but this was not exercised in a browser); the identity and presence
sources are disabled through the existing capability flag but not run here;
`check_source_capabilities_browser.py` was updated to the new control but needs its fixtures and
was not run; the export path was not compared with and without coverage.

## FASTA without a map (W14: item 10)

`FastaOnlyAssembly` scans a plain or gzip FASTA once (streamed) and returns each record's length and
its runs of N at or above the window, writes a real `.hict.hdf5` through the existing
`McoolToHictConverter` from a Cooler-shaped file with an empty pixel table and root metadata
`SEQUENCE_ONLY` / `ABSENT` (no hictk needed), and describes splitting as an AGP applied by the normal
AGP import. Tests (`FastaOnlyAssemblyTest`, 8): records, lengths and gaps, lower-case n, gzip equals
plain, no empty pieces and source coordinates kept, the AGP text with true gap lengths, refusal of a
duplicate name (with both lines), sequence before a header, no sequence, a zero window and
cancellation, resolution selection, the written file's contigs and ABSENT state, and split pieces
with their source offsets after the AGP import. Live server: a 249 Mb, 96-record FASTA is built in
1.3 s; a split FASTA's assembly sequence equals the non-gap source pieces, and the exported FASTA
records equal the input records byte for byte (gaps at 150 and 120, lower-case n kept). Browser gate
(27 checks): the dialog (accessible modal, focus, Tab containment, file list, N window shown only
when splitting, invalid window explained), duplicate names refused with the lines and nothing
opened, the split open (6 contigs, lengths, SEQUENCE_ONLY/ABSENT, FASTA linked, not counted as
unsaved), exports equal the input, a Reverse from the context menu then Undo restores the exact
assembly, a saved session restores the same contigs, the 250 Mb FASTA opens in 4 s as 96 contigs,
and the chip reads Assembly. Not done: cancel of a long read was not exercised in a browser (it is
unit-tested); the opening wizard has no FASTA-only path; matrix-dependent sidebar sections are only
disabled and explained, not hidden; tracks and exports on a FASTA-only map were not driven one by
one; gzip was tested at the unit level only.

## Examples catalog and downloads (W15 and part of W13: item 11)

Server: `ExamplesCatalog` (validation, merge, ordering from data, duplicate ids refused, bad local
files reported with file and JSON pointer, 1 MiB limit; `ExamplesCatalogTest`, 8 tests), `ExampleDownloader`
(`ExampleDownloaderTest`, 16 tests against a controlled local server: checksum, wrong length, resume
with Range and If-Range, ignored range, 416, wrong resume position, unknown length, redirects and their
limit, the URL policy on http, loopback, private, link-local, carrier-grade NAT, credentials, other
ports, a redirect to a metadata address, no disk space, optional versus required files, cancel keeping the
partial file, one job at a time, file names that try to leave the folder) and the three routes. The test
constructor that allows plain http and loopback is package-private; the server only constructs the
production policy. Browser gate (22 checks, real https): the dialog and sections, a card's content,
citation links, provider and terms, no invented licence, draft marking, invalid-catalog report, Tab
containment, a real 71,567-byte download with an optional file skipped and the manifest, progress in
bytes, one download at a time, cancel then resume from the partial file, and Open in HiCT giving the wizard
the downloaded file. Not done: the two DNA Zoo examples were not downloaded in full or opened (the card is
marked draft); there is no persistent Background jobs panel and no restart reconciliation; the generic job
contract that W13 describes is not built (FASTA-only, coverage and download each have their own service); a
JSON Schema library is not used, the schema file is documentation checked only for shape; DNS rebinding
between the address check and the connection is not pinned.

## Interface compaction, dialogs and narrow windows (W11: items 2, 7, 9; partial)

Done: the Normalization control and the Advanced split (W2, W10), the FASTA-only and Examples dialogs
(W14, W15), and in this step: narrow-window layout (at 390 px the map starts at y=409, before 548; at 768 px
the map is 568 px wide, before 306, because the panel starts hidden under 900 px), Borders & labels starting
collapsed for new users (saved preferences kept), Tab containment, modal semantics and initial focus for the
Tracks dialog (Tab left it on 29 of 40 presses), the opening wizard (all 40), About and the on-screen display
settings (40 of 40 inside afterwards), and job status messages no longer toasting on every poll. Regression
gates re-run on the rebuilt interface: sessions 25/25, dirty-edit prompts, navigation and Escape, anchor and
selection, and the signal controls 36/36 (that gate, and `check_source_capabilities_browser.py`, were updated for the
Normalization control that replaced the Cooler weights switch).
Not done: a shorter opening wizard (Choose dataset / Review and open), one transaction for opening (the NaN
prompt after the wizard still recomputes thresholds as a second step), moving the undo journal out of the
Visualization panel, hiding matrix-only sections on maps without contacts (they are disabled with a reason), an
inspectable effect or stated prerequisite for every preset, focus return to a menu item that has closed
(it falls back to the page), a minimum supported window size declaration beyond "usable at 390 px", and
actionable-error and copy-diagnostics handling for every action.

## Repeated-open memory (W6: R25)

Measured in headless Chromium with forced garbage collection between cycles (open a 96-contig map, close it,
15 times). Before: JS heap 20.5 -> 128.3 MB (about 7.6 MB per cycle), DOM nodes 1,314 -> 4,084, event listeners
326 -> 420, about 1,081 decoded tile images still alive after 6 cycles. Cause found: `ContactMapManager.dispose`
detached the map from the page but left its layers, tile sources (with decoded images), interactions and the
size observer reachable. Fix: disconnect the observer and clear and dispose the layers, sources, interactions and
the map and overview map. After: JS heap 15.1 -> 18.8 MB over cycles 4 to 15, nodes 1,184 -> 1,294, tile images
alive 1. **Not yet a plateau:** about 0.3 MB, 10 DOM nodes, 6 listeners and 3 canvases per cycle still accumulate.
A heap snapshot diff shows three vector layers (borders), their features, selection interactions and two ruler
controls per cycle still retained by something outside the map, which was not found in this pass. Backend
executors and file handles were not examined. Gate for the plateau (heap, nodes and listeners flat after warmup)
is therefore not met.

## A readable map after every opening path (U1)

On `combined_ind2_4DN`, turning off Cooler weights previously left a multiplication by
`__builtin_cooler_weights__` in the pipeline and showed an empty map. The weight selection now
publishes options together with the matching pipeline. The opening threshold uses the 99.5th
percentile of the visible tiles for the active signal expression, with a bounded matrix query
when tile metadata is unavailable. Explicitly replacing excluded weights with 1 uses the 90th
percentile to keep the balanced signal visible when unweighted counts dominate the upper tail.

Show O/E maps reciprocal ratios to opposite colors with a neutral midpoint at 1. The default
ratio range is 0.5 to 2. Observed restores its previous range when selected again.

The browser gate `scripts/check_opening_readable_browser.py` covers Manual Open, Open Wizard,
Attach, normalization and excluded-bin choices, four and eight diagonal zoom steps, and the O/E
round trip on `mat18_100k`, `combined_ind2_4DN` and `arabiensis.0`. It requires at least half of
the visible diagonal samples to differ from the background, checks pipeline and sidebar
agreement, and looks for both ends of the O/E palette in the rendered image.

The fat JAR and WebUI browser gate passed 148 checks with zero page or console errors.
The full WebUI build, JVM tests and changed-file lint also passed. The zoom checks passed
at both depths on all three fixtures with the opening threshold.

## Save and restore sessions after Split (U2)

Session snapshots retain native split coordinates, orientation and component IDs. The
restorer replays those splits against the fingerprinted source before applying the saved
AGP, then checks the assembly descriptors, bin lengths and AGP hash. Save verification also
compares the native contact-bin mapping. Split replay records participate in Undo and Redo.
Snapshots without split records follow the existing AGP restore path.

The JVM suite passed, including forward and reversed cuts, two cuts in one component,
Undo and Redo, and malformed split metadata. The browser snapshot gate passed 18 checks,
including saving, closing and restoring after Split, reversed Split, Split followed by
Group or Debris, and two splits in one contig. All five preserved the AGP hash and assembly
descriptors. A forced HTTP 400 showed the server reason in the save dialog and kept the
unsaved edit. The browser reported zero page errors.

## Direct opening and interface controls (U3)

The empty workspace opens the existing file picker directly, with separate actions for
the wizard, examples and FASTA-only creation. The picker requires selection before Load.
The wizard omits conversion when every selected source opens directly and omits track
precompute when no tracks are selected. The information overlay refreshes on view changes
and when the new tiles finish rendering. Twelve dialogs use modal focus handling with
nested-dialog support.

Chromium opened `mat18_100k` from the empty workspace with a double-click at 1280×720
and Enter at 1440×900. Both runs verified rendered contact pixels, picker focus
containment and an assembly context menu within the window. The opening regression
passed 51 checks on that fixture, including the wizard, Attach and the signal controls.
All 18 menu surfaces passed, including confirmation that the pipeline editor shows the
enabled state set on the server. The WebUI build passed and ESLint reported zero errors
on the changed files.
A subsequent wheel-zoom check on `combined_ind2_4DN` changed the displayed bin resolution
from 250,000 to 100,000 while the pointer stayed still.

## Rulers at assembly coordinates (U4)

Tick intervals use the 1–2–5 series. Each tick projects through the map's fractional
base-pair coordinate mapping inside a visible contig. Contig and scaffold modes restart
at the entity boundary. Narrow contigs share a band segment, and nearby break marks
combine their hidden lengths and counts in the tooltip.

`check-ruler-ticks.mjs`, included in the WebUI viewport suite, passed against the
production coordinate holder for hidden spans, reversed and split components, scopes,
and one-bin contigs. Chromium captures verified whole-map and mid-zoom rulers on the
7,790-contig `AGWG.draft` map, and rulers after Reverse, Split and at FASTA letter zoom
on `tiny-primary`. At letter zoom the PNG was 1035×791 pixels and the SVG viewport was
1034.40625×791. The SVG contained both ruler units, entity names and sequence letters.
The browser reported zero page errors, and the full WebUI build passed.

## One message per failed action (U5)

The transport rejects failed requests to the caller. The action presents the server
reason once and records diagnostic details separately. Console errors and unhandled
promise rejections go to Notifications. Session save keeps its error dialog and offers
Copy diagnostics there.

`check_failure_reporting_browser.py` passed seven checks against the fat JAR. It forced
failures in open, FASTA link, track add, session save and PNG export, verified one visible
message per action, and checked the copied diagnostics. It also verified that console and
unhandled errors stayed out of toasts and that a second coverage preparation returned
HTTP 409. The JVM scheduler test verified HTTP 503 for a saturated queue and no leaked
task tracking across ten repetitions. Full JVM and WebUI checks passed.

## Server resource lifecycle and offline API reference (U6)

The conversion expiry clock starts at completion. Multipart uploads are staged under
DATA_DIR and removed from the dataset listing. The request releases its upload after
submission, the worker removes the staged input when it ends, and the result remains
available through the job until expiry. Tile edges are limited to 1024 pixels, and
overflowing coordinates return HTTP 400.

Map replacement, close and session restore invalidate the coverage job and tile
generation before closing previous resources. FASTA-only contact tiles and statistics
return empty results without scanning blocks, and coverage preparation returns HTTP 409.
Swagger UI 5.30.2 is bundled and its external validator is disabled.
[Maven Central artifact](https://central.sonatype.com/artifact/org.webjars/swagger-ui/5.30.2).

The JVM suite passed, including expiry boundaries, upload listing, invalid tile dimensions,
FASTA-only responses, offline assets, and atomic session restore. Chromium rendered the
API reference with external requests blocked, converted and downloaded a multipart
HiCT upload, verified staged input cleanup, and confirmed that close, open and restore
cancel a running coverage job.

## Startup and map disposal (U7)

Vite's dynamic chunks keep the file picker, wizard, graph editor, Plotly, PDF export,
and other tools out of startup. The browser downloaded 1,332,060 decoded JavaScript
bytes on an empty workspace, down from 4,365,018 bytes. Hashed assets are immutable
for one year. HTML revalidates, and raw PNG tiles bypass HTTP gzip and ImageIO disk
caching. The packaged-server HTTP checks verified these headers and the PNG signature.

The baseline snapshot retained 30 ruler controls after 15 closes. The shortest path
from the global handles ran through DevTools' console entry, the map manager, and a
view listener. Removing the object log and explicitly releasing ruler listeners and
frames eliminated that path. Tile sources also abort requests and cancel pending range
updates on disposal. The final snapshot retained zero ruler controls. Over the 15
post-warmup cycles with JIT disabled, forced collection left 6.52–6.79 MB of JS heap,
1,195–1,211 DOM nodes and 328–333 listeners. With JIT enabled, nodes and listeners
stayed flat after cycle 12. The heap still gained 0.50 MB from cycle 15 to 30. Snapshot
comparison attributed most of that JS growth to compiled code, and also showed
Chromium's native inspector data accumulating for recorded requests.

The JVM and WebUI suites passed. The resolution table verified the existing level
selection in bp per screen pixel. Chromium verified all 18 menu surfaces and the
pipeline graph round trip, including a delayed threshold calculation concurrent with
an explicit pipeline edit. Pan and zoom ran at 16.7–16.8 ms p95 with no long tasks
on the 96-, 1,765-, and 7,790-contig fixtures. In the matched before run the dense
fixture reached 33.3 ms p95 and one 50 ms long task on repeat open. The release-wide
timing and soak checks remain part of U13.

## Independent matrix resolutions (U8)

The mixed display chooses the nearest stored level per source by log-distance.
A fresh tie goes to the finer level, and a 4% hysteresis band avoids repeated
switching. The primary grid remains authoritative for rulers, tracks and scopes.
Sources that declare the same original reference intervals attach directly even
when their level sets are disjoint. Explicit confirmation remains available for
datasets with mismatched reference intervals.

The projection intersects owned native bins with the primary bin footprint in
source base-pair coordinates. It clips partial bins and handles reverse
orientation, hidden sequence and split offsets. Counts are summed with overlap
fractions, identity is averaged by overlapping area of positive scores, and
presence uses any nonzero contribution. The secondary tile uses a subdivision
only when there is room to display it at the current scale. Outgoing imagery
stays visible during the transition, and its source releases obsolete requests
without scheduling new tiles. Requests use bounded matrix reads, decoded tiles
are evicted outside the active viewport, and the server cancels work on client
disconnect. The read guard limits a single projection to 32 million native
cells, read in blocks of up to 128 × 128. A request outside that envelope requires
a smaller visible region or a coarser native level.

Sequence overlay planning estimates window memory and uncompressed HDF5 capacity
before allocating the analysis. The disk estimate accounts for three int64
columns per cell, geometric capacity growth and per-block minimum allocation.
Checked arithmetic rejects estimates outside the representable range. The API
offers a smaller region or fewer resolutions when a request exceeds the budget.

The JVM and WebUI suites passed. Chromium verified 16 animated view landings
across fine-only, coarse-only, interleaved and non-nested pyramids and kept the
same landmark after Reverse, native Split and contig scoping. The measured
maximum was 11 concurrent secondary requests. The ModDotPlot gate passed nine
checks, including real generation, the identity scale, three 5 kb/10 kb round
trips and preservation of primary pixels. Secondary replacement and confirmation
also passed using an actual reference-interval mismatch.

## Exact alignment at letter zoom (U9)

The new bounded alignment endpoint captures two windows of at most 4,096 bases
from the active FASTA and assembly revision. The bundled aligner emits extended
CIGAR with -c --eqx. A cancellable sequence job parses matches, substitutions,
insertions and deletions into zero-based half-open intervals. Noncanonical bases
are marked ambiguous. Finished results are cached by the assembly snapshot,
FASTA identity and stamp, intervals and extracted sequence. Repeated queries
reuse completed jobs, and the completed-job cap bounds the cache.

Alignment detail appears at the measured FASTA-letter threshold. The renderer
projects base centres through the same fractional assembly coordinate holder as
the sequence axes. It cancels obsolete reads and discards late responses. Display
work is limited to 8,192 bases. A missing alignment is reported explicitly.

The export policy is visible-viewport detail. PNG, SVG and PDF include the same
rendered alignment layer at letter zoom. Whole-map exports use their fitted
scale, where the detail layer is hidden. On the synthetic substitution fixture,
PNG and independently rasterized SVG and PDF all contained 21 substitution-color
pixels with their centre at the same pixel.

The JVM and WebUI suites passed. The real bundled aligner gate verified a known
substitution, insertion, deletion, reverse complement, repeat and N/IUPAC bases.
Chromium checked the substitution against its FASTA base centre, after native
Split, Reverse and Undo, and across a matrix-level change at letter zoom. A
deliberately delayed response was discarded after navigation into an unaligned
region, which displayed the no-alignment message. The temporary alignment files
were removed after completion.

## Background jobs and shutdown (U10)

GET /jobs adapts conversion, legacy dotplot, sequence search, ModDotPlot,
sequence dotplot, exact alignment, track precompute, coverage, FASTA-only and
example downloads. IDs carry their service prefix. The read-only snapshot
normalizes states, progress and timestamps and includes output actions.
POST /jobs/cancel dispatches to the owning service. The navbar shows an active
count, progress, failures, cancellation and finished results. Result opening uses
the existing assembly, track and overlay entry points. Sequence results retain
their originating map filename. The panel preserves each service's retention
policy, including map-scoped track and coverage state and the latest FASTA and
download job. Search, ModDotPlot and FASTA-only dialogs can close while running.

Server shutdown cancels every service before closing executors. Native processes
are killed with their descendants. Launcher Stop and Quit also retain descendant
handles before terminating the server, covering the forced-stop fallback.
Conversion and legacy dotplot outputs are built in job-owned incomplete folders
and published after success. Existing conversion outputs survive a failed
replacement. Track writers mark groups incomplete before writing and publish a
complete marker after both arrays, with legacy marker-free caches still readable.
Cancellation propagates to queued, compute and writer futures. Downloads retain
explicit .part files for a user-triggered retry. Jobs never resume automatically.

The downloader resolves and validates each redirect once and connects to that IP
with the origin Host, TLS SNI and hostname verification. Streaming uses one-chunk
backpressure. Separate connections avoid reusing a redirected response while it
is being discarded. Cancellation resets the active request. Partial responses use
Content-Range totals even when Content-Length is absent.

The full JVM suite passed with 427 tests, including seven existing optional-data
skips. New regressions cover pinned-host connections, chunked 206 resume, service
ordering and shutdown failures, publication conflicts and native descendants.
The WebUI build and checks passed. ESLint on edited files reported zero errors
and 11 existing warnings. Chromium completed ten checks across all job kinds,
closed the three busy dialogs, cancelled a download in the panel and opened the
finished search track, identity overlay and FASTA-only assembly. No page errors
occurred. Evidence is in the directory named by /tmp/hict-u10-ready-dir.
A controlled long-running aligner executable with a child process was stopped
through the real packaged server. Its process tree terminated, no matrix was
published and the next launch returned an empty jobs list. Evidence is in
/tmp/hict-u10-shutdown/results.json.
The existing search delivery browser regression also passed, including lost-reply
retry and slow track queries. The nine-check ModDotPlot zoom and preset gate
passed with a maximum heartbeat gap of 69 ms. Evidence is in the directories
named by /tmp/hict-u10-search-dir and /tmp/hict-u10-moddotplot-ready-dir.


## Verified examples and portable heap (U11)

Three candidates from DNA Zoo and GEO were presented after a search that also
covered 4DN. The tardigrade pair was selected for its directly matching reference
and 45,250,776-byte total download. The catalog records exact file sizes, hashes,
provider terms and citations. All three bundled cards passed real downloads,
conversion, main-map rendering and linked FASTA reads. The quick-start browser
gate reached five chromosomes in 20.27 seconds with three trusted clicks and
31,145 signal pixels in the actual main viewport. It reported no page or console
errors. The complete large-example measurements and source references are in
`docs/examples-catalog.md`. Large-map main-viewport signal counts were 41,693
and 33,130 pixels. Peak sampled Java heaps were 2.41 and 1.93 GiB.

Default `.mcool` pyramid imports now pass the selected resolution URI directly
to hictk zoomify. The bundled hictk rejects the former intermediate conversion.
A real-tool regression imports a generated multiresolution fixture. Entirely
unmatched draft assembly layouts are rejected before publication. The two large
example recipes open their scaffolded references directly. Downloader instances
borrow the current Vert.x runtime when present and preserve its ownership.

An initial quick-start implementation exposed unbounded updates in the existing
toast library. A single finite preparation notification and the existing Jobs
panel now carry progress. A fresh-browser repeat passed. Portable Linux,
Windows, macOS and Java Launcher commands default to MaxRAMPercentage=50.0,
providing a 4 GiB Java limit on an 8 GiB host. Explicit heap options remain
supported. Linux syntax and Java option precedence were checked locally.
Windows and macOS runtime checks belong to the U13 platform gate.

WebUI build and checks passed. Edited-file ESLint reported zero errors and four
existing warnings. The JVM suite passed, including the new real `.mcool`,
assembly-layout and downloader-ownership regressions. One early large-example
trial was invalidated by rebuilding its running JAR. All qualified trials used
immutable JAR and WebUI copies. The evidence directories in the catalog document
contain qualified results, source hashes, screenshots and sampled resources.


## Dependency upgrades (U12)

The sequential upgrade targets are jsPDF 4.2.1, axios 1.21.1, DOMPurify 3.4.17,
Vue 3.5.45 and the latest stable Vert.x 4.x BOM, 4.5.36. Versions were checked
against npm and Maven metadata on 2026-11-11. jsPDF's dependency update supplies
DOMPurify 3.4.17 through its existing transitive relationship. The same browser
gate qualifies that pair. Vue required an explicit unknown type on the existing
sequence-annotation canvas ref callback. Its handler already validates the DOM
instance. No dependency upgrade has been reverted.

Each frontend change ran the production build and all WebUI checks. The browser
suite includes exact alignment and edits, mixed-resolution navigation, all 18
menu destinations and a pipeline graph round trip. Export comparison independently
rasterizes SVG with Inkscape and PDF with Poppler at 96 dpi. Each format places
the 12 measured substitution pixels at the same centre in a 1034 by 791 image.
The first menu attempt used a synthetic fixture without the diagonal signal its
oracle requires. The qualified menu runs use the existing real-map fixture.
Evidence pointers use `/tmp/hict-u12-<dependency>-<suite>-dir`, with the jsPDF
qualified menu run named `/tmp/hict-u12-jspdf-menus-ready-dir`.

Upstream version references are the [jsPDF releases](https://github.com/parallax/jsPDF/releases),
[axios releases](https://github.com/axios/axios/releases),
[DOMPurify releases](https://github.com/cure53/DOMPurify/releases),
[Vue changelog](https://github.com/vuejs/core/blob/main/CHANGELOG.md) and
[Vert.x Maven metadata](https://repo.maven.apache.org/maven2/io/vertx/vertx-stack-depchain/maven-metadata.xml).


The Vert.x BOM selects SLF4J 2.x. The former Logback 1.2 binding prevented
startup, which the native-startup regression caught. SLF4J 2.0.20 and Logback
1.5.38 restore the binding. The Logback configuration uses its default shutdown
hook, and the standalone logging test initializes its own MDC adapter. The full
431-test JVM suite passed with seven optional-data skips after these changes.
The asynchronous sink test still verifies nonblocking requests and shutdown drain.
Gradle dependency locks were regenerated through dependencies --write-locks.


Compatible transitive security updates then passed the complete WebUI build and
the same four browser suites. Production npm audit reports zero advisories.
The complete development graph retains 15 findings, comprising 12 moderate and
three high findings, with zero critical findings. Affected dependency chains
include Electron packaging, Vite and its plugins, esbuild, Vue language tooling,
minimatch, brace-expansion and vue-template-compiler. Major development-tool
migrations remain outside this five-dependency upgrade. Exact audit reports are
`/tmp/hict-u12-audit-compatible.json` and `/tmp/hict-u12-audit-production.json`.
No upgrade was reverted. Matching export substitution counts and centres passed
again against Vert.x and the final compatible dependency graph.


## Release documents, citations and version (U12)

The installation README now covers Linux, Windows, both macOS architectures,
the 45 MB quick start, an actual example screenshot, supported formats, measured
resource needs, SSH forwarding and bind-host restriction. The API guide documents
session lineage, background jobs, projected grids and whole-map coverage.
The user guide covers editing, sessions, tracks, analyses, exports and shortcuts.
Help links to that guide. Both CFF files pass the official 1.2.0 JSON Schema.
The author lists match existing project attribution. BLAST+ is listed in JVM
startup and the visible WebUI attribution list.

About embeds the WebUI's own package version and source commit at build time.
Its citation block includes that version and revision. The browser gate reads
the actual Git revision and verifies the visible metadata and citation. The
release-version CLI accepts an explicit stable version and preserves semantic
release tags in platform CI. Four regressions cover its prior run-specific
behavior, explicit assignment, invalid versions and stable release tags.

The aggregated third-party file contains 394 component records and 248 distinct
upstream notices. The generator reads the installed production npm graph,
resolved JVM runtime JARs and staged native-tool license directories. Portable
scripts copy the aggregate into licenses alongside the original tool notices
and runtime/legal. Generate it with scripts/generate_third_party_notices.py,
passing the WebUI, a line-separated runtime JAR list and staged toolchain root.
The complete Java regression workflow runs the full test task on this branch,
including every reinforcement test class, with the required copyWebUI exclusion.
Windows and macOS runtime/manual results remain pending in the platform checklist.


The release-document gate passed the About revision/citation checks and the
18 menu surfaces at both viewport sizes. The JVM suite and final WebUI build
passed. Edited release UI files had zero lint errors and three existing warnings.
Evidence pointers are `/tmp/hict-u12-docs-metadata-final-dir` and
`/tmp/hict-u12-docs-menus-final-dir`. The compiled WebUI reports its own 2.0.0
version and full source commit independently of the running server's version.


## Full interface audit in progress (U13)

The capability browser gate exposed a stale resolution-dependent normalization
state. Raw asynchronous map callbacks replaced a plain view-state field without
notifying computed controls. The existing shared view-state container is now
shallow-reactive. Five capability scenarios pass, including weights present at
one level, absent at another, secondary sources, identity sources and legacy
unknown metadata. The full WebUI build and all 18 menu checks pass. Edited-file
ESLint has zero errors and two existing warnings. Evidence is named by
`/tmp/hict-u13-reactive-source-capabilities-dir` and
`/tmp/hict-u13-reactive-menus-dir`.

The audit also corrected test setup. Chromium rejects API port 6000. Embedded
assets require an isolated server working directory because development serving
prefers a sibling checkout. The ModDotPlot helper must discard its listener key
instead of serializing the OpenLayers object graph. The release-review FASTA
step now uses the user's linking flow, updating frontend capabilities before
checking the sequence canvases. Qualified reruns passed BLAST, nine ModDotPlot
checks, structural edits, ten job checks, the release-review journeys, rotated
exports, exact signal bounds and histograms. Earlier setup failures remain in
separate evidence directories. Full package and soak qualification is pending.

The concurrent-client gate initially attempted a pan beyond the anchored map
boundary, producing no view change and no save. It now waits for Attach to finish,
zooms to a pannable view, verifies the changed centre and counts requests before
the competing edit. The qualified run made exactly one rejected save, retained
the newer server style and recovered through explicit Attach. Evidence is named
by `/tmp/hict-u13-reactive-concurrent-pan-dir`. Two obsolete static control anchors
were refreshed to the current transform and legacy-summary controls. Static
reconciliation validates all 232 contracts and retains pending runtime status.


A shutdown audit found private gzip FASTA indexes surviving normal server stop
while a map remained open. Main now closes the published session after stopping
jobs and request pools, using the same close-session implementation as the API.
The stop promise reports cleanup failures. Two regressions verify both native
indexes are removed on undeploy and a failure completes shutdown exceptionally.
The full JVM suite passes with 433 tests and seven optional skips. A real browser
opened a gzip reference and attached a secondary reference. SIGTERM removed both
indexes and exited with status 143. Evidence is in `/tmp/hict-u13-shutdown-p_hur22i`.

The extended first-run gate downloads the small example with three trusted clicks,
then navigates, Shift-drags a selection, reverses, splits, undoes the split, saves,
closes, restores and exports AGP. Opening took 19.67 seconds and the complete
journey 26.59 seconds. Restored assembly and FASTA matched the session, and the
AGP bytes matched its saved SHA-256. The restored main viewport contained 31,102
signal pixels. No page or console errors occurred. Evidence is named by
`/tmp/hict-u13-first-run-ready-dir`. Packaged repetitions and the full soak follow.

## Focused editor Escape cancellation during U13

The expanded workspace audit reproduced Escape being intercepted by the global
map handler while renaming a preset. The capture-phase handler now lets focused
inputs, textareas, selects, editable content and textbox roles handle Escape when
there is no application dialog to dismiss. The existing dialog dismissal and
map-tool cancellation paths stay active in their respective contexts.

The full WebUI build and source checks pass. Focused ESLint reports zero errors
and two existing warnings. The workspace gate verifies preset Enter acceptance
and Escape cancellation, bookmark save/navigation/export/import, all eleven OSD
field visibility choices, field order, overlay position and persisted settings.
The menu suite again verifies all 18 surfaces and focus behavior. The standalone
workspace gate also arms Split and verifies Escape disarms it outside an editor.
Evidence pointers are `/tmp/hict-u13-workspace-escape-isolated-dir` and
`/tmp/hict-u13-escape-menus-ready-dir`.

## Cooler export capability controls during U13

The audit found selectable Zstandard and LZF output modes even though the
internal writer uses Deflate fallback and the hictk-assisted copy writer uses
Deflate. Those unsupported choices are now visibly disabled with an explanation.
Level 0 remains available for uncompressed output. Balancing is disabled for the
direct internal exporter, and its request explicitly leaves balancing off while
retaining the user's preference for supported modes.

The full frontend build passes. Focused ESLint initially reported one formatting warning, resolved by the
final focused formatting pass. The file now has zero errors and zero warnings. The new real-browser gate reverses a known 4,000-base assembly
and exports through internal, hictk and auto modes. Every contact equals the
independent deterministic oracle. It checks both native resolutions in the
internal multiresolution export, the unedited source layout through hictk,
and edited output with finite positive generated weights through balanced auto
export. Requests preserve the selected level, thread count and assembly policy.
The browser reports no unexpected errors. Evidence is named by
`/tmp/hict-u13-export-controls-dir`.


## Focused release formatting during U13

The export capability change introduced one Prettier warning on a multiline
conditional tag. The separate lint pass formats that tag. Focused ESLint now
reports zero errors and zero warnings. The change preserves template semantics.
The complete source lint baseline retains 100 existing warnings.

## Named bookmark persistence during U13

The inventory's bookmark scenario exposed names being held only in an input's
local state. Bookmark records now carry an optional name through the existing
session store, export/import and native session client state. Editing updates
the stored record immediately. Older unnamed records retain their default label.
Bookmark imports reuse the session library validator before replacing records.
The validator accepts optional string names and reports malformed name fields.

The full frontend build and existing source regressions pass. Added session
regressions cover legacy unnamed records, Unicode names in v2 and v3 documents
and malformed name types. Focused lint has zero errors and the ten existing
format warnings in the pre-existing session regression script. The production
files have zero warnings.

The real browser first reproduces the missing exported name, then verifies named
bookmark export/import, sidebar remount and native session recovery after page
reload into an empty bookmark store. A legacy unnamed import retains its default
label. The complete five-scenario workspace gate also passes preset Enter/Escape,
map-tool Escape, eleven OSD fields and notification clearing with zero errors.
Evidence pointers are `/tmp/hict-u13-bookmark-name-before-dir` and
`/tmp/hict-u13-bookmark-name-qualified-dir`.

The browser helper explicitly renders an idle frame before checking completion.
This resolves a stale OpenLayers completion flag observed after Split with zero
queued/loading tiles. Structural state, AGP and completed-frame assertions remain
in place. The generated structural fixture passes all seven edit/Undo/Redo cases.

## Exact Cooler compression during U13

Independent HDF5 inspection exposed hictk-assisted output using its default gzip
level 6 after the interface requested level 4. Streaming loads now receive the
selected level. For uncompressed single-resolution output, the existing direct
writer supplies interaction data and the native balancer runs when requested.
The exporter preserves assembly metadata and cancellation checkpoints. The dialog
explains that level 0 leaves interaction data uncompressed.

The full JVM regression suite and frontend build pass. Focused dialog ESLint
reports zero errors and warnings. The real-browser export gate passes five cases
across internal, hictk-assisted and automatic modes. Independent h5py checks verify
every exported contact, resolution, assembly policy, actual gzip filter level or
absence of compression, and nonuniform positive balancing weights. Evidence is
`/tmp/hict-u13-compression-actual.aZfFnW/evidence/`.

## Keyboard menus and final resolution during U13

The keyboard audit reproduced Escape being consumed before Bootstrap could close
an open menu. The global map handler now yields to the focused open dropdown.
Focused text fields and app-dialog dismissal retain their existing routing.

The same audit exposed the last enabled resolution switch displaying unchecked
while the model retained that level. The final active switch is disabled with a
reason, so the view and control stay consistent. Other level switches remain
interactive.

The frontend build and existing regression commands pass. Focused ESLint reports
zero errors and five existing warnings. The browser gate checks all six menu
keyboard paths and focus return, exact graph/view preservation after reload,
sidebar collapse/restore, all eleven native resolution switches, the final-level
guard, valid bounds and rejected reversed bounds. Evidence is under the pointer
`/tmp/hict-u13-navigation-controls-fixed-dir`.

## Empty conversion list during U13

The real batch audit found View current conversion jobs displaying Running after
the server returned an empty active set. The progress surface now displays
No active conversion jobs. when its job list is empty.

The frontend build and focused ESLint pass with zero errors and warnings. The
four-scenario browser gate checks exact All/None/Invert/Converted/Unconverted
filename sets, settings Back, hictk attribution, a real native batch conversion
with independent 2×2 contacts, dismissal while work completes, overwrite Cancel
with an unchanged output SHA-256 and unchanged server job list, and empty current
jobs matching the actual server response. Evidence is under
`/tmp/hict-u13-batch-controls-qualified-dir`. The fixture is a standards-compliant
Cooler generated from explicit contact values. Earlier minimal fixture metadata
and route/method harness errors are preserved as failed runs.

## Assembly-aware sidebar navigation during U13

The sidebar Go to actions divided bp positions by the current view zoom factor.
On the four-contig fixture, Go to three centered at approximately 15,530 instead
of the independently expected coordinate 10,000. Both contig and scaffold actions
now reuse LinearTrackManager.centerOnBpRange, matching header search and existing
assembly-aware coordinate projection and range fitting.

The full frontend build and existing regressions pass. Focused ESLint has zero
errors and warnings. The seven-scenario browser workspace gate verifies contig
three at coordinate 10,000 and scaffold scf_two at 6,000 from both native 100 bp
and 200 bp levels. Existing scopes, diagonal navigation, bookmark persistence,
option presets, focused Escape, all OSD fields and notifications pass in the same
run. Before/fixed evidence pointers are `/tmp/hict-u13-sidebar-go-to-before-dir`
and `/tmp/hict-u13-sidebar-go-to-fixed-dir`.

## Entity name export during U13

The actual sidebar Export names action failed to download because the frontend
used POST for the existing GET /names/export route. RequestManager now specifies
GET, matching both the server handler and OpenAPI declaration.

The frontend build and focused ESLint pass with zero errors and warnings. The
expanded real-browser rename gate verifies the download uses GET, the complete
JSON equals the server mapping, Reset changes only the selected scaffold, import
restores the exact mapping and native AGP, and Show 500 more renders exactly
300, 800, 1,300 and 1,765 ordered contigs without duplicates. The last pagination
button disappears and the accepted scaffold override survives pagination. Evidence
is under `/tmp/hict-u13-entity-name-roundtrip-dir`.

## Atomic preset import during U13

A real malformed import reproduced a valid first preset being stored before a
later null item failed. Imports now parse into a temporary map with fresh IDs and
queue threshold updates. The component publishes them only after the whole batch
parses successfully. Invalid data/list/item shapes produce one clear failure.
Existing deduplication, normalization defaults, savedLocations aliases and unnamed
legacy preset labels remain supported.

The full frontend build and existing regressions pass. Focused ESLint has zero
errors and five existing warnings. The seven-scenario browser gate verifies an
invalid later item leaves the exact dropdown contents, local thresholds and server
pipeline unchanged. Two valid items receive distinct IDs and both survive. A
legacy unnamed savedLocations entry gets its expected default label. All existing
workspace, navigation, bookmarks, OSD and notification scenarios also pass, with
explicit unhandled-promise capture in the shared browser harness. Evidence is
under `/tmp/hict-u13-atomic-preset-qualified-dir`.

## Concurrent portable extraction during U13

Two real first starts reproduced one launcher deleting the directory while the
other tar process extracted into it. The wrapper now extracts each candidate in
a private directory, verifies its executable and marker, then atomically renames
the complete directory into the cache. Concurrent losers reuse the completed
winner. Invalid existing cache entries get a separate recovered destination.
Failed extraction cleans its private staging directory.

Four focused tests generate the exact wrapper header with a small real tar.xz
payload. A deliberately paused tar process deterministically reproduces the old
race. That regression fails on the old wrapper and passes on the correction.
The other tests verify cache reuse, preservation/recovery of invalid entries and
failed-extraction cleanup/retry. Shell syntax checks and the existing full smoke
suite also pass.

Both actual corrected .run processes begin extraction into the same cold cache
and complete successfully. One executes the three-click example journey through
Reverse, Split, Undo, session Save/Close/Restore and exact AGP export. The other
executes the offline edit/restart/session/export/Swagger journey. The example
opens in 20.277 s, the complete journey takes 28.092 s and the restored view has
31,102 signal pixels with zero browser errors. Evidence pointers are
`/tmp/hict-u13-safe-run-journey-dir` and `/tmp/hict-u13-safe-run-offline-dir`.
Focused logs are `/tmp/hict-u13-portable-extraction-tests.log` and
`/tmp/hict-u13-portable-extraction-before.log`.

## Exact resolution scaling during U13

The real signal-control audit reproduced 10/4 linear scaling dividing by 2 instead
of 2.5. ChunkedFile calculated a long integer ratio and squared it before casting.
It now performs both division and squaring as doubles. This also removes integer
overflow for large resolution ratios.

Three real converted-fixture cases cover fractional 10/4, preserved integer 8/4
and 10,000,000,000/1 ratios. The old code fails the fractional and overflow cases.
The corrected full JVM suite passes 436 tests with seven optional skips.

The browser gate checks 25 independently known fine-level contacts, actual slider
keyboard/drag color changes, slider sensitivity and domain repair, loaded/exact
ranges, log1p(x)/log(2), invalid-base preservation, panel remount and declared
Log10 Reset. Coarse native contacts [11,0,0,11] match exact None/linear/quadratic
scaling at ratio 2.5. Pixel [249,185,185] changes to [246,158,158] after contrast
adjustment while raw contacts remain exact. Evidence pointers are
`/tmp/hict-u13-signal-control-audit-dir` and
`/tmp/hict-u13-signal-controls-fractional-fixed-dir`.

## Dialog focus and notice overlap during U13

The eighteen-destination audit reproduced menu-launched dialogs returning focus
to the document body. Bootstrap can hide its focused action before a lazy dialog
mounts. The shared focus helper now remembers the activating control weakly,
restores its visible dropdown toggle when necessary and removes activation
listeners when its last component owner unmounts. Resolution restriction, matrix
conversion and the assembly FASTA prompt now use the same helper and named dialogs.

The 15-second excluded-bin notice also covered footer Close buttons at 1280×720.
Its visibility now yields to the reactive modal focus stack, preserving its normal
action and lifetime. This applies only to this informational notice. The installed
toast component drops supplied class attributes, so the implementation uses its
supported style option with an inherited CSS visibility variable.

The browser gate verifies all eighteen actual destinations, Tab and Shift+Tab
containment, footer/header dismissal, visible return focus, exact native AGP
preservation, missing-FASTA picker cancellation and zero unexpected console
errors or unhandled rejections. Both direct-picker viewport cases pass. Evidence
is under `/tmp/hict-u13-dialog-all-modal-verified-dir`. Seven workspace scenarios
and the four exact signal-control scenarios pass at
`/tmp/hict-u13-dialog-workspace-dir` and `/tmp/hict-u13-dialog-signals-dir`.
The WebUI build and numerical/source regressions pass. Full source ESLint reports
zero errors and the existing 99 warnings. Failed intermediate overlap attempts
remain recorded under `/tmp/hict-u13-dialog-final-controls-dir`,
`/tmp/hict-u13-dialog-toast-verified-dir` and
`/tmp/hict-u13-dialog-guidance-verified-dir`.

## Source-independent rendering graphs during U13

The expanded pipeline audit returned exact constant values [2,2,6,2] while the
actual map pixels remained fully transparent. Per-source compilation discarded
branches without an explicit contact-source node. Constant, coordinate and
track-only expressions now render on the primary grid. Secondary source tiles
remain transparent for these branches, so the expression is drawn once.

Four parameterized regressions cover constant, row-coordinate, built-in weight
and imported coverage-track inputs across both triangles, the diagonal and Swap.
All four fail on the preceding implementation. The full corrected JVM suite
passes 440 tests with seven optional skips. Existing source-specific triangle
routing, identity transparency and blended-source tests remain green.

The expanded real-browser pipeline gate passes twelve scenarios. In addition to
the retained graph/import/histogram/conflict cases, Preview returns [2,2,6,2] and
actual opaque red-gradient pixels at values 2/8 and 6/8. Swap returns [6,6,2,6]
and exchanges those actual pixel colors. Enable toggling retains the expressions.
Preset Load changes the draft, Apply publishes it, Reload fetches server changes
and Reset survives close/reopen with identical numerical signals. The editor
serializes implicit colormap limits as explicit constant inputs. The assertion
checks those constants against the original limits before comparing the graph.
Preview publishes the graph for rendering and hides the editor.

Evidence pointers are `/tmp/hict-u13-source-independent-pipeline-dir`,
`/tmp/hict-u13-source-independent-mixed-dir` and
`/tmp/hict-u13-source-independent-identity-dir`. The mixed-pyramid and real
ModDotPlot identity suites pass. The original failed pixel audit and transparent
RGBA sample are retained under `/tmp/hict-u13-dialog-pipeline-pixel-diagnostic-dir`.
Focused failing-unit evidence is `/tmp/hict-u13-source-independent-before.log`.
The full test/build log is `/tmp/hict-u13-source-independent-tests-build.log`.

## Move cancellation during U13

The expanded structural audit reproduced Escape disarming translocation while
leaving its arrows visible. Shared tool deactivation now refreshes the existing
arrow renderer immediately. The focused browser regression checks that Escape
clears the active tool, hides every arrow layer and preserves the exact native
assembly snapshot. It then reselects, enters Move again, crosses a native level
and performs a real arrow move with exact Undo/Redo/Undo state checks.

All seven structural scenarios pass at `/tmp/hict-u13-move-escape-fixed-dir`.
The preceding failing regression remains under `/tmp/hict-u13-final-move-escape-dir`.
The WebUI build and existing numerical/source regressions pass. Focused lint
reports zero errors and three existing warnings in ContactMapManager.

## Extended release-control regressions during U13

The fifteen-scenario lifecycle gate now exercises all alignment and identity
settings through their actual dialogs. Artifact provenance records the chosen
k-mer, window, chain, minimum length, sampling interval, sketch size, neighboring
context and worker allocation. Empty resolution sets disable both submissions.
An invalid identity delta step preserves the job list, then a supported value
completes successfully. An independent scalar PAF endpoint oracle counts 49 owned
samples and matches all 1,600 native output cells through a separate read-only
server. Evidence is `/tmp/hict-u13-advanced-sequence-empty-levels/`.

The workspace gate cycles all three ruler modes while preserving the exact view,
then checks absent search suggestions preserve that view. The existing shared
ruler regression suite verifies numerical tick coordinates and relative resets.
The picker gate now attempts rejected directory traversal through Go, checks the
inline error and preserved rows, recovers through Root and loads independently
known matrix values. It accounts for exactly the expected Chromium 400 console
message. Evidence pointers are `/tmp/hict-u13-final-workspace-ruler-search-dir`
and `/tmp/hict-u13-picker-rejected-path-complete-dir`.

The packaged soak now opens and closes a real resolution dialog on every cycle,
checks Shift+Tab containment and returns focus to View before navigation sampling.
Its limits and warmup remain unchanged. Raw native-memory categories, heap/RSS
samples, retained failed results and derived metrics remain available for audit.

## Preset action reachability and retention measurements during U13

The full editor-preset audit reproduced success confirmations covering Apply.
Hovering the obstruction paused its timer. Success toasts now yield while a modal
owns focus, extending the existing targeted excluded-bin notice behavior. The
all-four editor Load/Apply journey completes with real primary and identity
sources, preserved input matrices and actual rendered pixels. All fifteen
lifecycle cases, twelve graph scenarios and eighteen modal destinations pass.
Catalog entries 3 and 4 use the descriptive names Red on white and Red on black.

The new fourteen-preset browser gate checks all 25 independently known contacts,
pre-log, imported weights, post-log, diagnostic unclamped values, colormap-clamped
values and actual RGBA against an independent calculation. Missing-overlay
actions preserve configuration and provide guidance. The active-source palette
uses manual bounds for its fixed-range assertion. Automatic range calculation
has separate numerical coverage. Evidence pointers are
`/tmp/hict-u13-all-builtin-presets-complete-dir`,
`/tmp/hict-u13-neutral-builtin-presets-dir`,
`/tmp/hict-u13-all-editor-presets-fixed/` and
`/tmp/hict-u13-modal-success-{pipeline,menus}-dir`.

The expanded thirty-minute workload failed its DOM-node gate after 226 cycles
while heap, RSS and timing limits passed. A focused twenty-dialog reproduction
retained exactly twenty detached dialog trees. Its shortest retaining path runs
through Global handles and DevTools console. Disposing console arguments, clearing
logs and adding frame checkpoints retained that behavior. Removing temporary
modal element handles from the measurement while preserving actual mouse Close,
Shift+Tab containment and visible focus return produces a flat ten-cycle result
of 1,787 nodes and 702 listeners. Chrome describes inspector-held objects in its
heap-snapshot guidance. [Chrome heap-snapshot guidance](https://developer.chrome.com/docs/devtools/memory-problems/heap-snapshots)

The soak now reads primitive DOM results and uses hit-tested mouse coordinates
for modal Close. Its original limits remain intact. DOM maxima and assertion
context are saved with the other metrics before validation. A failed run now
sets qualifiesThirtyMinutes to false. The corrected 120-second packaged preflight
passes sixteen mixed cycles, with early/late node maxima 683/689, listener maxima
229/229, frame p95 16.7 ms, acknowledgement p95 56.842 ms and zero captured errors.
The full qualification must be repeated. Focused snapshots and raw counters are
under `/tmp/hict-u13-modal-memory-{diagnostic,console,primitives,mouse,no-handles}-dir`.
The failed full run remains under
`/tmp/hict-u13-source-independent-release-soak-valid-port-dir`, and the corrected
preflight is under `/tmp/hict-u13-primitive-modal-soak-preflight-dir`.


### U13 track controls and immediate background repaint

The track background settings changed host styling while opaque track canvases
kept their previous fill until another render. MainUI now watches the effective
track background and repaints both axes from cached data. Its component scope
owns the watcher. The normal WebUI build and focused lint pass, with two existing
explicit-any warnings.

The real-browser gate passes named BED browsing and addition, exact feature
interval and score attributes, primary and secondary weight-track toggles with
independent values 2 through 6, secondary attachment and detachment, explicit and
inherited background pixels on both displayed and exported canvases, and
precompute completion after closing the track dialog. Assembly AGP is unchanged.
The prior background failure remains in
`/tmp/hict-u13-track-controls-feature-count-dir`. Passing evidence is in
`/tmp/hict-u13-track-background-fixed-dir`.


### U13 Windows source identity regressions

Existing GitHub Actions run 43010279561 at fd46e1fd passes Linux, macOS arm64
and macOS x86-64 Java jobs. Its Windows JUnit artifact reports three failures
in conversion-cache invalidation and plain/gzip FASTA replacement detection.
Only existing runs, logs and artifacts were read or downloaded.
[Java regression run](https://github.com/ctlab/HiCT_JVM/actions/runs/43010279561)

Conversion cache hits now compare full-precision modification time and bounded
content identity before reusing full SHA-256/SHA-512 results. Fresh hashes verify
that the source remains stable before caching. FASTA stamps use the same bounded
content check when the provider exposes no file key. HICT-QUICK-1 sampling moved
to a shared library utility while retaining its existing session byte format.
Large files retain the existing approximately 9 MiB sampling bound.

The conversion regression explicitly restores the original timestamp after an
equal-size write. The session regression independently checks the sampling
format digest. All 440 local Java tests pass with seven optional skips and zero
failures, recorded in `/tmp/hict-u13-windows-identity-full.log`. All fifteen
sequence lifecycle browser cases pass against the corrected fat JAR, including
FASTA linking, split/invert sequence delivery, BLAST, minimap, overlays, graph
round trips and selected sequence exports. Evidence is under
`/tmp/hict-u13-file-identity-lifecycle/`. Windows execution of this local correction
remains unverified. Existing portable run 43010279649 is still in progress at
this checkpoint.


### U11 owner-selected Borrelia Hi-C and yeast Micro-C

Both owner-selected examples now have three-click quick-start cards. The existing
tardigrade card remains available. Borrelia downloads a pinned 1,907,128-byte
MCool and the original 406,111-byte GenBank FASTA gzip. Its declarative recipe
selects 19 records, checks their exact lengths, preserves sequence case and bases,
renames their headers and atomically publishes the checksum-pinned reference.
The original gzip is retained. The prepared FASTA is 1,477,994 bytes uncompressed.
The earlier 393,716-byte figure describes its compressed form. Yeast uses the
166,935,036-byte MCool and 3,820,548-byte sacCer3 FASTA gzip directly.

Focused tests cover selection, ordering, case preservation, original-file
immutability, duplicate/missing/wrong-length records, unsafe catalog names,
bounded memory, checksum failure, cancellation, output preservation and temporary
cleanup. A downloader integration test verifies both original and derived hashes
in the manifest. All 445 Java tests pass, with seven optional skips and zero
failures. The normal WebUI build passes. Focused lint has zero errors and two
existing explicit-any warnings.

The generalized browser gate independently checks every mapped chromosome name
and length against MCool, all prepared Borrelia records against original GenBank
bases, the first 1,000 linked bases, rendered signal, navigation, Reverse,
Split/Undo, Save/Close/restore and the exact exported AGP hash. Borrelia completes
from empty data in three clicks, with map opening at 9.191 seconds and the full
journey at 12.772 seconds. It restores 86,371 signal pixels. Evidence is under
`/tmp/hict-u13-borrelia-journey-complete-dir`.

Yeast's initial empty-data run downloads the public sources, opens and verifies
the map and reference, then encounters the test's inherited five-contig assertion.
Subsequent repeats also expose the five-component AGP assertion. The generalized
checks now use each dataset's actual contig count. The passing repeat uses copied
verified downloads, performs a fresh conversion, opens in 129.886 seconds and
completes its journey in 135.473 seconds, restoring 7,630 signal pixels. Both
passing journeys capture zero page or console errors. Evidence is under
`/tmp/hict-u13-yeast-journey-complete-dir`. Failed harness runs are retained under
`/tmp/hict-u13-{borrelia,yeast}-selected-journey-dir` and the intervening pointers.
The split action now waits for actual loaded pixels after Reverse before double
clicking. It also clears the preceding selection with Escape.

The exact final preparation source also passes a fresh public-download Borrelia
journey at 9.098 seconds to the map and 12.646 seconds overall, with 86,371
restored signal pixels and zero captured errors. Evidence is under
`/tmp/hict-u13-borrelia-final-source-journey-dir`. Catalog validation also reports
a malformed file-list JSON pointer safely when a preparation recipe is present.


### U13 asynchronous logging CI test correction

The owner-pushed 2e48a9f0 revision passes Windows and Intel macOS Java CI. Linux
and Apple Silicon macOS fail only AsyncLoggingTest. Their downloaded JUnit
artifacts show the fake console throws AssertionError from InterruptedException
when AsyncAppender.stop interrupts its worker immediately after the release
latch opens. This terminates the test worker before any of its 201 events drain.
[Java regression run](https://github.com/ctlab/HiCT_JVM/actions/runs/43014849030)

The fake sink now accepts that shutdown interruption after release, preserves
the interrupt status and continues counting delivered events. An interruption
before release still fails. The test repeats twenty times with the same checks
for nonblocking enqueue, zero delivery while blocked and complete delivery of
all 201 events on stop. All repetitions pass in
`/tmp/hict-u13-async-shutdown-repeat.log`. The current working-tree Java suite
passes 466 tests with seven optional skips, including in-progress full-preset
regressions, in `/tmp/hict-u13-async-shutdown-full.log`. CI execution containing
this test correction remains pending. Existing failing artifacts are retained
under `/tmp/hict-u13-identity-ci-junit/`. All remote inspection was read-only.


### U13 complete rendering graph presets

Save now captures applied options and both rendering branches from one scheduler
snapshot. The control explicitly labels that applied-state behavior and shows
Saving while the request is pending. The optional renderPipeline payload travels
through local preset records, deduplication, JSON export/import and native session
libraries. Older option-only records retain their application path. Native restore
preserves preset IDs. Wizard source-style choices explicitly label complete presets
when extracting their options to construct the wizard presentation.

A shared bounded parser validates expressions before committing an imported batch.
It preserves the original AST, branch flags and optional editor state. Editor cache
records include the applied expression signature. Capture uses matching layouts
and disconnected inspectors, and tolerates unavailable optional storage. Snapshot
sRGB float components are rounded back to their original 8-bit channels before
using the legacy option decoder. This fixes a reproduced fractional RGB string
that the API rejected during the first full-preset application attempt.

Explicit preset publication uses the existing atomic options-and-pipeline request
through VisualizationManager. Existing option events and expected-profile refresh
run after acceptance. Enabled pipelines with unavailable secondary or track inputs
receive HTTP 409 before publication. Background and border styles apply after
acceptance. Same-filename imports reset their file input after reading, and file
read failures provide feedback. Invalid later graph records preserve the entire
library and current view. Native document bounds support the editor's deep graphs
while retaining expression, total-value and byte limits.

The six-case browser gate compares all 25 signal values and actual pixels for two
distinct constant branches with swapped triangles, exact exported/imported AST,
editor viewport and a disconnected histogram, native-session library identity,
100 nested unary nodes, expected-mode profile initialization and independent
nonzero-contact diagonal means. Missing secondary and track cases check exact
server options, client options, graph, styles, background, view and resolution.
Their two intentional HTTP conflicts are explicitly counted. A valid first item
followed by an invalid graph leaves the import batch unapplied. Evidence is under
`/tmp/hict-u13-full-graph-presets-complete-profile-dir`.

Existing browser suites pass all fourteen built-in presets, twelve editor cases,
legacy session validation and all seven workspace scenarios with the required
large structural fixture. Evidence pointers are
`/tmp/hict-u13-full-preset-builtins-dir`,
`/tmp/hict-u13-full-preset-editor-dir`,
`/tmp/hict-u13-full-preset-session-validation-dir` and
`/tmp/hict-u13-full-preset-workspace-large-dir`.
Unit checks cover detached copies, invalid expressions, unavailable storage,
matching and stale layout signatures, exact float-color reconstruction and native
session depth boundaries. The normal WebUI build and explicit session document
suite pass. Full lint records zero errors.
The Java working-tree suite passes 466 tests with seven optional skips, including
twenty logging-race repetitions. Final package qualification remains required.

The final six-case gate also restores non-default contig/scaffold widths and
label size with the saved background. Full-graph imports preserve the active
visualization until selection. Import reuses native library validation, including
style validation and legacy empty-style normalization. An invalid style record
preserves the library and live state. Existing option-only import handling remains
supported. Final evidence is under
`/tmp/hict-u13-full-presets-import-atomic-final-dir`, with the seven-scenario
workspace repeat under `/tmp/hict-u13-full-presets-workspace-final-dir`.

API alpha decoding restores byte values within the rounding error of its
six-decimal serialization and rejects values outside zero through one before
integer conversion. Other fractional inputs retain their established truncation,
including 0.5 mapping to 127 and 0.1 mapping to 25. The previous
floor conversion could lose one alpha level when reading the API's six-decimal
color representation. The DTO regression covers all 256 alpha values and the
snapshot serializer covers all 256 channel values. The final Java suite passes
468 tests with seven optional skips in
`/tmp/hict-u13-full-preset-alpha-full.log`. Final UI lint records zero errors and
99 warnings in `/tmp/hict-u13-full-preset-lossless-final-lint.log`.


### U13 alpha compatibility and secondary replacement regate

The full session browser regression caught a compatibility change introduced by
general nearest-byte alpha rounding. Decoding now snaps to a byte only within
the error bound of the API's six-decimal serializer. Existing fractional inputs
retain truncation, including 0.5 to 127 and 0.1 to 25. All 256 serialized byte
values still restore exactly. Finite-range validation remains in place.

Secondary replacement synchronization now waits for the completed notification
to exist while its parent modal intentionally suppresses success toasts. The
subsequent assertions still compare both exact edited contact matrices, client
strands, the journal, rejected/cancelled replacement preservation and the
confirmed replacement's returned assembly.
The full Java suite passes 468 tests with seven optional skips in
`/tmp/hict-u13-alpha-compatible-tests.log`. Browser result records are in
`/tmp/hict-u13-alpha-compatible-browser.json`.


### U13 remaining navigation and caption controls

The navigation gate now compares all rendered layer canvases before and after
Reload, alongside the exact graph and viewport. Actual toolbar and native
zoom controls preserve the constrained center and resolution at both endpoints.
The workspace gate now collapses and expands Borders & labels and applies all
five caption sides to contig and scaffold names. Independent SVG text coordinates
include each ancestor affine transform and distinguish map captions from ruler
band labels. Above/Inside/Below and Left/Inside/Right positions are ordered
independently for the known contig and scaffold. All eight workspace cases pass.
Evidence pointers are `/tmp/hict-u13-navigation-pixels-endpoints-dir` and
`/tmp/hict-u13-caption-side-controls-transformed-dir`.


### U13 custom colormap targeting and alpha preservation

The source/triangle audit reproduced a contrast edit changing a custom blended
colormap's opaque minimum to transparent. Generic hydration inferred a source-wide
transparency policy from any top-layer occurrence. Generic publication also
rewrote top-layer colors beyond the selected target. These overrides have been
removed. Hydration reads the target's configured colors, and publication applies
the chosen style to the selected target set. Built-in overlay constructors retain
their explicit transparent colors. Explicit color alpha remains editable.

The new browser scenario starts with three primary colormaps and one secondary
colormap across the two triangles. Its selected target has alpha127. An individual
threshold edit preserves that alpha, every unselected expression and lower-triangle
pixels. Linked editing updates the primary target set, and secondary editing
preserves primary branches. Both source-order and triangle swap controls round
trip the exact graph and pixels while all raw contacts retain known values.
The focused scenario passes under `/tmp/hict-u13-preserve-target-alpha-dir`.
The full WebUI build passes, and full lint records zero errors and99warnings.
The interface sweep and focused overlay suites pass. A resolution-checkbox
timeout during the parallel sweep passes on its isolated repeat with the same
checks and timeout. Its failed record remains available for review.
Final packages and the full-duration soak must qualify this newer UI revision.

The export audit also passes source browsing and dismissal, actual overwrite of
a disposable existing output, Refresh jobs preserving a draft and exact job IDs,
and all eight independent Cooler contact/weight/compression oracles. Evidence is
`/tmp/hict-u13-compatible-final-export-controls-retry-dir`.
The sixteen-case sequence lifecycle adds exact/substitution modes, literal and
expanded IUPAC ambiguity, forward-only and both strands. Independent scans
match every exported BED coordinate and strand in five variants. Evidence is
`/tmp/hict-u13-compatible-final-search-controls/`. Final Launcher acceptance and
the isolated96-contig navigation repeat also pass.


Primary sidebar publication now reuses the atomic options-and-graph endpoint.
Secondary style publication retains its graph transaction. Manual Apply consumes
its pending live-apply timer, and sidebar failures use the existing deduplicated
notification helper with diagnostics. The forced HTTP503 case holds beyond the
debounce interval, observes exactly one request and one error, and compares the
unchanged server options, graph and displayed pixels before an explicit retry.
Both actual endpoint color pickers accept fractional alpha and update precisely
the linked primary target set, with different displayed colors and unchanged
raw contacts. Final evidence is under
`/tmp/hict-u13-target-alpha-color-pickers-dir`.

All fourteen built-ins, six full-graph cases, twelve editor cases, five track
cases and sixteen real-tool sequence cases pass on the final implementation.
Results are in `/tmp/hict-u13-target-alpha-final-focused.json`. The normal build
and full lint pass with zero errors and99warnings. The broader interface sweep
is recorded in `/tmp/hict-u13-target-alpha-regate-{core,open-session,quad,extra}.json`,
with the navigation repeat in `/tmp/hict-u13-target-alpha-navigation-isolated.json`.
The matrix export extension also browses a custom identity AGP and checks every
resulting contact independently. It passes under
`/tmp/hict-u13-target-alpha-export-agp-dir`.


### U13 pending weight-track toggle race

The actual ea0b44d9/b6e01ce Linux package passed seven source-style, six full-preset,
fourteen built-in and twelve editor scenarios. Its track gate then reproduced
a rapid off toggle before the dialog had finished refreshing the added weight
track. The manager's track snapshot already contained its ID while the dialog's
local ID list remained stale. The off handler saw an empty list and retained
the weight track. The package qualification stopped at that failure.

Both weight switches now share a pending-operation state until the track list
and precompute status refresh have completed. The dialog shows progress and
prevents overlapping toggles while the mutation settles. Each input restores
its committed checked state after success or failure. The existing reportFailure
path provides the failed action's diagnostics.

The six-case browser gate holds actual primary and secondary creation requests
while asserting the switches are busy, then releases them and verifies all five
known weight values and removal. A forced503 create response restores the
unchecked, enabled control and removes progress while preserving the exact
track list and feature value. The original background/export/precompute cases
also pass. Evidence is under `/tmp/hict-u13-weight-toggle-race-complete-dir`.
The preceding unsuccessful extended harness attempted to open Tracks while its
completed Jobs dialog remained open. It now closes that dialog between scenarios.
This newer UI requires its own packaged qualification and full-duration soak.

The full six-case gate passes three further repetitions with all original value,
pixel and lifecycle assertions. Results are recorded in
`/tmp/hict-u13-weight-toggle-repetitions.json`. The complete WebUI build passes.
The one new directive-format warning was corrected locally, preserving the
existing full-source lint baseline.

## U13 assembly-strand correction

Public hictk v2.2.0 fuzzer inputs exposed inverted AGP orientation in the matrix
importer. PLUS, UNKNOWN and IRRELEVANT wrote the persisted REVERSED ordinal.
The importer now uses the named ContigDirection constants for both orientations.
A regression fails on the previous mapping and verifies all five accepted AGP
strand tokens, stored metadata, reopened assembly direction and every matrix cell
of a nonuniform diagonal fixture. The full JVM suite passes 470 tests with zero
failures, zero errors and seven optional skips.

Conversion-cache generation advances from 2 to 3. Older cache records request
fresh conversion through the existing UI. A regression verifies this decision
and preservation of the existing output and cache record. Existing standalone
HiCT files retain their recorded assembly. Regenerate affected imports from
the original matrix and layout to obtain corrected strand interpretation.

`scripts/check_matrix_input_formats_browser.py` uses actual browser opening,
batch conversion with one thread, completion, converted-map opening, rendered
signal and original-source cached reopening. Independent bundled hictk dumps
supply all 625 first-chromosome RAW_COUNTS values at 10 Mb for each format.
Both v8 and v9 pass, including unchanged source hashes and no extra conversion
job on reopening. Test evidence is under
`/tmp/hict-u13-strand-public-hic.iLrKJC/evidence`.

The fixtures are the official hictk v2.2.0 fuzzer datasets. Verified downloads
retain their original bytes with a `.hic` suffix for the normal picker.

| Format | Bytes | MD5 | SHA-256 |
|---|---|---|---|
| v8 | 412820 | `2b946bfd2627e38fae8985df9f4a711d` | `cef8c89214b977e4ca09d08cfa58d339818aa3fddc576f7217c082e675fdeb80` |
| v9 | 426063 | `1a5cc53b4f079e95d61c71f275fe513e` | `a710aca0fef452c367636b7d8d29bd2bf6ab4d03aba3e6eba678e7cbebf89971` |

Provenance is recorded in the [hictk fixture manifest](https://github.com/paulsengroup/hictk/blob/v2.2.0/test/fuzzer/test_files.json),
[v8 dataset](https://zenodo.org/records/13754786) and
[v9 dataset](https://zenodo.org/records/13754808).

The owner deferred Windows and macOS interactive GUI qualification on
2026-11-11. `docs/release-manual-checklist.md` retains those internal follow-ups.
Current platform evidence records automated CI scope and local Linux interaction.

## U13 explicit performance thresholds

The baseline command retains its diagnostic default and accepts `--release-gate`
for release qualification. The explicit mode requires frame p95 at most 33 ms
and input acknowledgement p95 at most 100 ms in both fresh-browser and repeat-open
samples. Results retain measured values and the enforced limits. Boundary checks
accept 33/100 ms and reject either 33.3 ms frames or 100.1 ms acknowledgement.
Evidence is `/tmp/hict-u13-release-limit-regression.json`.

The first corrected-package 7790-contig diagnostic records frame p95 of 33.30 and
33.355 ms, with one and nine long tasks. These samples exceed the release frame
limit and remain retained. At the time, read-only host inspection found a separate
32 GiB-limit external JVM using approximately 33 GB RSS and nearly full 31 GiB
host swap. The earlier 96- and 1765-contig samples recorded frame p95 of 16.8 ms.
Repeat measurements follow the full soak under the original limits.

## U13 corrected-package qualification

JVM `c9b15edb` and WebUI `95866b5` pass the 24-stage sequential Linux functional
qualifier. Both immutable packages pass public `.hic` v8/v9 source counts and
cached reopening. The stages cover applied custom graphs, all built-in palettes,
editor, pending/error track switches, sessions, independent matrix exports,
metadata, selected example journeys, offline behavior, concurrent extraction,
Launcher and actual Tauri/Electron Save/Cancel, reset and rasterized exports.
Artifact identities are in `/tmp/hict-u13-strand-final-artifacts.json`.

The full mixed soak passes 227 cycles in 1803.964 seconds with zero errors.
Browser heap growth is 985832 bytes with a 0.056408 MiB/min slope. Java heap growth
is 2104320 bytes with a 0.123285 MiB/min slope. RSS early/late growth is 18661376
bytes and its regression slope is -0.554945 MiB/min. DOM and listener maxima
remain 693/693 and 233/233. Frame p95 is 16.8 ms and acknowledgement p95 is
58.4743 ms. Original limits remain unchanged. The separate host workload was
closed about halfway through this soak. All raw samples remain retained under
`/tmp/hict-u13-strand-final-soak.PXzZK3/evidence`.

All three after-soak performance repetitions pass `--release-gate`. Every fresh
and repeat sample has zero long tasks. Frame p95 is 16.7–16.8 ms and input
acknowledgement p95 is at most 40.3 ms. The initial loaded-host 7790-contig timing
failure remains retained. Fresh-browser/repeat-open conditions retain OS and
server caches. Results use `/tmp/hict-u13-strand-final-release-timing-<count>-dir`.

Both larger DNA Zoo cards pass fresh conversions using verified source downloads.
Cheetah conversion/open takes 598.36 seconds and Aedes takes 302.04 seconds. Each
has 11 levels, the expected 19/3 contigs, zero reference mismatch warnings and no
unexpected browser errors. Independent original 2.5 Mb pixel sums match all
576/1024 first-chromosome cells at generated 10 Mb. Coarse diagonal aggregation
counts each upper-triangle contact once. Both first 1000 forward FASTA bases
match exactly, all catalog source checksums match and visible signal counts are
1398/2715. The first Cheetah oracle requested a generated level absent from its
original `.hic`. That failed record remains preserved. The corrected oracle
continues on the fresh converted output without repeating conversion.

A test-preparation failure filled temporary storage when requested reflinks fell
back to full copies. The redundant unused source copy and pre-launch partial
copy were removed. Completed build payload caches were retired after matching
embedded JAR hashes to frozen manifests. Original inputs and immutable artifacts
remain retained. Large-card testing then used a single disposable physical copy
moved into its isolated data directory.

The retained tardigrade quick start passes its full journey in 28.63 seconds,
reaching the map in 21.29 seconds and restoring 31289 signal pixels. The revised
quick-start check verifies all five mapped reference names, lengths and forward
directions and the first 1000 original FASTA bases exactly. Full source and
reference checks are also retained for the owner-selected Borrelia and yeast.
The updated catalog records each case's scope and timings.

Current-source Java CI 43058445089 passes all four targets with 470 full-suite
tests each plus a separate successful native baseline. Optional skips are 12 on
Linux/macOS and 14 on Windows. Independent XML reports confirm these counts.
The owner approved cancelling superseded portable run 43045033635 after its Linux
prerequisite installation spent over an hour retrying mirror DNS failures.
Current portable run 43058445223 passes Windows and Linux packaging with full
WebUI revision `95866b5d89a7946d44de2b5bee8ec09db70637b9`. Packaged helper startup,
AVX2/AVX512 and gapped BLAST fixtures with threads 1/2 pass on those targets.
macOS results remain in progress at this record's creation. Source pushes remain
owner-controlled. GitHub operations comprise read-only inspection, the explicitly approved
superseded-run cancellation and dispatch of the current-source build-only WebUI
CI workflow.

## U13 final cross-platform result

Java workflow 43058445089 and WebUI workflow 43076202090 pass all four targets at
JVM `c9b15edb` and WebUI `95866b5`. Portable workflow 43058445223 passes all four
native-tool jobs, all four package jobs and publication job 130725909419. Intel
package job 130700908332 confirms the full expected UI revision and executes
seqkit, ModDotPlot, hict-moddotplot, hictk, minimap2, AVX2 and the gapped BLAST
fixture with one and two threads successfully.

The published CI version is `2.0.252-ci1-c9b15ed`. Local stable artifacts remain
version 2.0.0. Build provenance identifies the exact JVM source and workflow.
All 16 aggregate checksum entries match GitHub's independently reported release
asset digests. Downloaded provenance/checksum files also match their API digests.
Evidence is `/tmp/hict-u13-published-ci252/verification.json`. Local delivery files
were independently rehashed and still match their frozen artifact manifest.
The final release report records both version families and per-platform hashes.

The release decision is GO for the frozen application revisions with the
owner’s Windows/macOS GUI checks deferred. That future checklist remains
in `docs/release-manual-checklist.md`. Optional private-data fixtures and existing
development dependency findings retain their documented scope. Qualification
records and acceptance-helper updates follow the frozen application build.
No Java, native processing or WebUI runtime source changes follow `c9b15edb` /
`95866b5` in this validation-record package.
# Advisor item 1: launcher return after Stop (2026-11-16)

Stop now restores the activity/start card on the Swing event-dispatch thread only
after owned server and browser processes have stopped. Configuration and logs
are hidden without clearing settings or log contents. Close-after-stop retains
its separate close behavior. Full JVM suite: 519 tests, zero failures/errors,
seven optional skips. Extended `scripts/check_launcher_reset.py` passed against
the rebuilt fat JAR on private Xvfb: a real server starts and serves `/version`,
Stop from the configuration view terminates it, the simple card is visible, the
log is hidden and the configuration toggle reads Show Configuration. Screenshot
visually inspected: `/tmp/hict-launcher-reset-blgi6lri/after-stop.png`.
Full native-wrapper/platform/repeated-start matrix remains pending; this check
does not qualify Windows or macOS. One premature acceptance invocation overlapped
JAR rebuilding and failed reading the incomplete archive; rerun after build
completion passed without changing application behavior.

## Advisor P1 prerequisite: explicit fragment-bin coordinates (2026-11-16)

Assembly descriptors now expose sparse source-forward first-bin origins per
resolution, derived from immutable physical ATU ownership. Compact responses
retain the nine-column contig rows and add offsets by contig ID. The WebUI
uses explicit origins for native-level integer and fractional coordinate
conversion, including reversed partial bins. Older responses and synthetic
levels retain their existing fallback path. No split algorithm is changed in
this increment: exact non-loss cuts, native shared-bin identity accounting,
session v4 and approximate matrix exports remain pending.

Validation: full Java suite 519 tests, zero failures/errors, seven optional
skips; WebUI build and included regressions passed. DTO checks cover legacy and
compact parity, invalid offsets and older responses. Source interval enumeration
checks cover partial bins and fractional inverse coordinates on both strands.
The independent browser oracle checks every retained base of a 4 kb source at
100/200 bp after a real legacy split, then after reversal. Focused evidence:
`/tmp/hict-bin-geometry-focused/fragment-geometry.json`; screenshot inspected.
The initial browser oracle caught native levels being marked ensured by the
renderer, bypassing explicit geometry; tracking native levels separately fixed
it. A reversed fractional lookup used a continuous position as an integer base
index; flooring before bin lookup fixed its half-base error. A Java oracle
initially assumed coarse bin ownership began at source bp 10; actual legacy
ATUs retain the source bp 0 bin, so the corrected oracle records origin -12
rather than changing historical cut semantics.

Final browser suite: all 17 scenarios passed with zero page errors, including
local BLAST, search delivery, ModDotPlot generation/accumulation, pipeline round
trips, selection exports and the new source-bin oracle. Evidence:
`/tmp/hict-bin-geometry-qualified/summary.json`. One harness-only rerun was needed
because the new scenario reloads the page: historical notification assertions
now run before that reload, preserving their original coverage. Focused ESLint
returned success with existing schema-resolution notices and no code errors.

## Advisor P1 exact-cut library and v4 sessions (2026-11-16)

The library now supports exact oriented-contig boundary cuts with atomic rollback,
intersecting ATU slices and typed exact lineage. Both children reference a bin
crossing the cut; their source sequence intervals are disjoint and exhaustive.
No contacts are rewritten. Unit tests enumerate every interior boundary of the
17 bp source on both strands at 4/10 bp resolutions, compare physical stripe/bin
references, check gap-free FASTA and unchanged source-file bytes, and exercise
repeated cuts, reversal, checkpoint undo/redo, invalid cuts and session replay.
Legacy cuts retain bin removal and their original replay path.

New captures use snapshot v2 and portable session v4. The readers still accept
snapshot v1 and portable sessions v1-v3. Exact lineage requires snapshot v2;
geometry fields are validated when present. A regression restores historical
snapshots lacking bin-origin fields without silently reintroducing sequence
removed by the older cut operation.

Validation: 523 JVM tests, zero failures/errors, seven optional skips; WebUI build
and included regression checks pass. Existing session browser suite passed all
18 checks with zero page errors (`/tmp/hict-exact-cut-session/snapshots/results.json`).
The new `check_exact_cut_session_browser.py` restores a prepared two-cut/reversed
library state through the real v4 file chooser, compares every retained base's
pixel/bin projection with independently enumerated source intervals, verifies the
17-base reverse-complement FASTA, then saves/closes/reopens it. All checks passed
(`/tmp/hict-exact-cut-ui/results.json`); the restored view screenshot was inspected.
The script uses disposable physical copies and its own server. Initial version
assertions in the HTTP test were updated from v1 to the new v2 capture contract;
all v1 compatibility tests remain. Focused ESLint has no errors and two existing
explicit-any warnings in MainUIComponent.vue.

Scope still open: HTTP exact-cut entry point and scissors/feature actions,
primary/secondary synchronization, native shared-bin statistics/provenance,
approximate Cooler export, long source offsets and sequence-scale display.
No claim of completed advisor item 21 or release readiness is made here.

## Advisor P1 guarded exact edits and source synchronization (2026-11-16)

The new exact-cut endpoint requires the displayed history session and revision
in headers and a matching revision in its strictly validated body. Stale edits
return 409. The existing journal snapshots both sources for undo/redo and
rollback. The WebUI adapter uses this contract; scissors retain their legacy
behavior until sequence display, statistics and export prerequisites are ready.

Synchronization now projects exact sequence components from immutable original
ATUs using all intersecting bins. This fixes the old AGP start-ownership policy
dropping crossing bins when the secondary pyramid differs. Optional legacy
descriptor flags/compact ID lists carry the policy, including projected sources
without local cut lineage. Session restoration reconstructs those ATUs and IDs.
Tests cover late attachment, repeated cuts, reversal, changing authority and
secondary-session replay at 4/10 versus 3/7 bp levels. A separate regression
confirms coverage preparation scans unique stored contacts and is unchanged by
repeated exact cuts.

Validation: 526 JVM tests, zero failures/errors, seven optional skips; WebUI build
and DTO checks passed. Chromium passed all nine exact-cut checks including the
request adapter, stale rejection, keyboard undo/redo and save/close/reopen; the
restored screenshot was inspected (`/tmp/hict-exact-api-browser-final/results.json`).
The existing session suite passed all 18 checks with zero page errors
(`/tmp/hict-exact-sync-session-browser/snapshots/results.json`). The new HTTP test
initially lacked the harness's visualization options required for session capture;
adding the production-equivalent options fixed it. A repeated-success notification
needed a nonunique-toast-aware browser locator. These were test setup failures.

## Advisor P1 numeric source-bin provenance (2026-11-16)

`/matrix/query` now labels source-bin references versus pipeline display-grid
expressions in JSON and browser-readable headers. Optional JSON-only provenance
returns compact, oriented physical-bin runs and per-window reference/unique-bin
counts. Padding is unrepresented by runs. Physical identity includes source and
resolution as well as stripe and index. Run union accounting distinguishes
different stripes and overlapping reversed ranges without expanding bins in
memory. Raw and weighted values are unchanged. Statistics counts are explicitly
labeled `DISPLAY_CELLS`; this is not a claim of independent-contact statistics.

Unit/HTTP regressions check original contact parity after repeated cuts and
reversal, query clipping and padding, hidden-vs-all-bin axes, immutable runs,
cross-stripe identities, primary/secondary queries, opt-in behavior, binary
semantics headers and rejection of binary provenance requests. All 528 JVM tests
passed with zero failures/errors and seven optional skips. No WebUI source changed.

Chromium passed all ten extended exact-cut/session checks against the built JAR,
including actual cross-origin header access, every numeric cell mapped to its
original physical bins and conservation after deduplicating unordered contacts.
The 4 bp level has seven references to five unique bins; 10 bp has five references
to two bins. Both retain the independently computed unique contact sum of 22.
Evidence: `/tmp/hict-provenance-browser/results.json`; restored screenshot inspected.
Remaining shared-bin expected/histogram accounting and approximate exports still
require implementation and qualification before switching scissors.

## Advisor P1 long source sequence offsets (2026-11-16)

Source sequence offsets are now longs across contig descriptors, exact slices,
historical cut arithmetic, AGP import, FASTA extraction calls, cut lineage and
legacy/compact responses. Session restoration reads longs. Session decoding
validates both offsets and complete source intervals against the maximum exact
JavaScript integer, preventing unsafe JSON coordinates. Existing session versions
and historical bin-removal semantics remain covered.

A real Cooler-to-HiCT fixture declares a 3,000,000,017 bp contig with only four
coarse bins. Tests cover nested exact cuts at source offsets 2,500,000,001 and
2,500,000,002, one-base fragment snapshots, source-bin mapping/session parity,
legacy AGP slices and historical cuts on both strands retaining the 3,000,000,000
source offset. Decoder tests cover large accepted offsets and rejected unsafe or
overflowing intervals. All 533 JVM tests passed with zero failures/errors and
seven optional skips. No WebUI source changed.

`check_long_source_offset_browser.py` passed all five acceptance checks with zero
page errors through the real adapter and session download/file chooser. Compact
responses, saved lineage, restored source intervals and AGP endpoints retain all
digits. Evidence: `/tmp/hict-long-offset-browser/results.json`; restored screenshot
inspected. Fixture generation uses `ReinforcementHdf5Fixture long-offset <tmp-dir>`
on the built JAR plus test classes. This deliberately creates no multi-gigabase
FASTA; large-FASTA I/O and proper sequence-scale display remain separate gates.

## Advisor P1 explicit approximate Cooler export (2026-11-16)

Added an opt-in sequence-overlap projection and float64 `.cool`/`.mcool` writer,
wired through library conversion, single/batch jobs, both CLI entry points and
the matrix export dialog. The native source is read-only. Projection uses
original physical-bin identities, actual terminal-bin lengths and independent
uniform endpoints. Source-diagonal contacts allocate twice the fraction product
to distinct target bins. Partial retention is not renormalized. Each stored
resolution is estimated independently, explicitly labeled in output metadata.
The method, limits and API/CLI examples are in `approximate-cooler-export.md`.

The writer uses a bounded endpoint cache and a bounded-memory/fan-in fractional
COO merge sort. It records sequence coverage for gap bins, original/retained/
exported totals and a conservation tolerance, and publishes only validated output.
Cancellation and output failures preserve an existing target and remove owned
intermediate files. Estimated exports bypass the ordinary conversion cache.
Ordinary export still requires explicit consent before using this approximation.
Incompatible balancing is rejected; the UI visibly selects internal, unbalanced
output and restores earlier settings when approximation is deselected.

Tests enumerate all interior cuts of the 17 bp fixture on both strands and compare
every projected cell with independent base-endpoint enumeration. Additional cases
cover repeated cuts, gaps, omitted sequence, short terminal bins, multiple physical
stripes, ambiguous/copying layouts, empty Cooler output, multipass fractional
sorting, cancellation, failure cleanup, CLI forms and older Jobs output schemas.
All 548 JVM tests passed: zero failures/errors and seven optional skips. WebUI
build and DTO regressions passed; focused ESLint has zero errors and five existing
unused-import warnings. No new runtime dependency was added.

The Chromium journey explicitly refuses export until consent, verifies control
restoration, dismisses the export dialog, downloads its finished Jobs result, and
then exercises batch export with default unbalanced output. Python Cooler opens
both actual artifacts and a separate base-endpoint oracle checks every cell and
the symmetric matrix reader. At 4/10 bp, both retain 22 unordered contacts; maximum
cell error is 8.9e-16. Native source SHA-256 is unchanged and there are zero page
errors. Evidence: `/tmp/hict-approximate-export-qualified/{results,cooler-parity,
batch-cooler-parity}.json`; both final screenshots were inspected. Screenshot
review prompted clearing corrected validation feedback and replacing raw Jobs
download endpoints with readable filenames via an optional `displayName` field.

Actual packaged `convert hict-to-mcool` CLI exports passed independent Cooler
parity with both a bare approximation flag and the launcher's `yes` form:
`/tmp/hict-approx-cli/{cooler-parity,boolean-cooler-parity}.json`. The guide includes
the required `convert` command prefix. An initial opt-in test accidentally chose
the generated input Cooler's filename as its output; a distinct export name fixed
that test setup. The browser harness also needed the list endpoint's POST method.

Remaining P1 gates include shared-bin expected/histogram accounting, uniform
sequence-scale geometry, scissors/feature actions, larger-map performance and
reimport provenance qualification. Neither P1 nor the publication release is
claimed complete by this increment.

## Advisor P1 estimated-source reimport provenance (2026-11-16)

Reopening an approximate Cooler previously retained fractional values but lost
its approximation marker. The importer now preserves the declared method,
resolution policy, estimation AGP and level metadata in the native file. A new
optional capability summary labels primary and secondary sources as Estimated
without inventing their signal units or changing normalization availability.
Session save/restore recomputes the declaration from the source file and retains
it in the capture. Further approximate export marks already-estimated input.
Sequence-derived overlays ignore contact-template estimation provenance.

The hictk preparation path restores provenance from the original input after
importing a prepared Cooler, since external preparation can drop root attributes.
A single-Cooler-root regression covers this bridge independently of its optional
group. Full tool-backed preparation still needs package qualification. The new
round-trip regression also caught a small-map HDF5 assertion when default chunks
exceeded axis dimensions; export chunks are now clamped to the level's bin count.

All 552 JVM tests passed with zero failures/errors and seven optional skips.
WebUI build and capability parser regressions passed. Focused ESLint has zero
errors and three pre-existing unused-handler warnings in NavigationBar.vue.
Tests check every fractional cell on both native levels with exact equality,
the saved source capabilities, incomplete declarations, preparation restoration,
already-estimated input and exclusion of sequence-derived score templates.

The extended Chromium journey exports and downloads the file, reimports it as a
background job, opens it through Jobs, attaches an estimated secondary source,
and saves/closes/restores the session. All twelve checks passed with zero page
errors. An independent Python Cooler reader confirms exact equality between the
reopened BINS query and the actual export, including hidden contigs; its separate
base-endpoint oracle still agrees to 8.9e-16. Evidence:
`/tmp/hict-reimport-immutable/{results,reimport-parity}.json`; the final screenshot
showing both Estimated labels was inspected. This does not qualify uniform
sequence-scale geometry or sub-resolution contig visibility.

One earlier browser run was invalidated by rebuilding its live JAR, causing JVM
class-loading failures. It is excluded from the evidence. The common browser
harness now snapshots the JAR before launch, rejects a source modified during
copying, records its SHA-256 and removes the owned copy after shutdown. The final
run used SHA-256 `b2a6e6b01b04b3597648ac5bef03c50887e1f174bf14c28685970e574abc4b92`
(`server.artifact.sha256`) with no class-loading errors and no leftover owned JAR.

Ordinary floating-point re-export remains explicitly unsupported by both legacy
export engines, as reproduced in `/tmp/hict-reimport-audit/export.log`; it does
not silently round the values. Closing that path remains required, together with
shared-bin expected/histogram accounting, sequence-scale geometry and final
release/publication qualification. The full goal is still active.


## Ordinary floating-point Cooler export (2026-11-16)

Closed the floating-point re-export gate identified above. The ordinary exporter
selects float64 or int64 independently per stored resolution and remaps source
bin identities without applying the approximate endpoint model again. Mixed
pyramids preserve integer counts above 2^53. Source units, estimation provenance
and genuine weights survive; reversing native bins or an AGP component reverses
weights consistently. Known absent filler vectors are omitted. Integer output
now uses int64 instead of narrowing to int32. Integer aggregate/duplicate overflow
is rejected, including by bypassing unchecked native compaction for large counts.
Both writers now distinguish cis totals from interchromosomal contacts.

Ordinary library and job/CLI exports stage output beside the destination, publish
only after success, and remove private files after failure/cancellation. Tests
cancel after axes are emitted and inject a NaN source value; existing destinations
and source bytes remain unchanged. Auto mode with balancing disabled uses the
internal mixed-type writer without requiring hictk. Optional hictk balancing after
this writer is implemented but remains a tool/platform qualification gate.

`./gradlew test shadowJar -x copyWebUI --offline` passed: 560 tests, zero
failures/errors, seven optional skips. Focused tests cover `.cool`/`.mcool`,
both native/AGP reversal, mixed types, large counts, real two-chromosome cis
accounting, overflow and reimport/ordinary re-export provenance. WebUI production
code was unchanged. `git diff --check` passed for the owned increment.

The extended Chromium journey passed all fourteen checks, with no page errors:
explicit approximate export, actual Jobs download, independent base-endpoint
oracle, import/Open result, primary/secondary Estimated labels, save/close/restore,
then ordinary export with approximate rebinning off. Python Cooler compares every
stored pixel/index and dense cell exactly at 4/10 bp. Both retain total 22, float64
counts and estimation provenance, with no invented weights. Current-assembly
chromosome names are checked against saved AGP because singleton scaffold names
can differ from input chromosome names. A separate packaged CLI export in auto
mode, without approximation or balancing, also matches exactly.

Evidence: `/tmp/hict-stored-float-qualified/{results,stored-parity,cli-parity}.json`;
`stored-export-job.png` was visually inspected. Immutable server SHA-256:
`caea851620538947cd5273ea9091ed2ad8701d0f5fa3731108654fe470b08238`
(`server.artifact.sha256`). This qualifies tiny-fixture correctness and Jobs delivery;
it does not establish large-map latency, optional balancing or final package UX.
Shared-bin expected/histogram accounting, sequence-scale geometry and the main
scissors transition remain outstanding P1 dependencies. The goal remains active.


## Shared-bin expected fitting and histogram semantics (2026-11-16)

Exact-cut aliases now share one unit of support per unordered physical source-bin
pair across the full fitting window. This includes repeated source-diagonal cells
and symmetric A/B, B/A orientations with unequal multiplicities in rectangular
views. Source keys include stripe identity. A sparse interval sweep computes
reference counts without allocating arrays indexed by source stripe size. The
same weights feed every tile chunk and scaffold domain. Expected means retain the
existing positive-finite policy at absolute display-bin separations. Native values
are not changed or summed into synthetic observations. See
[the explicit method and limitations](shared-bin-statistics.md).

Primary viewport profiles, raster fallbacks and normalized numeric queries share
the policy. BINS queries fit their own axis, including hidden contigs, and do not
reuse PIXELS caches. Color histograms intentionally remain display distributions;
tile statistics now declare DISPLAY_CELLS, the dialog labels displayed matrix
cells and explains repeated references for exact-cut assemblies. O/E's tooltip
explains shared support. No additional routine dialog or control was introduced.

All 564 JVM tests passed, zero failures/errors, seven optional skips. WebUI build,
histogram/range aggregation and API DTO regression scripts passed. Focused ESLint
exited successfully with no reported lint errors (the existing plugin schema
resolver prints directiveConfigSchema diagnostics). Tests cover reversed/padded
axes, stripe indices near two billion, weighted contact-support conservation,
unequal reverse multiplicities, rectangular windows and per-cell chunk invariance.
The existing expected/scaffold/native-rendering tests also pass.

Chromium exercised two exact cuts, reversal, scaffolding, the Expected/O/E control
and the real selection histogram. Four numerical windows (square, rectangular,
unequal-alias rectangle and BINS) agree with an independent unordered-pair
occurrence enumeration with maximum cell error zero. Cached and uncached primary
fits agree. Tile statistics report 49 display cells for a 7x7 window. Native source
SHA-256 is unchanged, and there are zero page errors. The histogram screenshot was
visually inspected; its displayed-sample explanation is readable.

Evidence: `/tmp/hict-shared-expected-qualified/{results.json,shared-histogram.png,shared-oe.png}`.
Immutable server SHA-256:
`27cb03a8414889983f42ab8622f828d9c16600234effe4daf2b16d58fb23d2ac`.
The checker serves the built WebUI independently of the JAR; this is feature
qualification, not packaged UI or cross-platform release qualification. The
screenshots still show the current bin-grid stretching of short fragments.
Uniform sequence-scale geometry, the main scissors transition and broader
performance/native/package gates remain pending. The full goal remains active.


## Native contact colors on a uniform sequence grid (2026-11-16)

Added an explicitly requested one-base display grid to `/get_tile`, backed by
`SequenceGridProjector`. Native contacts are read and colored at the chosen stored
resolution, then clipped/copied onto retained sequence intervals. This preserves
one-base fragments, source-forward bin origins, reversal, terminal-bin widths
and hidden components. Out-of-assembly bases or sequence without bins remain
transparent. No source data is rewritten, apportioned or interpolated. Responses
distinguish display scale from native resolution and omit misleading native-cell
histogram counts. Bounds and cancellation cover preparation and output copying.
Upper/lower graph branches are resolved per displayed base, including within
native diagonal cells and after Swap. [Contract](sequence-scale-rendering.md).

This also fixes a pre-existing pipeline bound that compared assembly-bin indices
with the physical source-bin count. Exact cuts can increase the former through
shared references; those valid rows/columns must remain colored. All three
pipeline paths now use current assembly BINS length. Numeric BINS dynamics and
track inputs retain their own coordinate axis. BINS track samples include hidden
components and padding, including exact ATU weights and feature projections;
visible 1D-track queries keep their existing path.

`./gradlew test shadowJar -x copyWebUI --offline` passed: 571 tests, zero
failures/errors, seven optional skips. New regressions enumerate every interior
cut of the 17 bp fixture on both strands at 4/10 bp, plus nested cuts, reordering,
weights, hidden/missing bins, padding, positions beyond 2.5 Gb, cancellation and
base-grid triangle assignment. A separate hidden one-base feature/weight test
checks BINS sampling without altering the visible track path. Optional large-file
track formats remain subject to their existing release qualification gates.

Chromium decoded real server PNG tiles for primary/secondary physical fixture
copies at both stored levels, through standard and graph renderers. All eight
cases match native colors exactly (3,452 checked pixels), including transparent
padding and native references beyond the original physical bin count. Four more
triangle/Swap cases verify 1,156 base pixels. JSON grid metadata, absence of native
sample statistics, numeric pipeline bounds and invalid parameter combinations
also pass. Source hashes are unchanged and there are no page errors. Observed
local tiny-fixture requests were 2.6–5.2 ms; these are not large-map latency or
p95 qualification. The diagnostic side-by-side PNG preview was visually inspected
and shows the short terminal/cut intervals at their proper base widths.

Evidence: `/tmp/hict-sequence-grid-qualified/{results.json,sequence-grid-comparison.png}`.
Immutable server SHA-256:
`54084fca05782496981908daa62348448b94638243c892e08782039000bba73f`.
An earlier comparison used pre-reversal source-triangle masks as its oracle and
was rejected. The qualified checker separates full-source color parity from
current base-grid triangle assignment and checks both explicitly.

WebUI production code was unchanged. The checker uses its existing built WebUI
and a diagnostic preview of actual tile images; the main UI transition remains
pending. Current map geometry, rulers, FASTA, selections, exports and edit
coordinates must be coordinated next. P1 and the full release/publication goal
remain active, including all platform, large-map and final soak gates.

## Main-map sequence-scale integration — 2026-11-16

The WebUI now uses the bounded sequence-grid renderer on capable servers. It
preserves a native coordinate snapshot, represents short fragments at their true
base widths and keeps the stored resolution explicit in the header/Info. Scissors
use revision-guarded exact cuts, and native raster extents update when shared
references add bins. Saved viewport and bookmark records carry additive BP/grid
fields, with legacy parsing retained. The overview uses a separate native
projection. Native BINS diagnostics and expected-profile caches remain distinct
from visible PIXELS; expanded base colors are not counted as new observations.

`npm run build:web` passed with type checks and all existing regressions, including
the newly enabled coordinate-snapshot suite, optional server capability parsing
and saved BP/grid validation. The known large-chunk warning remains. JVM
`./gradlew test shadowJar -x copyWebUI --offline` passed: 571 tests, zero
failures/errors, seven optional skips. Logs:
`/tmp/hict-sequence-view-resize-web.log` and
`/tmp/hict-sequence-view-final-jvm.log`.

`scripts/check_sequence_view_browser.py` creates physical disposable 4 kb
Cooler/native, IUPAC FASTA and stranded BED6 fixtures. Chromium operates the real
scissors twice (253 bp, then 1 bp), keyboard undo/redo and Reverse Selection.
Final fragments are 3747/252/1 bp, with native raster axes 42/22 references at
100/200 bp. Every retained base matches the full 4 kb reverse complement. The
three-base feature occupies 60 screen pixels at 20 pixels/base. Native/base
saved centers, real wheel events, overview clicks, O/E controls, viewport
PNG/SVG/PDF, and save/close/open session pass. Eight wheel steps reach visible
letters; each preserves the fixed-client pointer locus within 1e-5 bp. Overview
click tolerance is 1e-3 bp to accommodate browser subpixel rounding. Export and
session restoration retain the BP center, and the source native hash is unchanged.
All 17 workflow checks pass without page errors. The real map, PNG and independently
rasterized PDF were visually inspected. SVG is valid XML with no external image
references; a standalone SVG-document screenshot timed out and is not counted as
standalone-viewer qualification.
The same SVG renders successfully as a Chromium image; that preview was visually
inspected too (`svg-image-preview.png`). Its FASTA text uses a different fallback
font outside the app. Portable SVG typography remains an export qualification
item; this increment does not certify publication-ready typography.

Evidence: `/tmp/hict-sequence-view-browser-19/`, including `results.json`,
`restored-sequence-view.png`, `sequence-view.{png,svg,pdf}` and `pdf-preview.png`.
Server SHA-256:
`64f0d7259f6a42ffa1143f665aa80fcbc30d35f6b77f1230af154274439e9293`.
WebUI distribution SHA-256 (sorted relative file names plus NUL plus file bytes):
`9646c2b00a51e39feef11f92c14fd04ec55367374d900e10faf95ddd480842d0`.
The JVM used an immutable artifact copy; WebUI was served separately. The artifact
name's older WebUI suffix does not qualify an embedded package for this increment.

`scripts/check_shared_expected_browser.py` now fits both units and verifies that
different BINS/PIXELS fitting windows cannot overwrite each other's cached means.
Four independent numerical comparisons and both cache-isolation probes have zero
error. O/E and the base-scale histogram reference labels pass, with unchanged
native bytes and no page errors. Evidence:
`/tmp/hict-shared-expected-bins-browser-03/results.json`, same server hash.

Browser failures during development exposed an active/future-grid lookup mix-up,
BP bookmarks clamped before FASTA-strip layout, and zooms interrupted by generic
gap handling or resized zoom limits. Those paths are corrected and exercised by
the final checker. Test clicks were also moved clear of map controls, and wheel
checks now wait for dispatch before waiting for animation completion.

P1 remains open: full secondary/disjoint-pyramid UI coverage, hidden/no-bin and
quantitative 1D tracks, feature-edge cuts, broader selection/export/bookmark
migration, large-map performance and packaged-platform/soak qualification.
Quantitative tracks still sample native bins before mapping their positions to
the base axis. No production/release/publication-complete claim follows from this
tiny-fixture qualification.

## Feature-edge cuts and feature navigation — 2026-11-16

GTF/GFF feature context menus now offer exact cuts at projected assembly start/end
boundaries. Both axes and both biological strands use zero-based, half-open bounds.
Existing joins/assembly endpoints are disabled. Targets retain their opening
session/revision and are revalidated before a command; busy/read-only/exporting,
disposed or changed assemblies cannot execute a stale target. The shared exact-cut
path preserves the current BP center while updating native reference axes. Failed
requests do not clear selection or apply a response; refresh failures are reported.

Advisor item 9 exposed two actual navigation defects: Center used a helper that
changed resolution, and Zoom calculated native pixel coordinates before entering
the base grid. Center now retains scale; Zoom restores a BP midpoint at measured
letter scale. General BP-range navigation uses the coordinate model too, with no
single-bin-scale cap for large ranges. The old copied-coordinate grouping was
explicit `toLocaleString("en-US")`; copied loci/ranges now use ASCII digits without
grouping. Display rulers keep their existing formatting.

`npm run build:web` passed, including type checks, existing regressions and the new
default `scripts/check-feature-edge-cuts.mjs` checks. Those exercise oriented offsets,
boundary validation, stale session/revision/menu targets, busy/read-only/export/
disposal guards, rejected requests, preserved BP views, Center/Zoom scale semantics
and large-range arithmetic. Existing clipboard/fallback checks pass with ungrouped
loci. JVM `./gradlew test shadowJar -x copyWebUI --offline` passed: 571 tests, zero
failures/errors, seven optional skips. Logs: `/tmp/hict-feature-actions-copy-web.log`
and `/tmp/hict-feature-cuts-jvm.log`. The known large WebUI chunk warning remains.

`scripts/check_feature_edge_cuts_browser.py` creates fresh physical 4 kb contacts,
IUPAC FASTA, GTF and GFF3 inputs. Real horizontal GTF menu clicks cut the 4–6 inclusive
source feature at assembly boundaries 3/6. After undo/reversal, vertical menu clicks
cut its projected boundaries 3994/3997. GFF3 source feature 1–1 is one retained base;
its reversed start creates a one-base terminal contig and its assembly-end cut is
disabled. Final lengths are 3994/3/2/1. A captured target becomes stale after undo
and sends no request. Every cut preserves its BP center exactly in this fixture,
including the first native-scale cut. History, full reverse-complement sequence,
save/close/restore, disabled joins and unchanged native bytes pass.

Actual Center and Zoom actions after a native cut both target BP 1049.5. Center
retains native resolution 0.006; Zoom reaches the visible FASTA letters at
0.000505050505050505 and the same BP midpoint. Real clipboard output is
`tiny_3_4000:1031-1063` (one-based inclusive, no separators). All 15 checks pass
without page errors. Evidence: `/tmp/hict-feature-actions-browser-02/results.json`
and screenshots, visually inspected. The existing scissors/wheel/overview/figure/
session workflow also passes all 17 checks:
`/tmp/hict-feature-actions-sequence-browser/results.json`.

Immutable server SHA-256:
`a42db34371d4cb70d29a3078de8889980593622131a297e08221662d53c60540`.
Separately served WebUI distribution SHA-256 (same sorted-path/NUL/bytes method):
`ae94799078646e2daab7a165234f7dade0cb46ab2251e627bf6756e4e9e4514f`.
Final embedded-package/platform, all annotation formats, secondary/disjoint levels,
all entity-navigation callers and large-map performance are not qualified here.
The feature-zoom screenshot also exposes sidebar control overflow to address in
P4 layout work. P1/P4 and the complete release/publication goal remain active.

## Base-grid 1D features, signals and native weights — 2026-11-16

The old BP track adapter first projected onto visible native bins, which could
omit hidden/binless sequence and stretch quantitative intervals beyond their
retained bases. New servers advertise additive `sequenceScaleTracks=true`.
Explicit `sequenceScale=true`, BP-unit requests build a uniform assembly-base
axis and bypass native precompute sidecars. Responses retain the stored source
level separately from the 1 bp display axis. Existing native queries and BINS
pipeline samples retain their paths; older servers retain the former adapter.

Source features and quantitative intervals project directly through source
offsets/strand at base scale. Cooler weights keep their actual source-bin values,
including hidden references, and report per-track native resolution. A disjoint
secondary source uses its finest stored level if the primary level is absent.
Missing bins and non-finite weights have no support. Coarse max buckets retain
support intervals separately so they cannot paint missing bases. Weight output
is capped at 16,384 intervals; queries cap width at 8192 and span at 1,048,576 BP.
Out-of-assembly queries return empty data. Explicit invalid flags/units and
oversized queries are rejected. Tooltips identify native weights and one-based
inclusive assembly coordinates. Normalization availability now checks the stored
source level rather than the virtual display level.

`SequenceTrackQueryTest` compares every base of a 17 bp independent fixture after
nested cuts, with hidden and binless children, before/after reversal at two stored
levels. BED values, one-base GTF features, indexed BAM coverage and native weights
match source-coordinate oracles. Native NaN masks and binless bases remain missing
even in a single coarse display bucket. Disjoint secondary weights retain their
own stored level/strand, and a feature beyond 2.5 Gb needs no stored 1 bp level.
Feature context, empty ranges, bounds, cancellation and unchanged native bytes pass.
DTO/capability checks verify explicit transport and native sampling metadata.

Full JVM `./gradlew test shadowJar -x copyWebUI --offline` passed: 574 tests, zero
failures/errors, seven optional skips. Full `npm run build:web` passed, including
type checks and existing regressions; the known large-chunk warning remains.
Logs: `/tmp/hict-base-tracks-final-jvm.log` and
`/tmp/hict-base-tracks-final-web.log`.

`SequenceTrackBrowserFixture.java` creates physical sources with one binless BP,
two hidden BP and 4 kb visible sequence. The real browser observes GTF features
on the first two components, BED values `[7,9,9,4,4]`, and native weights
`[missing,20,20,100,100]` at their first five assembly positions. Ordinary native
queries retain their visibility behavior. Both stored levels, base feature
context, source-level normalization availability, tooltip metadata, a real cut
inside hidden sequence, full reversal, session save/restore and invalid request
rejection pass. A coarse weight bucket also excludes the binless base. All 13
checks pass without page errors and the native hash is unchanged. The map/track
screenshots were visually inspected. Evidence:
`/tmp/hict-base-tracks-browser-04/{results.json,hidden-binless-tracks.png,restored-sequence-tracks.png}`.

Immutable server SHA-256:
`5ce5c27cab211e87cb30ae760ddc351198703a620b0166c21152d01f287e593e`.
Separately served WebUI SHA-256 (sorted relative path/NUL/file bytes):
`3f4bd4a18458c2c5abd70cee1baaea6bf607ef783a0baa8e8a2304fd0a87e1ef`.
The established 17-case sequence/export/session and 15-case feature-action browser
workflows also pass after the base-track transport change:
`/tmp/hict-base-tracks-sequence-browser/results.json` and
`/tmp/hict-base-tracks-feature-browser/results.json`. Those two runs used server
SHA-256 `4d769d70674accdf8df29fe7fcee0a735d4a19bce1ac589e8e37b324b3678834`,
before the final coarse-weight support correction; neither loads a weights track.

BigWig/VCF and all signal modes, large-file latency (including contention with
native precompute), the full secondary UI matrix, responsive layout and final
embedded/platform qualification remain open. This increment establishes numerical
and primary browser correctness; it does not certify all tracks or performance.

## Independent selections and exact region entry — 2026-11-16

Shift entity selection now uses a transient DragBox and stable contig/scaffold
IDs, with stroke-only highlights drawn independently from ordinary border style.
Ctrl/Command region selection uses a persistent Extent and authoritative half-open
assembly BP bounds. The two modes clear each other. A plain click outside the
region or Escape clears it; assembly/scope changes invalidate selections. Structural
commands consume entity IDs, and analysis-region context menus exclude those edits.
The Selection menu exposes mode, selection summary, coordinates and scoped border
hiding. Border hiding retains selected outlines and names.

The coordinate dialog accepts independent row/column assembly positions or
current-contig offsets in explicit one-based inclusive/BED conventions. Parsing
rejects unsafe integers, fractions, malformed grouping, ambiguous/unknown contigs,
empty/inverted intervals and scope violations before changing selection. An open
dialog captures assembly session/revision and scope revision. Native/base grid
changes reproject its BP endpoints without native-bin rounding; FASTA export reads
those endpoints directly. Export rendering hides/restores both selection layers.

`check-region-selection.mjs` exercises the production parser and manager methods,
including colon-containing names, >2.5 Gb positions, fractional native projection,
grid changes, entity/region isolation and invalid-input preservation. It runs in
the full WebUI build. `npm run build:web` passes, including types and existing
regressions (`/tmp/hict-selection-final-web.log`); the known chunk warning remains.
JVM `./gradlew test shadowJar -x copyWebUI --offline` passes with 574 tests, zero
failures/errors and seven optional skips (`/tmp/hict-selection-jvm.log`). No JVM
production source changed in this increment.

`check_selection_modes_browser.py` creates a disposable physical 4 kb source,
makes two exact cuts and uses real Shift/Ctrl pointer gestures and menu controls.
Seven scenarios cover entity/scaffold highlights after zoom, transient box removal,
region persistence/edit isolation, scoped border hiding, exact contig-coordinate
entry, invalid/stale input, Escape, scale changes and downloads. FASTA records
match independent source slices, including the default scaffold separator across
the row interval. PNG exports with either entity highlights or a region are
byte-identical to the clean PNG; selections are restored afterward. Evidence:
`/tmp/hict-selection-browser-06/{results.json,coordinate-dialog.png,selected.fasta,clean-export.png,entity-export.png,region-export.png}`.
The native hash is unchanged and no page errors occur. Screenshots of the selection
menu, scoped border selection, coordinate form and restored region were inspected.

The existing sequence-view/export/session (17), feature-cut/navigation/clipboard
(15) and hidden/binless-track/session (13) workflows also pass without page errors:
`/tmp/hict-selection-{view,feature,tracks}-browser-01/results.json`. These runs
precede final label pluralization and unused-import cleanup; the seven selection
scenarios are repeated on that final distribution. Immutable server SHA-256:
`ebaaf550d99e0a5eb6881975d50c30c5677fb9cc6f1a2a4a306f80f538e7a7c1`.
Final separately served WebUI SHA-256 (sorted relative path/NUL/file bytes):
`045d85df832f2156cbaec06b48482765e779329b805d1379991af1b820e8f898`.

FASTA-track repeat measurement, real macOS/native modifier behavior, secondary
source interactions, large-map selection performance, whole-map export and final
embedded/platform qualification remain pending. This does not complete P2 or
the release gate. Plan files and protected initial changes remain untouched.

## FASTA interval selection and repeat measurement — 2026-11-16

FASTA strips now support Ctrl/Command-drag selection, a context-menu plain-drag
selection mode, one-letter intervals and Shift-arrow extension within visible
bases. Plain drag otherwise keeps its pan behavior. A shared card gives exact
length/assembly coordinates and copies the frozen selected strand's sequence.
The reverse row copies reverse-complement IUPAC sequence in reading direction.
The highlight is a separate presentation element, excluded from figure captures.
Escape cancels a partial gesture; edits, scope changes and FASTA changes clear
the selection. Panning preserves it, and sessions do not serialize it.

Explicit Estimate period submits a bounded immutable gapless window to the shared
sequence worker through `/sequence/repeat_period`. History headers guard capture;
source stamps, assembly fingerprints and interval SHA-256 identify the frozen input. Settings
advertise support, DTO transport preserves long positions, and status/cancellation
use existing Jobs infrastructure. Job summaries identify the captured interval.
Late submissions/results cannot reopen or overwrite a cancelled/closed card.
`ungapped-autocorrelation-v1` reports exact periodicity or a thresholded imperfect
candidate, excluding ambiguous symbols and composition-biased similarity. The
UI and [method document](repeat-measurement.md) explain thresholds, overlapping
pair dependence and the lack of indel modeling/significance testing.

Library tests compare exact motifs with an independent shift oracle, including
the advisor's CT repeat pair, partial copies, a point substitution, homopolymers,
ambiguity, composition bias, lowercase, size bounds and cancellation. DTO tests
cover invalid inputs and >2.5 Gb offsets. The real HTTP test occupies the actual
sequence worker, cancels queued jobs, fills/rejects the queue, modifies/reverses
the live assembly and verifies frozen analysis/provenance. It also checks stale
headers, missing/out-of-range requests, Jobs summaries, capability and unchanged
native bytes. Full JVM checks pass with 581 tests, zero failures/errors and seven
optional skips (`/tmp/hict-repeat-provenance-jvm.log`, full test/shadowJar).
The queue saturation/cancellation checks also pass in
`/tmp/hict-repeat-qualified-jvm.log`. Full WebUI build/regressions
and type checks pass (`/tmp/hict-repeat-final-web.log`,
`/tmp/hict-repeat-provenance-types.log`); the known chunk warning remains.

The final Chromium measurement script passes 12 scenarios:
`/tmp/hict-repeat-final-browser/results.json`. Real horizontal/vertical gestures,
both strands, clipboard bytes, length/endpoints, exact/imperfect repeat jobs,
ambiguity, Jobs summaries, input checksums, Escape mid-drag, delayed-response ownership, exact
cross-bin cutting, reversal, pan preservation, compact card layout, clean exports
and actual save/close/reopen all pass. The known one-substitution interval reports
period 2 and 8/10 informative matches; all-N input reports no supported period.
Selection-active and clean PNG bytes match exactly. Native source bytes remain
unchanged and there are no page errors. Screenshots of the card, both strands,
Jobs, and 1024×768 layout were visually inspected.

Existing browser workflows also pass on the same runtime artifacts: independent
selections (7), main sequence view/exports/sessions (17), hidden/binless tracks
(13), and feature cuts/navigation/clipboard (15). Together with the 12 measurement
scenarios, all 64 checks pass without page errors. Evidence:
`/tmp/hict-repeat-final-{selection,view,tracks,feature}-browser/results.json`.
Immutable server SHA-256:
`916db831c7bdcd1f69e66c7bc29d56d664cfc087368c3d81f30dc0de6326e735`.
Separately served WebUI SHA-256 (sorted relative path/NUL/file bytes):
`97d5786272388c617d3f6318a2e5d37d16454c79e9b8f066cc4604bf83c3b1cc`.

The card fits the inspected compact viewport; existing surrounding toolbar/sidebar
overflow remains part of the pending responsive-layout work. Real macOS/native
modifier behavior, the secondary-source matrix, accessibility, large-map/queue
latency and final embedded/platform qualification remain pending. No broad repeat
annotation or publication-release claim is made. Plan files and protected initial
changes remain untouched.

## Continuous alignment preview and short repeats — 2026-11-16

The preview replaces per-base icon points with continuous run polylines through
each aligned base boundary. This spans the full letter intersection, preserves
reverse slope and native fragment kinks, and distinguishes substitutions,
ambiguity and indels with shared colors/dashes. Rendering is bounded to 4096
runs and 65,536 vertices; malformed/oversized data fails atomically. The map and
PNG/SVG/PDF use the same geometry; vector exports retain paths rather than icon
images. The legend explains the visible local comparison without extra controls.

The backend already processed all PAF records. A disposable native-tool probe
showed that the original 7-BP seeds/chain thresholds returned zero hits for
`CTCTCTCTCTCT × CTCTCTCTCT`. Tiny windows (both ≤64 BP) now use
`short-window-v1` (k2/w1/m2/n1/s2); larger windows retain
`seeded-window-v1` (k7/w3/m12/n2/s12). All reported extended-CIGAR alignments
are retained, within the existing 256-record/4096-segment bounds. Canonical
`=`/`X` operations are checked against the actual frozen source bases, including
reverse complementation. This exposed and corrected a contradictory base in an
older synthetic reverse-strand test. Ambiguous positions remain ambiguous.
Responses add applied profile, method, interval/snapshot and input/tool checksums.
The heuristic/local-comparison limits are documented in
[alignment-preview.md](alignment-preview.md).

Navigation cancels known jobs immediately. The initial submission is allowed to
return its job ID, with a bounded 15-second transport timeout, so a late obsolete
submission can be cancelled. Polling remains abortable; stale responses cannot
redraw disabled/disposed/changed views. Grid metadata participates in the client
request key. Controller regressions verify cancellation of both known and late
submissions; transport tests verify its timeout and displayed history guard.

Full JVM `test shadowJar -x copyWebUI --offline` passes with 583 tests, zero
failures/errors and seven optional skips (`/tmp/hict-alignment-lines-final-jvm.log`).
Full WebUI build/types/regressions pass
(`/tmp/hict-alignment-lines-qualified-web-02.log`); the known large-chunk warning
remains. Geometry tests cover all operations/strands, one-base substitutions,
nonuniform mappings, >2.5 Gb positions and invalid/bounded input. Profile tests
cover the 64/65-BP boundary. Source checks reject inconsistent canonical CIGARs.

`check_alignment_runs_browser.py` passes 12 real Chromium scenarios on fresh
physical HDF5/FASTA data: all ten CT diagonals checked against an independent
maximal-diagonal oracle, a substitution, short and larger-window indels, reverse
complements, ambiguity/no-hit, exact vertices, requested further letter zoom,
PNG/SVG/PDF, disable/resume, precise shared-bin fragments, reversal, undo/redo and
scoped session restoration. Input checksums match independent source slices.
Evidence: `/tmp/hict-alignment-qualified-browser/results.json`. Native bytes are
unchanged and there are no page errors. The current map/figure screenshots were
visually inspected. Standalone SVG and PDF were rendered independently; orange
substitution bounds in PNG/PDF agree within one pixel in the inspected figure:
`/tmp/hict-alignment-lines-browser-02/{alignment-lines.png,alignment-lines-svg.png,alignment-lines-pdf.png,export-color-bounds.json}`.
The later UI-only changes add the request timeout and simplify the legend text;
the exported line geometry remains the same.

The existing measurement (12), sequence-view/export/session (17), independent
selection (7), hidden/binless tracks (13) and feature-action (15) browser suites
also pass on the qualified runtime, for 76 total checks without page errors.
Evidence: `/tmp/hict-alignment-qualified-{repeat,view,selection,tracks,feature}-browser/results.json`.
Immutable server SHA-256:
`53f12b3e5282712bbb5b957a36ed6244e84344eb056578612f0b1dda591d9f70`.
Separately served WebUI SHA-256 (sorted relative path/NUL/file bytes):
`6a8ab67db4be78a7464e0a68bc8911ae6965321b740b822a7b244e1b99a64d0f`.

Short-seed matches are exploratory sequence comparisons, not significance/homology
calls or an exhaustive alignment enumeration. Broader secondary/source/platform,
accessibility, dense-window latency and final embedded-package qualification
remain open. Plan files and protected initial changes remain untouched.

## File destinations and frozen downloads — 2026-11-16

The shared WebUI save service requests a browser destination synchronously from
the click, separates confirmed/cancelled/download-started outcomes and propagates
failed writes. Picker denial does not trigger an unwanted download. Browser
handles remember successful destinations per purpose. Electron/Tauri persist
separate purpose directories only after staged write/sync/close/rename succeeds.
Sessions mark only the captured revision saved after confirmation; later edits
stay dirty. Cancellation/failure/download fallback cannot release the close
guard. Errors within that guard render inline. File → Open Examples and validated
session reopening are available; input selectors retain server/purpose folder
and file choices and require an explicit Load click.

New real HTTP tests first exposed that export-priority dispatch bypassed the
assembly history interceptor. FASTA/AGP now perform short revision-checked capture
on the assembly queue, then materialize immutable records/layout on the export
pool. Cancellation is 409 with retry guidance, never a successful empty file.
Frozen-layout tests export the original interval after real exact cut/reversal
and verify unchanged source bytes. Legacy clients without history headers remain
supported. External FASTA mutation during a download is not newly qualified.

Full JVM test/shadowJar checks pass with 585 tests, zero failures/errors and seven
optional skips (`/tmp/hict-file-save-qualified-jvm.log`). Full WebUI checks pass
(`/tmp/hict-file-save-final-web-02.log`), retaining the known large-chunk warning.
Electron compilation passes (`/tmp/hict-file-save-electron-types.log`). Actual
Electron IPC real-disk tests include typed-array offsets, separate remembered
directories, cancellation, injected rename failure, old-file preservation and
temporary-file cleanup (`/tmp/hict-file-save-native-destination.log`). The actual
Tauri filesystem function is extracted, compiled and executed unchanged by
`scripts/check-tauri-export.mjs` (`/tmp/hict-file-save-tauri-function.log`). Full
Tauri compilation fails before source compilation: installed Rust 1.75 cannot
parse Cargo lockfile v4; the project requires 1.88
(`/tmp/hict-file-save-tauri.log`). This is an environment qualification limit,
not a passing native-wrapper build.

Ninety Chromium scenarios pass, with fresh physical disposable fixtures:

- File workflow (14): `/tmp/hict-file-save-final-browser/results.json`.
- Alignment (12): `/tmp/hict-file-save-alignment-browser/results.json`.
- Feature actions (15): `/tmp/hict-file-save-feature-browser/results.json`.
- Hidden/binless tracks (13): `/tmp/hict-file-save-tracks-browser/results.json`.
- Sequence view/exports/session (17): `/tmp/hict-file-save-final-sequence_view-browser/results.json`.
- Independent selections (7): `/tmp/hict-file-save-final-selection_modes-browser/results.json`.
- FASTA measurement (12): `/tmp/hict-file-save-final-fasta_measurement-browser/results.json`.

The new workflow checks cover real session JSON/restore, cancellation, denied
picker, write-close failure, inline guard failure, readback/open action, newer
edits during close, assembly and exact-region FASTA bytes, AGP, stale picker-delayed
selection rejected with 409, three figure formats, download-only dirty guards,
and distinct server map/FASTA folder/file memory. Native input bytes are unchanged.
The expected stale-selection HTTP 409 console message is recorded explicitly;
there are no unexpected page errors. Failure/fallback/remembered-file screenshots
were visually inspected. Save-picker outcomes are controlled fixtures, not
native dialogs. Other suites now use a controlled writable handle that emits real
downloads; their output checks remain active. This intentionally models confirmed
writes separately from the dedicated unconfirmed-browser-download scenarios.

Immutable server SHA-256:
`d325099dffc264de6f8d060e6e6f09a21bdf670fb19103b529c01984f69fb572`.
Separately served WebUI SHA-256 (sorted relative path/NUL/file bytes):
`e291ad29fcbc438052a7d8c2be07058bc28972cca803a0d65febda164e9ac9e9`.
The API documentation update after these runs describes the tested guards; it
does not change the qualified Java implementation. Final embedded/package,
native-dialog/platform, streaming/large-memory, all chooser routes, secondary
sources, accessibility, restart and large-directory gates remain open. Plan files
and protected initial changes remain untouched. See [file-destinations.md](file-destinations.md).

## Browser streaming FASTA/AGP — 2026-11-16

FASTA/AGP exports use fetch streaming when the selected destination supports a
writable byte stream. Each disk write completes before the next network chunk is
pulled; this path creates no assembly-sized ArrayBuffer or Blob. HTTP errors and
unexpected response types cancel without opening a writer. Read/write/close
failures cancel the source and abort the writer; destination memory advances only
after confirmed close. The progress notice cancels preparation/transfer, then
reports one journal outcome. The existing desktop/download-only path is explicit
and remains buffered; large-file qualification for those adapters is pending.

Focused tests transfer 128 MiB in 64 KiB chunks while asserting at most two chunks
of source pull-ahead. They cover reader/open/write/close failure, blocked-read
cancellation, released locks, unchanged destination memory, captured revision,
cancelled legacy transport, and one notification outcome. Adapter tests verify
raw streaming DTO/headers/signal and discard HTTP/unexpected-content responses
without buffering. The UI initially exposed a rendering loop from reused toast
IDs/manual dismissal and replacement in the installed vue-sonner version. The
final implementation uses one promise-managed notice, with no manual replacement;
focused tests reject those unsafe operations and real UI workflows pass.

Full WebUI build/types/regressions pass (`/tmp/hict-stream-web-03.log`), retaining
the known large-chunk warning. JVM test/shadowJar checks pass with 585 tests,
zero failures/errors and seven optional skips (`/tmp/hict-stream-jvm.log`).
Java production code is unchanged in this increment; the fixture now supports
bounded larger inputs while retaining its default 4 kb behavior.

`check_streaming_exports_browser.py` passes six checks using a real persistent
Chromium private-filesystem writer: 512 Mb of independent deterministic IUPAC
sequence exports to 518,400,010 FASTA bytes in 4,530 writes. Maximum observed
chunk is 2 MiB. The chooser is substituted; the writer and direct HTTP transfer
are real. The output is downloaded from the file handle and checked with an
independent sequence hash. Cancelled and injected-failure transfers abort the
real writer; independent readback hashes prove that each retained the previously
saved output. Both source files remain unchanged. Saved/error notices were
visually inspected. Evidence: `/tmp/hict-stream-browser-09/results.json` and its
PNG/FASTA artifacts. This qualifies the transfer/writer integration, not an
OS-native destination dialog. Failure injection occurs before a real write; it
does not simulate every filesystem failure mode.

The final run samples 247 animation frames and 58 trusted browser pointer-move
events injected through CDP
throughout preparation/transfer: frame p95 16.7 ms, DOM event acknowledgement p95
13.2 ms. No page errors occur. Linux Ryzen 9 7900X (24 logical CPUs), 64 GB host
memory, Playwright Chromium headless 131.0.6778.33, persistent local profile,
1600×1100 viewport. This is a sparse synthetic-map transfer workload; dense maps,
all tracks, desktop OS input dispatch, and the release soak remain separate gates.
Natural sampled peaks, without periodic GC during the transfer:

| Metric | Baseline bytes | Peak bytes | Increase bytes |
|---|---:|---:|---:|
| JavaScript heap | 7,581,048 | 8,685,940 | 1,104,892 |
| Renderer RSS | 173,563,904 | 330,190,848 | 156,626,944 |
| All owned Chromium processes RSS | 544,477,184 | 716,996,608 | 172,519,424 |

The JavaScript metric excludes native buffers; total process RSS is therefore
reported separately. Earlier upload-backed and incognito-OPFS measurements were
not accepted as production memory evidence: artificial uploads allocate extra
buffers, and incognito private files reside in memory. The final fixture uses
persistent disk-backed storage and direct fetch without Playwright interception.
A prior isolated persistent run also passes (`/tmp/hict-stream-browser-08`), with
16.7 ms frame p95 and 12.2 ms input p95. These two runs are not an exhaustive
scaling or hardware benchmark.

The 14 existing file workflows pass:
`/tmp/hict-stream-file-browser-03/results.json`. The existing sequence view (17),
independent selection (7), FASTA measurement (12) and alignment (12) workflows
also pass on the same final build:
`/tmp/hict-stream-final-{sequence_view,selection_modes,fasta_measurement,alignment_runs}-browser/results.json`.
The hidden/binless-track (13) and feature-edge (15) workflows also pass:
`/tmp/hict-stream-final-{sequence_tracks,feature_edges}-browser/results.json`.
That is all 90 existing browser scenarios plus the six large-transfer checks on
the final build, with no unexpected page errors.

Immutable server SHA-256:
`4226a8a70c429756c552e9906cf9f23e6ec047597f3e9959dce316d86f20a7ba`.
Separately served WebUI SHA-256 (sorted relative path/NUL/file bytes):
`e647bd1fc4b1f1efdfde04b5bc79cc9530445d474e2abd4245c78e88b701bdec`.
The server artifact contains the preceding embedded WebUI; these runs explicitly
serve the new WebUI separately. Final embedded/platform qualification is pending.
Plans, manuscript and protected initial changes remain untouched. P3 and the
full production/publication release remain incomplete.

## Electron native streaming and graceful quit — 2026-11-16

The current Electron adapter sends only a guarded coordinate-export request through
IPC: URL, JSON options, captured journal identity and operation ID. The main
process validates trusted-local URLs/read endpoints, rejects redirects and
unexpected content, opens the native destination chooser, and sequentially writes
fetch chunks to a uniquely owned sibling file. It synchronizes/closes before
rename and remembers a destination only after that commit. Native file bytes
never cross renderer IPC. Legacy/Tauri/download adapters remain compatible and
buffered; their genome-scale memory qualification is still pending.

One transfer belongs to each window. Matching operation IDs permit cancellation;
other windows/tokens cannot cancel it. Navigation, destruction and renderer loss
abort owned work. Graceful quit waits for transfer cleanup before retrying quit;
unanswered choosers own no file and cannot start I/O after cancellation. Window
IDs are captured before destruction. Streamed results advertise no browser file
readback; Blob-backed sessions keep that capability. Permission/quota failures
have actionable messages, and preload errors omit IPC internals.

Full WebUI checks pass (`/tmp/hict-native-stream-web-final.log`), including the real
HTTP/filesystem service tests. Focused checks after the final native-only changes
also pass: `/tmp/hict-native-stream-api-final.log`,
`/tmp/hict-native-stream-unit-last.log`,
`/tmp/hict-native-stream-legacy-last.log` and
`/tmp/hict-native-stream-electron-qualified.log`. Tests transfer and independently hash
128 MiB, validate frozen headers, reject stale/HTML/truncated/redirected responses,
exercise cancellation before/after chooser completion, ownership, overlapping
operations, failed replacement, shutdown during I/O and a pending chooser, and
verify previous files/preferences and temporary-file cleanup. Native bridge tests
assert request-only payloads, cancellation IDs, stream bypass and readable errors.
JVM test/shadowJar checks pass with 585 tests, zero failures/errors and seven
optional skips (`/tmp/hict-native-stream-jvm.log`). Java production code is unchanged.

`check_native_coordinate_exports.py` passes nine checks using real Electron
41.10.7 preload/IPC, the built Linux payload and actual GTK save/cancel dialogs on
an owned Xvfb display. It verifies 512 Mb FASTA (518,400,010 output bytes), exact
selected FASTA, AGP, chooser cancellation without a fetch, in-progress cancellation,
a real filesystem permission failure, session cancellation/confirmed v4 save and
readback restore, navigation cancellation and last-window close during staged I/O.
The latter asserts exit code zero and proves that graceful quit leaves no owned staging file. Original
FASTA/HDF5 input hashes remain unchanged; no page errors occur. Native dialogs and
saved/error notices were visually inspected. Evidence:
`/tmp/hict-native-coordinate-08/results.json`, `native-dialog.png`, `saved.png`,
`failed.png`, native exports and `chosen/session.json`.

A test-only forwarding proxy delivers actual JVM responses, throttles transfer
for cancellation, and gates a response so directory permission can change after
the chooser. It preserves journal and cancellation response headers. Earlier
proxy omissions blocked session capture and generated unsafe-header warnings;
those runs were not accepted as final evidence. No production CORS policy changed.
The final run uses localhost to reach the IPv4-bound proxy, exercising the native
fetch resolver. Renderer observation records zero coordinate-download requests.
The exported sequence hash matches an independently generated IUPAC reference.
Main-process RSS grows from 202,498,048 to 303,726,592 bytes while transferring
518,400,010 bytes; this is sampled natural RSS on the same Ryzen 9 7900X/64 GB Linux
host, not a dense-map latency or universal scaling benchmark. The driver uses a
private X display, disables GPU and uses the payload's Linux no-sandbox flag.
It verifies isolated preload/no renderer Node globals, not OS sandbox qualification.

All 90 existing browser scenarios also pass on the final WebUI assets:
`/tmp/hict-native-qualified-{sequence_view,selection_modes,fasta_measurement,alignment_runs,sequence_tracks,feature_edge_cuts,file_destinations}-browser/results.json`.
They retain their documented controlled writable-picker fixtures. The final browser build includes the HTTP-compatible random operation IDs.
Native main/preload lifecycle and error reporting use the same compiled tree
qualified by the payload acceptance. This adds nine genuine native-dialog workflows.

Immutable server SHA-256:
`404e9a29fe6e6bcbbdcee6bafab644539c49aa4d4c724a0a00eb209872260ab0`.
WebUI tree SHA-256 (sorted relative paths/NUL/bytes, excluding `dist/electron`):
`32f370a33022c1df640e3faec0d96030ade579b6b16173458cbeb9d925f4fae4`.
Compiled Electron tree SHA-256 (same method, rooted at `dist/electron`):
`b976c790c8accce73a9f282dadadb3d2e758e020fcb1f898920ef6d55a9ea809`.
Built payload: `/tmp/hict-native-stream-payload-04`; its manifest tree SHA-256 is
`bbf7198733a664d9ff9b0c1f900718beabab5f3dc62c5ec5d147f11acb9a6e14`.
The packaged main/preload/module match the compiled files; the existing builder
copies the complete main directory. The online UI is served separately from the
matching current WebUI assets. The JAR still embeds the preceding WebUI, so this
is not final combined-SFX qualification.

Tauri's Rust 1.88/WebKit SDK setup and streaming, download-only browser streaming,
other advertised platforms, portals/Wayland, multi-cycle and dense-map performance,
hard-crash/legacy-Blob shutdown, accessibility and final combined packages remain
open. Static inspection also found that the packaged Electron fallback path needs
separate offline-start qualification/fix (`fallbackFileUrl` assumes the development
layout). P3 and the full publication release are not complete. Protected files,
plans and manuscript remain untouched.


## Packaged Electron offline recovery (2026-11-16)

The packaged wrapper now serves its bundled public UI through a lazy owned
127.0.0.1 HTTP listener when the configured page is unavailable. Both development
and portable directory layouts resolve correctly, including paths with spaces.
The payload includes root icons/notices as well as hashed assets. GET/HEAD public
resources receive the existing CSP and no-store headers; HEAD releases its unread
file body; canonical containment,
blocked main/preload files and an exact Host check restrict serving. There is no
API proxy. The server owns its listener before navigation, remembers a usable
port for the profile, falls back to an owned ephemeral port on collision, and
closes connections on quit. A collision or unwritable profile can change its
storage origin; migration across such origins remains a qualification limit.

Hosted navigation revalidates its entry response. Offline/HTTP-error loads return
to a complete interface with a compact Reconnect notice. Concurrent retries share
one operation; online pages cannot request recovery navigation. The existing
session save/discard/cancel guard protects reconnection. Approved departure skips
only the duplicate unload prompt for that exact history identity. Window loss,
cancelled navigation and missing resources have focused lifecycle checks.

Real runtime evidence changed the implementation: file-loaded ES modules fail in
the packaged runtime with GrantFileProtocolExtraPrivileges disabled. A trial
custom scheme rendered but the unchanged JVM CorsHandler rejected its origin
with HTTP 403. Direct origin probes confirmed ordinary HTTP is accepted. Neither
fuses, webSecurity, isolation, sandbox settings nor JVM CORS/binding were relaxed.
A subsequent offline restart revealed Chromium reusing the hosted page from
cache; entry-response revalidation fixes that observed case. Initial acceptance
scripts also needed the existing alertdialog role and a window wait after async
storage reset. Those failed runs are excluded from final acceptance.

Checks: npm run build:web (including recovery/resource/port regressions) and
npm run electron:compile pass. The existing bundle-size warning remains. The full
JVM run in /tmp/hict-recovery-jvm.log passes: 585 tests, zero failures/errors,
seven optional skips. Java production code is unchanged in this increment.

Final packaged acceptance: /tmp/hict-recovery-ui-10/results.json has eight passing
workflows: unassisted offline bootstrap with native preload/icons, refused retry,
HTTP 503 without a loop, real API attachment/edit/cancel, approved departure
without a duplicate dialog, restored hosted UI with recovery IPC denied there,
offline restart after an online visit with retained preferences, and explicit
reset with a 1024 px recovery notice. Page errors and unexpected dialogs are empty;
the immutable HDF5 input hash is unchanged. Screenshots offline-start.png,
reconnect-edits.png, reconnected.png and offline-1024.png were visually inspected.
This qualifies the notice and edit prompt, not every existing compact-screen
layout. The driver is scripts/check_electron_recovery.py, using a private X display
and isolated profile on the same documented Linux host.

Native transfer regression: /tmp/hict-recovery-native-03/results.json passes all
nine real GTK workflows, including 518,400,010 FASTA bytes, exact selection/AGP,
cancellation/failure, session readback and last-window cleanup/exit 0. Independent
FASTA SHA-256 is 6a6897c500a89f7b643ff6ae37af34616e89e2d10205d4350cf56d27794cfb1c.
Sampled main RSS is 197,595,136 to 297,312,256 bytes; no coordinate requests occur
in the renderer. This remains a synthetic sparse-map transfer measurement.

Final browser evidence: /tmp/hict-recovery-final-{sequence_view,selection_modes,
fasta_measurement,alignment_runs,sequence_tracks,feature_edge_cuts,
file_destinations}-browser/results.json. All 90 scenarios pass against the final
WebUI assets with their documented controlled picker fixtures.

Immutable test JAR SHA-256:
c9b83af70c95730e400787841978c926f2e311172be7cd01ac9f9df77c9595c0.
WebUI tree SHA-256 (sorted relative paths/NUL/bytes, excluding dist/electron):
db2e2aac985331611186c92e958e7ce58c009e629be46e4ff8c7ec4080e71ba8.
Compiled Electron tree SHA-256 (same method rooted at dist/electron):
dbfbd053fb55cd22fad3bac1b584725839653a499ccfe873e04db4bfb6099357.
Payload /tmp/hict recovery payload 05, Electron 41.10.7, manifest tree SHA-256:
6ba3bd6711a757718073f65faed02efb27245f59c19e85bb3a6242c7cef8fb33.
The JAR embeds the preceding WebUI; these tests serve the final matching WebUI
separately or from the copied payload, not from the JAR. Final combined-SFX,
other platforms, unresponsive-server timing, Wayland/portals, accessibility,
origin-conflict preference migration and the full publication gates remain open.
Plans, protected files and manuscript remain untouched.


## Separated tracks/sources and live map weights (2026-11-16)

View → Tracks and layers now has keyboard-accessible 1D tracks and Sources and
layers tabs. Add offers a file or map weights from either source. Track cards
retain visibility/axis/color/name/order/removal controls; signal settings,
background appearance and preparation are disclosures. The layer action opens
and focuses the existing sidebar. File versus square-root coverage weights and
known unavailability have explicit hints; unknown legacy provenance stays usable.
See [the user guide](tracks-and-sources.md).

CommittedTrackInput preserves focused name/range/log-base drafts across server
refreshes, then reconciles the accepted value. LinearTrackManager rejects stale
list responses and responses arriving after disposal. A real delayed backend
snapshot verifies both draft retention and that an older list cannot revert a
newer edit. Preparation polls only while its disclosure is open and stops on
close/unmount, including when the initial request finishes late.

Resize completion refreshes map/rulers/tracks after Vue commits the grid. Track
frames use an inset outline, removing the two-pixel CSS scaling caused by borders.
Mouse, keyboard, lost-window-focus cleanup and workspace shrink preserve bounded
panel sizes. Workspace clamping runs in a coalesced animation frame, outside
resize-observer delivery; its accessible maximum follows the workspace. At three panel sizes, bitmap dimensions equal their CSS surface and
map-axis dimensions; dark corner/background and edge pixels contain no white
strip. A later visual review caught a ResizeObserver-loop toast missed by console/page
error collection. A capture-phase window-error probe now rejects this warning.
The original one-size baseline did not reproduce the advisor's stripe;
the final checks establish these tested geometries, not every display/driver.

Normalization acceptance uncovered a separate scientific defect: native weight
tracks could reuse precomputed values from the preceding normalization mode.
The focused TrackWeightsNormalizationTest failed before the fix with five file
weights instead of the four supported coverage weights. Map weights now bypass
precompute/cache loading and read the resident active vectors. Their preparation
control explains this and is disabled when only weights are loaded. The regression
uses independent row sums [4,10,7,0,11], symmetric total 32 and the full-map
preserving scale, with different stored vectors for primary and secondary. It
checks file → coverage → file, manual precompute, missing zero-coverage support,
both sources and identical input bytes. Existing sequence-grid weight oracles
continue to pass. Dense-map performance remains a release gate.

Full ./gradlew test shadowJar -x copyWebUI --offline passes: 586 tests, zero
failures/errors, seven optional skips (/tmp/hict-tracks-qualified-jvm.log).
The focused before/after logs are /tmp/hict-tracks-weights-{before,after}.log.
npm run build:web passes (/tmp/hict-tracks-final-observer-web.log), retaining the known
large-chunk warning. Python acceptance drivers compile successfully.

Final Chromium evidence: /tmp/hict-tracks-ui-12/results.json has 13 manager,
source, keyboard, resize, delayed-response, normalization and capability cases.
Its controls/results.json adds six numeric/import/background/preparation/error
cases, including [2,3,4,5,6] file weights and an independently checked BED feature.
Source HDF5 hashes are unchanged; normal page-error and capture-phase window-error lists are empty. Visual review
covers file import, both tabs at 1440/1024/800 px, dark resized panels, a shrunk
800 px workspace, and coverage-source labels/map. This is a desktop Chromium
DPR-1 qualification on the same declared Linux host, using disposable fixtures.

/tmp/hict-tracks-source-regressions-06/{secondary-primary,secondary-owner,
failure-reporting,source-capabilities}/results.json adds 5+5+6+5 cases: exact
edited replacement/cancellation under both assembly authorities, one useful
message per injected failure, and stored-resolution/source capability checks.
The failure fixture now uses the same controlled writable chooser as the other
browser checks. Capability checks use the native source descriptor rather than
the virtual 1 bp display descriptor. --legacy-file preserves the existing real-map
default; this run supplies a small physical clone with provenance attributes
removed. A modern file explicitly marked ABSENT is not an unknown legacy file.
Intentional HTTP/console failures are expected only in the failure tests.

All 90 established scenarios also pass on this exact JVM/WebUI pair:
/tmp/hict-tracks-verified-{sequence_view,selection_modes,fasta_measurement,
alignment_runs,sequence_tracks,feature_edge_cuts,file_destinations}-browser/results.json.
Together these are 130 passing browser cases; their documented picker/probe
substitutions remain in force. Development runs with incomplete fixtures,
incorrect pixel expectations or timing assumptions are not final evidence.

Test JAR hict_server-2.0.28-5e7edd4-webui_a1839b7-fat.jar SHA-256:
dff6d37370a077b375d071cc7779d25ed7728db127e3e83018e50b3bc444e090.
WebUI tree SHA-256 (sorted relative paths/NUL/bytes, excluding dist/electron):
8ae741465780626ea725b34adfb2ee8987e5f579ec3b68a62411332aba18acb3.
The JAR embeds the preceding WebUI; acceptance serves the matching new assets
separately. Final combined-SFX, real large-file/format and platform matrices,
retina/touch/accessibility, narrow main-workspace layout, navigation latency and
mixed soak remain open. At 800 px after the sidebar was explicitly opened, its
controls remain cropped;
that main-workspace responsiveness issue is a follow-up. The manager itself fits
that width. P4 and the publication release remain in progress. Protected
files/plans are untouched.

## Responsive navigation and sidebar — 2026-11-16

Resolves the main-workspace clipping follow-up in the preceding tracks record.
With two long map names and linked FASTA, the old navigation had a 1307 px
scroll width at 800/900/1024 px. Fixed-width signal selectors stretched the
sidebar's implicit grid column past its right edge. At 640 px the stacked
sidebar shrank to 15 px high. Navigation now wraps metadata, preserves full
filenames in titles, keeps Jobs available, and positions/scrolls dropdowns within
the viewport. This includes the separately rendered Selection dropdown. Signal
controls shrink or wrap; the narrow stacked sidebar receives its content height
and a vertical collapse indicator. Explicit saved panel choices remain intact.
Dev, the wizard, OSD and backend bind/CORS behavior are preserved.

`npm run build:web` passes on the final sources:
`/tmp/hict-responsive-web-03.log`. The existing large-chunk warning remains.
There are no Java runtime changes; the unchanged backend's full 586-test result
(zero failures/errors, seven optional skips) is recorded in the preceding section.
The new Python acceptance driver was formatted with an identical AST and compiles.

Final Chromium evidence, on disposable data at DPR 1 on the same Linux host:

- `/tmp/hict-responsive-ui-06/results.json`: 15 scenarios. Six viewport sizes,
  1440x1000, 1280x900, 1024x768, 900x700, 800x600 and 640x800, have no horizontal
  root overflow or out-of-bounds sidebar controls. All seven menus fit and their
  last enabled item can be reached; Escape closes each. Primary, secondary,
  FASTA and annotation data are present. Actual range inputs/Apply change sampled
  matrix pixels while the independent 25-cell raw-contact oracle stays identical.
  Automatic alignment is disabled through its View control for this pixel test
  so alignment lines cannot mask the matrix color. Explicit panel show/hide
  survives viewport changes/reload; a fresh narrow context starts collapsed and
  can open its empty panel. Source hashes are unchanged. Page and capture-phase
  window errors are empty across reloads. Screenshots reviewed include the
  800 px workspace, both 640 px Selection/Dev menus and the empty narrow view.
- `/tmp/hict-responsive-tracks-02/results.json` and `controls/results.json`:
  13 manager/source/resize/live-weight cases plus six track-control scenarios.
- `/tmp/hict-responsive-source-regressions-02/{secondary-primary,secondary-owner,
  failure-reporting,source-capabilities}/results.json`: 5+5+6+5 scenarios. The
  capability run uses the disposable legacy-provenance clone described above.
- `/tmp/hict-responsive-{sequence_view,selection_modes,fasta_measurement,
  alignment_runs,sequence_tracks,feature_edge_cuts,file_destinations}-02/results.json`:
  all 90 established sequence, selection, alignment, exact-cut and save scenarios.

Together these are 145 passing scenarios. The established controlled-chooser and
benchmark setup substitutions remain in force; intentional injected console/HTTP
failures are expected only in their failure scenarios. Early runs with disabled
menu-item hit tests, an alignment-covered sample, an implicit rather than explicit
panel choice, and an interrupted wrapper are diagnostic runs, not acceptance.
The final groups completed with successful process exits.

Backend JAR `hict_server-2.0.28-5e7edd4-webui_a1839b7-fat.jar` SHA-256:
`dff6d37370a077b375d071cc7779d25ed7728db127e3e83018e50b3bc444e090`.
Final WebUI tree SHA-256 (sorted relative paths/NUL/bytes, excluding dist/electron):
`0f55a55520de2d76643b96a2ebf8d8981b688dee7934c70bc19231cd40f77fef`.
The JAR embeds the preceding WebUI; this acceptance serves the matching final
assets separately. Final combined-SFX/native packages, other platforms, retina,
touch, broader accessibility, real dense files, navigation latency and mixed soak
remain pending. P4 and the full publication release remain in progress. Protected
files/plans are untouched. Nothing was pushed, merged or tagged.

## Coverage explanation and independent source totals — 2026-11-16

Advisor item 20 now has a whole-map row/column-sum definition with the diagonal
counted once, a sentence explaining when to use coverage normalization, and an
optional keyboard-accessible formula disclosure. It explains the single
source/resolution scale factor, both symmetric halves in the matrix total,
inter-contig contacts, zero-coverage support and additional signal transforms.
The short definition remains visible only with square-root coverage selected;
the formula starts collapsed. docs/coverage-normalization.md adds a worked
2x2 example and explains repeated source references after exact cuts.

The existing track acceptance now replaces a weak inequality check with an
independent numerical oracle. Enumerated raw rows yield [4,10,7,0,11], total 32
and scale C=8.264455657238736. It verifies the four valid weight-track values to
1e-12 relative tolerance and all 25 weighted matrix values for both PRIMARY and
SECONDARY to 1e-6. Each normalized Float32 matrix sums to 32.00000047683716,
within that tolerance of 32. Both raw matrices and source hashes are unchanged.
The disclosure opens/closes using Enter and fits 1440/800 px screenshots.

Final evidence: /tmp/hict-coverage-help-tracks-02/results.json records 13 manager,
source, resize and coverage scenarios, its coverageOracle contains the numerical
values, and controls/results.json adds six existing track scenarios.
/tmp/hict-coverage-help-responsive-02/results.json adds 15 window/menu/range/panel
scenarios at six sizes down to 640 px. All 34 pass with successful process exits;
page and capture-phase window-error lists are empty. Final screenshots were
reviewed, including coverage-explanation-800.png. Controlled-chooser and benchmark
setup substitutions remain as described in the preceding records.

npm run build:web passes (/tmp/hict-coverage-help-web-02.log), with the existing
large-chunk warning. Prettier check and Python compile pass. A formatting-only
rebuild changed the WebUI byte hash, so both browser suites were rerun on the final
assets; the -01 runs are preliminary. There are no Java runtime changes. The
unchanged backend's full 586-test result (zero failures/errors, seven optional
skips) remains recorded in the tracks section; it was not rerun for help text.

Test backend JAR hict_server-2.0.28-5e7edd4-webui_a1839b7-fat.jar SHA-256:
dff6d37370a077b375d071cc7779d25ed7728db127e3e83018e50b3bc444e090.
Final WebUI tree SHA-256 (sorted relative paths/NUL/bytes, excluding dist/electron):
eaf7bf9ef352a27c089d7339eaed6c926f86cc5d3563e16433bfc17bf21b0c3b.
The JAR embeds the preceding interface; browser acceptance serves the final
matching assets separately. Combined packages, platforms and broader release
qualification remain pending. Protected files/plans remain untouched.

P6 audit also establishes the next work, rather than treating existing controls
as completion. /tmp/hict-oe-range-repro-01/results.json reproduces a real UI
round trip: set O/E range to [0.25,4], switch to Observed, switch back to O/E,
and read [0.5,2] with no page errors. VisualizationManager.setSignalDisplayMode
contains that reset. TileHandlersHolder bypasses the enabled graph for non-observed
modes, and TileVisualizationProcessor uses a dedicated O/E color path; therefore
ordinary sidebar color pickers alone do not satisfy advisor item 19. The current
inline range refresh also waits 250 ms and only aggregates loaded tile limits;
it does not satisfy the requested viewport histogram, levels/midpoint or 150 ms
pan gate (item 22). Those items remain pending. P6 and the full goal stay active.
Nothing was pushed, merged or tagged.

## O/E palettes and live range controls — 2026-11-16

Advisor item 19 now has functioning O/E endpoint pickers, alpha, separate
Expected/O/E palette memory across mode changes and browser reloads, and active
palette/session restoration. Default O/E colors are explicitly blue/red with
neutral white at 1 and limits 0.5/2. The shared ObservedExpectedPalette mapper is
also available to an Observed-view graph's colormap node as
mode OBSERVED_OVER_EXPECTED. It colors an already-computed ratio signal, honors
literal or connected limits, and preserves that mode/input across editor Apply
and sidebar edits. API documentation describes this contract. The guide is
[observed-expected-palettes.md](observed-expected-palettes.md).

This is a partial P6 increment. The custom graph still controls Observed;
Expected/O/E retain separate primary-map processing. Direct editing of the active
Show O/E palette through a projected pipeline graph, named O/E palette presets
and the inline viewport histogram/levels/midpoint remain pending. No change was
made to expected-profile fitting: its existing positive-finite-signal policy,
scaffold/contig domains, native-bin distances and shared-reference weights remain.
Older sessions now honor their stored endpoint colors, which the previous O/E
renderer ignored. The guide states this visible behavior change.

Real acceptance exposed two slider bugs. With an upper threshold outside the
automatic domain, dragging shrank the computed domain on each move: a 70% drag
from a selected 0–11 range landed at 2.4016386594 rather than about 7.7. Both limits
are now captured before pointer/keyboard adjustment. Changing source, target or
display mode starts its own domain; loaded-range and Focus actions retain their
explicit behavior. The legacy signal-controls test now chooses the native grid
for its loaded-tile limits, since base-grid images deliberately omit native-cell
statistics. This does not qualify base-grid viewport histogram support.

The second bug suppressed automatic publication: store round trips set the
hydration guard even when local values/colors already matched. Synchronization
now runs only when values differ, so ordinary handle changes publish after the
existing debounce and reload pixels without pressing Apply. O/E handles stay on
opposite sides of neutral 1; invalid typed ranges receive a useful message. The
rail marks neutral white at the corresponding slider position.

Full ./gradlew test shadowJar -x copyWebUI --offline passes: 588 tests, zero
failures/errors, seven optional skips (/tmp/hict-oe-full-jvm-02.log). New numerical
regressions independently check asymmetric logarithmic bounds, half-strength
colors, exact RGBA endpoints, neutral 1, measured zero, undefined/negative ratios,
connected limits and pipeline serialization. Existing default palette expectations
now explicitly select the blue/red endpoints; custom RGBA is tested separately.
npm run build:web passes (/tmp/hict-oe-web-06.log), with the existing large-chunk
warning. Storage tests cover fresh realms, malformed preferences and a failed write
with an older disk value. AST-preserving formatting and Python compile pass.

Final Chromium evidence, DPR 1 on the same Linux host, disposable data:

- /tmp/hict-oe-palette-ui-10/results.json: six focused cases. Actual picker actions
  change tile RGBA and preserve the Observed graph; Expected/O/E retain independent
  ranges; reload and saved-session restore retain chosen colors, limits and pixels;
  keyboard adjustment automatically changes both committed limits and pixels;
  ratio nodes survive the real editor Apply/close and a sidebar edit. The active
  fitting profile determines the positive ratio used by the color oracle; it is
  not assumed that the highest cell reaches the chosen color limit. Undefined
  masking and exact endpoint/interpolation values also have independent JVM tests.
  Source hashes, page-error and capture-phase window-error checks pass across reloads.
- Its display_mode_roundtrip/results.json adds 12 cases covering disabled graphs,
  split/overlay identity sources, unchanged Observed graphs/secondary pixels,
  sessions and rejected publication. Its signal_controls/results.json adds seven
  signal/drag/domain/log/scaling/source/atomic-failure/color cases. Each baseline
  suite owns an independent backend; the focused suite owns another. Apply is
  acknowledged through the actual API commit rather than a toast hidden by a modal.
- /tmp/hict-oe-tracks-02/results.json and controls/results.json add 13+6 cases,
  including independent coverage totals, guarded drafts, resized canvas pixels,
  both source weight modes and source capability hints.
- /tmp/hict-oe-responsive-02/results.json adds 15 cases at six widths down to 640 px.
- /tmp/hict-oe-regression-{sequence_view,selection_modes,fasta_measurement,
  alignment_runs,sequence_tracks,feature_edge_cuts,file_destinations}-03/results.json
  adds all 90 established scientific/selection/sequence/export/save scenarios.

Together these are 149 passing browser scenarios with successful process exits.
Intentional injected console/HTTP failures are expected only in their failure
cases. Existing benchmark setup and controlled-chooser substitutions remain.
Visual review includes the custom translucent O/E map/rail and ratio node editor.
Diagnostic -01 through -09 runs include the reproduced drag/live bugs and corrected
fixture, selector, ratio-endpoint and toast expectations; they are not final gates.

Backend JAR hict_server-2.0.31-f08ec1b-webui_293038a-fat.jar SHA-256:
85907de031ac5b4f38c3f1d43f2843f8adee22e1604d5dc14566a53616775b21.
Final WebUI tree SHA-256 (sorted relative paths/NUL/bytes, excluding dist/electron):
0c076eec9350723b76f7e4de7df15e5dc911a82d013ba7a9b55be89ddcc1c886.
The JAR embeds the preceding WebUI; acceptance serves the matching final assets
separately. Combined SFX/native packages, other platforms, dense-map latency,
retina/touch/accessibility, custom O/E figure-output matrix and mixed soak remain
pending. P6 and the full publication release stay in progress. Protected
files/plans remain untouched. Nothing was pushed, merged or tagged.


## Active primary O/E editor acceptance

The active primary O/E editor is separate from the Observed graph and uses the
same named presets as the sidebar. It preserves the exact Observed graph JSON,
all non-colormap processing options, secondary selection and nontransparent
secondary tile pixels. Updates serialize and coalesce superseded choices; injected
failure supports retry without committing invalid values. Palette import/export
uses a distinct PRIMARY_OE settings schema and the shared destination workflow.
Preview return reloads sidebar changes and reconciles display-mode changes.

Final npm run build:web passes (/tmp/hict-oe-editor-web-10.log), including TypeScript
and all existing Node checks; only the existing large-chunk warning remains.
Changed Vue/TS formatting, Python compilation and git diff --check pass. No JVM
runtime changes were made; the preceding full 588-test JVM run (zero failures or
errors, seven optional skips, /tmp/hict-oe-full-jvm-02.log) remains the backend
baseline, rather than a claimed new run.

Final Chromium evidence on the frozen build, DPR 1, disposable fixture copies:

- /tmp/hict-oe-editor-ui-08/results.json: 11 focused cases. Its independent
  display_mode_roundtrip and signal_controls baselines add 12 and seven cases.
  /tmp/hict-oe-editor-ui-10/results.json repeats the 11 focused cases with explicit
  equality of all non-colormap processing options. Capture-phase window errors
  and page errors are empty, source hashes unchanged. Cases cover visible overlay
  isolation, presets and actual RGBA endpoints, keyboard tabs, compact dimensions,
  Preview/sidebar reconciliation, palette export/reset/import, delayed writes
  (held 6 → queued 8 → latest 4 publishes only 6 and 4), injected 503/retry,
  invalid-draft close and rejection outside O/E mode.
- /tmp/hict-oe-observed-existing-02/browser/results.json: 12 existing Observed
  editor cases, including import/export, inspector, layout, Preview, stale cache,
  delayed thresholds, branch pixels, swaps, presets and reset. Source.json confirms
  an unchanged physical 154,317,488-byte map copy, SHA-256
  8cce355723881effce56ea8da89e25ca3087ebb30159ef13c7e85af20ca5f317.
  The legacy driver now uses the same controlled writable destination as other
  browser save checks and records capture-phase window errors across reloads.
- /tmp/hict-oe-editor-tracks-03/results.json and controls/results.json: 13+8
  track/source, guarded editing, resized canvas and signal-control cases.
- /tmp/hict-oe-editor-responsive-03/results.json: 15 cases down to 640 px.
- /tmp/hict-oe-editor-regression-{sequence_view,selection_modes,fasta_measurement,
  alignment_runs,sequence_tracks,feature_edge_cuts,file_destinations}-02/results.json:
  all 90 established sequence, selection, scientific cut and save scenarios.

These are 168 passing scenarios with successful process exits, plus the focused
11-case rerun, counted separately. Expected injected HTTP/console failures remain
confined to their failure cases. Visual inspection covers active-oe-editor.png,
active-oe-editor-800.png and active-oe-editor-640.png in the focused output.
Diagnostic earlier runs include corrected fixture/route/accessible-name setup and
the reproduced stale-Preview palette; they are not acceptance runs. Tracks -02,
responsive -02 and focused -09 could not bind sockets inside the sandbox and were
rerun successfully with local-server permission as -03, -03 and -10 respectively.

Backend JAR hict_server-2.0.31-f08ec1b-webui_293038a-fat.jar SHA-256:
85907de031ac5b4f38c3f1d43f2843f8adee22e1604d5dc14566a53616775b21.
Frozen WebUI tree SHA-256 (sorted relative paths/NUL/bytes, excluding dist/electron):
c007a136a5438951a41d595419915e5f6fc4e0ba65bd68f828f827a9a6fdb3e6.
Acceptance serves these assets separately from the JAR's embedded preceding UI.
Combined SFX/native packages, other platforms, dense-map latency, retina/touch,
full accessibility, figure-output matrix and mixed soak remain unqualified.
Viewport histogram/levels and the remaining advisor/release/publication gates are
still pending. Protected plans/files are unchanged. Nothing pushed, merged or tagged.


## Inline viewport histogram, fitting and missing O/E diagnostics

Implemented a 64-bin inline curve inside SignalRangeControl with aligned threshold
marks, exact-sample percentile fitting, source/input selection, bounded querying,
cancellation and failed-refresh Retry. Vertical square-root count scaling and
bounded Catmull–Rom smoothing are drawing aids only. Out-of-axis samples are counted
separately. A 2-D stratum grid replaces flattened sampling, which could select only
one column on square matrices. Interactive previews retain at most 65,536 cells
in 16 blocks of 64×64 with four concurrent queries. Existing detailed/1-D inspectors
retain their previous default block sizes and sequential I/O.

TRADITIONAL_NORMALIZED numeric queries use the renderer's missing-O/E mask. Supported
zero and missing expected support are distinct (NaN in binary, null in JSON).
OpenAPI documents nullable signals, BINS response units and matching expected-profile
reuse. Legacy internal finite normalization remains available. A real-fixture JVM
regression compares within-domain signals, zeros, missing cross-domain support and
image alpha. O/E fitted limits use positive percentiles with 0.5/2 fallbacks where
samples do not supply a side of neutral 1. An ordinary empty viewport fits 0–1,
keeping zero at the lower color instead of inventing midrange contrast.

A diagnostic cold pan took 558.6 ms (/tmp/hict-inline-hist-ui-10); the next pans were
47.4 and 31.1 ms. Traced repeat -11 showed matrix request elapsed times dominated
by browser-side stalls before transmission during tile loading (e.g. ~35 ms before
send versus ~2 ms to first response); tile requests also queued before transmission.
This indicates HTTP/1 connection pressure, though it does not prove the entire
outlier's cause. OpenLayers now budgets three image loads for the main map and
one for the minimap. It retains queue priority and existing PNG/binary cancellation
semantics while reserving connection capacity for interactive requests. No new
transport or server-priority bypass was introduced. Final independent cold runs
below verify a peak of four actual tile transfers and the 150 ms fixture gates.

npm run build:web passes (/tmp/hict-inline-hist-web-08.log), including all TypeScript
and Node suites, with the existing large-chunk warning. Numerical checks cover exact
quantiles, flat/zero signals, signed/zero/missing/outside bins, logarithmic axis
binning, bounded spline controls, 2-D sample coverage and query/concurrency/memory
bounds. Formatting, Python compile and diff checks pass. Full ./gradlew test shadowJar
-x copyWebUI --offline passes: 589 tests, zero failures/errors, seven optional skips
(/tmp/hict-inline-hist-jvm-full-03.log), including the corrected API resource schema.

Final Chromium evidence, frozen UI/JAR, Linux DPR 1, disposable copies:

- /tmp/hict-inline-hist-ui-14/results.json: 13 focused cases. Known 5×5 contacts
  yield 25 finite cells/19 zeros. O/E yields 13 finite ratios/7 measured zeros and
  excludes 12 unsupported cells; independent expected values include 1/6 and 0.6.
  Numeric JSON distinguishes null from zero, and binary-driven curve counts agree.
  Fit publishes limits and changes tile pixels while preserving contacts. Secondary
  custom ×3 fitting reaches 33 with Primary graph/pixels unchanged. At 800×600 and
  640×800 the chart/actions fit. Larger viewports explicitly sample 65,536 cells;
  panning changes actual query windows. Empty fitting preserves zero-colored pixels.
  Injected 503 clears stale data, disables Fit, preserves graph/options and recovers
  through Retry. Sources and unexpected page/console-error checks pass.
- The independent fresh-server repeat /tmp/hict-inline-hist-ui-15/results.json passes
  the same 13 cases. -14 pan-latency.json: 8.4, 21.8, 17.1, 18.7 ms; its sampled
  counterpart: 29.7, 28.9, 31.3 ms. -15: 10.8, 15.3, 15.6, 15.5 ms and 25.5, 28.0,
  26.6 ms. Both assert the 150 ms bound and measure peakTileTransfers=4. Timing,
  browser long-task and request traces are retained. These are isolated synthetic
  fixture claims, not dense production-map or packaged-build qualification. The
  tiny 17 bp source always fits at permitted zoom; varying cropped windows are
  proved separately with the larger fixture.
- /tmp/hict-inline-hist-oe-04/results.json and its display_mode_roundtrip and
  signal_controls directories: 11+12+7 palette, processing, session, graph, Secondary,
  Preview, delayed-write, drag and atomic-failure cases. Empty capture-phase window
  errors across reloads are also checked by the O/E driver.
- /tmp/hict-inline-hist-responsive-04/results.json: 15 responsive/navigation/control
  cases. /tmp/hict-inline-hist-tracks-04/results.json and controls/results.json:
  13+8 track/source/signal/resize cases.
- /tmp/hict-inline-hist-regression-{sequence_view,selection_modes,fasta_measurement,
  alignment_runs,sequence_tracks,feature_edge_cuts,file_destinations}-04/results.json:
  all 90 established sequence, selection, scientific cut and save cases.
- /tmp/hict-inline-hist-observed-04/browser/results.json: 12 Observed-editor cases.
  /tmp/hict-inline-hist-legacy-04/browser/results.json: four detailed histogram
  row/column/scalar, log/rebin/no-extra-I/O, mobile and cancel/retry cases. The detailed
  driver uses the established controlled writable chooser. Both physical 154,317,488
  byte real-map copies stay unchanged, SHA-256
  8cce355723881effce56ea8da89e25ca3087ebb30159ef13c7e85af20ca5f317.

Together: 185 passing browser scenarios with successful process exits. The
independent 13-case focused repeat is counted separately. Visual review covers the inline O/E chart at 640 px,
desktop thresholds and sampled captions. Earlier diagnostics corrected a debounced
write wait, tooltip integer formatting, tiny-map full-extent assumption and the
O/E/empty-region fitting policies. Initial Vite builds could not spawn Git inside
the sandbox; permitted builds passed. Injected 503 console failures are expected
only in that failure case.

A separate existing normalization race appeared in -13: a user-entered Upper=100
could be replaced by 11 when the preceding normalization refit finished. The focused
fixture now awaits the real policy/refit promise, acknowledges and verifies its
initial 100 commit before testing Fit. It does not qualify rapid edits during a
refit. Source-scoped serialization/coalescing and protection of newer user drafts
from refit/hydration remain mandatory release work, recorded in the progress ledger.

Final development JAR hict_server-2.0.33-1f68b42-webui_0a1e9fd-fat.jar SHA-256:
64fd988b0bddf228f2197ecdf72f2b20c1af69ea0746fbc5d6353defc367a429.
Frozen WebUI tree SHA-256 (sorted relative paths/NUL/bytes, excluding dist/electron):
1fc967cb6fae8b6f9845884ff1ef8ae15f10d2126f21ecdbd3be3e20490da318.
Acceptance serves these assets separately from the JAR's preceding embedded UI.
Earlier code-equivalent JAR 974b9400… precedes the nullable/BINS schema correction.
Combined SFX/native packages, other platforms, dense-map latency/soak, retina/touch,
full accessibility and figure-output qualification remain pending. Draggable color
midpoint/gamma is not delivered; O/E neutral must remain 1. P6 and the overall goal
remain active. Protected plans/files are unchanged. Nothing pushed, merged or tagged.

## Visualization transaction and normalization draft regression

WebUI commit `b994994`; matching version
`2.0.35-7909e72-webui_b994994`.

The committed preceding WebUI reproduced the rapid-edit defect in
`/tmp/hict-viz-transactions-red-01`: choosing normalization then entering Log2 and
Upper=100 ended at Linear/11. The final implementation records field revisions,
captures source/target/mode, serializes mutations, coalesces obsolete queued
applications and merges pending fields over fresh server state. Manual palette
edits cancel automatic refitting; processing-only edits defer it until committed.
Fresh reads are generation guarded. Presets, display-mode changes, session capture
and opening the graph editor wait behind pending visualization changes. A failed
graph read cannot be interpreted as a disabled pipeline. Failed writes retain the
draft for retry without replacing the last committed graph/map.

Final WebUI build: `/tmp/hict-viz-transactions-web-10.log`, including type checking,
all existing numerical/DTO/streaming checks and new field-revision/projection
checks. Explicit Prettier checks pass for all 11 changed/new WebUI files; the new
Python browser driver passes Black checking. No JVM runtime source changed; the
preceding 589 tests, zero failures/errors and seven optional skips remain the JVM
baseline (`/tmp/hict-inline-hist-jvm-full-03.log`), not a new execution.

Final Chromium runs, all successful process exits:

- `/tmp/hict-viz-transactions-ui-07/results.json`: 16 new delayed-normalization,
  transform/range, source switching, coalescing, stale-read, failed-read/write,
  session, mode, source/global preset and editor-opening cases. Actual backend
  options/graphs, numeric inputs, map pixels and saved session are asserted.
- `/tmp/hict-viz-transactions-oe-03/{results.json,display_mode_roundtrip/results.json,
  signal_controls/results.json}`: 11+12+7 existing palette/mode/signal cases.
- `/tmp/hict-viz-transactions-responsive-01/results.json`: 15 responsive cases.
  `/tmp/hict-viz-transactions-tracks-01/{results.json,controls/results.json}`:
  13+8 tracks/source/control cases.
- `/tmp/hict-viz-transactions-regression-{sequence_view,selection_modes,
  alignment_runs,sequence_tracks,feature_edge_cuts,file_destinations}-01/results.json`
  plus `/tmp/hict-viz-transactions-regression-fasta_measurement-02/results.json`:
  90 existing sequence, selection, exact-cut and file-destination cases.
- `/tmp/hict-viz-transactions-observed-02/browser/results.json`: 12 existing
  Observed graph-editor cases. `/tmp/hict-viz-transactions-legacy-01/browser/results.json`:
  four detailed histogram cases. Both 154,317,488 byte physical real-map copies
  remain unchanged, SHA-256
  `8cce355723881effce56ea8da89e25ca3087ebb30159ef13c7e85af20ca5f317`.
- `/tmp/hict-viz-transactions-hist-01/results.json`: 13 inline histogram cases,
  including unchanged contact hashes, O/E missing-versus-zero, actual fit pixels,
  sampling limits and network-image peak four. Isolated fixture pan acknowledgements
  are 16.4–21.1 ms; sampled pans 21.6–25.2 ms, below the 150 ms fixture gate.

Together: 201 passing final-build browser scenarios. The first long scientific
wrapper exited 143 during FASTA measurement; that incomplete run is excluded and
its standalone -02 rerun passed all 12. Earlier iterations exposed and corrected
an inherited Primary weight policy mislabeled on Secondary, Vue-proxy identity
comparison in refit ownership, and a rejected mode request leaving an optimistic
header while Secondary was selected. The existing inactive-O/E guard message is
preserved. Intermediate builds/runs are diagnostic, not final acceptance. Expected
503 console errors are allowed only in the explicitly injected failure cases;
final capture-phase window errors are empty. Final screenshot review covers the
16-case driver's desktop/range/failure captures and 640 px responsive Help menu.

Frozen final WebUI tree SHA-256 (sorted relative paths/NUL/bytes, excluding
dist/electron): `da24080446d6c179614ab8842abd7caf182faec9b83ca0528fc993a7725ba375`.
Development backend `hict_server-2.0.33-1f68b42-webui_0a1e9fd-fat.jar` SHA-256:
`64fd988b0bddf228f2197ecdf72f2b20c1af69ea0746fbc5d6353defc367a429`.
Browser acceptance serves the frozen new assets separately from the JAR's older
embedded interface. This does not qualify a combined SFX/native package. Shared
FILE/coverage vector selection remains global; arbitrary graph-editor publication
and local preset track/background styling are not all moved into this queue.
Draggable midpoint/gamma, derived-view exact-scan applicability, broader graph/NaN
provenance, dense-map/soak/accessibility and release/publication gates remain open.
All protected plans/owner files remain unchanged. Nothing pushed, merged or tagged.

## Whole-resolution Expected/O/E statistics

WebUI commit `9aea0ec`; matching version `2.0.36-5381e0b-webui_9aea0ec`.

`Scan whole map` previously sent the Observed colormap expression in every display
mode, and `/matrix/statistics` forced PIPELINE_SIGNAL. The endpoint now accepts
TRADITIONAL_NORMALIZED for the primary display signal, without an expression.
Its default remains PIPELINE_SIGNAL. In a derived view, an independent, immutable
whole-resolution expected profile is fitted in 512 × 512 blocks and reused across
all scan blocks. A single-entry profile cache is keyed by server revision and
resolution and does not replace the viewport cache. Thread interruption and tile
generation checks bound cancellation to a block during fitting. Matrix storage is
block-bounded; the profile grows with distance bins. Finite counts include supported
zeros and exclude missing O/E support. Counts remain display cells, including
repeated source-bin references after cuts, not independent physical contacts.

The sidebar captures display mode/expression in the scan key, waits for processing
drafts, guards metadata and block responses, offers Cancel, and retains completed
limits during pan at the same resolution. Changing signal context cancels an active
scan. Derived limits are explicitly labeled whole-map; they are independent of the
viewport profile and do not apply thresholds or graph changes. The irrelevant
Observed loaded-tile action and identity percentage scale are not substituted for
the active primary derived signal. API schema and user guidance document the scope.

Evidence:

- `/tmp/hict-exact-derived-red-01.log`: final UI against the preceding backend
  reproduces Expected max 8 instead of 5. This is an intentional failing diagnostic.
- `/tmp/hict-exact-derived-jvm-focused-02.log`: new HTTP regression passes on a
  513-bin source with diagonal contacts 2 and 8 and one distance-512 contact 4.
  Across four partial/full blocks, Expected max is 5 and finite count 263169;
  O/E max is 1.6 and finite count 515. The deliberately different viewport fit
  remains the same object, source bytes stay unchanged, the legacy constant graph
  expression still yields 99, ambiguous requests are rejected and obsolete
  revisions return 409. The first attempt lacked the test server's initial
  visualization-options fixture; corrected setup passes without changing assertions.
- `/tmp/hict-exact-derived-jvm-full-01.log`: all 590 tests, zero failures/errors,
  seven optional skips. `/tmp/hict-exact-derived-web-02.log`: build/type checks
  and existing numerical/DTO/streaming regressions pass. Explicit Prettier passes
  for the two changed WebUI files; the new Python driver passes Black checking.
- `/tmp/hict-exact-derived-ui-03/results.json`: ten new real-control cases for
  Observed, Expected/O/E profiles, exact cache, cancellation/retry, stale mode,
  immediate transform then Scan and 640 px controls. Options/graphs, native
  display counts, readback of the independent viewport fit and source hashes are
  asserted. `/tmp/hict-exact-derived-ui-04/results.json` independently repeats all
  ten with additional byte-identical rendered PNG checks before/after derived scans.
  An earlier driver attempt used the wrong JSON field (`matrix` instead of the
  actual flattened `values`) and is excluded from acceptance.
- `/tmp/hict-exact-derived-transactions-01/results.json`: 16 delayed-write/read,
  source switching, presets, session and graph-editor barrier cases.
- `/tmp/hict-exact-derived-oe-01/{results.json,display_mode_roundtrip/results.json,
  signal_controls/results.json}`: 11+12+7 existing O/E/mode/signal cases.
- `/tmp/hict-exact-derived-responsive-01/results.json`: 15 responsive cases.
  `/tmp/hict-exact-derived-tracks-01/{results.json,controls/results.json}`:
  13+8 tracks/source/control cases.
- `/tmp/hict-exact-derived-observed-01/browser/results.json`: 12 Observed-editor
  cases; `/tmp/hict-exact-derived-legacy-01/browser/results.json`: four detailed
  histogram cases. Both 154,317,488 byte physical map copies stay unchanged, SHA-256
  `8cce355723881effce56ea8da89e25ca3087ebb30159ef13c7e85af20ca5f317`.
- `/tmp/hict-exact-derived-hist-01/results.json`: 13 inline histogram cases.
  Isolated pan acknowledgements 14.4–16.5 ms; sampled pan 24.0–31.2 ms; network
  image-transfer peak four. These meet the 150 ms fixture gate, not a production
  dense-map latency or package qualification.

Together: 121 passing final-build browser cases, plus an independent ten-case
repeat. All processes exited successfully. Browser window-error captures are empty;
injected 503s are allowed only in the existing explicit failure cases. Final visual
review covers desktop whole-map Expected wording/limits and the 640 px exact scan
controls and transformed result. Scientific sequence/cut/file suites were not
rerun for this increment; their previous 90-case evidence is retained separately.

Frozen WebUI tree SHA-256 (sorted relative paths/NUL/bytes, excluding dist/electron):
`c258c498ef032ff44770d453a17840af7369d5498d41c9ce41beeaec8cfa0eac`.
Development JAR `hict_server-2.0.35-7909e72-webui_b994994-fat.jar` SHA-256:
`e05ef39bab38444292bdd2ea1b4c53a14b9a1909d6d332925c4b3f9e8ed449a3`.
Browser tests serve these new assets separately from the embedded older interface.
Combined/native package, dense-map profile memory/first-pass duration, broader
custom graphs, midpoint/gamma and the other release/publication gates remain open.
Protected plans and owner files are unchanged. Nothing pushed, merged or tagged.

## Ordinary color midpoint and gamma persistence

WebUI commit `28ba7f9`; matching version `2.0.37-44f1f5b-webui_28ba7f9`.

SimpleLinearGradient now carries optional gamma (0.05–100, default 1). Ordinary
RGBA transfer uses pow(clamp((signal-min)/(max-min),0,1),gamma); the midpoint is the
signal whose blend is one half. This does not modify numeric input, thresholds,
normalization or samples. The diamond supports pointer, keyboard/fine/page/Home/End
adjustment and reset, with distinct space when thresholds are close. The rail
evaluates the curve on its linear/log adjustment axis. O/E neutral remains 1 and
identity palettes keep their own mapping, without an ordinary midpoint handle.

The native path transforms its copied flat color buffer before calling the existing
linear kernel; original arrays and the native ABI are unchanged. Java and LINEAR
pipeline colors use the same power transfer. Non-default gamma survives DTO,
session, source/global preset, editor, derived palette memory and export metadata
paths. Default gamma is omitted where legacy representations omitted it. Saving a
user preset waits for pending visualization changes. A real browser case exposed
the existing Secondary-selected/single-layout preset path resetting Primary; that
path now uses the captured Secondary scope.
This applies to numeric/colormap settings; preset background and track styles
remain shared scene settings.

Final evidence:

- `/tmp/hict-midpoint-web-05.log`: build, type check, all existing numerical/DTO/
  streaming checks and expanded field-draft tests pass. New midpoint inverse tests
  prove the half-color relation and known square/square-root cases, and retain a
  newer gamma draft over an older acknowledgement. Prettier passes for all 17
  changed/new WebUI files; the new Python driver passes Black checking.
- `/tmp/hict-midpoint-jvm-full-03.log`: 594 tests, zero failures/errors, seven optional
  skips. Four new ColorGammaTest cases cover known RGBA interpolation/endpoints,
  finite bounds, legacy/explicit DTO encoding, lossless session transfer, graph/
  traditional color agreement with unchanged numeric graph values, and native/Java
  agreement within one channel byte without modifying input. The native gamma
  test executes successfully, not as another optional skip.
- `/tmp/hict-midpoint-ui-05/results.json`: 13 real-control cases. They assert gamma
  against actual PNG colors and unchanged numeric inputs/other-source graph,
  pointer position, thresholds/log-axis/reset, close handles, independent source/
  Expected curves and fixed O/E neutral, source preset scope, portable graph
  presets, immediate Save then exported user preset, graph-editor close, byte-
  identical tiles after session restore, and 640 px keyboard endpoints.
  `/tmp/hict-midpoint-ui-06/results.json` independently repeats all 13 and captures
  the actual crowded-handle state before Focus range resets the domain.
- `/tmp/hict-midpoint-transactions-01/results.json`: 16 previous delayed read/write,
  normalization/source/preset/session/editor cases. `/tmp/hict-midpoint-exact-01/
  results.json`: ten whole-map signal-scan, profile, cache/cancel and stale-mode cases.
- `/tmp/hict-midpoint-oe-02/{results.json,display_mode_roundtrip/results.json,
  signal_controls/results.json}`: 11+12+7 O/E/mode/signal cases.
- `/tmp/hict-midpoint-responsive-01/results.json`: 15 responsive cases.
  `/tmp/hict-midpoint-tracks-01/{results.json,controls/results.json}`: 13+8 track/
  source/control cases.
- `/tmp/hict-midpoint-observed-01/browser/results.json`: 12 Observed graph-editor
  cases. `/tmp/hict-midpoint-legacy-01/browser/results.json`: four detailed histogram
  cases. Both 154,317,488 byte physical real-map copies remain unchanged, SHA-256
  `8cce355723881effce56ea8da89e25ca3087ebb30159ef13c7e85af20ca5f317`.
- `/tmp/hict-midpoint-hist-01/results.json`: 13 inline histogram cases. Isolated
  pan acknowledgements 13.2–22.9 ms; sampled pans 29.3–30.5 ms; image-transfer peak
  four. This default-gamma fixture meets the 150 ms gate; dense-map/non-default-gamma
  performance and packaged builds remain unqualified.

Together: 134 final-build passing browser scenarios, plus the independent 13-case
repeat, all with successful process exits and empty window-error captures. Existing
injected 503s remain limited to explicit failure cases. Final screenshots reviewed
include desktop pointer/curve controls, the truly crowded-handle lane in -06, and
640 px controls. Earlier diagnostics found the real source-preset bug and missing
gamma in two serialization paths. Initial driver runs also needed the Save tooltip
locator (the icon changes its accessible text) and exact Rendering pipeline… menu
label. Those incomplete runs are not counted. An early TypeScript check caught an
overbroad edit to a percentile argument; the corrected final build passes. The
first new graph unit assertion assumed raw rather than the existing clamped
numeric-root semantics; the corrected independent expected clamp is unchanged by
gamma. Scientific sequence/cut/file suites were not rerun for this increment.

Frozen WebUI tree SHA-256 (sorted relative paths/NUL/bytes, excluding dist/electron):
`63ddfd5e36526536175b624ef58896ff1c2ad30bf4acc35af5f7ddae2dae87c8`.
Development JAR `hict_server-2.0.36-5381e0b-webui_9aea0ec-fat.jar` SHA-256:
`fde4daf84a2c4db269d6263b2eec0caa64fbe0f70b194dc17fec10385b7aecbc`.
Browser acceptance serves these new assets separately from the JAR's older embedded
interface. Combined SFX/native/platform, dense-map/soak/non-default-gamma performance,
wider graph/NaN provenance and the remaining release/publication gates stay open.
Protected plans/owner files are unchanged. Nothing pushed, merged or tagged.

## Jobs-first example downloads and recovery (2026-11-17)

This increment implements the download handoff portion of P7. The catalog, real
downloader, HTTP job routes and actual Jobs controls are exercised together using
a controlled local TLS provider. The provider serves a checksum-pinned valid HDF
fixture, supports Range/ETag resume, can pause transfers, fail required/optional
files and omit Content-Length. A test-only constructor policy permits its owned
TLS port; production URL validation, bind/CORS and bundled catalogs are unchanged.
The fixture class and main API server run from frozen JAR snapshots; browser tests
serve the new WebUI assets separately. This qualifies a development pair, not a
combined SFX, native wrapper or public example provider.

Final evidence:

- `/tmp/hict-example-jobs-web-08.log`: WebUI build, type checking and existing
  numerical/DTO/streaming checks pass. All six changed/new UI files pass Prettier;
  the three changed/new Python browser scripts pass Black checking.
- `/tmp/hict-example-jobs-jvm-full-08.log`: 597 tests, zero failures/errors and
  seven optional skips. Added tests cover retained attempt identities, scoped
  cancellation, bounded history, warnings and truly unknown transfer totals.
- `/tmp/hict-example-jobs-ui-06/results.json`: 11 browser cases, plus missing-ID
  HTTP 400, unknown-ID HTTP 404 and stale-cancellation HTTP 404 assertions. Cases
  cover catalog Retry, focused named byte progress, checksum-verified map opening
  exactly once with known raw tensor cells, closure preserving transfer without
  auto-opening, failed attempts/retry under new identities, actual Range resume,
  indeterminate totals, workspace changes, late start replies, optional warnings
  and 640 px scrolling/keyboard access. `/tmp/hict-example-jobs-ui-07/results.json`
  independently repeats all 11 with successful exit.
- `/tmp/hict-example-jobs-other-05/browser/results.json`: ten existing service and
  FASTA-only cases. Its controlled file chooser replaces obsolete test selectors.
  FASTA-only Create/Open from Jobs followed by the actual AGP export downloads a
  complete 12,017 bp component row. Source hashes remain unchanged. The optional
  public DNA Zoo download block is explicitly skipped; the dedicated TLS suite
  covers example downloads without external-provider variability.
- `/tmp/hict-example-jobs-responsive-02/results.json`: 15 cases;
  `/tmp/hict-example-jobs-files-02/results.json`: 14 destination/cancellation cases.
- `/tmp/hict-example-jobs-transactions-03/results.json`: 16 delayed normalization,
  read/write, source/preset/session/editor cases. An earlier repeat timed out
  because the opening preset had already selected None; choosing None again
  correctly performed no operation. The driver now explicitly establishes and
  asserts weighted initial state before intercepting the opposite change. The
  original race and pixel assertions remain intact. The incomplete run is not
  counted.
- `/tmp/hict-example-jobs-midpoint-02/results.json`: 13 midpoint cases.
  `/tmp/hict-example-jobs-oe-02/{results.json,display_mode_roundtrip/results.json,
  signal_controls/results.json}`: 11+12+7 O/E/mode/signal cases.
  `/tmp/hict-example-jobs-tracks-02/{results.json,controls/results.json}`: 13+8
  track/source cases. `/tmp/hict-example-jobs-exact-02/results.json`: ten exact
  whole-map signal scan/cancellation cases.
- `/tmp/hict-example-jobs-observed-02/browser/results.json`: 12 graph-editor cases;
  `/tmp/hict-example-jobs-legacy-02/browser/results.json`: four detailed histogram
  cases. Both physical real-map copies remain unchanged at 154,317,488 bytes,
  SHA-256 `8cce355723881effce56ea8da89e25ca3087ebb30159ef13c7e85af20ca5f317`.

Final screenshot review covers the named byte-progress card and 640 px scrolled
history with the fixed Close header and retained optional-file warning. Browser
window-error captures are empty; deliberately injected provider failures remain
confined to their failure scenarios. Earlier fixture/driver diagnostics included
Java source-launch classloader access, an attribute assertion and obsolete
FASTA-only selectors; corrected final compiled-fixture runs pass. Scientific
sequence/cut suites were not rerun for this increment.

Frozen WebUI dist SHA-256 (sorted relative paths/NUL/bytes, excluding its electron subtree):
`b59b81795e75504e9f59d2ff1a6daf1b9b4472e8175e107555449957959812ee`.
Development JAR `hict_server-2.0.37-44f1f5b-webui_28ba7f9-fat.jar` SHA-256:
`0d362f66754de88b9f0b3739170914c7c04f361780caa09a44b11407b4423098`.
Subsequent conversion/wizard phases, cache/catalog integrity provenance, public
provider availability, OSD resolution/diagonal checks, other platforms and final
release/publication gates remain open. Protected plans and owner files are
unchanged. Nothing pushed, merged or tagged.

`/tmp/hict-example-jobs-hist-01/results.json` adds 13 final inline histogram cases
with unchanged inputs, empty window errors and image-transfer peak four. Isolated
pan acknowledgements are 15.7–17.4 ms; sampled pans are 24.0–26.1 ms against the
150 ms fixture gate. This does not qualify dense maps or native packages. Together
the final development pair passes 169 browser scenarios plus the independent
11-case download repeat, all with successful process exits.

## Cached example opening integrity (2026-11-17)

Cached cards and historical results now create a cancellable verification job
before opening. Catalog discovery compares a canonical declaration fingerprint
and recorded file metadata without hashing large datasets. Opening rehashes actual
bytes, including same-size/same-mtime corruption, repairs damaged files and refreshes
manifest provenance. Unpinned files need matching source declarations and verified
recorded hashes; unchanged sources survive recipe/attribution updates without a
network transfer. Malformed records fall back to downloading. Late start/catalog
replies cannot replace another workspace.

Final `/tmp/hict-example-cache-web-04.log` build/type/numerical checks pass; all six
changed UI files pass Prettier and the browser driver passes Black. Final JVM log
`/tmp/hict-example-cache-jvm-full-04.log` and XML reports show 599 tests, zero
failures/errors, seven optional skips. Two added tests cover declaration ordering,
response-only fields, changed recipes/sources, tampering with restored timestamps,
metadata discovery, reuse without provider requests and malformed manifest shapes.
`/tmp/hict-example-cache-ui-03/results.json` passes 16 actual-control cases, including
historical-result revalidation, cache reuse, blocked opening during repair, exact
repaired SHA-256 and refreshed attribution. Existing retry/cancel/Range, stale
intent, optional warnings and 640 px history still pass. Other job services and
FASTA-only AGP pass ten cases in `/tmp/hict-example-cache-other-02/browser/results.json`;
the physical input hashes remain unchanged. Both browser processes exit zero.
No broad rendering/performance or scientific cut suite was rerun for this scoped
increment. Initial diagnostic runs exposed a missing test import, an intermediate
event type, mixed pre-fix compiled sources and a GET instead of POST in the new
driver; only the consistent final artifacts above are counted.

Frozen WebUI dist SHA-256 (sorted relative paths/NUL/bytes, excluding its electron
subtree): `5d3817e4761624364e1a12d13e89a7040606153000962524ddb08f183b08bf86`.
Development JAR `hict_server-2.0.38-013586d-webui_d670b94-fat.jar` SHA-256:
`a29e3adf204cba80f8be23c738f82cc3ac021ad0adfc8b950679fd74f3d98174`.
The browser serves current assets separately from older embedded assets; this is
not SFX/native qualification. Final screenshot review covers repair progress and
640 px history. It also exposes repeated startup sensitivity/progress notifications
overlapping the dialog; this UX issue remains a release gate. Public providers,
conversion/wizard phases, catalog/session provenance transfer, OSD/diagonal behavior
and final platform/publication gates remain open. Protected files are unchanged;
nothing pushed, merged or tagged.

## Example conversion handoff, recovery and quiet opening (2026-11-17)

Quick-start conversion now hands progress to Jobs and retains validated example
context so results reopen through the recipe with their FASTA. Close/workspace/
failure/late replies revoke automatic opening; unrelated sources and custom cached
destinations are rejected before output preparation. Startup notices no longer
accumulate; failures have a short summary, expandable diagnostic and focused retry.
Final JVM `/tmp/hict-example-conversion-jvm-full-04.log` and XML: 600 tests, zero
failures/errors, seven optional skips. Final WebUI `/tmp/hict-example-conversion-web-03.log`
build/type/numerical checks pass, including omitted/present example-ID DTO checks;
six UI/script files pass Prettier and both Python scripts pass Black. Final
`/tmp/hict-example-conversion-ui-05/results.json` passes 23 actual-control cases
with empty window errors; `/tmp/hict-example-conversion-other-03/browser/results.json`
adds ten ordinary-conversion/other-service/FASTA-only AGP cases. Both exit zero.
Real bundled hictk metadata/zoomify/balance/import run on a Cooler-generated seed;
linked FASTA export matches all 17 bases. Independent fresh-server verification in
ui-05/full-cell-parity.json matches all 25 known cells in the original and four
converted matrices, and ui-05/input-integrity.json confirms all ten downloaded
matrix/reference files still match their pinned hashes. Historical result opening
reuses one completed conversion and preserves its reference. Actual Jobs completion
and accepted submission replies are held for close/workspace/late-intent checks;
no states or conversion outputs are fabricated. Bad source/destination requests
return HTTP 400 without creating the misrouted output. Screenshot review covers
handoff, compact/expanded failure, focused retry and empty notification history.
Diagnostics caught a real omitted DTO field; fixture/driver corrections supplied a
standards-compatible Cooler, explicit provider routing for delayed starts, completed
FASTA linking before export, and an existing alternate map. Incomplete runs are
excluded. Reproduce with Cooler/h5py/pandas via --hdf-python. Frozen dist SHA-256
(sorted relative paths/NUL/bytes, excluding its electron subtree):
`6381073040376055b7911f4399241a4ec95d8852ac7133d17497bf74e64da135`.
Development JAR `hict_server-2.0.39-210bbe0-webui_d787595-fat.jar` SHA-256:
`cb5e4abb1523e4109cf64df5d952380e3ca03a5149319ed8c667128966b53a7d`.
Assets are served separately from older embedded assets, qualifying a development
pair. Broad rendering/cut/sequence/performance/native suites were not rerun. Live
conversion cancellation, large/public examples, wider wizard workflows, catalog/
session provenance transfer, OSD/diagonal behavior and release/publication gates
remain open. Protected plans/owner files are unchanged; nothing pushed/merged/tagged.


### Info, map-switch notifications and disjoint-resolution diagnostics

Info uses view/layer metadata rather than TileLayer.getData. The before-fix real
yeast probe (`/tmp/hict-osd-baseline-01/results.json`) made 425 pixel reads for 25
pointer moves with Info hidden and 425 with it visible; a stationary delayed zoom
left Info blank. Final `/tmp/hict-osd-ui-08/results.json` passes 16 cases with empty
window errors, zero reads in both probes and maximum measured update durations
0.1 ms hidden/0.4 ms visible. Four real resolution transitions hold tile replies;
current guidance remains populated. Loaded tiles, stationary pan/toggle/settings,
literal markup names, released old watchers, outside-assembly coordinates, mixed
pyramids and a genuinely late old-map masking callback are covered. These small
probes are not broad latency or large-map performance qualification.

The exact tiny NaN-weighted-map switch reproduced a renderer exit with
vue-sonner 0.4.2: its delete watch effect fed height updates back into itself.
Pinned 2.0.9 uses the maintained dismissal path and requires the explicit CSS
import. Final `/tmp/hict-osd-notification-02/results.json` passes the one-case real
notice/dismiss/map-replacement regression with empty errors and unchanged inputs.
The real mixed-pyramid UI also uncovered a BINS PIPELINE_SIGNAL HTTP 500; the
existing projection now respects BINS/PIXELS visibility on both grids and skips
Secondary for Primary-only expressions. Two new HTTP tests assert all projected
cells/mass for disjoint fine/coarse levels and hidden Primary/Secondary bins,
including the legacy omitted-type source expression. Full final JVM
`/tmp/hict-osd-jvm-full-02.log` and XML: 602 tests, zero failures/errors, seven
optional skips. WebUI `/tmp/hict-osd-web-04.log` build/type/numerical checks pass;
five WebUI files pass Prettier and three new Python scripts pass Black.

Final independent `/tmp/hict-osd-ui-08/diagonal.json` matches all 669,584 raw cells
in 17 windows on chrI/chrVI/chrM at 50/100/500 bp against Cooler balance=False.
Scanning all source stored cis pixels confirms offsets 0–3 at 50 bp and 0–1 at
100 bp are empty, while the 500 bp diagonal contains 3,418,162 contacts. Bin offsets
are not exact read-pair distances. The source establishes that this band predates
HiCT import; the upstream processing description does not establish its precise
cause. `source-hashes.json` confirms immutable physical copies: source.mcool
SHA-256 `11a8e588694cfd686958ab23576aa3030dcf69f0c868d760082b34fc5d918a4b`,
yeast.hict.hdf5 `dff20b25ee9cdaa6d01802c59e3f251e0694d36c10f025cc69fa221c32e7f722`.
Reproduce with check_osd_resolution_browser.py using --jar, --webui, --source-dir,
--output and --hdf-python (Cooler/h5py/NumPy). It invokes the independent
check_microc_diagonal.py after opening the copied map on a fresh server.

Final browser evidence adds transactions-02 (16), examples-01 (23),
other-jobs-01/browser (10), failures-02/browser (6), tracks-01 plus controls (13+8),
responsive-01 (15) and histogram-01 (13), all under `/tmp/hict-osd-`, all exit zero.
Together with the 16 Info and one dismissal case, these are 121 cases. Error arrays
are empty where captured; failure-reporting deliberately injects errors. Histogram
qualification runs in isolation and observes at most four tile transfers. Screenshot
review covers delayed-tile guidance, literal names, absent old-map notice, current
error-toast styling, dark track edges after resizing and responsive controls.
Initial helper failures (Done/Close selector, ruler versus map pointer target,
incomplete Secondary frontend setup and NumPy scalar serialization) are corrected
and excluded. The old-library renderer crashes and actual BINS HTTP failure are
recorded as fixed defects, not environmental flakiness; incomplete runs are not
counted. No out-of-memory kill was observed.

Frozen separately served dist SHA-256 (sorted relative paths/NUL/bytes, excluding
electron): `613bc21dde832be5db24a4112517b94ef12baad19223229d7963efd0f4109065`.
Development JAR `hict_server-2.0.40-c99eabd-webui_d5c1bc0-fat.jar` SHA-256:
`ac1a8107093f9a8860bdfb95f395837c13999fc077a78d861fdd13619133f664`.
This qualifies the development pair, not older embedded assets or a combined
SFX/native release. Broader sequence/cut/native/performance suites were not rerun.
Live conversion cancellation, larger/public examples, wider wizard workflows,
catalog/session provenance transfer and release/publication gates remain open.
Protected plans/owner files are unchanged; nothing pushed, merged or tagged.


### Stage-weighted conversion progress and live cancellation

Example conversion details carry a title/recipe rather than byte totals. Jobs now
uses numeric conversion progress while unknown-length example downloads stay
indeterminate. The backend previously normalized against only the stages up to the
current one, causing repeated 100% values; it now includes all stage weights.
The focused regression `/tmp/hict-conversion-cancel-focused-01.log` verifies
20%, 20%, 35%, 50%, 50%, 100% across an independent 2/3/5 stage budget. The final
`/tmp/hict-conversion-cancel-jvm-02.log` and XML pass 603 tests with zero
failures/errors and seven optional skips. WebUI
`/tmp/hict-conversion-cancel-web-01.log` build/type/numerical checks and explicit
Prettier pass; the updated browser driver and new process helper pass Black.

Final `/tmp/hict-conversion-cancel-ui-02/results.json`: 26 actual-control cases,
empty errors, exit zero. The optional --live-cancellation Linux check pauses an
actual owned hictk zoomify child using SIGSTOP, identified through /proc by parent,
PID and kernel start time. It does not replace tools, job states, output or process
responses. Pausing makes a normally brief stage observable; the bar correctly
shows the server's numeric fraction below 100%. Clicking Cancel returns HTTP 200
in 23.467 ms, stops the identified child and removes job staging without publishing
an output. The current map remains tiny-fine. Both input hashes remain unchanged:
example.fa `21844eb908519057b7ef6f3dd5774748b6156aa2062ff303522bfb22e968772d`,
example.mcool `b60d964ac3e1d94777da8f9c9b2de8b0c64bf2723c11456262f200b55d9427e4`.
The neutral cancelled row has no alert, error detail or Open result, and its retry
focuses the matching card. A new job completes through the actual bundled tools,
links and exports all 17 reference bases, and preserves the cancelled attempt.
`native-temporary-cleanup.json` independently confirms removal of the exact native
pipeline directory recorded in the stopped process command. The process guard
resumes its owned child on diagnostic failure so normal server cleanup can run.
This qualifies cancellation on this host, not unpaused throughput or other OSes.

`/tmp/hict-conversion-cancel-other-01/browser/results.json` adds ten ordinary
conversion/job-service/FASTA-only AGP cases; failures-01/browser adds six injected
failure-reporting cases. All exit zero, giving 42 browser cases on the frozen pair.
Screenshots were inspected for the partial numeric bar, neutral cancellation,
focused retry and the resulting linked FASTA. Initial ui-01 exposed the actual
backend progress defect and compared a first-rendered value before the next Jobs
poll; it is excluded. The corrected driver waits for normal polling and hashes
only downloaded payloads, since verification legitimately refreshes the manifest.

Frozen dist SHA-256 (sorted relative paths/NUL/bytes, excluding electron):
`90c4afd8b2dafc76c9965963a79cb8236e30ae4e5ba3180c9d516fc55f0a0f1f`.
Development JAR `hict_server-2.0.41-0ef23f9-webui_0b930de-fat.jar` SHA-256:
`8cdad5c0febeaec42a0839e16456ef4a78ca80c593f3d7983b7cf52890361b3e`.
Assets are served separately; this does not qualify combined SFX/native packages.
Broader rendering, sequence, cut, performance and platform suites were not rerun.
Wider wizard/public example qualification, catalog/session provenance transfer,
P8/P9 and remaining release/publication gates remain open. Protected plans and
owner files are unchanged; nothing pushed, merged or tagged.


### Example recipe tracks and recorded source/session provenance

The existing wizard previously omitted openingRecipe.tracks. It now preselects
available declared tracks in order, keeps selections stable through asynchronous
probes and omits unavailable optional files identified by downloadedFiles in the
cached catalog response. A removed entry is not restored by its late real probe.
Screenshot review also found AGP:none after successful wizard AGP loading; the
wizard now updates that status after the successful load.

Source wrappers own frozen, copied example records. Capture associates a declared
ready HDF matrix or a current converted input/output cache. Snapshot/restore retain
these records per source. Restore validates the recorded catalog through its
existing schema, fingerprint, required/pinned file hashes/sizes and source binding;
it does not read the live catalog or execute any recorded recipe/URL. Four new
JUnit tests cover capture, catalog independence, converted cache validity,
malformed/coerced metadata and unrelated maps. Final
`/tmp/hict-provenance-jvm-full-02.log` and XML: 607 tests, zero failures/errors,
seven optional skips. WebUI `/tmp/hict-provenance-web-02.log` build/type/numerical
checks pass; four WebUI files pass Prettier and two Python drivers pass Black.

Final `/tmp/hict-provenance-ui-03/results.json` passes 34 cases with empty errors,
exit zero, including the existing example/cancellation journeys. New cases save
converted-source provenance, download/configure a non-quick-start matrix/FASTA/
AGP/BED recipe, omit a genuinely failed optional track, remove a selection while
its actual probe replies are held, and complete the wizard. Assembly FASTA equals
TACGTACGTACGTACGT, the 17-base reverse complement. Saved metadata remains identical
after live catalog edits; an ordinary replacement has no example metadata. The
saved session restores assembly/track/provenance after catalog removal. A mismatched
record receives HTTP 400 before map replacement. `recipe-input-integrity.json`
independently rehashes all four downloaded payloads and matches their recorded
sizes/hashes after the complete journey. Screenshots were inspected for selected
tracks, omitted optional files, removal, reversed sequence/feature placement,
correct AGP chip and restored state.

Final `/tmp/hict-provenance-tracks-02/{results.json,controls/results.json}` adds
13+8 cases; transactions-02 adds 16; other-01/browser adds ten; failures-01/browser
adds six, all under `/tmp/hict-provenance-`, all exit zero. Total: 87 browser cases.
Expected injected failures remain confined to the failure-reporting suite. Fresh
physical-copy `/tmp/hict-provenance-sources-01/results.json` adds five HTTP cases:
independent Primary/Secondary datasets; restore without catalog; rejected Secondary
binding without replacing either source; detach retaining Primary; ordinary
replacement without inherited attribution. All copied HDF hashes remain unchanged.
The new check_example_provenance_api.py reproduces those cases from the browser
fixture data via --jar, --source-dir and --output.

Incomplete ui-01 used the quick-start button label for a wizard-only example;
the driver now exercises its actual Download/Open result and cached Open in HiCT
flows. Ui-02 passed the earlier pair but screenshot review exposed the AGP header
issue; only ui-03 qualifies the final pair. Transactions-01 lacked the explicit
HDF Python and failed with ModuleNotFoundError:h5py before browser execution;
transactions-02 uses the correct --hdf-python. Incomplete runs are excluded.

Frozen dist SHA-256 (sorted relative paths/NUL/bytes, excluding electron):
`83c4fb7b1a002b1b45e7760a5df2ea87aa0dd2b9663c417116d9f77738cd28fd`.
Development JAR `hict_server-2.0.42-9ea9009-webui_dbf4e75-fat.jar` SHA-256:
`fa72657f5d70a73bbce1ba6c7c4766529d21381dd44d63b66e78468e436f9d62`.
Separately served assets qualify a development pair, not embedded assets or a
combined SFX/native release. The records preserve historical declarations/cache
hashes, not session authenticity or fresh full-file verification on every open.
Broader public-example/wizard/source/format and native/performance qualification,
restored-layout status presentation, provenance viewing/figure metadata and
remaining release/publication packages remain open. Protected plans and owner
files are unchanged; nothing pushed, merged or tagged.


### Opt-in remote BLAST with bounded external-coordinate results

A new fixed-argument BLAST+ remote runner accepts explicit consent, database
scope, at most ten/100,000-base queries, 1–30 minute provider waits and up to 500
subject records per query. Its independent single-worker/two-queued-job pool keeps
local analysis available. Queued cancellation releases capacity without executing
an upload. The process-wide client gate separates starts by ten seconds. Failure,
timeout, cancellation or malformed/oversized output publishes no result action.
The existing local assembly history guard remains intact; remote requests omit
map history headers and do not read linked FASTA. JSON retains external one-based
inclusive coordinates and provider/query/client provenance; full bounded raw TSV
is exported as a Blob through the file-save flow.

Final JVM build/test: `/tmp/hict-remote-blast-jvm-full-08.log`, BUILD SUCCESSFUL;
XML totals 616 tests, zero failures/errors, seven optional skips. Nine new JUnit
cases cover strict consent/settings, argv/private descriptions, gapped external
HSPs, malformed values, owned process results, timeout and cancellation. The WebUI
build/type/numerical and DTO/remote report checks pass in `/tmp/hict-remote-blast-web-10.log`. Screenshot review after
web-09 exposed faint map text through the search surface; web-10 makes that
dialog opaque. The final ui-06 run repeats the remote workflow with a computed
opaque-surface assertion. The broader existing suites ran before this CSS-only
correction.

`/tmp/hict-remote-blast-ui-06/results.json` passes 18 cases with empty errors.
It drives actual menus, forms, Jobs and downloads against the actual JVM handlers,
using an identified controlled external executable, not fabricated job responses.
Coverage includes missing consent, no-map submission, fresh consent after database
change, external coordinates/literal unsafe accessions, bounded paging/filtering,
complete JSON/TSV saves, local default, reopen after map change, ignored late reply,
zero hits, bounded queue and queued cancellation, real local SeqKit during remote
waiting, running cancellation, provider errors, selected one-minute timeout,
exact 16S command alias, malformed results and 20,001 raw rows with 20,000-row JSON.
Cancellation acknowledgement was 47.6 ms on this host, not a cross-platform latency
claim. Generated uploaded FASTA excludes descriptions; source input hashes remain
unchanged. Screenshot review confirms accessible controls at 640×600 and a
scrollable data table, separate query/subject labels, safe literal text, fixed
footer and honest failure/timeout messages.

`/tmp/hict-remote-blast-local-03/summary.json` passes 17 existing local sequence
journeys using the actual bundled tools, including real gapped BLAST, exact IUPAC
strands/BED positions, ModDotPlot, alignment, clipping and clipped-bin parity.
Other-01/browser/results.json passes ten Jobs/FASTA-only AGP cases; failures-01/
browser/results.json passes six failure-reporting cases. All three process exits
are zero. Together these are 51 browser cases. Lifecycle-02/results.json adds two
HTTP/process cases: 33 MiB raw output is rejected and removed; a slow provider
client dies after SIGTERM is sent only to its owning JVM. Group signalling is
explicitly excluded from that assertion. Shutdown took 113.7 ms here.

Earlier incomplete attempts are excluded: the test fixture initially omitted
bothStrands in lifecycle-01; browser setup used an incorrect local FASTA record
name before ui-04. UI findings fixed a filter label association and raw TSV being
parsed as JSON. The initial live 16S attempt exposed a genuine remote-name error,
now fixed as rRNA_typestrains/16S_ribosomal_RNA. An intermediate DTO test loader
needed the real request class to test the new history-header exclusion. Full-04's
log reported success but its observing shell returned 143; final full-08 is the
qualifying run. Local-01 selected a Python environment without Cooler; local-03
uses the HDF environment with Playwright appended to its module path.

Live probes use only the public NCBI NR_024570.1 positions 1–160, independently
fetched and hashed under `/tmp/hict-remote-blast-public-reference-01`. No project
FASTA/contact map is submitted. Live-01 records NCBI's rejected short 16S database
name. Live-02 uses the corrected name and reaches the selected three-minute
provider timeout, killing the owned client without a result or automatic retry.
Live-03 independently probes nt and reaches its selected ten-minute provider
timeout (600.277 seconds), with no result or automatic retry. Its terminal status
is retained under `/tmp/hict-remote-blast-live-03/status.json`; its assertion exits
one because provider success was not established. Live-04 investigates the
corrected 16S connection with NCBI connection-parameter logging enabled only in
the owned test process. Controlled success does not qualify provider success. P8 live-provider and platform checks remain open.

Frozen development JAR `hict_server-2.0.43-2114efe-webui_6bd484b-fat.jar` SHA-256:
`4fdd9f119ae47b77e81102747f7e9e69e5a950bfcb70e48e558f8a615fcf70a2`.
Dist SHA-256 (sorted relative paths/NUL/bytes, excluding electron):
`c85bd1064796149394e19d37d88fffaa5aedcb7cdfbd3c8a9deb4ace5380d6f9`.
The API server and separately served WebUI qualify a development pair, not embedded
assets or a combined native/SFX release. P7/P9 and the remaining release,
performance, scientific and publication gates stay open. Protected plans and owner
files are unchanged; nothing pushed, merged or tagged.


### Reopen exported PNG/SVG figures inside the WebUI

File → Open exported figure opens an independent read-only image viewer. It
uses a remembered browser figure handle when available and the standard local
file input otherwise. Actual-size/fit/zoom controls and a scrollable preview keep
controls available at 640×600. Invalid/empty/oversized files report a recoverable
error without discarding the previous preview. File bytes are limited to 64 MiB;
PNG dimensions are checked before decoding and all decoded images are limited to
128 megapixels. Disposed/obsolete decode replies cannot publish an image, and
published/pending object URLs are released. SVG is loaded only through an image
URL, never inserted as document markup; see
[Mozilla's SVG image-context restrictions](https://developer.mozilla.org/en-US/docs/Web/SVG/Guides/SVG_as_an_image).

`/tmp/hict-figure-viewer-web-02.log` passes build/type/DTO/numerical checks. File
picker regressions cover the figure purpose, remembered handle, cancellation and
read failures while retaining existing session behavior. Four changed WebUI
files pass Prettier; the new Python driver passes Black. Final JVM
`/tmp/hict-figure-viewer-jvm-full-01.log` reports BUILD SUCCESSFUL, with 616 tests,
zero failures/errors and seven optional skips. This increment changes no JVM
production logic.

Final `/tmp/hict-figure-viewer-ui-03/results.json` passes 14 cases with no browser
errors. The driver generates fresh physical inputs, exports actual PNG/SVG from
the map's controls, opens them through the viewer's file chooser and verifies
correct dimensions. SHA-256 of every decoded PNG RGBA pixel matches an independent
Pillow decode. Other cases check fit/actual size/zoom, 640-pixel layout, cancelled
choices, malformed PNG, excessive dimensions rejected before decoding, inert SVG
scripts/external references, Escape with identical primary session state,
controlled browser picker memory/cancellation and a late real decode after close
with its object URL released. Picker boundary stubs return actual exported bytes;
they do not qualify a native OS dialog. Source file hashes remain unchanged.
Screenshots were reviewed for image content, controls, responsive fit and errors.
Other-01/browser/results.json adds ten Jobs/FASTA-only AGP cases and failures-01/
browser/results.json adds six failure cases, giving 30 browser cases in total.

Ui-01 passed the original 11 cases; ui-02 passed 12 before the driver accidentally
returned its injected cancellation function to Playwright, which invoked it.
The injection now uses a block returning undefined. Only ui-03 qualifies all 14
cases on the final pair; earlier incomplete runs are excluded.

Development JAR `hict_server-2.0.44-ec1b976-webui_041569a-fat.jar` SHA-256:
`111f03cab89d18c472dbc829f04b2a1a351d48aece04b121b2d61f0bf7a32376`.
Dist SHA-256 (sorted relative paths/NUL/bytes, excluding electron):
`a2d692302592681d5cf99b0a2dd1a9b46d93468fac83c9be03a6ac64b4228fa6`.
Separately served assets are not combined native/SFX qualification. P9 composition,
immutable cut/scaffold comparisons, ModDotPlot panels, snapshot/session provenance
and release/publication gates remain open. Protected plans and owner files remain
unchanged; nothing pushed, merged or tagged.

Remote-provider follow-up: `/tmp/hict-remote-blast-live-04/status.json` records
the corrected 16S name reaching its ten-minute timeout (600.192 seconds). It
publishes no output and the assertion exits one. Parameter-only diagnostics did
not explain the delay. The kernel disallowed a read-only strace attach to that
owned test child; no tracing or process control was thereby applied. Live-05 uses
NCBI's documented CONN_DEBUG_PRINTOUT=ALL, DIAG_POST_LEVEL=Info and DIAG_TRACE=1
only in the owned test environment, with a selected 30-minute limit. The query
remains the same hashed public 160-base NCBI reference. The trace confirms NCBI
returned a request ID over HTTPS and subsequent provider replies without logged
connection errors. This establishes submission/connectivity, not successful
result delivery. No additional query is submitted while that probe runs, and P8
remains unqualified for live-provider success. Detailed tracing is not enabled in
normal HiCT runs. See the
[NCBI tracing documentation](https://ncbi.github.io/cxx-toolkit/pages/ch_debug).


### Frozen figure export dialog and cross-format provenance

The new dialog replaces the image-export dropdown, captures a preview once and
saves all three formats from that immutable scene. Controls cover viewport,
whole-map/selected rectangle, crop, rulers/FASTA and 1D tracks. Option changes
require refresh. Source/assembly/rendering configuration and client presentation
are compared around rendering; stale/closed captures cannot publish. Existing
direct export methods remain available. SVG metadata stays at the root through
cropping. PNG stores the same JSON in a CRC-checked UTF-8 iTXt chunk; PDF uses
escaped XMP. File identification retains HICT-QUICK-1 rather than calling sampled
fingerprints full-file hashes. Metadata distinguishes requested and rendered
views, records map offset/size, base bounds and CSS-pixel crop geometry.

`/tmp/hict-frozen-figure-web-08.log` passes the full WebUI build/type/numerical
checks, including the new provenance/PNG regression. Its independent Node zlib
CRC check verifies every chunk and that all original image chunks are byte
identical. An independent Pillow/zlib decode of the Unicode golden fixture also
passes. Current source fields preserve IDENTITY_PERCENT with scoreScale=10000.
The canvas recorder's actual-browser scene check passes exclusion/live-widget,
vector label, single root metadata and hook restoration assertions, including
nonpositive/infinite text widths omitted exactly as canvas omits them.
`/tmp/hict-frozen-figure-scene-01.log` records that check. Changed WebUI sources
pass Prettier; new/touched focused Python drivers pass Black and all changed
Python drivers compile. Legacy dense test files are not broadly reformatted.

`/tmp/hict-frozen-figure-jvm-full-01.log` reports BUILD SUCCESSFUL in 52 seconds;
XML totals 616 tests, zero failures/errors, seven optional skips. This increment
changes no JVM production logic. The owned test server now explicitly passes
--serve-webui=false: the CLI's default had overridden the harness environment
setting and caused irrelevant port-8080 bind failures in earlier development
runs. No application bind/CORS behavior or owner process is changed.

Final browser evidence, all under `/tmp/hict-frozen-figure-`:

- ui-08/results.json: ten cases, empty errors, terminal zero. A three-format
  capture has identical metadata. Composition changes, 640×600 controls, fitted
  whole map with exact live-view restoration, selected rectangle base/crop bounds,
  byte-identical old PNG after an exact cut, new assembly/history identity,
  rejection of a real concurrent visualization edit and ignored late provenance
  after close all pass. Source input hashes remain unchanged.
- sequence-07/summary.json: 18 cases, status ok, terminal zero. Seventeen existing
  real-tool/local-sequence/coordinate cases remain green. The added case generates
  an actual ModDotPlot, applies the Hi-C-above/identity-below preset with tracks,
  exports PNG/SVG/PDF through the dialog and verifies matching metadata. The
  legacy fixture primary honestly retains UNKNOWN units; its generated Secondary
  records IDENTITY_PERCENT and scale 10000. Independently rasterized SVG/PDF match
  2,279 flat interior map samples within the declared four-channel-unit limit.
  These samples exclude antialiased text/track edges. The original native HDF
  SHA-256 remains c1a636000d8cf196ca15020604ff387be194c3a6184fe516a87345305a79c555.
- viewer-03/results.json: 14 cases, empty errors, terminal zero. Actual exported
  PNG/SVG reopen with correct image content, full PNG pixel parity, fit/zoom,
  responsive controls, inert SVG and picker/late-decode cleanup.
- failures-02/browser/results.json: six cases, terminal zero. Export failure is
  injected at the new captured-save boundary and appears exactly once inside the
  dialog. Other failed actions retain their existing reporting.
- other-02/browser/results.json: ten Jobs/FASTA-only AGP cases, terminal zero.
- destinations-02/results.json: 14 cases, status ok, terminal zero. All three
  format buttons use the figure save purpose and real controlled writes. Existing
  session, delayed chooser, exact FASTA/AGP, cancellation/failure and folder-memory
  checks remain green. Total: 72 Chromium cases.

Screenshot review covers actual map/annotation content, small-window controls,
options awaiting refresh, exact cut, stale capture, cancellation and a readable
whole-map image. Before this change a small whole-map figure was 17×17 pixels;
the fitted map-only fixture is now 533×560 while retaining its native signal.
The live view is exactly restored; export resizing no longer reclamps its panels.

Excluded intermediate attempts: web-01 lacked Array.from for the project's DOM
iterator target; ui-01 exposed nested crop metadata; ui-02 exposed negative SVG
textLength from hidden panels. Sequence-01 assumed the direct fixture converter
had declared contact units; it is a legacy UNKNOWN source, and the test now checks
that exact fact. Sequence-02 sampled antialiased annotation edges in the map-color
oracle; the oracle now checks flat interior map pixels with a separate vector
label test. Sequence-03 exposed a stale workspace write during an administrative
fixture reset; the driver now navigates its old client away before replacing the
server state and retains strict error checks. Ui-04 wrapped an API visualization
body incorrectly; the real endpoint uses a flat DTO. Ui-07 exposed live panel
clamping and restoration during export, fixed in the workspace observer and
fitted capture path. Earlier passing subsets do not qualify the final pair.

Final development JAR hict_server-2.0.45-c1c00fd-webui_6493d85-fat.jar SHA-256:
`57ec318f8866e3604d54d846b33abedea472875fd6516a5da9d7ccbaa425f425`.
Frozen dist SHA-256 (sorted relative paths/NUL/bytes, excluding electron):
`85031445adad3cdbddee1ac78b275d08de63f9258d4ed463765805bc8c1c8050`.
This is a development API/static-assets pair, not embedded/SFX/native release
qualification. Protected plans and owner files remain unchanged; nothing pushed,
merged or tagged.

Expected/O/E follow-up is not counted among those 72 passing cases. The separate
`/tmp/hict-expected-figure-audit-02/results.json` records requests for a native-4-bp input
profile at rows 1152–1853/columns 1345–2071, followed during whole capture by an
intermediate window, full 0–3005 window, then the original window on restoration.
Those refreshes are asynchronous and not part of ordinary visualization-option
comparison. The probe also contains initial default-connection failures because
it omitted the standard route before opening; it is an audit trace, not a green
browser acceptance run. Audit-01 used the tiny filename for the mixed fixture
and failed before opening, so it is excluded. A pinned/recorded fitting context
and runtime parity/rollback across scopes are a required next scientific capture
gate. Stored cut/scaffold snapshots, comparison/session persistence, publication
figures and remaining release gates also stay open.

NCBI follow-up: live-05's real bundled client reaches its selected thirty-minute
limit (1800.248 seconds), kills its owned client and publishes no output. Its
assertion exits one. A read of the already accepted public RID FR9P7TGF016 after
client exit records Status=WAITING / Searching after 31 minutes; a later read at
06:34 local still records WAITING. Files public-rid-status*.txt/json under that
probe directory preserve the NCBI response and requested URLs/times. Neither GET
submits a new query. This isolates accepted submission/provider waiting from
HiCT transport failure but does not prove result delivery; P8 live qualification
remains open. Only the hashed public 160-base reference is used, never a project
sequence. No automatic resubmission or provider-latency claim is made.

## Pinned Expected/O/E figure rendering contexts

Final qualification uses JVM JAR
`build/libs/hict_server-2.0.46-1e4e101-webui_eb6caf7-fat.jar`, SHA-256
`8323e9544c50e6172cb3b30f1db77339c6eca8651b5ca3d95ec4d46eb7eefe80`,
and separately served WebUI dist SHA-256 (sorted relative paths/NUL/bytes,
excluding electron)
`7f074ed540b094ad9aa9d7195de651d78697b9936e57daef2061352a60c23fe3`.
The packaged OpenAPI bytes equal the current API source. Browser harnesses
snapshot the JAR; no dist was rebuilt under a live acceptance browser. This
qualifies the development API/assets pair only, not an embedded/native/SFX
release or a cross-platform package.

- `/tmp/hict-render-capture-jvm-full-04.log`: `gradlew test shadowJar -x
  copyWebUI --offline`, BUILD SUCCESSFUL in 52s; 623 tests, zero failures/errors,
  seven optional skips. New registry tests cover expiry/release, permanent
  identity/generation invalidation, budgets, independent metadata and actual
  means/domain/grid fingerprints. HTTP tests compare pinned Expected/O/E tile
  images byte-for-byte with a full-window reference while replacing the live
  viewport profile, including native-BINS sequence expansion. Wrong-grid tiles
  poison final validation; release, rendering changes, context count and native
  source SHA preservation are checked. Fit-budget tests reject oversized square
  and thin windows without integer overflow. Existing OpenAPI coverage passes.
- `/tmp/hict-render-capture-web-05.log`: full `npm run build:web`, terminal zero,
  including type/DTO/numerical and source lifecycle checks. The lifecycle fixture
  now initializes its real WeakMap field and proves failed tiles affect only the
  active capture. Explicit changed-file Prettier, focused Python Black/compile
  and both repository diff whitespace checks pass.
- `/tmp/hict-render-capture-expected-07/results.json`: seventeen cases, empty
  unexpected errors, terminal zero. Real export-dialog controls cover viewport,
  whole-map and rectangle in Expected/O/E; shared-reference sources with distinct
  native grids; native-BINS fitting during sequence-level zoom; active prepared
  SQRT_COVERAGE algorithm/weights; real rendering and weights edits invalidating
  drawing; a controlled failed tile rejecting promptly; late-create cleanup.
  Camera, context namespace, interactions, release and unchanged primary source
  SHA-256 `59c272d9e10bdcab891102911c5c89e8be4d5dc27b94a960d4a82c08c1a9120a`
  are checked. Only intentional validate-409 and the exact injected tile-503
  responses are allowed; an unexpected tile-409 fails the driver. Temporary
  whole-map layouts do not refit the shared viewport profile. Actual SVG figures,
  screenshots and bounded render diagnostics are retained in that directory.
- `/tmp/hict-render-capture-frozen-05/results.json`: ten cases, empty errors,
  terminal zero. Shared PNG/SVG/PDF scientific metadata, fixed captured bytes,
  composition, 640-pixel controls, exact cut, stale input and close are rechecked.
- `/tmp/hict-render-capture-sequence-03/summary.json`: eighteen cases, status ok,
  terminal zero. Real native sequence tools, actual generated ModDotPlot with
  tracks, exact coordinates, selection exports and assembly reattachment pass.
  `identity-figure-validation.json` retains three-format metadata and 2,279 flat
  interior map colors independently rasterized from SVG/PDF, within four channel
  units. Native SHA remains
  `87c6b9c6de9ab5c8397eb3c06145d524a9aa21378b0f1ce586048605b9d97a20`.
- `/tmp/hict-render-capture-viewer-01/results.json`: fourteen cases, empty errors,
  terminal zero. Actual captured PNG/SVG reopening, full PNG pixel equality,
  fit/zoom, responsive controls, inert SVG and chooser/late-decode cleanup pass.
  Total: 59 final Chromium cases. Expected/O/E dialog cases exercise SVG capture;
  the shared three-format encoders are rechecked with Observed and real identity
  captures. Font/annotation-edge antialiasing is not claimed pixel-identical.

Screenshots were inspected for whole-map and sequence/coverage content, complete
previews, controlled failure messaging and existing responsive figure controls.
The sparse mixed fixture contains mostly empty contacts; blank off-diagonal
regions do not establish a rendering defect. Scientific Expected fitting retains
its existing unique unordered source-pair support and display-bin distance.
The context fingerprint hashes actual profile means/domains, not merely a
requested rectangle. Effective normalization is recorded independently of
ordinary visualization option equality.

Retained limits: four contexts, 64 MiB profile/metadata budget, five-minute lease
checked on registry access, one active fit, default 16,777,216 cells and at most
65,536 native bins per axis. Larger fits ask for a coarser view without silently
changing the grid. Contexts release on all normal completion/failure/close paths;
lost replies rely on bounded expiry. This does not establish whole-genome latency
budgets on every supported machine. Stored cut/scaffold comparisons, session
persistence, publication artifacts and wider release gates remain open.

Excluded intermediates: focused-02 omitted newly used JUnit static imports;
web-02's prototype-only lifecycle fixture lacked the new WeakMap initializer.
Expected-01 passed the early seven-case subset before later fixes. Expected-02/03
exposed the shared overview sending coarser tile requests under the primary
profile. Expected-04 reached its functional assertions but its synthetic 503
response omitted the exposed cancellation header, causing a strict browser
console failure; the controlled response now carries that header. Expected-05
exposed a repeated mixed-grid projected-overlay render timeout; expected-06 was
timing-dependent and is superseded by the explicit capture overlay promotion.
Earlier frozen/sequence subsets and pre-budget builds do not qualify the final
pair. Final evidence is only the runs named above.

A read-only follow-up of public NCBI RID FR9P7TGF016 at
2026-11-17 03:31:05 UTC (07:31 local) still records WAITING in
`/tmp/hict-remote-blast-live-05/public-rid-status-after-figure-capture.{txt,json}`.
It submits no new query and uploads no project sequence. P8 successful live
result delivery remains unqualified. Protected plans and owner files are
unchanged; nothing pushed, merged or tagged.
