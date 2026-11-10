# Reinforcement implementation evidence

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
