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
