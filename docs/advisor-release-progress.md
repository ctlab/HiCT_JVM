# Advisor release implementation ledger

This ledger tracks implementation of the 2026-11-16 advisor/release plan agreed
in the owner conversation. Historical W/U/R sections in `plan-reinforce-final.md`
describe earlier releases; they must not override the decisions below.

## Owner decisions

- All 22 advisor requests are publication-release requirements.
- Preserve Dev accessibility, OSD design, wizard structure and bind/CORS behavior.
  Network deployment remains qualified for trusted environments.
- Exact cuts retain all sequence and source contacts. Children share crossing
  source bins through ATUs, with exact clipped FASTA intervals and no cut gap.
- Approximate Cooler export is opt-in, labeled and conservation-tested. Native
  contacts stay unchanged; shared references are not new contact observations.
- Target an Application Note. The supplied manuscript is read-only.
- Keep protected files and pre-existing dirty documents untouched. Commit by
  explicit paths per verified increment. The owner explicitly authorized pushes
  and cancellation of superseded Actions runs on 2026-11-17; check CI before
  each commit/push and monitor the replacement runs. Never merge or manually
  create release tags.

## Execution and coverage

| Package | Advisor items | Status |
|---|---|---|
| P1 Exact cuts, clipped bin geometry, sessions and exports | 21 | In progress: exact API/scissors/feature-edge actions, source synchronization, v4 replay, long offsets, approximate Cooler export and main-map base scale implemented; primary FASTA/BED6/GTF/GFF3 cuts, wheel/overview/viewport exports/session workflows verified; broader tracks/secondary/selection coverage and release qualification pending |
| P2 Entity/region selection, coordinates, FASTA measurement | 2, 7, 13, 17 | In progress: entity/region gestures, coordinates, scoped borders and FASTA interval/repeat measurement implemented and primary Chromium verified; platform, secondary-source, accessibility and large-map qualification pending |
| P3 File destinations, saved/cancelled/failed results, sessions | 4, 6, 15 | In progress: shared save outcomes, confirmed sessions, frozen revision-guarded FASTA/AGP, browser/Electron/Tauri request streaming, per-purpose file memory and File Examples implemented; final version-68 Linux Electron/Tauri payloads pass native coordinate/export, cancellation, lifecycle and recovery cases; download-only browsers, other platforms and wider chooser coverage pending |
| P4 Tracks/sources separation, feature actions, resize | 9, 10, 11 | In progress: Center/Zoom and copied coordinates corrected; base-grid features/signals include hidden/binless sequence with native-weight provenance, browser verified; 1D/source tabs, file/map-weight Add choices, guarded editing and aligned resizing implemented; final acceptance is recorded below, with broad formats, platforms and performance pending |
| P5 Alignment runs/full cells and FASTA navigation | 5, 8 | In progress: continuous exact-boundary lines, short-repeat profile, source-checked CIGARs and vector exports implemented and primary Chromium verified; broader source/platform/performance qualification pending |
| P6 O/E controls, coverage explanation, viewport histogram | 19, 20, 22 | In progress: coverage help, O/E colors/alpha, persistence, presets, active primary O/E editor, ratio nodes, live thresholds, viewport histogram/levels, exact derived-view limits, normalization draft protection and draggable ordinary color midpoint implemented and Chromium verified; broader source/NaN, native/performance and final qualification pending |
| P7 Jobs/examples, FASTA-only, OSD and diagonal provenance | 12, 14, 16 | In progress: Jobs-first downloads/conversions, guarded opening, cache integrity, live cancellation/retry, Info and yeast diagonal provenance verified; full recipe prefilling and per-source session provenance implemented and verified; broader wizard/public examples and release qualification pending |
| P8 Opt-in remote BLAST with external-subject results | 18 | In progress: consent, independent Jobs, external-coordinate viewer and bounded exports implemented; controlled submission/lifecycle checks and live known-RID recovery pass, including ten public-reference hits, map-independent delivery and exports; automatic recovery without a known RID and platform/package qualification pending |
| P9 Export viewer, immutable cut/scaffold figure snapshots | 3, 11 | Implementation and primary publication artifact verified: PNG/SVG viewer, three-format captures, fitting contexts, portable snapshots, column sizes and compact grids; the real public-data figure has source checks, caption/alt text, 7.05-pt text, a zero-raster vector PDF/SVG and a verified 900-dpi TIFF; overall release/platform qualification remains open |
| P10 Launcher Stop returns to simple start view | 1 | Implemented; Linux private-display acceptance passed; wrapper/platform matrix pending |

P10 is independent and is being completed before the coordinated P1 changes.
No remaining package or platform is implicitly qualified by this ordering.

## Audit baseline

JVM `cd4f6540`, WebUI `f9d48ce`: 519 JVM tests with no failures/errors and
seven optional skips; WebUI build passed. Chromium exercised 18 menu destinations,
16 sequence lifecycle cases, and 20 zoom checks. Medium-map navigation p95 was
16.7–16.8 ms per frame and 29–31 ms input acknowledgement on the audit host.
FASTA-only AGP matched all 19 Borrelia records. Yeast raw 200x200 windows at
50/100 bp matched the source Cooler exactly, including its empty diagonal.
Evidence remains under `/tmp/hict-plan-audit`; archive release evidence before
claiming final qualification. Native and cross-platform GUI coverage is pending.

## P1 coordinate prerequisite

Assembly responses now expose sparse per-resolution source-forward bin origins.
Legacy descriptors carry `contigBinOffsets`; compact-v1 carries the corresponding
map by contig ID while preserving its row shape. The WebUI uses these origins
for native-level bp/bin/pixel conversion and fractional interpolation. Older
servers and synthetic guidance levels retain their previous mapping path.
Focused browser evidence: `/tmp/hict-bin-geometry-focused/fragment-geometry.json`.
All bases of the independent 4 kb fixture were checked against enumerated source
intervals at 100/200 bp after a cut and reversal. That prerequisite alone did not
change cut semantics. Exact operations and their main-map integration are
described below; legacy destructive split replay remains readable.

## P1 exact library cuts and sessions

`ScaffoldingOperations.splitContigAtBp` cuts an oriented contig at an exact base
boundary. Each child's ATUs retain all intersecting original bins; crossing bins
are shared, without rewriting contacts. Sequence intervals meet exactly, and
one-base fragments are retained. The operation is atomic and checkpoints preserve
its typed exact-cut lineage. New snapshot v2/session v4 captures support exact
replay; older snapshots and bin-removing edits remain readable. Historical
snapshots missing the recently added bin-origin fields also restore correctly.

Validated: 523 JVM tests (zero failures/errors, seven optional skips), WebUI build,
18 existing browser session checks, and a new browser test restoring two exact
cuts plus reversal, checking every base's pixel coordinates and gap-free FASTA,
then saving/closing/reopening through actual session controls. Evidence:
`/tmp/hict-exact-cut-ui/results.json` and
`/tmp/hict-exact-cut-session/snapshots/results.json`.

The guarded `/split_contig_at_bp` API and WebUI request adapter now require the
displayed history session and revision. Source synchronization retains every
intersecting immutable bin across different resolution pyramids, including late
attachment and switching assembly authority. Session v2 snapshots preserve this
policy through optional descriptor flags, even for projected sources without
their own cut history. Coverage preparation already scans unique stored contacts;
a regression proves its weights remain identical after repeated exact cuts.

Numeric queries now identify source-reference versus pipeline display-grid
semantics. Optional JSON provenance exposes compact axis runs keyed by source,
resolution, stripe and bin index, with counts of referenced and unique physical
bins. Padding has no physical identity. Statistics explicitly count display cells.
This metadata leaves raw values unchanged. Expected fitting now assigns one unit
of support per unordered physical source-bin pair across all its displayed aliases
in the fitting window, including both symmetric orientations. Chunking does not
reset support. Primary numeric fallbacks and raster paths use the same policy;
BINS queries include hidden contigs and fit their own coordinate axis. Histogram
counts intentionally describe displayed cells for contrast fitting, including
arbitrary pipeline outputs, and are labeled accordingly. See
[the method and limits](shared-bin-statistics.md). Chromium verified four fitting
windows against an independent enumeration with zero cell error, O/E controls,
visible histogram labels and unchanged native bytes:
`/tmp/hict-shared-expected-qualified/results.json`. Large-map performance remains
unqualified; primary sequence-scale geometry is covered by the later workflow below.

Explicit approximate Cooler export is implemented through the library, CLI,
single/batch jobs and the matrix export dialog. It scans original unordered
contacts once, redistributes by sequence-overlap fractions, writes labeled
unbalanced float64 estimates and verifies retained-contact conservation. Diagonal
contacts include both exchangeable endpoint orders. Gaps expose sequence coverage;
omitted sequence is not renormalized. Native data is unchanged. Bounded caches,
external sorting and atomic publication cover cancellation/failure. Ordinary
export does not silently enable this policy, and estimated exports do not enter
the ordinary conversion cache. See [the method and limitations](approximate-cooler-export.md).

Chromium verified explicit consent, restored controls, Jobs delivery/download and
batch export. Independent Python Cooler/base-endpoint oracles matched every
exported cell at 4/10 bp and retained the total 22. Evidence:
`/tmp/hict-approximate-export-qualified/results.json`. Multi-stripe, all-interior-cut,
both-strand, gap, partial-retention, empty-output and cleanup tests also pass.
Reimport now retains fractional cells, original estimation AGP/level metadata and
an optional capabilities declaration. Primary/secondary chips label the data as
estimated, including after session restoration. Preparation restores provenance
from the original input; sequence-derived overlays do not inherit a contact
template's estimation label. Direct `.mcool` reimport is browser/Cooler verified;
the original-input preparation bridge has a single-Cooler-root regression, with
tool-backed preparation qualification still pending. A subsequent approximate export records already-
estimated input. Ordinary float re-export now preserves stored float64 pixels
without another estimation step; mixed integer levels remain int64. Browser Jobs
and packaged CLI outputs match independent Cooler reads exactly. Existing outputs
survive failed/cancelled exports. Optional tool-backed balancing, large-map
performance and final native/package qualification remain pending. Each stored resolution is estimated independently,
not asserted to be an exact aggregation of the finest estimated level.

A bounded sequence-grid rendering primitive now draws native cell colors at one
base per output pixel. It clips short/reversed fragments to their retained
sequence widths, includes hidden native bins, leaves absent sequence contacts
transparent and preserves base-grid upper/lower branches and Swap. Stored contact
resolution remains explicit; no new counts are interpolated. Native pipeline
bounds now accommodate shared assembly references, and numeric BINS track inputs
retain hidden-bin weights/features. Chromium decoded every PNG channel for both
sources and two levels with standard/graph renderers, with zero error and no
page errors: `/tmp/hict-sequence-grid-qualified/results.json`. See
[the rendering contract](sequence-scale-rendering.md).

The main WebUI now switches to a uniform base axis on capable servers, with an
explicit native-bin status, separate native coordinate snapshot and bounded
sequence-grid tiles. Real scissors use the guarded exact-cut endpoint. Native
raster extents grow with shared references. Rulers, FASTA and BED6 feature positions,
wheel pointer anchors, overview projection/clicks, BP saved views, keyboard history,
viewport PNG/SVG/PDF and portable session restoration passed the deterministic
4 kb primary-source browser workflow. Eight wheel steps reached readable FASTA
letters without changing the base under the pointer. All 4,000 IUPAC bases remain
in their exact reverse-complement order and the source file hash is unchanged.
Evidence: `/tmp/hict-sequence-view-browser-19/results.json` (17 checks, no page
errors). PNG and PDF output were visually inspected. BINS and PIXELS expected
profiles now have independent caches; both fit-window isolation probes match an
independent oracle exactly: `/tmp/hict-shared-expected-bins-browser-03/results.json`.

This is a qualified primary workflow, not completion of P1. The full
secondary/disjoint-pyramid UI matrix, selection/whole-map
exports, saved-bookmark migration, large-map latency and platform/package gates
remain open. The browser served the current WebUI distribution separately from
the JVM artifact; final embedded-package qualification is still required.

Source offsets now use
longs throughout descriptors, cut lineage, AGP slices, FASTA calls and responses.
Portable sessions also validate the complete source interval against the maximum
exact JavaScript integer. A tiny contact fixture with a declared 3 Gb contig
validates nested cuts past 2.5 Gb, both-strand historical cut replay, AGP slices,
compact responses and actual session save/close/reopen. Large-FASTA I/O remains
unqualified. Native-level displays retain reference bins; sequence-scale displays
clip short fragments to their retained base widths. P1 is not complete.

Primary GTF/GFF3 feature menus now cut at projected start/end boundaries, with
session/revision snapshots and revalidation. Existing joins are disabled, and
undo makes a captured target stale without sending a cut. Cuts preserve the
visible BP locus at native and base scales. Center keeps the current zoom;
Zoom reaches readable letters at the same feature across a grid transition.
Copied coordinates use ungrouped digits (the old commas came from explicit
`toLocaleString("en-US")` calls). Real clipboard output after a cut is
`tiny_3_4000:1031-1063`, one-based inclusive. Evidence:
`/tmp/hict-feature-actions-browser-02/results.json` (15 checks, no page errors).
All 4,000 IUPAC bases and native bytes remain unchanged. The existing 17-case
sequence-view/export/session workflow also passes:
`/tmp/hict-feature-actions-sequence-browser/results.json`.
Broad track/source, entity navigation, layout and packaged release gates remain
open. A sidebar control overflow seen in the feature-zoom screenshot belongs in
the pending layout work; this increment does not qualify every responsive layout.

## Base-grid track qualification

Base-grid track queries now project features and quantitative intervals directly
in assembly BP, including hidden/binless components, on servers advertising the
new optional capability. Native-sidecar axes are not reused. Weight tracks retain
native values/resolution and missing support through cuts, reversal and coarse
max aggregation; normalization availability uses the stored level. Per-base
BED/GTF/indexed-BAM/weights tests, disjoint secondary weights and >2.5 Gb positions
pass. Chromium verifies a physical binless/hidden/visible fixture, real hidden
feature cutting, values/tooltips, normalization availability and session restore:
`/tmp/hict-base-tracks-browser-04/results.json` (13 checks, no page errors).
Native bytes are unchanged. BigWig/VCF, all aggregation modes, large-file latency,
the broader secondary UI matrix, layout and package/platform gates remain open.

## Independent selections and exact coordinate entry

Shift-click/drag selects contigs or scaffolds for editing, chosen in the Selection
menu. Its rubber band disappears on release and stroke-only entity highlights
remain. Selected IDs survive viewport feature rebuilds; structural changes and
scope changes clear transient selections. Ctrl-drag (Command on macOS) creates a
persistent analysis region, independently of entity selection. Clicking inside
keeps it; clicking outside or Escape clears it. Region context menus no longer
offer structural edits, and region selection cannot reverse/group entities.

Coordinate entry accepts assembly positions and current-contig offsets, including
names containing colons. One-based inclusive and BED conventions are explicit.
Invalid, ambiguous, unsafe and out-of-scope entries preserve the prior selection;
an assembly/scope change invalidates an already-open dialog. BP endpoints remain
authoritative through native/base projection and FASTA downloads. Both narrowed
axes enable ordinary border hiding while selected borders and ruler names remain.

Full WebUI build/regressions and 574 JVM tests pass (zero failures/errors, seven
optional skips). Seven primary Chromium scenarios verify real gestures, both
entity kinds, exact/scoped coordinates, independent FASTA sequence contents,
stale-dialog rejection and selection-free image exports with restored selection.
Evidence: `/tmp/hict-selection-browser-06/results.json`. Existing sequence-view
(17), feature-action (15) and hidden/binless-track (13) workflows also pass:
`/tmp/hict-selection-{view,feature,tracks}-browser-01/results.json`.
Native files remain unchanged and no page errors were recorded. Regression runs
precede a final label-only pluralization/import cleanup; the seven selection
scenarios are repeated on the final build. FASTA-track repeat measurement is
implemented below. macOS/native modifier behavior, full secondary-source/UI
coverage, large-map selection latency and embedded/platform qualification remain
open. P2 is not complete.

## FASTA interval and repeat measurement

Ctrl/Command-drag on either FASTA strip selects whole letters without panning.
A context-menu selection mode permits ordinary dragging, and Shift-arrows extend
within visible bases. A single shared card shows exact length/assembly coordinates,
copies forward or reverse-complement sequence and submits an explicit repeat job.
Escape cancels an incomplete drag or clears the card. Navigation preserves it;
assembly/scope/FASTA changes clear it. Neither sessions nor image exports include
transient measurement overlays.

The new bounded `/sequence/repeat_period` endpoint captures the gapless interval
under the history guard and runs ungapped autocorrelation on the existing sequence
worker. Results appear in the card and Jobs; cancellation, stale input, queue
limits and late-response ownership follow the same job infrastructure. Exact
periodicity and imperfect candidates have distinct explanations. Ambiguous bases
never match and indels are not modeled. The method and thresholds are documented
in [repeat-measurement.md](repeat-measurement.md).

Library/DTO and real HTTP tests cover known repeats, a substitution, ambiguity,
safe coordinates, deterministic queued cancellation, frozen input after edits,
stale headers, summary publication and unchanged source bytes. Full JVM checks
pass with 581 tests, zero failures/errors and seven optional skips; full WebUI
build/regressions pass. Twelve Chromium measurement scenarios pass, including
input checksums and actual session restore:
`/tmp/hict-repeat-final-browser/results.json`. Existing selection/map/feature/track
workflows also pass (52 checks):
`/tmp/hict-repeat-final-{selection,view,feature,tracks}-browser/results.json`.
The measurement card fits the inspected 1024×768 viewport; that does
not qualify the surrounding application's known responsive-layout issues.
Native platform/modifier, secondary-source, accessibility and large-map/queue
performance qualification remain open. P2 and the publication release are not complete.

## Continuous alignment preview

Alignment geometry now uses continuous base-boundary runs instead of isolated
per-base icons, preserving forward/reverse direction, substitutions, ambiguity
and indels in the map and vector exports. Tiny windows use an explicit short-seed
profile, while larger windows retain the previous seeded settings. The backend
already parsed all PAF records; short-window seeding was why the advisor's CT pair
returned no hit. The corrected pair yields all ten independently enumerated
maximal exact diagonals. Source/CIGAR checks reject inconsistent canonical labels
on both strands. Results declare profile, input/tool checksums and snapshot.

Navigation cancels known jobs immediately, and late initial submissions retain
their job IDs long enough to cancel obsolete work. Bounded transport and generation
guards cover delayed responses. Grid metadata forces correct reprojection.
Method and limits: [alignment-preview.md](alignment-preview.md).

The full JVM suite passes with 583 tests, zero failures/errors and seven optional
skips; full WebUI checks pass. Real Chromium checks cover CT repeats, substitutions,
short/long indels, reverse complements, ambiguity/no-hit, line coordinates, zoom,
PNG/SVG/PDF, exact fragments, reversal, undo/redo and scoped session restore.
Evidence: `/tmp/hict-alignment-qualified-browser/results.json` (12 checks).
The existing measurement/map/selection/track/feature workflows also pass (64):
`/tmp/hict-alignment-qualified-{repeat,view,selection,tracks,feature}-browser/results.json`.
Standalone SVG/PDF outputs were rendered and visually inspected; PNG/PDF orange
substitution bounds agree within one pixel in the inspected fixture. Broader
source/platform, accessibility, dense-window latency and embedded-package gates
remain open. P5 and the publication release are not complete.

## P3 destinations and confirmed session saves

Sessions, assembly/selection FASTA, AGP, figures and local settings exports share
explicit saved/cancelled/download-started outcomes. Browser pickers retain click
activation and do not fall back after denial. Desktop adapters remember separate
successful folders by purpose and stage disk writes before replacement. Session
state becomes saved only after confirmed completion of the captured revision;
later edits remain dirty. Save-from-close failures stay in the original dialog.
Download-only browsers receive honest completion guidance and retain the guard.
File exposes Open Examples; session opening and the success action use existing
validated restore. Server input selectors remember/preselect previous files by
server and purpose without opening them automatically.

FASTA/AGP exports now capture immutable metadata on the assembly queue under the
displayed-revision guard, then perform I/O on the export worker. A real HTTP
regression exposed that EXPORT-priority requests previously bypassed the assembly
interceptor. The two-phase dispatch fixes this; cancellation returns 409 instead
of an empty successful file. Frozen export tests retain the original sequence
after exact cuts/reversal. Method and limits: [file-destinations.md](file-destinations.md).

Full JVM checks pass with 585 tests, zero failures/errors and seven optional skips;
full WebUI build/regressions and Electron compilation pass. Ninety Chromium
checks pass on the final WebUI build, including 14 new file workflows and all 76
existing alignment/measurement/map/selection/track/feature checks. File workflows
use controlled picker outcomes; other suites use a controlled writer producing
real downloadable output. These do not qualify OS dialogs. Electron's actual IPC
handler passes real-disk failure/replacement tests; the actual Tauri filesystem
routine passes an isolated Rust test. Full Tauri compilation is blocked by host
Rust 1.75 versus lockfile v4/project Rust 1.88 requirements. Native GUI/platform
qualification, bounded-memory large exports, remaining chooser/secondary-source
routes, accessibility, restart and large-directory coverage remain open. P3 and
the publication release are not complete.

## Browser streaming qualification

FASTA/AGP browser destinations now consume a fetch byte stream with one awaited
write per chunk, avoiding the old complete-response ArrayBuffer/Blob. The export
notice owns preparation/transfer cancellation and reports one final outcome in
the notification journal. Stale/failed HTTP responses are rejected before writer
creation; read/write/close failures cancel the response and abort the writer.
Desktop and download-only compatibility paths remain buffered and unqualified
for genome-scale memory use.

An actual Chromium private-filesystem writer in a persistent profile saves a
512 Mb synthetic assembly (518,400,010 FASTA bytes), verified against an independent
sequence checksum. Cancellation and injected write failure abort the real writer
and retain the preceding file, independently downloaded and hashed after each.
The chooser alone is substituted and the backend response is fetched directly.
The final run's frame p95 is 16.7 ms and pointer acknowledgement p95 is 13.2 ms;
sampled total Chromium RSS grows by 172.5 MB (peak 717.0 MB). These measurements
cover this sparse-map transfer workload on the declared Linux host, not the full
multi-track/large-map release workload. Evidence:
`/tmp/hict-stream-browser-09/results.json`. The normal file workflow passes again
on this build. See the detailed validation record for all regression evidence.

The runtime notification integration uses the installed library's single promise
lifecycle. Reused toast IDs and manual dismiss/replacement caused a rendering
loop during development and are excluded by focused tests and real UI checks.
P3 still needs desktop/download streaming, OS-dialog/platform, wider source/input
and release-package qualification. No publication-release claim is made.

## Electron native request transfers

Current Electron FASTA/AGP exports carry only the captured request through IPC.
The main process opens a native destination chooser, streams guarded HTTP bytes
to a staged file, commits by rename and updates per-purpose destination memory.
One operation belongs to each window; cancellation, navigation, renderer loss and
graceful quit clean up owned staging. Quit waits for active transfer cleanup.
Filesystem errors provide useful text without IPC internals. Streamed results
advertise no browser readback; Blob-backed session readback stays available.

The built Linux Electron 41.10.7 payload passes real GTK dialog acceptance on an
isolated X display: 512 Mb full FASTA, exact selected FASTA, AGP, native/dialog
cancellation, disk-permission failure, confirmed/cancelled session save/readback,
navigation cancellation and last-window shutdown. The sequence checksum matches
the independent reference. Native download requests are absent from the renderer;
sampled main-process RSS stays well below the file size. The normal 90 browser
scenarios and full JVM/WebUI checks also pass. Exact evidence/hashes are recorded
in [reinforcement-validation.md](reinforcement-validation.md).

P3 remains in progress. Tauri still needs streaming and its Rust 1.88/WebKit SDK;
download-only browsers still buffer exports. Other platforms, portals/Wayland,
legacy-Blob shutdown/crash durability, source/chooser/accessibility coverage and
final combined-SFX qualification remain open. The packaged offline-start defect identified during that inspection is now fixed
and qualified below.

## Electron bundled recovery

The portable and development layouts now locate the bundled public interface.
If the hosted page is refused or returns an HTTP error, an owned read-only
127.0.0.1 listener serves it with the existing CSP and a small Reconnect notice.
The wrapper revalidates the hosted entry page, including after an online visit,
so a cached success cannot hide offline recovery. Reconnect uses the existing
save/discard/cancel guard and cannot be requested by an online page.

Real Linux Electron 41.10.7 acceptance covers offline startup, repeated refusal,
HTTP 503, live backend attachment and edits, cancelled/approved reconnection,
return online, offline restart preferences, explicit reset and the notice at
1024 px. No unexpected page errors or dialogs occur. Native exports and the
90 existing browser cases also pass on the final build. Evidence and exact
artifact identities are in [reinforcement-validation.md](reinforcement-validation.md).

The listener claims its port before navigation and releases it on quit. A busy
preferred port gets a newly owned port, which changes the browser storage origin;
read-only profiles also cannot guarantee a stable origin. Normal writable-profile
restarts retain it. Host/path/symlink checks and occupied-port behavior have focused
regressions. JVM bind/CORS, Electron file fuses and renderer isolation are unchanged.
Tauri, other platforms, Wayland/portals, wider accessibility and input coverage,
soak/performance and final combined-SFX qualification remain open. P3 and the full
publication release remain in progress.

## Tracks, sources and live weights

The manager now separates **1D tracks** from **Sources and layers**. Add offers a
file or map weights from either source. Weights identify their active file or
square-root coverage origin, preserve unknown-provenance legacy access, and
explain known unavailability. Per-track signal controls and preparation are
optional disclosures. The layer action opens the existing sidebar with focus.
The [user guide](tracks-and-sources.md) describes the workflow and keyboard access.

Names and numbers retain their drafts across refreshes, and late list responses
cannot undo newer edits or reset local log settings. Resize completion refreshes
the grid; track frames no longer scale the coordinate surface. Keyboard resizing,
workspace shrink and lost-focus cleanup share the same size constraints.

Normalization acceptance exposed a native-track defect: weight precompute could
retain the preceding normalization's values. These weights are already resident
in the map and now bypass persistent precompute. An independent numerical
regression covers both sources, both mode transitions, masked bins and unchanged
source bytes. Optional preparation explains that map weights need no preparation.

P4 remains in progress. The detailed validation record identifies accepted browser
workflows and exact artifacts; wider real-file, platform, accessibility, dense-map
and soak qualification remain open. At 800 px after explicitly opening the
sidebar, main-workspace controls still crop; responsive sidebar/ribbon work is
a recorded follow-up. The manager modal itself fits that width. The full publication release is not complete.


## Responsive workspace follow-up

The recorded 800 px clipping is resolved. File badges wrap without displacing
menus/Jobs, all seven dropdowns stay inside narrow windows, and signal controls
fit the sidebar. Below 700 px the stacked panel keeps its content height and
scrolls through the workspace. Explicit panel choices remain saved.

Final WebUI checks pass. Chromium passes 145 scenarios, including six window
sizes from 1440 to 640 px, actual range-to-pixel changes with unchanged raw
contacts, menu reachability, panel-choice reload and the existing 130 workflows.
Visual inspection covers narrow Selection/Dev menus, workspace controls and the
empty state. Exact evidence and hashes are in reinforcement-validation.md.
This resolves that specific follow-up; P4's broader format, native-platform,
accessibility and performance gates and P6–P9 remain open.


## Coverage explanation and P6 audit

Advisor item 20 now explains total contacts as a whole-map row/column sum with
its diagonal counted once. An optional formula and the coverage-normalization.md
worked guide explain the total-preserving factor and shared source-bin references.
Independent browser oracles verify both sources' coverage weights and all 25
normalized matrix values, with symmetric total 32 preserved within Float32 error.
The explanation is keyboard accessible and fits 1440/800 px. WebUI checks and
34 track/control/responsive scenarios pass on the final assets.

O/E settings and viewport levels remain pending. A real O/E → Observed → O/E
round trip loses a chosen [0.25,4] range and returns [0.5,2]. The audit identifies
the standard-renderer bypass and dedicated O/E color path, plus the current
250 ms limits-only refresh. Evidence is in reinforcement-validation.md; these
existing paths cannot qualify items 19/22 or the publication release.


## O/E palette and slider prerequisites

O/E now honors its endpoint colors/alpha and retains its own palette across mode
switches, reload and sessions. Expected has independent palette memory. The
Observed graph keeps its original state, and a ratio-palette mode shares the
same neutral-at-1 mapping in pipeline nodes. The user guide documents both paths.
Direct active-O/E pipeline projection, named palette presets and inline viewport
histogram/levels remain pending, so this does not complete item 19 or P6.

Browser checks also reproduced and fixed a shrinking-domain drag and suppressed
live threshold publication. Limits stay fixed during adjustment, O/E guards keep
handles around neutral 1, and changes now update pixels without Apply. The native
loaded-range test explicitly uses native coordinates; base-grid histogram support
remains open. Full JVM checks pass (588 tests, seven optional skips), WebUI checks
pass, and 149 final browser scenarios pass on the exact recorded artifact pair.
Evidence, exclusions and hashes are in reinforcement-validation.md.

## Mandatory release gates

Each change needs a focused regression, applicable existing checks, and actual
UI/output verification on disposable data. Java changes run
`./gradlew test -x copyWebUI`; WebUI changes run `npm run build:web`.
Final checks use the packaged WebUI and exact artifact hashes. Test all edits,
undo/redo, session migration/restore, shared-bin geometry and conservation,
failure/cancellation, stale jobs, matrix/sequence/figure exports, 15 open/close
cycles and a 30-minute mixed soak. Preserve p95 input <=100 ms and frame <=33 ms
on declared hardware. All advertised platforms require actual package/native
dialog qualification; unrun checks remain pending.


## Active primary O/E editor and shared presets

Rendering Pipeline now opens a compact Primary O/E colormap editor when Show
O/E is active, alongside the separate Observed graph. Shared blue–white–red and
purple–white–green presets, endpoint colors/alpha, ratio handles, automatic preview,
reset and typed palette-file import/export operate on the active primary palette.
Secondary selection does not redirect edits. Serialized, coalesced writes preserve
newer choices; invalid or failed changes leave the last committed palette intact.
Returning from Preview reconciles actual sidebar palette/display-mode changes.
Unchanged Observed graph closes no longer rewrite its stored representation.

Final WebUI build and 168 Chromium scenarios pass, plus an 11-case focused rerun
with explicit processing-option isolation assertions. Visual review includes the
compact editor and reachable footer at 800×600. Existing Observed graph workflows
pass on an unchanged 154 MB real-map copy. Details, artifact hashes and limitations
are in reinforcement-validation.md. No JVM runtime code changed in this increment.
This implements the direct active-O/E editor and preset part of item 19; inline
viewport histogram/levels (item 22), native/package, performance and other release
and publication gates remain open. P6 and the overall goal remain in progress.


## Inline viewport histogram and sample-based levels

A compact histogram now sits under the color slider, aligned with its threshold
marks and linear/log sensitivity axis. It refreshes on navigation, source/input
changes and raster revisions. Native bin-reference sampling at letter zoom avoids
counting every FASTA letter as another contact. Large views explicitly say Sampled
viewport and use two-dimensional block strata, with at most 65,536 cells and four
concurrent queries. A failed refresh clears the old curve and offers Retry.

Fit levels uses exact sample percentiles, changes actual map colors and preserves
contacts, numeric graph inputs and the other source. O/E uses positive ratios for
its logarithmic bounds while retaining measured zeros in the histogram. Missing
expected support stays NaN/null in binary/JSON diagnostics, matching transparent
map cells. An empty ordinary viewport keeps zero at the lower endpoint color.
API documentation now states the nullable-value and BINS contracts.

A cold sampled pan exceeded 150 ms before limiting image transfers. Traces showed
browser-side request stalls during tile loading; this indicates connection pressure,
not proof of the entire outlier's cause. Main-map/minimap image budgets now sum to
four, leaving HTTP/1 capacity for interactive queries. Two fresh final runs observe
at most four image transfers and meet the 150 ms fixture gate: sampled pans are
28.9–31.3 ms and 25.5–28.0 ms. Dense-map/packaged performance remains unqualified.

All 185 final browser scenarios pass, plus an independent 13-case focused repeat.
Artifact details are recorded in reinforcement-validation.md.
Full JVM tests pass (589 tests, seven optional skips, zero failures/errors), as do
the WebUI build and numerical checks. User guidance is viewport-histogram.md.
Draggable midpoint/gamma, native/package, dense-map/soak/accessibility and remaining
release/publication gates are still pending. Item 22/P6 is not complete.

A separate existing race was reproduced during setup: choosing normalization and
immediately entering Upper=100 can end at 11 after a late normalization refit.
The focused histogram fixture now awaits the real normalization/refit promise and
verifies the initial 100 write before testing Fit. This isolates the histogram;
it does not resolve or qualify that rapid-edit workflow. Fix before release:
serialize/coalesce source-scoped visualization writes and protect newer user drafts
from hydration/refitting. Add a delayed-refit browser regression for thresholds,
transforms and source switching, proving actual graph/options/pixels and session
state. This is the next P6 prerequisite alongside the draggable midpoint.

## Visualization draft ownership through normalization

The preceding normalization race is now fixed. Visualization edits carry field
revisions and their original source/colormap/mode. Serial writes read fresh graph
state; obsolete queued range writes coalesce. Late reads and acknowledgements
cannot hydrate over newer drafts. Manual palette edits supersede normalization
refits; processing-only edits defer the fit until their transform is committed.
Source switching, source/whole-graph presets, mode changes, session capture and
opening Rendering Pipeline respect pending changes. Failed graph reads and atomic
writes retain the committed map and show a retryable pending draft.

The committed baseline reproduced Log2/Upper=100 ending at Linear/11. The final
build passes a new 16-case browser suite with delayed requests and actual backend,
numeric-input, pixel and session assertions. All 185 established browser scenarios
also pass, for 201 total. The separately repeated FASTA measurement suite replaces
an interrupted wrapper run; incomplete runs are not counted. Final fixture sampled
pan acknowledgements are 21.6–25.2 ms with at most four active image transfers.
This is fixture qualification, not production-size or packaged-build performance.
Visual review covers final desktop, failure recovery and 640 px navigation captures.

WebUI build, type checks and existing/new numerical regressions pass. JVM runtime
is unchanged, so the previous 589-test baseline is retained rather than rerun or
presented as fresh. Evidence and artifact hashes are in reinforcement-validation.md;
user guidance is visualization-transactions.md. Draggable midpoint/gamma, derived
view whole-resolution exact-scan semantics, wider custom graphs/NaN provenance,
native/platform/performance/accessibility qualification and the remaining packages
stay open. P6 and the overall release/publication goal remain in progress.

## Exact limits for the selected derived view

Whole-resolution scans previously evaluated Observed graph input even in Expected
or O/E. They now use the primary display signal in those modes and fit a separate
whole-resolution expected profile once, reusing it across 512 × 512 blocks. The
viewport profile and map pixels stay unchanged. Undefined ratios do not count as
zeros. Observed/Secondary expressions and the API's default pipeline mode remain
supported. Scans wait for pending processing edits, offer Cancel, reject late
responses after cancellation/mode changes and retain completed limits while panning.
Percentage identity controls keep their fixed scale, independently of derived views.

An independent 513-bin fixture crosses both block axes: counts 2 and 8 on the main
diagonal imply Expected=5 and supported O/E max=1.6. The old backend returns 8
instead of 5. New HTTP and browser regressions verify four complete blocks, finite
counts, cache reuse, unchanged viewport fit, pixel bytes, settings and source file.
All 590 JVM tests pass (zero failures/errors, seven optional skips). The WebUI build
and 121 browser scenarios pass, including the ten new scan cases, prior transaction,
O/E, track/source, responsive, graph-editor and histogram cases. A separate ten-case
repeat adds explicit rendered-tile hash preservation. Fixture sampled pans are
24.0–31.2 ms with image-transfer peak four; production-size/package performance is
not implied. Evidence is in reinforcement-validation.md; user guidance is
whole-map-signal-limits.md.

This resolves the recorded derived-view exact-scan signal mismatch. Expected
profile memory still grows with distance bins and the initial whole-resolution
fit adds a full pass; dense-map resource/latency qualification remains required.
Draggable midpoint/gamma, wider graph/NaN provenance, accessibility, platform/native
packages, P7–P9 and final release/publication deliverables remain pending. The full
goal is active; protected plans and owner files are unchanged.

## Color midpoint across rendering and persistence

Ordinary Observed/Expected gradients now offer a draggable diamond marking the
half-RGBA blend signal. Keyboard arrows, fine steps, Home/End and Reset midpoint
work without changing thresholds, input expressions or histogram samples. The
rail previews the actual curve on the chosen slider axis. Close threshold handles
get a separate midpoint lane. O/E retains neutral 1 and identity palettes retain
their fixed mapping, without the ordinary midpoint control.

The optional gamma parameter persists through DTOs, draft revisions, derived-mode
memory, source/whole-graph/user presets, session restore, graph-editor round trips
and export metadata. Legacy omitted/default gamma keeps the original appearance
and representation. The native renderer applies the color curve to its own flat
buffer, leaving numeric data intact and preserving its ABI. Rendering Pipeline
uses the same ordinary colormap parameter. Saving user presets now flushes pending
changes. Browser testing also found and fixed a Secondary preset resetting Primary
in the single-map layout; Secondary stays scoped to its selected source.

Final WebUI build/type checks and all 594 JVM tests pass (zero failures/errors,
seven optional skips). Final Chromium acceptance has 134 passing scenarios plus
an independent 13-case midpoint repeat. It includes actual PNG colors, unchanged
numeric inputs and other-source state, pointer/keyboard/close-handle interactions,
presets/export, graph editor and identical pixels after session restore. Existing
O/E, transactions, exact scans, responsive, tracks and histogram workflows pass.
Fixture sampled-pan acknowledgements are 29.3–30.5 ms with image-transfer peak four;
this default-curve fixture result does not qualify dense-map gamma performance or
native packages. Final visual review covers the separate crowded-handle lane and
640 px controls. Details/hashes are in reinforcement-validation.md; user guidance
is color-midpoint.md.

The midpoint portion of item 22 is implemented. Dense-map/soak/profile-memory and
non-default-gamma performance, wider graph/NaN provenance, accessibility, native
platforms, P7–P9 and release/publication artifacts still need qualification or
implementation. P6 and the overall goal remain active. Protected plans/owner files
are unchanged; nothing pushed, merged or tagged.

## Example downloads in Jobs and recovery

Starting an example download now hands progress to Jobs, focused on the named
dataset. Known totals show bytes/percentage; unknown totals remain indeterminate.
Reference preparation and cancellable verification have explicit messages. Optional
file warnings remain on the result. The server retains at most 50 attempts, each
with its own identity. Cancelling an older attempt cannot cancel its replacement.
Failure/cancellation offers a return to the matching example card for retry or
compatible HTTP Range resume. Catalog errors have a separate Retry action.

Quick-start automatic opening belongs to the accepted download attempt. Closing
Jobs, changing workspace, failure or cancellation revokes that intent. A late start
reply after catalog closure cannot reopen Jobs. Completed results remain available
explicitly. Jobs polling rejects stale host/lifecycle replies and its fixed header
keeps Close reachable while scrolling long histories. Catalog filesystem checks
run off the HTTP event loop. Existing API clients and other job services retain
their response behavior; optional job details extend the shared DTO.

Real Chromium controls exercise the actual download handlers and downloader against
a controlled TLS provider, with pinned checksums, a valid HDF matrix, interrupted
Range transfers, failure/retry, unknown-length bodies, optional warnings and
independent attempt identities. This does not change the production URL policy.
FASTA-only Create/Open from Jobs followed by Assembly Export to AGP preserves all
12,017 sequence bases in the regression fixture. The WebUI build and 597 JVM tests
pass, with zero failures/errors and seven optional skips. Detailed final browser
evidence, artifact hashes and diagnostics are in reinforcement-validation.md;
user guidance is example-jobs.md.

P7 remains in progress: conversion/wizard phases keep their existing workflow;
cache readiness currently checks manifest/file existence rather than proving a
current-catalog fingerprint and rehashing cached content. Public example providers,
OSD resolution transitions and diagonal provenance require their wider checks.
P8/P9, other package qualification, accessibility, dense-map performance, native
platforms and publication/release artifacts remain open. The goal stays active.
Protected plans/owner files are unchanged; nothing pushed, merged or tagged.

## Cached example verification and current-catalog provenance

Cached cards and historical example results now start cancellable Jobs verification
before opening. Manifests retain the canonical current declaration, its fingerprint
and file hash/size/modification metadata. Catalog discovery stays metadata-only;
opening rehashes even same-size/same-mtime content. Valid pinned cached files are
reused without downloading, while unpinned files require a matching source
declaration and verified recorded hash. Recipe/attribution changes refresh
provenance without refetching unchanged sources; legacy records and changed
unpinned source declarations require downloading again. Late verification
start/catalog replies cannot replace a different workspace. P7 remains in progress:
subsequent conversion/wizard phases, real public example qualification, OSD resolution
transitions/diagonal provenance, catalog/session provenance transfer and release/platform
gates remain open. Screenshot review also found repeated startup sensitivity/progress
notifications overlapping Jobs; remove that noise before release. Final validation
evidence is recorded in reinforcement-validation.md.

## Quick-start conversion progress and quiet opening

Quick-start examples now hand conversion progress to Jobs instead of following an
invisible polling loop. Their server-side conversion results retain validated
example context and return through the current recipe, revalidating cached inputs
and reusing completed conversion while preserving the linked FASTA. Closing Jobs,
changing workspace, failure and late resolution/submission replies revoke automatic
opening. Context is accepted only for a current ready quick-start recipe, its exact
source, a conversion to HiCT and the standard cached destination. Other conversion
results keep their existing behavior. Internal slide-sensitivity notices and
redundant Preparing example notifications no longer accumulate on map opening.
Long job diagnostics stay under Error details; failed example conversions offer
a focused return to their catalog card for retry.

Primary validation and final artifact evidence are recorded in
reinforcement-validation.md. P7 remains active for larger/public examples, live
conversion cancellation qualification, wider wizard workflows, catalog/session
provenance transfer, OSD resolution/diagonal behavior and release/platform gates.
Protected plans and owner files remain unchanged; nothing pushed, merged or tagged.

## Info responsiveness and source-supported Micro-C diagonals

Info now selects its current source/resolution from layer and view metadata,
without GPU tile-pixel reads. It refreshes a stationary pointer during delayed
resolution changes and when existing field/position settings change. Replaced
controls release their settings watchers; contig names display literally and
coordinates outside the assembly remain empty. The OSD design is preserved.

Real-map qualification exposed a map-switch renderer crash in vue-sonner 0.4.2's
programmatic dismissal. The exact small NaN-weighted-map reproduction now passes
with pinned vue-sonner 2.0.9 and its explicit stylesheet. Masking notices, prompts
and late callbacks belong to their originating map. Notification history remains
available. Mixed native pyramids also exposed an HTTP 500 in numeric pipeline BINS
queries: required Secondary signals now use the existing native projection path,
including hidden bins on both grids. Primary-only expressions skip unused reads.
Two new HTTP regressions verify exact cells and projected mass; native input
counts are unchanged.

Independent source comparison matches all 669,584 raw cells in 17 windows over
three chromosomes and three resolutions. A full stored cis-diagonal scan confirms
the fine-resolution empty band already exists in the deposited Cooler. The
coarser diagonal contains contacts. The example card and
[Info/Micro-C guide](info-and-microc-diagonal.md) explain the result without
inventing an upstream filter or biological cause.

The final development pair passes 602 JVM tests with zero failures/errors and
seven optional skips, the WebUI build/type/numerical checks, and 121 actual-control
Chromium cases. Screenshots were inspected for delayed tiles, literal names,
replacement maps, notification failures, track resizing and responsive layouts.
Artifacts, input hashes and limits are in reinforcement-validation.md. This does
not qualify a combined native/SFX package or all large-map/platform performance.
P7 remains active for live conversion cancellation, wider wizard/public examples
and catalog/session provenance transfer. P8/P9 and remaining release/publication
qualification remain open. Protected plans and owner files remain unchanged;
nothing pushed, merged or tagged.

## Overall conversion progress and live cancellation

Example conversion progress is now determinate even when its context contains no
download byte count. Only unknown-length example downloads use an indeterminate
bar. The backend's overall fraction now includes every stage's budget: previously
each stage reached 100% because the denominator ended at that stage. A focused
weighted-stage regression verifies continuity across transitions. Cancellation is
shown as an expected outcome, with retained-input and focused retry guidance,
without a failure alert or an Open result action for the cancelled attempt.

The final Linux browser check pauses an identified, real bundled hictk zoomify
child of the owned test server. Cancel terminates that child, removes unpublished
output and staging, preserves matrix/reference hashes and keeps the current map.
Retry creates a separate attempt and opens the completed recipe with its 17-base
reference. The original attempt remains cancelled. Cancellation HTTP acknowledgement
was 23.5 ms on this host; this is not a throughput or cross-platform benchmark.
The native pipeline temporary directory was also confirmed removed.

Final validation: 603 JVM tests, zero failures/errors, seven optional skips; WebUI
build/type/numerical checks; 42 browser cases (26 examples, ten other services and
six failure-reporting cases). Screenshots confirm numeric progress, neutral
cancellation, focused retry and the resulting linked reference. Exact artifacts,
process identity and evidence are in reinforcement-validation.md. This closes the
stated live-conversion-cancellation acceptance on Linux, not the platform matrix.
P7 remains active for wider wizard/public examples and catalog/session provenance
transfer. P8/P9 and remaining scientific, performance, native and publication/release
qualification remain open. Protected plans and owner files remain unchanged;
nothing pushed, merged or tagged.

## Example recipe files and frozen per-source provenance

Examples routed to the existing wizard now preselect their declared annotation
tracks alongside matrix/FASTA/assembly. Cached catalog responses identify available
files so missing optional recipe tracks do not break opening; download warnings
remain in Jobs. Track selections are inserted before probes complete, and an
obsolete probe cannot resurrect a removed track or update a replacement entry.
The wizard also updates the AGP header after actually loading a layout.

A source wrapper owns a frozen copy of its example declaration, citation,
attribution, recipe and recorded cache hashes. Only a ready matching declaration,
or a converted result with a current input/output conversion cache, is associated.
Primary and Secondary carry independent records. Session snapshots preserve them;
restore validates version, catalog schema/fingerprint, file records and source
binding without consulting the current catalog, fetching URLs or executing a
recipe. Older sessions remain supported, and ordinary maps inherit no attribution.
These records do not authenticate a session or imply fresh full-file hashing.
See example-session-provenance.md and the extended session API schema.

Final validation: 607 JVM tests, zero failures/errors, seven optional skips; the
WebUI build/type/numerical checks; 87 actual-control browser cases; five further
HTTP acceptance cases on fresh physical copies. The full controlled recipe loads
FASTA, reversed AGP and BED, exports the expected 17-base reverse complement,
retains provenance after catalog edits/removal and restores its actual assembly
and track. All four downloaded recipe inputs retain their recorded hashes.
Primary/Secondary restore and rejection/detach/replacement cases preserve source
identity and leave native HDF files unchanged. Screenshots caught and verified the
AGP header correction. Exact evidence and artifact hashes are recorded in
reinforcement-validation.md.

P7 remains active for broader wizard/source/format combinations and current public
examples. The restored-session layout status chip still needs a source-aware
presentation rather than implying that no saved layout exists; source metadata
viewing and figure provenance belong to subsequent publication work. P8/P9 and
remaining scientific, performance, native/platform and release/publication gates
remain open. Protected plans and owner files remain unchanged; nothing pushed,
merged or tagged.


## Opt-in remote BLAST and independent external reports

Tools now offers Search NCBI databases without a contact map or linked FASTA.
Ordinary sequence search still defaults to the local assembly. Remote requests
require a fresh explicit upload decision, fixed supported database, bounded query
count/length and timeout. Generated query IDs and pasted sequences are uploaded;
FASTA descriptions stay local. A separate bounded worker queue keeps actual local
SeqKit searches available while the provider client waits. Jobs retains progress,
cancellation, useful diagnostics and report reopening across map changes.

Reports retain external subject IDs, one-based inclusive coordinates, scores,
parameters, dates and query/client hashes. They never create assembly BED tracks.
The viewer filters and pages through a bounded JSON report; complete bounded raw
TSV remains exportable through the file-save flow. Late report replies cannot
reopen a dismissed viewer. See remote-blast.md for limits and provider semantics.

Controlled qualification passes 18 new actual-control Chromium cases, 17 existing
local sequence cases, ten other Jobs cases and six failure-reporting cases. Two
further HTTP/process cases reject oversized output and confirm that signalling
only the JVM stops its owned provider client. The full JVM suite passes 616 tests
with zero failures/errors and seven optional skips. Screenshots were inspected at
normal and 640-pixel widths. These controlled commands never contact NCBI and do
not establish live service success. The first live public-reference query exposed
a rejected 16S short database name; it now maps to the remote service name. A
corrected-name attempt reached its selected three-minute timeout. The public nt
probe is recorded separately in reinforcement-validation.md. No project reference
was uploaded. P8 remains in progress until live-provider and platform gates are
qualified. P7/P9 and the remaining scientific, performance, native and publication/
release gates remain open. Protected plans and owner files are unchanged; nothing
pushed, merged or tagged.


## Read-only exported figure viewer

File now offers Open exported figure independently of the current map. The
viewer opens PNG/SVG through a local chooser, fits the image, provides actual-size
and zoom buttons, and scrolls larger previews. Browser file handles remember the
figure selection/folder and can begin from a figure save destination. Cancelled
choices retain the preview; invalid files explain the problem without replacing
a previously opened figure. SVG stays in image context, without inserting file
markup into the application DOM. File size and pixel dimensions are bounded.
Closing during image decoding cannot reopen the viewer and releases its URL.

Fourteen focused Chromium cases reopen actual exported PNG/SVG, compare every PNG
pixel, check zoom and 640-pixel controls, rejection/recovery, inert SVG content,
chooser memory/cancellation and late decoding cleanup. The existing ten Jobs and
six failure-reporting cases are rechecked for shared navigation/file flows.
The WebUI build/type/numerical checks and 616 JVM tests pass with zero
failures/errors and seven optional skips. Exact artifacts and evidence are in
reinforcement-validation.md. The API and separately served assets qualify a
development pair, not the combined native/SFX distributable.

This implements the PNG/SVG reopening acceptance within P9. Composition, actual
ModDotPlot/track figure panels, immutable cut/scaffold comparisons and stored
snapshot provenance remain open, along with wider release qualification. P8 also
remains active: its corrected 16S live-04 probe reached its ten-minute timeout.
A longer public-reference probe with NCBI tracing confirms an accepted provider
request ID and responses without connection errors; provider result success is
still pending. No project sequence is uploaded. Protected plans and owner files
remain unchanged; nothing pushed, merged or tagged.


## Frozen figure previews and scientific configuration provenance

Export now opens a dedicated preview dialog, captures the current map once and
uses that immutable scene for PNG, SVG and PDF saves. Area and ruler/FASTA/1D-track
choices require an explicit refresh. Displayed ModDotPlot layers use their real
rendering graph; a native-tool journey generates and exports a split Hi-C/identity
map with tracks. Exports retain source fingerprints/units, assembly hashes,
rendering settings, client presentation, actual rendered grid, exact selection
bounds and crop geometry. SVG has one root record; PNG uses UTF-8 iTXt without
changing image chunks; PDF uses escaped XMP. Quick fingerprints are labeled and
are not full-file verification. See figure-capture.md.

Two UI findings were fixed: cropped metadata had been nested in the drawing,
and hidden narrow panels produced invalid SVG text widths. Whole-map checks
also exposed temporary export resizing clamping live track-panel sizes and
shifting the restored view. The workspace observer now ignores export-only
layout; small whole maps are fitted to a readable size. Native count bytes are
unchanged, and a retained capture stays byte-identical after an exact cut.

Final development-pair validation: 616 JVM tests, zero failures/errors, seven
optional skips; WebUI build/type/numerical checks; 72 Chromium cases, including
three-format ModDotPlot parity and exact-cut/stale/cancelled capture. Independent
SVG/PDF rasterization matches 2,279 flat interior map samples. Screenshots were
reviewed at ordinary and 640-pixel sizes. Artifacts and limits are in
reinforcement-validation.md; these are separately served assets, not a qualified
combined SFX/native package.

A further Expected/O/E audit records multiple asynchronous profile-window updates
during whole-map capture. That state is outside ordinary configuration snapshots.
Pinning and recording the fitting context is the next required scientific
capture transaction before those modes are qualified. Named cut/scaffold
snapshot comparisons, session persistence and final publication figures remain
open. The NCBI public-reference job reached its thirty-minute client limit; its
existing request still reports WAITING at the provider, without another query
submission. P8 live success and the other release/performance/platform gates
remain open. Protected plans and owner files remain unchanged; nothing pushed,
merged or tagged.

## Expected/O/E figure transactions and rendering failure isolation

P9 now pins a server-side fitting context independently of the live viewport.
Expected/O/E figures use one actual primary profile across tiles and retain its
SHA-256, domains, native grid and units. Base-level captures fit BINS rather than
the synthetic one-base axis. Figure provenance also records effective FILE or
SQRT_COVERAGE normalization, application of weights and coverage algorithm.
Source/configuration and tile/track generations invalidate stale captures.
Concurrent navigation can update the live viewport profile without changing the
figure. Counts remain native and unchanged.

Contexts are bounded by count, retained profile/metadata budget, lease and fit
work; both cell count and thin-rectangle axes are checked before allocation.
Success, error and close release the context, including a late creation reply.
Tile request/cancellation/decode failures reject an incomplete figure promptly.
The stricter checks exposed a shared coarser overview source requesting the wrong
fitting grid; the overview is paused for capture and restored. Repeated
mixed-resolution captures also exposed a projected-overlay transition timeout.
Capture now promotes the complete overlay without a cross-fade while ordinary
navigation retains transitions. The actual camera and interactions are restored.

Final development-pair validation: 623 JVM tests, zero failures/errors, seven
optional skips; full WebUI build/type/numerical checks; 59 Chromium cases. These
include 17 Expected/O/E cases across scopes, mixed native grids, sequence zoom,
real prepared coverage and concurrent rendering/weight changes; ten frozen
figure cases; eighteen real sequence/ModDotPlot/track cases; fourteen reopened
figure cases. The existing independent SVG/PDF rasterization comparison matches
2,279 flat interior map samples. Screenshots were inspected, including whole-map,
coverage, responsive and error states. See reinforcement-validation.md and
figure-capture.md for exact artifacts, limits and excluded intermediate attempts.

This closes the fitting-context prerequisite within P9, not P9 or the release.
Named immutable cut/scaffold snapshots, comparison composition, portable session
persistence, publication figures and the remaining platform/performance/release
gates remain open. This is an API/static-assets development pair, not a qualified
combined native/SFX distributable. P8 live provider success remains open: a sparse
read of the existing public NCBI RID at 07:31 local still reports WAITING, without
resubmitting any sequence. Protected plans and owner files remain unchanged;
nothing pushed, merged or tagged.

## Portable named snapshots and immutable comparisons

P9 now stores named captures independently of subsequent assembly edits. The
Export dialog can reopen an original snapshot, compare it with the current
capture horizontally or vertically, retain the comparison itself, and export
PNG/SVG/PDF with vector labels and complete per-panel provenance. Removing a
snapshot has an Undo action. The library starts collapsed and its controls wrap
at 640 pixels. Stored figures travel in session envelope v5; sessions without
figures keep their compatible v3/v4 envelopes.

Snapshot changes participate in close/replace/navigation protection. Confirmed
writes mark only the captured revision saved; newer figures remain unsaved after
a delayed write. Session import validates and decodes bounded, reconstructed SVG
and PNG records before replacing the live workspace. It rejects active/external
content, malformed chunks, dimension/provenance mismatches and excessive metadata.
CRC32 detects corruption; recorded scientific provenance is not authentication.

The real session journey uncovered legacy AGP fragment IDs being reassigned after
Reverse. Restore now imports recorded identities for legacy and exact-cut
snapshots while retaining strict source interval, ATU, matrix and rollback checks.
Repeated Expected/O/E capture also exposed a native secondary layer surviving a
switch to the projected grid, unnecessarily loading thousands of hidden-underlay
tiles. Only the current projected pair is visible now; the browser regression
explicitly crosses sequence/native and projected grids.

Validation uses 625 JVM tests, zero failures/errors and seven optional skips,
full WebUI build/type/numerical checks, 88 primary Chromium cases plus a repeated
17-case Expected/O/E qualification, and a DOM boundary check rejecting 17 unsafe
records. Independent SVG/PDF rasterization matches 1,436 comparison samples and
2,279 real ModDotPlot samples; original snapshot PNG bytes and each comparison
panel's pixels survive unchanged. Exact commands, hashes, exclusions and artifacts
are in reinforcement-validation.md. Screenshots were inspected for the stored
original, real identity/track comparison and narrow layout.

Publication presets and final real-example figures at intended print size remain
open, as do P8 live provider success and the wider performance/platform/release
gates. This validates separately served API/assets, not a combined native/SFX
package. Protected plans and owner files remain unchanged; nothing pushed,
merged or tagged.

## Physical figure sizes and PDF page integrity

P9's Export dialog now offers captured size and 86/178 mm column widths, matching
the current Bioinformatics figure-width guidance. All formats record the same
physical dimensions, original raster dimensions, effective DPI and pixel-preserving
policy. SVG retains its coordinate viewBox with millimetre dimensions, PDF uses
that physical page and PNG replaces only physical-size/provenance chunks. Matrix
pixels, fitting inputs, captured vector geometry and stored snapshots remain
unchanged. Returning to captured size gives the original PNG bytes.

The audit also found jsPDF silently caps pages above 14,400 points. Oversized
captured/print pages now reject with a smaller-size/area recovery message rather
than publish clipped drawing. Focused Node checks cover that guard and independent
PNG chunk/CRC/image-data validation, and run in build:web. The full WebUI build,
62 Chromium cases and independent SVG/PDF rasterization checks pass. Portrait
comparisons, saved-snapshot independence, physical sizes, editable labels and
640-pixel controls were checked; screenshots were inspected. Exact artifacts are
in reinforcement-validation.md. JVM runtime source is unchanged in this increment.

The DPI shown is derived from existing capture pixels, not a claim of added
resolution or publication acceptance. Final real-example figure composition,
readability at print size, archival artifacts and the remaining release gates
are still required. The tested API/static-assets pair is not a packaged/native
release. Protected plans/owner files remain unchanged; nothing pushed, merged
or tagged.


## Exact package identity and integrated Linux candidate

Portable packagers previously chose an alphabetically last JAR on Linux/macOS
and a newest-by-mtime JAR on Windows. The current working directory contains a
2.0.9 artifact that sorts after 2.0.49, so a fresh package could silently contain
an old application. All packagers now require the exact versioned JAR and reject
its absence. CI native checks, smoke checks and uploads use the stamped version,
including platform paths, instead of selecting unrelated files or falling back
to another JAR. Explicit CI version assignment records HICT_BUILD_VERSION;
ordinary local stamping has no environment-file side effect.

Six focused selection/wiring cases execute Bash and PowerShell against current
and stale artifacts, misleading timestamps, spaces and missing-current cases.
The complete Node safeguard set passes 25 cases, version tests pass five cases,
workflow actionlint and changed-script syntax/format checks pass. The integrated
build embeds the current WebUI and passes 625 JVM tests with zero failures/errors
and seven optional skips. Linux .run and .tar.gz packages build, validate their
checksums, contain the expected JAR and pass bundled native/sequence-tool smoke
checks. Their identities and the sequential qualifier are documented in
release-qualification.md and reinforcement-validation.md.

The extracted package's runtime serves 53 checked asset files byte-for-byte.
Packaged metadata and both-size mouse/keyboard menu checks pass. The structural
review exposed a historical driver still waiting for the removed-bin split API;
it now checks the exact-base endpoint, retained total bases and replay lineage,
with its original Undo/Redo equality assertions retained. The corrected structural suite and offline workflow pass on the first candidate.
The 4- and 1,765-contig performance fixtures meet the unchanged limits. A later
input has no complete HiCT metadata; it is correctly rejected, but its HTTP 500
exposed a poor error classification. The library now reports that condition as
bad input with an actionable recreation message. JVM coverage retains the edited
primary/secondary, FASTA, tracks, workspace and matrix cells, and two Chromium
picker attempts retain the same manager and exact pixels. The corrected 50
candidate passes the full 625-test suite and independent checksum/self-extraction
checks. Sequential qualification is running under /tmp/hict-integrated-release-06
from that .run extraction, with complete 96-, 1,765- and 7,790-contig inputs.
Performance/soak and current-source cross-platform/native GUI gates remain open.
Protected plans and owner files remain unchanged; nothing pushed, merged or tagged.

A read-only follow-up of the existing public 160-base NCBI request at
2026-11-17 06:03 UTC reports READY with hits. Its bounded result was retrieved
at 06:04 UTC: ten hits, the expected query length, first accession NR_180316.
Files public-rid-status/result-after-packaging under /tmp/hict-remote-blast-live-05
retain the responses. No query was resubmitted and no project DNA was uploaded.
The earlier HiCT client had already timed out, so successful provider completion
still does not prove HiCT's live result delivery. Late-result recovery remains
required before closing P8.

## Map completion after obsolete tile cancellation

Candidate 50's integrated run passes offline recovery and its structural
navigation timing, but stops before measuring the 96-contig map: tiles are
loaded and the queue is empty while OpenLayers still reports an incomplete
frame. This is distinct from the incomplete file rejected in the preceding
candidate. A focused repetition reproduces it naturally; a diagnostic frame
changes only completion state, not the camera, tile states or scientific data.

The main map now observes OpenLayers loadstart/loadend. While loading, one
100-ms timer checks queue and layer readiness; it requests the missing frame
only after both are idle. It never marks tiles or frames complete. Completion
and disposal stop polling and remove listeners. The small private readiness
adapter is pinned to the installed OpenLayers version, with an upgrade guard.
A controlled real-OpenLayers regression fails against the old bundle and passes
six successive obsolete-queue cases plus disposal cleanup against the fix.

Full WebUI build/type/numerical checks pass. Twelve fresh/repeat navigation
samples retain the original limits: frame p95 <=16.8 ms, acknowledgement p95
<=30.9 ms. Seventeen Expected/O/E, fourteen snapshot/session, nineteen real
sequence/ModDotPlot/track and ten frozen-figure cases pass with no unexpected
errors. The integrated JVM/WebUI build passes 625 JVM tests with zero
failures/errors and seven optional skips. Exact artifacts, diagnostics and
excluded attempts are in reinforcement-validation.md.

Candidate 51, version 2.0.51-5cf6aaa-webui_1a37de6, passes package generation,
checksum verification and actual .run extraction. Fresh sequential qualification
/tmp/hict-integrated-release-07 is terminal failed on the 7,790-contig map:
fresh/repeat frame p95 is 33.4 ms against the unchanged 33 ms limit. The
structural, 96- and 1,765-contig navigation samples pass, as do metadata, menus,
structural editing, invalid-input recovery and offline workflows. Map completion
is healthy; large-map rendering needs profiling. The full soak was not reached. P8 late provider-result recovery, final publication figures,
platform/native-shell and other release gates stay open. Protected plans and
owner files remain unchanged; nothing pushed, merged or tagged.


Dense-map ruler measurements now skip bands narrower than six-pixel padding.
Profiled text-shaping self time falls from roughly 2.3 seconds to 28–32 ms per
navigation sample, with identical ruler pixels and a passing full WebUI build.
The separate unprofiled large-map gate still fails at 33.3 ms frame p95;
acknowledgement remains below 41 ms. Boundary-feature rebuilds are the next
measured target. This is a verified optimization, not release qualification;
the integrated matrix and soak remain open. See reinforcement-validation.md.


Unchanged contig/scaffold border geometry now survives pan/zoom within a display
resolution. The bounded cache invalidates on assembly/grid/style/name/bounds
changes; viewport labels and independent exports retain their rules. Vector
sources update changed features while retaining unchanged spatial-index entries.
No borders are faded or omitted.

The complete 7,790-contig development gate now passes: fresh/repeat frame p95
16.8 ms, acknowledgement below 40 ms, unchanged 33/100-ms limits. Six real border
control/pixel checks and 67 structural/Expected/snapshot/frozen/sequence checks
pass. Full WebUI build and focused lint/type/numerical checks pass (existing lint
warnings recorded). Candidate-53 integrated package qualification now runs under
/tmp/hict-integrated-release-08. Its full integrated build passes 625 JVM tests
(zero failures/errors, seven skips), and package generation, archive verification,
actual .run extraction and served metadata/assets pass. Performance and full
soak remain pending; no release-readiness claim follows from development checks.
Weighted-source readability, late remote-result delivery, final publication
figures and wider native/platform/source-format gates remain open. Protected
plans/owner changes are untouched; nothing pushed, merged or tagged.


Candidate-53 run 08 passes the complete packaged performance matrix: eight
fresh/repeat samples, frame p95 <=16.8 ms, acknowledgement p95 <=35.8 ms and
zero long tasks. Its first soak edit exposes an obsolete test shortcut: a region
is not an entity selection. The run ends failed with no memory samples. The
soak now uses a real Shift click and verifies selected IDs. Fresh run 09 repeats
the full sequence with the same verified package; the full soak remains open.


Run 09 completes 30 minutes/223 cycles and fails the listener-growth gate:
251 to 282 versus an allowed +20. Frame p95 16.7 ms, edit acknowledgement p95
47.18 ms and browser/JVM memory slopes pass. Native inputs remain unchanged.
The cause is the pinned notification dependency's missing document-listener
cleanup. A version/signature-guarded production and development bundler fix now
registers Vue cleanup correctly, without changing other notification behavior.

A real-Vue regression reproduces 100 leaks in the published hook and zero in the
fix. The old real-browser export test fails after one notification; twelve
production and four live-development save/dismiss cycles pass against the fix.
Old/new exported figure pixels are identical. Full WebUI checks pass. The
corrected soak driver uses actual Shift selection and export/picker controls.
A fresh integrated package and full qualification remain required. Current
platform CI, real publication figures, wider source/native coverage and P8
late-result delivery remain open; the existing public RID can be retrieved in
documented JSON format, but HiCT recovery is not implemented.


Notification cleanup validation also uses platform-native file URLs rather than
URL pathname, so Windows drive letters/escaped paths reach the actual dependency
entry. Focused regression passes; this is not Windows runtime qualification.
Candidate 54's full integrated build passes 625 JVM tests (zero failures/errors,
seven skips); the final revision needs a fresh stamped build before packaging.


Candidate 55 (notification cleanup plus portable regression paths) now passes
the full integrated build: 625 JVM tests, zero failures/errors and seven skips.
Its Linux package producer, archive verification, actual extraction and full
qualification subsequently pass (terminal receipts below). Obsolete 50/51 software-only copies
were removed after full archive/content verification, preserving original
inputs and all evidence. See reinforcement-validation.md for receipts.

Candidate 55 passes the complete integrated Linux qualification in
`/tmp/hict-integrated-release-10/qualification.json`, including its own extracted
runtime/assets/toolchains, strict navigation gates and a 1,805.96-second mixed
soak (229 cycles). Frame p95 is 16.7 ms and edit acknowledgement p95 42.08 ms.
Browser heap slope is 0.0898 MiB/min; JVM RSS slope is 0.8516 MiB/min. DOM remains
632 and listeners grow by four, within the unchanged limits. No unexpected
errors occur; package JAR and native source bytes remain unchanged. This closes
the integrated candidate-55 Linux performance/soak gate, not the overall release.

P8 now adds download-only recovery for a known completed NCBI RID. A separate
capability-checked endpoint prevents an older server from submitting the pasted
queries. The server sends only the RID to NCBI, validates returned aligned bases
against the locally pasted original query, records full/partial/no-alignment
verification honestly, and delivers external results through Jobs without an
open map or local BLAST executable. Local retrieval filters and provider settings
are recorded separately; no assembly annotation is fabricated.

Five live browser checks recover the existing approved public-reference request
FR9P7TGF016, verify ten hits and export JSON/TSV. Eighteen controlled submission
browser checks pass, including cancellation, timeouts and narrow layout. All
641 JVM tests pass (zero failures/errors, seven optional skips), including reverse
query validation and deletion of partial output after a malformed late hit.
Full WebUI build and focused lint pass (existing warnings). This development
increment is newer than candidate 55 and is not package qualified. Native
submissions that time out without exposing a RID still lack automatic recovery;
P8 remains open. Final publication figures/readability, wider formats and native
GUI/platform qualification also remain required. Protected plans remain unchanged.
The final live recovery run also passes a sixth check at 640 pixels, with
visually inspected wrapping, horizontally scrollable data and visible exports.

## Physical text size in figure export

Export now reports the smallest vector text in points at the chosen print width.
It measures the immutable scene, including inherited fonts, tspan overrides,
rotation, scale and nested comparison viewports. It uses an allowlisted scene
and removes its temporary measurement DOM. Captured pixels and all output
geometry remain unchanged. Text below 6 pt receives a concrete capture/layout
suggestion; this is a readability cue, not a journal compliance certificate.

An existing 1,894-pixel side-by-side comparison has 9-pixel labels: approximately
2.4 pt at 178 mm. The verified 911-pixel single capture reports 5.0 pt at 178 mm
and 2.4 pt at 86 mm. Real print-size browser checks pass in PNG/SVG/PDF with
identical pixels/provenance, responsive controls and independent saved captures.
Browser transform tests and the full WebUI build pass. Screenshots were inspected.
Final real-example cut/scaffold/ModDotPlot publication figures and readable
composition remain open, along with the recorded release/platform gates.

## Compact native-grid captures on real public data

Whole-map export now offers an explicit expansion choice. The existing expanded
default is preserved; disabling it captures at the native grid instead of
stretching a small map to the window. Changing it requires Refresh capture.
Provenance records FIT_WINDOW/NATIVE_GRID; immutable previews, snapshots and
output formats retain the selected geometry and actual physical text readout.

Eleven frozen-export browser checks pass, including compact/native resolution,
unchanged scientific provenance and restoration of the live camera. The full
WebUI build and focused lint pass. The version-57 integrated JVM build also
passes 641 tests with zero failures/errors and seven optional skips.

The new reproducible Borrelia preparation script generates real ModDotPlot and
a GC BigWig from the matched public FASTA, performs an exact feature-menu cut
at 450,123 bp, groups its children through the scaffold button, and saves three
named captures in PNG/SVG/PDF. Run 05 passes with no browser errors or source
changes; the 200-base cut-crossing sequence matches before/after edits. Pipeline
settings stay identical across all panels. Captures are 309×279 then 310×280
pixels, suitable for composing a two-column figure without shrinking labels to
the sizes exposed by the previous expanded capture. Independent BigWig checks
match all 155 GC windows across 19 replicons within Float32 rounding.

The edit marker is explicitly illustrative, not a claimed misassembly or gene.
Final figure composition, caption/alt text, readable print inspection and an
archive from the committed build remain required. This does not qualify a new
portable package or the wider platform/source-format gates.

## Real-data track-title and scale clipping

Composing the public-data panels exposed two defects. Vertical titles inherited
center alignment from a preceding signal scale, cropping the start of the next
track name. Horizontal scale labels assumed a fixed width, truncating the real
GC maximum 0.478 to 0.4; rotated vertical scale labels could extend above their
canvas and clamped tick positions could suppress the maximum.

Track titles now set their own alignment/baseline. Scale labels reserve their
measured text width and stay inside the panel; ticks use actual normalized plot
positions. Tiny regions omit scale annotations that cannot fit. A real-data
red/green browser check verifies both titles and numerical bounding boxes, with
all three cut/scaffold captures passing. Eleven frozen-export and thirteen
track/workspace regressions pass, as do the full WebUI build and focused lint.
The preceding integrated version-58 build passes 641 JVM tests, zero failures/
errors, seven optional skips. A newly stamped final figure archive remains next;
no portable qualification is inferred from these development checks.

## Retained public-data figure and transformed text measurement

`docs/publication/borrelia-editing/` now retains the real-data composition,
three original SVG panels, matching SVG/PDF provenance, embedded fonts, GC/marker
inputs, caption/alt text and reproduction commands. Fresh version-59 captures
pass source/sequence/pipeline and both-axis text-bound checks. Native counts
match all 24,025 cells of the pinned public Cooler exactly. All 155 GC windows
match independent FASTA/BigWig reads. Eleven excluded-weight bins match the
source mask; persisted coefficients differ only at <=2.995e-13 relative, explicitly
recorded rather than called bitwise equal. Every displayed palette cell matches.

The 178×179.106-mm composition has measured minimum 7.051-pt vector text.
Original contact-image bytes are retained; PDF XMP and an attached JSON record
match the SVG's scientific provenance. The figure and both scale maxima were
visually inspected. The manuscript is unchanged. This closes the real-data
composition/readability artifact prerequisite, while final journal artwork-
resolution/TIFF and overall release/native/platform gates remain open.

A new independent nested-SVG test also exposes a print-readout error: getCTM
stops at the nearest viewport and omits an outer group's scale. Measurement now
uses screen matrices normalized to the root capture. Seven browser transform
cases (plus root-font preservation), all five physical-export checks, full WebUI
build and focused lint pass. The archived images remain from committed
`2.0.59-0878f4d-webui_d27c8ac`; this newer readout fix does not change their drawing.

## Publication artwork encoding and resolution qualification

The figure's `artwork/` directory now contains vector-cell PDF/SVG, a 900-dpi
RGB TIFF in a gzip wrapper, preview, original contact PNGs and qualification
receipts. All 259,811 captured RGBA pixels reconstruct exactly, without gaps or
overlaps. Native-size whole-scene rendering is byte-identical to the immutable
composition. The PDF contains zero raster page images, embedded fonts, XMP/JSON
provenance and attached original PNGs. No genomic samples or finer binning are
invented. The TIFF is 6307×6346, internally uncompressed, with physical DPI and
provenance tags; gzip decompression restores the exact uncompressed file.

Seven independent small-field cases and the complete real figure pass. Fifteen
actual-browser file-viewer checks pass, including opening/zooming the retained
vector SVG, narrow layout, cancellation, malformed/oversized files, image-context
isolation and assembly preservation. The fresh version-60 integrated build
passes 641 JVM tests with zero failures/errors and seven optional skips. The
figure's composition/artwork/readability requirements are now verified. This
does not complete overall release/native/platform qualification or manuscript
editing; the supplied manuscript remains unchanged.


## Native integrations and portable command permissions

All five formerly skipped real native checks now pass in the full 641-test
suite: seqkit search, gapped BLAST, HTTP native search provenance, accumulated
ModDotPlot versions and Java/native math. Two large-input integrations remain
skipped; this does not qualify broader formats or other platforms.

The Linux producer now restores every checksummed manifest command's execute
bits after JAR extraction, including nested sequence tools. Seven regressions
and a real 737-MB copied payload pass. A fresh-cache Java resolver and all four
sequence CLI smoke checks run directly from that copy without extraction.
This closes the identified permission defect at source and copied-payload level.
A fresh portable artifact, cold-prefix analysis and current-version release
qualification remain open. The earlier version-55 soak remains scoped to its
original exact artifact; native tool byte identity did not establish prefix
origin. See the reinforcement record for evidence paths and precise scope.


Version 62 additionally passes 19 real sequence-browser cases using its staged
runtime, JAR, assets and tools. All 11 saved native jobs have verified command
paths and executable hashes under that prefix; the private Java temporary
directory has no fallback toolchain extraction. Snapshots/figure exports and
overlay/comparison screenshots pass. The completed Linux producer and independent archive/extracted-entry identity
checks pass, including all executable modes. The actual .run-created application
is retained under `/tmp/hict-command-permissions-extracted-01`. The full
version-62 release gate under `/tmp/hict-integrated-release-11` passes all ten
stages, including four performance checks and 225 mixed cycles over 1806.54
seconds. Frame p95 is 16.7 ms and input acknowledgement p95 is 50.93 ms, with
no recorded browser errors, unchanged native/JAR bytes, stable DOM maxima and
listener growth within the gate. This qualifies the exercised packaged Chromium
and native-tool workflows, not Tauri or other platform dialogs. Interrupted packaging
attempts are recorded explicitly in the reinforcement evidence.


## Bounded Tauri coordinate downloads and native save qualification

Tauri 0.1.2 now opens the native chooser before connecting, then downloads
revision-frozen FASTA/AGP directly in Rust and writes 64-KiB chunks to an owned
sibling temporary file. Sequence bytes do not cross the WebView. Blob exports
use ordered acknowledged binary IPC with the same staged commit and per-window
ownership. Storage failures/cancellation preserve the original, successful saves
alone advance per-purpose directories, and graceful native close/Quit wait for
cleanup. App command handlers, generated permissions and the existing localhost
capability agree. Bind/CORS and the native local-host policy remain unchanged.

The isolated Ubuntu 22.04/Rust 1.88/WebKit 4.1 SDK builds the full wrapper and all
18 Rust tests pass. Full WebUI build and transfer-contract checks pass. A real
GTK bounded-IPC-only prototype exposed WebKit's response buffering; the native
HTTP implementation resolves that transfer path. The final development binary
saved 512,000,000 sequence bases (518,400,014 file bytes) in 3.04 seconds, with
17-ms frame p95 and 7.22-ms acknowledgement p95. Its output matches the retained
byte hash and independent sequence checksum. These are host measurements.

The committed repeatable runner passes eight cases in trial 04: chooser Cancel,
confirmed AGP overwrite, complete FASTA identity/responsiveness, 8-MiB full-volume
preservation, session save/restore, transfer Cancel, File Quit and native
WM_DELETE_WINDOW close. The last two resumed after correcting an observer that
mistook the expected lost click acknowledgement on Quit for a failure. Earlier
late attach-notification and truncated-process-name observer errors are recorded.
No source dataset changed. Real SVG/PNG/PDF saves and opening the saved SVG also
pass in trial 03, including an 887,503-byte multi-chunk SVG.

This qualifies the exercised Linux native development pair, not an updated final
portable artifact. WebDriver DELETE /window left a partial file in both
prototypes; do not equate it to graceful native close or claim hard-crash orphan
recovery. Automated WebKit suppresses HTML open choosers, so restore/view checks
supply actual saved files through WebDriver file input. Native navigation,
renderer loss, broader real-data native workflows, other platforms and final
installer/package gates remain open. See `tauri-native-validation.md` and the
reinforcement evidence for exact hashes, receipts and reproduction commands.


## Version-63 package qualification and native lifecycle follow-up

The version-63 producer succeeds. The actual .run-created application, staged
application and all 3,100 tar/extracted file/link entries match in bytes and modes.
Its ten-stage packaged Chromium gate passes, including 225 mixed cycles over
1800.56 s: frame p95 16.7 ms, input acknowledgement p95 48.03 ms, unchanged
632-node and 224-listener maxima. Ordinary packaged Tauri coordinate cases and
real-map export checks also pass. This does not erase the later lifecycle failures.

A controlled native Blob writer left 64 KiB staged after refresh. Actual SIGTERM,
matching Unix launcher Stop, left an 87-MB staged FASTA. A late native chooser
also exposed Tauri's custom-protocol-to-postMessage retry opening a second dialog.
All failures are retained as negative controls; original final destinations were
not replaced by partial files.

Tauri 0.1.3 now gives each page an independently revocable generation, captures
it before showing a chooser, rejects stale writes/commits/answers, and cleans only
the replaced generation. Navigation invalidation does not wait on the disk mutex.
A window-scoped chooser guard prevents overlapping/replayed dialogs. Unix
SIGTERM/SIGINT route through awaited application cleanup, and legacy byte-array
saves share the registered staged writer. No bind/CORS or remote submission
behavior changes. Existing dependency pins/checksums remain intact.

The final development executable passes 23 Rust tests, four native lifecycle
cases, eight coordinate-export cases, 17 real-map/render/export/reset checks and
nine legacy IPC checks. Native HTTP refresh cleanup takes 78 ms and SIGTERM
cleanup 111 ms in the recorded run. The 512-Mb output retains its independent
sequence/file hashes. The full WebUI build passes. Source/payload commits and the
updated portable artifact are the next gates; these development checks do not
qualify Windows/macOS, hard kills or arbitrary portal/runtime variants.


## GitHub Actions audit before the next push

The owner now authorizes pushes and cancellation of superseded Actions runs.
WebUI CI is green on its published head. JVM Windows/macOS tests fail because
example provenance canonicalizes a data-root spelling used in conversion-cache
keys; Windows also retains mapped BAM index state during temporary-folder cleanup.
These failures block the portable publication gate even when package builds pass.
A new Linux alias test reproduces the provenance defect before the fix. Lookup
now supports supplied/canonical spellings with real-file identity preserved, and
Windows uses nonmapped BAM index I/O. Full local validation passes 642 JVM tests,
two optional skips, one native-baseline parity test and 25 workflow contracts.
Remote Windows/macOS validation of the next exact commits remains required.
See `ci-release-validation.md`; keep checking branch CI before commits/pushes and
monitor replacement runs, preserving the existing publication gate.


## Published lifecycle fixes and platform CI follow-up

WebUI `3804e455` is green on all four GitHub platforms. JVM `2f923de3` passes
Windows and Linux in push/PR CI; macOS ARM identifies an error in the new alias
fixture's assumption that TempDir is canonical. Its canonical branch now calls
`toRealPath()` explicitly, retaining both lookup cases and the original
cross-platform regression. Focused cache/provenance tests pass locally; the
replacement macOS/package gates remain required. The integrated version-64 JAR
build passes with the committed WebUI and the exact GUI-qualified Tauri binary.


## Export disconnects and the remaining Windows portable regression

JVM `55448708` passes both four-platform Java matrices and WebUI `3804e455`
passes its four-platform matrix. Portable Windows then fails a Rust writer
fixture before its intended rename failure: copying directory permissions onto
a file returns Windows error 87. The fixture now opens a valid destination,
writes staged bytes, then creates the blocking directory before committing.
No assertion or native test is skipped. All 16 shared Rust writer tests and the
full local WebUI build pass; replacement Windows CI remains required.

A real TCP disconnect test reproduces another issue: a client can leave while
the JVM continues preparing FASTA. Export-domain requests now cancel only their
own worker on connection closure, discard queued work and reject work submitted
after its client already disconnected. Four new tests cover preparation/temp
cleanup, transfer cleanup, queued isolation and late submission. Full local
validation passes 646 JVM tests, two optional skips and native parity.

The version-65 archive identity passes, but its subsequent packaged gate does
not qualify: local disk exhaustion interrupted evidence persistence during the
structural stage. Four packaged native lifecycle cases pass; the coordinate
suite completes seven cases and then times out on the final close case during
the same storage incident. A later native pre-header probe cannot be counted
as passed after an invalid WebDriver session; its diagnostics and screenshot
are retained. Fresh native/package qualification of the next increment remains
required. The runner now requires input-copy space plus 2 GiB headroom and
atomically replaces its result file so failed writes retain the prior evidence.

The owner also authorizes cleanup of task-created duplicate test inputs/exports
under `/tmp/hict-*` after exact verification against retained originals/lossless
archives and checks for active consumers. Original datasets, release archives
and test logs/results remain protected. The first authorized batch reclaims
1,906,694,306 bytes; receipt is retained.


The fresh native development trial 11 now passes all eight ordinary coordinate
export cases on the version-66 server code. The 512-Mb FASTA retains its file
and sequence hashes; recorded frame p95 is 20 ms and acknowledgement p95 is
11.95 ms. Native window close cleans staging in 243 ms. This uses an immutable
intermediate server JAR and packaged version-65 runtime/WebUI/native snapshots,
so final committed package qualification remains required. Receipt:
`/tmp/hict-tauri-streaming-native-11/native-coordinate-qa-01/qualification.json`.


Fresh native development trial 12 also passes all four lifecycle cases on the
intermediate version-66 server: Blob refresh, late chooser answer, HTTP refresh
and SIGTERM. Recorded HTTP refresh cleanup is 85 ms and SIGTERM cleanup 136 ms.
Receipt: `/tmp/hict-tauri-streaming-native-12/lifecycle-qa-01/qualification.json`.
The coordinated source increment pins WebUI `e15f17d2b8440544e61a4ba62b7547ca3671e592`
and version `2.0.66-5544870-webui_e15f17d`; its post-commit WebUI build passes.
Final package and replacement platform CI gates remain pending.


## Patched Electron runtime and early native CI checks

The next source increment updates only Electron in the npm lockfile, from
41.10.7 to exact 43.8.10. The full audit no longer flags that delivered runtime,
while 28 other development/build advisories remain. The production-only audit
alone is insufficient because Electron is declared as a devDependency.

The development executable and built payload each pass nine native export cases;
the built payload also passes eight offline/reconnect/storage cases and fourteen
real-map histogram, SVG/PNG/PDF rendering, restart and reset checks. Native GTK
controls and the 512-Mb byte identity are exercised. The figure checker now
accepts the built payload directly. The WebUI matrix adds the actual shared
Rust writer tests on Windows, Linux and both macOS architectures, before the
longer complete portable build can be mistaken for native test coverage.

The version-66 package gate fails the large map's first frame p95 (33.3 ms,
limit 33 ms) during concurrent GUI QA. A targeted run without that QA passes
16.7/16.8 ms and about 35-ms acknowledgements. Its limit is unchanged; the
failed complete gate remains retained and does not qualify the package. Repeat
the final package gate/soak without competing agent GUI tests. See
`electron-native-validation.md` for the source pin, evidence and limits.


The coordinated runtime increment pins WebUI
`72a4d80991802e6cf40e8b93751dceaa37165e19` and version
`2.0.67-bef9025-webui_72a4d80`. Its post-commit WebUI/production Electron
payload build passes; the integrated JVM build passes 646 tests, two optional
skips and native parity. The Tauri payload is being rebuilt against the same
committed frontend assets. Final native/package tests and platform release
conclusions remain required.


## Exact frontend resources in incremental builds

Artifact comparison found 56 obsolete hashed JavaScript chunks in the local
version-67 fat JAR: 120 bundled files versus 64 current frontend files. Every
current file matched, but additive Gradle Copy retained old generated chunks.
`copyWebUI` now synchronizes its generated destination. It skips synchronization
when an optional frontend build is absent, preserving the API-only fallback.

The version-68 incremental build now has exactly 64 frontend files in the JAR,
all byte-identical to the current dist, with no extras or missing files. A
missing alternate frontend probe also preserves all 64 existing files without
fetching another checkout. Full validation passes 646 JVM tests, two optional
skips and native parity. The source pin remains WebUI `72a4d809`, whose new
four-platform CI step passes all 16 native writer tests, including the Windows
rename failure. Final artifact/native/workflow/soak gates remain required.

## Version-68 native acceptance and remaining format audit

Compared with November 15, 18:00 UTC+4, the committed branch has 68 JVM and 52
WebUI increments. The full source/commit comparison and task-level progress
report are retained under `build/release-evidence/2.0.68/`. Commit counts describe
history, not completed acceptance requirements.

The actual version-68 `.run` extraction passes the 3,102-entry archive identity
check. Its final Tauri payload passes eight coordinate/export and four lifecycle
cases. Its final Electron payload passes nine coordinate/export, eight recovery
and fourteen figure/rendering/storage cases. All 43 cases use the final packaged
payloads and JRE; a full JDK is used only for fixture generation. Native tests
retain exact 512-million-base FASTA byte/sequence checks, real GTK controls,
failed-write preservation, session readback and refresh/shutdown cleanup.
Small receipts are archived durably under `build/release-evidence/2.0.68/final-*`.

The complete serial package gate passes all workflow stages and all four maps'
strict fresh/repeat performance checks. The full mixed soak passes 1,803.74 seconds / 227 cycles: frame p95 16.7 ms,
acknowledgement p95 44.34 ms, no unexpected errors and unchanged native/JAR
hashes. DOM maxima remain 632; listener maxima fall from 228 to 224. Browser
and JVM heap slopes are 0.0781 and 0.0606 MiB/min; JVM RSS trends downward.
Current Java push/PR and WebUI matrices
are green on four platforms; the sequence-tool matrix passes all eight native
components and four assemblies. Both push and PR Autobuild Release pass. The
Windows Portable Packages job fails before its HTTP cancellation assertion,
while its directory-entry metadata observer waits for the staging size. A
live-handle metadata change is prepared, preserving the two-second cancellation
bound and every assertion; all 23 full native tests pass locally, while replacement Windows validation
remains required. See `ci-release-validation.md`.

The remaining static control audit finds four stale anchors. Their reviewed
updates now reconcile all 232 contracts, with 227 inspected bindings, five
historical ambiguities and no inferred runtime passes. The broader format audit
also finds that VCF reference spans ignore INFO/END. New regression tests cover
legacy and modern symbolic/reference-block spans, exact cuts/reversal, gzip,
long coordinates and malformed-input preservation. All five original cases fail meaningfully before the reader correction.
Afterward, six VCF and three existing sequence-track cases pass, including
fifteen rejected malformed/overflow inputs. The full updated source suite passes 652 JVM tests (zero failures/errors,
two optional skips), native parity and the complete WebUI build; final packaged acceptance of the new reader remains required. A separate BigWig
aggregation/label audit and broader formats, accessibility, download-only browser
and AppImage qualification remain open. This is not a final release sign-off.

The coordinated follow-up is version `2.0.69-bf6ee7a-webui_ea5c2c7`,
with WebUI `ea5c2c76d80db1906c1202f7ac531bcbe72765e2`. Its post-commit
WebUI build passes. Replacement CI and packaged acceptance remain required;
the version-68 qualification above retains its exact source/artifact scope.


## Signal semantics and current Windows confirmation

The next source increment corrects BAM depth across skipped/deleted CIGAR bases,
uses immutable source read centers, and weights BAM/BigWig coarse means by bases
through native partial bins, cuts and reversal. It invalidates only their old
precompute caches. BigWig and BAM mode labels now explain their operation and
fit narrow windows. The source suite passes 657 tests (two optional skips),
native parity and the WebUI build; twenty new browser checks plus thirteen
sequence-track and fifteen feature-edge checks pass. Independent BigWig values,
unchanged input hashes and exact restored sequence coordinates are retained
under `build/release-evidence/signal-base-weighting/`.

Version-69 Windows Portable Packages now passes, including all 23 native HTTP
tests with zero ignored cases. Both JVM Autobuild Release runs and Java matrices
are green. Portable Packages and Sequence tools remain in progress at this
checkpoint. These results do not transfer package qualification from version 68
to newer source. Signed/nonfinite BigWig handling, broader formats,
accessibility/download-only browsers, Linux AppImage and the final source-matched
package gate remain pending. The full production/publication goal stays active.

The coordinated candidate is `2.0.70-e77dd80-webui_a331682`, with WebUI
`a331682b0c9058acb4f5b73f430518a2582807bf`. Static reconciliation passes all
232 reviewed control contracts (227 inspected, five historical ambiguities);
it does not infer runtime outcomes. Source-matched packaged acceptance and
replacement platform Actions remain required.


## Signed signals and Examples CI repair

The signal audit now preserves negative BigWig values through means, negative-only
maxima, persistent caches, exact edits and restored sessions. Both axes use zero
baselines and signed automatic ranges; log scaling preserves sign. Non-finite
source records produce explicit errors with coordinates. Twenty-four signed
browser checks pass across two grids, including independently checked draw
rectangles and visual review; the source suite passes 659 JVM tests with two
optional skips, native parity and the frontend build.

Candidate-70 WebUI CI and PR Java matrix are green. Its push Java matrix fails
on Windows and Apple Silicon with the same existing Examples response-handler
race. The prepared client change registers handlers before sending; full local
regressions pass without relaxing any assertion. Replacement platform evidence
remains required. Broader formats/secondary sources, accessibility/download-only
browsers, dense-track performance, Linux AppImage, final source-matched portable
qualification and dependency advisory triage remain open. Version-68 package
qualification keeps its exact-byte scope; the overall goal remains active.

The coordinated follow-up is `2.0.71-0241054-webui_d6f771b`, with WebUI
`d6f771b3a5c6d70da1c52064946519df5db23ce4`. All 232 static control contracts reconcile;
no runtime outcomes are inferred. Replacement CI and final portable gates
remain required.


## Delivered runtime dependency follow-up

The resolved runtime audit identifies eight advisories in Commons IO/Compress/
Lang, JSON-java and Snappy-java. Reviewed upstream fixes and published release
metadata support updating all five and locking every affected configuration.
The new graph adds Commons Codec transitively: 77 Maven artifacts / 79 files.
The repeated OSV query completes with zero flagged Maven components; two local
JARs and native/tool/browser payloads are outside its coverage. Reproducible
commands and exact versions are in `runtime-dependency-audit.md`.

The full updated source suite passes 661 JVM tests, zero failures/errors and two
optional skips, plus native parity. Two compatibility cases exercise delivered
native Snappy arrays/streams and byte-preserving BZip2. Candidate 71's committed
browser checks pass 24 signed-signal and ten secondary replacement cases under
both assembly authorities, with unchanged source hashes. Both candidate-71 Java
matrices and WebUI CI are green; complete release workflows and the dependency
follow-up's replacement platform/artifact checks remain required. Receipts:
`build/release-evidence/runtime-dependency-update/` and `build/release-evidence/2.0.71/`.

The dependency candidate is `2.0.72-ccdf1e2-webui_d6f771b`; WebUI remains pinned to
`d6f771b3a5c6d70da1c52064946519df5db23ce4`. Replacement Java/platform
and portable results must qualify these updated runtime bytes.


## Native dependency soundness and real Firefox downloads

The native audit finds fixable anyhow/Tokio/rustix/serde_with/XML issues. Their
compatible updates build with the declared Rust 1.88 SDK. The Linux GTK stack's
last compatible GLib 0.18.5 still contains the upstream iterator soundness bug.
A new optimized regression crashes with SIGSEGV before its two-line upstream
backport. With the pinned/verified source patch, all 24 native tests pass with
zero ignored cases. Normal frontend builds verify the original archive, every
one of its 121 files and the exact allowed two-line change; upstream licenses
and checksums remain included. The complete frontend build passes.

The repeated native OSV query checks 508 registry components and retains ten
maintenance-only notices; local application/GLib are explicitly listed separately.
The actual Linux feature graph excludes chrono. No blanket native-security
clearance is claimed. Review details and reproducible commands are recorded in
`native-dependency-audit.md`; cross-platform replacement builds remain required.

Playwright 1.65.2 / Firefox 159.0 passes eleven real download-only cases without
polyfilled writable/open-file APIs: reverse editing; session download/dirty state;
unsaved guard and keyboard focus trapping; independently checked whole/selection
FASTA and AGP; PNG/SVG/PDF; figure reopening; and ordinary HTML session input.
Source bytes are unchanged. Receipts are in
`build/release-evidence/native-dependency-update/firefox-download-01/`.
The broader final native/package, AppImage, accessibility and performance gates
remain open. Candidate 72's push/PR Java matrices are both green on four platforms;
its complete release workflows remain running at this checkpoint.

The coordinated candidate is `2.0.73-9b07395-webui_bce3207`, WebUI
`bce320752ef4d903caccbbea21d757d4cbc9cac8`. Source-matched native payload and platform release
qualification remain required; earlier package receipts retain their exact scope.

## Portable CI cache validation — 2026-11-18

Local workflow checks passed for stable native cache keys, Linux Conan reuse and
parallel JVM regression checks. Hosted warm-cache timing and publication remain
pending. See [CI validation](ci-release-validation.md#portable-ci-cache-stability-and-critical-path--2026-11-18).

Autobuild JAR caching is prepared locally. Hosted cache-hit timing remains pending.

The next JAR build includes the selected WebUI checkout’s package version in the version endpoint.

## Linux startup qualification — 2026-11-18

Candidate 75 reduced median JVM launch to working version API and WebUI index
from 4.747 s to 1.779 s on the release-test host. The trial used the published
Linux runtime and JAR with the candidate loader overlaid. A final packaged
candidate and other operating systems still require verification.
[Measurement and scope](reinforcement-validation.md#linux-hdf5-startup--2026-11-18)

## Published Linux package browser qualification — 2026-11-18

The 2.0.364 package passed citation metadata, menu destinations, structural
edits with exact undo/redo, offline use and two attempts to replace an edited
map with an incomplete file. Both attempts returned HTTP 400 and preserved
the existing map manager, assembly and pixel data. The browser test was
corrected to keep its map-manager reference in the page, avoiding serialization
of the entire object through Playwright. The package qualification continues
with the performance and 30-minute soak gates.

At 1280×720 the same package's file-open notification intercepted a click on
the assembly menu's Move to debris and hide action. Chromium against the
candidate WebUI put the menu first in pointer hit-testing at the same
coordinates, and the actual edit completed with HTTP 200. Candidate WebUI
commit `f2292f4` places open assembly and axis menus above the notifications.

## Selection clipboard and file displays beside Jobs — 2026-11-18

Following owner review of the published Linux installer, WebUI commit
`52526c1` adds clipboard actions to the Selection menu and the active
rectangle's context menu. Coordinate copies use ungrouped 1-based inclusive
assembly positions. Rectangles include separate row and column bounds.
The PNG action reuses the selection exporter and writes directly to the
image clipboard.

Chromium verification on the published Linux backend read back the exact
asymmetric rectangle coordinates, decoded a 766 × 601 PNG, and received it
through a keyboard paste event. Hi-C, FASTA and AGP remain grouped beside Jobs
at widths of 640, 800, 1280 and 1440 px. The native desktop clipboard and
hosted package require their own qualification. Browser receipts are under
`build/release-evidence/portable-publication-priority/hosted-linux-qualification/`.

Firefox, Linux Electron and Linux Tauri checks confirmed coordinate paste.
Electron initially denied PNG writes. WebUI commit `ed184e8` grants only
sanitized clipboard writes to the primary HiCT page. The isolated desktop test
then pasted a 722 × 566 PNG. The bundled Tauri shell exported valid
`image/png` at 1068 × 844, read and saved by an independent GTK process.
A paste into WebKit's own test field gave an empty image event on that stack.
The PNG and receipts are in the same evidence directory. GitHub's portable
run 44215590944 pinned `ed184e8` for all platforms.

### Terminal qualification for the published Linux 2.0.364 package

The published JAR with SHA-256
`39d107f6da823f7a689d999ed169b47a06b0c296f9487f3a3cec46e93d4ec3e9`
passed all ten stages. Chromium completed 226 cycles across the representative
maps and mixed-grid fixture in 1,802.87 seconds. Frame p95 was 16.7 ms and
input acknowledgement p95 was 55.6 ms. There were no browser errors. Browser
heap fell by 1.34 MiB, with a fitted growth rate of 0.054 MiB/min. The JVM heap
and RSS trends passed, and post-warmup peak DOM and listener counts were
unchanged. The harness verified that all ten HDF5 files and the packaged JAR
retained their initial checksums. The exact receipt and performance samples are
in `build/release-evidence/portable-publication-priority/hosted-linux-qualification/`.

## Track workspace and dense navigation qualification — 2026-11-18

The published 2.0.364 backend and candidate WebUI passed 13 track workspace
checks plus the separate numeric controls suite. The checks cover keyboard tab
switching, adding Cooler weights from either map, retained drafts, stale
requests, track resizing, and unknown-vs-unavailable weight provenance.
The signed format suite passed for plain and gzip VCF, indexed BAM and
BigWig. It verified both track axes, signed linear and log values, source
coordinates, exact cuts, reversals and session restoration.

The navigation benchmark accepted 240,000 deterministic BED annotations on
the 7,790-contig AGWG assembly, visible on both axes. Each of two runs
performed 20 zoom actions and six pans. Frame p95 was 16.8 ms in each run.
Input acknowledgement p95 was 31.7 ms in the fresh browser and 26.9 ms on
repeat. Chromium reported no long tasks, HTTP errors or page errors during
the measurements. The source HDF5 checksum was verified before and after.
Inputs, measurement receipts and screenshots are under
`build/release-evidence/portable-publication-priority/hosted-linux-qualification/`.

## Candidate accessibility checks — 2026-11-18

WebUI commit `58e05d6` names the file-picker close control, gives repeated
layer style controls unique label bindings, adds a main workspace landmark,
and provides 24-pixel targets for zoom and sidebar controls. The map's
pointer-operated slider no longer takes a keyboard tab stop. The keyboard
zoom buttons still move the map.

Axe 4.13.0 reported no confirmed WCAG-tagged violations across six Chromium
views of the candidate. Forward and reverse keyboard traversal remained
inside the picker, track manager and Jobs dialogs. Escape closed the dialogs.
The file picker and Jobs returned focus to their launch controls.
Rendered-background sampling covered the text that axe could not assess over
gradients and images. The inactive Sources and layers tab improved from
4.39 to 15.62 in the sampled modal after WebUI commit `d1e3eec`. The lowest
sampled value for active text was 4.76 on the map's range summary. The probes
cover the default theme and tested fixture. Native assistive technology and
other theme states require separate verification.

The audit, sampled contrast and screenshots are under
`build/release-evidence/portable-publication-priority/hosted-linux-qualification/accessibility/`.

## Reproduction of publication artwork with the candidate WebUI

The published Linux 2.0.364 backend and candidate WebUI regenerated the
19-replicon Borrelia figure with real ModDotPlot, GC and cut-marker tracks.
The exact left-edge cut yielded 450,123 and 460,601 bp, and the 200 bases
around it matched the source before and after scaffolding. The source inputs
retained their hashes. The regenerated BigWig is byte-identical to the
retained figure's track.

The regenerated three panels match all 259,811 retained RGBA pixels and
the whole composition's native-size pixel hash matches. The 178-mm PDF
contains zero raster page images, and the minimum vector text size was
7.051 pt. The 900-dpi TIFF passed the gzip round trip. The candidate viewer
opened and zoomed the regenerated SVG and PNG. All 16 viewer checks passed.
Receipts are under
`build/release-evidence/portable-publication-priority/hosted-linux-qualification/publication/`.

## Fresh yeast download, Info and conversion progress

A three-click browser run downloaded both catalog inputs to a fresh directory.
The Cooler and FASTA matched their catalog SHA-256 hashes. Download and
conversion completed in Jobs, then the 17-chromosome map with 17 generated
resolutions and its linked FASTA opened. The cold end-to-end time on this host
was 162.9 s, including download and balancing.

The new map passed all 16 Info cases without browser errors. Tests covered
stationary-pointer updates during delayed tile loads, current-source guidance,
map replacement and old-control disposal. The independent source check matched
all 669,584 raw cells in 17 windows. Source offsets 0–3 at 50 bp and 0–1 at
100 bp were empty. The 500 bp source diagonal contained 3,418,162 contacts.

The native tool's pyramid plan now provides the stage denominator. Replaying
the recorded 17-level hictk transcript advanced from 0 through 16 completed
levels, with the successful exit marking completion. The full JVM suite ran
663 tests with 656 passed, no failures, and seven skipped. Evidence is under
`build/release-evidence/portable-publication-priority/hosted-linux-qualification/yeast/`.

The full yeast workflow was then repeated with the candidate conversion classes
overlaid on a private copy of the published JAR, using checksum-verified cached
downloads. The browser opened the map in 149.2 s with both Jobs finished and no
browser or console errors. The live stage trace increased through 94.12% and
reported 100% once, when all 17 pyramid levels had completed.

The same candidate WebUI passed 18 remote-search workflow checks with a
controlled local provider. The cases cover consent, timeouts, cancellation,
bounded results, exports, and delivery independent of map editing. This run
used no real external search submission. The receipt is
`hosted-linux-qualification/remote-search-fixture.json` in the release evidence.

## Opening a map with no usable Cooler weights

The offline AppImage conversion produced a four-contig test map with genuine
diagonal contacts and no finite weights in any of its 30,236 real bins.
Direct HDF5 verification used the stored stripe lengths to exclude padding.
With the published 2.0.364 defaults, the map appeared blank in the manual
picker and Open Wizard. Only 11% and 7% of the respective diagonal samples
had a non-background pixel, mainly at assembly borders.

The JVM now reports on Open and Attach whether any real bin has a finite,
nonzero weight. WebUI commit `4405f9c` uses that information when applying
opening presets. A map with unusable weights opens with unweighted signal and
a short explanation. Restoring a session or an explicitly enabled custom
pipeline keeps the user's rendering configuration.

Both opening routes drew the diagonal in all 240 sampled neighborhoods with
the candidate JVM and WebUI. The sidebar, server options and Wizard pipeline
agreed. All 14 browser checks passed, including the no-dialog check. The
positive JVM regression also verified that a usable weight at a different
resolution is recognized and that padding never counts as a weight.
The full JVM suite finished with 657 passed, seven skipped and no failures.
Receipts are in `hosted-linux-qualification/unusable-weights/` under the
release evidence directory.
