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
  explicit paths per verified increment. Never push, merge or create release tags.

## Execution and coverage

| Package | Advisor items | Status |
|---|---|---|
| P1 Exact cuts, clipped bin geometry, sessions and exports | 21 | In progress: exact API/scissors/feature-edge actions, source synchronization, v4 replay, long offsets, approximate Cooler export and main-map base scale implemented; primary FASTA/BED6/GTF/GFF3 cuts, wheel/overview/viewport exports/session workflows verified; broader tracks/secondary/selection coverage and release qualification pending |
| P2 Entity/region selection, coordinates, FASTA measurement | 2, 7, 13, 17 | In progress: entity/region gestures, coordinates, scoped borders and FASTA interval/repeat measurement implemented and primary Chromium verified; platform, secondary-source, accessibility and large-map qualification pending |
| P3 File destinations, saved/cancelled/failed results, sessions | 4, 6, 15 | In progress: shared save outcomes, confirmed sessions, frozen revision-guarded FASTA/AGP, browser/Electron streaming, per-purpose file memory and File Examples implemented; primary browser and real Linux Electron dialog/512 Mb transfer/quit workflows verified; Tauri/download streaming, other platforms and wider chooser coverage pending |
| P4 Tracks/sources separation, feature actions, resize | 9, 10, 11 | In progress: Center/Zoom and copied coordinates corrected; base-grid features/signals include hidden/binless sequence with native-weight provenance, browser verified; 1D/source tabs, file/map-weight Add choices, guarded editing and aligned resizing implemented; final acceptance is recorded below, with broad formats, platforms and performance pending |
| P5 Alignment runs/full cells and FASTA navigation | 5, 8 | In progress: continuous exact-boundary lines, short-repeat profile, source-checked CIGARs and vector exports implemented and primary Chromium verified; broader source/platform/performance qualification pending |
| P6 O/E controls, coverage explanation, viewport histogram | 19, 20, 22 | In progress: coverage help, O/E colors/alpha, persistence, presets, active primary O/E editor, ratio nodes, live thresholds, viewport histogram/levels, exact derived-view limits, normalization draft protection and draggable ordinary color midpoint implemented and Chromium verified; broader source/NaN, native/performance and final qualification pending |
| P7 Jobs/examples, FASTA-only, OSD and diagonal provenance | 12, 14, 16 | In progress: Jobs-first downloads/conversions, guarded opening, cache integrity, live cancellation/retry, Info and yeast diagonal provenance verified; full recipe prefilling and per-source session provenance implemented and verified; broader wizard/public examples and release qualification pending |
| P8 Opt-in remote BLAST with external-subject results | 18 | Pending |
| P9 Export viewer, immutable cut/scaffold figure snapshots | 3, 11 | Pending |
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
