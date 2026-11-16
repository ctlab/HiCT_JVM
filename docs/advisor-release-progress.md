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
| P3 File destinations, saved/cancelled/failed results, sessions | 4, 6, 15 | Pending |
| P4 Tracks/sources separation, feature actions, resize | 9, 10, 11 | In progress: Center/Zoom and copied coordinates corrected; base-grid features/signals include hidden/binless sequence with native-weight provenance, browser verified; track/source UI separation, resize, broad formats and performance pending |
| P5 Alignment runs/full cells and FASTA navigation | 5, 8 | In progress: continuous exact-boundary lines, short-repeat profile, source-checked CIGARs and vector exports implemented and primary Chromium verified; broader source/platform/performance qualification pending |
| P6 O/E controls, coverage explanation, viewport histogram | 19, 20, 22 | Pending |
| P7 Jobs/examples, FASTA-only, OSD and diagonal provenance | 12, 14, 16 | Pending; baseline checks verified |
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
