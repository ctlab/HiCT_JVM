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
| P1 Exact cuts, clipped bin geometry, sessions and exports | 21 | In progress: exact API/scissors, source synchronization, v4 replay, long offsets, approximate Cooler export and main-map base scale implemented; primary cuts/FASTA/BED6/wheel/overview/viewport exports/session workflow verified; feature-edge cuts, broader tracks/secondary/selection coverage and release qualification pending |
| P2 Entity/region selection, coordinates, FASTA measurement | 2, 7, 13, 17 | Pending |
| P3 File destinations, saved/cancelled/failed results, sessions | 4, 6, 15 | Pending |
| P4 Tracks/sources separation, feature actions, resize | 9, 10, 11 | Pending |
| P5 Alignment runs/full cells and FASTA navigation | 5, 8 | Pending; existing wheel-to-letters verified |
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

This is a qualified primary workflow, not completion of P1. Quantitative 1D
tracks still sample stored bins, and hidden/no-bin track components, the full
secondary/disjoint-pyramid UI matrix, feature-edge cutting, selection/whole-map
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
