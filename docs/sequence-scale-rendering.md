# Sequence-scale contact colors

`GET /get_tile?source=PRIMARY&bpResolution=1000&sequenceScale=true` can draw a
native contact map on a uniform one-base display grid. `source=SECONDARY` uses
that source's own assembly and stored resolution. The WebUI now integrates this
projection into the main map on servers advertising `sequenceScaleTiles=true`.
It does not add a 1 bp contact resolution to the file.

With this option, tile positions index current contig-tree assembly base
coordinates. For `PNG_BY_PIXELS`, `row` and `col` are start bases and `rows` and
`cols` are base counts. Other formats retain tile-number indexing, with each
output pixel covering one base. Intervals are zero-based and half-open. This
projection does not insert AGP gap bases or use a padded FASTA export domain.

The projector finds intersecting contigs, maps retained bases to their original
owned native bins, reads a bounded native matrix window in BINS units and renders
its colors. It then copies those exact colors over the bases covered by each
cell. Short terminal bins and exact-cut fragments are clipped to their actual
sequence widths. Reversal uses source-bin origins and strand. Hidden components
with native bins remain available at sequence scale; bases with no native bin,
and coordinates outside the assembly, stay transparent.

There is no contact interpolation, fractional redistribution or new observation.
Graphs run at native bins: resolution, balancing, scaling, coordinate and track
inputs retain native-bin semantics. Upper/lower source branches are selected at
display-base positions, including within a native diagonal cell and after Swap.
Native cell colors can therefore be rendered with each triangle's branch before
copying them onto the base grid. Expected/O/E retains native-bin fitting semantics.

JSON results include `displayGrid`, which declares `bpPerPixel=1`, the actual
`sourceBpResolution`, bounds and `SOURCE_BIN_REFERENCE_COLORS`. All successful
formats also return `x-hict-display-grid: SEQUENCE_BP` and
`x-hict-source-bp-resolution`. Native-cell statistics are deliberately omitted
from these images because they would not describe the expanded display samples.
Existing source contrast ranges remain available. Clients must keep stored
contact resolution distinct from the display scale and must not infer base
contacts or counts from the raster.

The option requires an explicit source. It cannot be combined with
`gridBpResolution` or `gridSampleFactor`. Edges are bounded to 1024 output bases
and at most 1,048,576 native cells per request. Oversized native windows fail with
an actionable error. Projection checks cancellation during axis preparation and
output copying. The original native file and contact arrays are unchanged.

Native pipeline bounds now use the current assembly's BINS length, which can
exceed its physical source-bin count after exact cuts. This keeps all valid
shared references colored. Numeric pipeline BINS queries retain their own axis;
their track samples include hidden bins and preserve padding. Cooler weights
are read from the matching ATUs. Feature tracks build an assembly-bin projection
without changing the visible-track query path. Numeric track sampling is bounded
to 1,048,576 bins.

Validation covers every interior cut on both strands at two native resolutions,
nested cuts, reordering, padding, hidden and missing bins, original weights,
positions beyond 2.5 Gb, cancellation and per-base triangle assignment. The
Chromium checker decodes actual server PNGs and compares native colors channel
by channel for primary/secondary sources with both standard and graph renderers.
Its diagnostic preview is an acceptance artifact separate from the main UI.
Large-map performance, all track formats and packaged-platform qualification
remain separate release gates.

## Main-map coordinate transition

The main map enters a uniform base axis at four screen pixels per base and
retains it down to two, avoiding repeated mode changes at the boundary. Readable
FASTA letters use the loaded font's measured width. The status label distinguishes
base display scale from the stored contact-bin resolution. Native raster layers
and their coordinate snapshot remain available; a virtual display level never
overwrites a real stored 1 bp level or its hidden-bin metadata.

Wheel and trackpad navigation translate anchors through assembly base coordinates.
Zoom-limit updates wait for an active gesture to finish when the letter strips
resize the viewport. The overview retains a separate native projection and maps
its viewport/clicks through base coordinates. Saved views and bookmarks carry
optional `centerBp`, `displayGrid` and `bpResolution` fields; old documents remain
readable. BP centers are reapplied after the letter-strip layout settles, with
intervening user input cancelling that correction.

On capable servers scissors round to a base boundary and call the revision-guarded
exact-cut API. All sequence and original source-bin references remain available.
Native raster extents are refreshed when a cut adds a shared reference. Older
servers retain their existing endpoint and its explicit bin-removal warning.
The guide and actual exact-cut request use the same boundary convention.
Feature context menus also offer **Cut at feature start/end** at their projected
assembly boundaries, independent of biological strand. These are zero-based,
half-open boundaries, distinct from the one-based inclusive coordinates copied
to the clipboard. Existing contig boundaries are disabled. Targets capture the
current session/revision and are revalidated before sending an edit; stale,
read-only, busy, exporting or disposed views cannot run a delayed cut. Both
scissors and feature cuts preserve the visible BP locus while native reference
axes are rebuilt.

**Center on feature** keeps the current resolution; **Zoom to feature** uses the
measured letter scale. Both carry BP destinations across coordinate transitions.
Range navigation likewise uses BP destinations and fits large ranges without a
single-bin-scale cap. Copied loci use ungrouped ASCII digits on every locale.

Sequence-scale viewport diagnostics sample native BINS, rather than treating
expanded base colors as new matrix observations. Expected-profile requests accept
`unit=PIXELS` (default) or `BINS`, with independent caches. Histograms label native
matrix references at base scale. Both cache axes retain unordered-source-pair
support weighting. Figure export holds its coordinate basis while rendering and
restores the user's position afterward.

Qualification currently covers the deterministic primary 4 kb contact/FASTA/BED6
workflow, real cuts producing a one-base fragment, reversal, keyboard history,
wheel zoom to letters, overview navigation, viewport PNG/SVG/PDF and session
save/close/restore. See `scripts/check_sequence_view_browser.py` and the validation
ledger. This does not yet qualify the full secondary/disjoint-pyramid UI matrix,
all track formats, hidden/no-bin 1D features, selection and
whole-map exports, or native desktop platforms. Base-scale track projection now
has its own explicit query path, described below; broad format/performance
qualification remains P1/P4 work.
The additional primary GTF/GFF3 browser workflow checks both feature edges on
forward/reversed contigs, vertical/horizontal menus, a one-base feature, disabled
joins, delayed actions after undo, BP view preservation, actual clipboard output,
distinct Center/Zoom actions and restored sessions. It does not qualify feature
edits on every track format or secondary-source assembly combination.

## Base-grid 1D tracks

Servers advertising `sequenceScaleTracks=true` accept `sequenceScale=true` on
`/tracks/query_1d` and `/tracks/feature_context`, with `unit=BP`. The requested
`bpResolution` is a stored source level. Results explicitly declare
`displayGrid=SEQUENCE_BP`, `bpResolution=1` and `sourceBpResolution`. Their pixel
bounds are assembly BP; this does not request or create a stored 1 bp contact map.
Older servers retain the previous native-bin adapter. Ordinary native queries
and numeric pipeline BINS samples keep their previous paths.

The base-grid builder includes every contig-tree sequence interval, regardless
of native visibility or bin ownership. Source features and quantitative intervals
project directly onto this axis, including partial/reversed fragments. Native
precompute sidecars cannot supply these results because their axes differ.
Queries clamp to retained assembly bounds, return empty data outside the assembly,
and are limited to 8192 requested display pixels and 1,048,576 requested bases. Context
margins are measured on the base axis. Cancellation is checked between tracks,
weight components and during bounded weight projection.

Cooler weights retain the selected source's actual native values. They are mapped
through retained source-bin origins and strands, including hidden bins, then
clipped to their sequence intervals. Binless or non-finite-weight bases remain
missing. If a secondary source lacks the requested level, its finest stored
level is used and reported in that track's `samplingBpResolution`. Display
buckets take the maximum finite weight; their support counts describe display
bases, not independent contact observations. Finite support is kept separate
from coarse buckets, so masked or binless bases remain missing after aggregation.
Weight output is bounded to 16,384 intervals; highly fragmented support asks the
user to zoom in. Tooltips state the native weight
resolution and distinguish one-based inclusive assembly BP from native bin
indices. Normalization availability also uses the actual stored source level.

Validation covers per-base BED signal, GTF features and indexed BAM coverage,
shared references, reversed/hidden/binless fragments, both stored levels,
disjoint secondary weight levels and positions beyond 2.5 Gb. Chromium verifies
the actual hidden/binless/visible fixture, quantitative signal and weight values,
tooltips, normalization availability, a feature cut inside hidden sequence,
reversal and portable session restoration. BigWig/VCF, all aggregation modes,
large-file responsiveness, the full secondary UI matrix and packaged platforms
are still separate gates.
