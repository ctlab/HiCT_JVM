# Sequence-scale contact colors

`GET /get_tile?source=PRIMARY&bpResolution=1000&sequenceScale=true` can draw a
native contact map on a uniform one-base display grid. `source=SECONDARY` uses
that source's own assembly and stored resolution. This is a rendering primitive;
the main map's mode transition, coordinate overlays and edit integration remain
pending. It does not add a 1 bp contact resolution to the file.

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
Its diagnostic preview is an acceptance artifact, not the implemented main UI.
Large-map performance, all track formats and packaged-platform qualification
remain separate release gates.
