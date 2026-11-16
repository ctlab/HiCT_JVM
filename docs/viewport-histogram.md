# Viewport histogram and levels

The curve below **Visualization → Color range** shows the numeric signal entering
the selected colormap in the visible rectangle. The threshold lines align with
the slider handles. **Slider scale → Linear / Log** changes the chart's horizontal
axis and adjustment sensitivity; it does not transform contact values. Changing
colors or moving a threshold does not itself change the sampled signal.

The caption distinguishes **Viewport** from **Sampled viewport**. Small regions
are queried completely. Larger ones use at most 16 blocks of 64 × 64 cells, spread
across two-dimensional strata: at most 65,536 samples and four concurrent queries.
Samples are not estimates of the total contact count or a guarantee of finding
extrema. A sparse feature may be absent from a sample. Hover the caption or chart
for zero/missing counts, values outside the slider limits, and native resolution.

At letter zoom, samples are native contact-bin references intersecting the visible
rectangle, rather than one contact per FASTA letter. Exact cuts can reference an
original source bin more than once. These are display references, not independent
physical contact observations; do not sum them as independent coverage measurements.
A boundary bin is included if any of it intersects the viewport.

The selected source and colormap input determine the signal. A custom Secondary
branch is evaluated with its own expression, including literal scaling, rather
than substituting Primary counts. In the primary Expected/O/E view, the active
traditional display signal is used, preserving its distance profile and processing
settings. Undefined O/E support is excluded as missing; a supported measured zero
is counted as zero. The usual within-scaffold positive-finite distance fitting
policy is unchanged. See [Expected and O/E palettes](observed-expected-palettes.md).

**Fit levels** sets the color thresholds to the sample's 0.5th and 99.5th percentiles
and focuses the slider on that range. This is an explicit action; panning updates
the histogram without resetting manual thresholds. Ordinary constant signals get
a small range around their value; an all-zero region uses 0–1 so zero keeps its
lower endpoint color. For O/E, positive samples define logarithmic
limits; measured zeros remain counted and receive the depleted color. Neutral is
always 1. If samples do not provide a bound on a side of 1, the corresponding
0.5 or 2 default is used. Other source colors and the numeric graph stay unchanged.
Changes publish through the same live update workflow as typed thresholds.

The curve uses 64 display-axis bins with square-root count height and a bounded
Catmull–Rom spline. Smoothing is only a drawing aid; percentiles are calculated
from the samples, never from the smoothed curve or bin centers. Samples outside
the axis are reported separately instead of accumulating in the edge bins.

Navigation, source/processing changes and raster revisions refresh the chart.
Obsolete queries are cancelled and cannot replace newer data. A failed refresh
clears the stale curve, disables Fit levels, and offers **Retry**. The detailed
histogram inspector in Rendering Pipeline remains available for axis and binning
controls. A draggable color midpoint is still planned separately; the O/E neutral
point must remain at 1.

For API clients, `/matrix/query` in `TRADITIONAL_NORMALIZED` mode now matches O/E
rendering: missing ratios are NaN in floating-point binary output and null in JSON.
They are distinct from supported zeros. Integer output cannot represent NaN.
