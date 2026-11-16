# Exact whole-map signal limits

Open **Visualization → Slider limits & data → Scan whole map** to find the
finite minimum and maximum at the current contact-map resolution. This reads every
visible display cell, including implicit zeros, in bounded 512 × 512 blocks. It
does not sample or round the extrema to float32. **Cancel scan** retains the previous
limits. Changing source, mode, processing, resolution or map invalidates an active
scan, and an obsolete response cannot replace newer results.

The scan sets the slider adjustment limits. It leaves the chosen color thresholds,
colors, graph and contact data unchanged. Current thresholds remain reachable even
if they fall outside the scanned limits. Panning keeps completed exact limits at
the same resolution. Running the same scan again reuses the result while its server
revision and signal expression still match.

In **Observed**, the scan evaluates the selected source's numeric colormap input,
including its graph transforms. Secondary uses its own expression. Percentage
identity colormaps keep the fixed 0–100 scale instead of offering an exact scan.

In primary **Expected** or **O/E**, the scan fits one expected profile across the
whole visible matrix at that resolution, using the existing within-scaffold,
positive-finite distance policy. It reuses that profile for every block. The
initial fit requires an additional full pass; it can be slow at fine resolutions.
Matrix blocks stay bounded; profile storage grows with the number of distance bins.
The result is labeled **whole-map Expected** or **whole-map O/E**. This profile is
independent of the viewport profile used for the map and inline histogram: a whole
map and a small viewport can legitimately have different limits. Scanning does not
replace the viewport's fit or change its pixels.

Undefined O/E support is missing and excluded from finite counts. A measured zero
with defined support is included. Counts are display cells, not independent
physical contacts. Exact cuts can retain multiple references to an original bin;
these must not be summed as independent observations. Hidden contigs at the chosen
resolution are outside the visible-pixel scan domain, and letter zoom does not
turn one native contact bin into multiple sequence-base observations.

For APIs, `/matrix/statistics` defaults to `PIPELINE_SIGNAL` for compatibility.
Use `TRADITIONAL_NORMALIZED` without `expression` for the primary display signal.
Metadata returns the extent and revision; subsequent blocks carry that revision.
Derived block responses identify `expectedProfileScope: WHOLE_MAP`. A rendering or
assembly change rejects an obsolete revision with HTTP 409. See the bundled
OpenAPI documentation and [Viewport histogram and levels](viewport-histogram.md).
