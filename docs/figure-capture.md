# Figure capture and scientific provenance

Export opens a dedicated preview dialog. The initial capture includes the current
map layers, rulers/FASTA and visible 1D tracks. Choose an area and panel options,
then Refresh capture to apply changes. PNG, SVG and PDF are encoded from that
capture. They do not read the live map again when a save destination is chosen.
The preview is a PNG image; there is no zoom slider in the export dialog. The
separate exported-image viewer provides fit, actual-size and zoom inspection.

ModDotPlot is included through its actual displayed split/overlay rendering graph.
Attach its result and apply the desired display preset first. Contact sources,
identity percentages and their score scale remain distinct in provenance. A
legacy source with undeclared units remains UNKNOWN; exporting does not invent
units or change native counts. The whole-map/rectangle capture temporarily
arranges the existing renderer and restores its view and workspace afterwards.

## What a capture records

The figure records the application versions, time, source filenames, file and
FASTA fingerprints, assembly AGP hashes, units, recorded example attribution,
rendering graph, visualization settings, annotation definitions and client
presentation. The requested view is distinguished from the grid and viewport
actually rendered. A selected rectangle records zero-based half-open base bounds.
Crop geometry uses CSS pixels and identifies the map's offset and viewport size.

The source/assembly/rendering configuration is checked before and after rendering.
Pending ordinary option changes are settled, and map tiles are refreshed.
Changes detected during capture reject the result before it becomes exportable.
The completed capture is immutable: later edits cannot change its SVG string or
PNG bytes. Closing during preparation ignores late replies and cancels an active
render wait, restoring interactions and temporary layout changes.

Ordinary source fingerprints use HICT-QUICK-1. They identify a file with sampled
SHA-256 data and size; they do not assert a fresh full-file hash or authenticate
an imported session. Example attribution remains a recorded historical declaration.
Publication evidence should retain independently verified dataset hashes and the
exact release artifacts alongside figures.

SVG stores one root metadata element, including after a crop. PNG stores the
same JSON in an uncompressed UTF-8 iTXt chunk named **HiCT provenance**, without
changing image-data chunks. This follows the
[PNG text-chunk specification](https://www.w3.org/TR/png-3/#11iTXt).
PDF stores the same JSON as escaped XMP metadata under `urn:hict:figure#`.
Saving the three formats from one capture therefore retains the same scientific
record, even though the containers and raster/vector encodings differ.

Expected and O/E use a separate server rendering context for every capture.
One primary fitting profile is computed for the actual rendered window; tiles
never replace it with a live viewport profile or a per-tile fit. Its record
includes grid resolution, PIXELS/BINS units, fitted bounds, diagonal/domain
counts, fitting semantics and SHA-256 of the actual means and domains. Base-level
sequence views still fit native BINS; their one-base colors do not invent contact
measurements. Secondary-source processing keeps its own rendering graph.
The record also identifies each source's active FILE or SQRT_COVERAGE weights,
the traditional applyCoolerWeights option, and the active coverage algorithm
version. That option is not a claim that every custom graph uses weights: the
recorded rendering graph determines its own operations.

Tile requests carry the context id and use server-rendered PNG colors. Source,
assembly, normalization, rendering or track generations changing during drawing
invalidate final validation. Failed/cancelled map tiles or failed image decoding
reject capture, rather than save an incomplete figure. Ordinary viewport profile
updates from another client cannot alter the pinned profile. The overview shares
a coarser tile source, so its layers are temporarily paused during capture and
restored afterwards; it is excluded from exported content. Projected secondary
layers are promoted without a cross-fade for capture, avoiding an outgoing
transition blocking the complete frame. Ordinary navigation keeps its transitions.

The context is released after success, failure or cancellation, including when a
creation response arrives after closing the dialog. Lost responses have a
five-minute lease checked on registry access; expired entries are reclaimed on
the next access/creation. At most four contexts and a 64 MiB profile/metadata
budget are retained. Only one capture fit runs at once. By default, Expected/O/E fitting is
limited to 16,777,216 cells and 65,536 native bins per axis before allocating
profile/provenance arrays. A larger request asks for a coarser view and does not
silently change the scientific grid or native counts.

The capture retains editable SVG paths/text where supported; heatmap imagery
remains raster. Independent SVG/PDF rasterization checks compare flat interior
map colors. Font antialiasing differs between readers and is not claimed to be
pixel-identical. Check the exported figure at its intended publication size.

## Implementation boundary and remaining P9 work

`ContactMapManager.captureCurrentFigure` freezes the captured scene through
`FrozenFigure.ts`; `saveCapturedFigure` encodes it without accessing assembly
state. The old direct export methods remain available. The dialog owns its
capture generation, abort controller and preview object URL. It discards obsolete
results and releases URLs on replacement or close.
Export-only workspace resizing must not clamp the live track-panel sizes.
The workspace resize observer ignores that temporary layout while exporting.
Small whole-map captures are enlarged to a readable fitted size; large maps keep
the selected data grid and existing export bounds.

`encodeFrozenFigure` receives captured scenes or snapshots rebuilt through the
restricted scene reader. Arbitrary exported-image viewer markup must not enter
its PDF/vector path. Imported session scenes are rebuilt with an explicit SVG
vocabulary, numeric/resource limits and local clipping references. Styles and
font programs from session files are not accepted. Restored vector text uses the
bundled HiCT fonts; the original PNG bytes remain unchanged. No imported scene
is inserted into the application DOM. Embedded rasters must be bounded PNGs with
valid chunk checksums; animation, external resources, active markup, conflicting
provenance/dimensions/times and broken references are rejected before the live
session replacement request.

## Named snapshots and comparisons

Name the preview and choose **Save snapshot**. It saves the scene currently shown,
including a comparison if one is displayed. The library is included when you
save the session. **Snapshots** opens the list. Select a name to view and export
that saved panel alone, or choose **Compare to snapshot** to pair it with the
current map capture. **Current capture** returns to the map preview. Viewing and
comparing never restore an old assembly or query the old fitting context.

Capture before cutting, make the edit, reopen Export and capture again. Give the
new panel a meaningful label. Comparisons can be side by side or above/below;
**Update comparison** applies label/layout changes. All three format buttons use
the completed comparison. Panels retain their own coordinate axes, regions,
source/assembly identities, graph, normalization inputs and capture time. Their
raster pixels are placed at one output pixel per captured pixel against a white
comparison background. Vector geometry/text is retained, and per-panel clip IDs
are made unique. Root metadata contains both complete panel records and offsets.
A layout does not rescale contact values or rerender old data. Check that the
chosen regions, scales and rendering settings answer the intended biological
question.

Removing a snapshot offers **Undo remove**. Added/removed snapshots are unsaved
session changes: closing/replacing the map or leaving the page protects them.
A confirmed session write marks only the captured library revision saved; newer
snapshots stay unsaved if a write finishes late. A download-started receipt still
requires the user to verify the downloaded file, as with assembly edits. Closing
during PNG preparation or comparison ignores late results and releases temporary
URLs. Ordinary Attach retains the window's existing snapshot library.

Sessions containing snapshots use client envelope version 5, so older clients
report that an update is needed instead of dropping figures. Existing sessions
without figures retain versions 3/4 and remain supported. Figure records contain
canonical vector scenes and original PNG data. Their CRC32 and PNG chunk checks
are corruption checks, not authenticity signatures; historical metadata remains
a recorded declaration. Full-file dataset hashes and exact release artifacts
still belong with publication evidence.

The client session file limit is 32 MiB. Stored figure data has a 24 MiB aggregate
budget, at most eight entries, 64 megapixels per image and 128 megapixels combined.
Scene parsing also bounds nodes, nesting and embedded rasters. Metadata JSON is
limited to 160 nesting levels and 250,000 values, with finite numeric values.
The limit is
checked before publishing a library change. Use a smaller capture or remove an
entry if necessary; data is not silently downsampled. The server assembly/track
snapshot limits are independent and unchanged. Reference matrices/FASTA are not
embedded in the session file.

A real-data figure candidate is retained in
[publication/borrelia-editing](publication/borrelia-editing/README.md): map,
generated ModDotPlot and GC/feature tracks before cutting, after an exact cut and
after scaffolding. Its 178-mm composition has measured minimum 7.05-pt vector
text, source-data checks, caption and alt text. Its publication derivatives use
verified vector native-colour cells and provide a 900-dpi TIFF; artwork encoding/
resolution checks pass. Wider source/performance/release qualification remains open. The
stored comparison journey also covers real ModDotPlot/tracks before and after
Reverse.


## Print size

Choose **Captured size**, **Single column · 86 mm** or **Double column · 178 mm**
in the export dialog. The column choices follow the current
[Bioinformatics author guidelines](https://academic.oup.com/bioinformatics/pages/author-guidelines)
(checked 2026-11-17). Height follows the captured aspect ratio. SVG uses physical
millimetre dimensions while retaining its original coordinate viewBox; PDF uses
the corresponding physical page; PNG records pixels per metre in its pHYs chunk.
Integer pixels-per-metre encoding can introduce a small physical-size rounding
error. The original PNG image chunks and decoded pixels remain unchanged.

All three formats record the same outputSize provenance: physical dimensions,
captured raster dimensions, effective raster DPI and PRESERVE_CAPTURE_PIXELS
policy. Changing print size never refits a profile, re-queries the map, resamples
contacts or adds image detail. Source snapshots retain their original capture,
independently of the selected output size. Captured size returns the original
PNG bytes. SVG/PDF text remains vector; embedded matrix imagery retains its
captured resolution. Inspect labels and signals at the intended print size.

The dialog reports the smallest vector text at the chosen print width, including
inherited fonts, rotated labels and nested viewport/group transforms. This does
not measure text embedded in images. For a compact whole-map panel, turn off
**Expand small maps to fit window**, then **Refresh capture**. The default stays
expanded. Compact captures use the native grid; provenance records the sizing
choice. Smaller figures can retain readable text when composing several panels.

The [publisher's artwork guidance](https://static.primary.prod.gcms.the-infra.com/static/site/journals/document/images-author-guidance.pdf?node=1bf05d0b2fbd9c529a23&version=490455%3A30c2211aa70bba63a5ee)
distinguishes vector artwork from raster-resolution targets; its suggested
combination-artwork range is 600–900 dpi. The reported effective raster DPI
describes the existing capture, not a guarantee about every panel. The retained
Borrelia figure additionally has a vector-cell PDF/SVG with exactly reconstructed
native RGBA values and a 900-dpi TIFF. This adds no genomic samples. This
print-size control alone does not qualify a manuscript figure. PDF pages beyond the encoder's
14,400-point limit are rejected with a smaller-size/area recovery message rather
than silently cropped.
