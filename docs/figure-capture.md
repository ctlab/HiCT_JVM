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
whether weights are applied, and the active coverage algorithm version.

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

`encodeFrozenFigure` currently receives only HiCT's own captured scenes. Do not
pass arbitrary session-imported SVG markup to its PDF/vector path. A stored-image
restore path must validate bounded content and preserve the image-only boundary,
or rebuild a trusted scene before vector encoding. Keep imported markup out of
the application DOM.

P9 next requires named immutable snapshots before/after cutting and scaffolding,
comparison composition, portable session persistence/validation, and final
publication-figure artifacts. Those workflows must reuse the captured data and
provenance, preserve source identity across changes, and qualify session restore
and malformed/obsolete inputs before claiming completion. The current dialog
does not yet provide that stored comparison library.
