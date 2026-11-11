# Release review and validation, 2026-10-18

This report tracks the supervisor review and the attached acceptance checklist.
The three quad input files under `/mnt/Models/HiCT/data/build/quad/` are read-only
test inputs. Test artifacts must be written outside DATA.

## Implementation and verification plan

1. Trace SVG recording, raster embedding, clipping and Inkscape compatibility.
2. Verify editable vector elements and document raster heatmap grouping.
3. Add map-bound cropping for viewport and whole-map exports.
4. Make border outlines optional, disabled by default; support one-pixel borders.
5. Trace PDF generation, progress and desktop save dialogs.
6. Exercise SVG, PNG and PDF with both export scopes.
7. Audit source-specific visualization settings and slider hydration.
8. Keep contrast edits limited to the selected colormap bounds.
9. Derive slider limits from current-resolution, current-pipeline values.
10. Preserve the primary expression during repeated overlay generation.
11. Check secondary resolution availability and session restoration.
12. Preserve rendering graph pan/zoom across preview and reopening.
13. Allow map and track interaction during preview.
14. Add explicit primary and secondary range nodes and colormap inputs.
15. Add histogram diagnostics for matrix and track expressions.
16. Add selection histograms with source, transform and triangle controls.
17. Simplify the default Global ruler and improve label readability.
18. Restrict zoom-out to the map-fit resolution.
19. Remove unavailable Background controls.
20. Persist visualization, tracks, labels and borders for session attachment.
21. Remove internal query-coordinate suffixes from sequence-search labels.
22. Add colored FASTA letters at base-level zoom in assembly coordinates.
23. Add explicit Launcher reset actions for JVM and browser settings.
24. Add focused behavioral regression coverage for changed contracts.
25. Run frontend type checks and production build.
26. Run relevant JVM and desktop checks.
27. Start an isolated application with the supplied real input files.
28. Exercise primary/secondary rendering, contrast, overlays and restoration.
29. Inspect exports and desktop/mobile browser screenshots.
30. Record measured results, artifacts and any remaining release blockers.

## Findings

- Canceled and old-epoch image/binary tile requests returned without settling
  their OpenLayers tile state. Loading slots remained occupied and
  `rendercomplete` could never fire. Both transports now settle abandoned tiles
  as EMPTY, with exactly-once lifecycle regression tests.
- Export progress reported only completion. Capture, font loading and PDF work
  now report intermediate progress; capture has a timeout with an actionable error.
- SVG paint attributes used RGBA/hex-alpha values unsupported by some SVG 1.1
  readers. Export separates paint and opacity and supplies image xlink references.
  Heatmaps remain embedded raster tiles; borders and labels remain vector elements.
  Selecting a clipping group in an editor can still select its contents as a group.
- Border styling unconditionally added a black outer stroke. It is now optional
  and off by default. The default colored stroke is one CSS pixel.
- Sequence-overlay presentation reconstructed PRIMARY from the shared sidebar
  store, potentially containing SECONDARY settings. Existing PRIMARY color
  expressions are now retained, including RGB/HSL/HSV expressions.
- The Attach NaN-warning probe wrote legacy settings back into the sidebar. It
  now reads diagnostics without mutating the visualization store.
- Numeric matrix dimensions were not CORS-exposed. Cross-port browsers received
  matrix bytes but interpreted their dimensions as zero. Dimension headers are
  now exposed, with a real-browser assertion.
- Sidebar hydration notifications caused unnecessary graph rebuilds. Read-only
  notifications are ignored by the editor, and rebuilds retain graph pan/zoom.
- Restricting the enabled pyramid could select a matrix layer outside that
  layer's original OpenLayers min/max resolution interval. `visible=true` was
  then insufficient: OpenLayers silently skipped the selected layer. Matrix
  layers now rely on the existing enabled-resolution visibility selector, not
  a second static interval. Generated overlays still use exact-resolution matching.

## Delivered behavior

| Review items | Implementation |
| --- | --- |
| 1-3 | Editor-compatible SVG; crop option; whole map at current resolution; optional outlines and one-pixel borders. |
| 4-6, 15, 19 | Source/colormap-specific contrast; exact whole-resolution processed-range scans; unchanged numeric processing; explicit PRIMARY/SECONDARY range nodes; retained primary expression on regeneration. |
| 7 | Preview retains the graph instance and viewport while allowing map interaction. Reopening restores graph viewport from session storage. |
| 8 | Numeric histogram inspector and selection dialog; raw, balanced and processed modes; source and triangle filters. Large windows are explicitly labelled as sampled. |
| 9, 14 | Tile lifecycle repair, export progress, capture timeout, parented Electron dialog and async Tauri save dialog. Linux native save/cancel tests pass; remaining platform and integration limits are listed below. |
| 10, 12, 13 | Larger single-line Global ruler labels, fit-limited zoom-out, Background color control only. |
| 11 | In-memory backend workspace snapshot, synchronized by the frontend and restored on Attach. This is not a new persistent session-file format. |
| 16 | Removed internal query-coordinate suffix from BLAST descriptions; retained hit/identity/alignment/E-value/bit-score information. |
| 17 | Horizontal/vertical colored FASTA letters at base-level zoom, bounded to 4096 bases, using current assembly orientation and coordinates. |
| 18 | Separate confirmed JVM-settings and browser-storage reset actions in Launcher. DATA/PROCESSED paths are retained by JVM reset. |
| 20 | Actual quad map/overlay/FASTA browser tests and exported artifacts, described below. |

Following the user's explicit choice of **exact whole-resolution limits**, automatic
contrast bounds no longer use the viewport histogram sampler. The browser visits
all 512 x 512 blocks of the resolution's displayed pixel domain, including both
triangles and implicit zeros. Contigs hidden at that resolution are outside this
displayed domain. Isolation and panning do not restrict the scan. The server
reduces finite values in double precision on the export worker pool, returning
only extrema/counts, not matrix arrays. A server-instance/revision token rejects
results spanning an assembly or rendering invalidation. The browser keeps at most
eight completed ranges keyed by resolution, expression and revision, shows pending
progress/failure explicitly, and retains usable bounds until the scan completes.
The fixed ModDotPlot percentage domain remains 0-100. Constant-valued data are
reported with their actual extrema, with a one-unit slider span for usability.

Full scans are quadratic in the displayed bin count and can take a long time at
fine resolutions; no sampling is substituted. Only one bounded block per editor
is in flight. Changing sources/resolutions or disposing the editor cancels obsolete
client work. Server invalidation currently includes cosmetic rendering changes,
so these can conservatively require a rescan. Selection/graph histograms remain
explicitly sampled (at most sixteen 256 x 256 matrix queries, or sixteen
4096-sample 1D queries); they are separate from automatic slider bounds. The
histogram is an inspection dialog, not a
brightness-curve editing tool. Changed server expressions still trigger automatic
layout, but an unchanged pipeline restores its editor nodes, histogram-only
branches, positions and viewport from bounded session storage. Export/import also
retains these details. Whole-map exports reject sides above 16384 pixels or
scenes above 64 million pixels rather than silently downsampling or exhausting RAM.

## Validation status

- `npm run build`: passed production WebUI build, TypeScript, frontend regression
  checks and Electron compilation. Additional frontend checks cover finite/signed/
  constant histograms, sampling bounds, cancellation, fractional/reversed bin
  projection, graph input preservation and both tile transport lifecycles.
- `./gradlew test -x processResources --offline`: 289 tests, zero failures,
  seven optional tests skipped. The first run caught missing OpenAPI entries for
  the new endpoints; those entries were added before rerunning successfully.
- Real Chromium acceptance: actual primary and secondary maps; all SVG/PNG/PDF
  viewport and whole-map exports; unchanged viewport after export; numeric
  expression equals twice raw counts for 144 cells; histogram dialog; Preview;
  Attach (including displayed thresholds, independent axis visibility and log
  base of a real Cooler-weights track); first 100 FASTA bases exactly equal the original source; both FASTA
  canvases visible; two independent 40 kb ModDotPlot selections at 5 kb preserve
  the PRIMARY expression. No uncaught browser exceptions in the completed run.
- Final assertions also verify that mouse-wheel zoom works during Preview,
  reopening Preview preserves graph scale/offset, and the map retains usable
  dimensions at 900 px and 390 px window widths. The sidebar moves below the map
  on narrow screens instead of squeezing it away.
- Inkscape command-line SVG rasterization and Poppler PDF rasterization completed;
  artifacts were visually inspected. Embedded web fonts may produce Inkscape
  warnings; portable fallback fonts are included. No native GUI interaction claim.
- Typical single-run image-export elapsed times on this machine were 0.1-1.2 s
  for these current-resolution test scenes. These are smoke-test timings, not
  statistically supported performance claims for arbitrary map sizes.
- `git diff --check`: passed for both repositories.
- ESLint on the new frontend modules: no errors. Linting all modified existing
  modules still reports 63 pre-existing errors; the same errors were reproduced
  from HEAD (62 in ContactMapManager, one in RenderingPipelineModal). Existing
  formatting warnings were not addressed by broad unrelated rewrites.

## Reproduction and artifacts

### Pipeline editor persistence and ordering follow-up

- Root cause: graph exports contained `liteGraph`, but imports ignored it and
  reconstructed only sink expressions. Histogram branches outside the rendering
  outputs and user positions were consequently lost. Closing/reopening similarly
  discarded these editor-only nodes.
- The importer now reconstructs supported HiCT node types and validated scalar
  properties and connections, instead of assigning arbitrary LiteGraph runtime
  fields from a file. It accepts LiteGraph's typed-array JSON positions, validates
  unique sinks, port compatibility, duplicate targets and cycles, and bounds
  graph size, depth and expression expansion. Imported sink semantics must match
  the explicit rendering expressions. Invalid import leaves the current graph
  intact. Old expression-only files remain supported.
- Session storage retains at most one 2 MB graph per server. Reopening restores
  its layout and inspector branches only when its sink expressions match the
  current server configuration. A newer server expression wins over stale cached
  layout. Unsaved changes to rendering expressions are not silently applied.
- The first real-browser run exposed another race: after accepting the NaN-weight
  prompt, an asynchronous automatic threshold calculation changed an explicitly
  set upper threshold from 37 to 705. Added pipeline mutation revisions and a
  current-server-config comparison before automatic commits; prompt follow-up
  work also carries its original revision. The subsequent run preserved 37.
- `scripts/check_pipeline_editor_browser.py` passed seven scenarios on actual
  quad input: histogram wired to selectedMax displays 37; close/reopen;
  export/import; Preview round trip; unsupported-node rejection with no graph
  mutation; stale-cache rejection; and a deterministically delayed automatic
  calculation that cannot overwrite newer explicit threshold 47. No uncaught
  browser exceptions. These tests do not generate sidecars or modify inputs.
- `scripts/check-pipeline-editor-state.mjs` covers positions, output slot 1,
  scalar-property validation, rejected script nodes/runtime fields, duplicate
  links, cycles, excessive expression expansion and equivalent-bound signatures.
  It is included in `npm run build`; production WebUI and Electron compilation
  pass. New module ESLint passes without errors or warnings.
- Evidence: `/tmp/hict-review-20261018/editor-browser/results.json`,
  `pipeline-original.json`, `pipeline-roundtrip.json`, `pipeline-restored.png`.
  The screenshot includes the expected friendly invalid-import notification.

### Histogram domain follow-up

- Found that graph histograms treated every input as a matrix, repeating row or
  column track values across the other axis. Numeric expressions now declare
  their coordinate dependencies conservatively: row-only and column-only inputs
  query one column or row respectively; scalar inputs query one cell. Mixed
  row/column signals remain two-dimensional. Numeric evaluation still uses the
  existing server pipeline. Selection histograms intentionally count matrix cells.
- The dialog refreshes on source/value/triangle changes, aborts obsolete work,
  and prevents stale responses from replacing the newer result. Incomplete
  responses are rejected rather than displayed as successful distributions.
- `scripts/check_histogram_browser.py` passed against the real quad input at
  1:250K. Built-in row and column Cooler-weight histograms each counted 1041
  samples, using a single 1041 x 1 or 1 x 1041 query. All 64 histogram bins matched
  an independent NumPy calculation from the raw query's weights (with the same
  finite-value and float32 transport rules). A constant 37 counted once.
- Evidence: `/tmp/hict-review-20261018/histogram-browser/results.json` and three
  screenshots. No track generation, FASTA linking or DATA writes were performed.
  The frontend build, focused ESLint checks, domain/sampling/cancellation unit
  checks, and repeated exact-range browser checks passed. ESLint emits existing
  schema-resolution warnings; it reported no lint errors for the two changed
  histogram modules. This does not close the desktop runtime release gates.

### Exact-range follow-up

- `scripts/check_exact_signal_range.py` passed against the temporary quad-data
  server. PRIMARY and SECONDARY at 1:1M (261 x 261 displayed pixels), processed
  `2 * PRIMARY` at 1:250K (1041 x 1041, nine blocks), and a constant 16777217
  matched full float64 matrix queries exactly. The latter guards against float32
  rounding. All 1,083,681 processed values were included in the multi-block case.
  Measured scans took 0.007-0.148 seconds in this warmed single run, not a general
  benchmark. Stale revisions returned HTTP 409 after rendering invalidation.
- `scripts/check_exact_signal_range_browser.py` exercises only read-only map
  opening and in-memory visualization changes. Panning preserves completed exact
  limits without scanning again; PRIMARY/SECONDARY give independent ranges, and
  switching back to PRIMARY reuses cached exact extrema without another scan.
- Evidence: `/tmp/hict-review-20261018/exact-ranges.json` and
  `/tmp/hict-review-20261018/exact-browser/`. The new server uses ports 5106/8186.
- The broader generation/browser suite was not rerun after this follow-up:
  auto-review rejected it because generation workflows may write sidecars through
  the known protected-DATA path issue. No attempt was made to bypass that decision.
  Prior broad-suite results below predate this exact-range change. The follow-up
  tests do not link FASTA, generate analyses, or export files into DATA.

Acceptance script: `scripts/check_release_review_browser.py`. Before rerunning
generation workflows, resolve and verify the sidecar-path issue described below;
symlinking inputs into a temporary DATA directory is not by itself proof that
every generated output stays there. The focused read-only follow-up tests use
`/tmp/hict-review-20261018/data/` and do not invoke those workflows.

Artifacts: `/tmp/hict-review-20261018/browser/`: `results.json`, `diagnostics.json`,
`viewport.{svg,png,pdf}`, `whole-map.{svg,png,pdf}`, Inkscape/Poppler rasterizations,
and screenshots of the overlay, histogram, graph, FASTA and narrow workspaces.
The browser test requires the existing validation venv with Playwright and
`PLAYWRIGHT_BROWSERS_PATH=/tmp/hict-review-browsers` on this machine.

### Isolated acceptance follow-up

The earlier generation-test rejection was resolved by changing the test setup,
not by bypassing the protected-path policy. Three **physical, single-link copies**
of the quad inputs were placed in `/tmp/hict-release-physical/data/`. A new server
on 5107/8187 used this DATA directory, a separate temporary PROCESSED directory,
and a temporary JVM/native working directory. The original `/mnt/Models` paths
were not passed to this server or these tests. Sequence jobs currently use
`DATA/processed/sequence-analysis` regardless of PROCESSED_DIR; this path contract
was traced, but not changed here. Thus all generation, precompute and provenance
writes in this run stayed under the disposable `/tmp` tree.

- `check_release_review_browser.py --url http://127.0.0.1:8187
  --api http://127.0.0.1:5107 --data-dir /tmp/hict-release-physical/data
  --output /tmp/hict-release-physical/browser` passed all 14 recorded checks.
  Both export scopes completed in SVG/PNG/PDF, with progress above zero from the
  start and exact viewport restoration. This warmed run took 0.13-1.18 seconds
  per image export; these timings are smoke-test observations, not benchmarks.
  Attach preserved styles, viewport, axis visibility, track log base and pipeline.
  FASTA bases matched 100 source bases, Preview permitted map zoom, and two
  successive 5K ModDotPlot selections retained the primary branch. The final map
  had zero queued/loading tiles and `renderComplete=true`; no browser exceptions
  were recorded. Inkscape and Poppler rendered the new viewport artifacts.
- `check_blast_browser.py` ran the real bundled BLAST tool through the search
  dialog on a 40 kb interval. It published and automatically opened a reusable
  BED, found 404 alignments, recovered the known 120-base interval in both strand
  orientations, and retained the expected red/blue strand colors. All 404 rendered
  feature labels contained the user description and alignment statistics, without
  the internal query-coordinate suffix. Evidence is in
  `/tmp/hict-release-physical/blast-browser/results.json`. The first two test
  attempts had overly strict oracle assumptions (internal BED IDs equal user
  headers; all BLAST hits exact); the final check correctly distinguishes query
  IDs, displayed descriptions, and approximate HSPs. No product change was needed.
- `check_electron_export.py --output /tmp/hict-release-physical/electron-complete`
  passed six actual GTK save dialogs (all formats and scopes), a whole-map PDF
  cancellation with no output file, and browser-storage persistence/reset checks.
  Every dialog had a nonzero `WM_TRANSIENT_FOR` parent. All export progress sequences
  reached completion; Poppler recognized the whole-map PDF as a valid single page.
  Local storage, cookies, IndexedDB and cache storage persisted across an ordinary
  Electron restart and were absent after `HICT_RESET_WEBUI=1`. Evidence includes
  native-dialog screenshots, exported files and `results.json` in that directory.
  The test runs the compiled Electron wrapper on an automatically allocated private
  Xvfb display, with temporary HOME/profile/config/cache directories. Distro Xvfb
  and xdotool packages were extracted under `/tmp`, not installed system-wide.
  The user's desktop and browser profile were not touched. DBus/portal warnings
  are expected because the test deliberately uses GTK without the user's bus.

### Edited-view acceptance follow-up

`scripts/check_edited_view_browser.py --data-dir /tmp/hict-release-physical/data`
passed against the dedicated 5107/8187 server. It used the same physical copies
described above, with no original DATA paths supplied. Results and SVG/screenshots
are in `/tmp/hict-release-physical/edited-view/`.

- Reversed the first contig, `chrX` (17,516,224 bp), through the normal selection
  event handler. Center, resolution and rotation remained exactly unchanged.
  The first and last 100 assembly bases matched an independent Python reverse
  complement of the copied source FASTA. All 38 letters drawn across both axis
  canvases matched their assembly positions and the configured base colors.
- Opened the selection histogram through the actual map right-click menu. All
  64 bins matched NumPy calculations for primary raw counts (both/upper),
  secondary raw counts (lower), primary weighted counts under the active weight
  policy, and primary processed signal. The square selection contained 1,024
  cells; each inclusive triangle contained 528. The explicit `2 * PRIMARY`
  expression produced extrema 202 and 39,638, twice the raw 101 and 19,819.
  This is a histogram/UI integration check using the matrix-query API as oracle,
  not an independent revalidation of the underlying matrix storage engine.
- Parsed both exported SVGs: each contained eight configured colored border
  strokes of width 1. The outline-enabled file additionally contained eight
  black strokes of width 4; the outline-disabled file contained none of those
  strokes. This proves the current viewport exporter retains the optional border
  styling, without conflating ruler or minimap strokes with assembly borders.
- No browser exceptions were recorded. The first test attempt stopped on an
  overly strict Playwright label selector; that test selector and the JSON oracle
  shape were corrected before the successful runs. No product change was needed
  for these additional acceptance cases.

### Enabled-resolution and ModDotPlot pixel follow-up

The actual-resolution test initially reproduced a blank primary map: enable only
5K/10K, select 5K, then magnify its bins 16 times. The descriptor correctly said
5K and the tile queue was idle, but the layer's static `minResolution=5` excluded
the view resolution `5/16`. This was a product bug, not an empty input region.
Removing the duplicate static min/max filter fixes it while retaining exactly
one selected primary layer and exact-availability checks for generated secondary
layers. Newly created layers start hidden until the selector synchronizes them.

- `check-map-viewport.mjs` now creates actual OpenLayers tile layers through the
  production manager and checks OpenLayers `inView`, not just `getVisible()`.
  It failed before the fix with no renderable primary layer, and passes afterward
  for restricted/full pyramids, fine magnification, coarse zoom and generated
  overlays with only one resolution.
- `scripts/check_moddotplot_zoom_browser.py` ran two real 40 kb ModDotPlot jobs
  at 5K. Both generations preserved 350,942 primary upper-triangle pixels exactly.
  Changing the secondary threshold to 90 retained the 0-100 percentage bounds,
  fixed-point division and primary pixels. Three 5K/10K/5K roundtrips restored
  identical interior map pixels; no secondary layer rendered at unavailable 10K.
  Raw generated identities included 1,000,000 (100% at scale 10,000). Both ordinary
  preset-list actions rendered successfully; overlay mode visibly added generated
  identities and returning to split mode preserved the primary branch/pixels.
  The largest 50 ms heartbeat gap was 58.5 ms in this run. This is a responsiveness
  smoke check, not a hardware-independent latency guarantee. There were no browser
  exceptions or HTTP 5xx responses. Screenshots/results:
  `/tmp/hict-release-physical/moddotplot-zoom/`.
- Pixel comparisons exclude map controls and the workspace notification's edge
  shadow. The first post-fix assertion included that shadow and differed only in
  the rightmost 16 pixels; the corrected test compares the map interior.
- Full `npm run build` passed, including type checking, all configured frontend
  regressions and Electron compilation. The broad browser suite also passed all
  14 recorded checks after this change, including both export scopes in all three
  formats. Final tile state was loading=0, queued=0, renderComplete=true. Expected
  aborted obsolete tile/statistics requests were recorded during transitions.
  Artifacts: `/tmp/hict-release-physical/browser-layer-visibility/`.

All new generation runs used the dedicated physical `/tmp` input copies and
server described above. No original `/mnt/Models` files were modified.

### Launcher confirmation and layout follow-up

`scripts/check_launcher_reset.py --jar build/libs/hict_server-1.0.209-3a9a100-webui_0cbecb8-fat.jar`
executes `LauncherResetAcceptance.java` against the current compiled classes and
the jar's dependencies, on a private Xvfb display. It uses a fresh temporary HOME,
settings file and data/processed paths; it never starts the real HiCT server or
opens the user's browser profile. Actual Swing buttons and parented modal
confirmation dialogs are exercised, rather than calling the reset implementation
directly. Screenshots/results from the final successful run are in
`/tmp/hict-launcher-reset-sk8wxhl_/`.

This check exposed two product issues. JVM reset hardcoded INFO instead of
restoring `HICT_LOG_LEVEL`; it now uses the same default resolver as initial
startup. More importantly, the configuration panel used an unbounded preferred
height in the NORTH of a BorderLayout, placing reset buttons below the window.
The configuration and activity/log views now share a bounded central CardLayout.
Configuration content scrolls while Save/Reset actions remain fixed. Native/log
controls have separate rows, the header title no longer overlaps its actions,
and wrapped field labels leave usable input widths at the minimum window size.

Seven acceptance checks passed:

1. Canceling JVM reset leaves memory and persisted settings unchanged.
2. Confirming JVM reset retains DATA/PROCESSED locations, restores environment
   TILE_SIZE=512 and HICT_LOG_LEVEL=WARN, and clears the custom JVM-options field.
3. Canceling WebUI reset changes neither settings nor process state.
4. Confirming WebUI reset persists both Electron and Tauri reset flags without
   modifying JVM configuration.
5. Reset stops an owned inert browser-process sentinel while an owned server
   sentinel stays alive. This verifies Launcher lifecycle separation; actual
   Electron cache/cookie/storage clearing was verified separately above.
6. Save/Reset controls lie within the minimum-size 720x500 window, and toggling
   Configuration retains field contents.
7. Scrolling reaches the final JVM field with an editable width of at least
   160 pixels. Screenshots were inspected at normal and minimum sizes.

The first visibility assertion used Swing `isShowing`, which alone does not prove
that a component is inside the window. The final test also checks screen bounds.
The scrolling check uses the actual scrollbar; JTextField's own scrolling method
scrolls its text and was not a valid form-scrolling test.

The final Java build and full JVM regression suite passed: 289 tests reported,
zero failures/errors, seven optional tests skipped. `git diff --check` and the
Python driver's compilation check also passed. These checks do not compile or
exercise the Tauri Rust wrapper.

## Rotated export follow-up

Review found that viewport cropping projected only the upper-left and lower-right
map corners. These are not the screen bounds after rotation: at 90/180 degrees,
the check could reject a visible map, and at 45 degrees it could truncate it.
`visibleMapBounds` now clips the four-corner polygon against the viewport before
computing its rectangular output bounds. Clipping the polygon rather than its
bounding box also rejects maps whose bounding box overlaps the viewport but whose
actual geometry is entirely outside it. A rotated rectangular image necessarily
retains corner background outside the rotated map; it does not discard map pixels
to manufacture a filled rectangle. Whole-map export remains unrotated and restores
the user's original rotation afterward.

Verification on physical quad Hi-C/dotplot copies under `/tmp`:

- `scripts/check-export-crop.mjs`: corner order, both winding directions, clipped
  diamonds, offscreen/touch-only geometry and invalid dimensions; integrated into
  `test:map-viewport`. Full WebUI build and Electron TypeScript compile passed.
- `scripts/check_rotated_export_browser.py`: real Chromium viewport exports at
  45, 90 and 180 degrees, plus whole-map exports starting from a 45-degree view.
  All twelve SVG/PNG/PDF downloads completed with progress reaching 100%, and
  center/resolution/rotation were exactly preserved after each export.
- Export dimensions matched independently calculated viewport bounds. PNG content,
  Poppler-rendered PDF content and Inkscape-rendered SVG content were nonblank in
  all four cases. Visual inspection of 90-degree PNG/SVG and 45-degree PDF showed
  the map and overlay, without the prior rejected/truncated crop. Rasterizer
  antialiasing differs, so these checks do not assert pixel-identical formats.
- No browser exceptions. Artifacts and machine-readable results are retained in
  `/tmp/hict-release-physical/rotated-export/`. No protected originals were changed.

This closes the rotated crop regression, not a new coordinate interpretation for
rotated rulers or tracks. Existing axis annotations are captured as displayed.

## Compressed FASTA and base-track follow-up

Inspection found that the gzip `SourceSession` path parsed the entire FASTA into
strings on every region request, including requests of only a few dozen bases.
It now uses the same random-access line index as plain FASTA. Gzip bytes are
streamed once into a private temporary file while the index is built; the genome
is never retained in a string. The cached index is invalidated when the source
path/size/mtime/file identity changes. Reader leases allow an already-open reader
to finish before a retired backing file is deleted, including on platforms that
cannot unlink open files. Map close disposes the index. Invalid input and canceled
index construction delete their partial copies.

This trades repeated decompression and genome-sized heap use for disk space equal
to one uncompressed FASTA per cached gzip source. Copies are in `java.io.tmpdir`,
never beside the input; configure that directory on a sufficiently large volume.
Plain FASTA still has no disk copy or `.fai` sidecar. Normal close/relink and failed
construction clean up automatically; an ungraceful process kill may leave temporary
files for operating-system temp cleanup.

Browser requests now abort when superseded or unmounted. Panning hides stale
letters until their new coordinates are available, without changing map size.
Letters occupy separate 24-pixel lanes inside the existing 64-pixel ruler panels,
and the ruler canvas is resized and repainted rather than overdrawn. An explicit,
awaited sequence-render callback during export avoids a debounce race and records
FASTA text as vector SVG elements.

Evidence from `scripts/check_large_fasta_browser.py` and
`scripts/LargeFastaIndexAcceptance.java` (physical quad copies only):

- 294,743,678 bases indexed with `-Xmx64m`; final run took 1.775 seconds. 7,060
  indexed intervals across the source records took 0.089 seconds. These are local
  warm-filesystem observations, not cross-hardware throughput guarantees.
- One hundred independently checked 4096-base HTTP intervals exactly matched the
  original FASTA: 0.215 seconds total, slowest 0.0105 seconds in the final run.
- Thirty rapid pans generated two base-region requests. All 160 rendered letters
  and their colors matched their final assembly positions; maximum 50-ms heartbeat
  interval was 53.2 ms. Delayed real server responses caused six request aborts;
  the final 160 letters still matched, without browser exceptions.
- Actual Assembly > Link FASTA relinking invalidated the base cache and generated
  two fresh requests. The retired gzip backing file was removed. Dedicated tests
  also cover reverse complements, changed source detection, active-reader cleanup,
  duplicate headers and cancellation cleanup.
- Geometry checks and screenshot inspection confirm nonoverlapping ruler/base
  lanes. The SVG export contains 640 base text elements (four vector copies used
  by the existing quadrant crop implementation), rather than losing them during
  export redraw. Artifacts: `/tmp/hict-release-physical/large-fasta/`.
- Full JVM suite: 291 tests, zero failures/errors, seven optional skips. Full
  WebUI regression/type-check/build and Electron compile passed.
- The broad quad browser suite was rerun after these layout/export changes:
  fourteen recorded checks passed, including all six format/scope exports,
  pipeline histogram/Preview/range nodes, Attach restoration, both FASTA axes
  and two real ModDotPlot generations. Artifacts are retained under
  `/tmp/hict-release-physical/browser-fasta-lanes/`.

## Concurrent Attach follow-up

A two-browser reproduction exposed a lost update in workspace autosaving. Browser
A saved a newer border width; browser B retained the older width and then panned.
B's full snapshot replaced A's saved styles, because the endpoint checked only
the filename. The baseline regression failed with the older width restored on the
server (`/tmp/hict-release-physical/concurrent-attach-before/results.json`).

Each successful open now creates a unique workspace session token and revision.
Workspace saves compare that token/revision atomically, copy the snapshot and
advance the revision. Stale writes return HTTP 409 without changing saved state.
The affected browser stops automatic retries, preserves its local view, and shows
a persistent status banner. Explicit Attach restores the latest saved state and
clears the banner. Reopening the same filename creates a different token, so a
previous session cannot overwrite it. Late responses cannot replace a newer
client-side token. This is optimistic protection for workspace snapshots, not
live collaborative assembly editing or automatic merging of concurrent changes.

The initial persistent-toast implementation exposed a browser crash during
programmatic dismissal in the installed toast component. It was replaced with a
reactive status banner, avoiding that component's dismissal path. The final test
waits six seconds before checking the notice, then explicitly reattaches and
checks its removal.

Verification:

- `WorkspaceStateStoreTest`: defensive copies, malformed/oversized state, stale
  revisions, fresh sessions and 24 concurrent writes competing for one revision.
  Exactly one concurrent writer commits. Full JVM suite: 295 tests, zero
  failures/errors, seven optional skips.
- Frontend API regressions cover version payloads, revision advancement, rejected
  saves and a late save response racing a newer open/attach token. Full WebUI
  regression/type-check/build and Electron TypeScript compile passed.
- `scripts/check_concurrent_attach_browser.py`: four real-browser checks pass on
  a physical quad-map copy: initial Attach, rejection of a stale pan without
  retries or local-style replacement, explicit reattach, and same-filename reopen.
  No browser exceptions. Results and screenshot:
  `/tmp/hict-release-physical/concurrent-attach/`.
- Broad quad-browser regression rerun: all fourteen recorded checks passed,
  including six format/scope exports, histogram/Preview/range nodes, ordinary
  Attach restoration, FASTA tracks and two ModDotPlot generations. Evidence:
  `/tmp/hict-release-physical/browser-workspace-cas/`. These tests used only
  dedicated physical input copies under `/tmp`.
- `POST /workspace/state` now requires `expectedVersion` from `/open` or `/attach`;
  the response supplies the next version. OpenAPI documents the contract. Deploy
  the matching backend and WebUI together; older clients cannot autosave through
  the new versioned endpoint.

## Linux Tauri native acceptance

The earlier toolchain blocker was worked around without changing system packages.
Rust 1.90.0 and the matching installed WebKitGTK 2.52.6 development packages were
downloaded into `/tmp`; the Rust archive was checked against its official SHA-256.
The locked dependencies compiled in both debug and optimized release profiles.
The first build exhausted the limited free root-disk space. Only its disposable
build directory was moved into private RAM-backed temporary storage; user data and
previous test artifacts were not removed. The release executable was retained and
the temporary toolchain/build directories were cleaned up after verification.

Source inspection found a reset-ordering risk: Wry 0.55.1's Linux generic
`clear_all_browsing_data` queues the WebKit operation and returns without waiting.
Starting navigation immediately afterward can therefore race data removal. The
Linux wrapper now navigates only from the successful completion callback, and
reports failures instead of loading a potentially stale page. This follows the
[WebKitGTK asynchronous clear contract](https://webkitgtk.org/reference/webkit2gtk/stable/method.WebsiteDataManager.clear.html).
No arbitrary delay or polling is used. Other platforms still require their own
runtime validation, including reset completion ordering.

`scripts/check_tauri_export.py` launches the production wrapper with a private
Xvfb display, isolated HOME/XDG directories and session D-Bus. A localhost fixture
calls the same production `save_export` IPC command used by ContactMapManager;
there are no mocked native dialogs. It uses the six real quad-map image artifacts
from the preceding browser suite. The fixture itself does not render the map.

Final optimized-release results:

- Six parented GTK dialogs saved viewport and whole-map SVG/PNG/PDF successfully.
  Every output's SHA-256 matches its browser-rendered input. JavaScript heartbeat
  ticks continued while the native chooser was active.
- Cancel returned null, did not change an existing PDF, and left the wrapper
  usable. The test checks the actual window's native transient-parent property.
- Three fresh cycles preserved cookies, localStorage, IndexedDB and Cache Storage
  across ordinary restart, then cleared all four before the first fixture load
  with `HICT_RESET_WEBUI=1`.
- All thirteen recorded checks passed for both debug and optimized release
  builds. The initial automation failures were GTK filename-completion behavior;
  the final driver accepts the prefilled filename with the actual Save button.
- Evidence: `/tmp/hict-release-physical/tauri-native-final/` (debug) and
  `/tmp/hict-release-physical/tauri-release/` (release), including dialog screenshots,
  six exported files and `results.json`. The verified optimized executable is
  retained at `/tmp/hict-release-physical/tauri-release/hict-tauri-browser`.

To reproduce with an already built wrapper and browser-suite artifacts:

```sh
python3 scripts/check_tauri_export.py \
  --binary /path/to/hict-tauri-browser \
  --exports /tmp/hict-release-physical/browser-workspace-cas \
  --output /tmp/hict-tauri-new-run \
  --xvfb /usr/bin/Xvfb --xdotool /usr/bin/xdotool
```

The output directory must be new. Linux native acceptance requires a working
session desktop portal with a file-chooser backend; the tested environment uses
`xdg-desktop-portal-gtk`. These IPC/native-save tests complement the Chromium and
Electron map-rendering tests, but do not prove full WebUI rendering in WebKit.

## Tauri integrated rendering regression

The follow-up runs the built WebUI inside the actual optimized Tauri wrapper,
opens physical temporary copies of both quad maps, and invokes the production
SVG/PNG/PDF exporters. This exposed two defects that the earlier byte-preserving
IPC fixture could not detect:

1. PDF export completed, but only the minimap raster and vector annotations were
   present. svg2pdf 2.8.1's data-URI regex fails on large PNG payloads in this
   WebKit runtime: a 12,000-character body matches, while 1,500,000 and 4,000,000
   characters return no match. The library falls back to URL loading, misidentifies
   the image format, then catches the image error and continues with no heatmap.
   The diagnosis is recorded under
   `/tmp/hict-release-physical/tauri-pdf-regex-length/`; the original incomplete
   exports are under `/tmp/hict-release-physical/tauri-webui/`.
2. Setting the workspace width did not prevent its parent flex layout from
   shrinking it. Whole-map output therefore contained only a central horizontal
   slice. Preventing shrinkage exposed a further 15-pixel loss from legacy
   `padding-right` on the map target. That padding is now removed, and whole-map
   capture temporarily sets explicit minimum dimensions and disables shrinking.
   Original workspace styles and the user's view are restored afterward.

PDF conversion now uses temporary blob URLs for embedded PNGs, with a `.png`
fragment for svg2pdf's format detection. Duplicate cropped references share one
blob. URLs are released after conversion, including failure paths; the exported
SVG remains self-contained and unchanged. This avoids patching dependencies or
globally replacing image/console APIs. The normal canvas raster is retained and
annotations remain vectors. Unit tests cover deduplication, URL lifetime, failure
cleanup, rejection of external images and preservation of the original SVG.

Final integrated evidence: `/tmp/hict-release-physical/tauri-webui-final/`.

- Six real native save dialogs produced viewport and whole-map SVG/PNG/PDF.
  Progress began above zero and finished at one; center, resolution and rotation
  were identical before and after every export. Native-dialog heartbeat checks
  passed, and canceled PDF saving left the existing file's SHA-256 unchanged.
- The full square map is now 1798 x 1798 pixels, including its aligned margins,
  rather than a 1063 x 1798 central slice. Its rulers begin at zero on both axes.
- Inkscape and Poppler independently rasterized the saved SVG/PDF at 96 dpi.
  Excluding minimap/rulers, all four comparisons against PNG have mean absolute
  RGB error below 0.43 on a 0-255 scale. Red-signal recall is above 99.97%, and
  dark-signal recall above 99.57%. This explicitly detects a missing heatmap;
  file existence or a successful save alone is insufficient.
- Three ordinary restarts preserve the fixture's cookie, localStorage, IndexedDB
  and Cache Storage; three reset restarts clear all four before the WebUI loads.
  Both map sources render after each startup. Twenty-four result records pass.
- Full frontend regression/type-check/build and Electron TypeScript compile pass.

Reproduce against a dedicated backend with physical copies, not original DATA:

```sh
../HiCT_Utils/.venv-gse-validation/bin/python scripts/check_tauri_export.py \
  --binary /tmp/hict-release-physical/tauri-release/hict-tauri-browser \
  --webui ../HiCT_WebUI/dist --api http://127.0.0.1:5107 \
  --data-dir /tmp/hict-release-physical/data \
  --output /tmp/hict-tauri-integrated-new-run --reset-runs 3
```

The integrated mode additionally requires Pillow, numpy, Inkscape and Poppler.
It tests actual native dialogs on a private X display and fails on svg2pdf image
warnings as well as missing raster content. The existing `--exports` IPC-only
mode remains available without those rasterization dependencies.

The same independent check on Chromium output exposed fractional-page resampling
in Inkscape. A 751.317-pixel SVG page rasterized to 751 pixels, while the matching
canvas used 752 pixels. Error increased toward the far edge despite unchanged
map values. Cropped SVG page dimensions/viewBox now use the same integer canvas
dimensions; the actual crop bounds and vector positions remain unchanged. A unit
regression checks that fractional geometry is neither rescaled nor truncated.
A further native run after this page-sizing change passes all sixteen checks
(one reset cycle), with identical pixel-comparison metrics, at
`/tmp/hict-release-physical/tauri-webui-final-page/`.

Final Chromium verification in `/tmp/hict-release-physical/browser-export-final/`
passes all fourteen workflow checks, with no page exceptions: six exports,
numeric pipeline queries, histogram/Preview/range nodes, Attach restoration,
source-checked FASTA bases and two ModDotPlot generations. All four independent
SVG/PDF raster comparisons now pass with mean absolute RGB error below 0.43,
red-signal recall above 99.86% and dark-signal recall above 99.51%. Export times
in this warmed test range from 0.08 seconds (viewport PNG) to 1.41 seconds
(whole-map PDF); these are case-specific observations, not hardware guarantees.

The user's exact whole-resolution slider choice was rechecked separately:
`/tmp/hict-release-physical/exact-range-final/`. Independent full float64 queries
match all four blockwise extrema cases, including a constant of 16,777,217 that
cannot be represented exactly in float32. Browser tests confirm independent
Primary/Secondary limits, no rescan after panning and cached extrema on returning
to Primary. Focused ESLint has zero errors; pre-existing style/unused-import
warnings and configuration-schema warnings remain. Python acceptance-driver
compilation and both repository whitespace checks pass.

## Cross-platform reset completion audit

The platform audit found that Wry 0.55.1's generic clear operation starts
asynchronous work and returns immediately on both Windows and macOS. Its
completion handlers are empty in the pinned [WebView2 implementation](https://github.com/tauri-apps/wry/blob/wry-v0.55.1/src/webview2/mod.rs)
and [WKWebView implementation](https://github.com/tauri-apps/wry/blob/wry-v0.55.1/src/wkwebview/mod.rs).
Calling navigation immediately afterwards could therefore load stale settings.
HiCT now uses each engine's completion callback before navigating, as it already
does for WebKitGTK. Windows setup/clear failures are logged and exit unsuccessfully.

The Launcher also removed its pending reset flag after only 1.2 seconds of
process survival, or a clean GUI handoff. This did not establish that clearing
had finished. Both wrappers now emit `HICT_WEBUI_RESET_COMPLETE` after clearing;
the Launcher consumes only the corresponding requested engine's flag on that
exact acknowledgement. Missing acknowledgements and delayed failures retain the
request for the next launch. Older wrappers without the acknowledgement retain
the pending request rather than silently losing it.

Validation in this audit:

- `cargo check --locked` passes for `x86_64-pc-windows-gnu` and
  `aarch64-apple-darwin`, including the final acknowledgement code. An isolated
  Rust 1.90 compiler, target standard libraries and Clang were used on Linux;
  no system packages were installed. These are compile checks, not native
  runtime tests. The manifest now declares Rust 1.88, already required by its
  previously locked `time` and `serde_with` dependencies.
- The Linux wrapper builds against the installed WebKitGTK runtime using
  development headers extracted into an isolated sysroot.
- Ten real Launcher GUI/process acceptance checks pass in
  `/tmp/hict-launcher-reset-w8a_2myu/`. The added cases cover clean handoff with
  no acknowledgement, delayed browser failure, and delayed successful clearing
  that consumes only the Electron flag while retaining the Tauri flag.
- Exact whole-resolution scan regressions were rerun successfully. The user's
  chosen policy remains whole-resolution, not viewport-dependent or sampled.
- Full `./gradlew test` passes: 295 tests, zero failures/errors, seven skips.
  This also rebuilt/type-checked the frontend, ran its domain regressions and
  compiled the Electron entry point.
- The rebuilt Linux Tauri wrapper passes all 24 integrated records in
  `/tmp/hict-release-physical/tauri-reset-ack-snapshot/`, including all six native
  exports, cancellation, viewport preservation, three storage persistence/reset
  cycles and four independent SVG/PDF-to-PNG content comparisons. Each reset
  emits exactly one acknowledgement before the page is ready. Mean RGB error
  remains below 0.43/255; red recall exceeds 99.97%, dark recall exceeds 99.57%.
  The tested binary is retained with those artifacts.

An initial desktop rerun was invalidated by running the Gradle build while it
served `dist`: the test retained old HTML while the build replaced hashed asset
names, so its first restart timed out. The acceptance driver now copies built
assets to a private immutable snapshot before launching. The successful rerun
above uses that snapshot; the interrupted run is not counted as a pass.

## Electron PDF security-policy regression

Retesting the WebKit PDF workaround in Electron exposed two failures before the
native save dialog: `fetch(data:...)` failed under `connect-src`, then svg2pdf's
XHR of the temporary `blob:` URL failed under the same policy. Embedded PNGs now
use the browser's `atob` decoder and typed bytes directly, with no data-URL
request. Electron explicitly permits local blob reads in `connect-src`; external
network origins and `data:` connections remain excluded. Blob lifetimes and
deduplication are unchanged. The unit regression forbids fetch calls and checks
decoded bytes, including values above 127, invalid base64 and cleanup on failure.

Final Electron evidence is in
`/tmp/hict-release-physical/electron-blob-csp-reset-final/`: all thirteen records
pass, covering six native saves, PDF cancellation, four independent content
comparisons, ordinary restart and acknowledged storage reset. All dialogs are
parented. RGB error is below 0.47/255 and red-signal recall exceeds 99.88%; this
fixture contains primary Hi-C only, so black-label pixels are not used as a
secondary-signal assertion. The dual-source Tauri fixture retains its independent
dark-signal requirement. The Electron driver now waits for actual page creation
after CDP startup, since storage reset can delay window creation, and fails
immediately on export errors instead of spending the entire dialog timeout.

The final shared decoder was also rechecked in real Tauri:
`/tmp/hict-release-physical/tauri-local-decode-final/` passes all sixteen records
(one reset cycle, six native exports, cancellation and four independent raster
comparisons), retaining the same image-content metrics as the three-cycle run.
Frontend build/type checks, focused PDF/viewport regressions, Python syntax checks
and both repository whitespace checks pass. Focused ESLint has zero errors;
existing formatting and schema warnings remain.

## Server-cancelled tile audit and final exact-range checks

The cancellation fallback contained a separate defect from the previously fixed
client abort lifecycle: its embedded PNG had an invalid IDAT checksum and decoded
as opaque white, not transparent. This matches the corrupt/truncated image data
URI in the original report. HTTP 200 also made it indistinguishable from a
successfully rendered tile. The server now encodes a transparent PNG once with
ImageIO and marks cancellation responses with `x-hict-cancelled: true` and
`Cache-Control: no-store`. JSON includes `cancelled: true`; the header is exposed
through CORS. PNG and binary browser transports settle these responses as EMPTY
without decoding, caching or applying their signal ranges. A subsequent normal
reload requests the current tile generation.

Verification on the final cancellation implementation:

- The real HTTP scheduler regression occupies both TILE workers, queues four
  requests, invalidates their generation, and checks JSON PNG, raw PNG, JSON
  matrix and float32 binary cancellation responses. Every PNG chunk CRC and the
  decoded alpha channel are checked; matrix cancellations are explicitly empty.
- `scripts/check_cancelled_tiles_browser.py` injects that response contract into
  the real OpenLayers map in Chromium. For each transport, four requests are
  cancelled, six visible cached tiles end EMPTY (including extent-empty tiles),
  the loading queue drains, and reload reproduces **631,176 pixels exactly**.
  No page errors or corrupt-image warnings occur. Artifacts:
  `/tmp/hict-release-physical/cancelled-tiles/`.
- Exact whole-resolution browser checks pass again: panning does not restart the
  scan or change bounds, PRIMARY/SECONDARY have independent bounds, and returning
  to PRIMARY uses its completed range. Independent full float64 matrix queries
  agree with all four blockwise test cases, including the constant **16,777,217**
  that float32 cannot represent. Stale revision requests return HTTP 409.
  Artifacts: `/tmp/hict-release-physical/exact-range-cancellation-final/`.
- `npm run build` passes, including type checks and tile lifecycle regressions.
  `./gradlew test -x processResources --offline` passes: **296 tests, zero failures
  or errors, seven skips**. Resources are excluded to avoid rebuilding/replacing
  the tested WebUI during server acceptance. OpenAPI YAML parses successfully;
  both repository whitespace checks pass. Focused source/test lint has no errors;
  existing formatting warnings remain.

The exact-range policy is the user's explicit choice: no viewport sampling is
substituted for whole-resolution limits. Fine-resolution scans can be lengthy;
the bounded background scan and progress display do not imply instant results.

## Packaged-JAR acceptance

The next audit closed a packaging gap: previous browser runs used compiled classes
plus an older dependency JAR and the checkout's `dist`. The current production
WebUI and JVM classes were packaged with
`./gradlew shadowJar -x buildWebUI --offline` (the already-validated frontend build
was reused). The JAR was launched from `/tmp`, without a development classpath or
`WEBUI_ROOT` override. Its index and **all 51 embedded WebUI assets** matched the
served HTTP bytes exactly.

Tested artifact:

- `build/libs/hict_server-1.0.209-3a9a100-webui_0cbecb8-fat.jar`
- SHA-256: `d4ffffa8c803faeb1ab9cdffd39a95a210c4c50fe27f191db7bfc83803b87d44`
- This is a local working-tree build, not a newly tagged release; its inherited
  filename does not uniquely identify these uncommitted changes. Use the hash.

All acceptance runs used the physical quad copies under
`/tmp/hict-release-physical/data/` and the packaged server on ports 5107/8187:

| Evidence directory under `/tmp/hict-release-physical/` | Verified result |
| --- | --- |
| `packaged-acceptance/` | All 14 workflow checks passed: six exports, numeric query, graph histogram, Preview interaction, Attach state, FASTA bases and two real ModDotPlot generations. No page errors or failed HTTP statuses; the 54 recorded request failures were explicit superseded-request aborts. |
| `packaged-acceptance/rasterization-results.json` | Four independent Inkscape/Poppler comparisons passed. Maximum mean RGB error 0.430/255, red recall above 99.86%, dark-overlay recall above 99.51%. |
| `packaged-edited-view/` | Eight checks passed: reversal preserves viewport and exact terminal FASTA bases, five source/mode/triangle histogram cases, and one-pixel SVG borders with outlines off/on. |
| `packaged-histograms-cancellation/` | ROW/COL histograms each match 1,041 independently derived weights; SCALAR counts exactly one value. Explicit server cancellation shows a recoverable error, never an empty valid chart; Compute retries successfully. |
| `packaged-editor/` | Seven checks passed: range maximum output, graph/layout persistence, JSON round trip, Preview restoration, invalid graph rejection, stale graph rejection and delayed automatic thresholds not overwriting explicit settings. |

The six packaged exports completed in 0.12-1.41 seconds on this fixture. These are
single-run acceptance timings, not a general throughput benchmark. Histogram
cancellation was already rejected by the existing shape guard; this audit added
real-browser regression coverage rather than changing working production logic.
The test server was stopped after acceptance. No protected original input was
modified by these runs.

## Goal completion audit

This audit follows the active twenty-item goal, not only the original review
numbering. "Verified" below means the concrete behavior and tested environments
named in the evidence, not an assertion about untested platforms or all inputs.

| Goal | Current evidence and status |
| --- | --- |
| 1 | Six packaged browser exports and six native exports in each Linux wrapper pass; native Windows/macOS acceptance remains unverified. |
| 2 | Crop geometry regressions, rotated exports and independent page rasterizations verify map-bound placement and retained axes. |
| 3 | `Track2DSymmetric` defaults `borderOutline` to false; packaged SVG tests verify explicit off/on behavior. |
| 4 | Packaged SVG tests assert one-pixel colored strokes; the runtime track uses screen-space stroke width. |
| 5 | Exact whole-resolution float64 comparisons and source-switch/pan browser tests pass; bounded scans show progress. |
| 6 | ModDotPlot pixel tests verify source-specific contrast retains the primary expression and numeric scaling. |
| 7 | Seven packaged editor cases include graph/layout persistence through close/reopen and Preview. |
| 8 | Packaged workflow verifies mouse-wheel map zoom while Preview is collapsed, then graph viewport restoration. |
| 9 | Packaged ROW/COL/SCALAR histogram distributions match independent values; cancelled queries recover without a false chart. |
| 10 | Packaged selection histograms match counts and extrema for both sources, triangles and numeric modes. |
| 11 | Same export evidence as goal 1; progress starts above zero and finishes at one. Cancellation queue regressions pass. Native untested platforms remain a gate. |
| 12 | Two packaged ModDotPlot generations retain PRIMARY; separate pixel-level preset/5K-10K round-trip tests pass. |
| 13 | Packaged graph has separate source range nodes; selectedMax connection, JSON persistence and stale graph/threshold guards pass. |
| 14 | Real BLAST fixture contains 404 displayed hits, retains strand and alignment information, and omits the internal query-coordinate suffix. |
| 15 | Packaged FASTA letter rendering and reversed terminal bases match the source; large/gzip indexing and vector-letter tests are recorded above. |
| 16 | Ten Launcher acceptance checks plus Linux Electron/Tauri reset persistence/completion tests pass. macOS/Windows reset code cross-compiles, but native runtime behavior is unverified. |
| 17 | Current Background component renders the color picker without visibility or border-shape controls; sidebar screenshots corroborate it. |
| 18 | Packaged browser workflow, edits, diagnostics, graph, dual-source pixels and desktop save/reset cases pass in the tested Linux environments. Cross-platform acceptance is incomplete. |
| 19 | No failures in the recorded final automated cases; this supports a Linux release candidate, not unconditional stability on every supported platform. |
| 20 | This report records implementations, commands, measured outcomes, artifacts, package identity and unresolved gates, including the protected-input incident. |

The remaining cross-platform gate has persisted across the latest three goal
turns. Local code/build/browser/packaging work has progressed, but no native
Windows or macOS runner has become available. Cross-compilation cannot establish
native save-dialog, portal or WebView storage-reset behavior. The full goal must
not be declared complete until that external runtime evidence is supplied.

## Release gates and limitations

1. Linux Tauri now has integrated WebUI rendering and real native
   save/cancel/reset evidence, including independently rasterized image-content
   checks. This covers the tested WebKitGTK/runtime combination, not every Linux
   distribution or desktop-portal backend.
2. Linux Electron save/cancel and storage reset now have native end-to-end evidence
   above. Launcher GUI reset confirmations now also have isolated Linux evidence.
   macOS/Windows reset implementations now pass cross-compilation, but their
   dialogs and reset behavior still require native runtime acceptance; do not
   infer runtime success from compilation or Linux results.
3. Concurrent workspace Attach now has lost-update regression evidence above;
   this does not provide live multi-user assembly synchronization. Gzip FASTA
   has bounded-heap and browser evidence above; multi-gigabase input on the
   target machines still needs practical temp-disk sizing and workload validation.
   The optional brightness-curve editor was not added. Legacy normalization
   controls remain available for compatibility.
4. Browser regression success is evidence for the exercised cases, not proof of
   universal stability or publication-ready performance. No unconditional release
   certification is made.

## Protected-input incident

An initial ModDotPlot test used original DATA with a separate PROCESSED_DIR.
Existing sequence-job code resolves `DATA/processed/sequence-analysis` directly,
so it created about 1.3 MB of test sidecars below the protected quad input folder.
The original map and FASTA files were not overwritten. Generated job IDs were
`437e90f8-daf6-4bc2-8362-dd4f52f83c29` and
`fe994231-3a0f-45b5-bb97-a6a0931c3838`, with their overlay registry metadata.
The user was informed. Cleanup was blocked by the protected-path policy and is
pending explicit approval; no workaround was attempted. Subsequent focused tests
used temporary DATA symlinks without generation; the latest broad acceptance and
BLAST tests used physical input copies and wrote only inside `/tmp`, as detailed
above. The old protected-path test artifacts were not removed or modified.
