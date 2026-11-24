# HiCT Changelog

## Unreleased

- Retry failed figure previews with the keyboard, keep export settings in place, and announce when the preview is ready.
- Keep the map wider in narrow windows, with expandable map tools and full filenames in Sources.
- Show the release and both source revisions in `--version`. Keep runtime libraries and temporary conversion files out of file pickers.
- Prefer per-user working folders in the Windows portable launcher and recover when redirected temporary folders are unwritable.
- Preserve Unicode command-line filenames in the Windows EXE and portable ZIP.
- Keep the Selection ribbon steady while selecting a region, clarify file-picker labels, and allow Escape to close matrix export.
- Make edited, original and AGP assembly order explicit when exporting a Cooler matrix.
- Add section links to the bundled user guide and keep notifications behind active dialogs.
- Keep Notifications visible in short windows while the editing tools scroll independently.
- Choose square-root coverage independently for each contact map, including when the other source is a ModDotPlot. Sessions restore each source's normalization choice.
- Refresh the threshold fields immediately when a visualization preset changes the signal range.
- Publish branch previews as versioned prereleases with one verified asset set per build.
- Keep the Selection menu accessible in short windows and replace rounded thresholds cleanly when entering exact values.
- Keep the empty workspace focused on opening a map, and retain full signal threshold precision while editing rounded values.
- Add browser checks for assembly safeguards, export destinations, error feedback, and contrast across zoom levels.
- Keep everyday dialog controls visible and move detailed options under Advanced. Open maps with optional sequence and layout in three steps, and read the user guide inside HiCT.
- Group region commands into Edit, Copy, Export and Analyse. Print figures at a chosen width with an adjustable minimum label size.
- Undo and redo from the toolbar, confirm scaffold removal, and link the matching FASTA directly from export guidance.
- Show readable errors with diagnostic details, check for an open map before joining a running session, and show the tool log when dotplot generation fails.

- Keep square-root coverage readable when zooming, restore exact contrast limits for each source, and keep zoom controls within the available detail levels.

- Keep assembly edits available while opening tracks or linking FASTA, retain Undo when overlays change, and protect open maps from replacement.

- Reject invalid map edits and file requests without changing the assembly, and refresh tiles after applying an AGP.

- Report the JVM and WebUI revisions in local builds and reject stale browser assets or analysis tools when packaging.

### Open, save and recover

- Open curated datasets from **File → Examples…**. Follow downloads and conversion in Jobs, cancel or retry a stage, and reopen a ready example with its FASTA and recorded source attribution.
- Browse large data folders in the file picker as entries load in pages. Start matrix conversion directly from a selected file or browse conversion folders to select several inputs.
- Convert unordered 4DN pairs with a configurable base bin size and build the map's resolution pyramid.
- Save sessions, FASTA, AGP and figures to a chosen destination. HiCT remembers file choices by purpose and keeps edits available if a write fails. Open sessions from the startup screen or the File menu and export AGP from FASTA-only assemblies.
- Find the active Hi-C map, FASTA and AGP next to Jobs. Stop HiCT returns the Launcher to its Start view and keeps the stopped session’s console available. The packaged Electron interface can reopen locally when its page is unavailable.

### Select, navigate and edit

- Shift-click or drag to select contigs or scaffolds. Ctrl-drag (⌘-drag on macOS) creates a persistent analysis region. Set its bounds in assembly or contig coordinates, copy the coordinates or a PNG to the clipboard, and export the selected sequence.
- Enter, convert and copy region boundaries in assembly, scaffold or contig coordinates. Choose 1-based inclusive or 0-based half-open coordinates in the region editor.
- Zoom with the wheel from the contact map to FASTA letters while staying anchored at the pointer. Center on a track feature at the current scale or zoom to its span. Hide assembly boundaries in a scoped intersection while retaining selection highlights.
- Search contigs, scaffolds and features from the map toolbar with a clearer search field and Go to control.
- Cut exactly with the scissors at FASTA zoom, by a typed coordinate, or at a GTF/GFF feature edge. HiCT preserves sequence and source contact bins, projects edits to secondary maps, and restores exact cuts when reopening sessions.

### Render maps and analyse sequences

- Manage 1D tracks separately from sources and layers. Add a track file or Cooler weights, resize the track panels, and examine signed BigWig values and aligned-base BAM depth.
- Show BED blocks, strand arrows and per-feature colors. Scroll each annotation track independently, and keep annotation density anchored to its source across assembly edits.
- Use the resolution pyramid appropriate to each map when comparing or overlaying sources, including in the minimap.
- Fit the visible signal with the histogram and range handles. Choose O/E colours in the sidebar or Rendering Pipeline. The square-root coverage help explains how a bin's total contacts are counted, and maps remain readable when their Cooler weights are unusable.
- Add and move curve points over the histogram to adjust how HiCT maps signal to color.
- Select FASTA letters to copy an interval or measure a repeat period. Alignment previews show continuous runs, substitutions and indels at base zoom. Search local databases or opt in to NCBI BLAST with query-upload consent and Jobs cancellation.
- Select a ModDotPlot or alignment dotplot resolution range, omit individual resolutions, or enter custom values. Display finer sequence bins as an overlay or in one triangle while the Hi-C map keeps its stored contact resolution.
- Export Cooler files with source counts and weights preserved where available. Sequence-cut rebins are labelled estimates, with their provenance carried forward when reopened.

### Capture and share figures

- Preview and export the map, rulers, tracks and ModDotPlot as PNG, SVG or PDF. Compare named snapshots of cuts and scaffolds, set print width, and reopen PNG or SVG inside HiCT.
- Review the dimensions and bin count before rendering a large region. Copy a region as PNG or choose PNG, SVG or PDF from the Export submenu.
- Reproduce the public-data Borrelia editing figure using the pinned inputs, caption, vector artwork and 900-dpi TIFF in the [figure guide](docs/publication/borrelia-editing/README.md).

### Portable packages

- Linux, Windows and macOS packages include the pinned WebUI and the native tools needed for the supported workflows. Release publication verifies the requested installers and their checksums while keeping the previous downloads available through an interrupted upload.
- Linux launchers use fewer glibc allocation arenas by default, reducing resident memory during repeated map opening and editing.
- Linux launchers start with a smaller Java heap and release idle heap more readily. The existing extra JVM options still take precedence.
- The bundled ModDotPlot uses updated imaging dependencies. Linux retains glibc 2.17 compatibility, and package checks now render real PNG and SVG plots.
- Portable package builds keep the verified BLAST cache when ModDotPlot requirements change.

## 2.0.0

### Release preparation and cleanup

- Enforce navigation timing limits in release checks and verify public-example strand, sequence and matrix-count fidelity.

- Remove Move arrows immediately when cancelling the tool with Escape.

- Render custom graphs built from constants, coordinates or tracks on the primary map grid.

- Keep dialog controls reachable, contain keyboard focus and return focus to the opening menu when dialogs close.

- Scale contact signals by exact resolution ratios, including non-divisible pyramid levels and very large resolutions.

- Start portable Linux packages safely when multiple first launches share the same payload cache.

- Preserve all presets and thresholds when an imported preset file contains an invalid item.

- Export contig and scaffold names through the supported API method for reliable name round trips.

- Navigate to contigs and scaffolds from the sidebar using their actual assembly coordinates at every zoom level.

- Show an empty conversion-job list accurately after all conversions finish.

- Close open menus with Escape and keep the last active map resolution visibly enabled.

- Honor the selected Cooler compression level, including uncompressed interaction data with optional balancing.

- Preserve edited bookmark names through export, import, sidebar changes and saved-session recovery.

- Explain the available Cooler export compression and balancing modes before starting an export.

- Let Escape cancel edits in focused text fields while keeping map-tool cancellation available outside editors.

- Release open maps and their private gzip FASTA indexes when stopping the server, including primary and secondary references.
- Keep normalization choices current when switching resolution levels, including files with balancing weights at only some levels.
- Add a five-minute example quick start, an illustrated installation guide, a user guide with shortcuts and machine-readable software citations.
- Show the built WebUI version and source revision in About, include aggregated third-party notices in portable packages and run the complete JVM regression suite in platform CI.

### Opening and examples

- Preserve AGP strand orientation during matrix imports and require fresh conversion of older cached outputs.
- Add Borrelia Hi-C and yeast Micro-C quick starts with verified matching references and automatic preparation.
- Detect rapid equal-size source changes before reusing conversions and validate FASTA replacements on filesystems without file keys.
- Open native maps directly through a keyboard-accessible file picker. Use Open with options for conversion, secondary inputs and preparation. Tools show their prerequisites while a map is closed.
- Open the small tardigrade example with matching FASTA in three clicks. Cheetah and mosquito examples have verified source hashes, complete conversion and linked-sequence checks. Catalogs preserve provider terms and citations.
- Import `.hic`, `.cool` and `.mcool` files through the bundled native tools. Generate requested pyramids from multiresolution Cooler inputs and validate incompatible assembly layouts before publication.
- Fit the opening color range to visible signal. Mask excluded weighted bins by default and retain a per-file policy. Failed primary, secondary and session opens preserve the working assembly.
- Open plain or gzip FASTA as a sequence-only assembly. Preserve sequence and gaps through optional splitting at N runs and report unusable or duplicate records.

### Assembly editing and sessions

- Reverse, move, split, group, ungroup, rename and mark debris with an edit journal and Undo/Redo. Keep insertion targets current across resolution changes and cancel armed tools with Escape.
- Save and restore assembly edits, native split lineage, tracks, scopes, input references, visualization graphs and view. Validate changed inputs before replacement. Save failures retain the unsaved state and explain their cause.
- Ask before discarding edits during open, close and restore. Preserve text-field Undo behavior and guard editing history against stale clients.
- Preserve viewport centre, zoom and rotation through structural edits. Refresh FASTA, annotation provenance and context menus after edits, Undo/Redo and session restore.
- Preserve AGP component types, inclusive coordinates, gap types and linkage evidence. Reject discontinuous or overlapping layouts before mutation. Retain source-bin ownership and hidden-contig coordinates in imported layouts and matrix queries.
- Share configurable AGP and FASTA export gaps, defaulting to 1,000 bp. Provide AGP validation and annotation commands and retain scaffold order in native and assisted Cooler exports.

### Visualization and coordinates

- Preserve custom colormap alpha during contrast edits and confine changes to the selected source and triangle.
- Publish primary styling atomically and report failed sidebar edits once with retryable diagnostics.
- Preserve fractional alpha behavior while restoring saved visualization colors exactly.
- Save complete custom rendering graphs, editor layouts and lossless colors in visualization presets, JSON libraries and native sessions.
- Retry updated preset imports with the same filename and preserve the current visualization when a required graph input is missing.
- Use descriptive red-palette names and keep success confirmations clear of dialog actions.
- Show genome-browser rulers with round ticks at exact assembly positions, relative contig/scaffold modes, hidden-span marks and readable entity bands. Keep the tick ruler beside the map and include it in exports.
- Anchor the map against the ruler corner, with an optional unlock. Preserve cursor-centred zoom, independent row/column scopes and exact sequence positions at letter zoom.
- Compare layers with different resolution pyramids on the primary assembly grid. Select each layer's nearest level with hysteresis, bounded reduction, cancellation and a loading indicator.
- Show exact bounded CIGAR alignment detail at letter zoom, including substitutions, gaps, reverse alignments and ambiguous sequence. Discard stale results after navigation or edits.
- Apply and retain signal transforms, normalization and per-source contrast settings consistently. Separate the slider scale from the signal transform. O/E uses a diverging log-ratio range centred at one.
- Compute square-root coverage over one whole-map domain per resolution. Publish all completed levels together and cancel obsolete map work. Bins without contacts remain empty.
- Manage visualization presets through a compact list. Preserve custom rendering graphs, layout, viewport and histogram-only branches across round trips. Validate imported graphs and protect newer explicit changes from delayed threshold scans.
- Inspect visible, selected and whole-resolution signal distributions, with bounded scans and source/triangle selection. Count scalar and one-dimensional inputs according to their own sample domains.
- Keep selection contrast visible across palettes. Bound context menus to the viewport, avoid colliding labels and refresh the on-screen display after stationary-pointer zoom.

### Sequence tools and tracks

- Keep Cooler weight switches consistent while track updates are pending and restore their committed state after failure.
- Update track backgrounds immediately on both map axes and preserve their export colors.
- Generate alignment and ModDotPlot identity matrices and run exact, substitution or BLAST sequence searches through bundled tools. Preserve source references, analysis provenance and effective worker counts.
- Publish reusable matrix and BED results under validated data-directory paths with overwrite protection. Retain deterministic ModDotPlot accumulation and percentage-scale display presets.
- Project search hits and annotations through edited source coordinates. Color strands independently and mark disrupted matches. Pack overlapping features into lanes with a bounded density fallback.
- Manage signal and annotation tracks, their order and independent axis visibility. Retain visibility and labels in sessions. Validate coordinates, signal ranges, log bases and corrected values before publication.
- Provide FASTA linking and mismatch guidance from each sequence-dependent tool. Render a forward sequence strip and optional reverse strand at measured letter scale.

### Jobs, server and performance

- Make shutdown logging release checks reliable under worker interruption.
- Follow conversion, sequence, coverage, track preparation, FASTA-only and example-download jobs in the navbar. Close busy dialogs, cancel work and open finished results from Jobs.
- Stop native process trees and background services on shutdown. Publish completed outputs atomically and identify incomplete work. A new launch starts with fresh job state.
- Resume verified example downloads through pinned public-host connections, including chunked partial responses. Clean conversion uploads and retain terminal results according to service TTL.
- Show one actionable failure message with copyable diagnostics. Keep background failures in Notifications and report queue saturation and conflicting coverage preparation explicitly.
- Bound tile dimensions, native read work, caches and obsolete requests. Avoid matrix block reads for sequence-only assemblies and publish session replacement atomically.
- Load heavy editors and export libraries on demand. Cache hashed assets, avoid PNG recompression and release listeners, rulers, tile sources and scheduled work on close.
- Read large-map weights in heap-bounded HDF5-aligned blocks, validate dataset shapes efficiently and restrict vector work to the visible region. Respect allocated CPU budgets in native conversion, alignment and sequence jobs.

### Exports and portable applications

- Verify reload pixel fidelity, zoom boundaries and all exported caption positions in release checks.
- Expand release validation for session recovery, offline use, native exports and analysis settings.
- Export viewport or fitted whole-map PNG, SVG and PDF with consistent crop geometry, rulers and visible letter-scale alignment detail. Preserve source images during PDF conversion and avoid duplicated SVG scenes.
- Use typed export bytes and parented save dialogs in Electron, and asynchronous native save selection in Tauri. Preserve output preferences after cancellation or failed saves.
- Improve narrow-window layout, scrolling tools, accessible names, modal focus and keyboard file opening. Retain compatible settings and presets across opening paths.
- Provide a compact Launcher with persisted data paths, port validation, process control, logs and reset actions. Stop native children when the server stops. Portable Java defaults to 50% of available RAM and accepts an explicit heap limit.
- Bundle the API reference for offline use. Update jsPDF, DOMPurify, axios, Vue and Vert.x with rendered-export and browser qualification, compatible logging and locked dependency resolution. Production npm audit reports zero advisories. Development-tool findings are recorded in the validation notes.

## 1.0.190-a5f9213-webui_040c1a1

Changes since `master` version `1.0.175-5ddcf3c-webui_a609177`:

- Added a lazy web file selector for large server-side data directories, with path entry, explorer/tree views, symbolic-link support, and safer parent-directory navigation.
- Added support for opening and converting additional matrix-like input formats through the conversion workflow, including sparse text formats supported by hictk.
- Fixed `.hic` plus Juicebox assembly conversion regressions and improved handling of hidden/missing contigs during conversion.
- Fixed `.hict.hdf5` to `.mcool` export and `.mcool` re-import so exported files preserve contigs, contig lengths, directions, scaffold IDs, hidden contigs, and raw pixels instead of reopening as a single `assembly` contig.
- Fixed Cooler export after `.hic` plus `.assembly` import so hidden zero-bin assembly placeholders are not serialized as real Cooler chromosomes and do not reappear after export-import.
- Added metadata-preserving internal `.mcool` export as the default automatic export path, while keeping explicit hictk-assisted export available.
- Added resolution restriction controls so users can keep the map at selected bin sizes while zooming.
- Added ruler modes for global, intra-contig, and intra-scaffold coordinates.
- Added row/column contig and scaffold isolation controls with searchable dropdowns, all-options browsing, and a row-to-column copy button.
- Made rulers respect isolated row/column extents, so horizontal and vertical rulers span only the selected contig/scaffold scope.
- Improved global ruler labels: the main label stays global, while the secondary label now shows intra-scaffold offsets when scaffolded and intra-contig offsets for unscaffolded regions.
- Added configurable OSD overlay settings, including field visibility, order, and position.
- Added an export workflow for converting `.hict.hdf5` matrices to `.mcool`, with current or custom AGP assembly state.
- Added native-assisted sorting and chunked direct export improvements for HiCT-to-Cooler conversion, reducing memory pressure while preserving the existing `.hict.hdf5` layout and Cooler contracts.
- Added default-on hictk zoomify and balancing controls for opening `.cool` and `.mcool` inputs, so single-resolution Cooler files can be expanded into an optimized resolution pyramid before import.
- Fixed Open Wizard Cooler import preparation so hictk zoomify/balance options invalidate stale direct-import caches and no longer silently open a single-resolution map when pyramid generation was requested.
- Added editable Cooler export filenames, with `.cool` used by default for finest-resolution exports and `.mcool` used by default for all-resolution exports.
- Added default-on balancing for exported Cooler files in hictk-assisted export mode, and made automatic export mode prefer hictk when it is available.
- Added real single-resolution `.cool` export support through both direct internal and hictk-assisted export, while keeping multi-resolution `.mcool` export for all-resolution workflows.
- Added WebUI, API, CLI, and OpenAPI options for Cooler input balancing, exported Cooler balancing, and explicit export filenames.
- Improved `.mcool` to `.hict.hdf5` conversion throughput and added round-trip validation coverage for `.mcool -> .hict.hdf5 -> .mcool` workflows.
- Improved portable package size by reducing redundant bundled content where possible.
- Split platform-specific portable runtime contents from the universal fat JAR intent so portable packages can avoid incompatible native payloads while the universal JAR can keep broad compatibility.
- Fixed macOS packaging checks around platform-specific JHDF5/HDF5 native payloads.
- Enabled native processing by default when a supported native library is available.
- Improved color threshold controls for live updates and very small signal values.
- Reduced WebUI freezes caused by large contig/scaffold style updates.
- Updated the About window with correspondence contacts and this changelog.
- Rendered the About window changelog as Markdown for readable headings, lists, links, and inline code.
- Added multi-select 1D track picking in Open Wizard, with persistent selections while browsing directories and duplicate-track avoidance.
- Fixed workspace exports so PNG/SVG/PDF export follows the interactive view, including current row/column isolation, overlays, visible tracks, names, and visualization settings.
- Added a portable `toolbox` CLI entry point for launching bundled hictk, minimap2, and mm2-plus with upstream project, license, and citation notices.
- Improved CLI boolean option parsing with optional true/false values and paired `--no-*` forms, plus concise parse errors instead of stack traces.
- Changed sealed `.hic`/Cooler imports without external assembly files to place each imported chromosome/contig into its own scaffold.
- Changed default `.hic` conversion to rebuild a hictk resolution pyramid unless exact source resolutions are explicitly requested, reducing bad/empty coarse-level imports from problematic `.hic` files.
- Fixed sealed `.hic` imports so converted files synthesize singleton scaffolds during conversion when no external `.assembly` file is available.
- Fixed coarse-resolution contig display by hiding contigs whose bp length is smaller than the active resolution bin, while keeping them available at finer zoom levels.
- Fixed hidden-contig projection across zoom levels so contact-map tiles, contig borders, contig labels, and cursor coordinates use the same per-resolution visible-contig layout instead of copying visibility from nearby resolutions.
- Improved initial map visibility by computing a one-time opening upper threshold from the 0.9 signal quantile of the coarsest safe full-map resolution, so coarse maps no longer open as blank white because of an overly high default threshold.
- Improved contig and scaffold border visibility with a contrast outline behind the configured border color.
- Improved BED/GFF/GTF feature tracks at coarse zooms with bounded density rendering, clearer strand arrows, and safer rendering caps for dense annotation files.
- Fixed Visualization Settings requests with blank numeric fields, so disabling auto-threshold and other toggles no longer causes a backend cast error.
- Added detection and user-facing handling for invalid `NaN` Cooler balancing weights, including a warning dialog, red sidebar warning, and configurable rendering policy.
- Added post-open control for invalid Cooler weight handling, so users can change the selected `NaN` weight policy after a map is already loaded.
- Fixed first render after opening maps with invalid Cooler weights and delayed track initialization by refreshing OpenLayers, rulers, minimap viewport, and 1D tracks after the workspace grid changes, so the map no longer stays blank until minimap interaction.
- Fixed first render after opening Hi-C plus dotplot overlay maps by applying overlay presentation before rendering, rebuilding secondary compatibility after assembly synchronization, then refreshing the workspace once more after the wizard closes.

# Coordinate consistency correction (2026-10-20)

- Share orientation-aware bp/bin conversion between matrix queries and 1D tracks; use retained source ATU origins for split-derived components.
- Correct reversed zero-remainder track bins and screen-selected splits of reversed components with a terminal partial bin.
- Include the final intersecting bin of half-open bp matrix query intervals.
- Exclude the internal zero-resolution bp axis from signal-track precomputation.
- Close native HDF5 readers when pooled dataset bundles are destroyed; repeated map opens no longer retain pooled file handles.
- Run coordinate and scaffold regressions in the cross-platform Java workflow; allow 600 seconds for portable native-tool smoke checks.
