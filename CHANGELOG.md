# HiCT Changelog

## Unreleased

- Release open maps and their private gzip FASTA indexes when stopping the server, including primary and secondary references.

- Keep normalization choices current when switching resolution levels, including files with balancing weights at only some levels.

- Add a five-minute example quick start, an illustrated installation guide, a user guide with shortcuts and machine-readable software citations.
- Show the built WebUI version and source revision in About, include aggregated third-party notices in portable packages and run the complete JVM regression suite in platform CI.

## 2.0.0

### Opening and examples

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

- Generate alignment and ModDotPlot identity matrices and run exact, substitution or BLAST sequence searches through bundled tools. Preserve source references, analysis provenance and effective worker counts.
- Publish reusable matrix and BED results under validated data-directory paths with overwrite protection. Retain deterministic ModDotPlot accumulation and percentage-scale display presets.
- Project search hits and annotations through edited source coordinates. Color strands independently and mark disrupted matches. Pack overlapping features into lanes with a bounded density fallback.
- Manage signal and annotation tracks, their order and independent axis visibility. Retain visibility and labels in sessions. Validate coordinates, signal ranges, log bases and corrected values before publication.
- Provide FASTA linking and mismatch guidance from each sequence-dependent tool. Render a forward sequence strip and optional reverse strand at measured letter scale.

### Jobs, server and performance

- Follow conversion, sequence, coverage, track preparation, FASTA-only and example-download jobs in the navbar. Close busy dialogs, cancel work and open finished results from Jobs.
- Stop native process trees and background services on shutdown. Publish completed outputs atomically and identify incomplete work. A new launch starts with fresh job state.
- Resume verified example downloads through pinned public-host connections, including chunked partial responses. Clean conversion uploads and retain terminal results according to service TTL.
- Show one actionable failure message with copyable diagnostics. Keep background failures in Notifications and report queue saturation and conflicting coverage preparation explicitly.
- Bound tile dimensions, native read work, caches and obsolete requests. Avoid matrix block reads for sequence-only assemblies and publish session replacement atomically.
- Load heavy editors and export libraries on demand. Cache hashed assets, avoid PNG recompression and release listeners, rulers, tile sources and scheduled work on close.
- Read large-map weights in heap-bounded HDF5-aligned blocks, validate dataset shapes efficiently and restrict vector work to the visible region. Respect allocated CPU budgets in native conversion, alignment and sequence jobs.

### Exports and portable applications

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
