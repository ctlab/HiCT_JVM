# HiCT Changelog

## Unreleased

### Navigation and visualization

- Package the exact requested JVM build on Linux, macOS and Windows, and keep release uploads and smoke checks on that version instead of selecting stale artifacts.
- Explain incomplete HiCT files as input errors with a recovery action, preserving the edited workspace when opening them fails.

- Export frozen figures at 86 mm or 178 mm column width with matching SVG/PDF/PNG physical sizes, preserved matrix pixels and recorded effective raster resolution.
- Reject oversized PDF pages with a clear recovery message instead of silently clipping the drawing.

- Store and view named figure snapshots, compare cut/scaffold/reversed states, and retain original pixels, vector labels and per-panel provenance in portable sessions.
- Validate stored figures before replacing a session, protect snapshot changes during delayed saves, preserve legacy AGP fragment IDs after reordered assembly edits, and avoid redundant fine-grid loading in projected comparisons.

- Pin Expected/O/E figure fitting profiles across viewport, whole-map, rectangle and sequence-level captures, recording actual profile fingerprints and active normalization inputs.
- Reject incomplete figure tiles and changed scientific inputs, restore the shared overview and live camera, and bound capture work, retained profiles and abandoned-context leases.

- Preview figure exports in a dedicated dialog, choose area and annotation panels, and save PNG, SVG and PDF from one frozen capture.
- Retain scientific source, assembly and rendering provenance in all three formats, reject concurrent changes during capture, and preserve crop metadata without invalid narrow-panel SVG text.

- Reopen exported PNG and SVG figures inside HiCT with fit, actual-size and zoom controls, remembered browser file choices, and clear invalid-image errors.

- Search NCBI nucleotide databases with explicit query-upload consent, independent Jobs cancellation and bounded provider waiting while local analysis remains available.
- Inspect external BLAST coordinates in a paginated, filterable report and save JSON provenance or complete raw TSV through the file-save flow.

- Preselect example recipe tracks in the opening wizard, omit unavailable optional tracks, keep removed tracks from reappearing after a late probe, and show the loaded AGP in the header.
- Retain each example source's recorded citations, attribution, recipe and file hashes through session save and restore, independently of later catalog changes.

- Show numeric overall progress for example conversions, accounting for later stages rather than reaching 100% after each stage.
- Treat cancelled example conversions as an expected outcome, keep downloaded inputs, and provide a direct retry without a failure alert.

- Keep Info current while zoom tiles load without reading tile pixels, and explain the yeast Micro-C example's source-supported short-range empty band.
- Dismiss map-specific masking notices safely when changing maps, and support numeric pipeline queries across different resolution pyramids, including hidden bins.

- Follow quick-start example conversions in Jobs, retain their FASTA when reopening results, offer clear errors and a direct retry, and revoke automatic opening when closing progress or changing workspace.
- Keep map startup quiet by removing internal sensitivity notices and redundant example preparation notifications.

- Verify cached examples through Jobs before opening, repair damaged files, and refresh ready status and provenance when the catalog changes.

- Follow example downloads in Jobs with dataset names, honest byte progress, retained results, optional-file warnings and focused recovery, while keeping automatic opening tied to the user's current intent.
- Keep Jobs controls reachable while scrolling, move catalog disk checks off the HTTP event loop, and make final example verification cancellable.

- Adjust ordinary gradient colors with a draggable and keyboard-accessible midpoint, retaining independent source curves through sessions, presets and Rendering Pipeline.
- Wait for preview changes when saving a user preset and keep Secondary presets scoped to that source in the single-map layout.

- Scan the selected Expected/O/E signal with one independent whole-map profile across all blocks, preserving the viewport fit and distinguishing unsupported ratios from measured zeros.
- Cancel exact-limit scans, reject obsolete responses, and keep completed slider limits steady while panning.

- Preserve newer thresholds, transforms and source-specific edits through delayed normalization, reads and preset writes, with clear pending and retry status.
- Apply pending visualization changes before session snapshots and opening Rendering Pipeline, and preserve committed graph state when a request fails.

- Show a viewport signal histogram below the color slider with sampled-data labels, threshold marks and Fit levels, and reserve transfer capacity for interactive requests.
- Preserve missing O/E support as missing in numeric diagnostics, while keeping measured zeros distinct and using meaningful positive ratios for fitted O/E limits.

- Edit the active primary O/E palette in Rendering Pipeline with shared presets, live preview, safe delayed updates and palette import/export, while preserving the Observed graph and secondary layer.
- Reload sidebar changes when returning from palette Preview and keep palette controls usable in compact windows.

- Honor O/E endpoint colors and alpha, remember separate Expected/O/E palettes, and add a neutral-at-1 ratio palette to pipeline colormap nodes.
- Keep slider limits steady during adjustment and apply live threshold changes, with O/E handles staying on the correct side of neutral.

- Explain square-root coverage with the row-sum definition, diagonal convention and whole-map scale factor, with an optional formula and worked guide.

- Keep menus, Jobs and file badges within narrow windows, and make the stacked sidebar scrollable with usable signal controls.

- Separate 1D tracks from map sources, add source weights through Add, and preserve typed settings and panel alignment across refreshes and resizing.
- Identify file versus coverage weights and keep weight tracks current when normalization changes, including after precompute.

- Open the bundled Electron interface when the hosted page is unavailable, with a Reconnect action that protects unsaved assembly edits.
- Preserve bundled-interface preferences across restarts and include packaged icons without weakening browser or backend access controls.

- Stream Electron FASTA/AGP exports in the main process with native destination selection, revision guards and no file-sized renderer IPC.
- Cancel owned transfers on navigation and wait for staged-file cleanup on graceful quit; show clear filesystem failures and preserve native session save/readback.

- Stream FASTA/AGP directly to writable browser destinations with backpressure, a cancellable progress notice, and abort-on-failure cleanup.

- Choose destinations for sessions, FASTA/AGP, figures and settings exports, remember file choices by purpose, and expose File → Open Examples.
- Mark sessions saved only after a confirmed write, preserve newer edits and failed-save guards, and freeze FASTA/AGP export layouts under the displayed-revision check.

- Draw continuous alignment runs through letter boundaries, with distinct substitutions, reverse matches and indels in the map and vector exports.
- Recover short tandem-repeat alignments with a bounded short-window profile, verify CIGAR labels against source bases, and cancel obsolete preview jobs without losing their IDs.

- Select and measure FASTA letters on either axis/strand, copy exact sequence and coordinates, and estimate a bounded repeat period through cancellable Jobs.
- Keep FASTA measurements stable during navigation, invalidate them after edits, and exclude transient measurement overlays from saved sessions and image exports.

- Separate Shift entity selection from persistent Ctrl/Command analysis regions, with stable border highlights and optional border hiding in scoped views.
- Select exact regions by assembly or contig coordinates, preserve their endpoints across scales and FASTA export, and exclude selection overlays from image exports.

- Project base-scale 1D features and signals across hidden and binless sequence, with bounded queries and native-bin weight provenance.
- Keep normalization availability tied to stored source resolution and label base-scale track coordinates explicitly.

- Cut exactly at GTF/GFF feature edges with stale-menu guards, preserve the BP view after cuts, and make Center/Zoom actions distinct across native/base scales.
- Copy genome coordinates without thousands separators; fit BP navigation ranges through the active coordinate model.

- Add a uniform base-scale map with explicit native-bin labels, aligned FASTA/features, pointer-anchored zoom and portable saved view coordinates.
- Use sequence-preserving exact scissors on capable servers, refresh raster extents after cuts, and separate native-bin expected profiles from visible-bin profiles.

- Keep native-resolution rulers and sequence coordinates aligned with partial source bins after contig edits, including reversed fragments.

- Keep notification stacks away from visualization controls on smaller windows.

- Show contig and scaffold names on the map whenever the contigs in view are wide enough to read. Small contigs elsewhere in the assembly no longer hide the names of large ones on screen.

- Keep opening-fitted contrast readable across pyramid levels, preserve explicit thresholds and separate O/E ranges, and show small-genome minimaps even when coarse levels are empty.
- Start log transforms at Log2, show masked bins without a permanent alarm, and keep history controls in Journal.
- Make developer tools opt-in, move tile reload into Dev, and provide quiet map Info and Help → Connection….
- Reduce dense whole-map name clutter while retaining ruler names, entity navigation and export layout.
- Use neutral assembly-file labels, consistent menus and a discoverable FASTA link action in its status chip.

### Sequence analysis and sessions

- Add an explicit sequence-scale tile projection that clips native contact colors to exact fragment widths, keeps base-level upper/lower source choices and declares the stored contact resolution.
- Keep shared references colored throughout the pipeline and preserve hidden-bin weights and feature samples in numeric BINS queries.

- Weight repeated exact-cut references once per unordered source-bin pair in expected fitting, keep fitting consistent across tile chunks, and label histogram counts as displayed samples.

- Preserve stored fractional values and genuine weights in ordinary Cooler re-export, retain int64 counts in mixed pyramids, and report cis totals separately from interchromosomal contacts.
- Preserve existing export destinations on failure or cancellation, with explicit overflow and invalid-value errors.

- Preserve estimation provenance and fractional values when reopening approximate Cooler files, label estimated primary/secondary sources, and retain their declaration in sessions.
- Clamp export chunks to small map dimensions so default settings work with short assemblies.

- Export sequence-cut assemblies to Cooler with explicit approximate rebinning, labeled fractional values and conserved retained contacts, while preserving native HiCT data.
- Retrieve completed exports from Jobs with readable result filenames after dismissing the export dialog.

- Preserve source sequence coordinates beyond 2.1 Gb in cuts, AGP components and saved sessions.

- Expose optional physical source-bin identities in numeric matrix queries, so repeated references after exact cuts can be identified without changing contact values.

- Preserve exact-cut source bins across secondary maps with different resolutions and saved sessions, and reject exact-cut API requests from an outdated assembly view.

- Save sessions in format v4 with explicit support for exact sequence-cut lineage, while retaining older session readers and historical cut behavior.

- Deliver completed overlays and search tracks through Jobs even after progress dialogs close, retain overlay/triangle choices, and default generation levels from finest through current.
- Keep primary and secondary palettes and custom graphs intact through Observed/Expected/O/E changes, mask undefined O/E ratios, and prevent unusably narrow threshold intervals.
- Pick overlapping track features by their displayed lanes and offer Zoom to feature at readable FASTA scale.
- Restore valid multi-source sessions beyond 100,000 values while retaining validation limits.

### Packaging and reliability

- Return to the launcher's simple start view after Stop HiCT, with configuration and logs hidden and settings retained.

- Keep rolling-release downloads available when publication fails, and update release metadata without reusing obsolete workflow targets.
- Complete bounded redirect responses reliably, preserve connection-error causes, and exercise redirect limits on every platform.
- Keep pinned tool builds available during GNU Savannah outages and resolve release JHDF5 assets strictly in Autobuild Release.

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
