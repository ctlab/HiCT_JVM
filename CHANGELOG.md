# HiCT Changelog

## 1.0.185-52f486a-webui_bea71e6

Changes since `master` version `1.0.175-5ddcf3c-webui_a609177`:

- Added a lazy web file selector for large server-side data directories, with path entry, explorer/tree views, symbolic-link support, and safer parent-directory navigation.
- Added support for opening and converting additional matrix-like input formats through the conversion workflow, including sparse text formats supported by hictk.
- Fixed `.hic` plus Juicebox assembly conversion regressions and improved handling of hidden/missing contigs during conversion.
- Fixed `.hict.hdf5` to `.mcool` export and `.mcool` re-import so exported files preserve contigs, contig lengths, directions, scaffold IDs, hidden contigs, and raw pixels instead of reopening as a single `assembly` contig.
- Fixed Cooler export after `.hic` plus `.assembly` import so hidden zero-bin assembly placeholders are not serialized as real Cooler chromosomes and do not reappear after export-import.
- Added metadata-preserving internal `.mcool` export as the default automatic export path, while keeping explicit hictk-assisted export available.
- Added resolution restriction controls so users can keep the map at selected bin sizes while zooming.
- Added ruler modes for global, intra-contig, and intra-scaffold coordinates.
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
