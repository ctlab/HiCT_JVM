# HiCT Changelog

## 1.0.181-7d4d4d9-webui_31467f1

Changes since `master` version `1.0.175-5ddcf3c-webui_a609177`:

- Added a lazy web file selector for large server-side data directories, with path entry, explorer/tree views, symbolic-link support, and safer parent-directory navigation.
- Added support for opening and converting additional matrix-like input formats through the conversion workflow, including sparse text formats supported by hictk.
- Fixed `.hic` plus Juicebox assembly conversion regressions and improved handling of hidden/missing contigs during conversion.
- Fixed `.hict.hdf5` to `.mcool` export and `.mcool` re-import so exported files preserve contigs, contig lengths, directions, scaffold IDs, hidden contigs, and raw pixels instead of reopening as a single `assembly` contig.
- Added metadata-preserving internal `.mcool` export as the default automatic export path, while keeping explicit hictk-assisted export available.
- Added resolution restriction controls so users can keep the map at selected bin sizes while zooming.
- Added ruler modes for global, intra-contig, and intra-scaffold coordinates.
- Added configurable OSD overlay settings, including field visibility, order, and position.
- Added an export workflow for converting `.hict.hdf5` matrices to `.mcool`, with current or custom AGP assembly state.
- Improved portable package size by reducing redundant bundled content where possible.
- Split platform-specific portable runtime contents from the universal fat JAR intent so portable packages can avoid incompatible native payloads while the universal JAR can keep broad compatibility.
- Fixed macOS packaging checks around platform-specific JHDF5/HDF5 native payloads.
- Enabled native processing by default when a supported native library is available.
- Improved color threshold controls for live updates and very small signal values.
- Reduced WebUI freezes caused by large contig/scaffold style updates.
- Updated the About window with correspondence contacts and this changelog.
- Rendered the About window changelog as Markdown for readable headings, lists, links, and inline code.
