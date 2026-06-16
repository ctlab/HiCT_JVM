# HiCT Changelog

## 1.0.178-eb2770f-webui_09b8ce7

Changes since the last merge point with `master`:

- Added a lazy web file selector for large server-side data directories, with path entry, explorer/tree views, symbolic-link support, and safer parent-directory navigation.
- Added support for opening and converting additional matrix-like input formats through the conversion workflow, including sparse text formats supported by hictk.
- Fixed `.hic` plus Juicebox assembly conversion regressions and improved handling of hidden/missing contigs during conversion.
- Added resolution restriction controls so users can keep the map at selected bin sizes while zooming.
- Added ruler modes for global, intra-contig, and intra-scaffold coordinates.
- Added configurable OSD overlay settings, including field visibility, order, and position.
- Added an export workflow for converting `.hict.hdf5` matrices to `.mcool`, with current or custom AGP assembly state.
- Improved portable package size by reducing redundant bundled content where possible.
- Enabled native processing by default when a supported native library is available.
- Improved color threshold controls for live updates and very small signal values.
- Reduced WebUI freezes caused by large contig/scaffold style updates.
- Updated the About window with correspondence contacts and this changelog.
