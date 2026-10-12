# Active assembly, unscaffolded AGP scope and partial overlays

Debris is now excluded from both active persistent-tree roots. Changing a
resolution-level visibility flag was insufficient: BINS queries and FASTA could
still reference those contigs. Exclusion shortens active coordinates at every
resolution and removes the selected ATUs from matrix queries and sequence
exports. Original HDF5/FASTA files and history snapshots remain intact. Undo/redo
restores the exact roots; an empty assembly returns empty ATU lists and zero
matrix windows. Selection still expands to whole scaffolds.

AGP imports without an explicit policy now request a choice for either omitted
sequence or explicit unscaffolded objects. HiCT's convention is a single-component
object named `unscaffolded_*`; ordinary singleton scaffolds remain scaffolds.
`SCAFFOLDED_ONLY` excludes both categories, `APPEND_UNPLACED` includes all sequence,
and legacy `EXCLUDE` still includes every explicit AGP component while excluding
only omitted intervals. Preview and cancellation do not alter roots or history.
All records, including excluded ones, are validated before mutation. The WebUI
explains that the choice controls heatmaps, queries and FASTA output together.

Overlay synchronization previously discarded partial components unless their
length equalled an entire target contig, and reset component coordinates to the
whole contig. On quad AGP 10 it skipped eight partial pieces and moved unrelated
secondary contacts to the beginning of the assembly. It now carries the absolute
source interval and orientation into the target importer. Visibility matches
active source intervals instead of ambiguous name aliases and historical
descriptors. Empty active scope also clears the target root. The agreed
source-bin-start ownership policy for unaligned cuts is unchanged.

Regression tests cover all matrix cells at multiple resolutions, reverse pieces,
zero-bin coarse pieces, FASTA exclusion, complete removal, history restoration,
preview atomicity, omitted and explicit unscaffolded components, and paired
overlay synchronization in both directions. No file-format, build-workflow or
native-library changes are required.
