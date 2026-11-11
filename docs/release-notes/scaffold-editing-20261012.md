# Scaffold editing semantics

Raw maps without a stored named scaffold layout open with one singleton scaffold per contig, named `scf_<contig-name>`. Existing named layouts and AGP imports retain their scaffold structure. Contig and scaffold display names are independent.

Reverse, move, and debris operations expand the selection to whole scaffolds. A move destination inside another scaffold is snapped before that scaffold in both coordinate trees. A destination inside the source is a no-op. Ungrouping creates singleton scaffolds. The explicit `POST /unscaffold_all` command leaves all contigs unplaced and participates in the persistent undo/redo journal.

Native scissors retain the existing rule that removes one finest-resolution bin. Splitting inside a multi-contig scaffold now creates two scaffolds, retaining the other contigs on the appropriate side and the parent's gap size. Splitting a singleton creates two named singletons. Pieces with no bins at a coarse resolution are hidden at that resolution. Undo restores the original roots and removed bin. AGP partial-component import continues to use its separate, base- and bin-preserving policy.

The edit algorithms still use the persistent coordinate trees. Bulk ungrouping necessarily creates one scaffold descriptor per selected contig. AGP export now uses its already-resolved scaffold descriptor for name lookup, avoiding a full scaffold traversal per output scaffold.

Validation: `ScaffoldEditingInvariantTest` covers singleton initialization, independent names, whole-scaffold movement/reversal, destinations within source/target scaffolds, debris, explicit unscaffolding, forward/reverse splits inside a multi-contig scaffold, zero-bin coarse pieces, exact matrix values at three resolutions, and snapshot restoration. OpenAPI coverage includes the new route. Additional production-browser and real-data evidence is maintained by the benchmark suite.

Track caches coordinate HDF5 reader and writer handle lifetimes to prevent an attempted read/write reopen while a query still holds a read-only handle. Cache hits do not enqueue another all-resolution indexing job. Cache identity continues to include assembly, resolution and track mode. The guard uses a bounded array of locks shared within the JVM and does not lock in-memory queries or matrix computation. Separate server processes should use separate processed directories.
