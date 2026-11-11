# Converter integrity and bounded work

The on-disk HiCT/Cooler schema, compression options, integer precision and assembly
mapping are unchanged. The importer skips redundant native sorting only when the
input coordinates are already row-major; unsorted blocks retain the stable sort.
Its count pass reads only column identifiers, and stripe batches respect both the
configured budget and the actual JVM heap. Producer/writer failures now propagate
instead of allowing a successful return with an incomplete output. Queue shutdown
and native reader lifetimes are coordinated.

Open HDF5 dataset handles keep compressed dataset caches alive during writes. The
writes deliberately use the path overload: the handle overload in this JHDF5
version may shrink a dataset when parallel stripes finish out of order. Regression
coverage checks dataset extent and every pixel across multiple parallel batches.

The exporter reuses a cursor and reads packed COO triples in blocks, avoiding a
record allocation per pixel and repeated primitive stream calls. Intermediate COO
counts remain exact 64-bit integers, including values above the exact range of
double precision. Final Cooler counts retain the existing int32 schema and
checked conversion; this change does not expand its representable range.
Truncated intermediate records fail explicitly.

Local RHINO 1 kb verification compares every input pixel directly with the HiCT
sparse-block arrays using an independent Python/HDF5 oracle. The original 6 GiB
heap run returned success but omitted an entire stripe batch (36,845,303 of
370,053,976 pixels); it is excluded from performance claims. Both the original
16 GiB run and the corrected 6 GiB run passed all 370,053,976 pixels. Memory
pressure is a suspected trigger of the original worker failure, not a proven
exception diagnosis. Repeatable paired timing and raw profiles are kept in the
benchmark suite; timings require completed correctness checks.
