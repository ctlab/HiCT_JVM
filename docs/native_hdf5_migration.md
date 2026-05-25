# Native HDF5 Migration Plan

HiCT currently uses JHDF5 for file I/O and optional JNI kernels for compute-heavy
inner loops. The target architecture is a selectable native HDF5 backend with
AVX2 and AVX-512 compute variants, but it must be migrated in stages so the Java
backend remains a trustworthy fallback.

## Stage 1: Isolated Native Kernels

Status: implemented for selected hot loops.

- Tile base-signal preparation.
- Post-log tile transform.
- Linear-gradient RGBA rasterization.
- Non-segmented expected/observed-over-expected transform.
- Converter dense/sparse stripe counting.
- Converter sparse stripe row-major sorting.
- 1D precomputed-track aggregation.
- BigWig/BED-style projected interval aggregation.
- FASTA reverse-complement export.

These kernels do not own files and are parity-tested against Java arrays. A
native rejection or failure disables native processing for the current process
and continues with Java.

## Stage 2: Native HDF5 Facade

Status: not implemented yet.

Introduce a narrow Java interface for the HDF5 operations HiCT actually needs:

- fixed-size numeric dataset reads by block and offset;
- fixed-size numeric dataset writes by block and offset;
- string and scalar metadata reads/writes;
- dataset shape/type checks;
- explicit close/lifetime management for per-worker readers and writers.

The first implementation stays backed by JHDF5. A second implementation can then
wrap the HDF5 C API through JNI. This avoids changing converter and tile code at
the same time as replacing the file backend.

## Stage 3: Native File Backend

Status: future work.

Build separate native HDF5/JHDF5-compatible payloads:

- generic amd64 HDF5/JHDF5 fallback;
- AVX2 optimized HDF5/JHDF5 build;
- AVX-512 optimized HDF5/JHDF5 build.

The runtime selector must choose AVX-512 only when CPU and JVM feature checks
agree. If native file backend initialization fails, HiCT must stay on JHDF5.

## Stage 4: File-Level Parity

Status: future work.

Every native file operation must be validated against JHDF5 using representative
datasets:

- `.mcool -> .hict.hdf5` conversion;
- `.hict.hdf5 -> .mcool` conversion;
- tile read/query paths at multiple resolutions;
- 1D sidecar cache read/write;
- optional compression plugin combinations present in released packages.

Benchmarks should compare Java/JHDF5, native AVX2, and native AVX-512 on the
same data and report both throughput and output checksums.
