# Additional Matrix Format Support Report

Date: 2026-06-14

Status: investigation and implementation plan only. No converter code was changed for this task before it was paused.

## Goal

Add a safe way to open more matrix-like inputs by converting them into `.hict.hdf5` through the existing HiCT conversion workflow.

Requested formats:

- HiC-Pro `.matrix` with companion `.bed` bin table.
- Generic TSV/CSV COO, optionally compressed.
- BEDPE / bedGraph2-like 2D.
- `.pairs`, `.pairs.gz`, `.validPairs`.
- PAF from minimap2-like aligners.
- SciPy sparse `.npz`.

## Current HiCT Architecture

Relevant backend files:

- `src/main/java/ru/itmo/ctlab/hict/hict_server/handlers/files/FSHandlersHolder.java`
  - Lists files for the UI.
  - Currently exposes convertible matrix files with suffixes: `.hic`, `.cool`, `.mcool`.
  - `/resolve_matrix_source` delegates to `MatrixConversionCacheManager`.

- `src/main/java/ru/itmo/ctlab/hict/hict_server/util/cache/MatrixConversionCacheManager.java`
  - Resolves open behavior:
    - `.hict.hdf5` / `.hict`: open directly.
    - `.cool` / `.mcool`: convert to `.hict.hdf5` or reuse cache.
    - `.hic`: convert to `.hict.hdf5` through hictk or reuse cache.
  - Stores conversion freshness metadata under `processed/matrix_conversion_cache`.

- `src/main/java/ru/itmo/ctlab/hict/hict_server/handlers/conversion/ConversionDirection.java`
  - Currently supports:
    - `hict-to-mcool`
    - `mcool-to-hict`
    - `hic-to-mcool`
    - `hic-to-hict`

- `src/main/java/ru/itmo/ctlab/hict/hict_server/handlers/conversion/HictkConversionPipeline.java`
  - Existing hictk-backed pipeline:
    - `.hic` metadata
    - `.hic` finest resolution to temporary `.cool`
    - `.cool` to `.mcool`
    - balance `.mcool`
    - import `.mcool` to `.hict.hdf5`

- `src/main/java/ru/itmo/ctlab/hict/hict_server/handlers/conversion/ExternalToolchainManager.java`
  - Resolves bundled/system hictk, minimap2/mm2-plus, cooler, and Python.
  - hictk is already the best available external dependency for most requested text formats.

Relevant frontend files:

- `HiCT_WebUI/src/app/ui/components/upper_ribbon/FileWizardModal.vue`
  - Source picker currently accepts `.hict.hdf5`, `.hic`, `.cool`, `.mcool`.
  - Calls `/resolve_matrix_source`.

- `HiCT_WebUI/src/app/ui/components/upper_ribbon/CoolerConverter.vue`
  - Batch conversion UI uses `/list_convertible_matrices`.
  - Has one optional `.assembly` sidecar field for `.hic` conversions.

- `HiCT_WebUI/src/app/core/net/api/request.ts`
  - `StartConversionJobRequest` and `StartBatchConversionJobsRequest` currently allow `assemblyFilename`, but not arbitrary sidecars such as bin tables or chrom sizes.

## Tooling Findings

Local bundled hictk was available at:

```text
HiCT_JVM/toolchains-dist/linux_x86_64/bin/hictk
```

`hictk load --help` shows the most useful path for this task:

- `hictk load interactions output-path`
- Supported compressed inputs: bzip2, gzip, lz4, lzo, xz, zstd.
- Supported input formats: `4dn`, `validpairs`, `bg2`, `coo`.
- For non-4DN inputs it requires either:
  - `--chrom-sizes` plus `--bin-size`, or
  - `--bin-table` as BED3+.
- Useful flags:
  - `--threads`, bounded to 2-24.
  - `--chunk-size`.
  - `--count-as-float`.
  - `--one-based` / `--zero-based`.
  - `--transpose-lower-triangular-pixels`.
  - `--drop-unknown-chroms`.

`hictk zoomify --help` confirms it can build `.mcool` pyramids from single-resolution `.cool`.

Important limitation: when loading interactions into `.cool`, hictk says only up to two threads are used. That means we can expose `parallelism`, but should not promise full 24-core utilization for the hictk load stage. Later stages may still use more CPU.

## External References

- hictk CLI reference: https://hictk.readthedocs.io/en/stable/cli_reference.html
- HiC-Pro results/output documentation: https://nservant.github.io/HiC-Pro/RESULTS.html
- 4DN pairs specification: https://github.com/4dn-dcic/pairix/blob/master/pairs_format_specification.md
- minimap2 PAF documentation: https://lh3.github.io/minimap2/minimap2.html
- SciPy sparse `.npz`: https://docs.scipy.org/doc/scipy/reference/generated/scipy.sparse.save_npz.html

## Recommended Conversion Routes

### 1. HiC-Pro `.matrix` + `.bed`

Best route:

```text
hictk load --format coo --bin-table <bins.bed> <matrix> <base.cool>
hictk zoomify <base.cool> <generated.mcool>
hictk balance ice ...
McoolToHictConverter -> .hict.hdf5
```

Rationale:

- HiC-Pro matrix rows are effectively COO pixels.
- HiC-Pro `.bed` provides the bin table.
- This can reuse the existing hictk + `.mcool` + HiCT importer pipeline.

Needed UI/backend additions:

- Detect `.matrix` as a convertible matrix source.
- Add optional or auto-detected `binTableFilename`.
- If the file is named `sample.matrix`, first try sibling candidates:
  - `sample.bed`
  - `sample_abs.bed`
  - any `.bed` selected by the user.

### 2. Generic TSV/CSV COO

Best route when coordinates are direct bin ids:

```text
generate synthetic BED3+ bin table
hictk load --format coo --bin-table <synthetic.bed> <coo> <base.cool>
...
```

Alternative route when genomic coordinates are present:

```text
hictk load --format coo --chrom-sizes <chrom.sizes> --bin-size <N> <coo> <base.cool>
...
```

Open questions:

- Need a UI option to distinguish:
  - direct bin ids: `row col count`
  - genomic bin coordinates / chrom-aware COO
- User requested default 1:1 resolution. For direct matrix indices, the safe default is:
  - one synthetic chromosome, e.g. `matrix`
  - bin size = 1
  - bin table rows for observed matrix dimension.

Implementation notes:

- A Java pre-scan can determine max row/col for synthetic bin table generation.
- It should stream the file and support gzip/xz/zstd. hictk can read these compressions directly, but Java pre-scan needs either:
  - a compression-aware reader, or
  - skip auto dimension detection and require explicit `matrixSize`.

### 3. BEDPE / bedGraph2-like 2D

Best route:

```text
hictk load --format bg2 --chrom-sizes <chrom.sizes> --bin-size <N> <bg2/bedpe-like> <base.cool>
...
```

Needed UI/backend additions:

- Detect useful suffixes, probably:
  - `.bg2`, `.bedgraph2`, `.bedpe`
  - compressed variants `.gz`, `.xz`, `.zst`, `.zstd`
- Ask for `.chrom.sizes` and bin size unless a `.chrom.sizes` sibling is found.

Risk:

- BEDPE conventions vary. hictk `bg2` should be the supported strict path. A generic BEDPE parser should not be hand-written for the release path.

### 4. `.pairs`, `.pairs.gz`, `.validPairs`

Best routes:

```text
hictk load --format 4dn <pairs> <base.cool>
```

or:

```text
hictk load --format validpairs --chrom-sizes <chrom.sizes> --bin-size <N> <validPairs> <base.cool>
```

Notes:

- 4DN pairs can carry useful header metadata. hictk can use it.
- `validPairs` usually needs chrom sizes and bin size.

Needed UI/backend additions:

- Detect:
  - `.pairs`
  - `.pairs.gz`
  - `.pairs.bgz`
  - `.validPairs`
  - `.validPairs.gz`
- Ask for `.chrom.sizes` + `binSize` when required.

### 5. SciPy sparse `.npz`

Do not implement as a direct Java parser first.

Reason:

- SciPy `.npz` can store `csc`, `csr`, `bsr`, `dia`, or `coo`.
- A robust Java implementation would need NumPy `.npy` parsing, dtype handling, sparse layout conversion, matrix shape validation, and probably compression handling.

Safer route:

```text
Python helper using scipy.sparse.load_npz
stream/export COO rows
generate synthetic BED3+ bin table
hictk load --format coo --bin-table <synthetic.bed> -
...
```

Required runtime:

- Python with SciPy available, either bundled or user configured through `HICT_PYTHON_BIN`.

Recommended behavior for first implementation:

- Recognize `.npz`.
- If Python/SciPy is unavailable, show a clear UI/backend error:
  - `SciPy .npz conversion requires Python with scipy.sparse available.`
- Later, add a bundled minimal Python helper only if packaging size is acceptable.

### 6. PAF

Do not treat PAF as a normal Hi-C matrix source without a design decision.

Reason:

- PAF is an alignment format, not a contact matrix format.
- Converting PAF into a Hi-C-like square matrix requires choices:
  - Which axis is query/reference?
  - Is the output symmetric?
  - What value should be accumulated: number of alignments, aligned bases, MAPQ, score?
  - What bin size should be used?
  - Should it become a dotplot/overlay track instead of a primary matrix?

Recommended first path:

- Keep PAF in dotplot/overlay tooling, not Open Wizard matrix opening.
- If matrix opening is required later, implement it as a separate `PAF dotplot matrix` mode with explicit options:
  - query FASTA / reference FASTA or chrom sizes
  - bin size
  - value aggregation method
  - symmetric vs rectangular projection policy

## Backend Implementation Plan

### Step 1: Add Source Format Model

Add a small backend model, e.g.:

```java
enum MatrixSourceFormat {
  HICT,
  COOL,
  MCOOL,
  HIC,
  HICPRO_MATRIX,
  COO,
  BG2,
  PAIRS_4DN,
  VALIDPAIRS,
  SCIPY_NPZ,
  PAF,
  UNKNOWN
}
```

This should centralize suffix detection and avoid scattering extension checks across:

- `ConversionDirection`
- `MatrixConversionCacheManager`
- `FSHandlersHolder`
- frontend predicates

### Step 2: Add New Conversion Directions

Add:

- `HICTK_LOAD_TO_HICT`
- optionally `HICTK_LOAD_TO_MCOOL`
- possibly `SCIPY_NPZ_TO_HICT` later

Keep `.hic` as its existing special path because it uses `hictk convert`, not `hictk load`.

### Step 3: Generalize HictkConversionPipeline

Add a method like:

```java
convertLoadedTextSourceToHict(...)
```

Pipeline:

```text
metadata/prepare sidecars
hictk load -> base.cool
hictk zoomify -> generated.mcool
hictk balance ice
McoolToHictConverter -> output.hict.hdf5
```

The existing stage/progress event format (`HICT_STAGE ...`) can be reused.

### Step 4: Add Request Sidecar DTO Fields

Extend conversion job request handling with optional fields:

- `sourceFormat`
- `binTableFilename`
- `chromSizesFilename`
- `binSize`
- `matrixSize`
- `oneBased`
- `countAsFloat`
- `assumeSorted`
- `transposeLowerTriangularPixels`
- `dropUnknownChroms`

For batch jobs, use maps when values differ by file:

- `binTableFilenameByFile`
- `chromSizesFilenameByFile`
- `binSizeByFile`
- `sourceFormatByFile`

### Step 5: Auto-Detect Sidecars

For release ergonomics:

- HiC-Pro `.matrix`: try same stem `.bed` first.
- BG2/validPairs/COO: try same stem `.chrom.sizes`.
- Generic COO direct-index mode: allow synthetic bin table generation.

If sidecars are missing, fail early with a readable message before starting hictk.

### Step 6: Cache Metadata

Conversion cache currently fingerprints only the source and output. For sidecar-dependent formats, cache metadata must include sidecar fingerprints and conversion options.

Add to cache metadata:

- `sourceFormat`
- sidecar file fingerprints
- bin size / matrix size / one-based/count-as-float flags
- hictk load format

Otherwise users could change a `.bed` or `.chrom.sizes` and HiCT would wrongly reuse stale `.hict.hdf5`.

## Frontend Implementation Plan

### File Wizard

Update source picker:

- Accepted source text should include:
  - `.hict.hdf5`, `.hic`, `.cool`, `.mcool`
  - `.matrix`
  - `.tsv`, `.csv`, `.coo`
  - `.bg2`, `.bedgraph2`, `.bedpe`
  - `.pairs`, `.validPairs`
  - `.npz` as recognized but conditionally unsupported
  - `.paf` as recognized but not a normal matrix source unless explicitly enabled

Add sidecar prompts only when needed:

- `.matrix`: bin table `.bed`.
- `.bg2`/`.bedpe`/`.validPairs`: `.chrom.sizes` + bin size, or bin table.
- generic COO: mode selector:
  - direct matrix bins
  - genomic coordinates

### Converter Modal

Current modal has one optional `.assembly` field. It should become a per-format settings section:

- hictk-backed text formats:
  - input format
  - sidecar files
  - bin size
  - indexing mode
  - count type
- `.hic`:
  - optional `.assembly`, unchanged.
- `.cool`/`.mcool`:
  - no extra sidecars.

### User Messaging

Show hictk note for all hictk-backed formats, not only `.hic`:

```text
This source will be converted with hictk.
Input format: coo/bg2/4dn/validpairs
Selected executable: ...
Project: hictk
Citation: Rossini R, Paulsen J. Bioinformatics 2024;40(7):btae408.
```

For unsupported/deferred formats:

- `.npz`: explain SciPy/Python requirement.
- `.paf`: explain that PAF is an alignment/dotplot source and needs explicit binning policy before opening as a matrix.

## Native/Performance Notes

The user requested native implementations for generic x86_64-v2, AVX2, and AVX512. Recommended approach:

1. First release should use hictk for supported text formats. It is already native C++ and bundled.
2. Do not duplicate hictk load in `hict-native` before there is a measured bottleneck.
3. If native acceleration is still required later, choose narrow helpers:
   - compressed pre-scan for direct COO max row/col
   - synthetic bin table generation
   - PAF aggregation if PAF matrix mode is approved
4. Avoid writing native `.cool`/`.mcool` writers in HiCT. That duplicates hictk/cooler and increases file-format risk.

Memory target:

- hictk `--chunk-size` should be exposed and defaulted conservatively.
- Java pre-scans should stream and avoid retaining interactions.
- For direct-index COO, only max row/col and basic validation are needed during pre-scan.

## Suggested Test Plan

Backend unit tests:

- Format detection for all suffixes, including compressed variants.
- Output filename derivation.
- Required sidecar validation.
- Command construction for:
  - HiC-Pro `.matrix` + `.bed`
  - COO + synthetic bin table
  - BG2 + chrom sizes/bin size
  - pairs 4DN
  - validPairs + chrom sizes/bin size
- Cache invalidation when sidecars change.

Backend integration tests:

- Generate tiny text fixtures and run hictk if available.
- Convert to `.hict.hdf5`.
- Open with `ChunkedFile`.
- Verify at least:
  - resolutions exist
  - contig/chromosome names are present
  - a known nonzero pixel survives conversion

Frontend tests/checks:

- File picker accepts new suffixes.
- Wizard requests sidecars only when needed.
- Human-readable unsupported messages for `.npz`/`.paf`.
- Batch converter blocks jobs with missing sidecars before submission.

Manual smoke tests:

- HiC-Pro matrix + bed fixture.
- 4DN pairs fixture.
- validPairs fixture.
- gzipped input fixture.
- zstd input fixture if bundled hictk supports it in release artifacts.

## Recommended Next Slice

When resuming, implement in this order:

1. Create central source-format detection in JVM.
2. Extend file listing and matrix source resolution for hictk-loadable formats.
3. Add a hictk load pipeline for:
   - HiC-Pro `.matrix` + `.bed`
   - generic `.coo` direct-index with synthetic bin table
   - `.pairs` / `.pairs.gz`
4. Add backend tests for command construction and cache metadata.
5. Add WebUI source picker support and simple sidecar prompts.
6. Add BG2/validPairs once the first hictk load path is proven.
7. Revisit `.npz` and PAF as separate, explicitly designed features.

## Key Risk Summary

- The safe production path is hictk-backed loading into `.cool`, then the existing `.mcool` and HiCT importer pipeline.
- `.npz` and PAF should not be silently advertised as fully supported until their semantics/runtime requirements are explicit.
- Cache metadata must include sidecar fingerprints, otherwise converted outputs can become stale without HiCT noticing.
- Full native reimplementation is not the right first release path; hictk already provides native loading and compression support for most requested formats.

## Implementation Status - 2026-06-14

Implemented release-scoped support without a global conversion overhaul:

- Backend source detection and output derivation now cover:
  - Hi-C Pro `.matrix` plus compressed variants
  - generic COO `.coo`, `.coo.tsv`, `.coo.csv`, `.tsv`, `.csv` plus compressed variants
  - BEDPE/bedGraph2 `.bedpe`, `.bg2`, `.bedgraph2` plus compressed variants
  - `.pairs`/`.pairs.gz`/`.pairs.bgz`
  - `.validPairs` plus compressed variants
- Existing `hictk` pipeline now supports `hictk load -> zoomify -> balance -> .hict.hdf5 import`.
- Matrix conversion cache schema was bumped and now includes sidecar fingerprints for selected or auto-discovered `.bed`/chrom-sizes files.
- File listing, Open Wizard, manual Open, and batch converter now expose the new hictk-loadable formats.
- Open Wizard and batch converter can pass optional BED bin table, chrom sizes, bin size, and float-count options.

Important hictk CLI finding from local smoke tests:

- `hictk load --format coo` in bundled hictk v2.2.0 rejects `--bin-table` for pre-binned COO input.
- HiCT therefore derives temporary `.chrom.sizes` plus bin size from Hi-C Pro BED bin tables, and uses `--chrom-sizes --bin-size` for COO/Hi-C Pro matrix loading.
- Generic COO without explicit chrom sizes is auto-scanned only for plain text and `.gz`; HiCT creates a synthetic single-chromosome `assembly` coordinate system with default bin size `1`.
- hictk COO parsing accepted tab-separated input in smoke testing. Space-separated generic COO is not guaranteed by hictk and should be normalized by users or handled by a future streaming normalizer if needed.

Still intentionally not implemented in this release slice:

- Direct SciPy `.npz` import.
- PAF as a primary matrix source. PAF is still better treated as dotplot/overlay input until matrix semantics are explicitly designed.
- Native reimplementation of hictk loaders in `hict-native`; this remains a later performance project after measuring real bottlenecks.
