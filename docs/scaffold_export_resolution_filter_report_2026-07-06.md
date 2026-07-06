# Scaffolded Cooler Export and Selected-Resolution Loading Report

Date: 2026-07-06

## Scope

This report records the fixes made for recent user reports around `.hict.hdf5` to Cooler export, AGP-driven scaffolded export, selected-resolution performance, WebUI action errors, and compact toolbar layout.

The main target dataset for release validation remains:

- `/mnt/Models/HiCT/data/micro-c/GSE286495/GSE286495_mESC_merged_15.6B_mm39.mcool`
- `/mnt/Models/HiCT/data/micro-c/GSE286495/GSE286495_mESC_merged_15.6B_mm39.mcool.agp`
- `/mnt/Models/HiCT/data/micro-c/GSE286495/IlyaAssembly.agp`

No implementation code writes to `/mnt/Models`; validation outputs should be placed under `/tmp` or a caller-selected writable scratch directory.

## Findings

1. Selected-resolution `.hict.hdf5` export initialized every resolution.
   `HictToMcoolConverter` and `HictToMcoolExportPipeline` constructed a full `ChunkedFile` before export resolution selection had taken effect. `ChunkedFile` initialization reads per-resolution metadata, ATL and Cooler weights, so exporting one coarse resolution still paid the initialization cost for every resolution in large files.

2. Batch conversion did not always apply the selected AGP before export.
   In the batch conversion path, an assembly path could be present while `applyAgpBeforeExport` remained false for `HICT_TO_MCOOL`. This made user-selected AGP files look accepted by the UI but not reflected in the exported Cooler layout.

3. WebUI scaffolding actions exposed raw JavaScript stack traces when no selection existed.
   Reverse/move/translocation code reduced an empty selected-contig collection and let the exception bubble to the UI.

4. Header buttons used too much fixed horizontal space.
   Export buttons carried full labels at all viewport widths, and the toolbar did not wrap cleanly, so OSD/ruler/export controls could overlap in narrower windows.

## Implemented Changes

### JVM

- `ChunkedFile` now supports selected-resolution initialization through `ChunkedFileOptions.selectedResolutions`.
- Export code reads available resolution names cheaply from HDF5 metadata first, resolves the user-selected set, and constructs `ChunkedFile` with only those resolutions.
- Requested resolutions are validated against complete resolution groups with clear errors when missing or incomplete.
- Batch `HICT_TO_MCOOL` conversion now applies an AGP whenever an assembly path is provided.
- Added a convenience CLI subcommand:

```bash
java -jar hict_server-...-fat.jar convert scaffold \
  --input input.mcool \
  --output output.mcool \
  --assembly layout.agp \
  --resolutions 10000000 \
  --export-mode hictk
```

The scaffold subcommand accepts supported source formats where possible, optional `.agp`/`.assembly`, optional FASTA, and output format selection through the output extension.

### WebUI

- Reverse, move-to-debris and translocation now show a friendly toast when no contig range is selected.
- Header controls now wrap responsively.
- SVG/PNG/PDF buttons collapse to icon-only controls at narrower viewport widths.

### Validation Utilities

Added scripts under `HiCT_Utils/scripts`:

- `validate_scaffolded_cooler.py`
  - validates AGP scaffold/chromosome order and sizes against exported Cooler metadata;
  - samples randomized windows within positive-orientation AGP components;
  - compares raw unbalanced matrices between source and exported Cooler files;
  - writes optional JSON validation reports.
- `benchmark_hict_conversion.py`
  - runs repeated import/export benchmarks through the HiCT CLI;
  - records CSV timing rows;
  - creates optional matplotlib boxplots and resolution scatterplots when matplotlib is available.

Both scripts are explicit about missing Python dependencies. `cooler` is required for matrix validation.

## Expected Behavior After Fix

- Exporting only `1:10M` from a large `.hict.hdf5` should initialize only the `10000000` resolution.
- Exporting one selected finest resolution for hictk zoomification should initialize only that finest resolution.
- WebUI exports from an already opened session can keep using the in-memory model for the active session path; CLI exports avoid building unrelated resolution models.
- Batch/WebUI conversion with an AGP selected for `HICT_TO_MCOOL` applies that AGP before writing Cooler bins/chrom metadata.
- If the user tries scaffolding actions without a selection, a readable message appears instead of a minified stack trace.

## Validation Status

Completed locally:

- `./gradlew compileJava` in `HiCT_JVM`.
- `npm run build:web` in `HiCT_WebUI`.
- `./gradlew shadowJar` in `HiCT_JVM`.
- `python3 -m py_compile` for the new validation and benchmark scripts.
- CLI help smoke test for `convert scaffold`.
- Small selected-resolution export smoke test:
  - input: `/mnt/Models/HiCT/data/_processed_backup/3L1.self.k17w5.hict.hdf5`
  - output: `/tmp/hict_3l1_smoke.cool`
  - command: `convert hict-to-mcool --export-mode=internal --balance-exported-coolers=false`
  - result: valid Cooler metadata from hictk, `bin-size=250`, `nchroms=1`, `nnz=648999`, output size `2.9 MiB`.
- Small `convert scaffold` wrapper smoke test with the same source:
  - output: `/tmp/hict_3l1_scaffold_smoke.cool`
  - result: valid Cooler metadata from hictk, `bin-size=250`, `nchroms=1`, `nnz=648999`, output size `2.9 MiB`.

Pending or environment-limited:

- Full GSE286495 end-to-end export validation was not executed in this pass because it is multi-hour scale and local Python `cooler` is not installed in the default interpreter.
- The validation script is ready for a Python environment with `cooler`, `numpy`, and `h5py`.
- Benchmark plotting requires `matplotlib`; the benchmark still writes CSV without it.

Suggested release validation command pattern:

```bash
python HiCT_Utils/scripts/validate_scaffolded_cooler.py \
  --source /mnt/Models/HiCT/data/micro-c/GSE286495/GSE286495_mESC_merged_15.6B_mm39.mcool \
  --exported /tmp/GSE286495_agp_export.mcool \
  --agp /mnt/Models/HiCT/data/micro-c/GSE286495/GSE286495_mESC_merged_15.6B_mm39.mcool.agp \
  --resolution 10000000 \
  --samples 20 \
  --window-bins 16 \
  --report-json /tmp/GSE286495_agp_export.validation.json
```

## Remaining Risk

Selected-resolution `ChunkedFile` initialization intentionally changes only export callers. Visualization and interactive assembly still use the full model unless explicitly changed later. This keeps existing UI contracts stable but means future callers must deliberately request filtered initialization when they only need selected resolutions.
