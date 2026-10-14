# HiCT_JVM API Reference for Python client (`hict_jvm_api`)

This document describes the endpoint subset used by the `jvm-api-v1` Python library.
All endpoints are served by HiCT_JVM API server (`start-api-server` or `start-server`).

Interactive documentation is served by the backend itself:

- `GET /api/v1/` - Swagger UI
- `GET /api/v1/openapi.yaml` - OpenAPI v1 source

## Base URL

Typically:

- `http://localhost:5000`
- `http://localhost:5001`

## Core endpoints

### Session and files

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/version` | Server/version metadata |
| `POST` | `/list_files` | List files under `DATA_DIR` |
| `POST` | `/list_files_detailed` | List files with metadata |
| `POST` | `/list_coolers` | List `.cool/.mcool` files |
| `POST` | `/list_fasta_files` | List FASTA files |
| `POST` | `/list_agp_files` | List AGP files |
| `POST` | `/open` | Open primary HiCT source |
| `POST` | `/open_progress` | Read open progress |
| `POST` | `/attach` | Attach existing in-memory session |
| `POST` | `/close` | Close active session |

### Secondary source

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/secondary/status` | Read secondary-source status |
| `POST` | `/secondary/open` | Attach secondary source (`allowMismatch` supported) |
| `POST` | `/secondary/close` | Detach secondary source |
| `POST` | `/secondary/set_assembly_source` | Set `PRIMARY` or `SECONDARY` assembly source |

### Tiles and rendering

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/get_tile` | Fetch rendered tile/region |
| `POST` | `/matrix/query` | Fetch numeric submatrix (raw/weighted/normalized/pipeline signal) |
| `POST` | `/tiles/reload` | Invalidate tile caches and bump versions |
| `POST` | `/get_visualization_options` | Get current visualization options |
| `POST` | `/set_visualization_options` | Set visualization options |
| `POST` | `/render_pipeline/get` | Get custom rendering pipeline graph |
| `POST` | `/render_pipeline/set` | Set custom rendering pipeline graph |
| `POST` | `/render_pipeline/reset` | Reset pipeline to defaults |
| `POST` | `/native_processing/status` | Read optional native-processing backend status |
| `POST` | `/native_processing/enabled` | Enable or disable optional native processing for the current process |

`/get_tile` supported formats:

- `JSON_PNG_WITH_RANGES` (JSON with base64 PNG + ranges)
- `PNG` (raw PNG bytes)
- `PNG_BY_PIXELS` (raw PNG bytes for arbitrary pixel-space region)

`/matrix/query` supports:

- Units: `PIXELS`, `BINS`, `BP`
- Signal modes: `RAW_COUNTS`, `COOLER_WEIGHTED`, `TRADITIONAL_NORMALIZED`, `PIPELINE_SIGNAL`
- Response formats: `BINARY_FLOAT32`, `BINARY_FLOAT64`, `BINARY_INT64`, `JSON`

### Scaffolding operations

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/reverse_selection_range` | Reverse assembly interval |
| `POST` | `/move_selection_range` | Move interval |
| `POST` | `/split_contig_at_bin` | Split contig at pixel/bin coordinate |
| `POST` | `/group_contigs_into_scaffold` | Group interval into scaffold |
| `POST` | `/ungroup_contigs_from_scaffold` | Ungroup scaffold interval |
| `POST` | `/move_selection_to_debris` | Move interval to debris |

### FASTA / AGP

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/link_fasta` | Link FASTA to current source |
| `POST` | `/get_fasta_for_assembly` | Export FASTA for full assembly |
| `POST` | `/get_fasta_for_selection` | Export FASTA for selected rectangle |
| `POST` | `/get_agp_for_assembly` | Export AGP |
| `POST` | `/load_agp` | Import AGP and update assembly state |

### Conversion jobs

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/convert/jobs` | Submit single conversion job |
| `POST` | `/convert/jobs/batch` | Submit batch conversion jobs |
| `POST` | `/convert/jobs/list` | List all conversion jobs |
| `GET` | `/convert/jobs/:jobId` | Get single job status |
| `POST` | `/convert/jobs/:jobId/stop` | Cancel running job |
| `POST` | `/dotplot/jobs/:jobId/stop` | Cancel running dotplot job |
| `GET` | `/convert/download/:jobId` | Download completed conversion output |

`/convert/jobs` and `/convert/jobs/batch` support `overwrite` boolean.

### Sequence analysis jobs

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/sequence/settings` | Read FASTA/tool readiness and sequence gap setting |
| `POST` | `/sequence/search/jobs` | Submit exact, substitution or BLAST search |
| `POST` | `/sequence/moddotplot/jobs` | Submit ModDotPlot identity matrix generation |
| `POST` | `/sequence/dotplot/jobs` | Submit active-assembly alignment dotplot generation |
| `POST` | `/sequence/jobs/list` | List sequence jobs, logs and effective worker counts |
| `POST` | `/sequence/jobs/:jobId/stop` | Cancel a sequence job |
| `POST` | `/sequence/cleanup` | Reclaim detached superseded managed overlays |

All job requests capture immutable assembly/FASTA provenance before background
execution. Optional `outputFilename` publishes a reusable result relative to
`DATA_DIR`; it must end in `.bed` for search or `.hict.hdf5` for dotplots and may
not target `processed/sequence-analysis`. `overwrite` defaults to false.

Search accepts `forwardColor` and `reverseColor` as `#RRGGBB`. Dotplot and search
thread fields use `-1` for all allocated CPUs, `0` for adaptive auto, `1` for
single-threaded execution, or a positive requested count capped by the JVM/Slurm
allocation. Status includes `requestedThreads`, `effectiveThreads`, and
`threadMode`; execution counts are excluded from scientific overlay compatibility.

### Diagnostics

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/diagnostics/workers` | Worker-pool and queue diagnostics |

## Notes for Python client authors

- Session state is server-side and mutable; reopen/attach as needed.
- Scaffolding operations update assembly state and bump tile generations.
- `PIXELS` are visible pixels (hidden contigs excluded) at selected resolution.
- For throughput-sensitive region fetches use `GET /get_tile?format=PNG_BY_PIXELS` with persistent HTTP client reuse.


### AGP partial components and omitted sequence

`POST /load_agp` accepts exact inclusive source intervals (AGP 2.1 columns 7–8).
The importer prepares and validates the complete layout before committing it.
It does not call the interactive scissors operation: no finest-resolution contact
bin is deleted. A crossing bin belongs to the interval containing its start in the
original source contig. Sequence coordinates remain exact; matrix boundaries can
be approximate by one bin at each resolution. `importReport.warnings` describes
splits and forward treatment of unknown orientations.

When source sequence is not covered, the default `REQUIRE_CHOICE` returns
`requiresMissingComponentsChoice: true` and an `importReport` without mutating
state. Repeat the request with `missingComponentsPolicy: "APPEND_UNPLACED"`
(or `"EXCLUDE"`). This choice governs the assembly and subsequent FASTA exports.

AGP and assembly FASTA exports accept `preserveImportedGaps: true` (the WebUI
sends it). This retains N/U gap annotations at surviving oriented adjacencies,
including reversed scaffold order. New adjacencies use the scaffold gap setting.
API clients can continue to request uniform `defaultSpacerLength` by leaving
`preserveImportedGaps` false. AGP component names refer to source FASTA components,
so exported split intervals can be imported into a fresh copy of the map.

`POST /matrix/query` with `unit: "BINS"` includes hidden contigs; it no longer
projects bin coordinates into a smaller visible-pixel rectangle. Response unit
and coordinate fields/headers identify BINS (`startRowBin`, `x-hict-start-row-bin`,
and analogous columns/ends) or PIXELS (`startRowPx`, `x-hict-start-row-px`).
Oversized windows are rejected before matrix allocation.
