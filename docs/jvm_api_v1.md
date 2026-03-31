# HiCT_JVM API Reference for Python client (`hict_jvm_api`)

This document describes the endpoint subset used by the `jvm-api-v1` Python library.
All endpoints are served by HiCT_JVM API server (`start-api-server` or `start-server`).

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
| `POST` | `/tiles/reload` | Invalidate tile caches and bump versions |
| `POST` | `/get_visualization_options` | Get current visualization options |
| `POST` | `/set_visualization_options` | Set visualization options |
| `POST` | `/render_pipeline/get` | Get custom rendering pipeline graph |
| `POST` | `/render_pipeline/set` | Set custom rendering pipeline graph |
| `POST` | `/render_pipeline/reset` | Reset pipeline to defaults |

`/get_tile` supported formats:

- `JSON_PNG_WITH_RANGES` (JSON with base64 PNG + ranges)
- `PNG` (raw PNG bytes)
- `PNG_BY_PIXELS` (raw PNG bytes for arbitrary pixel-space region)

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
| `GET` | `/convert/download/:jobId` | Download completed conversion output |

`/convert/jobs` and `/convert/jobs/batch` support `overwrite` boolean.

### Diagnostics

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/diagnostics/workers` | Worker-pool and queue diagnostics |

## Notes for Python client authors

- Session state is server-side and mutable; reopen/attach as needed.
- Scaffolding operations update assembly state and bump tile generations.
- `PIXELS` are visible pixels (hidden contigs excluded) at selected resolution.
- For throughput-sensitive region fetches use `GET /get_tile?format=PNG_BY_PIXELS` with persistent HTTP client reuse.
