# Paired dense-query benchmarks

These tools compare unbalanced, dense Cooler queries with HiCT's numeric
`POST /matrix/query` endpoint. They do not measure interactive rendering or
end-to-end file opening. The analytical scripts at the end estimate block counts
and do not produce measured performance results.

## Setup and prerequisites

Use Python 3.10 or newer and Bash on Linux or macOS.

```bash
python3 -m venv benchmarking/.venv
benchmarking/.venv/bin/python -m pip install -r benchmarking/requirements.txt
export PYTHON="$PWD/benchmarking/.venv/bin/python"
```

The input must be a fixed-bin `.mcool` with groups `/resolutions/<bp>`.
Convert that source to HiCT, open it as the **primary** map, and keep the assembly
in its original chromosome order and orientation. Do not edit the assembly or
run concurrent matrix requests during measurement. Both backends must see the
same source values. The scripts never open, convert, or edit a HiCT assembly.
Checksums detect a different assembly or source when it changes queried values;
they cannot certify the identity of unqueried regions.

Use an otherwise idle machine. Record the HiCT version/commit, JVM flags, source
file checksum, conversion settings and storage/cache conditions alongside the
results. `environment.txt` records hardware, OS, Java and Python library versions,
but cannot infer the running server's JVM configuration or source provenance.

## Run a pair

From the repository root, with HiCT already running:

```bash
PYTHON="$PYTHON" HICT_SERVER=http://127.0.0.1:5000 \
  SIDES=64,256 WINDOWS_PER_CELL=20 WARMUPS=3 REPS=10 \
  benchmarking/run_pair_benchmark.sh sample /data/sample.mcool \
  1000,10000 benchmarking/results/sample-run-01
```

The output directory must **not exist**. A failed run retains its diagnostics;
choose a new directory for a repeat. Individual scripts also refuse to overwrite
or append to existing output files. Defaults are sides `64,256,1024,2048`,
20 windows per resolution/side, 3 warmups per query, 10 measured repetitions,
seed `20260915`, and `DTYPE=int64`. Choose smaller sides/resolutions when no
chromosome is at least four window spans long. Query generation fails explicitly
instead of silently changing the workload. Chromosomes are sampled uniformly
from eligible chromosomes; windows are not sampled uniformly across the genome.
Half of the windows (rounded down) are diagonal, the rest non-overlapping
same-chromosome off-diagonal windows. Sampling may repeat a window.

The runner writes a deterministic `manifest.csv`, `cooler.csv`, `hict.csv`,
`environment.txt` and one validated `summary-<side>.tex` per side. The table
requires LaTeX's `booktabs` package. It pools diagonal and off-diagonal samples
within each dataset/resolution/side; it is not a per-query speedup statistic.

For floating-point counts, set `DTYPE=float64`. `float32` is also available for
an explicitly lower-precision transfer comparison. The chosen dtype controls
HiCT's response format **and both checksums**. Integer mode rejects fractional
counts and int64 overflow. Checksums cover shape and canonical little-endian
values with SHA-256 and normalize signed floating zero. Floating comparisons are
exact after conversion to the selected dtype, without a tolerance. A HiCT import
that changed source precision will therefore fail validation unless the selected
comparison dtype represents both matrices identically. A matching float32
checksum does not prove equality at higher precision.

A table is emitted only if every selected manifest query has both backends,
exactly the requested repetitions, valid shapes, finite positive latencies,
and one consistent checksum across both backends and all repetitions. Mismatches,
missing queries, duplicate samples and incomplete runs exit nonzero. To summarize
several datasets, repeat `--manifest`:

```bash
"$PYTHON" benchmarking/summarize_benchmarks.py \
  benchmarking/results/sample-run-01/cooler.csv \
  benchmarking/results/sample-run-01/hict.csv \
  --manifest benchmarking/results/sample-run-01/manifest.csv \
  --reps 10 --side 256 --output benchmarking/results/table-256.tex
```

## Measurement boundaries

Cooler timing includes `matrix(balance=False, sparse=False).fetch(region1,
region2)` and its dense NumPy materialization. HiCT timing includes JSON request
encoding, HTTP round-trip, server extraction/encoding, and receipt of the entire
binary response. Client decoding and checksumming are outside both timers.
This is a local-library versus HTTP-service comparison, with different transport
and serialization costs. The default endpoint uses little-endian binary values;
other byte orders are rejected.

The Cooler suite runs before the HiCT suite. Warmups and OS caches affect timings;
no cache eviction is performed and no cold-cache claim is justified. Warmups are
per query, not process initialization. `WARMUPS=0` disables them and must be
reported as such. Use independent fresh output directories and alternate backend
order manually with the individual scripts when evaluating order effects. The
individual runners accept the same manifest, dataset, dtype, warmups and reps.
Large dense matrices can consume significant client and server memory.

`discover_matrix_query.py --server URL` reads the running server's OpenAPI spec,
starting with HiCT's `/api/v1/openapi.yaml`. It exits nonzero if no matrix schema
is found. The request template fixes `BINS`, `RAW_COUNTS` and `PRIMARY`; changing
its rectangle or signal semantics is rejected by the paired client.

## Analytical estimates and regression checks

```bash
"$PYTHON" benchmarking/sensitivity_curves.py --contig-bins 1 --max-m 4096
"$PYTHON" benchmarking/short_contig_stress.py \
  --lengths /data/contigs.tsv --resolution 1000 --stripe-width 256
"$PYTHON" -m unittest discover -s benchmarking -p 'test_*.py' -v
```

Lengths must be positive integers, either one per line or in a CSV/TSV
`length_bp` column. Blank lines and `#` comments are allowed. Invalid rows and
out-of-range selections are rejected. Both analytical models round each contig
up to full bins. They assume consecutive source ranges on a fixed grid, with no
assembly gaps or visibility filtering; they estimate possible stripe/block
pairs, not actual HDF5 I/O, cache misses, or runtime. Sensitivity output includes
separate contig-aligned and fixed-grid columns for each width. The short-contig
script uses the exact worst integer-bin alignment bound; sensitivity curves use
the looser conservative bound stated in their docstring.

The regression suite uses a real temporary Cooler file and a deterministic
localhost HTTP fixture for integer and floating response formats. It checks
source chromosome offsets, paired checksums, rejection paths and rerun safety.
It does not start a real HiCT server or certify performance on a production dataset.
