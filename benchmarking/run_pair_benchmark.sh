#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  run_pair_benchmark.sh DATASET MCOOL RESOLUTIONS [OUTDIR]

Example:
  ./benchmarking/run_pair_benchmark.sh Anopheles /data/anopheles.mcool 1000,10000,100000 results/anopheles

Environment overrides:
  HICT_SERVER=http://127.0.0.1:5000
  PYTHON=python3
  SIDES=64,256,1024,2048
  WINDOWS_PER_CELL=20
  WARMUPS=3
  REPS=10
  DTYPE=int64
  BYTE_ORDER=little

Before running, open the matching .hict.hdf5 as the PRIMARY source in HiCT
and keep the assembly in the identity/source state.
USAGE
}

if [[ $# -lt 3 || $# -gt 4 ]]; then usage >&2; exit 2; fi
DATASET="$1"
MCOOL="$2"
RESOLUTIONS="$3"
OUTDIR="${4:-results/${DATASET}}"
SERVER="${HICT_SERVER:-http://127.0.0.1:5000}"
SIDES="${SIDES:-64,256,1024,2048}"
WINDOWS="${WINDOWS_PER_CELL:-20}"
WARMUPS="${WARMUPS:-3}"
REPS="${REPS:-10}"
BYTE_ORDER="${BYTE_ORDER:-little}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON="${PYTHON:-python3}"
DTYPE="${DTYPE:-int64}"
# Reserve a fresh directory before writing any provenance or measurements.
mkdir -p "$(dirname "${OUTDIR}")"
mkdir "${OUTDIR}"
PYTHON="${PYTHON}" "${SCRIPT_DIR}/capture_environment.sh" "${OUTDIR}/environment.txt"
MANIFEST="${OUTDIR}/manifest.csv"
COOLER_CSV="${OUTDIR}/cooler.csv"
HICT_CSV="${OUTDIR}/hict.csv"

"${PYTHON}" "${SCRIPT_DIR}/generate_manifest.py" \
  --mcool "${MCOOL}" --dataset "${DATASET}" \
  --resolutions "${RESOLUTIONS}" --sides "${SIDES}" \
  --windows-per-cell "${WINDOWS}" --output "${MANIFEST}"

"${PYTHON}" "${SCRIPT_DIR}/benchmark_cooler.py" \
  --mcool "${MCOOL}" --manifest "${MANIFEST}" --dataset "${DATASET}" \
  --output "${COOLER_CSV}" --warmups "${WARMUPS}" --reps "${REPS}" --dtype "${DTYPE}"

"${PYTHON}" "${SCRIPT_DIR}/benchmark_hict_http.py" \
  --server "${SERVER}" --mcool "${MCOOL}" --manifest "${MANIFEST}" \
  --dataset "${DATASET}" --output "${HICT_CSV}" \
  --warmups "${WARMUPS}" --reps "${REPS}" --dtype "${DTYPE}" --byte-order "${BYTE_ORDER}"

IFS=',' read -r -a SIDE_VALUES <<< "${SIDES}"
for side in "${SIDE_VALUES[@]}"; do
  "${PYTHON}" "${SCRIPT_DIR}/summarize_benchmarks.py" "${COOLER_CSV}" "${HICT_CSV}" \
    --manifest "${MANIFEST}" --reps "${REPS}" --side "${side}" --output "${OUTDIR}/summary-${side}.tex"
done

printf 'Validated %s\n  manifest: %s\n  Cooler:   %s\n  HiCT:     %s\n' \
  "${DATASET}" "${MANIFEST}" "${COOLER_CSV}" "${HICT_CSV}"
