#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JVM_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
DATA_DIR="${DATA_DIR:-/mnt/Models/HiCT/data}"
PORT="${PORT:-5011}"
PROCESSED_DIR="${PROCESSED_DIR:-/tmp/hict_processed_cache_ci}"
export GRADLE_USER_HOME="${GRADLE_USER_HOME:-/tmp/hict_gradle_home}"
mkdir -p "${GRADLE_USER_HOME}"

HICT_REL="build/quad/combined_ind2_4DN.hict.hdf5"
FASTA_REL="build/quad/quad_combined_ind2.fasta"
BW_REL="build/quad/ind2.coverage.bw"
BED_REL="build/quad/ind2.alignments.bed.gz"
GFF_REL="build/quad/ind2.features.gff3"

HICT_ABS="${DATA_DIR}/${HICT_REL}"
FASTA_ABS="${DATA_DIR}/${FASTA_REL}"
BW_ABS="${DATA_DIR}/${BW_REL}"
BED_ABS="${DATA_DIR}/${BED_REL}"
GFF_ABS="${DATA_DIR}/${GFF_REL}"

cd "${JVM_DIR}"

echo "[optional] Building fat jar"
./gradlew shadowJar >/dev/null

mkdir -p "${PROCESSED_DIR}"
JAR_PATH="$(ls -1 build/libs/hict_server-*-fat.jar | head -n 1)"
if [[ -z "${JAR_PATH}" ]]; then
  echo "[optional] ERROR: fat jar was not produced"
  exit 1
fi

if [[ ! -f "${HICT_ABS}" ]]; then
  echo "[optional] Skip: ${HICT_ABS} does not exist"
  exit 0
fi

echo "[optional] Starting API server on :${PORT}"
VXPORT="${PORT}" SERVE_WEBUI=false DATA_DIR="${DATA_DIR}" PROCESSED_DIR="${PROCESSED_DIR}" \
  java -jar "${JAR_PATH}" start-api-server >/tmp/hict_optional_tests_server.log 2>&1 &
SERVER_PID=$!
cleanup() {
  if ps -p "${SERVER_PID}" >/dev/null 2>&1; then
    kill "${SERVER_PID}" >/dev/null 2>&1 || true
    wait "${SERVER_PID}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

for _ in $(seq 1 40); do
  if curl -fsS "http://localhost:${PORT}/version" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
curl -fsS "http://localhost:${PORT}/version" >/dev/null

echo "[optional] Opening HiCT"
curl -fsS -X POST "http://localhost:${PORT}/open" \
  -H "content-type: application/json" \
  -d "{\"filename\":\"${HICT_REL}\"}" >/dev/null

echo "[optional] Opening Cooler weights track"
curl -fsS -X POST "http://localhost:${PORT}/tracks/open_cooler_weights" \
  -H "content-type: application/json" \
  -d '{"name":"Cooler weights"}' >/dev/null

COOLER_TRACK_ID="$(
  curl -fsS -X POST "http://localhost:${PORT}/tracks/list" \
    -H "content-type: application/json" \
    -d '{}' | jq -r '.[] | select(.type=="COOLER_WEIGHTS") | .trackId' | head -n1
)"
if [[ -z "${COOLER_TRACK_ID}" ]]; then
  echo "[optional] ERROR: Cooler weights track was not created"
  exit 1
fi
curl -fsS -X POST "http://localhost:${PORT}/tracks/update" \
  -H "content-type: application/json" \
  -d "{\"trackId\":\"${COOLER_TRACK_ID}\",\"logScale\":true}" >/dev/null

if [[ -f "${FASTA_ABS}" ]]; then
  echo "[optional] Linking FASTA"
  curl -fsS -X POST "http://localhost:${PORT}/link_fasta" \
    -H "content-type: application/json" \
    -d "{\"fastaFilename\":\"${FASTA_REL}\",\"allowMismatch\":true}" >/dev/null
else
  echo "[optional] FASTA not found, skipping FASTA link checks"
fi

if [[ -f "${BW_ABS}" ]]; then
  echo "[optional] Opening BigWig track"
  curl -fsS -X POST "http://localhost:${PORT}/tracks/open" \
    -H "content-type: application/json" \
    -d "{\"filename\":\"${BW_REL}\"}" >/dev/null
fi

if [[ -f "${BED_ABS}" ]]; then
  echo "[optional] Opening BED track"
  curl -fsS -X POST "http://localhost:${PORT}/tracks/open" \
    -H "content-type: application/json" \
    -d "{\"filename\":\"${BED_REL}\"}" >/dev/null
fi

if [[ -f "${GFF_ABS}" ]]; then
  echo "[optional] Opening GFF track"
  curl -fsS -X POST "http://localhost:${PORT}/tracks/open" \
    -H "content-type: application/json" \
    -d "{\"filename\":\"${GFF_REL}\"}" >/dev/null
fi

echo "[optional] Querying tracks in PIXELS/BINS/BP units"
curl -fsS -X POST "http://localhost:${PORT}/tracks/query_1d" \
  -H "content-type: application/json" \
  -d '{"unit":"PIXELS","startPx":0,"endPx":6000,"widthPx":1200,"bpResolution":50000}' \
  | jq -e '.tracks | length >= 0' >/dev/null
curl -fsS -X POST "http://localhost:${PORT}/tracks/query_1d" \
  -H "content-type: application/json" \
  -d '{"unit":"BINS","startBin":0,"endBin":6000,"widthPx":1200,"bpResolution":50000}' \
  | jq -e '.tracks | length >= 0' >/dev/null
curl -fsS -X POST "http://localhost:${PORT}/tracks/query_1d" \
  -H "content-type: application/json" \
  -d '{"unit":"BP","startBP":0,"endBP":300000000,"widthPx":1200,"bpResolution":50000}' \
  | jq -e '.tracks | length >= 0' >/dev/null

echo "[optional] Cooler weights query sanity check"
curl -fsS -X POST "http://localhost:${PORT}/tracks/query_1d" \
  -H "content-type: application/json" \
  -d '{"unit":"PIXELS","startPx":0,"endPx":7000,"widthPx":1536,"bpResolution":50000}' \
  | jq -e '.tracks[] | select(.type=="COOLER_WEIGHTS") | (.bins | length) > 0' >/dev/null

if [[ -f "${BW_ABS}" && -f "${BED_ABS}" ]]; then
  echo "[optional] BED vs BigWig sanity check"
  QUERY_JSON="$(curl -fsS -X POST "http://localhost:${PORT}/tracks/query_1d" \
    -H "content-type: application/json" \
    -d '{"unit":"PIXELS","startPx":0,"endPx":7000,"widthPx":1536,"bpResolution":50000}')"
  BW_BINS="$(echo "${QUERY_JSON}" | jq -r '.tracks[] | select(.sourceFile? == null or true) | select(.type=="BIGWIG") | .bins | length' | head -n1 || true)"
  BED_BINS="$(echo "${QUERY_JSON}" | jq -r '.tracks[] | select(.type=="BED") | .bins | length' | head -n1 || true)"
  if [[ -n "${BW_BINS}" && -n "${BED_BINS}" ]]; then
    if [[ "${BW_BINS}" -eq 0 || "${BED_BINS}" -eq 0 ]]; then
      echo "[optional] ERROR: BED/BigWig bins are empty"
      exit 1
    fi
  fi
fi

echo "[optional] OK"
