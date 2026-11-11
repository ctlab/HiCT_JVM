#!/usr/bin/env bash
set -euo pipefail
set -o noclobber
PYTHON="${PYTHON:-python3}"
OUT="${1:-benchmark_environment.txt}"
{
  echo "timestamp_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "hostname=$(hostname 2>/dev/null || true)"
  echo "kernel=$(uname -a 2>/dev/null || true)"
  echo "cpu:"; (lscpu 2>/dev/null || cat /proc/cpuinfo 2>/dev/null | head -80 || true)
  echo "memory:"; (free -h 2>/dev/null || true)
  echo "java:"; (java -version 2>&1 || true)
  echo "python:"; ("${PYTHON}" --version 2>&1 || true)
  echo "cooler:"; ("${PYTHON}" - <<'PY' 2>/dev/null || true
try:
 import cooler
 print(cooler.__version__)
except Exception as e:
 print(e)
PY
  )
  echo "numpy:"; ("${PYTHON}" - <<'PY' 2>/dev/null || true
try:
 import numpy
 print(numpy.__version__)
except Exception as e:
 print(e)
PY
  )
  echo "storage:"; (lsblk -o NAME,MODEL,TYPE,SIZE,ROTA,MOUNTPOINTS 2>/dev/null || true)
} > "${OUT}"
echo "wrote ${OUT}"
