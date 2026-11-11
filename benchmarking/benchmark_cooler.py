#!/usr/bin/env python3
"""Benchmark dense raw Cooler range queries from a shared CSV manifest.

Example:
  python benchmark_cooler.py --mcool /data/sample.mcool \
      --manifest benchmark_manifest.csv --dataset GSE286495 \
      --output cooler_results.csv --warmups 5 --reps 30

The manifest must use genomic regions that exist in the chosen file.
"""

import argparse
import csv
import time

import cooler
from benchmark_common import (
    RESULT_FIELDS,
    checksum,
    load_manifest,
    nonnegative_int,
    open_results,
    positive_int,
    validate_window,
)


def region(chrom: str, start: str, end: str):
    return (chrom, int(start), int(end))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mcool", required=True)
    ap.add_argument("--manifest", required=True)
    ap.add_argument("--dataset", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--warmups", type=nonnegative_int, default=1)
    ap.add_argument("--reps", type=positive_int, default=3)
    ap.add_argument("--dtype", choices=["int64", "float64", "float32"], default="int64")
    args = ap.parse_args()

    rows = load_manifest(args.manifest, args.dataset)

    handles = {}
    selectors = {}
    for res in sorted({int(r["resolution_bp"]) for r in rows}):
        uri = f"{args.mcool}::resolutions/{res}"
        c = cooler.Cooler(uri)
        handles[res] = c
        selectors[res] = c.matrix(balance=False, sparse=False)

    for row in rows:
        validate_window(handles[int(row["resolution_bp"])], row)
    with open_results(args.output) as fout:
        w = csv.DictWriter(fout, fieldnames=RESULT_FIELDS)
        w.writeheader()

        for r in rows:
            res = int(r["resolution_bp"])
            q1 = region(r["chrom1"], r["start1_bp"], r["end1_bp"])
            q2 = region(r["chrom2"], r["start2_bp"], r["end2_bp"])
            sel = selectors[res]

            for _ in range(args.warmups):
                _ = sel.fetch(q1, q2)

            expected_side = int(r["side_bins"])
            for rep in range(args.reps):
                t0 = time.perf_counter_ns()
                a = sel.fetch(q1, q2)
                t1 = time.perf_counter_ns()
                shape_ok = a.shape == (expected_side, expected_side)
                if not shape_ok:
                    raise ValueError(f"unexpected Cooler shape: {a.shape}")
                w.writerow(
                    {
                        "dataset": r["dataset"],
                        "resolution_bp": res,
                        "side_bins": expected_side,
                        "kind": r["kind"],
                        "query_id": r["query_id"],
                        "backend": "cooler_dense_raw",
                        "rep": rep,
                        "latency_ms": f"{(t1-t0)/1e6:.6f}",
                        "shape_ok": int(shape_ok),
                        "checksum": checksum(a, args.dtype),
                        "notes": f"balance=False;sparse=False;checksum_dtype={args.dtype}",
                    }
                )
                fout.flush()


if __name__ == "__main__":
    main()
