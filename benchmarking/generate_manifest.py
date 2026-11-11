#!/usr/bin/env python3
"""Generate deterministic same-chromosome benchmark windows from an .mcool file.

This is intended to create one shared query manifest for both the Cooler and HiCT
backends. Coordinates are aligned to the selected bin size, so dense query shapes
should match side_bins x side_bins.
"""

import argparse
import csv
import random
import cooler
from benchmark_common import MANIFEST_FIELDS, positive_int


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mcool", required=True)
    ap.add_argument("--dataset", required=True)
    ap.add_argument(
        "--resolutions", required=True, help="comma-separated bp resolutions"
    )
    ap.add_argument(
        "--sides", default="256,1024,2048", help="comma-separated bin side lengths"
    )
    ap.add_argument("--windows-per-cell", type=positive_int, default=10)
    ap.add_argument("--seed", type=int, default=20260915)
    ap.add_argument("--output", required=True)
    args = ap.parse_args()

    rng = random.Random(args.seed)
    resolutions = [positive_int(x) for x in args.resolutions.split(",")]
    sides = [positive_int(x) for x in args.sides.split(",")]
    if len(set(resolutions)) != len(resolutions) or len(set(sides)) != len(sides):
        ap.error("resolutions and sides must not contain duplicates")
    fields = MANIFEST_FIELDS

    rows = []
    for res in resolutions:
        c = cooler.Cooler(f"{args.mcool}::resolutions/{res}")
        if c.binsize != res:
            raise ValueError(
                f"Cooler binsize does not match requested resolution {res}"
            )
        chromsizes = {str(k): int(v) for k, v in c.chromsizes.items()}
        for side in sides:
            span = side * res
            eligible = [(ch, L) for ch, L in chromsizes.items() if L >= 4 * span]
            if not eligible:
                raise RuntimeError(
                    f"No chromosome long enough for res={res}, side={side}"
                )
            half = args.windows_per_cell // 2
            for qid in range(args.windows_per_cell):
                kind = "diagonal" if qid < half else "offdiagonal"
                ch, L = rng.choice(eligible)
                max_bin_start = (L - span) // res
                b1 = rng.randint(0, max_bin_start)
                if kind == "diagonal":
                    b2 = b1
                else:
                    # Keep the second window on the same chromosome but away from the first.
                    candidates = []
                    if b1 - side >= 0:
                        candidates.append((0, b1 - side))
                    if b1 + side <= max_bin_start:
                        candidates.append((b1 + side, max_bin_start))
                    # L >= 4*span guarantees a non-overlapping window.
                    lo, hi = rng.choice(candidates)
                    b2 = rng.randint(lo, hi)
                s1 = b1 * res
                s2 = b2 * res
                rows.append(
                    {
                        "dataset": args.dataset,
                        "resolution_bp": res,
                        "side_bins": side,
                        "kind": kind,
                        "query_id": qid,
                        "chrom1": ch,
                        "start1_bp": s1,
                        "end1_bp": s1 + span,
                        "chrom2": ch,
                        "start2_bp": s2,
                        "end2_bp": s2 + span,
                    }
                )

    with open(args.output, "x", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)


if __name__ == "__main__":
    main()
