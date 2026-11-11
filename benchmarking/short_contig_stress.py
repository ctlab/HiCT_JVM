#!/usr/bin/env python3
"""Estimate block counts for consecutive contigs; this is not a timing benchmark.

Input has a length_bp column in CSV/TSV, or one bare positive integer per row.
Each contig contributes ceil(length_bp/resolution) bins to this analytical model.
"""

import argparse
import csv
from pathlib import Path

from benchmark_common import nonnegative_int, positive_int


def ceil_div(value, divisor):
    return (value + divisor - 1) // divisor


def read_lengths(path):
    lines = [
        line
        for line in Path(path).read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]
    if not lines:
        raise ValueError("no lengths parsed")
    delimiter = "\t" if "\t" in lines[0] else ","
    reader = csv.DictReader(lines, delimiter=delimiter)
    if "length_bp" in (reader.fieldnames or []):
        return [positive_int(row["length_bp"]) for row in reader]
    # Do not guess which column of a malformed table is a length.
    return [positive_int(line.strip()) for line in lines]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lengths", required=True)
    parser.add_argument("--resolution", type=positive_int, required=True)
    parser.add_argument("--stripe-width", type=positive_int, default=256)
    parser.add_argument("--start-index", type=nonnegative_int, default=0)
    parser.add_argument(
        "--count", type=nonnegative_int, default=0, help="0 = all remaining contigs"
    )
    args = parser.parse_args()
    try:
        lengths = read_lengths(args.lengths)
    except (ValueError, TypeError) as error:
        parser.error(f"invalid length input: {error}")
    end = len(lengths) if args.count == 0 else args.start_index + args.count
    if args.start_index >= len(lengths) or end > len(lengths):
        parser.error("requested contig range exceeds the input")
    selected = lengths[args.start_index : end]
    bins = [ceil_div(length, args.resolution) for length in selected]
    stripes = sum(ceil_div(length, args.stripe_width) for length in bins)
    span = sum(bins)
    aligned = ceil_div(span, args.stripe_width)
    # For an integer-bin grid, the most adverse starting offset is W-1.
    worst = ceil_div(span + args.stripe_width - 1, args.stripe_width)
    print(f"components_m={len(selected)}")
    print(f"span_bins_L={span}")
    print(f"legacy_stripes={stripes}")
    print(f"legacy_block_pairs={stripes**2}")
    print(f"fixed_stripes_if_grid_aligned={aligned}")
    print(f"fixed_stripes_worst_alignment_bound={worst}")
    print(f"fixed_block_pairs_worst_bound={worst**2}")
    print(f"legacy/fixed_worst_pair_ratio={stripes**2 / worst**2:.3f}")


if __name__ == "__main__":
    main()
