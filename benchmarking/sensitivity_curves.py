#!/usr/bin/env python3
"""Print analytical block-count bounds, not measured performance.

For m contigs with B bins each and stripe width W, contig-aligned storage
uses m*ceil(B/W) stripes. A fixed grid touches at most (ceil(m*B/W)+1)^2
block pairs. The fixed-grid expression is a conservative alignment bound.
"""

import argparse

from benchmark_common import positive_int
from short_contig_stress import ceil_div


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--widths", default="64,256,1024")
    parser.add_argument("--contig-bins", type=positive_int, default=1)
    parser.add_argument("--max-m", type=positive_int, default=4096)
    args = parser.parse_args()
    try:
        widths = [positive_int(value) for value in args.widths.split(",")]
    except ValueError as error:
        parser.error(str(error))
    print(
        "m\tL_bins\t"
        + "\t".join(f"contig_aligned_W={w}\tfixed_bound_W={w}" for w in widths)
    )
    m = 1
    while m <= args.max_m:
        span = m * args.contig_bins
        row = [m, span]
        for width in widths:
            row.extend(
                [
                    (m * ceil_div(args.contig_bins, width)) ** 2,
                    (ceil_div(span, width) + 1) ** 2,
                ]
            )
        print("\t".join(str(value) for value in row))
        m *= 4


if __name__ == "__main__":
    main()
