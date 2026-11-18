#!/usr/bin/env python3
"""Independently compare deposited Cooler counts and diagonal support with an open HiCT map."""

import argparse
import json
from pathlib import Path
from urllib.request import Request, urlopen
import cooler
import h5py
import numpy as np

parser = argparse.ArgumentParser(description=__doc__)
for key in ("source", "api", "output"):
    parser.add_argument("--" + key, required=True)
args = parser.parse_args()
source, output = Path(args.source).resolve(), Path(args.output).absolute()
assert (
    source.is_relative_to("/tmp")
    and output == output.resolve()
    and output.is_relative_to("/tmp")
)
report = {"source": source.name, "resolutions": {}, "windows": []}
with h5py.File(source, "r") as handle:
    for resolution in (50, 100, 500):
        group = handle[f"resolutions/{resolution}"]
        chrom = group["bins/chrom"][:]
        pixels = group["pixels"]
        sums, nonzero = np.zeros(11, dtype=np.int64), np.zeros(11, dtype=np.int64)
        for begin in range(0, len(pixels["count"]), 131072):
            end = begin + 131072
            row, col, counts = (
                pixels[k][begin:end] for k in ("bin1_id", "bin2_id", "count")
            )
            distance = col - row
            cis = chrom[row] == chrom[col]
            for offset in range(11):
                values = counts[cis & (distance == offset)]
                sums[offset] += values.sum(dtype=np.int64)
                nonzero[offset] += np.count_nonzero(values)
        report["resolutions"][str(resolution)] = {
            "sourceCisDiagonalOffsets": [
                {
                    "offsetBins": k,
                    "nominalDistanceBp": k * resolution,
                    "contacts": int(sums[k]),
                    "nonzeroPixels": int(nonzero[k]),
                }
                for k in range(11)
            ]
        }
        matrix = cooler.Cooler(f"{source}::/resolutions/{resolution}")
        for chromosome in (
            matrix.chromnames[0],
            matrix.chromnames[5],
            matrix.chromnames[-1],
        ):
            lo, hi = matrix.extent(chromosome)
            for position in sorted({lo, max(lo, lo + (hi - lo) // 2 - 100)}):
                end = min(hi, position + 200)
                expected = matrix.matrix(balance=False)[position:end, position:end]
                payload = {
                    "bpResolution": resolution,
                    "unit": "PIXELS",
                    "startRowPx": int(position),
                    "endRowPx": int(end),
                    "startColPx": int(position),
                    "endColPx": int(end),
                    "signalMode": "RAW_COUNTS",
                    "format": "JSON",
                }
                request = Request(
                    args.api + "/matrix/query",
                    data=json.dumps(payload).encode(),
                    headers={"Content-Type": "application/json"},
                )
                with urlopen(request, timeout=60) as response:
                    actual = json.load(response)
                got = np.asarray(actual["values"]).reshape(expected.shape)
                assert np.array_equal(got, expected), (
                    resolution,
                    chromosome,
                    position,
                    int(np.count_nonzero(got != expected)),
                )
                report["windows"].append(
                    {
                        "resolution": resolution,
                        "chromosome": chromosome,
                        "startBin": int(position),
                        "bins": int(end - position),
                        "checkedCells": int(expected.size),
                        "exact": True,
                        "diagonalContacts": int(np.trace(expected)),
                    }
                )
output.write_text(json.dumps(report, indent=2))
print(
    "Exact source parity:",
    len(report["windows"]),
    "windows,",
    sum(w["checkedCells"] for w in report["windows"]),
    "cells",
)
for resolution, details in report["resolutions"].items():
    print(resolution, details["sourceCisDiagonalOffsets"])
