#!/usr/bin/env python3
"""Build a standards-compatible Cooler seed from the tiny independent fixture."""

import sys
from pathlib import Path

import cooler
import h5py
import pandas as pd

data = Path(sys.argv[1]).resolve()
assert data.is_relative_to("/tmp")
target = data / "valid-example.mcool"
assert not target.exists()
with h5py.File(data / "tiny-primary.mcool", "r") as source:
    names = source["chroms/name"].asstr()[:]
    for index, resolution in enumerate((4, 10)):
        group = source[f"resolutions/{resolution}"]
        bins = pd.DataFrame(
            {
                "chrom": [names[i] for i in group["bins/chrom"][:]],
                "start": group["bins/start"][:],
                "end": group["bins/end"][:],
            }
        )
        pixels = pd.DataFrame(
            {key: group[f"pixels/{key}"][:] for key in ("bin1_id", "bin2_id", "count")}
        )
        cooler.create_cooler(
            f"{target}::/resolutions/{resolution}",
            bins,
            pixels,
            ordered=True,
            mode="w" if index == 0 else "a",
        )
with h5py.File(target, "r+") as output:
    output.attrs["format"] = "HDF5::MCOOL"
    output.attrs["format-version"] = 2
