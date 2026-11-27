#!/usr/bin/env python3
"""Build a dense, physically independent multiresolution matrix export fixture."""

import argparse
from pathlib import Path
import subprocess

import cooler
import h5py
import numpy as np
import pandas as pd


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--jar", type=Path, required=True)
    parser.add_argument("--data-dir", type=Path, required=True)
    args = parser.parse_args()
    directory = args.data_dir.absolute()
    if directory != directory.resolve() or not directory.is_relative_to("/tmp"):
        parser.error("Use a nonsymlinked disposable directory under /tmp")
    directory.mkdir(exist_ok=True, parents=True)
    cooler_path = directory / "matrix-original.mcool"
    hict_path = directory / "export-fixture.hict.hdf5"
    if cooler_path.exists() or hict_path.exists():
        parser.error("Fixture output already exists")
    for resolution in (100, 200):
        starts = np.arange(0, 4000, resolution)
        bins = pd.DataFrame({
            "chrom": ["assembly"] * len(starts),
            "start": starts,
            "end": starts + resolution,
        })
        rows, columns = np.triu_indices(len(starts))
        pixels = pd.DataFrame({
            "bin1_id": rows,
            "bin2_id": columns,
            "count": (rows + 1) * 43 + columns + 1,
        })
        cooler.create_cooler(
            f"{cooler_path}::/resolutions/{resolution}", bins, pixels,
            ordered=True, mode="a",
        )
    with h5py.File(cooler_path, "r+") as multi:
        multi.attrs["format"] = "HDF5::MCOOL"
        multi.attrs["format-version"] = 2
    subprocess.run([
        "java", "-jar", str(args.jar.resolve()), "convert", "mcool-to-hict",
        "--input", str(cooler_path), "--output", str(hict_path),
        "--resolutions", "100,200", "--parallelism", "1",
        "--no-build-resolution-pyramid", "--no-balance-input-coolers",
    ], check=True)
    print(hict_path)


if __name__ == "__main__":
    main()
