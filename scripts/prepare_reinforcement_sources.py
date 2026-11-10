#!/usr/bin/env python3
"""Add synthetic weighted and identity sources to freshly generated tiny HDF5 fixtures."""
import argparse
import shutil
import sys
from pathlib import Path

import h5py
import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent / "sequence_analysis"))
from overlay_store import OverlayWriter


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", required=True)
    args = parser.parse_args()
    root = Path(args.data_dir).absolute()
    if root != root.resolve() or not root.is_relative_to("/tmp"):
        raise ValueError("Use an isolated nonsymlinked data directory under /tmp")
    source = root / "tiny-primary.hict.hdf5"
    if source.is_symlink() or source.stat().st_nlink != 1:
        raise ValueError("Use a physical fixture copy")
    weighted, identity = root / "tiny-weighted.hict.hdf5", root / "tiny-identity.hict.hdf5"
    if weighted.exists() or weighted.is_symlink() or identity.exists() or identity.is_symlink():
        raise ValueError("Refusing to overwrite existing fixtures")
    with source.open("rb") as reader, weighted.open("xb") as writer:
        shutil.copyfileobj(reader, writer)
    # Explicit test metadata, not an inference applied to arbitrary user files.
    with h5py.File(weighted, "r+") as handle:
        handle.attrs["hict_source_units"] = "CONTACT_COUNT"
        handle.attrs["contact_data_state"] = "PRESENT"
        handle["resolutions/4"].attrs["hict_imported_weights"] = "PRESENT"
        handle["resolutions/4/stripes/stripes_bin_weights"][0, :5] = [2, 3, 4, 5, 6]
    with OverlayWriter(source, identity, [4], {"kind": "moddotplot", "parameters": {"identity": 80}}) as writer:
        axis = np.array([[0, i] for i in range(5)], dtype=np.int64)
        writer.patch(4, axis, axis, np.eye(5) * 90)
        writer.record_coverage(4, axis, axis, {"fixture": "synthetic diagonal identity"})
    print("Prepared synthetic weighted and identity fixtures")


if __name__ == "__main__":
    main()
