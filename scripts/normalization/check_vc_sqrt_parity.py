#!/usr/bin/env python3
"""Compare the server's square-root coverage weights with the Juicebox VC_SQRT reference on a real map.

Needs a running HiCT server whose DATA_DIR holds the map, h5py and numpy, and the Juicebox classes
described in README.md. Only maps without hidden contigs at the chosen resolution can be compared bin
for bin (use a single-chromosome or fully visible map). The check reads the stored contacts straight
from the .hict.hdf5 file, so it does not depend on the server's own reader.

  check_vc_sqrt_parity.py --api http://127.0.0.1:5000 --file arabiensis.0.hict.hdf5 --path /data/arabiensis.0.hict.hdf5 \
      --resolution 25000 --juicebox-classes out --juicebox-lib /path/to/Juicebox/lib
"""
import argparse, json, subprocess, sys, tempfile, time, urllib.request
from pathlib import Path

import h5py
import numpy as np


def post(api, path, body=None):
    request = urllib.request.Request(api.rstrip("/") + "/" + path, data=json.dumps(body or {}).encode(),
                                     headers={"content-type": "application/json"}, method="POST")
    with urllib.request.urlopen(request, timeout=900) as response:
        return json.loads(response.read())


def stored_contacts(path, resolution):
    group = h5py.File(path, "r")["resolutions/%d" % resolution]
    lengths = group["stripes/stripe_length_bins"][:]
    count = len(lengths)
    starts = np.concatenate([[0], np.cumsum(lengths)])
    block_length = group["treap_coo/block_length"][:].reshape(count, count)
    block_offset = group["treap_coo/block_offset"][:].reshape(count, count)
    rows_ds, cols_ds, vals_ds, dense = (group["treap_coo/" + n] for n in ("block_rows", "block_cols", "block_vals", "dense_blocks"))
    out = []
    for r in range(count):
        for c in range(r, count):
            n = block_length[r, c]
            if n <= 0:
                continue
            offset = block_offset[r, c]
            if offset >= 0:
                rows, cols, vals = rows_ds[offset:offset + n], cols_ds[offset:offset + n], vals_ds[offset:offset + n].astype(float)
            else:
                block = dense[-(offset + 1), 0].astype(float)
                if r == c:
                    block = np.triu(block)
                rows, cols = np.nonzero(block)
                vals = block[rows, cols]
            out.append(np.column_stack([starts[r] + rows, starts[c] + cols, vals]))
    return int(starts[-1]), np.concatenate(out)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--api", required=True)
    parser.add_argument("--file", required=True, help="name relative to the server's DATA_DIR")
    parser.add_argument("--path", required=True, help="the same file on disk")
    parser.add_argument("--resolution", type=int, required=True)
    parser.add_argument("--juicebox-classes", required=True)
    parser.add_argument("--juicebox-lib", required=True)
    parser.add_argument("--tolerance", type=float, default=1e-5)
    args = parser.parse_args()

    n, records = stored_contacts(args.path, args.resolution)
    with tempfile.TemporaryDirectory() as work:
        matrix = Path(work, "matrix.txt")
        with matrix.open("w") as handle:
            handle.write("%d\n" % n)
            np.savetxt(handle, records, fmt="%d %d %g")
        classpath = args.juicebox_classes + ":" + ":".join(str(p) for p in Path(args.juicebox_lib).rglob("*.jar"))
        run = subprocess.run(["java", "-Xmx3g", "-cp", classpath, "juicebox.tools.utils.norm.GoldenVcSqrt", str(matrix)],
                             capture_output=True, text=True, check=True)
    reference = np.array([x if x is not None else 0.0 for x in json.loads(run.stdout)["vector"]])

    post(args.api, "open", {"filename": args.file})
    post(args.api, "normalization/coverage/prepare")
    while post(args.api, "normalization/coverage/status")["state"] == "RUNNING":
        time.sleep(1)
    post(args.api, "normalization/weights", {"mode": "SQRT_COVERAGE"})
    reply = post(args.api, "matrix/query", {
        "bpResolution": args.resolution, "unit": "BINS", "startRowBin": 0, "endRowBin": n, "startColBin": 0, "endColBin": 1,
        "signalMode": "PIPELINE_SIGNAL", "format": "JSON",
        "expression": {"type": "track1d", "trackId": "__builtin_cooler_weights__", "axis": "ROW"}})
    weights = np.array(reply["values"], dtype=float)
    valid = reference > 0
    ratio = weights[valid] * reference[valid]
    worst = float(np.max(np.abs(ratio - 1)))
    print("bins %d, valid %d, contacts %d, worst |weight x divisor - 1| = %.3g" % (n, valid.sum(), len(records), worst))
    stray = int(((weights != 0) & ~valid).sum())
    print("weights present where the reference has none:", stray)
    return 0 if worst <= args.tolerance and stray == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
