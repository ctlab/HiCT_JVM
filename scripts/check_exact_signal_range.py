#!/usr/bin/env python3
"""Compare blockwise exact extrema with whole-matrix float64 queries on quad data.

Use a dedicated test server with DATA_DIR pointing at temporary input symlinks.
This script changes only that server's in-memory session; artifacts go to --output.
"""
import argparse
import json
import time
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import numpy as np


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--api", default="http://127.0.0.1:5106")
    parser.add_argument("--output", default="/tmp/hict-review-20261018/exact-ranges.json")
    args = parser.parse_args()

    def post(path, body):
        request = Request(args.api + path, data=json.dumps(body).encode(),
                          headers={"Content-Type": "application/json"})
        with urlopen(request, timeout=120) as response:
            return json.load(response)

    post("/open", {"filename": "combined_ind2_4DN.hict.hdf5"})
    post("/secondary/open", {"filename": "ind2.self.hict.hdf5", "allowMismatch": True})
    primary = {"type": "source", "source": "PRIMARY"}
    cases = [(1000000, primary),
             (1000000, {"type": "source", "source": "SECONDARY"}),
             (250000, {"type": "binary", "op": "MUL", "left": primary,
                       "right": {"type": "constant", "value": 2}}),
             (1000000, {"type": "constant", "value": 16777217})]
    results = []
    for resolution, expression in cases:
        start = time.monotonic()
        snapshot = post("/matrix/statistics", {"bpResolution": resolution, "metadataOnly": True})
        size, revision = snapshot["size"], snapshot["revision"]
        count, minimum, maximum, blocks = 0, float("inf"), float("-inf"), 0
        for row in range(0, size, 512):
            for col in range(0, size, 512):
                block = post("/matrix/statistics", {"bpResolution": resolution, "expression": expression,
                    "revision": revision, "startRowPx": row, "startColPx": col})
                assert block["revision"] == revision and block["size"] == size
                assert block["rows"] == min(512, size-row) and block["cols"] == min(512, size-col)
                count += block["count"]
                if block["count"]:
                    minimum = min(minimum, block["min"])
                    maximum = max(maximum, block["max"])
                blocks += 1
        duration = time.monotonic() - start
        full = post("/matrix/query", {"bpResolution": resolution, "unit": "PIXELS",
                    "startRowPx": 0, "startColPx": 0, "endRowPx": size, "endColPx": size,
                    "format": "JSON", "signalMode": "PIPELINE_SIGNAL", "expression": expression})
        values = np.asarray(full["values"], dtype=np.float64)
        values = values[np.isfinite(values)]
        assert count == values.size == size * size
        assert (minimum, maximum) == (values.min(), values.max()), (minimum, maximum, values.min(), values.max())
        results.append(dict(resolution=resolution, expression=expression, size=size, blocks=blocks,
                            count=count, min=minimum, max=maximum, seconds=duration))
        print(json.dumps(results[-1]), flush=True)

    config = post("/render_pipeline/get", {})
    post("/render_pipeline/set", config)
    try:
        post("/matrix/statistics", {"bpResolution": resolution, "revision": revision, "expression": primary})
    except HTTPError as error:
        assert error.code == 409, error.read()
    else:
        raise AssertionError("Stale snapshot was accepted after rendering invalidation")
    results.append({"test": "stale revision rejected", "status": 409})
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(results, indent=2) + "\n")


if __name__ == "__main__":
    main()
