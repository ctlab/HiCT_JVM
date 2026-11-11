#!/usr/bin/env python3
"""Benchmark HiCT POST /matrix/query against the shared manifest.

The request body follows the current numeric-query convention: unit=BINS with
startRowBin/endRowBin/startColBin/endColBin, RAW_COUNTS, BINARY_INT64.  The body
is kept in a separate JSON template because the API is explicitly marked evolving;
if a local checkout differs, edit that template rather than the timing loop.

Before running: start HiCT, open the corresponding .hict.hdf5 as the PRIMARY source,
and keep the assembly in the identity/source state.
"""

import argparse
import base64
import csv
import json
import re
import time
from pathlib import Path
import numpy as np
import requests
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

PLACEHOLDER = re.compile(r"^\$\{([A-Za-z0-9_]+)\}$")


def substitute(obj, vals):
    if isinstance(obj, dict):
        return {k: substitute(v, vals) for k, v in obj.items()}
    if isinstance(obj, list):
        return [substitute(v, vals) for v in obj]
    if isinstance(obj, str):
        m = PLACEHOLDER.match(obj)
        if m:
            v = vals[m.group(1)]
            # Keep numeric placeholders numeric in JSON.
            return int(v) if isinstance(v, (int, np.integer)) or str(v).isdigit() else v
    return obj


def decode_response(resp, rows, cols, dtype):
    if resp.headers.get("x-hict-cancelled", "").lower() == "true":
        raise RuntimeError("matrix request was cancelled")
    for key, expected in [("x-hict-rows", rows), ("x-hict-cols", cols)]:
        if key in resp.headers and int(resp.headers[key]) != expected:
            raise RuntimeError(f"unexpected {key}: {resp.headers[key]}")
    expected_dtype = np.dtype(dtype).name
    if resp.headers.get("x-hict-dtype", expected_dtype) != expected_dtype:
        raise RuntimeError("response dtype differs from requested format")
    for key, expected in [
        ("x-hict-source", "PRIMARY"),
        ("x-hict-unit", "BINS"),
        ("x-hict-signal-mode", "RAW_COUNTS"),
    ]:
        if resp.headers.get(key, expected) != expected:
            raise RuntimeError(f"unexpected response {key}")
    ctype = resp.headers.get("content-type", "").lower()
    if "application/octet-stream" in ctype or "application/x-binary" in ctype:
        arr = np.frombuffer(resp.content, dtype=dtype)
        if arr.size != rows * cols:
            raise RuntimeError(
                f"binary response has {arr.size} elements, expected {rows*cols}"
            )
        return arr.reshape(rows, cols)
    if "application/json" not in ctype:
        raise RuntimeError(f"unsupported response content-type: {ctype}")
    data = resp.json()
    if isinstance(data, dict):
        if data.get("cancelled"):
            raise RuntimeError("matrix request was cancelled")
        for key, expected in [
            ("rows", rows),
            ("cols", cols),
            ("dtype", expected_dtype),
        ]:
            if data.get(key, expected) != expected:
                raise RuntimeError(f"unexpected JSON response {key}")
    # Common JSON layouts.
    if isinstance(data, list):
        arr = np.asarray(data)
    elif isinstance(data, dict):
        for key in ("matrix", "data", "values"):
            if key in data and isinstance(data[key], list):
                arr = np.asarray(data[key])
                break
        else:
            # Base64 binary payloads.
            b64 = None
            for key in ("binary", "payload", "dataBase64", "base64"):
                if key in data and isinstance(data[key], str):
                    b64 = data[key]
                    break
            if b64 is None:
                raise RuntimeError(
                    "JSON response does not contain matrix/data/values or a recognized base64 payload"
                )
            raw = base64.b64decode(b64, validate=True)
            arr = np.frombuffer(raw, dtype=dtype)
    else:
        raise RuntimeError(f"unexpected JSON response type: {type(data)}")
    arr = np.asarray(arr)
    if arr.ndim == 1 and arr.size == rows * cols:
        arr = arr.reshape(rows, cols)
    if arr.shape != (rows, cols):
        raise RuntimeError(f"decoded shape {arr.shape}, expected {(rows,cols)}")
    return arr


def global_bin_start(cool, chrom, start_bp, res):
    lo, _ = cool.extent(chrom)
    if start_bp % res:
        raise ValueError(f"{chrom}:{start_bp} is not aligned to resolution {res}")
    return int(lo + start_bp // res)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--server", default="http://127.0.0.1:5000")
    ap.add_argument("--endpoint", default="/matrix/query")
    ap.add_argument(
        "--mcool",
        required=True,
        help="paired source .mcool, used only for exact global-bin offsets",
    )
    ap.add_argument("--manifest", required=True)
    ap.add_argument("--dataset", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument(
        "--request-template",
        default=str(Path(__file__).with_name("matrix_query_request_template.json")),
    )
    ap.add_argument("--warmups", type=nonnegative_int, default=1)
    ap.add_argument("--reps", type=positive_int, default=3)
    ap.add_argument("--dtype", choices=["int64", "float64", "float32"], default="int64")
    ap.add_argument(
        "--byte-order",
        choices=["little"],
        default="little",
        help="byte order for binary matrix payloads; default little",
    )
    ap.add_argument("--timeout", type=float, default=120.0)
    args = ap.parse_args()
    prefix = "<"
    dtype = {
        "int64": prefix + "i8",
        "float64": prefix + "f8",
        "float32": prefix + "f4",
    }[args.dtype]
    template = json.loads(Path(args.request_template).read_text())
    rows = load_manifest(args.manifest, args.dataset)
    if not np.isfinite(args.timeout) or args.timeout <= 0:
        ap.error("--timeout must be finite and positive")
    cools = {}
    with requests.Session() as sess, open_results(args.output) as f:
        w = csv.DictWriter(f, fieldnames=RESULT_FIELDS)
        w.writeheader()
        for r in rows:
            res = int(r["resolution_bp"])
            side = int(r["side_bins"])
            if res not in cools:
                cools[res] = cooler.Cooler(f"{args.mcool}::resolutions/{res}")
            c = cools[res]
            validate_window(c, r)
            row_bin = global_bin_start(c, r["chrom1"], int(r["start1_bp"]), res)
            col_bin = global_bin_start(c, r["chrom2"], int(r["start2_bp"]), res)
            vals = {
                "resolution_bp": res,
                "row_bin": row_bin,
                "row_end_bin": row_bin + side,
                "col_bin": col_bin,
                "col_end_bin": col_bin + side,
                "rows": side,
                "cols": side,
            }
            body = substitute(template, vals)
            body["format"] = {
                "int64": "BINARY_INT64",
                "float64": "BINARY_FLOAT64",
                "float32": "BINARY_FLOAT32",
            }[args.dtype]
            if (
                body.get("signalMode") != "RAW_COUNTS"
                or body.get("unit") != "BINS"
                or body.get("source", "PRIMARY") != "PRIMARY"
            ):
                raise ValueError("paired benchmark requires PRIMARY RAW_COUNTS in BINS")
            expected = {
                "bpResolution": res,
                "startRowBin": row_bin,
                "endRowBin": row_bin + side,
                "startColBin": col_bin,
                "endColBin": col_bin + side,
            }
            if any(body.get(key) != value for key, value in expected.items()):
                raise ValueError(
                    "request template does not describe the manifest rectangle"
                )

            def call():
                t0 = time.perf_counter_ns()
                resp = sess.post(
                    args.server.rstrip("/") + args.endpoint,
                    json=body,
                    timeout=args.timeout,
                )
                t1 = (
                    time.perf_counter_ns()
                )  # full response received; client decoding excluded
                if resp.status_code != 200:
                    raise RuntimeError(
                        f"HiCT HTTP {resp.status_code}: {resp.text[:1200]}"
                    )
                a = decode_response(resp, side, side, dtype)
                return a, t0, t1

            for _ in range(args.warmups):
                call()
            for rep in range(args.reps):
                a, t0, t1 = call()
                w.writerow(
                    {
                        "dataset": r["dataset"],
                        "resolution_bp": res,
                        "side_bins": side,
                        "kind": r["kind"],
                        "query_id": r["query_id"],
                        "backend": "hict_jvm_dense_raw",
                        "rep": rep,
                        "latency_ms": f"{(t1-t0)/1e6:.6f}",
                        "shape_ok": int(a.shape == (side, side)),
                        "checksum": checksum(a, args.dtype),
                        "notes": f"identity assembly; BINS; RAW_COUNTS; dense transfer; byte_order={args.byte_order};checksum_dtype={args.dtype}",
                    }
                )
                f.flush()


if __name__ == "__main__":
    main()
