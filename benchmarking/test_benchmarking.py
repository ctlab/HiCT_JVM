#!/usr/bin/env python3
"""Regression checks with a real Cooler file and a deterministic HTTP fixture."""

import csv
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import cooler
import numpy as np
import pandas as pd
import requests

from benchmark_common import checksum, load_manifest
from benchmark_hict_http import decode_response
from short_contig_stress import read_lengths
from summarize_benchmarks import validate

ROOT = Path(__file__).resolve().parent


class BenchmarkTests(unittest.TestCase):
    def test_checksum_preserves_values_and_byte_order(self):
        integer = np.array([[1, 2], [3, 4]], dtype=">i8")
        self.assertEqual(checksum(integer), checksum(integer.astype("<i8")))
        with self.assertRaises(ValueError):
            checksum(np.array([[1.1]]))
        with self.assertRaises(ValueError):
            checksum(np.array([[2**63]], dtype="uint64"))
        with self.assertRaises(ValueError):
            checksum(np.array([[float("nan")]]), "float64")
        self.assertNotEqual(
            checksum(np.array([[1.1]]), "float64"),
            checksum(np.array([[1.9]]), "float64"),
        )
        # Decoded binary arrays are read-only; checksumming must not mutate them.
        raw = np.frombuffer(np.array([-0.0, 2.0], dtype="<f8").tobytes(), dtype="<f8")
        self.assertEqual(
            checksum(raw, "float64"), checksum(np.array([0.0, 2.0]), "float64")
        )

    def test_decoder_rejects_wrong_metadata_and_cancelled_requests(self):
        response = requests.Response()
        response.status_code = 200
        response._content = np.arange(4, dtype="<i8").tobytes()
        response.headers.update(
            {"content-type": "application/octet-stream", "x-hict-dtype": "int64"}
        )
        np.testing.assert_array_equal(
            decode_response(response, 2, 2, "<i8"), [[0, 1], [2, 3]]
        )
        response.headers["x-hict-dtype"] = "float64"
        with self.assertRaises(RuntimeError):
            decode_response(response, 2, 2, "<i8")
        response.headers["x-hict-cancelled"] = "true"
        with self.assertRaises(RuntimeError):
            decode_response(response, 2, 2, "<i8")

    def test_summary_refuses_invalid_pairings(self):
        base = dict(
            dataset="fixture",
            resolution_bp="100",
            side_bins="2",
            kind="diagonal",
            query_id="0",
            rep="0",
            latency_ms="1.0",
            shape_ok="1",
            checksum="same",
        )
        paired = [
            dict(base, backend=backend)
            for backend in ["hict_jvm_dense_raw", "cooler_dense_raw"]
        ]
        self.assertTrue(validate(paired, 2))
        manifest = [dict(base, side_bins="2")]
        self.assertTrue(validate(paired, 2, manifest, 1))
        with self.assertRaises(ValueError):
            validate(paired, 2, manifest + [dict(base, query_id="missing")], 1)
        with self.assertRaises(ValueError):
            validate(paired, 2, manifest, 2)
        mutations = [
            ("checksum", "different"),
            ("shape_ok", "0"),
            ("rep", "1"),
            ("latency_ms", "nan"),
            ("latency_ms", "-1"),
            ("backend", "other"),
        ]
        for field, value in mutations:
            with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                validate([paired[0], dict(paired[1], **{field: value})], 2)
        with self.assertRaises(ValueError):
            validate(paired[:1], 2)
        with self.assertRaises(ValueError):
            validate(paired + paired, 2)
        with self.assertRaises(ValueError):
            validate([], 2)

    def test_length_input_is_strict(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "lengths.tsv"
            path.write_text("name\tlength_bp\nchr1\t100\nchr2\t200\n")
            self.assertEqual(read_lengths(path), [100, 200])
            for text in [
                "name\tother\nchr1\t100\n",
                "length_bp\n-1\n",
                "100\ninvalid\n",
            ]:
                path.write_text(text)
                with self.assertRaises(ValueError):
                    read_lengths(path)
            path.write_text("100\n200\n")
            result = subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "short_contig_stress.py"),
                    "--lengths",
                    str(path),
                    "--resolution",
                    "100",
                    "--stripe-width",
                    "1",
                ],
                capture_output=True,
                text=True,
                check=True,
            )
            self.assertIn("fixed_stripes_worst_alignment_bound=3", result.stdout)

    def test_sensitivity_uses_contig_stripes_for_each_width(self):
        result = subprocess.run(
            [
                sys.executable,
                str(ROOT / "sensitivity_curves.py"),
                "--widths",
                "64,128",
                "--contig-bins",
                "65",
                "--max-m",
                "1",
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        self.assertEqual(result.stdout.splitlines()[1], "1\t65\t4\t9\t1\t4")

    def test_manifest_rejects_malformed_and_duplicate_windows(self):
        from benchmark_common import MANIFEST_FIELDS

        base = dict(
            dataset="fixture",
            resolution_bp="100",
            side_bins="2",
            kind="diagonal",
            query_id="0",
            chrom1="chrA",
            start1_bp="0",
            end1_bp="200",
            chrom2="chrA",
            start2_bp="0",
            end2_bp="200",
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "manifest.csv"
            for rows in [
                [base, base],
                [dict(base, end1_bp="300")],
                [dict(base, start1_bp="1", end1_bp="201")],
                [dict(base, resolution_bp="0")],
                [dict(base, kind="offdiagonal")],
            ]:
                with path.open("w", newline="") as stream:
                    writer = csv.DictWriter(stream, fieldnames=MANIFEST_FIELDS)
                    writer.writeheader()
                    writer.writerows(rows)
                with self.assertRaises(ValueError):
                    load_manifest(path, "fixture")

    def test_pair_runner_with_real_cooler_and_http_fixture(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            source = directory / "input.mcool"
            bins = cooler.binnify(pd.Series([10000, 8000], index=["chrZ", "chrA"]), 100)
            count = len(bins)
            i, j = np.triu_indices(count)
            counts = i * count + j + 1
            cooler.create_cooler(
                f"{source}::resolutions/100",
                bins,
                pd.DataFrame({"bin1_id": i, "bin2_id": j, "count": counts}),
            )
            matrix = np.zeros((count, count), dtype=np.int64)
            matrix[i, j] = counts
            matrix[j, i] = counts
            bodies = []

            class Handler(BaseHTTPRequestHandler):
                def log_message(self, *args):
                    pass

                def do_GET(self):
                    if self.path != "/api/v1/openapi.yaml":
                        self.send_error(404)
                        return
                    self.send_response(200)
                    self.send_header("content-type", "application/yaml")
                    self.end_headers()
                    self.wfile.write(
                        b"openapi: 3.0.3\npaths:\n  /matrix/query:\n    post:\n      operationId: queryMatrix\n"
                    )

                def do_POST(self):
                    body = json.loads(
                        self.rfile.read(int(self.headers["Content-Length"]))
                    )
                    bodies.append(body)
                    self.server.test.assertEqual(body["unit"], "BINS")
                    self.server.test.assertEqual(body["signalMode"], "RAW_COUNTS")
                    self.server.test.assertEqual(body["bpResolution"], 100)
                    formats = {"BINARY_INT64": "<i8", "BINARY_FLOAT64": "<f8"}
                    array = matrix[
                        body["startRowBin"] : body["endRowBin"],
                        body["startColBin"] : body["endColBin"],
                    ].astype(formats[body["format"]])
                    self.send_response(200)
                    self.send_header("content-type", "application/octet-stream")
                    self.send_header("x-hict-dtype", array.dtype.name)
                    self.send_header("x-hict-rows", str(array.shape[0]))
                    self.send_header("x-hict-cols", str(array.shape[1]))
                    self.end_headers()
                    self.wfile.write(array.tobytes())

            server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
            server.test = self
            worker = threading.Thread(target=server.serve_forever, daemon=True)
            worker.start()
            try:
                discovery = subprocess.run(
                    [
                        sys.executable,
                        str(ROOT / "discover_matrix_query.py"),
                        "--server",
                        f"http://127.0.0.1:{server.server_port}",
                    ],
                    capture_output=True,
                    text=True,
                    check=True,
                )
                self.assertIn("/matrix/query", json.loads(discovery.stdout)["paths"])
                for dtype in ["int64", "float64"]:
                    output = directory / dtype
                    environment = dict(
                        os.environ,
                        PYTHON=sys.executable,
                        DTYPE=dtype,
                        HICT_SERVER=f"http://127.0.0.1:{server.server_port}",
                        SIDES="2,4",
                        WINDOWS_PER_CELL="6",
                        WARMUPS="0",
                        REPS="2",
                    )
                    command = [
                        "bash",
                        str(ROOT / "run_pair_benchmark.sh"),
                        "fixture",
                        str(source),
                        "100",
                        str(output),
                    ]
                    result = subprocess.run(
                        command, env=environment, capture_output=True, text=True
                    )
                    self.assertEqual(
                        result.returncode, 0, result.stdout + result.stderr
                    )
                    manifest = load_manifest(output / "manifest.csv", "fixture")
                    self.assertEqual(len(manifest), 12)
                    for row in manifest:
                        if row["kind"] == "offdiagonal":
                            self.assertGreaterEqual(
                                abs(int(row["start1_bp"]) - int(row["start2_bp"])),
                                int(row["side_bins"]) * 100,
                            )
                    self.assertTrue((output / "summary-2.tex").exists())
                    cooler_before = (output / "cooler.csv").read_bytes()
                    repeated = subprocess.run(
                        command, env=environment, capture_output=True
                    )
                    self.assertNotEqual(repeated.returncode, 0)
                    self.assertEqual(
                        (output / "cooler.csv").read_bytes(), cooler_before
                    )
                    with (output / "hict.csv").open(newline="") as stream:
                        rows = list(csv.DictReader(stream))
                    rows[0]["checksum"] = "mismatch"
                    corrupted = directory / f"bad-{dtype}.csv"
                    with corrupted.open("w", newline="") as stream:
                        writer = csv.DictWriter(stream, fieldnames=rows[0].keys())
                        writer.writeheader()
                        writer.writerows(rows)
                    table = directory / f"bad-{dtype}.tex"
                    rejected = subprocess.run(
                        [
                            sys.executable,
                            str(ROOT / "summarize_benchmarks.py"),
                            str(output / "cooler.csv"),
                            str(corrupted),
                            "--manifest",
                            str(output / "manifest.csv"),
                            "--reps",
                            "2",
                            "--side",
                            "2",
                            "--output",
                            str(table),
                        ],
                        capture_output=True,
                    )
                    self.assertNotEqual(rejected.returncode, 0)
                    self.assertIn(b"checksum mismatch", rejected.stderr)
                    self.assertFalse(table.exists())
                self.assertTrue(any(body["startRowBin"] >= 100 for body in bodies))
            finally:
                server.shutdown()
                server.server_close()
                worker.join()


if __name__ == "__main__":
    unittest.main()
