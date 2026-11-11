#!/usr/bin/env python3
"""Require complete, correct paired measurements before emitting a LaTeX table."""

import argparse
import csv
import math
from collections import defaultdict
from pathlib import Path

import numpy as np

from benchmark_common import RESULT_FIELDS, load_manifest, positive_int

BACKENDS = {"hict_jvm_dense_raw", "cooler_dense_raw"}


def esc(value):
    replacements = {
        "\\": r"\textbackslash{}",
        "&": r"\&",
        "%": r"\%",
        "$": r"\$",
        "#": r"\#",
        "_": r"\_",
        "{": r"\{",
        "}": r"\}",
        "~": r"\textasciitilde{}",
        "^": r"\textasciicircum{}",
    }
    return "".join(replacements.get(char, char) for char in str(value))


def validate(rows, side, manifest=None, reps=None):
    queries = defaultdict(lambda: defaultdict(dict))
    for row in rows:
        if int(row["side_bins"]) != side:
            continue
        if row["shape_ok"] not in {"1", "true", "True"}:
            raise ValueError("a measured matrix has an invalid shape")
        if row["backend"] not in BACKENDS:
            raise ValueError(f"unknown backend: {row['backend']}")
        latency = float(row["latency_ms"])
        if not math.isfinite(latency) or latency <= 0:
            raise ValueError("latencies must be finite and positive")
        if not row["checksum"]:
            raise ValueError("missing matrix checksum")
        key = (
            row["dataset"],
            positive_int(row["resolution_bp"]),
            row["kind"],
            row["query_id"],
        )
        rep = int(row["rep"])
        samples = queries[key][row["backend"]]
        if rep < 0 or rep in samples:
            raise ValueError(f"duplicate or invalid repetition for {key}")
        samples[rep] = row
    if not queries:
        raise ValueError(f"no measurements for side_bins={side}")
    if manifest is not None:
        expected = {
            (row["dataset"], int(row["resolution_bp"]), row["kind"], row["query_id"])
            for row in manifest
            if int(row["side_bins"]) == side
        }
        if len(expected) != sum(int(row["side_bins"]) == side for row in manifest):
            raise ValueError("duplicate query across supplied manifests")
        if set(queries) != expected:
            raise ValueError("measured queries do not match the complete manifest")
    groups = defaultdict(list)
    for key, backends in queries.items():
        if set(backends) != BACKENDS:
            raise ValueError(f"missing paired backend for {key}")
        rep_sets = [set(samples) for samples in backends.values()]
        if rep_sets[0] != rep_sets[1] or rep_sets[0] != set(range(len(rep_sets[0]))):
            raise ValueError(f"incomplete paired repetitions for {key}")
        if reps is not None and rep_sets[0] != set(range(reps)):
            raise ValueError(f"missing requested repetitions for {key}")
        checksums = {
            row["checksum"] for samples in backends.values() for row in samples.values()
        }
        if len(checksums) != 1:
            raise ValueError(
                f"checksum mismatch for {key}; timings must not be published"
            )
        for backend, samples in backends.items():
            groups[(key[0], key[1], backend)].extend(
                float(row["latency_ms"]) for row in samples.values()
            )
    return groups


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inputs", nargs="+")
    parser.add_argument("--side", type=positive_int, default=1024)
    parser.add_argument(
        "--manifest",
        action="append",
        required=True,
        help="source manifest; repeat this option for multiple datasets",
    )
    parser.add_argument(
        "--reps",
        type=positive_int,
        required=True,
        help="expected measured repetitions for every query and backend",
    )
    parser.add_argument("--output", default="benchmark_results_table_measured.tex")
    args = parser.parse_args()
    rows = []
    for path in args.inputs:
        with open(path, newline="", encoding="utf-8") as stream:
            reader = csv.DictReader(stream)
            if not set(RESULT_FIELDS).issubset(reader.fieldnames or []):
                parser.error(f"{path}: missing result columns")
            rows.extend(reader)
    try:
        manifest = [row for path in args.manifest for row in load_manifest(path, None)]
        values = validate(rows, args.side, manifest, args.reps)
    except (ValueError, KeyError) as error:
        parser.error(str(error))
    groups = defaultdict(dict)
    for (dataset, resolution, backend), samples in values.items():
        groups[(dataset, resolution)][backend] = (
            float(np.median(samples)),
            float(np.percentile(samples, 95)),
        )
    lines = [
        r"\begin{table}[!ht]",
        r"\centering",
        rf"\caption{{Warm dense raw-query latency for {args.side}$\times${args.side} windows. Values are median / p95 in ms.}}",
        r"\label{tab:benchmark}",
        r"\begin{tabular}{lrrrr}",
        r"\toprule",
        r"Dataset & Resolution & HiCT & Cooler & $C/H$ \\",
        r"\midrule",
    ]
    for (dataset, resolution), backends in sorted(groups.items()):
        hict = backends["hict_jvm_dense_raw"]
        cooler = backends["cooler_dense_raw"]
        lines.append(
            f"{esc(dataset)} & {resolution} bp & {hict[0]:.2f} / {hict[1]:.2f}"
            f" & {cooler[0]:.2f} / {cooler[1]:.2f} & {cooler[0]/hict[0]:.2f} \\\\"
        )
    lines.extend([r"\bottomrule", r"\end{tabular}", r"\end{table}"])
    # Refuse overwrites so a failed later run cannot leave an apparently fresh table.
    with Path(args.output).open("x", encoding="utf-8") as stream:
        stream.write("\n".join(lines) + "\n")
    print(f"wrote {args.output}; all selected queries and repetitions match")


if __name__ == "__main__":
    main()
