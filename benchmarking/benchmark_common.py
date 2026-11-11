"""Shared validation and portable checksums for paired dense-query benchmarks."""

import csv
import hashlib
from pathlib import Path

import numpy as np

MANIFEST_FIELDS = [
    "dataset",
    "resolution_bp",
    "side_bins",
    "kind",
    "query_id",
    "chrom1",
    "start1_bp",
    "end1_bp",
    "chrom2",
    "start2_bp",
    "end2_bp",
]
RESULT_FIELDS = [
    "dataset",
    "resolution_bp",
    "side_bins",
    "kind",
    "query_id",
    "backend",
    "rep",
    "latency_ms",
    "shape_ok",
    "checksum",
    "notes",
]


def positive_int(value):
    result = int(value)
    if result <= 0:
        raise ValueError("must be a positive integer")
    return result


def nonnegative_int(value):
    result = int(value)
    if result < 0:
        raise ValueError("must be a nonnegative integer")
    return result


def checksum(matrix, dtype="int64"):
    """Hash shape and canonical little-endian values without silent truncation."""
    array = np.asarray(matrix)
    if not np.isfinite(array).all():
        raise ValueError("matrix contains non-finite counts")
    if dtype == "int64":
        if array.dtype.kind not in "iu":
            if (
                (array != np.floor(array)).any()
                or (array < -(2**63)).any()
                or (array >= 2**63).any()
            ):
                raise ValueError(
                    "counts are not representable as int64; use --dtype float64"
                )
        elif array.dtype.kind == "u" and (array > np.iinfo(np.int64).max).any():
            raise ValueError("counts exceed int64")
    with np.errstate(over="raise", invalid="raise"):
        canonical = np.array(
            array,
            copy=True,
            order="C",
            dtype={"int64": "<i8", "float64": "<f8", "float32": "<f4"}[dtype],
        )
    # Normalize signed zero for equal numerical matrices.
    if canonical.dtype.kind == "f":
        canonical[canonical == 0] = 0
    digest = hashlib.sha256()
    digest.update(f"{dtype}:{array.shape}:".encode("ascii"))
    digest.update(canonical.tobytes())
    return digest.hexdigest()


def load_manifest(path, dataset):
    with open(path, newline="", encoding="utf-8") as stream:
        reader = csv.DictReader(stream)
        if not set(MANIFEST_FIELDS).issubset(reader.fieldnames or []):
            raise ValueError("manifest is missing required columns")
        rows = [row for row in reader if dataset is None or row["dataset"] == dataset]
    if not rows:
        raise ValueError(f"no manifest rows for dataset={dataset}")
    seen = set()
    for row in rows:
        res = positive_int(row["resolution_bp"])
        side = positive_int(row["side_bins"])
        if row["kind"] not in {"diagonal", "offdiagonal"}:
            raise ValueError(f"invalid query kind: {row['kind']}")
        key = (row["dataset"], res, side, row["kind"], row["query_id"])
        if not all(row.get(field) for field in MANIFEST_FIELDS):
            raise ValueError(f"empty manifest field in query {key}")
        if key in seen:
            raise ValueError(f"duplicate manifest query: {key}")
        seen.add(key)
        for axis in (1, 2):
            start = nonnegative_int(row[f"start{axis}_bp"])
            end = positive_int(row[f"end{axis}_bp"])
            if start % res or end - start != res * side:
                raise ValueError(
                    f"query {key}: unaligned coordinates or inconsistent side_bins"
                )
        same_chrom = row["chrom1"] == row["chrom2"]
        same_start = int(row["start1_bp"]) == int(row["start2_bp"])
        if row["kind"] == "diagonal" and not (same_chrom and same_start):
            raise ValueError(f"query {key}: diagonal windows must be identical")
        if (
            row["kind"] == "offdiagonal"
            and same_chrom
            and abs(int(row["start1_bp"]) - int(row["start2_bp"])) < res * side
        ):
            raise ValueError(f"query {key}: offdiagonal windows must not overlap")
    return rows


def validate_window(cool, row):
    res = int(row["resolution_bp"])
    if cool.binsize != res:
        raise ValueError(f"Cooler binsize {cool.binsize} does not match {res}")
    for axis in (1, 2):
        chrom = row[f"chrom{axis}"]
        if chrom not in cool.chromsizes or int(row[f"end{axis}_bp"]) > int(
            cool.chromsizes[chrom]
        ):
            raise ValueError(f"query exceeds chromosome bounds: {chrom}")


def open_results(path):
    """Never mix a new benchmark with stale measurements."""
    return Path(path).open("x", newline="", encoding="utf-8")
