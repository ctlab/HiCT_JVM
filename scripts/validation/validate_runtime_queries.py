#!/usr/bin/env python3
"""Compare QueryAssembly's real HiCT queries to independent Cooler matrix reads."""
import argparse
import json
from pathlib import Path

import cooler
import h5py
import numpy as np


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cooler", required=True)
    parser.add_argument("--queries", type=Path, required=True)
    parser.add_argument("--agp", type=Path)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    probe = json.loads(args.queries.read_text())
    resolution = probe["resolution"]
    matrix = cooler.Cooler(args.cooler)
    components = {}
    with h5py.File(matrix.filename, "r") as source:
        group = source.get("hict_metadata/assembly")
        if group is not None and "agp_component_name" in group:
            names = group["agp_component_name"].asstr()[:]
            scaffold_names = group["agp_scaffold_name"].asstr()[:]
            for index, name in enumerate(names):
                components[name] = (
                    scaffold_names[group["agp_component_scaffold_id"][index]],
                    int(group["agp_component_scaffold_start_bp"][index]) // resolution,
                )
    if args.agp:
        records = [line.split() for line in args.agp.read_text().splitlines() if line.strip() and not line.startswith("#")]
        expected = [(r[5], int(r[7]) - int(r[6]) + 1, "REVERSED" if len(r) > 8 and r[8] == "-" else "FORWARD")
                    for r in records if r[4] not in ("N", "U")]
        actual = [(c["name"], c["length"], c["direction"]) for c in probe["contigs"]]
        assert actual == expected, "Runtime component order, length or orientation differs from AGP"
    indices = []
    for contig in probe["contigs"]:
        name = contig["name"]
        if name in components:
            chrom, local_start = components[name]
            start = matrix.extent(chrom)[0] + local_start
            mapped = np.arange(start, start + contig["bins"])
        else:
            start, end = matrix.extent(name)
            assert end - start == contig["bins"], f"Bin count mismatch: {name}"
            mapped = np.arange(start, end)
            if contig["direction"] == "REVERSED":
                mapped = mapped[::-1]
        indices.extend(mapped.tolist())
    indices = np.asarray(indices, dtype=np.int64)
    selector = matrix.matrix(balance=False)

    def runs(values):
        boundaries = np.r_[0, np.flatnonzero(np.diff(values) != 1) + 1, len(values)]
        return [(int(a), int(b)) for a, b in zip(boundaries[:-1], boundaries[1:])]

    for query in probe["queries"]:
        actual = np.asarray(query["values"], dtype=np.int64)
        rows = indices[query["row"]:query["row"] + actual.shape[0]]
        cols = indices[query["col"]:query["col"] + actual.shape[1]]
        expected = np.zeros_like(actual)
        for ra, rb in runs(rows):
            for ca, cb in runs(cols):
                expected[ra:rb, ca:cb] = selector[int(rows[ra]):int(rows[rb - 1]) + 1, int(cols[ca]):int(cols[cb - 1]) + 1]
        np.testing.assert_array_equal(actual, expected, err_msg=f"HiCT query row={query['row']} col={query['col']}")
    report = {"cooler": args.cooler, "hict": probe["input"], "resolution": resolution,
              "components": len(probe["contigs"]), "queries": len(probe["queries"]),
              "raw_matrix_equality": True, "agp_assembly_equality": bool(args.agp)}
    args.report.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report))


if __name__ == "__main__":
    main()
