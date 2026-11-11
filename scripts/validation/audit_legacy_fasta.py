"""Compare a legacy assembled FASTA against independently sourced AGP components."""
import argparse
import json
from pathlib import Path

from Bio.Seq import Seq

from validate_agp_fasta import sequences


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-fasta", type=Path, required=True)
    parser.add_argument("--assembled-fasta", type=Path, required=True)
    parser.add_argument("--agp", type=Path, required=True)
    parser.add_argument("--gap", type=int, default=1000)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    source = sequences(args.source_fasta)
    actual = sequences(args.assembled_fasta)
    objects = {}
    for line in args.agp.read_text().splitlines():
        if not line.strip() or line.startswith("#"):
            continue
        row = line.split()
        if row[4] not in ("N", "U"):
            objects.setdefault(row[0], []).append(row)
    if len(actual) != len(objects):
        raise ValueError("Different object counts; cannot compare record order")
    results = []
    # Legacy unscaffolded labels can differ. Record both names and compare bases,
    # rather than treating the positional pairing as proof of correspondence.
    for (name, rows), (actual_name, observed) in zip(objects.items(), actual.items()):
        variants = {key: [] for key in ("complete", "source_last_base_missing", "oriented_last_base_missing")}
        for row in rows:
            component = source[row[5]][int(row[6]) - 1:int(row[7])]
            orient = lambda value: str(Seq(value).reverse_complement()) if row[8] == "-" else value
            variants["complete"].append(orient(component))
            variants["source_last_base_missing"].append(orient(component[:-1]))
            variants["oriented_last_base_missing"].append(orient(component)[:-1])
        expected = {key: ("N" * args.gap).join(parts) for key, parts in variants.items()}
        results.append({"agp_object": name, "fasta_object": actual_name,
                        "component_count": len(rows), "observed_length": len(observed),
                        "expected_length": len(expected["complete"]),
                        "exact_matches": [key for key, value in expected.items() if observed == value],
                        "case_insensitive_matches": [key for key, value in expected.items() if observed.upper() == value.upper()]})
    report = {"gap_bp": args.gap, "objects": results,
              "matching_counts": {key: sum(key in row["case_insensitive_matches"] for row in results) for key in variants},
              "comparison_note": "Summary ignores softmask case; per-object exact matches are also recorded."}
    args.report.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report["matching_counts"]))


if __name__ == "__main__":
    main()
