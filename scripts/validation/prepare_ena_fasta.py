"""Combine downloaded ENA sequences, keeping exact bases and accession.version IDs."""
import argparse
import gzip
import json

from validate_agp_fasta import sequences, digest
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("inputs", type=Path, nargs="+")
    args = parser.parse_args()
    assert args.output.resolve() not in [path.resolve() for path in args.inputs]
    bases = {}
    for path in args.inputs:
        for name, sequence in sequences(path).items():
            assert name not in bases, (name, "duplicate accession")
            bases[name] = sequence
    model = json.loads(args.model.read_text())
    assert set(bases) == {contig["name"] for contig in model}, "Accession sets differ"
    for contig in model:
        assert len(bases[contig["name"]]) == contig["length"], (contig, "length differs")
    with gzip.open(args.output, "wt", compresslevel=1) as output:
        for contig in model:
            name = contig["name"]
            output.write(">" + name + "\n")
            sequence = bases[name]
            for start in range(0, len(sequence), 80):
                output.write(sequence[start:start + 80] + "\n")
    report = {"inputs": {str(path): digest(path) for path in args.inputs},
              "output": str(args.output), "sha256": digest(args.output), "contigs": len(model),
              "bases": sum(contig["length"] for contig in model)}
    args.output.with_suffix(".json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report))


if __name__ == "__main__":
    main()
