"""Audit an AGP against source FASTA, then verify actual HiCT exports base for base.

Only part numbers are repaired in a separate copy; coordinate or length errors fail.
Requires Biopython. Source files are always opened read-only.
"""
import argparse
import gzip
import hashlib
import json
from collections import OrderedDict
from pathlib import Path

from Bio import SeqIO
from Bio.Seq import Seq


def sequences(path):
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt") as handle:
        return OrderedDict((record.id.split("|")[-1] if record.id.startswith("ENA|") else record.id, str(record.seq))
                           for record in SeqIO.parse(handle, "fasta"))


def digest(path):
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-fasta", type=Path, required=True)
    parser.add_argument("--agp", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--export-dir", type=Path)
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    source = sequences(args.source_fasta)
    objects = OrderedDict()
    repaired_rows, repairs = [], []
    missing_components = set()
    with args.agp.open() as handle:
        for line_number, line in enumerate(handle, 1):
            if not line.strip() or line.startswith("#"):
                continue
            row = line.split()
            assert len(row) == 9, (line_number, row)
            obj, start, end, part, kind = row[:5]
            records = objects.setdefault(obj, [])
            assert int(start) == (int(records[-1][2]) + 1 if records else 1), (line_number, "non-contiguous coordinates")
            length = int(end) - int(start) + 1
            assert length > 0, (line_number, "non-positive interval")
            if kind in ("N", "U"):
                assert length == int(row[5]), (line_number, "gap length mismatch")
                assert kind != "U" or length == 100
            else:
                assert kind in "ADFGOPW", (line_number, "component type")
                assert int(row[6]) >= 1
                if row[5] in source:
                    assert int(row[7]) <= len(source[row[5]]), (line_number, "source bounds")
                else:
                    missing_components.add(row[5])
                assert length == int(row[7]) - int(row[6]) + 1, (line_number, "component length mismatch")
            correct_part = str(len(records) + 1)
            if part != correct_part:
                repairs.append({"line": line_number, "field": "part_number", "old": part, "new": correct_part})
            row[3] = correct_part
            records.append(row)
            repaired_rows.append(row)
    corrected = args.output_dir / "input-renumbered.agp"
    assert corrected.resolve() != args.agp.resolve()
    corrected.write_text("\n".join("\t".join(row) for row in repaired_rows) + "\n")
    report = {"source_agp": str(args.agp), "source_agp_sha256": digest(args.agp),
              "source_fasta_sha256": digest(args.source_fasta), "objects": len(objects),
              "components": sum(row[4] not in ("N", "U") for row in repaired_rows),
              "coordinate_errors": 0, "part_number_repairs": repairs, "corrected_copy": str(corrected), "exports": []}
    report["missing_source_components"] = sorted(missing_components)
    report["matching_object_lengths"] = [
        {"object": name, "agp_length": int(records[-1][2]), "fasta_length": len(source[name]),
         "fasta_minus_agp": len(source[name]) - int(records[-1][2])}
        for name, records in objects.items() if name in source
    ]
    if args.export_dir:
        if missing_components:
            raise ValueError("The FASTA lacks original AGP components; provide the original contig FASTA, not an assembled output")
        model = json.loads((args.export_dir / "source-contigs.json").read_text())
        for contig in model:
            assert contig["length"] == len(source[contig["name"]]), (contig, "map/FASTA length mismatch")
        for agp in sorted(args.export_dir.glob("assembly-gap*.agp")):
            gap = int(agp.stem.removeprefix("assembly-gap"))
            actual = sequences(agp.with_suffix(".fasta"))
            assert list(actual) == list(objects), "Exported FASTA object order differs"
            expected_rows, checked = [], []
            for obj, records in objects.items():
                cursor, part = 1, 1
                parts = []
                components = [row for row in records if row[4] not in ("N", "U")]
                for index, row in enumerate(components):
                    component = source[row[5]][int(row[6]) - 1:int(row[7])]
                    if row[8] == "-":
                        component = str(Seq(component).reverse_complement())
                    parts.append(component)
                    expected_rows.append([obj, str(cursor), str(cursor + len(component) - 1), str(part), "W", *row[5:]])
                    cursor += len(component)
                    part += 1
                    if index < len(components) - 1 and gap:
                        expected_rows.append([obj, str(cursor), str(cursor + gap - 1), str(part), "N", str(gap), "scaffold", "yes", "proximity_ligation"])
                        cursor += gap
                        part += 1
                expected = ("N" * gap).join(parts)
                assert actual[obj] == expected, (obj, gap, "FASTA differs from independent reconstruction")
                assert cursor - 1 == len(actual[obj]), (obj, "AGP end differs from FASTA length")
                checked.append({"object": obj, "length": len(expected), "first_base": expected[0], "last_base": expected[-1],
                                "sha256": hashlib.sha256(expected.encode()).hexdigest()})
            actual_rows = [line.split() for line in agp.read_text().splitlines() if line and not line.startswith("#")]
            assert actual_rows == expected_rows, "AGP fields differ from independent reconstruction"
            report["exports"].append({"gap_bp": gap, "agp": str(agp), "fasta": str(agp.with_suffix(".fasta")), "passed": True, "objects": checked})
    report_path = args.output_dir / ("validation.json" if args.export_dir else "input-audit.json")
    report_path.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"report": str(report_path), "objects": len(objects), "part_number_repairs": len(repairs), "exports_checked": len(report["exports"])}))


if __name__ == "__main__":
    main()
