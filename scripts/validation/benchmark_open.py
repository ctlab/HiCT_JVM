#!/usr/bin/env python3
"""Run read-only JVM opening/query probes; each repetition uses a fresh process."""
import argparse
import json
from pathlib import Path
import subprocess

DATASETS = {
    "human": "4DNFIM351CAA_HomoSapiens_HiC_1kb.hict.hdf5",
    "rhino": "RHINO_1k.mcool.hict.hdf5",
    "aedes": "DNAZoo/AedesAegypti/AaegL5.0.hict.hdf5",
    "micro": "micro-c/GSE286495/GSE286495_mESC_merged_15.6B_mm39.mcool.local.hict.hdf5",
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--jar", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--data", type=Path, default=Path("/mnt/Models/HiCT/data"))
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--reference", type=Path)
    args = parser.parse_args()
    if args.runs < 1:
        parser.error("--runs must be positive")
    args.output.mkdir(parents=True, exist_ok=False)
    subprocess.run(["javac", "-cp", str(args.jar), "-d", str(args.output),
                    str(Path(__file__).with_name("ProfileOpen.java"))], check=True)
    results = []
    for name, relative in DATASETS.items():
        for run in range(1, args.runs + 1):
            prefix = args.output / f"{name}-{run}"
            with prefix.with_suffix(".log").open("w") as log:
                subprocess.run(["java", "-Xmx8g", "-cp", f"{args.output}:{args.jar}",
                                "ProfileOpen", str(args.data / relative),
                                str(prefix.with_suffix(".json"))],
                               stdout=log, stderr=subprocess.STDOUT, check=True, timeout=900)
            result = json.loads(prefix.with_suffix(".json").read_text())
            result.update(dataset=name, run=run)
            if args.reference:
                reference = json.loads((args.reference / f"{name}-{run}.json").read_text())
                for key in ("sha256", "resolutions", "contigs", "queries", "nonFiniteWeights"):
                    if result[key] != reference[key]:
                        raise AssertionError(f"{name}: changed {key}: {reference[key]} -> {result[key]}")
            results.append(result)
            (args.output / "results.json").write_text(json.dumps(results, indent=2))
            print(f"{name} run {run}: {result['openSeconds']:.3f}s; {result['queries']} queries", flush=True)


if __name__ == "__main__":
    main()
