"""Exercise real bundled tools through HiCT and renamed Bash/Slurm dry runs.

All outputs are written to --output-dir. Requires h5py and numpy for pixel checks.
"""
import argparse
from collections import Counter
import json
import os
from pathlib import Path
import random
import shutil
import subprocess
import time

import h5py
import numpy as np


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--jar", type=Path, required=True)
    parser.add_argument("--toolchain", type=Path, required=True)
    parser.add_argument("--script", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    root = args.output_dir.resolve()
    root.mkdir(parents=True, exist_ok=False)
    env = {key: value for key, value in os.environ.items() if not key.startswith(("SLURM_", "HICT_"))}
    env.update(SLURM_JOB_ID="thread-check", SLURM_CPUS_PER_TASK="2", SLURM_NTASKS="1",
               HICT_TOOLCHAIN_DIR=str(args.toolchain.resolve()), HICT_HICTK_THREADS="-1", HICT_ALIGNMENT_THREADS="-1")
    java = ["java", "-Xmx512m", "-cp", str(args.jar.resolve()), "ru.itmo.ctlab.hict.hict_server.tools.HictCli"]
    results = []

    def run(label, tool, command, selected=None):
        start = time.monotonic()
        proc = subprocess.run(java + ["toolbox", tool] + list(map(str, command)), env=env,
                              capture_output=True, text=True, timeout=120)
        (root / f"{label}.stdout.log").write_text(proc.stdout)
        (root / f"{label}.stderr.log").write_text(proc.stderr)
        if proc.returncode:
            raise RuntimeError(f"{label} failed ({proc.returncode}): {proc.stderr[-2000:]}")
        if selected is not None:
            assert f"selected={selected}," in proc.stderr, (label, proc.stderr)
        results.append({"command": label, "seconds": time.monotonic() - start, "selected_threads": selected})
        return proc.stdout

    sizes = root / "chrom.sizes"
    sizes.write_text("chr1\t100000\n")
    pixels = {(i, j): (i + j) % 7 + 1 for i in range(100) for j in range(i, 100)}
    coo = root / "input.coo"
    coo.write_text("".join(f"{i}\t{j}\t{n}\n" for (i, j), n in pixels.items()))
    cool, mcool = root / "base.cool", root / "pyramid.mcool"
    run("load", "hictk", ["load", "--format", "coo", "--chrom-sizes", sizes, "--bin-size", "1000", coo, cool], 2)
    # This ordering also guards CLI11's variadic resolution/positional parsing.
    run("zoomify", "hictk", ["zoomify", "--copy-base-resolution", "--resolutions", "1000", "2000", "5000", cool, mcool], 2)
    with h5py.File(mcool, "r") as handle:
        for resolution in (1000, 2000, 5000):
            expected = Counter()
            for (i, j), n in pixels.items():
                expected[i // (resolution // 1000), j // (resolution // 1000)] += n
            table = handle[f"resolutions/{resolution}/pixels"]
            actual = dict(zip(zip(table["bin1_id"][:], table["bin2_id"][:]), table["count"][:]))
            assert actual == expected, resolution
    hic, restored, merged = root / "converted.hic", root / "restored.cool", root / "merged.cool"
    run("convert-to-hic", "hictk", ["convert", cool, hic, "--resolutions", "1000"], 2)
    run("convert-to-cool", "hictk", ["convert", hic, restored, "--resolutions", "1000"], 2)
    run("merge", "hictk", ["merge", cool, cool, "--output-file", merged], 2)
    for filename, factor in ((restored, 1), (merged, 2)):
        with h5py.File(filename, "r") as handle:
            table = handle["pixels"]
            actual = dict(zip(zip(table["bin1_id"][:], table["bin2_id"][:]), table["count"][:]))
            assert actual == {key: value * factor for key, value in pixels.items()}, filename
    run("balance", "hictk", ["balance", "ice", "--ignore-diags", "0", "--min-nnz", "0", "--create-weight-link", mcool], 2)
    weights = {}
    with h5py.File(mcool, "r") as handle:
        for resolution in (1000, 2000, 5000):
            weights[resolution] = handle[f"resolutions/{resolution}/bins/weight"][:]
            assert np.isfinite(weights[resolution]).any()
    run("balance-explicit", "hictk", ["balance", "ice", "--threads=1", "--force", "--ignore-diags", "0", "--min-nnz", "0", mcool], 1)
    with h5py.File(mcool, "r") as handle:
        for resolution, expected in weights.items():
            np.testing.assert_allclose(handle[f"resolutions/{resolution}/bins/weight"][:], expected, equal_nan=True)
    run("dump", "hictk", ["dump", cool])
    assert "HiCT tool threads:" not in (root / "dump.stderr.log").read_text()
    rng = random.Random(27)
    dna = "".join(rng.choices("ACGT", k=20000))
    fasta = root / "reference.fa"
    fasta.write_text(">chr1\n" + dna + "\n")
    for tool in ("minimap2", "mm2-plus-avx2", "mm2-plus-avx512"):
        paf = run(tool, tool, ["-x", "asm5", fasta, fasta], 2)
        assert any(line.startswith("chr1\t20000\t") for line in paf.splitlines()), tool
        assert "HiCT tool threads:" not in paf, "Diagnostic text contaminated PAF stdout"

    renamed = root / "arbitrary-name.sbatch"
    shutil.copyfile(args.script, renamed)
    runfile = root / "HiCT-test.run"
    runfile.write_text("#!/usr/bin/env bash\nexit 99\n")
    agp = root / "input.agp"
    agp.write_text("chr1\t1\t100000\t1\tW\tchr1\t1\t100000\t+\n")
    for label, overrides, expected in (
        ("auto", {}, 2), ("one", {"THREADS": "1"}, 1),
        ("clamped", {"THREADS": "999"}, 2), ("invalid", {"THREADS": "bad"}, None),
    ):
        case_env = env | dict(HICT_HOME=str(root), HICT_RUNFILE=str(runfile), INPUT_AGP=str(agp),
                              SOURCE_MCOOL=str(cool), INPUT_HICT=str(root / "absent.hict.hdf5"),
                              OUTPUT_MCOOL=str(root / "dry-run-output.mcool"), DRY_RUN="1",
                              SLURM_SUBMIT_DIR=str(root), SLURM_JOB_ID=label, HICT_JAVA_HEAP="2g") | overrides
        proc = subprocess.run(["bash", str(renamed)], env=case_env, capture_output=True, text=True, timeout=30)
        assert proc.returncode == (2 if expected is None else 0), (label, proc.stderr)
        if expected is not None:
            progress = (root / "logs" / label / "conversion.progress.log").read_text()
            assert f"hictk threads: {expected}" in progress
            assert f"--threads {expected}" in progress
        results.append({"script_case": label, "exit_code": proc.returncode})
    (root / "results.json").write_text(json.dumps(results, indent=2) + "\n")
    print(json.dumps({"passed": len(results), "results": str(root / "results.json")}))


if __name__ == "__main__":
    main()
