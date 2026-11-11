#!/usr/bin/env python3
"""Plot measured opening times; requires matplotlib, never opens source datasets."""
import argparse
import csv
import json
from pathlib import Path
import statistics

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--before", type=Path, required=True)
    parser.add_argument("--after", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    runs = {name: json.loads((directory / "results.json").read_text())
            for name, directory in (("Before", args.before), ("After", args.after))}
    names = ("human", "rhino", "aedes", "micro")
    labels = ("Human", "RHINO", "Aedes", "Micro-C")
    colors = {"Before": "#666666", "After": "#157f70"}
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.8), layout="constrained")
    rows = []
    for index, name in enumerate(names):
        for variant, offset in (("Before", -0.18), ("After", 0.18)):
            values = [r["openSeconds"] for r in runs[variant] if r["dataset"] == name]
            if not values:
                raise ValueError(f"No results for {variant} {name}")
            axes[0].boxplot([values], positions=[index + offset], widths=0.28,
                            patch_artist=True, manage_ticks=False,
                            boxprops={"facecolor": colors[variant], "alpha": 0.5},
                            medianprops={"color": "black"})
            axes[0].scatter([index + offset] * len(values), values, color=colors[variant], s=18, zorder=3)
            for result in runs[variant]:
                if result["dataset"] != name:
                    continue
                metadata_seconds = sum(s["seconds"] for s in result["stages"]
                                       if s["stage"] == "Reading stripe weights")
                axes[1].scatter(index + offset, metadata_seconds, color=colors[variant], s=25,
                                label=variant if index == 0 and result["run"] == 1 else None)
                rows.append({"dataset": name, "variant": variant, "run": result["run"],
                             "open_seconds": result["openSeconds"], "weight_metadata_seconds": metadata_seconds,
                             "queries": result["queries"], "sha256": result["sha256"]})
        before = statistics.median(r["openSeconds"] for r in runs["Before"] if r["dataset"] == name)
        after = statistics.median(r["openSeconds"] for r in runs["After"] if r["dataset"] == name)
        print(f"{name}: median {before:.3f} -> {after:.3f} seconds; {before / after:.2f}x")
    for ax in axes:
        ax.set_xticks(range(len(names)), labels)
        ax.set_yscale("log")
        ax.yaxis.set_major_formatter(FuncFormatter(lambda value, _: f"{value:g}"))
        ax.yaxis.set_minor_formatter(FuncFormatter(lambda value, _: f"{value:g}"))
        ax.grid(axis="y", alpha=0.2, which="both")
        ax.set_axisbelow(True)
        ax.set_ylabel("Seconds (log scale)")
    axes[0].set_title("File open including native initialization")
    axes[1].set_title("Weights and per-resolution ATU metadata")
    axes[1].legend(frameon=False)
    fig.suptitle("HiCT opening benchmark: three fresh JVM runs per dataset and version\n"
                 "Linux, Ryzen 9 7900X, 8 GiB heap; filesystem caches not cleared", fontsize=11)
    for extension in ("png", "pdf", "svg"):
        fig.savefig(args.output / f"opening.{extension}", dpi=200)
    plt.close(fig)
    with (args.output / "opening.csv").open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    (args.output / "raw-results.json").write_text(json.dumps(runs, indent=2))


if __name__ == "__main__":
    main()
