"""Controlled external-command fixture. Never contacts NCBI or fabricates server job state."""

import json
import os
from pathlib import Path
import sys
import time


def run_mock(root):
    root = Path(root).resolve()
    assert root.is_relative_to("/tmp")
    args = sys.argv[1:]
    if "-remote" not in args:
        raise SystemExit("This controlled fixture only accepts explicit -remote calls")
    query_path = Path(args[args.index("-query") + 1])
    output = Path(args[args.index("-out") + 1])
    assert query_path.is_relative_to("/tmp") and output.is_relative_to("/tmp")
    queries = []
    for line in query_path.read_text().splitlines():
        if line.startswith(">"):
            queries.append([line[1:], ""])
        else:
            queries[-1][1] += line
    settings = json.loads((root / "control.json").read_text())
    fields = Path("/proc/self/stat").read_text().rsplit(")", 1)[1].split()
    receipt = {
        "pid": os.getpid(),
        "parent": os.getppid(),
        "startTicks": fields[19],
        "args": args,
        "queryFile": query_path.read_text(),
        "mode": settings["mode"],
    }
    (root / "calls" / (str(os.getpid()) + ".json")).write_text(
        json.dumps(receipt, indent=2)
    )
    mode = settings["mode"]
    if mode == "slow":
        time.sleep(3600)
    if mode == "failure":
        print(
            "Controlled provider unavailable. Try again explicitly after checking connectivity.",
            file=sys.stderr,
        )
        raise SystemExit(2)
    if mode == "oversized":
        output.write_bytes(b"x" * (33 * 1024 * 1024))
        return
    if mode == "malformed":
        output.write_text("not a BLAST table\n")
        return
    count = 0 if mode == "empty" else settings.get("count", 125)
    with output.open("w") as stream:
        for i in range(count):
            query, sequence = queries[i % len(queries)]
            length = len(sequence)
            accession = "javascript:alert(1)" if i == 0 else "NC_000001.1"
            title = (
                '<img src=x onerror="window.__remoteAttack=1">'
                if i == 0
                else "Controlled external subject " + str(i)
            )
            stream.write(
                "\t".join(
                    map(
                        str,
                        [
                            query,
                            "external_" + str(i),
                            accession,
                            2,
                            length - 1,
                            100 + length,
                            102,
                            97.5,
                            length - 1,
                            1,
                            1,
                            "1e-20",
                            100,
                            length,
                            1000,
                            title,
                        ],
                    )
                )
                + "\n"
            )


def make_executable(root):
    root = Path(root).absolute()
    assert root == root.resolve() and root.is_relative_to("/tmp")
    root.mkdir(exist_ok=False)
    (root / "calls").mkdir()
    (root / "control.json").write_text(json.dumps({"mode": "success", "count": 125}))
    executable = root / "blastn"
    executable.write_text(
        "#!"
        + sys.executable
        + "\nimport sys\nsys.path.insert(0, "
        + repr(str(Path(__file__).resolve().parent))
        + ")\nfrom remote_blast_fixture import run_mock\nrun_mock("
        + repr(str(root))
        + ")\n"
    )
    executable.chmod(0o700)
    return executable
