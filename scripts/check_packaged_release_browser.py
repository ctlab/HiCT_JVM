#!/usr/bin/env python3
"""Run sequential browser gates on an extracted Linux package and its own runtime/assets."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import sys
import time
from urllib.request import urlopen


def sha256(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--app", required=True, help="Fresh extracted Linux package root"
    )
    parser.add_argument("--output", required=True, help="Fresh physical /tmp directory")
    parser.add_argument(
        "--source-map",
        action="append",
        default=[],
        help="Physically copy a map into disposable DATA",
    )
    parser.add_argument("--webui-commit", required=True)
    parser.add_argument(
        "--browser-python",
        default=sys.executable,
        help="Python with Playwright and NumPy for browser child processes",
    )
    parser.add_argument("--soak-seconds", type=int, default=1800)
    args = parser.parse_args()
    app, out = Path(args.app).absolute(), Path(args.output).absolute()
    for path in (app, out):
        assert path == path.resolve() and path.is_relative_to("/tmp"), path
    assert args.soak_seconds > 0
    out.mkdir(exist_ok=False)
    data = out / "data"
    data.mkdir()
    required = sum(Path(source).stat().st_size for source in args.source_map) + 2 * (1 << 30)
    assert shutil.disk_usage(out).free >= required, (
        f"Release qualification needs {required} free bytes for physical input copies "
        "and 2 GiB of working headroom; free space before starting"
    )
    jar = app / "lib/hict.jar"
    assert jar.is_file() and (app / "webui/index.html").is_file()
    evidence = {
        "status": "preparing",
        "jarSha256": sha256(jar),
        "app": str(app),
        "stages": [],
    }

    def save():
        pending = out / ".qualification.json.tmp"
        try:
            pending.write_text(json.dumps(evidence, indent=2))
            pending.replace(out / "qualification.json")
        finally:
            pending.unlink(missing_ok=True)

    save()
    with (out / "fixture.log").open("w") as log:
        subprocess.run(
            [
                "java",
                "--class-path",
                str(jar),
                "src/test/java/ru/itmo/ctlab/hict/fixtures/ReinforcementHdf5Fixture.java",
                "mixed-pyramids",
                str(data),
            ],
            check=True,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
    from check_structural_edits_browser import prepare_fixture

    prepare_fixture(data, jar)
    (data / "mixed.fa").write_text(">tiny\n" + ("ACGTTGCA" * 1503)[:12017] + "\n")
    import h5py

    with h5py.File(data / "incomplete.hict.hdf5", "w") as partial:
        partial.create_dataset("resolutions/100000/treap_coo/block_length", data=[1])
    names = ["mixed-primary.hict.hdf5"]
    inputs = []
    for source in map(Path, args.source_map):
        assert source.is_file() and not source.is_symlink()
        target = data / source.name
        assert not target.exists(), target
        shutil.copyfile(source, target)
        source_hash = sha256(source)
        assert sha256(target) == source_hash, f"Source changed while copying: {source}"
        inputs.append(
            {
                "filename": source.name,
                "sha256": source_hash,
                "bytes": target.stat().st_size,
            }
        )
        names.append(source.name)
    evidence["inputSources"] = inputs
    hashes = {p.name: sha256(p) for p in data.glob("*.hdf5")}
    sockets = []
    try:
        for _ in range(2):
            reservation = socket.socket()
            reservation.bind(("127.0.0.1", 0))
            sockets.append(reservation)
        api_port, web_port = [s.getsockname()[1] for s in sockets]
    finally:
        for reservation in sockets:
            reservation.close()
    api, web = f"http://127.0.0.1:{api_port}", f"http://127.0.0.1:{web_port}"
    env = {
        **os.environ,
        "DATA_DIR": str(data),
        "PROCESSED_DIR": str(out / "processed"),
        "VXPORT": str(api_port),
        "WEBUI_PORT": str(web_port),
        "HICT_BIND_HOST": "127.0.0.1",
        "AUTO_OPEN_BROWSER": "false",
        "WEBUI_ROOT": str(app / "webui"),
        "HICT_TOOLCHAIN_DIR": str(app / "toolchains/linux_x86_64"),
        "HICT_JAVA_OPTS": "-Xmx2g -XX:NativeMemoryTracking=summary",
    }
    env.pop("HICT_JHDF5_NATIVES_ARCHIVE", None)
    server = None
    try:
        with (out / "server.log").open("w") as log:
            server = subprocess.Popen(
                [str(app / "bin/hict"), "start-server", "--serve-webui=true"],
                env=env,
                cwd=data,
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
            evidence.update(status="starting", api=api, url=web, pid=server.pid)
            save()
            deadline = time.monotonic() + 60
            while True:
                assert server.poll() is None, "Packaged server exited during startup"
                try:
                    with urlopen(api + "/version", timeout=2) as response:
                        version = json.load(response)
                    with urlopen(web + "/", timeout=2) as response:
                        index = response.read()
                    break
                except OSError:
                    if time.monotonic() > deadline:
                        raise TimeoutError("Packaged API/WebUI startup timed out")
                    time.sleep(0.1)
            assert index == (app / "webui/index.html").read_bytes()
            assets = []
            for asset in sorted((app / "webui/assets").rglob("*")):
                if not asset.is_file():
                    continue
                relative = asset.relative_to(app / "webui").as_posix()
                with urlopen(web + "/" + relative, timeout=20) as response:
                    actual = response.read()
                assert hashlib.sha256(actual).hexdigest() == sha256(asset), relative
                assets.append(
                    {"path": relative, "sha256": sha256(asset), "bytes": len(actual)}
                )
            evidence.update(status="running", version=version, assetCount=len(assets))
            (out / "served-assets.json").write_text(json.dumps(assets, indent=2))
            save()
            common = ["--url", web, "--api", api]

            def stage(name, script, arguments):
                target = out / name
                print("Starting", name, flush=True)
                row = {"stage": name, "status": "running", "startedAt": time.time()}
                evidence["stages"].append(row)
                save()
                with (out / (name + ".log")).open("w") as stage_log:
                    result = subprocess.run(
                        [
                            args.browser_python,
                            "scripts/" + script,
                            *common,
                            "--output",
                            str(target),
                            *arguments,
                        ],
                        stdout=stage_log,
                        stderr=subprocess.STDOUT,
                    )
                row.update(
                    status="passed" if result.returncode == 0 else "failed",
                    exitCode=result.returncode,
                    elapsedSeconds=time.time() - row["startedAt"],
                )
                save()
                assert (
                    result.returncode == 0
                ), f'{name} failed; inspect {out/(name+".log")}'
                print("Passed", name, flush=True)

            stage(
                "metadata",
                "check_release_metadata_browser.py",
                [
                    "--webui-version",
                    "2.0.0",
                    "--webui-commit",
                    args.webui_commit,
                    "--file",
                    "mixed-primary.hict.hdf5",
                ],
            )
            stage(
                "menus",
                "check_menu_surfaces_browser.py",
                ["--file", "structural.hict.hdf5"],
            )
            stage("structural", "check_structural_edits_browser.py", [])
            stage("incomplete-open", "check_incomplete_map_browser.py", [])
            stage("offline", "check_offline_browser.py", [])
            for name in ["structural.hict.hdf5", *names[1:]]:
                stage(
                    "performance-" + Path(name).stem,
                    "check_reinforcement_baseline_browser.py",
                    ["--filename", name, "--release-gate"],
                )
            stage(
                "soak",
                "check_release_soak_browser.py",
                [
                    "--files",
                    *names,
                    "--duration-seconds",
                    str(args.soak_seconds),
                    "--server-pid",
                    str(server.pid),
                    "--native-memory",
                ],
            )
            assert hashes == {
                p.name: sha256(p) for p in data.glob("*.hdf5")
            }, "Native source bytes changed"
            assert sha256(jar) == evidence["jarSha256"], "Packaged JAR changed"
            evidence.update(
                status="passed",
                sourceHashes=hashes,
                qualifiesThirtyMinuteSoak=args.soak_seconds >= 1800,
            )
            save()
    except BaseException as error:
        evidence.update(status="failed", error=str(error))
        save()
        raise
    finally:
        if server and server.poll() is None:
            os.killpg(server.pid, signal.SIGTERM)
            try:
                server.wait(timeout=20)
            except subprocess.TimeoutExpired:
                os.killpg(server.pid, signal.SIGKILL)
                server.wait(timeout=10)


if __name__ == "__main__":
    main()
