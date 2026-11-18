#!/usr/bin/env python3
"""Run Launcher confirmation/reset acceptance on a private Xvfb display."""

import argparse
import os
from pathlib import Path
import select
import shutil
import signal
import socket
import subprocess
import tempfile


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--xvfb", default=shutil.which("Xvfb") or "/tmp/hict-desktop-test/runtime/usr/bin/Xvfb")
    parser.add_argument("--jar", required=True)
    parser.add_argument("--java", default="java", help="Java runtime used to launch the acceptance test")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    out = Path(tempfile.mkdtemp(prefix="hict-launcher-reset-"))
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("HICT_", "JAVA_", "JDK_", "_JAVA"))
    }
    for key in ("HOME", "XDG_CONFIG_HOME", "XDG_CACHE_HOME", "XDG_RUNTIME_DIR"):
        directory = out / key.lower()
        directory.mkdir(mode=0o700)
        env[key] = str(directory)
    with socket.socket() as api, socket.socket() as webui:
        api.bind(("127.0.0.1", 0))
        webui.bind(("127.0.0.1", 0))
        env.update(VXPORT=str(api.getsockname()[1]), WEBUI_PORT=str(webui.getsockname()[1]))
    env.update(
        HICT_APP_HOME=str(out),
        DATA_DIR=str(out / "data"),
        PROCESSED_DIR=str(out / "processed"),
        HICT_LAUNCHER_CONFIG=str(out / "launcher.properties"),
        HICT_LOG_LEVEL="WARN",
        TILE_SIZE="512",
        HICT_TEST_PRIVATE_DISPLAY="1",
        HICT_JAR_PATH=str(Path(args.jar).resolve(strict=True)),
    )
    xvfb_env = env.copy()
    libraries = Path(args.xvfb).resolve().parent.parent / "lib/x86_64-linux-gnu"
    if libraries.is_dir():
        xvfb_env["LD_LIBRARY_PATH"] = str(libraries)
    subprocess.run(["javac", "-d", str(out), str(root / "scripts/LauncherResetAcceptance.java")], check=True)
    read_fd, write_fd = os.pipe()
    print(f"Launcher acceptance output: {out}", flush=True)
    with (out / "xvfb.log").open("w") as log:
        xvfb = subprocess.Popen(
            [
                args.xvfb,
                "-displayfd",
                str(write_fd),
                "-screen",
                "0",
                "1600x1100x24",
                "-nolisten",
                "tcp",
            ],
            env=xvfb_env,
            pass_fds=(write_fd,),
            stdout=log,
            stderr=log,
        )
        os.close(write_fd)
        try:
            if not select.select([read_fd], [], [], 15)[0]:
                raise RuntimeError("Xvfb did not allocate a private display")
            display = os.read(read_fd, 32).decode().strip()
            if not display.isdecimal():
                raise RuntimeError("Xvfb exited before allocating a display")
            env["DISPLAY"] = ":" + display
            process = subprocess.Popen(
                [
                    args.java,
                    "-Duser.home=" + env["HOME"],
                    "-Djava.io.tmpdir=" + str(out),
                    "--class-path",
                    os.pathsep.join((str(out), env["HICT_JAR_PATH"])),
                    "LauncherResetAcceptance",
                    str(out),
                ],
                env=env,
                cwd=out,
                start_new_session=True,
            )
            try:
                if process.wait(timeout=150) != 0:
                    raise RuntimeError("Launcher acceptance failed")
            finally:
                try:
                    os.killpg(process.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait()
        finally:
            os.close(read_fd)
            xvfb.terminate()
            try:
                xvfb.wait(timeout=10)
            except subprocess.TimeoutExpired:
                xvfb.kill()
                xvfb.wait()


if __name__ == "__main__":
    main()
