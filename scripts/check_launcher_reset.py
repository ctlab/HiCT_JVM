#!/usr/bin/env python3
"""Run Launcher confirmation/reset acceptance on a private Xvfb display."""

import argparse
import os
from pathlib import Path
import select
import signal
import subprocess
import tempfile


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--xvfb", default="/tmp/hict-desktop-test/runtime/usr/bin/Xvfb")
    parser.add_argument("--jar", required=True)
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
    env.update(
        HICT_APP_HOME=str(out),
        DATA_DIR=str(out / "data"),
        PROCESSED_DIR=str(out / "processed"),
        HICT_LAUNCHER_CONFIG=str(out / "launcher.properties"),
        HICT_LOG_LEVEL="WARN",
        TILE_SIZE="512",
        VXPORT="5189",
        WEBUI_PORT="8189",
        HICT_TEST_PRIVATE_DISPLAY="1",
        HICT_JAR_PATH=str(Path(args.jar).resolve()),
        LD_LIBRARY_PATH="/tmp/hict-desktop-test/runtime/usr/lib/x86_64-linux-gnu",
    )
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
            env=env,
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
                    "java",
                    "-Duser.home=" + env["HOME"],
                    "-Djava.io.tmpdir=" + str(out),
                    "--class-path",
                    str(root / "build/classes/java/main")
                    + os.pathsep
                    + str(Path(args.jar).resolve()),
                    str(root / "scripts/LauncherResetAcceptance.java"),
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
