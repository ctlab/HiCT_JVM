#!/usr/bin/env python3
"""Start a packaged HiCT launcher and verify the API and bundled WebUI."""

import argparse
import json
import os
import re
import signal
import socket
import subprocess
import tempfile
import time
from html.parser import HTMLParser
from pathlib import Path
from urllib.request import ProxyHandler, build_opener


class Entrypoints(HTMLParser):
    def __init__(self):
        super().__init__()
        self.scripts = []

    def handle_starttag(self, tag, attrs):
        if tag == "script":
            attributes = dict(attrs)
            source = attributes.get("src") or ""
            entrypoint = re.fullmatch(r"(?:/|\./)?(assets/[A-Za-z0-9_.-]+\.js)", source)
            if entrypoint:
                self.scripts.append("/" + entrypoint.group(1))


def stop(process):
    if process.poll() is not None:
        return
    if os.name == "nt":
        result = subprocess.run(
            ["taskkill", "/pid", str(process.pid), "/t", "/f"],
            capture_output=True, text=True, errors="replace", timeout=15,
        )
        if result.returncode and process.poll() is None:
            print(f"Windows launcher shutdown reported: {result.stderr.strip()}", flush=True)
    else:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
    try:
        process.wait(timeout=15)
    except subprocess.TimeoutExpired:
        if os.name == "nt":
            process.kill()
        else:
            os.killpg(process.pid, signal.SIGKILL)
        process.wait(timeout=15)


def clean_up(directory, retry_seconds):
    deadline = time.monotonic() + retry_seconds
    while True:
        try:
            directory.cleanup()
            return
        except PermissionError:
            if time.monotonic() >= deadline:
                raise
            # Terminated Windows children can retain native DLL handles briefly.
            time.sleep(0.2)


def check(launcher, expected_version, work):
    data = work / "data"
    data.mkdir()
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    base_url = f"http://127.0.0.1:{port}"
    env = dict(
        os.environ, DATA_DIR=str(data), PROCESSED_DIR=str(data / "processed"),
        HICT_PORTABLE_DATA_DIR=str(data), HICT_PORT=str(port),
        HICT_BIND_HOST="127.0.0.1", AUTO_OPEN_BROWSER="false", HICT_LAUNCHER_MODE="cli",
    )
    for key in ("WEBUI_ROOT", "HICT_APP_HOME", "HICT_TOOLCHAIN_DIR", "HICT_JAR_PATH",
                "HICT_JHDF5_NATIVES_ARCHIVE", "HICT_TEMP_DIR"):
        env.pop(key, None)
    command = [str(launcher), "start-server", "--serve-webui=true"]
    if os.name == "nt" and launcher.suffix.lower() == ".cmd":
        env["HICT_SMOKE_LAUNCHER"] = str(launcher)
        cmd = os.environ.get("ComSpec", "cmd.exe")
        # cmd.exe strips the outer quotes after /s /c. Popen's argument-list
        # conversion would escape the inner quotes for the C runtime instead.
        command = f'"{cmd}" /d /s /v:off /c ""%HICT_SMOKE_LAUNCHER%" start-server --serve-webui=true"'
    http = build_opener(ProxyHandler({}))
    log_path = work / "server.log"
    with log_path.open("w+b") as log:
        process = subprocess.Popen(
            command, cwd=data, env=env, stdout=log, stderr=subprocess.STDOUT,
            start_new_session=os.name != "nt",
        )
        try:
            deadline = time.monotonic() + 180
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    raise RuntimeError(f"Packaged launcher exited with status {process.returncode}")
                try:
                    with http.open(base_url + "/version", timeout=3) as response:
                        versions = json.load(response)
                    with http.open(base_url + "/", timeout=3) as response:
                        index = response.read().decode("utf-8")
                    break
                except OSError:
                    time.sleep(0.2)
            else:
                raise RuntimeError("Packaged API and WebUI did not become ready within 180 seconds")

            if versions["version"] != expected_version or not re.match(r"^\d+\.\d+\.\d+", versions["webuiVersion"]):
                raise RuntimeError(f"Unexpected packaged version information: {versions}")
            entrypoints = Entrypoints()
            entrypoints.feed(index)
            scripts = []
            for asset in entrypoints.scripts:
                with http.open(base_url + asset, timeout=10) as response:
                    scripts.append(response.read(16 << 20))
            if not scripts or any(not script.strip() for script in scripts):
                raise RuntimeError("Packaged index did not serve its JavaScript entrypoints")
            expected_webui_commit = env.get("HICT_WEBUI_REF", "")
            if re.fullmatch(r"[0-9a-f]{40}", expected_webui_commit) and not any(
                expected_webui_commit.encode() in script for script in scripts
            ):
                raise RuntimeError(f"Packaged JavaScript does not contain WebUI revision {expected_webui_commit}")
            print(f"Packaged API and WebUI started successfully: {launcher.name} {versions}")
        finally:
            stop(process)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("launcher", type=Path)
    parser.add_argument("version")
    args = parser.parse_args()
    directory = tempfile.TemporaryDirectory(prefix="hict-packaged-server-")
    work = Path(directory.name)
    try:
        check(args.launcher.resolve(strict=True), args.version, work)
    except Exception:
        log = work / "server.log"
        if log.exists():
            print(log.read_text(encoding="utf-8", errors="replace")[-16000:], flush=True)
        raise
    finally:
        clean_up(directory, retry_seconds=15 if os.name == "nt" else 0)


if __name__ == "__main__":
    main()
