#!/usr/bin/env python3
"""Prepare disposable fixtures and run browser regressions against one built JAR and WebUI."""
import argparse
import functools
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import threading

from check_structural_edits_browser import prepare_fixture
from sequence_browser_harness import HictTestServer

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"


class QuietHandler(SimpleHTTPRequestHandler):
    def log_message(self, *_):
        pass


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ("jar", "webui", "output"):
        parser.add_argument("--" + key, required=True, type=Path)
    parser.add_argument("--repeat", type=int, default=1)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    checks = {
        "check_menu_surfaces_browser.py": ["--file", "structural.hict.hdf5"],
        "check_structural_edits_browser.py": [],
        "check_ui_crawl_browser.py": ["--file", "structural.hict.hdf5", "--track", "dense.bed"],
        "check_export_guidance_browser.py": ["--file", "structural.hict.hdf5"],
        "check_error_feedback_browser.py": ["--file", "structural.hict.hdf5"],
        "check_sqrt_zoom_fit_browser.py": ["--files", "structural.hict.hdf5"],
    }
    attached = {
        "check_color_midpoint_browser.py": True,
        "check_figure_print_size_browser.py": False,
        "check_visualization_transactions_browser.py": True,
        "check_tracks_workspace_browser.py": True,
        "check_responsive_workspace_browser.py": True,
        "check_expected_figure_browser.py": False,
    }
    failures = []
    with tempfile.TemporaryDirectory(prefix="hict-browser-checks-", dir="/tmp") as scratch:
        scratch = Path(scratch)
        fixtures = scratch / "fixtures"
        fixtures.mkdir()
        subprocess.run(["java", "--class-path", str(args.jar.resolve()),
            str(ROOT / "src/test/java/ru/itmo/ctlab/hict/fixtures/ReinforcementHdf5Fixture.java"),
            str(fixtures)], check=True)
        prepare_fixture(fixtures, args.jar)
        (fixtures / "dense.bed").write_text("".join(
            f"one\t0\t400000\tFeature-{i}\t42\t+\n" for i in range(90)))
        (fixtures / "invalid.agp").write_text("# malformed\none\t1\twrong\n")
        handler = functools.partial(QuietHandler, directory=args.webui.resolve())
        with ThreadingHTTPServer(("127.0.0.1", 0), handler) as web:
            thread = threading.Thread(target=web.serve_forever, daemon=True)
            thread.start()
            try:
                for run in range(1, args.repeat + 1):
                    for name, options in checks.items():
                        out = args.output.resolve() / f"run-{run}" / Path(name).stem
                        out.mkdir(parents=True)
                        data = scratch / "data"
                        shutil.copytree(fixtures, data)
                        try:
                            with HictTestServer(args.jar, data, out / "server.log") as server:
                                command = [sys.executable, str(SCRIPTS / name),
                                    "--url", f"http://127.0.0.1:{web.server_port}",
                                    "--api", server.url, "--output", str(out / "screens"), *options]
                                if not run_check(command, out):
                                    failures.append(f"{run}: {name}")
                        finally:
                            shutil.rmtree(data)
                    for name, separate_data in attached.items():
                        out = args.output.resolve() / f"run-{run}" / Path(name).stem
                        out.mkdir(parents=True)
                        command = [sys.executable, str(SCRIPTS / name), "--jar", str(args.jar.resolve()),
                            "--webui", str(args.webui.resolve())]
                        if separate_data:
                            command += ["--data-dir", str(scratch / Path(name).stem)]
                        command += ["--output", str(scratch / (Path(name).stem + "-results"))]
                        if not run_check(command, out):
                            failures.append(f"{run}: {name}")
                        results = Path(command[-1])
                        if results.is_dir():
                            shutil.copytree(results, out / "screens")
                            shutil.rmtree(results)
                        shutil.rmtree(scratch / Path(name).stem, ignore_errors=True)
            finally:
                web.shutdown()
                thread.join()
    if failures:
        sys.exit("Browser checks failed:\n" + "\n".join(failures))


def run_check(command, out):
    with (out / "run.log").open("w") as log:
        try:
            result = subprocess.run(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, timeout=900)
        except subprocess.TimeoutExpired:
            print(f"TIMEOUT {out.name}", flush=True)
            return False
    print(f"{'PASS' if result.returncode == 0 else 'FAIL'} {out.name}", flush=True)
    return result.returncode == 0


if __name__ == "__main__":
    main()
