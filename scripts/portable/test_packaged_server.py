#!/usr/bin/env python3
"""Exercise the packaged server verifier through the Windows batch launcher."""

import http.server
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from unittest.mock import Mock

from check_packaged_server import clean_up


REVISION = "a" * 40


def serve():
    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            pages = {
                "/version": json.dumps({"version": "2.0.0-test", "webuiVersion": "2.0.0"}),
                "/": '<script type="module" src="/assets/index.js"></script>',
                "/assets/index.js": f'const revision = "{REVISION}";',
            }
            body = pages.get(self.path)
            if body is None:
                self.send_error(404)
                return
            payload = body.encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    with http.server.HTTPServer(("127.0.0.1", int(os.environ["HICT_PORT"])), Handler) as server:
        server.serve_forever()


def main():
    if sys.argv[1:] == ["start-server", "--serve-webui=true"]:
        serve()
        return
    directory = Mock()
    directory.cleanup.side_effect = [PermissionError("held DLL"), None]
    clean_up(directory, retry_seconds=1)
    assert directory.cleanup.call_count == 2

    directory.cleanup.side_effect = PermissionError("persistent lock")
    try:
        clean_up(directory, retry_seconds=0)
    except PermissionError:
        pass
    else:
        raise AssertionError("A persistent cleanup failure must fail the verifier")


    verifier = Path(__file__).with_name("check_packaged_server.py").resolve()
    with tempfile.TemporaryDirectory(prefix="HiCT smoke & test ") as work:
        launcher = Path(work) / ("run.cmd" if os.name == "nt" else "run.sh")
        if os.name == 'nt':
            launcher.write_text('@echo off\n"%HICT_TEST_PYTHON%" "%HICT_TEST_FIXTURE%" %*\n', encoding="ascii")
        else:
            launcher.write_text('#!/bin/sh\nexec "$HICT_TEST_PYTHON" "$HICT_TEST_FIXTURE" "$@"\n')
            launcher.chmod(0o755)
        env = dict(os.environ, HICT_TEST_PYTHON=sys.executable, HICT_TEST_FIXTURE=str(Path(__file__).resolve()),
                   HICT_WEBUI_REF=REVISION)
        subprocess.run([sys.executable, str(verifier), str(launcher), "2.0.0-test"],
                       env=env, check=True, timeout=35)
        mismatch = subprocess.run([sys.executable, str(verifier), str(launcher), "2.0.0-test"],
            env={**env, "HICT_WEBUI_REF": "b" * 40}, capture_output=True, text=True, timeout=35)
        assert mismatch.returncode and "Packaged JavaScript does not contain WebUI revision" in mismatch.stderr



if __name__ == "__main__":
    main()
