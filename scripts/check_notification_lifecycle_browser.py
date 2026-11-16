#!/usr/bin/env python3
"""Regress the masked-notice map-switch renderer crash with a tiny real NaN-weighted map."""

import argparse
import functools
import hashlib
import http.server
import json
from pathlib import Path
import shutil
import subprocess
import threading
from playwright.sync_api import expect, sync_playwright
from sequence_browser_harness import BrowserCheck, HictTestServer

parser = argparse.ArgumentParser(description=__doc__)
for key in ("jar", "webui", "output", "hdf-python"):
    parser.add_argument("--" + key, required=True)
args = parser.parse_args()
out = Path(args.output).absolute()
assert out == out.resolve() and out.is_relative_to("/tmp")
out.mkdir(exist_ok=False)
data = out / "data"
data.mkdir()
jar = Path(args.jar).resolve()
with (out / "fixture.log").open("w") as log:
    subprocess.run(
        [
            "java",
            "--class-path",
            str(jar),
            str(
                Path(__file__).resolve().parents[1]
                / "src/test/java/ru/itmo/ctlab/hict/fixtures/ReinforcementHdf5Fixture.java"
            ),
            str(data),
        ],
        check=True,
        stdout=log,
        stderr=subprocess.STDOUT,
    )
    shutil.copyfile(data / "tiny-primary.hict.hdf5", data / "tiny-nan.hict.hdf5")
    subprocess.run(
        [
            args.hdf_python,
            "-c",
            "import h5py,sys; f=h5py.File(sys.argv[1],'r+'); f['resolutions/4'].attrs['hict_imported_weights']='PRESENT'; f['resolutions/4/stripes/stripes_bin_weights'][0,:5]=[float('nan'),1,1,float('nan'),1]; f.close()",
            str(data / "tiny-nan.hict.hdf5"),
        ],
        check=True,
        stdout=log,
        stderr=subprocess.STDOUT,
    )
hashes = {
    p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in data.glob("*.hict.hdf5")
}


class Quiet(http.server.SimpleHTTPRequestHandler):
    def log_message(self, *_):
        pass


web = http.server.ThreadingHTTPServer(
    ("127.0.0.1", 0),
    functools.partial(Quiet, directory=str(Path(args.webui).resolve())),
)
threading.Thread(target=web.serve_forever, daemon=True).start()
try:
    with HictTestServer(jar, data, out / "server.log") as api, sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1440, "height": 1000})
        page.add_init_script("window.__HICT_BENCHMARK_ENABLED__=true")
        page.route(
            "http://localhost:5000/**",
            lambda route: route.continue_(
                url=api.url + route.request.url.removeprefix("http://localhost:5000")
            ),
        )
        check = BrowserCheck(page, out)

        def crashed():
            (out / "crash.json").write_text("{}")
            browser.close()

        page.on("crash", crashed)
        try:
            page.goto(f"http://127.0.0.1:{web.server_port}")
            page.evaluate(
                "api=>window.__hictBenchmark.open('tiny-nan.hict.hdf5',api)", api.url
            )
            check.settle()
            notice = page.locator("[data-sonner-toast]").filter(
                has_text="2 excluded bins masked."
            )
            expect(notice).to_have_count(1)
            expect(
                notice.get_by_role("button", name="Change", exact=True)
            ).to_be_visible()
            page.locator("#toggle-osd-button").click()
            page.mouse.move(500, 450)
            page.screenshot(path=str(out / "before-switch.png"))
            page.evaluate(
                "async api=>await Promise.race([window.__hictBenchmark.open('tiny-fine.hict.hdf5',api),new Promise((_,reject)=>setTimeout(()=>reject(new Error('Map replacement did not finish')),15000))])",
                api.url,
            )
            check.settle()
            expect(notice).to_have_count(0)
            assert (
                page.evaluate("window.__hictBenchmark.manager().getOptions().filename")
                == "tiny-fine.hict.hdf5"
            )
            assert not check.errors, check.errors
            page.screenshot(path=str(out / "after-switch.png"))
            assert hashes == {
                name: hashlib.sha256((data / name).read_bytes()).hexdigest()
                for name in hashes
            }
            (out / "results.json").write_text(
                json.dumps(
                    {
                        "checks": [
                            {
                                "test": "programmatic-masked-notice-dismissal-keeps-the-renderer-alive-and-the-new-map-current",
                                "status": "passed",
                            }
                        ],
                        "sourceHashes": hashes,
                        "errors": check.errors,
                    },
                    indent=2,
                )
            )
            print("Passed tiny real masked-notice map-switch regression", flush=True)
        finally:
            browser.close()
finally:
    web.shutdown()
    web.server_close()
