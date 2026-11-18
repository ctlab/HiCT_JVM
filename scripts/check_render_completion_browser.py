#!/usr/bin/env python3
"""Exercise the real OpenLayers obsolete-queue race without changing source pixels."""

import argparse
import functools
import hashlib
import http.server
import json
from pathlib import Path
import shutil
import threading
from playwright.sync_api import sync_playwright
from sequence_browser_harness import BrowserCheck, HictTestServer


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ("jar", "webui", "source-map", "output"):
        parser.add_argument("--" + key, required=True)
    args = parser.parse_args()
    out = Path(args.output).absolute()
    assert out == out.resolve() and out.is_relative_to("/tmp")
    out.mkdir(exist_ok=False)
    data = out / "data"
    data.mkdir()
    native = data / "map.hict.hdf5"
    shutil.copyfile(args.source_map, native)
    digest = hashlib.sha256(native.read_bytes()).hexdigest()

    class Quiet(http.server.SimpleHTTPRequestHandler):
        def log_message(self, *_):
            pass

    web = http.server.ThreadingHTTPServer(
        ("127.0.0.1", 0),
        functools.partial(Quiet, directory=str(Path(args.webui).resolve())),
    )
    threading.Thread(target=web.serve_forever, daemon=True).start()
    try:
        with HictTestServer(
            args.jar, data, out / "server.log"
        ) as api, sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            page = browser.new_page(viewport={"width": 1440, "height": 1000})
            page.add_init_script("window.__HICT_BENCHMARK_ENABLED__=true")
            page.route(
                "http://localhost:5000/**",
                lambda r: r.continue_(
                    url=api.url + r.request.url.removeprefix("http://localhost:5000")
                ),
            )
            errors = []
            page.on("pageerror", lambda error: errors.append(str(error)))
            page.on(
                "response",
                lambda r: (
                    errors.append(f"{r.status} {r.url}") if r.status >= 400 else None
                ),
            )
            rows = []

            def ready(timeout=30000):
                # Deliberately passive: no test-driven render()/renderSync().
                page.wait_for_function(
                    """()=>{const m=window.__hictBenchmark.manager(),map=m.getMap(),v=m.getView();return !v.getAnimating()&&!v.getInteracting()&&map.tileQueue_.getCount()===0&&map.tileQueue_.getTilesLoading()===0&&map.renderComplete_}""",
                    timeout=timeout,
                )

            def inject():
                return page.evaluate("""()=>{
                    const map=window.__hictBenchmark.manager().getMap();
                    const source=window.__hictBenchmark.manager().viewAndLayersManager.layersHolder.primaryHiCDataLayers.find(l=>l.getVisible()).getSource();
                    let exemplar;source.tileCache.forEach(t=>{if(t.getState()===2)exemplar=t});
                    if(!exemplar)throw Error('A decoded real tile is required');
                    const marker='hict-obsolete-probe-'+performance.now();
                    const tile=new exemplar.constructor([0,0,0],0,marker,null,()=>{throw Error('An obsolete tile must never load')},{transition:0});
                    const result=window.__completionProbe={start:performance.now(),marker};
                    map.once('postrender',()=>{
                        // Enqueue while wanted, then invalidate before handlePostRender.
                        // Map computes readiness before reprioritize drops this entry.
                        const wanted=map.frameState_.wantedTiles;
                        wanted[marker]={[tile.getKey()]:true};
                        result.enqueued=map.tileQueue_.enqueue([tile,marker,map.getView().getCenter().slice(),map.getView().getResolution()]);
                        delete wanted[marker];
                    });
                    map.renderSync();
                    result.initialComplete=map.renderComplete_;
                    return result;
                }""")

            try:
                page.goto(f"http://127.0.0.1:{web.server_port}/")
                page.evaluate(
                    "api=>window.__hictBenchmark.open('map.hict.hdf5',api)", api.url
                )
                BrowserCheck(page, out).settle()  # Initial setup may request frames.
                page.wait_for_timeout(600)
                reference = page.evaluate(
                    """async()=>{const m=window.__hictBenchmark.manager();return {agp:(await m.networkManager.requestManager.getSessionSnapshot(true)).primary.agpSha256,view:[m.getView().getCenter(),m.getView().getResolution()]}}"""
                )
                for i in range(6):
                    probe = inject()
                    assert (
                        probe["enqueued"] and probe["initialComplete"] is False
                    ), probe
                    ready(timeout=1500)
                    elapsed = page.evaluate(
                        "performance.now()-window.__completionProbe.start"
                    )
                    current = page.evaluate(
                        """async()=>{const m=window.__hictBenchmark.manager();return {agp:(await m.networkManager.requestManager.getSessionSnapshot(true)).primary.agpSha256,view:[m.getView().getCenter(),m.getView().getResolution()]}}"""
                    )
                    assert current == reference
                    rows.append(
                        {
                            "test": f"obsolete-queue-{i}-settles-without-test-driven-frames",
                            "elapsedMs": elapsed,
                            "status": "passed",
                        }
                    )
                    print("Passed", rows[-1]["test"], flush=True)
                page.screenshot(path=str(out / "recovered-map.png"))
                # Track only intervals born during the injected loading period.
                page.evaluate(
                    """()=>{window.__completionIntervals=new Set();window.__originalSetInterval=window.setInterval;window.__originalClearInterval=window.clearInterval;window.setInterval=(f,t,...a)=>{const id=window.__originalSetInterval(f,t,...a);window.__completionIntervals.add(id);return id};window.clearInterval=id=>{window.__completionIntervals.delete(id);window.__originalClearInterval(id)}}"""
                )
                inject()
                page.wait_for_function("window.__completionIntervals.size>0")
                page.evaluate("window.__hictBenchmark.manager().dispose()")
                assert page.evaluate("window.__completionIntervals.size") == 0
                page.evaluate(
                    "window.setInterval=window.__originalSetInterval;window.clearInterval=window.__originalClearInterval"
                )
                rows.append(
                    {
                        "test": "dispose-during-load-releases-completion-interval",
                        "status": "passed",
                    }
                )
                assert hashlib.sha256(native.read_bytes()).hexdigest() == digest
                assert not errors, errors
                (out / "results.json").write_text(
                    json.dumps(
                        {"checks": rows, "errors": errors, "sourceSha256": digest},
                        indent=2,
                    )
                )
            finally:
                browser.close()
    finally:
        web.shutdown()
        web.server_close()


if __name__ == "__main__":
    main()
