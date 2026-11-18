#!/usr/bin/env python3
"""Verify border reuse preserves pixels, assembly state and actual style controls."""

import argparse
import functools
import hashlib
import http.server
import json
from pathlib import Path
import shutil
import threading

from PIL import Image, ImageChops
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
    rows, errors = [], []
    try:
        with HictTestServer(args.jar, data, out / "server.log") as api:
            with sync_playwright() as pw:
                browser = pw.chromium.launch(headless=True)
                page = browser.new_page(viewport={"width": 1440, "height": 1000})
                page.set_default_timeout(60000)
                page.add_init_script("window.__HICT_BENCHMARK_ENABLED__=true")
                page.route(
                    "http://localhost:5000/**",
                    lambda r: r.continue_(
                        url=api.url
                        + r.request.url.removeprefix("http://localhost:5000")
                    ),
                )
                page.on("pageerror", lambda e: errors.append(str(e)))
                page.on(
                    "console",
                    lambda e: errors.append(e.text) if e.type == "error" else None,
                )
                page.on(
                    "response",
                    lambda r: (
                        errors.append(f"{r.status} {r.url}")
                        if r.status >= 400
                        else None
                    ),
                )
                check = BrowserCheck(page, out)
                try:
                    page.goto(f"http://127.0.0.1:{web.server_port}/")
                    page.evaluate(
                        "api=>window.__hictBenchmark.open('map.hict.hdf5',api)", api.url
                    )
                    check.settle()
                    page.evaluate("""()=>{
                        const m=window.__hictBenchmark.manager(),l=m.viewAndLayersManager;
                        l.rebuildVectorViewport();
                        window.__borderProbe={manager:m,layers:l,events:[],previous:[]};
                        const p=window.__borderProbe;
                        p.borders=()=>[l.track2DHolder.contigBordersTrack,l.track2DHolder.scaffoldBordersTrack]
                          .flatMap(t=>t.features.get(l.currentViewState.resolutionDesciptor.bpResolution)??[])
                          .filter(f=>['contigBorders','scaffoldBorders'].includes(f.get('trackType')));
                        p.previous=p.borders();
                        p.geometry=p.previous.map(f=>f.getGeometry().getCoordinates());
                        for(const layer of [...l.layersHolder.contigBordersLayers,...l.layersHolder.scaffoldBordersLayers]){
                          layer.getSource().on(['addfeature','removefeature'],e=>{
                            if(['contigBorders','scaffoldBorders'].includes(e.feature?.get('trackType')))p.events.push(e.type);
                          });
                        }
                    }""")
                    check.settle()
                    reference = page.evaluate(
                        "async()=> (await window.__borderProbe.manager.networkManager.requestManager.getSessionSnapshot(true)).primary.agpSha256"
                    )
                    clip = page.evaluate(
                        "window.__borderProbe.manager.getMap().getViewport().getBoundingClientRect().toJSON()"
                    )
                    clip = {k: clip[k] for k in ("x", "y", "width", "height")}
                    page.screenshot(path=str(out / "before.png"), clip=clip)
                    for index in range(3):
                        row = page.evaluate("""()=>{
                            const p=window.__borderProbe;p.layers.rebuildVectorViewport();
                            const next=p.borders();
                            return {count:next.length,reused:next.every((f,i)=>f===p.previous[i]),
                              geometryUnchanged:JSON.stringify(next.map(f=>f.getGeometry().getCoordinates()))===JSON.stringify(p.geometry),
                              borderEvents:p.events.slice()};
                        }""")
                        assert (
                            row["count"] > 0
                            and row["reused"]
                            and row["geometryUnchanged"]
                        ), row
                        assert not row["borderEvents"], row
                        check.settle()
                        page.screenshot(
                            path=str(out / f"rebuild-{index}.png"), clip=clip
                        )
                        with Image.open(out / "before.png") as a, Image.open(
                            out / f"rebuild-{index}.png"
                        ) as b:
                            assert (
                                ImageChops.difference(
                                    a.convert("RGB"), b.convert("RGB")
                                ).getbbox()
                                is None
                            )
                        rows.append(
                            {
                                "case": f"rebuild-{index}",
                                **row,
                                "mapPixelsIdentical": True,
                            }
                        )
                    page.get_by_role(
                        "button", name="Borders and labels", exact=True
                    ).click()
                    record = page.locator(".layer-record").filter(
                        has=page.locator('.layer-name[title="Contig borders"]')
                    )
                    initial_shape = page.evaluate(
                        "window.__borderProbe.layers.track2DHolder.contigBordersTrack.getStyleType()"
                    )
                    for step in (1, 2, 3):
                        shape = (initial_shape + step) % 3
                        record.locator(".border-style-btn").click()
                        check.settle()
                        row = page.evaluate("""()=>{
                            const p=window.__borderProbe,next=p.borders();
                            const previous=new Set(p.previous);
                            const result={shape:p.layers.track2DHolder.contigBordersTrack.getStyleType(),
                              contigs:next.filter(f=>f.get('trackType')==='contigBorders').length,
                              staleContigs:next.filter(f=>f.get('trackType')==='contigBorders'&&previous.has(f)).length};
                            p.previous=next;return result;
                        }""")
                        assert (
                            row["shape"] == shape
                            and row["contigs"] > 0
                            and not row["staleContigs"]
                        ), row
                        page.screenshot(path=str(out / f"style-{shape}.png"))
                        rows.append({"case": f"style-{shape}", **row})
                    assert reference == page.evaluate(
                        "async()=> (await window.__borderProbe.manager.networkManager.requestManager.getSessionSnapshot(true)).primary.agpSha256"
                    )
                    assert hashlib.sha256(native.read_bytes()).hexdigest() == digest
                    assert not errors, errors
                finally:
                    (out / "results.json").write_text(
                        json.dumps(
                            {"cases": rows, "errors": errors, "nativeSha256": digest},
                            indent=2,
                        )
                        + "\n"
                    )
                    browser.close()
    finally:
        web.shutdown()
        web.server_close()
    print(json.dumps({"cases": len(rows), "output": str(out)}))


if __name__ == "__main__":
    main()
