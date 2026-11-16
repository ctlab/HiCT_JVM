#!/usr/bin/env python3
"""Verify Info through unloaded resolutions, settings and map replacement on real Micro-C data."""

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
for key in ("jar", "webui", "source-dir", "output", "hdf-python"):
    parser.add_argument("--" + key, required=True)
parser.add_argument("--browser-debug", action="store_true")
args = parser.parse_args()
out = Path(args.output).absolute()
assert out == out.resolve() and out.is_relative_to("/tmp")
out.mkdir(exist_ok=False)
data = out / "data"
data.mkdir()
source = Path(args.source_dir).resolve()
for original, name in [
    ("yeast-wt3.hict.hdf5", "yeast.hict.hdf5"),
    ("yeast-wt3.mcool", "source.mcool"),
]:
    shutil.copyfile(source / original, data / name)


def digest(path):
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1048576), b""):
            h.update(block)
    return h.hexdigest()


hashes = {p.name: digest(p) for p in data.iterdir()}
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


class Quiet(http.server.SimpleHTTPRequestHandler):
    def log_message(self, *_):
        pass


web = http.server.ThreadingHTTPServer(
    ("127.0.0.1", 0),
    functools.partial(Quiet, directory=str(Path(args.webui).resolve())),
)
threading.Thread(target=web.serve_forever, daemon=True).start()
checks, probes, held = [], {}, []
try:
    with HictTestServer(jar, data, out / "server.log") as api:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(
                headless=True,
                args=["--remote-debugging-port=0"] if args.browser_debug else [],
            )
            if args.browser_debug:
                session = browser.new_browser_cdp_session()
                session.send("Target.setDiscoverTargets", {"discover": True})
                session.on(
                    "Target.targetCrashed",
                    lambda event: (out / "target-crash.json").write_text(
                        json.dumps(event)
                    ),
                )
                command = session.send("Browser.getBrowserCommandLine")["arguments"]
                profile = next(
                    a.split("=", 1)[1]
                    for a in command
                    if a.startswith("--user-data-dir=")
                )
                port = (
                    (Path(profile) / "DevToolsActivePort").read_text().splitlines()[0]
                )
                (out / "browser-debug.json").write_text(
                    json.dumps(
                        {
                            "endpoint": "http://127.0.0.1:" + port,
                            "version": session.send("Browser.getVersion"),
                        }
                    )
                )
            page = browser.new_page(viewport={"width": 1440, "height": 1000})

            def crashed():
                (out / "page-crash.json").write_text("{}")
                browser.close()

            page.on("crash", crashed)
            page.set_default_timeout(60000)
            page.add_init_script("window.__HICT_BENCHMARK_ENABLED__=true")
            page.route(
                "http://localhost:5000/**",
                lambda r: r.continue_(
                    url=api.url + r.request.url.removeprefix("http://localhost:5000")
                ),
            )
            check = BrowserCheck(page, out)
            overlay = page.locator(".ol-mouse-position")

            def record(name):
                page.screenshot(path=str(out / (name + ".png")))
                checks.append({"test": name, "status": "passed"})
                print("Passed", name, flush=True)

            def instrument():
                page.evaluate(
                    """()=>{const m=window.__hictBenchmark.manager(),c=m.getMap().getControls().getArray().find(c=>c.layersManager&&c.updateHTML_);window.__osd=c;window.__osdProbe={reads:0,times:[]};for(const l of c.layers){const f=l.getData.bind(l);l.getData=(...a)=>{window.__osdProbe.reads++;return f(...a)}}const f=c.updateHTML_.bind(c);c.probeCalls=0;c.updateHTML_=(...a)=>{const t=performance.now();c.probeCalls++;try{return f(...a)}finally{window.__osdProbe.times.push(performance.now()-t)}}}"""
                )

            def move():
                for i in range(25):
                    page.mouse.move(350 + i * 3, 350 + i * 2)

            def expect_current():
                expected = page.evaluate(
                    "()=>window.__hictBenchmark.manager().viewAndLayersManager.getFinestVisibleSourceResolutionDescriptor()"
                )
                text = (
                    (
                        "Base scale; native bins: "
                        + str(expected["sourceBpResolution"])
                        + " bp"
                    )
                    if expected.get("displayGrid") == "SEQUENCE_BP"
                    else "Bin resolution: 1:" + str(expected["bpResolution"])
                )
                expect(overlay).to_contain_text(text)
                expect(overlay).to_contain_text(
                    "Guidance source: " + expected.get("sourceName", "PRIMARY")
                )
                return expected

            try:
                page.goto(f"http://127.0.0.1:{web.server_port}")
                page.evaluate(
                    "api=>window.__hictBenchmark.open('yeast.hict.hdf5',api)", api.url
                )
                check.settle()
                instrument()
                move()
                probes["hidden"] = page.evaluate("window.__osdProbe")
                assert probes["hidden"]["reads"] == 0
                expect(overlay).to_have_text("")
                record("hidden-info-performs-no-tile-pixel-reads")
                page.locator("#toggle-osd-button").evaluate("e=>e.click()")
                expect_current()
                record(
                    "info-toggle-refreshes-a-stationary-pointer-without-a-map-render"
                )
                page.evaluate("window.__osdProbe={reads:0,times:[]}")
                move()
                expect_current()
                probes["visible"] = page.evaluate("window.__osdProbe")
                assert probes["visible"]["reads"] == 0
                record("visible-info-uses-metadata-without-tile-pixel-reads")
                page.route("**/get_tile**", lambda route: held.append(route))
                for index, delta in enumerate((3, -1, 2, -3)):
                    page.evaluate(
                        "d=>{const v=window.__hictBenchmark.manager().getMap().getView();v.setZoom(v.getZoom()+d)}",
                        delta,
                    )
                    expect_current()
                    probes["transition-" + str(index)] = {
                        "descriptor": expect_current(),
                        "text": overlay.inner_text(),
                        "heldTiles": len(held),
                    }
                    record(
                        "stationary-info-updates-during-delayed-resolution-"
                        + str(index)
                    )
                page.unroute("**/get_tile**")
                for route in held:
                    route.fallback()
                held.clear()
                check.settle()
                expect_current()
                record("loaded-tiles-do-not-replace-the-current-guidance-resolution")
                before = overlay.inner_text()
                page.evaluate(
                    "()=>{const v=window.__hictBenchmark.manager().getMap().getView(),c=v.getCenter(),r=v.getResolution();v.setCenter([c[0]+r*40,c[1]-r*30])}"
                )
                page.wait_for_function(
                    "text=>window.__osd.element.innerText!==text", arg=before
                )
                expect_current()
                record("stationary-pointer-position-updates-after-pan")
                page.locator("#toggle-osd-button").evaluate("e=>e.click()")
                expect(overlay).to_have_text("")
                page.locator("#toggle-osd-button").evaluate("e=>e.click()")
                expect_current()
                record("hiding-and-reshowing-info-keeps-the-stationary-pointer")
                check.menu("View", "Info settings…")
                dialog = page.get_by_role("dialog", name="On-screen display settings")
                dialog.locator("#osd-position-select").select_option("bottom-left")
                dialog.locator("#osd-field-resolution").uncheck()
                dialog.get_by_role("button", name="Done", exact=True).click()
                move()
                expect(overlay).not_to_contain_text("Bin resolution:")
                expect(overlay.locator("div")).to_have_css("bottom", "12px")
                record("info-position-and-field-settings-retain-the-existing-layout")
                check.menu("View", "Info settings…")
                dialog = page.get_by_role("dialog", name="On-screen display settings")
                dialog.locator("#osd-field-resolution").check()
                dialog.get_by_role("button", name="Done", exact=True).click()
                move()
                expect_current()
                page.evaluate(
                    """()=>{const c=window.__osd,h=c.getDimensionHolderForDescriptor(c.getGuidanceResolutionDescriptor({})),f=h.getContigNameByPx;try{h.getContigNameByPx=()=>'<em>literal&name</em>';c.updateHTML_(c.pointerPixel)}finally{h.getContigNameByPx=f}}"""
                )
                expect(overlay).to_contain_text("<em>literal&name</em>")
                expect(overlay.locator("em")).to_have_count(0)
                record("contig-names-render-as-literal-text")
                page.evaluate("window.__oldOsd=window.__osd")
                print("Opening the replacement map", flush=True)
                page.evaluate(
                    "api=>window.__hictBenchmark.open('tiny-primary.hict.hdf5',api)",
                    api.url,
                )
                check.settle()
                instrument()
                move()
                expect_current()
                expect(
                    page.get_by_text("excluded bins masked.", exact=False)
                ).to_have_count(0)
                count = page.evaluate("window.__oldOsd.probeCalls")
                page.locator("#toggle-osd-button").evaluate("e=>e.click()")
                expect(overlay).to_have_text("")
                page.locator("#toggle-osd-button").evaluate("e=>e.click()")
                expect_current()
                assert page.evaluate("window.__oldOsd.probeCalls") == count
                assert page.evaluate("window.__oldOsd.getMap()===null")
                record("replaced-map-controls-release-their-settings-watchers")
                blank = page.evaluate(
                    """()=>{const map=window.__hictBenchmark.manager().getMap(),e=map.getView().getProjection().getExtent(),p=map.getPixelFromCoordinate([e[2],(e[1]+e[3])/2]),r=map.getViewport().getBoundingClientRect();return [r.x+p[0]+10,r.y+p[1]]}"""
                )
                page.mouse.move(*blank)
                expect(overlay).not_to_contain_text("Position: bp1=")
                record("pointer-outside-the-assembly-does-not-show-clamped-coordinates")
                page.evaluate(
                    "async()=>{const m=window.__hictBenchmark.manager(),r=m.networkManager.requestManager,s=await r.openSecondarySource('tiny-fine.hict.hdf5');m.viewAndLayersManager.setSecondaryResolutionModel(s.compatibility);const color=source=>({type:'colormap',mode:'LINEAR',input:{type:'source',source},minSignal:0,maxSignal:11,startColor:'#ffffff00',endColor:'#ff0000ff'});await r.setRenderPipelineConfig({enabled:true,swapUpperLower:false,upperExpression:color('PRIMARY'),lowerExpression:color('SECONDARY')});await m.reloadTilesFromBackend();m.getMap().getView().setResolution(4)}"
                )
                check.settle()
                move()
                expect_current()
                expect(overlay).to_contain_text("Secondary 1:")
                record("mixed-native-resolutions-retain-correct-source-guidance")
                assert page.evaluate("window.__osdProbe.reads") == 0
                assert not check.errors, check.errors
                page.close()
                page = browser.new_page(viewport={"width": 1440, "height": 1000})
                page.on("crash", crashed)
                page.set_default_timeout(60000)
                page.add_init_script("window.__HICT_BENCHMARK_ENABLED__=true")
                page.route(
                    "http://localhost:5000/**",
                    lambda r: r.continue_(
                        url=api.url
                        + r.request.url.removeprefix("http://localhost:5000")
                    ),
                )
                check = BrowserCheck(page, out)
                page.goto(f"http://127.0.0.1:{web.server_port}")
                page.evaluate(
                    "api=>{window.__oldOpening=window.__hictBenchmark.open('yeast.hict.hdf5',api)}",
                    api.url,
                )
                page.wait_for_function("window.__hictBenchmark.manager()!==undefined")
                page.evaluate(
                    """()=>{const m=window.__hictBenchmark.manager(),v=m.visualizationManager,a=v.applyCoolerWeightsPolicy.bind(v),s=m.stabilizeInitialViewport.bind(m);v.applyCoolerWeightsPolicy=(...args)=>{if(args[0]==='REPLACE_NANS_WITH_ZERO')window.__maskInvoked=true;return a(...args)};m.stabilizeInitialViewport=async(...args)=>{const result=await s(...args);if(window.__maskInvoked&&!window.__maskGateUsed){window.__maskGateUsed=true;window.__maskWaiting=true;await new Promise(resolve=>window.__maskRelease=resolve)}return result}}"""
                )
                page.wait_for_function("window.__maskWaiting===true")
                page.evaluate(
                    "api=>window.__hictBenchmark.open('tiny-primary.hict.hdf5',api)",
                    api.url,
                )
                check.settle()
                page.evaluate(
                    "async()=>{window.__maskRelease();await window.__oldOpening;await new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve)))}"
                )
                expect(
                    page.get_by_text("excluded bins masked.", exact=False)
                ).to_have_count(0)
                assert (
                    page.evaluate(
                        "window.__hictBenchmark.manager().getOptions().filename"
                    )
                    == "tiny-primary.hict.hdf5"
                )
                assert not check.errors, check.errors
                record("late-excluded-bin-guidance-cannot-target-a-replacement-map")
                (out / "results.json").write_text(
                    json.dumps(
                        {"checks": checks, "probes": probes, "errors": check.errors},
                        indent=2,
                    )
                )
            except BaseException:
                if not page.is_closed():
                    page.screenshot(path=str(out / "failure.png"), timeout=5000)
                    (out / "failure.html").write_text(page.content())
                (out / "errors.json").write_text(json.dumps(check.errors))
                raise
            finally:
                browser.close()
        api.http("/open", {"filename": "yeast.hict.hdf5"})
        with (out / "diagonal.log").open("w") as log:
            subprocess.run(
                [
                    args.hdf_python,
                    str(Path(__file__).with_name("check_microc_diagonal.py")),
                    "--source",
                    str(data / "source.mcool"),
                    "--api",
                    api.url,
                    "--output",
                    str(out / "diagonal.json"),
                ],
                check=True,
                stdout=log,
                stderr=subprocess.STDOUT,
            )
        assert hashes == {name: digest(data / name) for name in hashes}
        (out / "source-hashes.json").write_text(json.dumps(hashes, indent=2))
finally:
    web.shutdown()
    web.server_close()
