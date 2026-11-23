#!/usr/bin/env python3
"""Exercise packaged navigation, edits, sequence, layers and exports for 30 minutes."""

import argparse
import json
import re
import subprocess
import statistics
import time
from pathlib import Path

import numpy as np
from playwright.sync_api import expect, sync_playwright
from check_reinforcement_baseline_browser import percentile
from sequence_browser_harness import install_download_picker


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("url", "api", "output"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--files", nargs="+", required=True)
    parser.add_argument("--duration-seconds", type=int, default=1800)
    parser.add_argument(
        "--server-pid",
        type=int,
        help="Owned Java server process for heap and RSS samples",
    )
    parser.add_argument(
        "--native-memory",
        action="store_true",
        help="Record JVM native-memory categories when tracking is enabled",
    )
    args = parser.parse_args()
    if args.native_memory and not args.server_pid:
        parser.error("--native-memory requires --server-pid")
    if args.duration_seconds <= 0:
        parser.error("--duration-seconds must be positive")
    if args.duration_seconds >= 1800 and not args.server_pid:
        parser.error("The full release soak requires --server-pid")
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    samples, errors, acknowledgements, frames = [], [], [], []
    started = time.monotonic()
    result = {
        "files": args.files,
        "durationRequestedSeconds": args.duration_seconds,
        "method": "One browser context, normal JIT, repeated GC after close, console handles discarded before sampling",
    }
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        context = browser.new_context(viewport={"width": 1440, "height": 1000})
        context.add_init_script("window.__HICT_BENCHMARK_ENABLED__=true")
        context.add_init_script(
            """window.addEventListener('unhandledrejection', event => {
            console.error('Unhandled promise in release soak', event.reason);
        });"""
        )
        context.route(
            "http://localhost:5000/**",
            lambda route: route.continue_(
                url=args.api + route.request.url.removeprefix("http://localhost:5000")
            ),
        )
        page = context.new_page()
        install_download_picker(page)
        page.set_default_timeout(60000)
        page.on("pageerror", lambda error: errors.append(str(error)))
        page.on(
            "console",
            lambda message: (
                errors.append(message.text) if message.type == "error" else None
            ),
        )
        cdp = context.new_cdp_session(page)
        cdp.send("Performance.enable")
        page.goto(args.url)
        page.wait_for_function("window.__hictBenchmark")
        cycle = 0
        overlay_file = None
        warm_opens = []
        try:
            while time.monotonic() - started < args.duration_seconds:
                filename = args.files[cycle % len(args.files)]
                sequence = "mixed.fa" if filename == "mixed-primary.hict.hdf5" else None
                page.evaluate(
                    "async args => {await window.__hictBenchmark.open(args.filename,args.api,args.sequence);}",
                    {"filename": filename, "api": args.api, "sequence": sequence},
                )
                page.locator(".open-progress-modal").wait_for(state="hidden")
                page.evaluate("""() => {
                    const m=window.__hictBenchmark.manager();
                    m.getMap().on('rendercomplete',()=>{});
                    m.viewAndLayersManager.fitViewToActiveExtent();
                }""")
                if sequence:
                    page.evaluate("""async () => {
                        const m=window.__hictBenchmark.manager(),l=m.viewAndLayersManager;
                        const color={type:'colormap',mode:'LINEAR',input:{type:'source',source:'PRIMARY'},
                            minSignal:0,maxSignal:1100,startColor:'#ffffffff',endColor:'#ff0000ff'};
                        await m.networkManager.requestManager.setRenderPipelineConfig({enabled:true,
                            swapUpperLower:false,upperExpression:color,lowerExpression:color});
                        await m.reloadTilesFromBackend();
                        const p=l.getPixelResolutionForBpResolution(4),grid=m.contigDimensionHolder;
                        m.getView().setResolution(p/4);
                        m.getView().setCenter([grid.getFractionalPxOfBp(962,4)*p,-grid.getFractionalPxOfBp(1002,4)*p]);
                        m.getMap().renderSync();
                    }""")
                page.wait_for_function(
                    """knownPoint => {
                    const m=window.__hictBenchmark.manager(),map=m.getMap(),view=map.getView();
                    if (knownPoint) {
                        const l=m.viewAndLayersManager,d=l.getVisibleSourceResolutionDescriptors().primary;
                        const h=m.contigDimensionHolder;
                        const pixel=map.getPixelFromCoordinate([
                            h.getFractionalPxOfBp(962,d.bpResolution)*d.pixelResolution,
                            -h.getFractionalPxOfBp(1002,d.bpResolution)*d.pixelResolution]);
                        return l.layersHolder.primaryHiCDataLayers.some(layer=>{
                            if (!layer.getVisible()) return false;
                            const rgba=layer.getData(pixel);
                            return rgba?.[3]>0 && rgba[0]>rgba[1]+20;
                        });
                    }
                    const [left,bottom,right,top]=view.getProjection().getExtent();
                    return Array.from({length:31},(_,i)=>(i+1)/32).some(f=>{
                        const pixel=map.getPixelFromCoordinate([left+(right-left)*f,top+(bottom-top)*f]);
                        return m.viewAndLayersManager.layersHolder.primaryHiCDataLayers.some(layer=>{
                            if(!layer.getVisible())return false;
                            const rgba=layer.getData(pixel);
                            return rgba?.length>=4 && rgba[3]>0 && Math.min(...rgba.slice(0,3))<240;
                        });
                    });
                }""",
                    arg=bool(sequence),
                )
                if sequence:
                    if overlay_file is None:
                        cold_start = time.monotonic()
                        response = page.request.post(args.api + "/sequence/dotplot/jobs", data={
                            "scope":"selection", "region":{"rowStart":900,"rowEnd":1100,"columnStart":900,"columnEnd":1100},
                            "mergeCompatible":False, "resolutions":[1], "sampleBp":1, "threads":1,
                        })
                        assert response.ok, response.text()
                        job_id = response.json()["jobId"]
                        while time.monotonic() - cold_start < 180:
                            job = next(j for j in page.request.post(args.api + "/sequence/jobs/list", data={}).json() if j["jobId"] == job_id)
                            if job["status"] == "finished":
                                overlay_file = job["outputFile"]
                                break
                            assert job["status"] not in ("failed", "cancelled"), job
                            time.sleep(0.25)
                        assert overlay_file, "Cold generation timed out"
                        result["coldPyramidSeconds"] = time.monotonic() - cold_start
                        result["fineOverlayFile"] = overlay_file
                    warm_start = time.monotonic()
                    page.evaluate("""async filename => {
                        const m=window.__hictBenchmark.manager(),requests=m.networkManager.requestManager;
                        const status=await requests.openSecondarySource(filename,false);
                        if (!status.attached) throw Error('Mixed-pyramid attachment failed');
                        m.viewAndLayersManager.setSecondaryResolutionModel(status.compatibility);
                        const color=(source,shade)=>({type:'colormap',mode:'LINEAR',input:{type:'source',source},
                            minSignal:0,maxSignal:source==='SECONDARY'?1:1100,startColor:'#ffffff00',endColor:shade});
                        await requests.setRenderPipelineConfig({enabled:true,swapUpperLower:false,
                            upperExpression:color('SECONDARY','#0000ffff'),lowerExpression:color('PRIMARY','#ff0000ff')});
                        await m.reloadTilesFromBackend();
                    }""", overlay_file)
                    warm_opens.append(time.monotonic()-warm_start)
                    result["warmAttachP95Seconds"] = percentile(warm_opens, 0.95)
                    page.evaluate("""() => {
                        const m=window.__hictBenchmark.manager(), l=m.viewAndLayersManager;
                        l.restoreViewPosition({centerBp:[1000.5,1000.5], resolution:l.getPixelResolutionForBpResolution(1)});
                        m.getMap().renderSync();
                    }""")
                    page.wait_for_function("""() => {
                        const m=window.__hictBenchmark.manager(),l=m.viewAndLayersManager;
                        const map=m.getMap(), center=map.getPixelFromCoordinate(m.getView().getCenter());
                        return l.getNativeResolutionDescriptor('SECONDARY').bpResolution===1 &&
                            l.layersHolder.secondaryHiCDataLayers.some(layer=> {
                                if(!layer.getVisible()) return false;
                                for (let y=-3; y<=3; y++) for(let x=-3; x<=3; x++) {
                                    const rgba=layer.getData([center[0]+x,center[1]+y]);
                                    if(rgba?.[3]>0 && rgba[2]>rgba[0]+20) return true;
                                }
                                return false;
                            });
                    }""")
                    result["warmFinePaintSeconds"] = time.monotonic()-warm_start
                page.get_by_role("link", name="View", exact=True).click()
                page.get_by_role(
                    "link", name="Restrict resolutions…", exact=True
                ).click()
                # Transient locator handles retain detached nodes in the inspector.
                # Keep these memory measurements to primitive DOM results and real input.
                page.wait_for_function("""() => {
                    const dialog=document.querySelector('[aria-labelledby="resolution-settings-title"]');
                    return dialog && dialog.getBoundingClientRect().width>0;
                }""")
                page.keyboard.press("Shift+Tab")
                assert page.evaluate(
                    """() => document.querySelector(
                    '[aria-labelledby="resolution-settings-title"]').contains(document.activeElement)"""
                )
                close = page.evaluate("""() => {
                    const button=document.querySelector('[aria-labelledby="resolution-settings-title"] .btn-close');
                    const bounds=button.getBoundingClientRect();
                    const x=bounds.x+bounds.width/2,y=bounds.y+bounds.height/2;
                    return {x,y,hit:document.elementFromPoint(x,y)===button};
                }""")
                assert close["hit"], close
                page.mouse.click(close["x"], close["y"])
                page.wait_for_function(
                    "!document.querySelector('[aria-labelledby=resolution-settings-title]')"
                )
                assert page.evaluate(
                    """() => Array.from(document.querySelectorAll('a.nav-link'))
                    .find(element=>element.textContent.trim()==='View')===document.activeElement"""
                )
                viewport = page.evaluate("""() => {
                    const r=window.__hictBenchmark.manager().getMap().getViewport().getBoundingClientRect();
                    return {x:r.left+r.width/2,y:r.top+r.height/2};
                }""")
                page.evaluate("""() => {
                    window.soakFrames=[];let last=performance.now();
                    const tick=now=>{window.soakFrames.push(now-last);last=now;window.soakRaf=requestAnimationFrame(tick)};
                    window.soakRaf=requestAnimationFrame(tick);
                }""")
                page.mouse.move(viewport["x"], viewport["y"])
                for _ in range(3):
                    page.mouse.wheel(0, -120)
                    page.wait_for_timeout(250)
                page.mouse.down()
                page.mouse.move(viewport["x"] + 70, viewport["y"] + 40, steps=12)
                page.mouse.up()
                page.wait_for_timeout(400)
                frames.extend(
                    page.evaluate(
                        "() => {cancelAnimationFrame(window.soakRaf);return window.soakFrames.slice(1);}"
                    )
                )
                if sequence:
                    before = page.request.post(
                        args.api + "/sequence/region", data={"startBp": 0, "endBp": 100}
                    ).json()["sequence"]
                    # Structural edits use entity selection. A persistent region
                    # belongs to sequence/export tools and must not stand in for it.
                    page.keyboard.down("Shift")
                    try:
                        page.mouse.click(viewport["x"], viewport["y"])
                    finally:
                        page.keyboard.up("Shift")
                    page.wait_for_function(
                        "window.__hictBenchmark.manager().viewAndLayersManager.currentViewState.selectedContigIds.length > 0"
                    )
                    for button, endpoint in [
                        ("Reverse Selection", "/reverse_selection_range"),
                        ("Undo assembly edit", "/assembly/history/restore"),
                    ]:
                        if button == "Undo assembly edit":
                            page.get_by_role(
                                "button", name="Journal", exact=True
                            ).click()
                        action_started = time.monotonic()
                        with page.expect_response(
                            lambda response: response.url.endswith(endpoint)
                        ) as response:
                            page.get_by_role("button", name=button, exact=True).click()
                        assert response.value.ok, response.value.text()
                        acknowledgements.append(
                            (time.monotonic() - action_started) * 1000
                        )
                    page.wait_for_function(
                        "!window.__hictBenchmark.manager().networkManager.requestManager.history.busy"
                    )
                    after = page.request.post(
                        args.api + "/sequence/region", data={"startBp": 0, "endBp": 100}
                    ).json()["sequence"]
                    assert before == after, "Undo changed the linked sequence"
                    page.locator("#export-dropdown-toggle").click()
                    dialog = page.get_by_role(
                        "dialog", name="Export figure", exact=True
                    )
                    expect(dialog.locator("#export-png-button")).to_be_enabled(
                        timeout=90000
                    )
                    with page.expect_download() as download:
                        dialog.locator("#export-png-button").click()
                    exported = output / "latest-viewport.png"
                    download.value.save_as(exported)
                    assert exported.stat().st_size > 500
                    dialog.get_by_role("button", name="Close", exact=True).last.click()
                    expect(dialog).to_have_count(0)
                page.get_by_role("link", name="File", exact=True).click()
                page.get_by_role("link", name="Close map", exact=True).click()
                page.wait_for_function("!window.__hictBenchmark.manager()")
                cdp.send("Runtime.discardConsoleEntries")
                cdp.send("HeapProfiler.collectGarbage")
                cdp.send("HeapProfiler.collectGarbage")
                metrics = {
                    item["name"]: item["value"]
                    for item in cdp.send("Performance.getMetrics")["metrics"]
                }
                sample = {
                    "elapsedSeconds": time.monotonic() - started,
                    "cycle": cycle,
                    "file": filename,
                    "heapBytes": metrics["JSHeapUsedSize"],
                    "nodes": metrics["Nodes"],
                    "listeners": metrics["JSEventListeners"],
                }
                if args.server_pid:
                    subprocess.run(
                        ["jcmd", str(args.server_pid), "GC.run"],
                        check=True,
                        capture_output=True,
                        timeout=15,
                    )
                    heap = subprocess.run(
                        ["jcmd", str(args.server_pid), "GC.heap_info"],
                        check=True,
                        capture_output=True,
                        text=True,
                        timeout=15,
                    )
                    matched = re.search(
                        r"heap\s+total\s+\d+[KMG],\s+used\s+(\d+)([KMG])", heap.stdout
                    )
                    assert matched, heap.stdout
                    sample["serverHeapBytes"] = (
                        int(matched[1])
                        * {"K": 1024, "M": 1024**2, "G": 1024**3}[matched[2]]
                    )
                    status = Path(f"/proc/{args.server_pid}/status").read_text()
                    sample["serverRssBytes"] = (
                        int(re.search(r"^VmRSS:\s+(\d+) kB", status, re.MULTILINE)[1])
                        * 1024
                    )
                    for source, target in (
                        ("RssAnon", "serverAnonymousRssBytes"),
                        ("RssFile", "serverFileRssBytes"),
                    ):
                        matched_rss = re.search(
                            r"^" + source + r":\s+(\d+) kB", status, re.MULTILINE
                        )
                        if matched_rss:
                            sample[target] = int(matched_rss[1]) * 1024
                    sample["serverThreads"] = int(
                        re.search(r"^Threads:\s+(\d+)", status, re.MULTILINE)[1]
                    )
                    committed = re.search(r"heap\s+total\s+(\d+)([KMG])", heap.stdout)
                    if committed:
                        sample["serverHeapCommittedBytes"] = (
                            int(committed[1])
                            * {"K": 1024, "M": 1024**2, "G": 1024**3}[committed[2]]
                        )
                    if args.native_memory:
                        native = subprocess.run(
                            [
                                "jcmd",
                                str(args.server_pid),
                                "VM.native_memory",
                                "summary",
                                "scale=KB",
                            ],
                            check=True,
                            capture_output=True,
                            text=True,
                            timeout=15,
                        )
                        sample["nativeCommittedBytes"] = {
                            match[1].strip(): int(match[3]) * 1024
                            for match in re.finditer(
                                r"-\s+(.+?)\s+\(reserved=(\d+)KB, committed=(\d+)KB",
                                native.stdout,
                            )
                        }
                        assert sample["nativeCommittedBytes"], native.stdout
                samples.append(sample)
                with (output / "samples.jsonl").open("a") as stream:
                    stream.write(json.dumps(sample) + "\n")
                assert not errors, errors
                cycle += 1
                page.wait_for_timeout(5000)
            stable = [
                row
                for row in samples
                if row["elapsedSeconds"] >= min(300, args.duration_seconds / 3)
            ]
            assert len(stable) >= 3, "Need at least three post-warmup measurements"
            third = max(1, len(stable) // 3)
            early, late = stable[:third], stable[-third:]
            heap_growth = statistics.median(
                row["heapBytes"] for row in late
            ) - statistics.median(row["heapBytes"] for row in early)
            slope = float(
                np.polyfit(
                    [row["elapsedSeconds"] / 60 for row in stable],
                    [row["heapBytes"] / 1024**2 for row in stable],
                    1,
                )[0]
            )
            backend_trends = {}
            if args.server_pid:
                for key in ("serverHeapBytes", "serverRssBytes"):
                    backend_trends[key] = {
                        "growthBytes": statistics.median(row[key] for row in late)
                        - statistics.median(row[key] for row in early),
                        "slopeMiBPerMinute": float(
                            np.polyfit(
                                [row["elapsedSeconds"] / 60 for row in stable],
                                [row[key] / 1024**2 for row in stable],
                                1,
                            )[0]
                        ),
                        "minimumBytes": min(row[key] for row in stable),
                        "maximumBytes": max(row[key] for row in stable),
                    }
            result.update(
                status="running",
                backendTrends=backend_trends,
                domTrends={
                    key: {
                        "earlyMaximum": max(row[key] for row in early),
                        "lateMaximum": max(row[key] for row in late),
                    }
                    for key in ("nodes", "listeners")
                },
                elapsedSeconds=time.monotonic() - started,
                cycles=cycle,
                heapGrowthBytes=heap_growth,
                heapSlopeMiBPerMinute=slope,
                frameP95Ms=percentile(frames, 0.95),
                acknowledgementP95Ms=percentile(acknowledgements, 0.95),
                qualifiesThirtyMinutes=args.duration_seconds >= 1800,
            )
            if args.duration_seconds >= 1800:
                assert (
                    backend_trends["serverHeapBytes"]["slopeMiBPerMinute"] <= 0.5
                ), backend_trends
                assert (
                    backend_trends["serverRssBytes"]["slopeMiBPerMinute"] <= 1
                ), backend_trends
                assert heap_growth <= 8 * 1024**2, heap_growth
                assert slope <= 0.2, slope
                assert (
                    max(row["nodes"] for row in late)
                    <= max(row["nodes"] for row in early) + 100
                ), result["domTrends"]
                assert (
                    max(row["listeners"] for row in late)
                    <= max(row["listeners"] for row in early) + 20
                ), result["domTrends"]
                assert percentile(frames, 0.95) <= 33, percentile(frames, 0.95)
                assert (
                    acknowledgements and percentile(acknowledgements, 0.95) <= 100
                ), acknowledgements
            result["status"] = "passed"
        except BaseException as failure:
            result.update(
                status="failed", failure=str(failure), qualifiesThirtyMinutes=False
            )
            raise
        finally:
            result.update(samples=samples, errors=errors)
            (output / "results.json").write_text(json.dumps(result, indent=2))
            browser.close()
    print(json.dumps({key: value for key, value in result.items() if key != "samples"}))


if __name__ == "__main__":
    main()
