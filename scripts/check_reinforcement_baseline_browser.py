#!/usr/bin/env python3
"""Measure a declared browser navigation baseline on a disposable HiCT server.

This is a diagnostic, not the W16 performance/release gate. Server and OS caches
are not cleared; the two samples are a fresh browser and a repeat in that browser.
"""
import argparse
import json
import platform
import time
from pathlib import Path

from playwright.sync_api import sync_playwright


def percentile(values, fraction):
    ordered = sorted(values)
    if not ordered:
        return None
    index = (len(ordered) - 1) * fraction
    low = int(index)
    return ordered[low] + (ordered[min(low + 1, len(ordered) - 1)] - ordered[low]) * (index - low)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True)
    parser.add_argument("--api", required=True)
    parser.add_argument("--filename", default="combined_ind2_4DN.hict.hdf5")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    results = {"platform": platform.platform(), "viewport": [1440, 1000], "runs": []}
    errors, http_errors = [], []
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        results["browser"] = browser.version
        context = browser.new_context(viewport={"width": 1440, "height": 1000})
        context.route("http://localhost:5000/**", lambda route: route.continue_(
            url=args.api + route.request.url.removeprefix("http://localhost:5000")))
        page = context.new_page()
        page.set_default_timeout(30000)
        page.on("pageerror", lambda error: errors.append(str(error)))
        page.on("response", lambda response: http_errors.append({"url": response.url, "status": response.status})
                if response.status >= 400 else None)
        page.add_init_script("window.__HICT_BENCHMARK_ENABLED__=true")
        cdp = context.new_cdp_session(page)
        cdp.send("Performance.enable")
        try:
            page.goto(args.url)
            page.wait_for_function("window.__hictBenchmark")
            for label in ("fresh-browser", "repeat-open"):
                opened = time.monotonic()
                page.evaluate("args=>window.__hictBenchmark.open(args.filename,args.api)", vars(args))
                button = page.get_by_role("button", name="Do not use Cooler weights", exact=True)
                if button.is_visible():
                    button.click()
                # The existing opening-defaults defect can leave an almost blank
                # map. Declare a readable raw-count graph for this controlled
                # rendering baseline; this does not test or certify the wizard.
                page.evaluate("""async () => {
                    const m=window.__hictBenchmark.manager();
                    const color={type:'colormap',mode:'LINEAR',input:{type:'source',source:'PRIMARY'},
                        minSignal:0,maxSignal:100,startColor:'#ffffff00',endColor:'#ff0000ff'};
                    await m.networkManager.requestManager.setRenderPipelineConfig({enabled:true,
                        swapUpperLower:false,upperExpression:color,lowerExpression:color});
                    await m.reloadTilesFromBackend();m.viewAndLayersManager.fitViewToActiveExtent();
                }""")
                settle(page)
                open_ms = (time.monotonic() - opened) * 1000
                page.evaluate("""() => {
                    window.__baseline={frames:[],longTasks:[],active:true};
                    const b=window.__baseline;let previous=performance.now();
                    const frame=now=>{if(!b.active)return;b.frames.push(now-previous);previous=now;requestAnimationFrame(frame)};
                    requestAnimationFrame(frame);
                    b.observer=new PerformanceObserver(list=>b.longTasks.push(...list.getEntries().map(e=>e.duration)));
                    b.observer.observe({type:'longtask'});
                    performance.clearResourceTimings();
                }""")
                actions = []
                for index in range(20):
                    before = view(page)
                    started = time.monotonic()
                    # Actual OpenLayers buttons, independently of the known broken toolbar zoom.
                    page.locator(".ol-zoom-in" if index % 2 == 0 else ".ol-zoom-out").click()
                    click_ms = (time.monotonic() - started) * 1000
                    settle(page)
                    after = view(page)
                    assert (after[1] < before[1]) if index % 2 == 0 else (after[1] > before[1]), (before, after)
                    actions.append({"action": "zoom-in" if index % 2 == 0 else "zoom-out",
                                    "clickRoundTripMs": click_ms, "settledMs": (time.monotonic() - started) * 1000})
                measured = page.evaluate("""() => {
                    const b=window.__baseline;b.active=false;b.observer.disconnect();
                    return {frames:b.frames,longTasks:b.longTasks,resources:performance.getEntriesByType('resource')
                        .filter(r=>/\\/(get_tile|tile|matrix)/.test(r.name)).map(r=>({url:r.name,durationMs:r.duration,bytes:r.transferSize}))};
                }""")
                cdp.send("HeapProfiler.collectGarbage")
                metrics = {m["name"]: m["value"] for m in cdp.send("Performance.getMetrics")["metrics"]}
                results["runs"].append({"label": label, "openAndConfigureToSettledMs": open_ms,
                    "rendering": "PRIMARY raw counts, linear red 0..100, both triangles", "actions": actions,
                    "frameMs": {"p50": percentile(measured["frames"], .5), "p95": percentile(measured["frames"], .95),
                                "count": len(measured["frames"])}, "longTasksMs": measured["longTasks"],
                    "tileResources": measured["resources"],
                    "heapAfterGcBytes": metrics.get("JSHeapUsedSize"), "domNodes": metrics.get("Nodes"),
                    "eventListeners": metrics.get("JSEventListeners")})
                page.screenshot(path=str(output / f"{label}.png"))
            assert not errors, errors
            assert not http_errors, http_errors
        finally:
            results.update(pageErrors=errors, httpErrors=http_errors)
            (output / "results.json").write_text(json.dumps(results, indent=2) + "\n")
            browser.close()
    print(json.dumps({"runs": len(results["runs"]), "evidence": str(output)}, indent=2))


def view(page):
    return page.evaluate("""() => {const v=window.__hictBenchmark.manager().getView();return [v.getCenter(),v.getResolution()]}""")


def settle(page):
    page.wait_for_function("""() => {const m=window.__hictBenchmark.manager(),map=m.getMap(),v=m.getView();
        return !v.getAnimating() && !v.getInteracting() && map.tileQueue_.getTilesLoading()===0
            && map.tileQueue_.getCount()===0 && map.renderComplete_;}""")


if __name__ == "__main__":
    main()
