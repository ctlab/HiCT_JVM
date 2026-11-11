#!/usr/bin/env python3
"""Measure a declared browser navigation baseline on a disposable HiCT server.

Use --release-gate to enforce the declared frame and input acknowledgement limits.
The two samples use a fresh browser and a repeat in that browser with retained
server and OS caches.
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
    parser.add_argument(
        "--release-gate", action="store_true",
        help="Require frame p95 <= 33 ms and input acknowledgement p95 <= 100 ms",
    )
    args = parser.parse_args()
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    results = {"platform": platform.platform(), "viewport": [1440, 1000], "runs": []}
    results['timingDefinitions'] = {
        'dispatchDelayMs': 'DOM event timestamp to capture listener; browser-injected input, not hardware latency',
        'firstViewChangeMs': 'Capture listener to first changed center/resolution property',
        'firstRenderedFrameMs': 'Capture listener to first postrender after that property change; not display presentation',
        'settledMs': 'Playwright action start through animation/tile settling; includes automation overhead',
        'cacheState': 'Fresh browser then repeat open; OS and server caches are not cleared',
    }
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
                # Use a declared raw-count graph for reproducible navigation measurements.
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
                    const m=window.__hictBenchmark.manager(),v=m.getView(),map=m.getMap();
                    b.input=e=>{const a=b.action;if(!a || a.receivedAt!==undefined)return;
                        if((a.kind==='pan' && e.type==='pointerdown') || (a.kind!=='pan' && e.type==='click')){
                            a.receivedAt=performance.now();a.eventTimestamp=e.timeStamp;
                            a.dispatchDelayMs=Math.max(0,a.receivedAt-e.timeStamp);
                        }};
                    b.changed=()=>{const a=b.action;if(!a || a.receivedAt===undefined)return;
                        const now=[v.getCenter(),v.getResolution()];
                        if(JSON.stringify(now)!==JSON.stringify(a.before) && a.firstViewChangeMs===undefined)
                            a.firstViewChangeMs=performance.now()-a.receivedAt;};
                    b.rendered=()=>{const a=b.action;if(a?.firstViewChangeMs!==undefined && a.firstRenderedFrameMs===undefined)
                        a.firstRenderedFrameMs=performance.now()-a.receivedAt;};
                    document.addEventListener('click',b.input,true);
                    document.addEventListener('pointerdown',b.input,true);
                    v.on(['change:center','change:resolution'],b.changed);map.on('postrender',b.rendered);
                    b.cleanup=()=>{document.removeEventListener('click',b.input,true);
                        document.removeEventListener('pointerdown',b.input,true);
                        v.un(['change:center','change:resolution'],b.changed);map.un('postrender',b.rendered);};
                    performance.clearResourceTimings();
                }""")
                actions = []
                for index in range(20):
                    before = view(page)
                    arm_action(page, 'zoom-in' if index % 2 == 0 else 'zoom-out', before)
                    started = time.monotonic()
                    # Exercise the actual OpenLayers zoom controls.
                    page.locator(".ol-zoom-in" if index % 2 == 0 else ".ol-zoom-out").click()
                    click_ms = (time.monotonic() - started) * 1000
                    settle(page)
                    after = view(page)
                    assert (after[1] < before[1]) if index % 2 == 0 else (after[1] > before[1]), (before, after)
                    actions.append({"action": "zoom-in" if index % 2 == 0 else "zoom-out",
                                    "clickRoundTripMs": click_ms, "settledMs": (time.monotonic() - started) * 1000,
                                    "browserTiming": action_timing(page)})
                # Zoom in enough to leave room for genuine drag movement instead
                # of testing an extent-constrained pan at the full-map fit.
                page.evaluate('window.__baseline.action=null')
                for _ in range(2):
                    page.locator('.ol-zoom-in').click()
                    settle(page)
                box = page.evaluate('window.__hictBenchmark.manager().getMap().getViewport().getBoundingClientRect().toJSON()')
                for index in range(6):
                    before = view(page)
                    arm_action(page, 'pan', before)
                    x, y = box['x'] + box['width'] * .5, box['y'] + box['height'] * .5
                    dx, dy = (80, 45) if index % 2 == 0 else (-80, -45)
                    started = time.monotonic()
                    page.mouse.move(x, y)
                    page.mouse.down()
                    page.mouse.move(x + dx, y + dy, steps=8)
                    page.mouse.up()
                    settle(page)
                    after = view(page)
                    assert after[0] != before[0] and after[1] == before[1], (before, after)
                    actions.append({'action': 'pan', 'settledMs': (time.monotonic()-started)*1000,
                                    'before': before, 'after': after, 'browserTiming': action_timing(page)})
                measured = page.evaluate("""() => {
                    const b=window.__baseline;b.active=false;b.observer.disconnect();b.cleanup();
                    return {frames:b.frames,longTasks:b.longTasks,resources:performance.getEntriesByType('resource')
                        .filter(r=>/\\/(get_tile|tile|matrix)/.test(r.name)).map(r=>({url:r.name,durationMs:r.duration,bytes:r.transferSize}))};
                }""")
                cdp.send("HeapProfiler.collectGarbage")
                metrics = {m["name"]: m["value"] for m in cdp.send("Performance.getMetrics")["metrics"]}
                results["runs"].append({"label": label, "openAndConfigureToSettledMs": open_ms,
                    "rendering": "PRIMARY raw counts, linear red 0..100, both triangles", "actions": actions,
                    "frameMs": {"p50": percentile(measured["frames"], .5), "p95": percentile(measured["frames"], .95),
                                "count": len(measured["frames"])}, "longTasksMs": measured["longTasks"],
                    "inputAcknowledgementP95Ms": percentile(
                        [action["browserTiming"]["firstViewChangeMs"] for action in actions], .95
                    ),
                    "tileResources": measured["resources"],
                    "heapAfterGcBytes": metrics.get("JSHeapUsedSize"), "domNodes": metrics.get("Nodes"),
                    "eventListeners": metrics.get("JSEventListeners")})
                page.screenshot(path=str(output / f"{label}.png"))
            results["releaseLimits"] = {
                "enforced": args.release_gate,
                "frameP95Ms": 33,
                "inputAcknowledgementP95Ms": 100,
                "satisfied": all(
                    run["frameMs"]["p95"] <= 33 and run["inputAcknowledgementP95Ms"] <= 100
                    for run in results["runs"]
                ),
            }
            if args.release_gate:
                assert results["releaseLimits"]["satisfied"], [
                    {
                        "label": run["label"],
                        "frameP95Ms": run["frameMs"]["p95"],
                        "inputAcknowledgementP95Ms": run["inputAcknowledgementP95Ms"],
                    }
                    for run in results["runs"]
                ]
            assert not errors, errors
            assert not http_errors, http_errors
        finally:
            results.update(pageErrors=errors, httpErrors=http_errors)
            (output / "results.json").write_text(json.dumps(results, indent=2) + "\n")
            browser.close()
    print(json.dumps({"runs": len(results["runs"]), "evidence": str(output)}, indent=2))


def arm_action(page, kind, before):
    page.evaluate('a=>window.__baseline.action=a', {'kind': kind, 'before': before})


def action_timing(page):
    # View completion and the subsequent map frame are separate events. The
    # queue can already be empty before that last postrender callback runs.
    page.wait_for_function('window.__baseline.action?.firstRenderedFrameMs !== undefined')
    timing = page.evaluate('window.__baseline.action')
    assert timing.get('firstViewChangeMs') is not None, timing
    assert timing.get('firstRenderedFrameMs') is not None, timing
    return timing


def view(page):
    return page.evaluate("""() => {const v=window.__hictBenchmark.manager().getView();return [v.getCenter(),v.getResolution()]}""")


def settle(page):
    page.wait_for_function("""() => {const m=window.__hictBenchmark.manager(),map=m.getMap(),v=m.getView();
        return !v.getAnimating() && !v.getInteracting() && map.tileQueue_.getTilesLoading()===0
            && map.tileQueue_.getCount()===0 && map.renderComplete_;}""")


if __name__ == "__main__":
    main()
