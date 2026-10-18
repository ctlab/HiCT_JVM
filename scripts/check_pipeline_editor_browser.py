#!/usr/bin/env python3
"""Read-only matrix input, in-memory graph round trips, JSON/screenshots in /tmp."""
import argparse
import json
from pathlib import Path
from playwright.sync_api import sync_playwright


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:8186")
    parser.add_argument("--api", default="http://127.0.0.1:5106")
    parser.add_argument("--output", default="/tmp/hict-review-20261018/editor-browser")
    args = parser.parse_args()
    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=True)
    results, errors = [], []
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1440, "height": 1000})
        page.set_default_timeout(60000)
        page.on("pageerror", lambda error: errors.append(str(error)))
        page.add_init_script("window.__HICT_BENCHMARK_ENABLED__ = true")
        page.route("http://localhost:5000/**", lambda route: route.continue_(
            url=args.api + route.request.url.removeprefix("http://localhost:5000")))
        page.goto(args.url)
        page.wait_for_function("window.__hictBenchmark")
        page.evaluate("api => window.__hictBenchmark.open('combined_ind2_4DN.hict.hdf5', api)", args.api)
        page.wait_for_timeout(1500)
        nan = page.get_by_role("button", name="Do not use Cooler weights", exact=True)
        if nan.is_visible():
            nan.click()
        page.evaluate("""async () => {const m=window.__hictBenchmark.manager();
            const color={type:'colormap',mode:'LINEAR',input:{type:'source',source:'PRIMARY'},
                minSignal:17,maxSignal:37,startColor:'#ffffffff',endColor:'#ff0000ff'};
            await m.networkManager.requestManager.setRenderPipelineConfig({enabled:true,
                swapUpperLower:false,upperExpression:color,lowerExpression:color});
            await m.reloadTilesFromBackend();}""")

        def open_editor():
            page.get_by_role("link", name="View", exact=True).click()
            page.get_by_role("link", name="Rendering pipeline...", exact=True).click()
            page.locator(".graph-canvas").wait_for(state="visible")
            page.wait_for_function("document.querySelector('.graph-canvas').data.graph._nodes.length>2")
            page.wait_for_timeout(300)

        def import_graph(payload):
            page.locator('.pipeline-root input[type="file"]').set_input_files({
                "name": "pipeline.json", "mimeType": "application/json", "buffer": json.dumps(payload).encode()})
            page.wait_for_timeout(300)

        open_editor()
        with page.expect_download() as download:
            page.get_by_role("button", name="Export graph", exact=True).click()
        path = out / "pipeline-original.json"
        download.value.save_as(path)
        payload = json.loads(path.read_text())
        nodes = payload["liteGraph"]["nodes"]
        range_node = next(node for node in nodes if node["type"] == "hict/signal_range")
        histogram_id = max(node["id"] for node in nodes) + 1
        nodes.append({"id": histogram_id, "type": "hict/histogram", "pos": [450, 600],
                      "size": [240, 80], "properties": {}, "title": "Range maximum histogram"})
        links = payload["liteGraph"]["links"]
        link_id = max(link[0] for link in links) + 1
        links.append([link_id, range_node["id"], 1, histogram_id, 0, "number"])
        payload["viewport"] = {"scale": 0.7, "offset": [123, 45]}
        import_graph(payload)

        def snapshot():
            return page.evaluate("""() => {const c=document.querySelector('.graph-canvas').data;
                return {viewport:[c.ds.scale,Array.from(c.ds.offset)],
                    nodes:c.graph._nodes.map(n=>({id:n.id,type:n.type,pos:Array.from(n.pos).slice(0,2),properties:n.properties})),
                    hist:c.graph._nodes.find(n=>n.type==='hict/histogram')?.getInputLink(0)?.origin_slot};}""")

        expected = snapshot()
        assert expected["hist"] == 1 and expected["viewport"] == [0.7, [123, 45]], expected
        page.evaluate("""() => {const n=document.querySelector('.graph-canvas').data.graph._nodes.find(n=>n.type==='hict/histogram');
            n.widgets.find(w=>w.name==='Inspect distribution').callback();}""")
        page.locator(".histogram-chart svg").wait_for(state="visible")
        histogram_text = page.locator(".histogram-dialog").inner_text()
        assert "37" in histogram_text, histogram_text
        page.locator(".histogram-dialog").get_by_role("button", name="Close", exact=True).click()
        results.append({"test": "import retains histogram connection to selectedMax", "histogram": histogram_text})
        page.locator(".pipeline-root .modal-footer").get_by_role("button", name="Close", exact=True).click()
        open_editor()
        assert snapshot() == expected, (expected, snapshot())
        results.append({"test": "close/reopen preserves disconnected inspector, layout and viewport"})
        with page.expect_download() as download:
            page.get_by_role("button", name="Export graph", exact=True).click()
        path = out / "pipeline-roundtrip.json"
        download.value.save_as(path)
        imported = json.loads(path.read_text())
        import_graph(imported)
        assert snapshot() == expected
        results.append({"test": "export/import preserves complete editor graph"})
        page.locator(".pipeline-root .modal-footer").get_by_role("button", name="Preview", exact=True).click()
        page.locator(".pipeline-preview").wait_for(state="visible")
        page.locator(".pipeline-root .modal-header button").first.click()
        page.locator(".graph-canvas").wait_for(state="visible")
        assert snapshot() == expected
        results.append({"test": "Preview round trip preserves inspector and layout"})
        before_invalid = snapshot()
        invalid = json.loads(json.dumps(imported))
        invalid["liteGraph"]["nodes"][0]["type"] = "basic/script"
        import_graph(invalid)
        assert snapshot() == before_invalid
        results.append({"test": "unsupported imported node is rejected without modifying graph"})
        page.screenshot(path=str(out / "pipeline-restored.png"))
        page.locator(".pipeline-root .modal-footer").get_by_role("button", name="Close", exact=True).click()
        page.evaluate("""async () => {
            const api=window.__hictBenchmark.manager().networkManager.requestManager;
            const config=await api.getRenderPipelineConfig();
            for(const branch of [config.upperExpression,config.lowerExpression]) {
                branch.maxSignal=43;if(branch.max)branch.max.value=43;
            }
            await api.setRenderPipelineConfig(config);
        }""")
        open_editor()
        assert all(node['type'] != 'hict/histogram' for node in snapshot()['nodes'])
        results.append({"test": "stale cached graph cannot replace changed server expressions"})
        page.locator(".pipeline-root .modal-footer").get_by_role("button", name="Close", exact=True).click()
        page.evaluate("""() => {
            const m=window.__hictBenchmark.manager(),api=m.networkManager.requestManager;
            const original=api.queryMatrixFloat32.bind(api);
            window.autoRangeWaiting=false;
            api.queryMatrixFloat32=async options => {
                api.queryMatrixFloat32=original;
                window.autoRangeWaiting=true;
                await new Promise(resolve=>window.releaseAutoRange=resolve);
                return original(options);
            };
            window.autoRangeTask=m.visualizationManager.applyOpeningFullMapQuantileThreshold();
        }""")
        page.wait_for_function('window.autoRangeWaiting')
        preserved = page.evaluate("""async () => {
            const api=window.__hictBenchmark.manager().networkManager.requestManager;
            const config=await api.getRenderPipelineConfig();
            for(const branch of [config.upperExpression,config.lowerExpression]) {
                branch.maxSignal=47;if(branch.max)branch.max.value=47;
            }
            await api.setRenderPipelineConfig(config);
            window.releaseAutoRange();await window.autoRangeTask;
            return JSON.stringify(await api.getRenderPipelineConfig())===JSON.stringify(config);
        }""")
        assert preserved, 'Outdated automatic thresholds overwrote newer explicit settings'
        results.append({"test": "delayed auto-threshold calculation cannot overwrite a newer pipeline"})
        assert not errors, errors
        (out / "results.json").write_text(json.dumps(results, indent=2) + "\n")
        browser.close()
        print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
