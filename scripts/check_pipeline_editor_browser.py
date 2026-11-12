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

    def record_result(result):
        results.append(result)
        (out / "results.json").write_text(json.dumps(results, indent=2) + "\n")
        print(result["test"], flush=True)

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1440, "height": 1000})
        page.set_default_timeout(60000)
        page.on("pageerror", lambda error: errors.append(str(error)))
        page.on("console", lambda message: errors.append(message.text) if message.type == "error" else None)
        page.add_init_script("window.addEventListener('unhandledrejection', e=>console.error('Unhandled pipeline-audit promise',e.reason))")
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
            page.get_by_role("link", name="Rendering pipeline…", exact=True).click()
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
        page.wait_for_function("document.querySelector('.interactive-histogram')?.data?.[0]?.y")
        histogram_text = page.locator(".histogram-dialog").inner_text()
        assert "37" in histogram_text, histogram_text
        page.locator(".histogram-dialog").get_by_role("button", name="Close", exact=True).click()
        record_result({"test": "import retains histogram connection to selectedMax", "histogram": histogram_text})
        page.locator(".pipeline-root .modal-footer").get_by_role("button", name="Close", exact=True).click()
        open_editor()
        assert snapshot() == expected, (expected, snapshot())
        record_result({"test": "close/reopen preserves disconnected inspector, layout and viewport"})
        with page.expect_download() as download:
            page.get_by_role("button", name="Export graph", exact=True).click()
        path = out / "pipeline-roundtrip.json"
        download.value.save_as(path)
        imported = json.loads(path.read_text())
        import_graph(imported)
        assert snapshot() == expected
        record_result({"test": "export/import preserves complete editor graph"})
        page.locator(".pipeline-root .modal-footer").get_by_role("button", name="Preview", exact=True).click()
        page.locator(".pipeline-preview").wait_for(state="visible")
        page.locator(".pipeline-root .modal-header button").first.click()
        page.locator(".graph-canvas").wait_for(state="visible")
        assert snapshot() == expected
        record_result({"test": "Preview round trip preserves inspector and layout"})
        before_invalid = snapshot()
        invalid = json.loads(json.dumps(imported))
        invalid["liteGraph"]["nodes"][0]["type"] = "basic/script"
        import_graph(invalid)
        assert snapshot() == before_invalid
        record_result({"test": "unsupported imported node is rejected without modifying graph"})
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
        record_result({"test": "stale cached graph cannot replace changed server expressions"})
        page.locator(".pipeline-root .modal-footer").get_by_role("button", name="Close", exact=True).click()
        page.evaluate("""() => {
            const m=window.__hictBenchmark.manager(),api=m.networkManager.requestManager;
            const layers=m.viewAndLayersManager;
            const histogram=layers.loadedViewportSignalRange;
            // Force the bounded fallback query so the test can delay its response.
            layers.loadedViewportSignalRange=()=>undefined;
            const original=api.queryMatrixFloat32.bind(api);
            window.autoRangeWaiting=false;
            api.queryMatrixFloat32=async options => {
                api.queryMatrixFloat32=original;
                window.autoRangeWaiting=true;
                await new Promise(resolve=>window.releaseAutoRange=resolve);
                return original(options);
            };
            window.autoRangeTask=m.visualizationManager.applyOpeningViewportQuantileThreshold()
                .finally(()=>layers.loadedViewportSignalRange=histogram);
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
        record_result({"test": "delayed auto-threshold calculation cannot overwrite a newer pipeline"})

        def server_config():
            return page.evaluate("async()=>await window.__hictBenchmark.manager().networkManager.requestManager.getRenderPipelineConfig()")

        def signal_values():
            return page.evaluate("""async()=>{
                const m=window.__hictBenchmark.manager();
                const bp=m.viewAndLayersManager.currentViewState.resolutionDesciptor.bpResolution;
                return Array.from((await m.networkManager.requestManager.queryMatrixFloat32({
                    bpResolution:bp,startRowPx:0,endRowPx:2,startColPx:0,endColPx:2,
                    source:'PRIMARY',signalMode:'PIPELINE_SIGNAL'})).values);
            }""")

        def apply_editor():
            with page.expect_response(lambda response: '/render_pipeline/set' in response.url and response.request.method == 'POST') as response:
                page.locator('.pipeline-root .modal-footer').get_by_role('button', name='Apply', exact=True).click()
            assert response.value.ok, response.value.text()
            page.wait_for_timeout(400)

        def constant_color(value):
            return {"type": "colormap", "mode": "LINEAR", "input": {"type": "constant", "value": value},
                    "minSignal": 0, "maxSignal": 8, "startColor": "#ffffffff", "endColor": "#ff0000ff"}

        def wait_for_branch_colors(swapped):
            try:
                page.wait_for_function("""swapped=>{
                    const m=window.__hictBenchmark.manager(),map=m.getMap();
                    const [left,bottom,right,top]=map.getView().getProjection().getExtent();
                    const layers=m.viewAndLayersManager.layersHolder.primaryHiCDataLayers;
                    window.__pipelineBranchSamples=[];
                    return [[.65,.35,swapped?6:2],[.35,.65,swapped?2:6]].every(([x,y,value])=>{
                        const pixel=map.getPixelFromCoordinate([left+x*(right-left),top+y*(bottom-top)]);
                        return layers.filter(layer=>layer.getVisible()).some(layer=>{
                            const rgba=layer.getData(pixel),expected=255*(1-value/8);
                            window.__pipelineBranchSamples.push({pixel,rgba:rgba?Array.from(rgba):null,expected});
                            return rgba?.[3]===255 && rgba[0]===255 && Math.abs(rgba[1]-expected)<=1 && Math.abs(rgba[2]-expected)<=1;
                        });
                    });
                }""", arg=swapped)
            except Exception:
                page.screenshot(path=str(out / 'branch-colors-failure.png'))
                (out / 'branch-colors-failure.json').write_text(json.dumps(page.evaluate('window.__pipelineBranchSamples'), indent=2) + '\n')
                raise


        open_editor()
        constant_config = {"enabled": True, "swapUpperLower": False,
                           "upperExpression": constant_color(2), "lowerExpression": constant_color(6)}
        before_import = server_config()
        import_graph(constant_config)
        assert server_config() == before_import, 'Import changed the server before Preview or Apply'
        page.locator('.pipeline-root .modal-footer').get_by_role('button', name='Preview', exact=True).click()
        page.locator('.pipeline-preview').wait_for(state='visible')
        assert signal_values() == [2, 2, 6, 2]
        wait_for_branch_colors(False)
        page.screenshot(path=str(out / 'constant-branch-preview.png'))
        preview_toggle = page.locator('.pipeline-root .modal-header button').first
        assert preview_toggle.inner_text().strip() == 'Show editor'
        preview_toggle.click()
        page.locator('.graph-canvas').wait_for(state='visible')
        record_result({"test": "Preview publishes exact independent constant branches and preserves the editor graph"})

        page.locator('#pipeline-swap').check()
        apply_editor()
        assert signal_values() == [6, 6, 2, 6]
        wait_for_branch_colors(True)
        swapped = server_config()
        assert swapped['swapUpperLower']
        page.locator('#pipeline-enabled').uncheck()
        apply_editor()
        disabled = server_config()
        assert not disabled['enabled']
        for branch in ('upperExpression', 'lowerExpression'):
            assert disabled[branch] == swapped[branch]
        page.locator('#pipeline-enabled').check()
        apply_editor()
        assert signal_values() == [6, 6, 2, 6]
        record_result({"test": "Swap selects the exact opposite numeric branches and enable toggling retains both graphs"})

        before_preset = server_config()
        page.locator('.pipeline-preset-select').select_option('primary_only')
        assert server_config() == before_preset
        page.get_by_role('button', name='Load preset', exact=True).click()
        page.wait_for_timeout(300)
        assert server_config() == before_preset
        apply_editor()
        primary = server_config()
        assert primary['enabled'] and not primary['swapUpperLower']
        assert primary['upperExpression'] == primary['lowerExpression']
        assert any(node['type'] == 'hict/source' for node in snapshot()['nodes']), snapshot()
        record_result({"test": "Preset selection and loading preserve server state until Apply publishes equal primary branches"})

        page.evaluate("async config=>await window.__hictBenchmark.manager().networkManager.requestManager.setRenderPipelineConfig(config)", constant_config)
        page.get_by_role('button', name='Reload', exact=True).click()
        page.wait_for_timeout(400)
        assert page.locator('#pipeline-enabled').is_checked()
        assert not page.locator('#pipeline-swap').is_checked()
        apply_editor()
        assert signal_values() == [2, 2, 6, 2]
        record_result({"test": "Reload fetches changed server branches without reviving the stale preset draft"})

        with page.expect_response(lambda response: '/render_pipeline/reset' in response.url and response.request.method == 'POST') as response:
            page.locator('.pipeline-root .modal-footer').get_by_role('button', name='Reset', exact=True).click()
        assert response.value.ok
        reset = response.value.json()
        page.wait_for_timeout(400)
        assert server_config() == reset
        assert page.locator('#pipeline-enabled').is_checked() == reset['enabled']
        assert page.locator('#pipeline-swap').is_checked() == reset['swapUpperLower']
        reset_signal = signal_values()
        page.locator('.pipeline-root .modal-footer').get_by_role('button', name='Close', exact=True).click()
        open_editor()
        reopened = server_config()
        (out / 'reset-roundtrip.json').write_text(json.dumps({'reset': reset, 'reopened': reopened}, indent=2) + '\n')
        # The editor makes implicit colormap limits explicit as constant inputs.
        for branch in ('upperExpression', 'lowerExpression'):
            for bound, signal in (('min', 'minSignal'), ('max', 'maxSignal')):
                connection = reopened[branch].pop(bound)
                assert connection['type'] == 'constant'
                assert connection['value'] == reset[branch][signal]
        assert reopened == reset, (reset, reopened)
        assert signal_values() == reset_signal
        record_result({"test": "Reset publishes the server default and preserves it through close and reopen"})
        assert not errors, errors
        (out / "results.json").write_text(json.dumps(results, indent=2) + "\n")
        browser.close()
        print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
