#!/usr/bin/env python3
"""Verify complete rendering presets through sidebar, JSON and native sessions."""

import argparse
import copy
import json
from pathlib import Path
from playwright.sync_api import expect, sync_playwright
from sequence_browser_harness import BrowserCheck


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ("url", "api", "output"):
        parser.add_argument("--" + key, required=True)
    args = parser.parse_args()
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    results = []
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1440, "height": 1000})
        page.set_default_timeout(30000)
        page.add_init_script("window.__HICT_BENCHMARK_ENABLED__=true")
        page.route(
            "http://localhost:5000/**",
            lambda route: route.continue_(
                url=args.api + route.request.url.removeprefix("http://localhost:5000")
            ),
        )
        check = BrowserCheck(page, output)
        group = page.get_by_role("group", name="Visualization presets", exact=True)
        picker = page.get_by_role("combobox", name="Visualization preset", exact=True)
        preset_name = "Complete custom graph"

        def api(path, payload=None):
            response = page.request.post(args.api + path, data=payload or {})
            assert response.ok, response.text()
            return response.json()

        def config():
            return api("/render_pipeline/get")

        def options():
            return api("/get_visualization_options")

        def query(mode):
            return api(
                "/matrix/query",
                {
                    "bpResolution": 4,
                    "startRowPx": 0,
                    "endRowPx": 5,
                    "startColPx": 0,
                    "endColPx": 5,
                    "source": "PRIMARY",
                    "format": "JSON",
                    "signalMode": mode,
                },
            )["values"]

        def editor_open():
            check.menu("View", "Rendering pipeline...")
            page.locator(".graph-canvas").wait_for(state="visible")
            page.wait_for_function(
                "document.querySelector('.graph-canvas').data.graph._nodes.length>2"
            )

        def editor_export(name):
            with page.expect_download() as download:
                page.get_by_role("button", name="Export graph", exact=True).click()
            target = output / name
            download.value.save_as(target)
            return json.loads(target.read_text())

        def editor_close():
            page.locator(".pipeline-root .modal-footer").get_by_role(
                "button", name="Close", exact=True
            ).click()
            page.locator(".graph-canvas").wait_for(state="hidden")

        def export_presets(name):
            with page.expect_download() as download:
                group.get_by_role("button", name="Export", exact=True).click()
            target = output / name
            download.value.save_as(target)
            return target, json.loads(target.read_text())

        def import_presets(document, filename):
            target = output / filename
            target.write_text(json.dumps(document))
            with page.expect_file_chooser() as chooser:
                group.get_by_role("button", name="Import", exact=True).click()
            chooser.value.set_files(target)
            expect(group.locator('input[type="file"]')).to_have_value("")

        def selected_preset(document):
            return next(
                item
                for item in document["data"]["savedVisualizationPresets"]
                if item["name"] == preset_name
            )

        def assert_values_and_pixels():
            values = query("PIPELINE_SIGNAL")
            assert values == [
                8 if row <= col else 2 for row in range(5) for col in range(5)
            ], values
            page.wait_for_function("""() => {
                const m=window.__hictBenchmark.manager(),map=m.getMap();
                const [left,bottom,right,top]=map.getView().getProjection().getExtent();
                const layers=m.viewAndLayersManager.layersHolder.primaryHiCDataLayers;
                const samples=[[.65,.35,[51,51,255,255]],[.35,.65,[255,204,204,255]]];
                window.__presetPixels=[];
                return samples.every(([x,y,expected])=>{
                    const pixel=map.getPixelFromCoordinate([left+x*(right-left),top+y*(bottom-top)]);
                    return layers.filter(layer=>layer.getVisible()).some(layer=>{
                        const rgba=layer.getData(pixel);
                        window.__presetPixels.push({pixel,rgba:rgba?Array.from(rgba):null,expected});
                        return rgba && expected.every((value,index)=>Math.abs(rgba[index]-value)<=1);
                    });
                });
            }""")
            assert query("RAW_COUNTS") == original_raw
            results.append(
                {"signals": values, "pixels": page.evaluate("window.__presetPixels")}
            )

        def save_applied_graph():
            def color(value, end):
                return {
                    "type": "colormap",
                    "mode": "LINEAR",
                    "input": {"type": "constant", "value": value},
                    "minSignal": 0,
                    "maxSignal": 10,
                    "startColor": "#ffffffff",
                    "endColor": end,
                }

            graph = {
                "enabled": True,
                "swapUpperLower": True,
                "upperExpression": color(2, "#ff0000ff"),
                "lowerExpression": color(8, "#0000ffff"),
            }
            page.evaluate(
                """async graph=>{const m=window.__hictBenchmark.manager();
                await m.networkManager.requestManager.setRenderPipelineConfig(graph);
                await m.reloadTilesFromBackend();}""",
                graph,
            )
            editor_open()
            exported = editor_export("graph-before-layout.json")
            disconnected_id = (
                max(node["id"] for node in exported["liteGraph"]["nodes"]) + 1
            )
            exported["liteGraph"]["nodes"].append(
                {
                    "id": disconnected_id,
                    "type": "hict/histogram",
                    "pos": [451, 603],
                    "size": [240, 80],
                    "properties": {},
                    "title": "Saved disconnected inspector",
                }
            )
            exported["viewport"] = {"scale": 0.7, "offset": [123, 45]}
            page.locator('.pipeline-root input[type="file"]').set_input_files(
                {
                    "name": "custom-layout.json",
                    "mimeType": "application/json",
                    "buffer": json.dumps(exported).encode(),
                }
            )
            page.wait_for_function(
                "document.querySelector('.graph-canvas').data.graph._nodes.some(node=>node.title==='Saved disconnected inspector')"
            )
            with page.expect_response("**/render_pipeline/set") as response:
                page.locator(".pipeline-root .modal-footer").get_by_role(
                    "button", name="Apply", exact=True
                ).click()
            assert response.value.ok, response.value.text()
            editor_close()
            page.evaluate("""()=>{const m=window.__hictBenchmark.manager();
                const styles=m.getLayersManager().getTrackStylePreset();
                styles.contigs.width=7;styles.contigs.labelSize=17;styles.scaffolds.width=11;
                m.getLayersManager().applyTrackStylePreset(styles);}""")
            baseline["styles"] = page.evaluate(
                "window.__hictBenchmark.manager().getLayersManager().getTrackStylePreset()"
            )
            baseline["background"] = page.evaluate(
                "document.querySelector('#app').__vue_app__.config.globalProperties.$pinia._s.get('styles').mapBackgroundColor.RGBA"
            )
            translucent = options()
            translucent["colormap"][
                "startColorRGBAString"
            ] = "rgba(255,255,255,0.498039)"
            translucent["colormap"]["endColorRGBAString"] = "rgba(232,0,0,0.368627)"
            translucent["renderPipeline"] = config()
            api("/set_visualization_options", translucent)
            baseline["config"] = config()
            baseline["options"] = options()
            assert (
                baseline["options"]["colormap"]["startColorRGBAString"]
                == "rgba(255,255,255,0.498039)"
            )
            assert (
                baseline["options"]["colormap"]["endColorRGBAString"]
                == "rgba(232,0,0,0.368627)"
            )
            assert_values_and_pixels()
            page.get_by_role("button", name="Manage…", exact=True).click()
            count = picker.locator("option").count()
            group.get_by_title(
                "Save applied visualization and complete rendering graph", exact=True
            ).click()
            expect(picker.locator("option")).to_have_count(count + 1)
            page.get_by_role("button", name="Rename preset", exact=True).last.click()
            field = page.get_by_role(
                "textbox", name="Name of visualization preset", exact=True
            )
            field.fill(preset_name)
            field.press("Enter")
            _, document = export_presets("full-presets.json")
            saved = selected_preset(document)
            assert saved["trackStyles"] == baseline["styles"]
            assert saved["backgroundColor"] == baseline["background"]
            assert {
                key: saved["renderPipeline"][key] for key in baseline["config"]
            } == baseline["config"]
            state = saved["renderPipeline"]["editorState"]
            assert state["viewport"] == {"scale": 0.7, "offset": [123, 45]}, state
            assert any(
                node["id"] == disconnected_id
                and node.get("title") == "Saved disconnected inspector"
                for node in state["liteGraph"]["nodes"]
            ), state
            baseline.update(
                saved=copy.deepcopy(saved),
                document=document,
                disconnectedId=disconnected_id,
            )

        def apply_roundtrip():
            page.get_by_role(
                "button", name="Remove visualization preset", exact=True
            ).last.click()
            expect(
                page.get_by_role("button", name=preset_name, exact=True)
            ).to_have_count(0)
            picker.select_option(label="Red on black")
            check.settle()
            before_import = state()
            import_presets(baseline["document"], "imported-full-presets.json")
            page.get_by_role("button", name=preset_name, exact=True).wait_for()
            assert state() == before_import
            assert config() != baseline["config"]
            page.evaluate("""()=>{const m=window.__hictBenchmark.manager();
                const styles=m.getLayersManager().getTrackStylePreset();
                styles.contigs.width=2;styles.contigs.labelSize=12;styles.scaffolds.width=4;
                m.getLayersManager().applyTrackStylePreset(styles);}""")
            assert state()["styles"] != baseline["styles"]
            assert state()["background"] != baseline["background"]
            with page.expect_response("**/set_visualization_options") as response:
                picker.select_option(label=preset_name)
            assert response.value.ok, response.value.text()
            assert config() == baseline["config"]
            assert options() == baseline["options"]
            restored_style = state()
            assert restored_style["styles"] == baseline["styles"]
            assert restored_style["background"] == baseline["background"]
            assert_values_and_pixels()
            editor_open()
            restored = editor_export("restored-editor-graph.json")
            assert restored["viewport"] == {
                "scale": 0.7,
                "offset": [123, 45],
            }, restored["viewport"]
            assert any(
                node.get("title") == "Saved disconnected inspector"
                for node in restored["liteGraph"]["nodes"]
            )
            editor_close()

        def state():
            return page.evaluate(
                """async()=>{const m=window.__hictBenchmark.manager();
                const pinia=document.querySelector('#app').__vue_app__.config.globalProperties.$pinia;
                return {config:await m.networkManager.requestManager.getRenderPipelineConfig(),
                    options:(await m.networkManager.requestManager.getSessionSnapshot()).visualizationOptions,
                    clientOptions:JSON.parse(JSON.stringify(pinia._s.get('visualizationOptions').asVisualizationOptions())),
                    background:pinia._s.get('styles').mapBackgroundColor.RGBA,
                    styles:m.getLayersManager().getTrackStylePreset(),
                    center:m.getView().getCenter(),resolution:m.getView().getResolution()};}"""
            )

        def rejected_inputs():
            for input_expression, message in [
                ({"type": "source", "source": "SECONDARY"}, "secondary source"),
                (
                    {
                        "type": "track1d",
                        "trackId": "missing-release-track",
                        "axis": "ROW",
                    },
                    "required track",
                ),
            ]:
                record = copy.deepcopy(baseline["saved"])
                record["name"] = "Missing " + message
                record["backgroundColor"] = "#123456"
                record["renderPipeline"].pop("editorState", None)
                record["renderPipeline"]["lowerExpression"] = {
                    "type": "colormap",
                    "input": input_expression,
                    "startColor": "#ffffff",
                    "endColor": "#ff0000",
                    "minSignal": 0,
                    "maxSignal": 10,
                }
                record["options"]["postLogBase"] = 2
                import_presets(
                    {"data": {"savedVisualizationPresets": [record]}},
                    "missing-input.json",
                )
                page.get_by_role("button", name=record["name"], exact=True).wait_for()
                before = state()
                with page.expect_response("**/set_visualization_options") as response:
                    picker.select_option(label=record["name"])
                assert response.value.status == 409, response.value.text()
                assert message in response.value.text(), response.value.text()
                assert state() == before
            _, before = export_presets("before-rejected-batch.json")
            valid = copy.deepcopy(baseline["saved"])
            valid["name"] = "Rejected partial graph"
            invalid = copy.deepcopy(valid)
            invalid["name"] = "Invalid graph"
            invalid["renderPipeline"]["upperExpression"] = {"type": "unknown"}
            before_state = state()
            import_presets(
                {"data": {"savedVisualizationPresets": [valid, invalid]}},
                "invalid-full-batch.json",
            )
            page.get_by_text(
                "Cannot import visualization options", exact=False
            ).first.wait_for()
            _, after = export_presets("after-rejected-batch.json")
            assert before == after and state() == before_state
            invalid_style = copy.deepcopy(baseline["saved"])
            invalid_style["name"] = "Invalid graph style"
            invalid_style["trackStyles"] = {"contigs": {"width": "invalid"}}
            import_presets(
                {"data": {"savedVisualizationPresets": [invalid_style]}},
                "invalid-graph-style.json",
            )
            page.get_by_text(
                "Cannot import visualization options", exact=False
            ).first.wait_for()
            page.wait_for_function(
                "!document.querySelector('.saved-visualization-options-div')?.textContent.includes('Invalid graph style')"
            )
            _, rejected_style = export_presets("after-rejected-style.json")
            assert rejected_style == before and state() == before_state
            expected_console = "Failed to load resource: the server responded with a status of 409 (Conflict)"
            assert check.errors == [expected_console, expected_console], check.errors
            check.errors.clear()
            results.append({"expectedHTTPConflicts": 2})
            results.append({"missingInputsPreserved": True, "atomicInvalidBatch": True})

        def native_session():
            with page.expect_download() as download:
                check.menu("File", "Save session")
            path = output / "full-graph-session.json"
            download.value.save_as(path)
            document = json.loads(path.read_text())
            library = document["client"]["savedVisualizationPresets"]
            saved = next(item for item in library if item["name"] == preset_name)
            assert saved["renderPipeline"] == baseline["saved"]["renderPipeline"]
            preserved_id = saved["option_id"]
            check.menu("File", "Close")
            page.wait_for_function("!window.__hictBenchmark.manager()")
            page.evaluate(
                "Object.keys(sessionStorage).filter(key=>key.startsWith('hict.renderPipeline.editor.v1:')).forEach(key=>sessionStorage.removeItem(key))"
            )
            with page.expect_file_chooser() as chooser:
                check.menu("File", "Open session")
            chooser.value.set_files(path)
            page.get_by_text(
                "Session restored. Undo history starts from the restored assembly.",
                exact=True,
            ).wait_for()
            if not group.is_visible():
                page.get_by_role("button", name="Manage…", exact=True).click()
            _, restored = export_presets("native-restored-presets.json")
            restored_record = selected_preset(restored)
            assert restored_record["renderPipeline"] == saved["renderPipeline"]
            assert restored_record["option_id"] == preserved_id
            with page.expect_response("**/set_visualization_options") as response:
                picker.select_option(label=preset_name)
            assert response.value.ok, response.value.text()
            assert config() == baseline["config"]
            assert_values_and_pixels()
            editor_open()
            restored_graph = editor_export("native-restored-editor.json")
            assert any(
                node.get("title") == "Saved disconnected inspector"
                for node in restored_graph["liteGraph"]["nodes"]
            )
            assert restored_graph["viewport"] == {"scale": 0.7, "offset": [123, 45]}
            editor_close()
            results.append({"nativeGraphAndLayout": True, "presetId": preserved_id})

        def deep_graph_session():
            deep = copy.deepcopy(baseline["config"])
            expression = deep["upperExpression"]["input"]
            for _ in range(100):
                expression = {"type": "unary", "op": "ABS", "input": expression}
            deep["upperExpression"]["input"] = expression
            page.evaluate(
                """async encoded=>{const graph=JSON.parse(encoded);const m=window.__hictBenchmark.manager();
                await m.networkManager.requestManager.setRenderPipelineConfig(graph);
                await m.reloadTilesFromBackend();}""",
                json.dumps(deep),
            )
            count = picker.locator("option").count()
            group.get_by_title(
                "Save applied visualization and complete rendering graph", exact=True
            ).click()
            expect(picker.locator("option")).to_have_count(count + 1)
            page.get_by_role("button", name="Rename preset", exact=True).last.click()
            field = page.get_by_role(
                "textbox", name="Name of visualization preset", exact=True
            )
            field.fill("Deep rendering graph")
            field.press("Enter")
            _, exported = export_presets("deep-graph-presets.json")
            saved = next(
                item
                for item in exported["data"]["savedVisualizationPresets"]
                if item["name"] == "Deep rendering graph"
            )
            assert (
                saved["renderPipeline"] == deep
            ), "A stale shallow layout must not replace a deeper applied graph"
            with page.expect_download() as download:
                check.menu("File", "Save session")
            path = output / "deep-graph-session.json"
            download.value.save_as(path)
            check.menu("File", "Close")
            page.wait_for_function("!window.__hictBenchmark.manager()")
            with page.expect_file_chooser() as chooser:
                check.menu("File", "Open session")
            chooser.value.set_files(path)
            page.get_by_text(
                "Session restored. Undo history starts from the restored assembly.",
                exact=True,
            ).wait_for()
            assert config() == deep
            if not group.is_visible():
                page.get_by_role("button", name="Manage…", exact=True).click()
            _, restored = export_presets("deep-native-restored-presets.json")
            restored_record = next(
                item
                for item in restored["data"]["savedVisualizationPresets"]
                if item["name"] == "Deep rendering graph"
            )
            assert restored_record == saved
            assert_values_and_pixels()
            results.append({"deepGraphUnaryNodes": 100, "nativeAndLibraryExact": True})

        def expected_mode_roundtrip():
            page.evaluate("""async()=>{const m=window.__hictBenchmark.manager();
                const store=document.querySelector('#app').__vue_app__.config.globalProperties.$pinia._s.get('visualizationOptions');
                const options=store.asVisualizationOptions();options.signalDisplayMode='EXPECTED';
                const pipeline=await m.networkManager.requestManager.getRenderPipelineConfig();pipeline.enabled=false;
                await m.visualizationManager.sendVisualizationOptionsToServer({skipAutoThresholdRefresh:true,
                    visualizationOptions:options,renderPipeline:pipeline});
                await m.reloadTilesFromBackend();}""")
            count = picker.locator("option").count()
            group.get_by_title(
                "Save applied visualization and complete rendering graph", exact=True
            ).click()
            expect(picker.locator("option")).to_have_count(count + 1)
            page.get_by_role("button", name="Rename preset", exact=True).last.click()
            field = page.get_by_role(
                "textbox", name="Name of visualization preset", exact=True
            )
            field.fill("Expected signal preset")
            field.press("Enter")
            expected = query("TRADITIONAL_NORMALIZED")
            diagonal_means = []
            for distance in range(5):
                weighted_contacts = [
                    original_raw[row * 5 + row + distance]
                    * (row + 2)
                    * (row + distance + 2)
                    for row in range(5 - distance)
                    if original_raw[row * 5 + row + distance] != 0
                ]
                diagonal_means.append(
                    sum(weighted_contacts) / len(weighted_contacts)
                    if weighted_contacts
                    else 0
                )
            oracle = [
                diagonal_means[abs(row - col)] for row in range(5) for col in range(5)
            ]
            assert (
                all(abs(a - b) < 1e-5 for a, b in zip(expected, oracle))
                and len(expected) == 25
            ), (expected, oracle)
            picker.select_option(label="Red on black")
            check.settle()
            with page.expect_response("**/visualization/expected_profile") as profile:
                with page.expect_response("**/set_visualization_options") as response:
                    picker.select_option(label="Expected signal preset")
                assert response.value.ok, response.value.text()
            assert profile.value.ok, profile.value.text()
            assert options()["signalDisplayMode"] == "EXPECTED"
            assert not config()["enabled"]
            restored = query("TRADITIONAL_NORMALIZED")
            assert restored == expected, (restored, expected)
            assert query("RAW_COUNTS") == original_raw
            results.append(
                {
                    "expectedProfileRequest": profile.value.request.post_data_json,
                    "independentExpectedSignal": oracle,
                    "restoredExpectedSignal": restored,
                }
            )

        baseline = {}
        try:
            page.goto(args.url)
            page.wait_for_function("window.__hictBenchmark")
            page.evaluate(
                "api=>window.__hictBenchmark.open('tiny-weighted.hict.hdf5',api)",
                args.api,
            )
            check.settle()
            original_raw = query("RAW_COUNTS")
            check.step("save-applied-full-graph-and-editor-layout", save_applied_graph)
            check.step(
                "json-library-roundtrip-numerics-and-rendered-pixels", apply_roundtrip
            )
            check.step("missing-input-and-invalid-batch-preservation", rejected_inputs)
            check.step(
                "native-session-full-preset-layout-and-stable-id", native_session
            )
            check.step(
                "deep-custom-graph-native-session-and-library", deep_graph_session
            )
            check.step(
                "expected-profile-initialization-on-full-preset-apply",
                expected_mode_roundtrip,
            )
            assert not check.errors, check.errors
        finally:
            (output / "results.json").write_text(
                json.dumps(
                    {"checks": results, "cases": check.rows, "errors": check.errors},
                    indent=2,
                )
                + "\n"
            )
            browser.close()


if __name__ == "__main__":
    main()
