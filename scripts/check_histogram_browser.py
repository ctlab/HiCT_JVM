#!/usr/bin/env python3
"""Read-only quad histogram checks using built-in weights, without track sidecars."""
import argparse
import json
from pathlib import Path

import numpy as np
from playwright.sync_api import sync_playwright


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:8186")
    parser.add_argument("--api", default="http://127.0.0.1:5106")
    parser.add_argument("--output", default="/tmp/hict-review-20261018/histogram-browser")
    args = parser.parse_args()
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    results, errors, queries = [], [], []
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1440, "height": 1000})
        page.set_default_timeout(60000)
        page.on("pageerror", lambda error: errors.append(str(error)))
        page.on("request", lambda request: queries.append(request.post_data_json)
                if request.url.endswith("/matrix/query") else None)
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
        page.get_by_role("link", name="View", exact=True).click()
        page.get_by_role("link", name="Rendering pipeline…", exact=True).click()
        page.wait_for_function("document.querySelector('.graph-canvas')?.data?.graph._nodes.length>2")

        # Inspector-only nodes do not alter either rendering branch.
        for axis in ["ROW", "COL", "SCALAR"]:
            page.evaluate("""() => {
                const graph=document.querySelector('.graph-canvas').data.graph;
                const existing=graph._nodes.filter(n=>n.type==='hict/histogram');
                for(const node of existing)graph.remove(node);
            }""")
            with page.expect_download() as download:
                page.get_by_role("button", name="Export graph", exact=True).click()
            path = output / f"graph-{axis.lower()}.json"
            download.value.save_as(path)
            payload = json.loads(path.read_text())
            nodes, links = payload["liteGraph"]["nodes"], payload["liteGraph"]["links"]
            origin = max(node["id"] for node in nodes) + 1
            nodes.extend([
                {"id": origin, "type": "hict/constant" if axis == "SCALAR" else "hict/track1d",
                 "pos": [20, 600], "size": [200, 80],
                 "properties": {"value": 37} if axis == "SCALAR" else {
                     "trackId": "__builtin_cooler_weights__", "axis": axis}},
                {"id": origin + 1, "type": "hict/histogram", "pos": [300, 600],
                 "size": [240, 80], "properties": {}}])
            links.append([max([link[0] for link in links], default=0) + 1,
                          origin, 0, origin + 1, 0, "number"])
            page.locator('.pipeline-root input[type="file"]').set_input_files({
                "name": "pipeline.json", "mimeType": "application/json", "buffer": json.dumps(payload).encode()})
            page.wait_for_function("id => document.querySelector('.graph-canvas').data.graph.getNodeById(id)", arg=origin + 1)
            queries.clear()
            page.evaluate("""() => {
                const node=document.querySelector('.graph-canvas').data.graph._nodes.find(n=>n.type==='hict/histogram');
                node.widgets.find(w=>w.name==='Inspect distribution').callback();
            }""")
            page.wait_for_function("document.querySelector('.interactive-histogram')?.data?.[0]?.y")
            actual = page.locator(".interactive-histogram").evaluate("node => node.data[0].y")
            text = page.locator(".histogram-dialog").inner_text()
            assert len(queries) == 1, queries
            query = queries[0]
            rows = query["endRowPx"] - query["startRowPx"]
            cols = query["endColPx"] - query["startColPx"]
            if axis == "SCALAR":
                assert rows == cols == 1, query
                expected_values = np.array([37.0])
            else:
                assert cols == 1 if axis == "ROW" else rows == 1, query
                oracle_query = dict(query, signalMode="RAW_COUNTS", format="JSON", includeWeights=True)
                oracle_query.pop("expression", None)
                response = page.request.post(args.api + "/matrix/query", data=oracle_query)
                assert response.ok, response.text()
                oracle = response.json()
                weights = oracle["rowWeights" if axis == "ROW" else "colWeights"]
                expected_values = np.array(weights, dtype=np.float64)
                expected_values[~np.isfinite(expected_values)] = 0
                expected_values = expected_values.astype(np.float32).astype(np.float64)
            minimum, maximum = expected_values.min(), expected_values.max()
            indices = np.zeros(expected_values.size, dtype=int) if minimum == maximum else np.minimum(
                63, np.floor(64 * (expected_values - minimum) / (maximum - minimum)).astype(int))
            expected = [expected_values.size] if minimum == maximum else np.bincount(indices, minlength=64).tolist()
            assert actual == expected, {"axis": axis, "actual": actual, "expected": expected, "query": query}
            results.append({"axis": axis, "rows": rows, "cols": cols, "count": sum(actual),
                            "min": minimum, "max": maximum, "histogram": text})
            graph_div = page.locator('.interactive-histogram')
            assert graph_div.evaluate("n=>n.layout.yaxis.type") == 'log'
            request_count = len(queries)
            page.get_by_label('Log X', exact=True).check()
            page.wait_for_function("document.querySelector('.interactive-histogram').layout.xaxis.type==='log'")
            positive = expected_values[expected_values > 0]
            logged = graph_div.evaluate('n=>n.data[0].y')
            assert sum(logged) == len(positive), (logged, positive)
            if len(positive) and positive.min() != positive.max():
                lo, hi = np.log10(positive.min()), np.log10(positive.max())
                indices = np.minimum(63, np.floor((np.log10(positive)-lo)/(hi-lo)*64).astype(int))
                assert logged == np.bincount(indices, minlength=64).tolist()
                initial_range = graph_div.evaluate('n=>n.layout.xaxis.range.slice()')
                drag = graph_div.locator('.nsewdrag').bounding_box()
                assert drag
                page.mouse.move(drag['x'] + drag['width'] * .15, drag['y'] + drag['height'] * .15)
                page.mouse.down()
                page.mouse.move(drag['x'] + drag['width'] * .65, drag['y'] + drag['height'] * .85, steps=8)
                page.mouse.up()
                page.wait_for_function("!document.querySelector('.interactive-histogram').layout.xaxis.autorange")
                zoomed = graph_div.evaluate('n=>n.layout.xaxis.range.slice()')
                assert zoomed[1] - zoomed[0] < initial_range[1] - initial_range[0]
            page.get_by_role('button', name='Autoscale both axes').click()
            page.wait_for_function("document.querySelector('.interactive-histogram').layout.xaxis.autorange===true")
            assert len(queries) == request_count, 'Changing histogram axes must not requery the map'
            page.screenshot(path=str(output / f"histogram-{axis.lower()}.png"))
            if axis == 'ROW':
                page.set_viewport_size({'width': 390, 'height': 844})
                page.wait_for_timeout(300)
                rect = page.locator('.histogram-dialog').bounding_box()
                assert rect and rect['x'] >= 0 and rect['x'] + rect['width'] <= 390
                assert graph_div.evaluate('n=>n.scrollWidth<=n.clientWidth+1')
                page.screenshot(path=str(output / 'histogram-mobile.png'))
                page.set_viewport_size({'width': 1440, 'height': 1000})
            page.locator(".histogram-dialog").get_by_role("button", name="Close", exact=True).click()

        def cancelled_matrix(route):
            if route.request.method != "POST":
                route.continue_()
                return
            route.fulfill(status=200, content_type="application/octet-stream", body=b"",
                          headers={"Access-Control-Allow-Origin": "*", "Cache-Control": "no-store",
                                   "Access-Control-Expose-Headers": "x-hict-cancelled,x-hict-rows,x-hict-cols",
                                   "x-hict-cancelled": "true", "x-hict-rows": "0", "x-hict-cols": "0"})

        page.route(args.api + "/matrix/query", cancelled_matrix)
        page.evaluate("""() => {
            const node=document.querySelector('.graph-canvas').data.graph._nodes.find(n=>n.type==='hict/histogram');
            node.widgets.find(w=>w.name==='Inspect distribution').callback();
        }""")
        dialog = page.locator(".histogram-dialog")
        alert = dialog.get_by_role("alert")
        alert.wait_for(state="visible")
        assert "interrupted" in alert.inner_text(), alert.inner_text()
        assert dialog.locator("svg").count() == 0, "Cancellation was displayed as valid signal data"
        page.screenshot(path=str(output / "histogram-cancelled.png"))
        page.unroute(args.api + "/matrix/query", cancelled_matrix)
        dialog.get_by_role("button", name="Compute", exact=True).click()
        page.wait_for_function("document.querySelector('.interactive-histogram')?.data?.[0]?.y")
        assert alert.count() == 0
        recovered = dialog.locator(".interactive-histogram").evaluate("n=>n.data[0].y")
        assert recovered == [1], recovered
        results.append({"test": "cancelled histogram shows an actionable error and recovers", "count": 1})
        assert not errors, errors
        (output / "results.json").write_text(json.dumps(results, indent=2) + "\n")
        browser.close()
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
