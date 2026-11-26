#!/usr/bin/env python3
"""Check Expected/O/E round trips preserve identity graphs, pixels and sessions.

Requires disposable tiny-primary.hict.hdf5, tiny-identity.hict.hdf5 and
tiny-two-scaffolds.agp fixtures.
Never point this acceptance check at a user's live server.
"""

import argparse
import io
import json
from pathlib import Path

import numpy as np
from PIL import Image
from playwright.sync_api import sync_playwright

from sequence_browser_harness import BrowserCheck, configure_test_api


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("url", "api", "output"):
        parser.add_argument("--" + name, required=True)
    args = parser.parse_args()
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    results = []
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1440, "height": 1000})
        page.set_default_timeout(30000)
        page.add_init_script("window.__HICT_BENCHMARK_ENABLED__=true")
        configure_test_api(page, args.api)
        check = BrowserCheck(page, output)

        def post(path, data=None):
            response = page.request.post(args.api + path, data=data or {})
            assert response.ok, response.text()
            return response.json()

        def graph():
            return post("/render_pipeline/get")

        def tile(source):
            response = page.request.get(
                args.api + "/get_tile",
                params={
                    "source": source,
                    "bpResolution": 4,
                    "format": "PNG_BY_PIXELS",
                    "row": 0,
                    "col": 0,
                    "rows": 5,
                    "cols": 5,
                },
            )
            assert response.ok, response.text()
            return np.array(Image.open(io.BytesIO(response.body())).convert("RGBA"))

        def select_mode(mode):
            control = page.locator('select[aria-label="Signal display"]')
            control.select_option(mode)
            page.wait_for_function(
                """mode => {
                const control=document.querySelector('select[aria-label="Signal display"]');
                return control && !control.disabled && control.value===mode;
            }""",
                arg=mode,
            )
            check.settle()
            assert post("/get_visualization_options")["signalDisplayMode"] == mode

        def save_session(name):
            with page.expect_download() as event:
                check.menu("File", "Save session…")
            path = output / (name + ".json")
            event.value.save_as(path)
            return path, json.loads(path.read_text())

        def scaffold_boundary_signals():
            check.menu("Assembly", "Load AGP…")
            check.choose_file("tiny-two-scaffolds.agp")
            with page.expect_response("**/load_agp") as loaded:
                page.get_by_role("button", name="Load", exact=True).click()
            assert loaded.value.ok, loaded.value.text()
            page.get_by_placeholder("Filter current folder by file name or path").wait_for(state="hidden")
            page.locator("#normalization-mode").select_option("none")
            check.settle()

            def values(signal_mode):
                response = post("/matrix/query", {
                    "source": "PRIMARY", "signalMode": signal_mode, "format": "JSON",
                    "bpResolution": 4, "startRowPx": 0, "endRowPx": 5,
                    "startColPx": 0, "endColPx": 5,
                })
                return np.array(response["values"], dtype=float).reshape(5, 5)

            # The fixture's four contacts. The cut is at bp 8, exactly between bins.
            observed = np.zeros((5, 5))
            observed[0, 0], observed[4, 4] = 1, 11
            observed[0, 1] = observed[1, 0] = 3
            observed[1, 2] = observed[2, 1] = 7
            np.testing.assert_array_equal(values("RAW_COUNTS"), observed)

            # Expected fits positive finite observations per scaffold and distance.
            # Observed zeros still have defined ratios when that fitted mean is positive.
            expected = np.zeros((5, 5))
            expected[:2, :2] = [[1, 3], [3, 1]]
            for index in range(2, 5):
                expected[index, index] = 11
            select_mode("EXPECTED")
            np.testing.assert_allclose(values("TRADITIONAL_NORMALIZED"), expected)

            select_mode("OBSERVED_OVER_EXPECTED")
            supported = expected > 0
            ratios = np.divide(observed, expected, out=np.full_like(expected, np.nan), where=supported)
            np.testing.assert_allclose(values("TRADITIONAL_NORMALIZED"), ratios, equal_nan=True)
            rgba = tile("PRIMARY")
            np.testing.assert_array_equal(rgba[:, :, 3] > 0, supported)
            measured_zeros = supported & (observed == 0)
            np.testing.assert_array_equal(rgba[measured_zeros], [[33, 102, 172, 255]] * 3)
            Image.fromarray(rgba).save(output / "scaffold-boundary-oe.png")
            select_mode("OBSERVED")
            np.testing.assert_array_equal(values("TRADITIONAL_NORMALIZED"), observed)
            reported_ratios = ratios.astype(object)
            reported_ratios[~supported] = None
            results.append({
                "boundaryBp": 8, "expected": expected.tolist(), "ratios": reported_ratios.tolist(),
                "measuredZeros": int(np.count_nonzero(measured_zeros)),
                "transparentUndefinedRatios": int(np.count_nonzero(~supported)),
            })

        try:
            page.goto(args.url)
            page.wait_for_function("window.__hictBenchmark")
            # A disabled graph is not the current standard-renderer palette.
            # Keep its graph untouched while remembering the actual observed options.
            page.evaluate(
                "api => window.__hictBenchmark.open('tiny-primary.hict.hdf5', api)",
                args.api,
            )
            check.settle()
            standard_graph = graph()
            standard_pixels = tile("PRIMARY")
            for mode in ("EXPECTED", "OBSERVED_OVER_EXPECTED", "EXPECTED", "OBSERVED"):
                select_mode(mode)
                assert graph() == standard_graph
            np.testing.assert_array_equal(tile("PRIMARY"), standard_pixels)
            results.append({"standardRendererRoundTrip": True})
            page.get_by_role("slider", name="Upper threshold", exact=True).press("Home")
            page.wait_for_timeout(500)
            upper = page.get_by_label("Upper signal threshold", exact=True)
            upper.focus()
            threshold = float(upper.input_value())
            upper.blur()
            assert threshold > 1e-100, threshold
            results.append({"zeroHandleAvoidsSubnormalThreshold": threshold})
            scaffold_boundary_signals()

            for presentation in ("split", "overlay"):
                page.evaluate(
                    """async api => {
                    await window.__hictBenchmark.open('tiny-primary.hict.hdf5',api);
                    const m=window.__hictBenchmark.manager();
                    const status=await m.networkManager.requestManager.openSecondarySource('tiny-identity.hict.hdf5',true);
                    m.viewAndLayersManager.setSecondaryResolutionModel(status.compatibility);
                }""",
                    args.api,
                )
                primary = {
                    "type": "colormap",
                    "mode": "LINEAR",
                    "input": {"type": "source", "source": "PRIMARY"},
                    "startColor": "#ffffff00",
                    "endColor": "#ff0000ff",
                    "minSignal": 0,
                    "maxSignal": 11,
                }
                secondary = {
                    "type": "colormap",
                    "mode": "MODDOTPLOT_IDENTITY",
                    "input": {
                        "type": "binary",
                        "op": "DIV",
                        "left": {"type": "source", "source": "SECONDARY"},
                        "right": {"type": "constant", "value": 10000},
                    },
                    "startColor": "#31369500",
                    "endColor": "#a50026ff",
                    "minSignal": 80,
                    "maxSignal": 100,
                }
                blend = {
                    "type": "pixel_blend",
                    "mode": "OVER",
                    "top": secondary,
                    "bottom": primary,
                    "topOpacity": 0.65,
                    "bottomOpacity": 1,
                }
                expected_graph = {
                    "enabled": True,
                    "swapUpperLower": False,
                    "upperExpression": primary if presentation == "split" else blend,
                    "lowerExpression": secondary if presentation == "split" else blend,
                }
                page.evaluate(
                    """async config => {
                    const m=window.__hictBenchmark.manager();
                    await m.networkManager.requestManager.setRenderPipelineConfig(config);
                    await m.reloadTilesFromBackend();m.viewAndLayersManager.fitViewToActiveExtent();
                }""",
                    expected_graph,
                )
                check.settle()
                page.get_by_role(
                    "group", name="Visualization source", exact=True
                ).get_by_role("button", name="Secondary", exact=True).click()
                check.settle()
                baseline_primary = tile("PRIMARY")
                baseline_secondary = tile("SECONDARY")
                assert baseline_primary[:, :, 3].max() > 0
                if presentation == "overlay":
                    assert baseline_secondary[:, :, 3].max() > 0
                for sequence in [
                    ("EXPECTED", "OBSERVED"),
                    ("OBSERVED_OVER_EXPECTED", "OBSERVED"),
                    ("EXPECTED", "OBSERVED_OVER_EXPECTED", "EXPECTED", "OBSERVED"),
                ]:
                    for mode in sequence:
                        select_mode(mode)
                        assert graph() == expected_graph, (presentation, mode, graph())
                        np.testing.assert_array_equal(
                            tile("SECONDARY"), baseline_secondary
                        )
                        if mode == "OBSERVED":
                            np.testing.assert_array_equal(
                                tile("PRIMARY"), baseline_primary
                            )
                        else:
                            assert not np.array_equal(
                                tile("PRIMARY"), baseline_primary
                            ), mode
                        upper = page.get_by_label("Upper signal threshold", exact=True
                        )
                        assert float(upper.input_value()) == 100, upper.input_value()
                        lower = page.get_by_label("Lower signal threshold", exact=True
                        )
                        assert float(lower.input_value()) == 80, lower.input_value()
                    results.append(
                        {
                            "presentation": presentation,
                            "modes": sequence,
                            "graphUnchanged": True,
                            "secondaryPixelsUnchanged": True,
                            "observedPrimaryPixelsRestored": True,
                        }
                    )
                select_mode("OBSERVED_OVER_EXPECTED")
                path, document = save_session(presentation + "-oe-session")
                assert document["snapshot"]["renderPipeline"] == expected_graph
                assert (
                    document["snapshot"]["visualizationOptions"]["signalDisplayMode"]
                    == "OBSERVED_OVER_EXPECTED"
                )
                check.menu("File", "Close map")
                page.get_by_role("heading", name="No contact map open").wait_for()
                with page.expect_file_chooser() as chooser:
                    check.menu("File", "Open session…")
                chooser.value.set_files(path)
                page.get_by_text(
                    "Session restored. Undo history starts from the restored assembly."
                ).wait_for()
                check.settle()
                assert graph() == expected_graph
                select_mode("OBSERVED")
                np.testing.assert_array_equal(tile("PRIMARY"), baseline_primary)
                np.testing.assert_array_equal(tile("SECONDARY"), baseline_secondary)
                results.append(
                    {"presentation": presentation, "savedInOEAndRestoredObserved": True}
                )
                # A rejected mode change must preserve both the graph and committed mode.
                errors_before = len(check.errors)
                page.route(
                    "**/set_visualization_options",
                    lambda route: route.fulfill(
                        status=503, json={"error": "deliberate display-mode rejection"}
                    ),
                )
                control = page.locator('select[aria-label="Signal display"]')
                control.select_option("EXPECTED")
                page.wait_for_function("""() => {
                    const select=document.querySelector('select[aria-label="Signal display"]');
                    return select && !select.disabled && select.value==='OBSERVED';
                }""")
                page.unroute("**/set_visualization_options")
                check.settle()
                assert (
                    post("/get_visualization_options")["signalDisplayMode"]
                    == "OBSERVED"
                )
                assert graph() == expected_graph
                np.testing.assert_array_equal(tile("PRIMARY"), baseline_primary)
                np.testing.assert_array_equal(tile("SECONDARY"), baseline_secondary)
                expected_errors = check.errors[errors_before:]
                assert all("503" in error for error in expected_errors), expected_errors
                del check.errors[errors_before:]
                results.append(
                    {"presentation": presentation, "rejectedModePreservesState": True}
                )
                page.screenshot(path=str(output / (presentation + "-restored.png")))
            assert not check.errors, check.errors
            (output / "results.json").write_text(
                json.dumps({"status": "passed", "checks": results}, indent=2) + "\n"
            )
        except BaseException:
            page.screenshot(path=str(output / "failure.png"))
            (output / "failure.json").write_text(
                json.dumps({"checks": results, "browserErrors": check.errors}, indent=2)
            )
            raise
        finally:
            browser.close()


if __name__ == "__main__":
    main()
