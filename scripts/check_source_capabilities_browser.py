#!/usr/bin/env python3
"""Check real source capabilities, controls, Attach, and exact tiny matrix values."""
import argparse
import json
from pathlib import Path

from playwright.sync_api import sync_playwright
from browser_review_helpers import select_visualization_source


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True)
    parser.add_argument("--api", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    results, errors, failures = [], [], []
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(viewport={"width": 1440, "height": 1000})
        context.add_init_script("window.__HICT_BENCHMARK_ENABLED__=true")
        context.route("http://localhost:5000/**", lambda route: route.continue_(
            url=args.api + route.request.url.removeprefix("http://localhost:5000")))
        page = context.new_page()
        page.set_default_timeout(15000)
        page.on("pageerror", lambda error: errors.append(str(error)))
        page.on("response", lambda response: failures.append({"url": response.url, "status": response.status})
                if response.status >= 400 else None)

        def open_file(filename):
            page.evaluate("args=>window.__hictBenchmark.open(args.filename,args.api)", {"filename": filename, "api": args.api})
            page.wait_for_function("window.__hictBenchmark.manager()?.networkManager.requestManager.sourceCapabilities.PRIMARY")
            prompt = page.get_by_role("button", name="Do not use Cooler weights", exact=True)
            if prompt.is_visible():
                prompt.click()
            details = page.locator("#signal-advanced")
            if details.get_attribute("open") is None:
                details.locator("summary").click()

        def resolution(bp):
            # A 17 bp map fits entirely at the finest level. Select a single
            # available level to test its capability without bypassing view bounds.
            page.evaluate("bp=>{const m=window.__hictBenchmark.manager();m.viewAndLayersManager.setEnabledBpResolutions([bp]);m.getView().setResolution(m.viewAndLayersManager.getPixelResolutionForBpResolution(bp));}", bp)
            page.wait_for_function("bp=>window.__hictBenchmark.manager().viewAndLayersManager.currentViewState.resolutionDesciptor.bpResolution===bp", arg=bp)

        def assert_disabled(selector, expected):
            page.wait_for_function("args=>document.querySelector(args.selector)?.disabled===args.expected", arg={"selector": selector, "expected": expected})
            element = page.locator(selector)
            if expected:
                reason = element.get_attribute("aria-describedby")
                assert reason and page.locator("#" + reason).inner_text().strip(), "Unavailable control needs a readable reason"

        def assert_weights_disabled(expected):
            # Weights from the file are one option of the Normalization select: unavailable when the
            # option is disabled, or when the whole select is (identity sources have no contact weights).
            page.wait_for_function(
                "expected=>{const s=document.querySelector('#normalization-mode');if(!s)return false;"
                "const o=s.querySelector('option[value=file]');return (s.disabled||o.disabled)===expected}",
                arg=expected)
            if expected:
                reason = page.locator("#imported-weights-unavailable, #contact-normalization-unavailable").first
                assert reason.inner_text().strip(), "Unavailable weights need a readable reason"

        try:
            page.goto(args.url)
            page.wait_for_function("window.__hictBenchmark")
            open_file("tiny-weighted.hict.hdf5")
            primary = page.evaluate("window.__hictBenchmark.manager().networkManager.requestManager.sourceCapabilities.PRIMARY")
            assert primary["units"] == "CONTACT_COUNT" and primary["contactDataState"] == "PRESENT", primary
            assert primary["importedWeightResolutions"] == [4] and primary["weightMetadataComplete"], primary
            resolution(4)
            assert_weights_disabled(False)
            assert_disabled("#scaling-across-zoom", False)
            values = page.evaluate("""async () => Array.from((await window.__hictBenchmark.manager().networkManager.requestManager.queryMatrixFloat32({
                bpResolution:4,startRowPx:0,startColPx:0,endRowPx:5,endColPx:5,source:'PRIMARY',signalMode:'RAW_COUNTS'})).values)""")
            assert values == [1,3,0,0,0,3,0,7,0,0,0,7,0,0,0,0,0,0,0,0,0,0,0,0,11], values
            resolution(10)
            assert_weights_disabled(True)
            results.append({"test": "real matrix values and per-resolution imported weight availability", "pixels": len(values)})
            page.screenshot(path=str(output / "unweighted-resolution.png"))

            page.evaluate("""async () => {const m=window.__hictBenchmark.manager();
                const status=await m.networkManager.requestManager.openSecondarySource('tiny-identity.hict.hdf5',true);
                m.viewAndLayersManager.setSecondaryResolutionModel(status.compatibility);
                const color=(source,maxSignal)=>({type:'colormap',mode:'LINEAR',input:{type:'source',source},
                    minSignal:0,maxSignal,startColor:'#ffffff00',endColor:'#ff0000ff'});
                await m.networkManager.requestManager.setRenderPipelineConfig({enabled:true,swapUpperLower:false,
                    upperExpression:color('PRIMARY',11),lowerExpression:color('SECONDARY',1000000)});
                await m.reloadTilesFromBackend();} """)
            select_visualization_source(page, "SECONDARY")
            resolution(4)
            assert_weights_disabled(True)
            assert_disabled("#scaling-across-zoom", True)
            assert_disabled("#scaling-across-zoom", True)
            capability = page.evaluate("window.__hictBenchmark.manager().networkManager.requestManager.sourceCapabilities.SECONDARY")
            assert capability["units"] == "IDENTITY_PERCENT" and capability["scoreScale"] == 10000, capability
            scores = page.evaluate("""async () => Array.from((await window.__hictBenchmark.manager().networkManager.requestManager.queryMatrixFloat32({
                bpResolution:4,startRowPx:0,startColPx:0,endRowPx:5,endColPx:5,source:'SECONDARY',signalMode:'RAW_COUNTS'})).values)""")
            assert [score / capability["scoreScale"] for score in scores] == [90 if row == col else 0 for row in range(5) for col in range(5)], scores
            page.screenshot(path=str(output / "identity-controls.png"))
            select_visualization_source(page, "PRIMARY")
            assert_weights_disabled(False)
            results.append({"test": "identity capability follows selected source without affecting primary controls"})

            # Attach uses a fresh window, not retained state from openFile().
            attached = context.new_page()
            attached.on("pageerror", lambda error: errors.append(str(error)))
            attached.goto(args.url)
            attached.get_by_role("link", name="File", exact=True).click()
            attached.get_by_role("link", name="Attach", exact=True).click()
            attached.wait_for_function("window.__hictBenchmark.manager()?.networkManager.requestManager.sourceCapabilities.PRIMARY?.units==='CONTACT_COUNT'")
            attached.wait_for_function("window.__hictBenchmark.manager()?.networkManager.requestManager.sourceCapabilities.SECONDARY?.units==='IDENTITY_PERCENT'")
            attached.close()
            results.append({"test": "fresh-window File Attach restores primary and secondary capabilities"})

            open_file("tiny-identity.hict.hdf5")
            assert_disabled("#scaling-across-zoom", True)
            assert_weights_disabled(True)
            results.append({"test": "identity remains non-contact when opened as primary"})
            open_file("combined_ind2_4DN.hict.hdf5")
            modal = page.get_by_role("button", name="Do not use Cooler weights", exact=True)
            if modal.is_visible():
                modal.click()
            assert_disabled("#scaling-across-zoom", False)
            assert_weights_disabled(False)
            legacy = page.evaluate("window.__hictBenchmark.manager().networkManager.requestManager.sourceCapabilities.PRIMARY")
            assert legacy["units"] == "UNKNOWN", legacy
            assert legacy["importedWeights"]["state"] == "UNKNOWN", legacy
            results.append({"test": "legacy unknown metadata preserves previously available controls"})
            assert not errors, errors
            assert not failures, failures
        finally:
            (output / "results.json").write_text(json.dumps({"results": results, "errors": errors, "httpErrors": failures}, indent=2) + "\n")
            page.screenshot(path=str(output / "final.png"))
            browser.close()
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
