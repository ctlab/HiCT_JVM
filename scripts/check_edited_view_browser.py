#!/usr/bin/env python3
"""Validate edited FASTA, selection histograms and border exports on physical copies."""
import argparse
import json
import xml.etree.ElementTree as ET
from pathlib import Path

import numpy as np
from playwright.sync_api import sync_playwright


def first_sequence(path):
    with path.open() as handle:
        name = handle.readline().removeprefix(">").split()[0]
        chunks = []
        for line in handle:
            if line.startswith(">"):
                break
            chunks.append(line.strip())
    return name, "".join(chunks).upper()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:8187")
    parser.add_argument("--api", default="http://127.0.0.1:5107")
    parser.add_argument("--data-dir", required=True)
    parser.add_argument("--output", default="/tmp/hict-release-physical/edited-view")
    args = parser.parse_args()
    data = Path(args.data_dir).absolute()
    if data != data.resolve() or not data.is_relative_to("/tmp"):
        raise ValueError("Use a nonsymlinked disposable DATA directory under /tmp")
    for name in ("combined_ind2_4DN.hict.hdf5", "ind2.self.hict.hdf5", "quad_combined_ind2.fasta"):
        path = data / name
        if not path.is_file() or path.is_symlink() or path.stat().st_nlink != 1:
            raise ValueError(f"Use physical input copies: {path}")
    name, sequence = first_sequence(data / "quad_combined_ind2.fasta")
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
        page.add_init_script("""window.__HICT_BENCHMARK_ENABLED__=true;
            const fill=CanvasRenderingContext2D.prototype.fillRect;
            CanvasRenderingContext2D.prototype.fillRect=function(...args) {
                if(this.canvas.classList.contains('sequence-axis'))this.canvas.__letters=[];
                return fill.apply(this,args);
            };
            const original=CanvasRenderingContext2D.prototype.fillText;
            CanvasRenderingContext2D.prototype.fillText=function(text,x,y,...args) {
                if(this.canvas.classList.contains('sequence-axis')) {
                    const calls=this.canvas.__letters ||= [];
                    calls.push({text,x,y,color:this.fillStyle});
                    if(calls.length>10000)calls.splice(0,5000);
                }
                return original.call(this,text,x,y,...args);
            };""")
        page.route("http://localhost:5000/**", lambda route: route.continue_(
            url=args.api + route.request.url.removeprefix("http://localhost:5000")))
        try:
            page.goto(args.url)
            page.wait_for_function("window.__hictBenchmark")
            page.evaluate("api=>window.__hictBenchmark.open('combined_ind2_4DN.hict.hdf5',api)", args.api)
            page.wait_for_timeout(1500)
            button = page.get_by_role("button", name="Do not use Cooler weights", exact=True)
            if button.is_visible():
                button.click()
            descriptor = page.evaluate("window.__hictBenchmark.manager().contigDimensionHolder.contigDescriptors[0]")
            assert descriptor["contigName"] == name, descriptor
            response = page.request.post(args.api + "/link_fasta", data={"fastaFilename": "quad_combined_ind2.fasta"})
            assert response.ok, response.text()
            page.evaluate("""() => {const m=window.__hictBenchmark.manager(),l=m.viewAndLayersManager;
                const p=l.currentViewState.resolutionDesciptor.pixelResolution;
                l.selectionInteractions.contigSelectExtent.setExtent([p,-2*p,2*p,-p]);
                const v=m.getView();v.setResolution(v.getMinResolution());v.setCenter([0.1,-0.1]);
                m.getMap().renderSync();} """)
            for axis in ("horizontal", "vertical"):
                page.locator(f".sequence-axis.{axis}").wait_for(state="visible")
            page.wait_for_function("document.querySelector('.sequence-axis.horizontal').__letters?.length>0")
            before = page.evaluate("""() => {const v=window.__hictBenchmark.manager().getView();
                return [v.getCenter(),v.getResolution(),v.getRotation()];}""")
            region = page.request.post(args.api + "/sequence/region", data={"startBp": 0, "endBp": 100}).json()
            assert region["sequence"].upper() == sequence[:100]
            page.evaluate("document.querySelectorAll('.sequence-axis').forEach(c=>c.__letters=[])")
            with page.expect_response("**/reverse_selection_range") as edited:
                page.evaluate("window.__hictBenchmark.manager().eventManager.onReverseSelectionClicked()")
            assert edited.value.ok, edited.value.text()
            page.wait_for_function("document.querySelector('.sequence-axis.horizontal').__letters?.length>0")
            page.wait_for_timeout(1000)
            after = page.evaluate("""() => {const v=window.__hictBenchmark.manager().getView();
                return [v.getCenter(),v.getResolution(),v.getRotation()];}""")
            assert before == after, {"before": before, "after": after}
            complement = str.maketrans("ACGTRYMKBDHVN", "TGCAYRKMVHDBN")
            expected = sequence.translate(complement)[::-1]
            for start in (0, len(sequence) - 100):
                response = page.request.post(args.api + "/sequence/region", data={"startBp": start, "endBp": start + 100})
                assert response.ok, response.text()
                assert response.json()["sequence"].upper() == expected[start:start + 100]
            letters = page.evaluate("""() => {const m=window.__hictBenchmark.manager();
                const d=m.viewAndLayersManager.currentViewState.resolutionDesciptor;
                return Array.from(document.querySelectorAll('.sequence-axis')).flatMap(canvas=>
                    canvas.__letters.map(letter=>{
                        const horizontal=canvas.classList.contains('horizontal');
                        const coordinate=m.getMap().getCoordinateFromPixel(horizontal?[letter.x,0]:[0,letter.y]);
                        const px=(horizontal?coordinate[0]:-coordinate[1])/d.pixelResolution;
                        return {...letter,axis:horizontal?'horizontal':'vertical',
                            bp:Math.floor(m.contigDimensionHolder.getFractionalBpOfPx(px,d.bpResolution))};
                    }));}""")
            assert letters
            colors = {"A": "#16823b", "C": "#1769c2", "G": "#996600", "T": "#c52a30"}
            for letter in letters:
                assert letter["text"] == expected[letter["bp"]], letter
                assert letter["color"] == colors.get(letter["text"], "#666666"), letter
            page.screenshot(path=str(output / "reversed-fasta.png"))
            results.append({"test": "reversal preserves viewport and updates FASTA", "contig": name,
                            "length": len(sequence), "verifiedBases": 200, "drawnLetters": len(letters)})

            page.evaluate("""async () => {const m=window.__hictBenchmark.manager(),l=m.viewAndLayersManager;
                const s=await m.networkManager.requestManager.openSecondarySource('ind2.self.hict.hdf5',true);
                l.setSecondaryResolutionModel(s.compatibility);l.fitViewToActiveExtent();
                const col=source=>({type:'colormap',mode:'LINEAR',input:{type:'binary',op:'MUL',
                    left:{type:'source',source},right:{type:'constant',value:2}},
                    minSignal:0,maxSignal:100,startColor:'#ffffff00',endColor:'#ff0000ff'});
                await m.networkManager.requestManager.setRenderPipelineConfig({enabled:true,
                    upperExpression:col('PRIMARY'),lowerExpression:col('SECONDARY'),swapUpperLower:false});
                await m.reloadTilesFromBackend();
            }""")
            page.wait_for_timeout(1000)
            point = page.evaluate("""() => {const m=window.__hictBenchmark.manager(),l=m.viewAndLayersManager;
                const p=l.currentViewState.resolutionDesciptor.pixelResolution;
                l.selectionInteractions.contigSelectExtent.setExtent([10*p,-42*p,42*p,-10*p]);
                return m.getMap().getPixelFromCoordinate([26*p,-26*p]);}""")
            box = page.locator("#hic-contact-map").bounding_box()
            page.mouse.click(box["x"] + point[0], box["y"] + point[1], button="right")
            queries.clear()
            page.get_by_role("menuitem", name="Signal histogram...", exact=True).click()
            dialog = page.locator(".histogram-dialog")
            for source, mode, triangle in (("PRIMARY", "RAW_COUNTS", "both"),
                                           ("PRIMARY", "RAW_COUNTS", "upper"),
                                           ("SECONDARY", "RAW_COUNTS", "lower"),
                                           ("PRIMARY", "COOLER_WEIGHTED", "both"),
                                           ("PRIMARY", "PIPELINE_SIGNAL", "both")):
                dialog.locator("select").nth(0).select_option(source)
                dialog.locator("select").nth(1).select_option(mode)
                dialog.locator("select").nth(2).select_option(triangle)
                page.locator(".histogram-chart svg").wait_for(state="visible")
                query = queries[-1]
                oracle_query = dict(query, format="JSON")
                response = page.request.post(args.api + "/matrix/query", data=oracle_query)
                assert response.ok, response.text()
                payload = response.json()
                values = np.array(payload["values"], dtype=np.float32).reshape(payload["rows"], payload["cols"])
                rows, cols = np.indices(values.shape)
                rows += query["startRowPx"]
                cols += query["startColPx"]
                mask = np.ones(values.shape, dtype=bool) if triangle == "both" else (
                    rows <= cols if triangle == "upper" else rows >= cols)
                values = values[mask].astype(np.float64)
                values = values[np.isfinite(values)]
                low, high = values.min(), values.max()
                indices = np.zeros(len(values), dtype=int) if low == high else np.minimum(
                    63, np.floor(64 * (values - low) / (high - low)).astype(int))
                actual = page.locator(".histogram-chart rect").evaluate_all("nodes=>nodes.map(n=>Number(n.dataset.count))")
                assert actual == np.bincount(indices, minlength=64).tolist(), (source, mode, triangle, query)
                results.append({"test": "selection histogram", "source": source, "mode": mode,
                                "triangle": triangle, "count": len(values), "min": low, "max": high})
            page.screenshot(path=str(output / "selection-histogram.png"))
            dialog.get_by_role("button", name="Close", exact=True).click()
            page.evaluate("window.__hictBenchmark.manager().eventManager.resetSelection()")
            for outline in (False, True):
                page.evaluate("""outline => {const l=window.__hictBenchmark.manager().viewAndLayersManager;
                    const preset=l.getTrackStylePreset();
                    for(const style of Object.values(preset)) {
                        style.width=1;style.borderOutline=outline;style.borderColor='#00cc44';
                        style.labelOutline=false;
                    }l.applyTrackStylePreset(preset);}""", outline)
                with page.expect_download() as download:
                    page.evaluate("window.__hictBenchmark.manager().exportCurrentMapSvg(undefined,{scope:'viewport',cropToMap:true})")
                target = output / f"border-outline-{str(outline).lower()}.svg"
                download.value.save_as(target)
                root = ET.parse(target).getroot()
                strokes = [element.attrib for element in root.iter()
                           if element.get("stroke") not in (None, "none")]
                borders = [s for s in strokes if s["stroke"].upper() == "#00CC44"]
                outlines = [s for s in strokes if s["stroke"].upper() == "#000000"
                            and s.get("stroke-width") == "4"]
                assert borders and all(s.get("stroke-width") == "1" for s in borders), borders
                assert len(outlines) == (len(borders) if outline else 0), outlines
                results.append({"test": "border export", "outline": outline, "bytes": target.stat().st_size,
                                "strokes": strokes})
            assert not errors, errors
            (output / "results.json").write_text(json.dumps(results, indent=2) + "\n")
        finally:
            (output / "diagnostics.json").write_text(json.dumps({"errors": errors, "queries": queries}, indent=2))
            page.screenshot(path=str(output / "final.png"))
            browser.close()
    print(json.dumps([{k: v for k, v in row.items() if k != "strokes"} for row in results], indent=2))


if __name__ == "__main__":
    main()
