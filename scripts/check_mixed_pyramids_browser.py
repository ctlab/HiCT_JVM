#!/usr/bin/env python3
"""Verify mixed resolution projection through real viewport rendering."""

import argparse
import io
import json
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import numpy as np
from PIL import Image
from playwright.sync_api import sync_playwright

parser = argparse.ArgumentParser()
for name in ("url", "api", "output"):
    parser.add_argument("--" + name, required=True)
args = parser.parse_args()
output = Path(args.output)
output.mkdir(parents=True, exist_ok=True)

results = []
with sync_playwright() as pw:
    browser = pw.chromium.launch(headless=True)
    for label, levels in (
        ("fine", [1]),
        ("coarse", [25]),
        ("disjoint", [3, 7]),
        ("interleaved", [4, 10]),
    ):
        context = browser.new_context(viewport={"width": 1280, "height": 800})
        page = context.new_page()
        page.add_init_script("window.__HICT_BENCHMARK_ENABLED__=true")
        page.route("http://localhost:5000/**", lambda route: route.continue_(
            url=args.api + route.request.url.removeprefix("http://localhost:5000")))
        pending = set()
        peak_pending = [0]
        requests = []
        errors = []
        def requested(request):
            query = parse_qs(urlsplit(request.url).query)
            if "/get_tile" in request.url and query.get("source") == ["SECONDARY"]:
                pending.add(request)
                requests.append(query)
                peak_pending[0] = max(peak_pending[0], len(pending))
        def response_completed(request):
            pending.discard(request)
        page.on("request", requested)
        page.on("requestfinished", response_completed)
        page.on("requestfailed", response_completed)
        page.on("pageerror", lambda error: errors.append(str(error)))
        page.on("console", lambda msg: errors.append(msg.text) if msg.type == "error" else None)
        page.goto(args.url)
        page.wait_for_function("window.__hictBenchmark")
        attached = page.evaluate("""async ({api,label}) => {
          const b=window.__hictBenchmark;
          await b.open('mixed-primary.hict.hdf5',api);
          const m=b.manager(), req=m.networkManager.requestManager;
          m.getMap().on("rendercomplete", () => {});
          const status=await req.openSecondarySource('mixed-'+label+'.hict.hdf5',false);
          if (!status.attached || status.requiresConfirmation) return status;
          m.viewAndLayersManager.setSecondaryResolutionModel(status.compatibility);
          const cmap=(source,color)=>({type:'colormap',mode:'LINEAR',
            input:{type:'source',source},minSignal:0,maxSignal:8,
            startColor:color+'00',endColor:color+'ff'});
          await req.setRenderPipelineConfig({enabled:true,swapUpperLower:false,
            upperExpression:cmap('PRIMARY','#ff0000'), lowerExpression:cmap('SECONDARY','#0000ff')});
          await m.reloadTilesFromBackend();
          return status;
        }""", {"api": args.api, "label": label})
        assert attached["attached"] and attached["compatibility"]["sharedReference"], attached

        def wait_for_render():
            page.wait_for_function("""() => {
              const m=window.__hictBenchmark.manager(),l=m.viewAndLayersManager,map=m.getMap();
              return !l.overlayStatus.loading.value && map.renderComplete_
                && map.tileQueue_.getTilesLoading()===0 && map.tileQueue_.getCount()===0;
            }""", timeout=15000)

        def landmark(name, col_bp, row_bp, center=False):
            x, y = page.evaluate("""({colBp,rowBp,center}) => {
              const m=window.__hictBenchmark.manager(),l=m.viewAndLayersManager,map=m.getMap();
              const d=l.getVisibleSourceResolutionDescriptors().primary;
              const grid=m.contigDimensionHolder;
              const coords=[grid.getFractionalPxOfBp(colBp,d.bpResolution)*d.pixelResolution,
                -grid.getFractionalPxOfBp(rowBp,d.bpResolution)*d.pixelResolution];
              if (center) m.getView().setCenter(coords);
              map.renderSync();
              return map.getPixelFromCoordinate(coords).map(Math.round);
            }""", {"colBp": col_bp, "rowBp": row_bp, "center": center})
            wait_for_render()
            screenshot = page.locator("#hic-contact-map").screenshot()
            (output / f"{name}.png").write_bytes(screenshot)
            rgb = np.asarray(Image.open(io.BytesIO(screenshot)).convert("RGB"))
            assert 0 <= y < rgb.shape[0] and 0 <= x < rgb.shape[1], (name, x, y)
            area = rgb[max(0, y-3):y+4, max(0, x-3):x+4]
            blue = int((area[..., 2].astype(int) - area[..., 0].astype(int) > 60).sum())
            assert blue > 0, (name, x, y)
            return blue

        for iteration, primary in enumerate((10, 4, 10, 4)):
            page.evaluate("""async bp=>{
              const m=window.__hictBenchmark.manager(),l=m.viewAndLayersManager;
              const p=l.getPixelResolutionForBpResolution(bp);
              await new Promise(resolve=>m.getView().animate({resolution:p,duration:250}, resolve));
              m.getView().setCenter([0,0]);
              await l.onViewResolutionChanged();
              m.getMap().renderSync();
            }""", primary)
            wait_for_render()
            state = page.evaluate("""() => {
              const l=window.__hictBenchmark.manager().viewAndLayersManager;
              return {primary:l.getVisibleSourceResolutionDescriptors().primary.bpResolution,
                layers:l.layersHolder.secondaryHiCDataLayers.map(x=>[x.get('bpResolution'),x.get('projectionKey'),x.getOpacity()])};
            }""")
            assert state["primary"] == primary, state
            expected = levels[0] if len(levels) == 1 or primary == 4 else levels[-1]
            assert state["layers"][0][0] == expected and state["layers"][0][2] == 1, state
            assert len(state["layers"]) == 1, state
            assert any(q.get("gridBpResolution") == [str(primary)] for q in requests), requests
            blue = landmark(f"{label}-{primary}-{iteration}", 960, 1001)
            results.append({"fixture": label, "view": primary, "state": state, "landmarkPixels": blue})
        if label == "disjoint":
            with page.expect_response(lambda r: r.url.endswith("/reverse_selection_range") and r.ok):
                page.evaluate("""() => {
                  const manager=window.__hictBenchmark.manager(),events=manager.eventManager;
                  const select=events.getSelectedContigBpRange;
                  events.getSelectedContigBpRange=()=>({startBP:0,endBP:12017});
                  events.onReverseSelectionClicked();
                  events.getSelectedContigBpRange=select;
                }""")
            page.wait_for_function("window.__hictBenchmark.manager().contigDimensionHolder.contigDescriptors[0].direction !== 1")
            landmark("reversed", 11015, 11056, center=True)
            with page.expect_response(lambda r: r.url.endswith("/split_contig_at_bin") and r.ok):
                page.evaluate("window.__hictBenchmark.manager().eventManager.onClickInScissorsMode([1500,1500],4)")
            page.wait_for_function("window.__hictBenchmark.manager().contigDimensionHolder.contig_count === 2")
            landmark("split", 11011, 11052, center=True)
            page.evaluate("""() => {
              const m=window.__hictBenchmark.manager(),l=m.viewAndLayersManager;
              const d=m.contigDimensionHolder.contigDescriptors.at(-1);
              const scope={kind:"contig",id:d.contigId,label:d.contigName};
              l.setAxisScopes(scope,scope);
            }""")
            landmark("scoped-split", 11011, 11052, center=True)
            results.append({"fixture": label, "verified": ["reversed", "split", "scoped-split"]})
        assert peak_pending[0] <= 16, peak_pending
        assert not errors, errors
        print(label, "projected secondary requests", len(requests), "peak", peak_pending[0], flush=True)
        context.close()
    browser.close()
(output / "results.json").write_text(json.dumps(results, indent=2))
