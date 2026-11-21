#!/usr/bin/env python3
"""Open a fine-only Borrelia layer beyond the tile read limit and use its Jobs result."""
import argparse
import hashlib
import json
from pathlib import Path

import h5py
from playwright.sync_api import sync_playwright


def make_legacy_file(source, target):
    with h5py.File(source, "r") as current, h5py.File(target, "x") as legacy:
        current.copy("contig_info", legacy)
        current.copy("resolutions/1", legacy.create_group("resolutions"))
        del legacy["resolutions/1"].attrs["method"]
        current.copy("sequence_analysis", legacy)
        analysis = legacy["sequence_analysis"]
        settings = json.loads(analysis.attrs["compatibility"])
        del settings["analysisLevels"]
        settings["resolutions"] = [1]
        analysis.attrs.modify("compatibility", json.dumps(settings))
        for update in analysis["updates"].values():
            for level in list(update):
                if level != "1":
                    del update[level]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("url", "api", "data-dir", "session", "output"):
        parser.add_argument("--" + name, required=True)
    args = parser.parse_args()
    data, out = Path(args.data_dir).resolve(), Path(args.output)
    assert data.is_relative_to("/tmp")
    out.mkdir(parents=True, exist_ok=True)
    snapshot = json.loads(Path(args.session).read_text())["snapshot"]
    folder = data / "legacy-fixture"
    folder.mkdir(exist_ok=True)
    legacy = folder / "fine-only.hict.hdf5"
    make_legacy_file(data / snapshot["secondary"]["filename"], legacy)
    before = hashlib.sha256(legacy.read_bytes()).hexdigest()
    with sync_playwright() as pw:
        browser = pw.chromium.launch()
        page = browser.new_page(viewport={"width": 1440, "height": 900})
        page.route("http://localhost:5000/**", lambda r: r.continue_(
            url=args.api + r.request.url.removeprefix("http://localhost:5000")))
        page.add_init_script("window.__HICT_BENCHMARK_ENABLED__=true")
        page.goto(args.url)
        page.wait_for_function("window.__hictBenchmark")
        page.evaluate("([name, api])=>window.__hictBenchmark.open(name, api)",
                      [snapshot["primary"]["filename"], args.api])
        weights = page.get_by_role("button", name="Do not use Cooler weights", exact=True)
        if weights.is_visible():
            weights.click()
        filename = legacy.relative_to(data).as_posix()
        with page.expect_response(lambda r: r.url.endswith("/sequence/overview/jobs")) as requested:
            page.evaluate("""async filename=>{
                const m=window.__hictBenchmark.manager(), req=m.networkManager.requestManager;
                const status=await req.openSecondarySource(filename);
                m.viewAndLayersManager.setSecondaryResolutionModel(status.compatibility);
                const color=(source,shade)=>({type:'colormap',mode:'LINEAR',input:{type:'source',source},
                    minSignal:0,maxSignal:source==='SECONDARY'?1:1100,startColor:'#ffffff00',endColor:shade});
                await req.setRenderPipelineConfig({enabled:true,swapUpperLower:false,
                    upperExpression:color('SECONDARY','#0000ffff'),lowerExpression:color('PRIMARY','#ff0000ff')});
                await m.reloadTilesFromBackend();
                m.viewAndLayersManager.fitViewToActiveExtent();
            }""", filename)
        job_id = requested.value.json()["jobId"]
        link = page.get_by_role("button", name="View overview job", exact=True)
        link.click()
        result = page.locator(f'article[data-job-id="sequence/{job_id}"]').get_by_role(
            "button", name="Open result", exact=True)
        result.wait_for(timeout=120000)
        page.screenshot(path=str(out / "overview-job.png"))
        with page.expect_response(lambda r: r.url.endswith("/secondary/open")) as attachment:
            result.click()
        status = attachment.value.json()
        assert status["attached"] and status["filename"] != filename, status
        link.wait_for(state="hidden")
        pixel = page.wait_for_function("""()=>{
            const m=window.__hictBenchmark.manager(), l=m.viewAndLayersManager;
            const d=l.currentViewState.resolutionDesciptor, h=m.contigDimensionHolder;
            const point=m.getMap().getPixelFromCoordinate([438780,435535].map((bp,i)=>
                (i?-1:1)*h.getFractionalPxOfBp(bp+.5,d.bpResolution)*d.pixelResolution));
            for(const layer of l.layersHolder.secondaryHiCDataLayers.filter(x=>x.getVisible()))
                for(let y=-3;y<=3;y++) for(let x=-3;x<=3;x++){
                    const rgba=layer.getData([point[0]+x,point[1]+y]);
                    if(rgba?.[3]>0) return {rgba:Array.from(rgba),level:layer.get('bpResolution')};
                }
            return null;
        }""").json_value()
        assert pixel["level"] >= 1000 and pixel["rgba"] == [0, 0, 255, 255], pixel
        assert before == hashlib.sha256(legacy.read_bytes()).hexdigest()
        jobs = page.request.post(args.api + "/sequence/jobs/list", data={}).json()
        job = next(j for j in jobs if j["jobId"] == job_id)
        assert job["status"] == "finished" and len(job["logs"]) > 1
        assert status["filename"] == job["outputFile"]
        receipt = {"originalSha256": before, "job": job, "wholeMapPixel": pixel}
        (out / "results.json").write_text(json.dumps(receipt, indent=2) + "\n")
        browser.close()


if __name__ == "__main__":
    main()
