#!/usr/bin/env python3
"""Gzip indexing and rapid-pan FASTA acceptance using only disposable input copies."""
import argparse
import gzip
import json
import random
import shutil
import subprocess
import time
from pathlib import Path

from playwright.sync_api import sync_playwright
from check_edited_view_browser import first_sequence


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:8187")
    parser.add_argument("--api", default="http://127.0.0.1:5107")
    parser.add_argument("--data-dir", required=True)
    parser.add_argument("--output", default="/tmp/hict-release-physical/large-fasta")
    args = parser.parse_args()
    data = Path(args.data_dir).absolute()
    if data != data.resolve() or not data.is_relative_to("/tmp"):
        raise ValueError("Use physical copies in a dedicated /tmp DATA directory")
    source = data / "quad_combined_ind2.fasta"
    for path in (source, data / "combined_ind2_4DN.hict.hdf5"):
        if not path.is_file() or path.is_symlink() or path.stat().st_nlink != 1:
            raise ValueError(f"Use a physical input copy: {path}")
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    compressed = data / "acceptance" / "quad-large-fasta.fa.gz"
    compressed.parent.mkdir(exist_ok=True)
    with source.open("rb") as reader, gzip.open(compressed, "wb", compresslevel=1) as writer:
        shutil.copyfileobj(reader, writer, length=1024 * 1024)
    name, sequence = first_sequence(source)
    root = Path(__file__).resolve().parents[1]
    classes = output / "classes"
    cache = output / "cache"
    classes.mkdir(exist_ok=True)
    cache.mkdir(exist_ok=True)
    subprocess.run(["javac", "-cp", str(root / "build/classes/java/main"), "-d", str(classes),
                    str(root / "scripts/LargeFastaIndexAcceptance.java")], check=True)
    bounded = subprocess.run(["java", "-Xmx64m", f"-Djava.io.tmpdir={cache}", "-cp",
        f"{classes}:{root / 'build/classes/java/main'}",
        "ru.itmo.ctlab.hict.hict_library.assembly.LargeFastaIndexAcceptance", str(compressed)],
        check=True, timeout=180, capture_output=True, text=True)
    (output / "bounded-heap.txt").write_text(bounded.stdout + bounded.stderr)
    assert not list(cache.iterdir()), "Gzip backing file was not removed on close"
    results, errors, requests, failures = [], [], [], []
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1440, "height": 1000})
        page.set_default_timeout(90000)
        page.on("pageerror", lambda error: errors.append(str(error)))
        page.on("request", lambda request: requests.append(request.post_data_json)
                if request.url.endswith("/sequence/region") else None)
        page.on("requestfailed", lambda request: failures.append(request.failure)
                if request.url.endswith("/sequence/region") else None)
        page.add_init_script("""window.__HICT_BENCHMARK_ENABLED__=true;
            const fill=CanvasRenderingContext2D.prototype.fillRect;
            CanvasRenderingContext2D.prototype.fillRect=function(...args) {
                if(this.canvas.classList.contains('sequence-axis'))this.canvas.__letters=[];
                return fill.apply(this,args);
            };
            const text=CanvasRenderingContext2D.prototype.fillText;
            CanvasRenderingContext2D.prototype.fillText=function(value,x,y,...args) {
                if(this.canvas.classList.contains('sequence-axis'))(this.canvas.__letters ||= []).push({value,x,y,color:this.fillStyle});
                return text.call(this,value,x,y,...args);
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
            start = time.monotonic()
            response = page.request.post(args.api + "/link_fasta", data={"fastaFilename": "acceptance/quad-large-fasta.fa.gz"})
            assert response.ok, response.text()
            results.append({"test": "gzip link", "seconds": time.monotonic() - start})
            rng = random.Random(654)
            timings = []
            for _ in range(100):
                first = rng.randrange(len(sequence) - 4096)
                start = time.monotonic()
                response = page.request.post(args.api + "/sequence/region", data={"startBp": first, "endBp": first + 4096})
                assert response.ok, response.text()
                assert response.json()["sequence"].upper() == sequence[first:first + 4096]
                timings.append(time.monotonic() - start)
            results.append({"test": "100 exact random gzip intervals", "contig": name,
                            "totalSeconds": sum(timings), "maxSeconds": max(timings)})
            requests.clear()
            page.evaluate("""async () => {const m=window.__hictBenchmark.manager(),v=m.getView();
                v.setResolution(v.getMinResolution());
                const d=m.viewAndLayersManager.currentViewState.resolutionDesciptor;
                window.__heartbeats=[];let previous=performance.now();
                window.__heartbeat=setInterval(()=>{const now=performance.now();window.__heartbeats.push(now-previous);previous=now},50);
                for(let i=0;i<30;i++) {
                    const px=m.contigDimensionHolder.getFractionalPxOfBp(10000+i*100,d.bpResolution)*d.pixelResolution;
                    v.setCenter([px,-px]);await new Promise(resolve=>setTimeout(resolve,25));
                }
                window.__panEnd=performance.now();m.getMap().renderSync();
            }""")
            page.wait_for_function("""Array.from(document.querySelectorAll('.sequence-axis')).every(c=>
                c.getBoundingClientRect().width>0 && c.__letters?.length>0)""")
            page.wait_for_timeout(400)
            letters = page.evaluate("""() => {const m=window.__hictBenchmark.manager(),d=m.viewAndLayersManager.currentViewState.resolutionDesciptor;
                return Array.from(document.querySelectorAll('.sequence-axis')).flatMap(canvas=>canvas.__letters.map(letter=>{
                    const horizontal=canvas.classList.contains('horizontal');
                    const coordinate=m.getMap().getCoordinateFromPixel(horizontal?[letter.x,0]:[0,letter.y]);
                    const bp=Math.floor(m.contigDimensionHolder.getFractionalBpOfPx((horizontal?coordinate[0]:-coordinate[1])/d.pixelResolution,d.bpResolution));
                    return {...letter,bp};}));}""")
            assert letters
            colors = {"A": "#16823b", "C": "#1769c2", "G": "#996600", "T": "#c52a30"}
            for letter in letters:
                assert letter["value"] == sequence[letter["bp"]], letter
                assert letter["color"] == colors.get(letter["value"], "#666666"), letter
            page.evaluate("""() => {for(const axis of ['horizontal','vertical']) {
                const ruler=document.querySelector('#'+axis+'-ruler-div').getBoundingClientRect();
                const bases=document.querySelector('.sequence-axis.'+axis).getBoundingClientRect();
                if(axis==='horizontal'?ruler.bottom>bases.top+0.1:ruler.right>bases.left+0.1)
                    throw new Error('FASTA letters overlap '+axis+' ruler');
                if(axis==='horizontal'?ruler.height<30:ruler.width<30)
                    throw new Error('Ruler lane too small');
                const painted=document.querySelector('#'+axis+'-ruler-div canvas').getBoundingClientRect();
                if(axis==='horizontal'?painted.bottom>bases.top+0.1:painted.right>bases.left+0.1)
                    throw new Error('Ruler canvas has not resized');
            }}""")
            assert all(0 < request["endBp"] - request["startBp"] <= 4096 for request in requests), requests
            assert len(requests) < 15, requests
            heartbeat = page.evaluate("clearInterval(window.__heartbeat);Math.max(...window.__heartbeats)")
            page.screenshot(path=str(output / "gzip-panning.png"))
            results.append({"test": "30 rapid pans", "requests": len(requests), "letters": len(letters),
                            "maxHeartbeatMs": heartbeat, "aborted": len(failures)})
            assert heartbeat < 500, heartbeat
            with page.expect_download() as download:
                page.evaluate("window.__hictBenchmark.manager().exportCurrentMapSvg(undefined,{scope:'viewport',cropToMap:true})")
            exported = output / "fasta-lanes.svg"
            download.value.save_as(exported)
            import xml.etree.ElementTree as ET
            texts = ET.parse(exported).getroot().findall(".//{http://www.w3.org/2000/svg}text")
            base_texts = [element for element in texts if element.text in ("A", "C", "G", "T", "N")]
            assert len(base_texts) >= len(letters), len(base_texts)
            results.append({"test": "FASTA letters retained in SVG export", "vectorLetters": len(base_texts)})

            # Delay real responses, allowing newer viewport requests to supersede
            # them. The payload still comes from HiCT, not a mocked sequence.
            delayed = [2]
            def delay_response(route):
                response = route.fetch()
                if delayed[0]:
                    delayed[0] -= 1
                    time.sleep(0.3)
                route.fulfill(response=response)
            page.route("**/sequence/region", delay_response)
            page.evaluate("""async () => {const m=window.__hictBenchmark.manager(),d=m.viewAndLayersManager.currentViewState.resolutionDesciptor;
                for(let i=0;i<4;i++) {
                    const px=m.contigDimensionHolder.getFractionalPxOfBp(30000+i*100,d.bpResolution)*d.pixelResolution;
                    m.getView().setCenter([px,-px]);await new Promise(resolve=>setTimeout(resolve,140));
                }}""")
            page.wait_for_timeout(700)
            page.unroute("**/sequence/region", delay_response)
            assert failures, "The slow-response case must exercise request cancellation"
            last_letters = page.evaluate("""() => {const m=window.__hictBenchmark.manager(),d=m.viewAndLayersManager.currentViewState.resolutionDesciptor;
                return Array.from(document.querySelectorAll('.sequence-axis')).flatMap(canvas=>{
                    if(!canvas.getBoundingClientRect().width)throw new Error('Sequence track did not recover');
                    return canvas.__letters.map(letter=>{
                        const h=canvas.classList.contains('horizontal'),p=m.getMap().getCoordinateFromPixel(h?[letter.x,0]:[0,letter.y]);
                        return {base:letter.value,bp:Math.floor(m.contigDimensionHolder.getFractionalBpOfPx((h?p[0]:-p[1])/d.pixelResolution,d.bpResolution))};});});}""")
            assert last_letters
            for letter in last_letters:
                assert letter["base"] == sequence[letter["bp"]], letter
            results.append({"test": "superseded real responses", "aborted": len(failures), "letters": len(last_letters)})

            before_revision = page.evaluate("window.__hictBenchmark.manager().networkManager.requestManager.sequenceRevision")
            requests.clear()
            page.get_by_role("link", name="Assembly", exact=True).click()
            page.get_by_role("link", name="Link FASTA", exact=True).click()
            dialog = page.locator(".modal.show").filter(has_text="Select FASTA file")
            dialog.locator(f'span[title="{source.name}"]').click()
            with page.expect_response("**/link_fasta") as relink:
                dialog.get_by_role("button", name="Load", exact=True).click()
            assert relink.value.ok, relink.value.text()
            page.wait_for_function("before=>window.__hictBenchmark.manager().networkManager.requestManager.sequenceRevision>before", arg=before_revision)
            page.wait_for_timeout(800)
            assert requests, "Relinking must invalidate cached base-track sequences"
            results.append({"test": "relink through Assembly menu invalidates base cache", "requests": len(requests)})
            assert not errors, errors
        finally:
            (output / "results.json").write_text(json.dumps({"results": results, "errors": errors}, indent=2))
            browser.close()
    print(bounded.stdout)
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
