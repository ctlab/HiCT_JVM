#!/usr/bin/env python3
"""Capture a reproducible public-data editing figure without modifying source inputs.

Use the Borrelia matrix and prepared reference documented in examples-catalog.md.
The cut is deliberately illustrative, not a biological assembly recommendation.
"""
import argparse
import functools
import hashlib
import http.server
import json
from pathlib import Path
import shutil
import subprocess
import threading
import time
import xml.etree.ElementTree as ET
from playwright.sync_api import expect, sync_playwright
from sequence_browser_harness import BrowserCheck, HictTestServer, configure_test_api


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("jar", "webui", "matrix", "fasta", "output"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--resolution", type=int, choices=[10000, 20000], default=10000)
    parser.add_argument("--bedgraph-to-bigwig", required=True, help="UCSC bedGraphToBigWig executable used only to prepare the derived GC track")
    parser.add_argument("--hide-map-names", action="store_true", help="Keep names in the ruler bands, hide duplicate captions inside the map")
    args = parser.parse_args()
    out = Path(args.output).absolute()
    assert out == out.resolve() and out.is_relative_to("/tmp")
    out.mkdir(exist_ok=False)
    data = out / "data"
    data.mkdir()
    sources = {"borrelia.hict.hdf5": Path(args.matrix).resolve(), "borrelia-matching.fa": Path(args.fasta).resolve()}
    hashes = {name: sha(path) for name, path in sources.items()}
    assert hashes["borrelia-matching.fa"] == "ca26a9bd04a3d5cbdd9e80c67a2b2a8c5c10f7c0fc98fad4163daf4716c85b94"
    for name, path in sources.items():
        shutil.copyfile(path, data / name)
        assert sha(data / name) == hashes[name]
    records = {}
    for line in (data / "borrelia-matching.fa").read_text().splitlines():
        if line.startswith(">"):
            current = line[1:].split()[0]
            records[current] = ""
        else:
            records[current] += line.strip()
    assert len(records) == 19
    first_name, first_sequence = next(iter(records.items()))
    cut = 450123
    assert len(first_sequence) > cut + 10000
    # GC among called A/C/G/T bases; the final partial window keeps its exact length.
    gc = []
    for name, sequence in records.items():
        for start in range(0, len(sequence), 10000):
            part = sequence[start:start + 10000].upper()
            called = sum(part.count(base) for base in "ACGT")
            if called:
                gc.append(f"{name}\t{start}\t{start + len(part)}\t{(part.count('G') + part.count('C')) / called:.8f}\n")
    gc.sort(key=lambda line: (line.split("\t")[0], int(line.split("\t")[1])))
    (data / "GC-fraction-10kb.bedgraph").write_text("".join(gc))
    (data / "chrom.sizes").write_text("".join(f"{name}\t{len(sequence)}\n" for name, sequence in records.items()))
    with (out / "gc-conversion.log").open("w") as log:
        subprocess.run([str(Path(args.bedgraph_to_bigwig).resolve()), str(data / "GC-fraction-10kb.bedgraph"), str(data / "chrom.sizes"), str(data / "GC-fraction-10kb.bw")], check=True, stdout=log, stderr=subprocess.STDOUT)
    (data / "Illustrative-cut.gff3").write_text(f"##gff-version 3\n{first_name}\tHiCT_demo\tregion\t{cut + 1}\t{cut + 10000}\t.\t+\t.\tID=illustrative_cut;Name=Cut_marker\n")

    class Quiet(http.server.SimpleHTTPRequestHandler):
        def log_message(self, *_):
            pass

    web = http.server.ThreadingHTTPServer(("127.0.0.1", 0), functools.partial(Quiet, directory=str(Path(args.webui).resolve())))
    threading.Thread(target=web.serve_forever, daemon=True).start()
    receipt = {"status": "running", "sources": hashes, "resolution": args.resolution, "cutOffsetBp": cut, "hideMapNames": args.hide_map_names, "jarSha256": sha(Path(args.jar).resolve()), "preparationScriptSha256": sha(Path(__file__).resolve()), "captures": []}
    try:
        with HictTestServer(args.jar, data, out / "server.log") as api, sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            page = browser.new_page(viewport={"width": 1440, "height": 1000})
            page.set_default_timeout(90000)
            page.add_init_script("window.__HICT_BENCHMARK_ENABLED__=true")
            configure_test_api(page, api.url)
            check = BrowserCheck(page, out)
            dialog = page.get_by_role("dialog", name="Export figure", exact=True)

            def post(path, body=None):
                return json.loads(api.http(path, body or {}))

            def capture(name):
                page.locator("#export-dropdown-toggle").click()
                expect(dialog.locator("#export-svg-button")).to_be_enabled()
                dialog.get_by_label("Export area", exact=True).select_option("whole-map")
                dialog.get_by_label("Expand small whole maps to fit window", exact=True).uncheck()
                dialog.get_by_role("button", name="Refresh capture", exact=True).click()
                expect(dialog.locator("#export-svg-button")).to_be_enabled()
                dialog.get_by_label("Snapshot name", exact=True).fill(name)
                dialog.get_by_role("button", name="Save snapshot", exact=True).click()
                expect(dialog).to_contain_text(f"Snapshot “{name}” added.")
                files = {}
                for extension in ("svg", "png", "pdf"):
                    with page.expect_download() as download:
                        dialog.locator(f"#export-{extension}-button").click()
                    path = out / f"{name}.{extension}"
                    download.value.save_as(path)
                    files[extension] = sha(path)
                root = ET.parse(out / f"{name}.svg").getroot()
                titles = [n for n in root.iter() if n.tag.endswith("}text") and n.text in ("GC fraction", "Cut marker")]
                assert titles and all(n.get("text-anchor") == "start" for n in titles), "Track titles inherit the preceding scale's centered alignment and may be clipped"
                if args.resolution == 10000:
                    maximum = f"{max(float(line.split()[3]) for line in gc):.3f}".rstrip("0").rstrip(".")
                    assert sum(n.text == maximum for n in root.iter() if n.tag.endswith("}text")) == 4, "Both axes must retain the GC maximum (fill and outline)"
                text_bounds = page.evaluate("""source=>{
                  const host=document.createElement('div');host.style.cssText='position:fixed;left:0;top:0;opacity:0;pointer-events:none';
                  host.innerHTML=source;document.body.append(host);
                  try{return Array.from(host.querySelectorAll('text')).filter(n=>['GC fraction','Cut marker'].includes(n.textContent)||getComputedStyle(n).fontFamily.includes('HiCT Mono')).map(n=>{
                    const svg=n.ownerSVGElement,b=n.getBBox(),m=svg.getCTM().inverse().multiply(n.getCTM()),v=svg.viewBox.baseVal;
                    const p=[[b.x,b.y],[b.x+b.width,b.y],[b.x,b.y+b.height],[b.x+b.width,b.y+b.height]].map(([x,y])=>new DOMPoint(x,y).matrixTransform(m));
                    return {text:n.textContent,left:Math.min(...p.map(p=>p.x)),right:Math.max(...p.map(p=>p.x)),top:Math.min(...p.map(p=>p.y)),bottom:Math.max(...p.map(p=>p.y)),width:v.width,height:v.height};
                  });}finally{host.remove();}
                }""", (out / f"{name}.svg").read_text())
                assert all(b["left"] >= 0 and b["top"] >= 0 and b["right"] <= b["width"] and b["bottom"] <= b["height"] for b in text_bounds), text_bounds
                metadata = json.loads(root.find("{http://www.w3.org/2000/svg}metadata").text)
                receipt["captures"].append({"name": name, "files": files, "width": float(root.get("width")), "height": float(root.get("height")), "metadata": metadata, "trackTextBounds": text_bounds})
                page.screenshot(path=str(out / f"{name}-ui.png"))
                dialog.get_by_role("button", name="Close", exact=True).last.click()
                print("Captured", name, root.get("width"), root.get("height"), flush=True)

            try:
                page.goto(f"http://127.0.0.1:{web.server_port}/")
                page.evaluate("api=>window.__hictBenchmark.open('borrelia.hict.hdf5',api,'borrelia-matching.fa')", api.url)
                check.settle()
                descriptors = page.evaluate("window.__hictBenchmark.manager().contigDimensionHolder.contigDescriptors.map(c=>({name:c.contigName,length:c.contigLengthBp}))")
                assert [(c["name"], c["length"]) for c in descriptors] == [(n, len(s)) for n, s in records.items()]
                receipt["descriptors"] = descriptors
                page.evaluate("async()=>{const t=window.__hictBenchmark.manager().linearTrackManager;await t.openTrack('GC-fraction-10kb.bw','GC fraction');await t.openTrack('Illustrative-cut.gff3','Cut marker');}")
                check.settle()
                check.menu("Tools", "ModDotPlot…")
                generate = page.get_by_role("dialog", name="ModDotPlot", exact=True)
                check.advanced()
                generate.get_by_label("Worker threads", exact=True).fill("2")
                generate.get_by_label("Finest detail", exact=True).select_option("custom")
                generate.locator("#sequence-overlay-resolutions").fill(str(args.resolution))
                generate.get_by_label("Show result as").select_option("below")
                generate.get_by_role("button", name="Generate", exact=True).click()
                generate.locator("footer").get_by_role("button", name="Close", exact=True).click()
                deadline = time.monotonic() + 600
                while time.monotonic() < deadline:
                    jobs = post("/sequence/jobs/list")
                    job = next((j for j in jobs if j["kind"] == "moddotplot"), None)
                    if job and job["status"] == "finished" and post("/secondary/status").get("attached"):
                        break
                    assert not job or job["status"] not in ("failed", "cancelled"), job
                    page.wait_for_timeout(250)
                else:
                    raise TimeoutError(jobs)
                receipt["moddotplotJob"] = job
                receipt["secondary"] = post("/secondary/status")
                print("Generated and attached real ModDotPlot", job["jobId"], flush=True)
                page.get_by_role("combobox", name="Visualization preset", exact=True).select_option(label="ModDotPlot: Hi-C above, identity below")
                page.evaluate("bp=>{const m=window.__hictBenchmark.manager(),l=m.viewAndLayersManager;l.setEnabledBpResolutions([bp]);m.getView().setResolution(l.getPixelResolutionForBpResolution(bp));l.fitViewToActiveExtent();}", args.resolution)
                # Caption placement is a normal display setting; Inside avoids external padding.
                borders = page.get_by_role("button", name="Borders and labels", exact=True)
                if borders.get_attribute("aria-expanded") != "true":
                    borders.click()
                for name in ("Contig names", "Scaffold names"):
                    page.get_by_label(name + " caption side", exact=True).select_option(label="Inside")
                    if args.hide_map_names:
                        page.locator(".layer-record").filter(has=page.get_by_label(name + " caption side", exact=True)).locator(".visibility-btn").click()
                for name in ("Horizontal tracks panel height", "Vertical tracks panel width"):
                    separator = page.get_by_role("separator", name=name, exact=True)
                    separator.focus()
                    separator.press("Home")
                check.settle()
                capture("A-original")
                # Use the actual feature context menu to choose an exact, deliberately non-bin cut.
                page.evaluate("bp=>window.__hictBenchmark.manager().viewAndLayersManager.restoreViewPosition({centerBp:[bp+5000,bp+5000],resolution:0.02,rotation:0})", cut)
                page.evaluate("async()=>await window.__hictBenchmark.manager().linearTrackManager.render({allowFetch:true})")
                page.wait_for_function("()=>{const t=window.__hictBenchmark.manager().linearTrackManager,id=t.tracks.find(t=>t.name==='Cut marker')?.trackId;return t.lastRenderedSnapshot.horizontal?.featureHits.some(h=>h.trackId===id);}")
                hit = page.evaluate("""()=>{
                  const t=window.__hictBenchmark.manager().linearTrackManager,s=t.lastRenderedSnapshot.horizontal;
                  const id=t.tracks.find(t=>t.name==='Cut marker').trackId;
                  const h=s.featureHits.find(h=>h.trackId===id);
                  if(!h)throw Error('Cut marker is not visible');
                  const r=document.querySelector('#horizontal-igv-track-div').getBoundingClientRect();
                  return [r.left+(h.axisStart+h.axisEnd)/2,r.top+(h.crossStart+h.crossEnd)/2];
                }""")
                page.mouse.click(*hit, button="right")
                menu = page.get_by_role("menu", name="Cut marker", exact=True)
                with page.expect_response("**/split_contig_at_bp") as response:
                    menu.get_by_role("menuitem", name="Cut at feature start", exact=True).click()
                assert response.value.ok, response.value.text()
                assert response.value.request.post_data_json["offsetBp"] == cut
                check.settle()
                receipt["afterCut"] = page.evaluate("window.__hictBenchmark.manager().contigDimensionHolder.contigDescriptors.map(c=>c.contigLengthBp)")
                assert receipt["afterCut"] == [cut, len(first_sequence) - cut] + [len(s) for s in list(records.values())[1:]]
                assert post("/sequence/region", {"startBp": cut - 100, "endBp": cut + 100})["sequence"].upper() == first_sequence[cut - 100:cut + 100].upper()
                capture("B-exact-cut")
                page.evaluate("end=>window.__hictBenchmark.manager().viewAndLayersManager.selectEntityRangeByBp(0,end)", len(first_sequence))
                with page.expect_response("**/group_contigs_into_scaffold") as response:
                    page.get_by_role("button", name="Group selection into scaffold", exact=True).click()
                assert response.value.ok, response.value.text()
                check.settle()
                capture("C-scaffolded")
                assert post("/sequence/region", {"startBp": cut - 100, "endBp": cut + 100})["sequence"].upper() == first_sequence[cut - 100:cut + 100].upper()
                pipeline = receipt["captures"][0]["metadata"]["provenance"]["renderPipeline"]
                assert all(c["metadata"]["provenance"]["renderPipeline"] == pipeline for c in receipt["captures"])
                assert all(c["metadata"]["wholeMapSizing"] == "NATIVE_GRID" for c in receipt["captures"])
                receipt["derivedTrack"] = {"filename": "GC-fraction-10kb.bw", "sha256": sha(data / "GC-fraction-10kb.bw"), "windowBp": 10000, "definition": "GC / ACGT among called bases, exact final partial window", "converterSha256": sha(Path(args.bedgraph_to_bigwig).resolve())}
                receipt["snapshots"] = page.evaluate("window.__hictBenchmark.manager().figureSnapshots.documents().map(s=>({name:s.name,id:s.id}))")
                assert len(receipt["snapshots"]) == 3
                assert not check.errors, check.errors
                for name, source in sources.items():
                    assert sha(source) == hashes[name] == sha(data / name)
                receipt["status"] = "passed"
            except BaseException as error:
                receipt.update(status="failed", error=str(error))
                raise
            finally:
                receipt["browserErrors"] = check.errors
                page.screenshot(path=str(out / "final-ui.png"))
                browser.close()
    finally:
        (out / "results.json").write_text(json.dumps(receipt, indent=2) + "\n")
        web.shutdown()
        web.server_close()


if __name__ == "__main__":
    main()
