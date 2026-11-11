#!/usr/bin/env python3
"""Verify bounded CIGAR alignment, exact base placement, edits and exports."""

import argparse, io, json, random, time
from pathlib import Path
import numpy as np
from PIL import Image
from playwright.sync_api import sync_playwright

p = argparse.ArgumentParser()
for k in ("url", "api", "output"):
    p.add_argument("--" + k, required=True)
p.add_argument("--data-dir", required=True)
a = p.parse_args()
data = Path(a.data_dir).resolve()
assert data.is_relative_to("/tmp")
rng = random.Random(44)
dna = "".join(rng.choice("ACGT") for _ in range(12017))
row = dna[:512]
substitution = row[:100] + ("A" if row[100] != "A" else "C") + row[101:]
insertion = row[:150] + next(b for b in "ACGT" if b not in row[149:151]) + row[150:]
deletion = row[:200] + row[201:]
reverse = row.translate(str.maketrans("ACGT", "TGCA"))[::-1]
ambiguous = row[:250] + "N" + row[251:300] + "R" + row[301:]
for start, sequence in (
    (1000, substitution),
    (2000, insertion),
    (3000, deletion),
    (4000, reverse),
    (5000, ambiguous),
    (6000, row),
    (7000, "N" * 512),
):
    dna = dna[:start] + sequence + dna[start + len(sequence) :]
(data / "mixed.fa").write_text(">tiny\n" + dna + "\n")
out = Path(a.output)
out.mkdir(parents=True, exist_ok=True)
with sync_playwright() as pw:
    browser = pw.chromium.launch(headless=True)
    page = browser.new_page(viewport={"width": 1440, "height": 900})
    errors = []
    page.on("pageerror", lambda e: errors.append(str(e)))
    page.add_init_script("window.__HICT_BENCHMARK_ENABLED__=true")
    page.route(
        "http://localhost:5000/**",
        lambda r: r.continue_(
            url=a.api + r.request.url.removeprefix("http://localhost:5000")
        ),
    )
    page.goto(a.url)
    page.wait_for_function("window.__hictBenchmark")
    page.evaluate(
        """async api=>{
        const b=window.__hictBenchmark;
        await b.open('mixed-primary.hict.hdf5',api,'mixed.fa');
        const m=b.manager(),l=m.viewAndLayersManager,d=l.getActiveVectorResolutionDescriptor();
        const p=l.getPixelResolutionForBpResolution(4);
        m.getView().setResolution(p/4/24);
        m.getView().setCenter([m.contigDimensionHolder.getFractionalPxOfBp(1100,4)*p,
          -m.contigDimensionHolder.getFractionalPxOfBp(100,4)*p]);
        m.getMap().renderSync();
    }""",
        a.api,
    )
    page.wait_for_function(
        """()=> {
      const l=window.__hictBenchmark.manager().alignmentDetail;
      return l.source.getFeatures().length>0 && l.state.message.value==='';
    }""",
        timeout=30000,
    )
    detail = page.evaluate("""()=>{
      const m=window.__hictBenchmark.manager(),l=m.alignmentDetail;
      const colors=l.source.getFeatures().map(f=>f.getStyle().getImage()?.getFill().getColor());
      const holder=m.contigDimensionHolder,d=m.viewAndLayersManager.getActiveVectorResolutionDescriptor();
      return {colors,point:m.getMap().getPixelFromCoordinate([
        holder.getFractionalPxOfBp(1100.5,d.bpResolution)*d.pixelResolution,
        -holder.getFractionalPxOfBp(100.5,d.bpResolution)*d.pixelResolution])};
    }""")
    assert "#c2410c" in detail["colors"], detail
    shot = page.locator("#hic-contact-map").screenshot()
    (out / "substitution.png").write_bytes(shot)
    im = np.array(Image.open(io.BytesIO(shot)).convert("RGB"))
    x, y = map(round, detail["point"])
    patch = im[y - 4 : y + 5, x - 4 : x + 5].astype(int)
    assert (
        (patch[:, :, 0] - patch[:, :, 1] > 50) & (patch[:, :, 1] - patch[:, :, 2] > 20)
    ).any(), detail
    print(
        "Rendered substitution at the exact FASTA base centre",
        detail["point"],
        flush=True,
    )
    for extension, method in (
        ("png", "exportCurrentMapPng"),
        ("svg", "exportCurrentMapSvg"),
        ("pdf", "exportCurrentMapPdf"),
    ):
        with page.expect_download() as saved:
            page.evaluate(
                "method=>window.__hictBenchmark.manager()[method](undefined,{scope:'viewport'})",
                method,
            )
        saved.value.save_as(out / f"alignment-detail.{extension}")
        assert (out / f"alignment-detail.{extension}").stat().st_size > 500
    print("PNG, SVG and PDF viewport exports completed", flush=True)

    def alignment(start, end=512):
        request = {
            "rowStart": 0,
            "rowEnd": 512,
            "columnStart": start,
            "columnEnd": start + end,
        }
        result = page.request.post(
            a.api + "/sequence/alignment_detail", data=request
        ).json()
        deadline = time.monotonic() + 30
        while (
            result["status"] in ("submitted", "running") and time.monotonic() < deadline
        ):
            time.sleep(0.05)
            result = page.request.post(
                a.api + "/sequence/alignment_detail", data={"jobId": result["jobId"]}
            ).json()
        assert result["status"] == "finished", result
        return result["result"]

    for start, span, operation in (
        (2000, 513, "INSERTION"),
        (3000, 511, "DELETION"),
        (5000, 512, "AMBIGUOUS"),
    ):
        result = alignment(start, span)
        assert any(
            s["operation"] == operation and not s["reverse"] for s in result["segments"]
        ), (operation, result)
    assert any(s["reverse"] for s in alignment(4000)["segments"])
    assert alignment(6000)["alignmentCount"] > 0
    assert alignment(7000)["alignmentCount"] == 0
    print(
        "Real aligner verified insertion, deletion, reverse complement, repeat, N/IUPAC and no alignment",
        flush=True,
    )

    def check_marker(name, column, row):
        page.evaluate(
            """({column,row})=>{
          const m=window.__hictBenchmark.manager(),l=m.viewAndLayersManager;
          const d=l.getActiveVectorResolutionDescriptor(),p=d.pixelResolution;
          m.getView().setCenter([m.contigDimensionHolder.getFractionalPxOfBp(column,d.bpResolution)*p,
            -m.contigDimensionHolder.getFractionalPxOfBp(row,d.bpResolution)*p]);
          m.getMap().renderSync();
        }""",
            {"column": column, "row": row},
        )
        page.wait_for_function(
            """()=> {
          const l=window.__hictBenchmark.manager().alignmentDetail;
          return l.source.getFeatures().some(f=>f.getStyle().getImage()?.getFill().getColor()==='#c2410c')
            && l.state.message.value==='';
        }""",
            timeout=30000,
        )
        point = page.evaluate(
            """({column,row})=>{
          const m=window.__hictBenchmark.manager(),d=m.viewAndLayersManager.getActiveVectorResolutionDescriptor();
          return m.getMap().getPixelFromCoordinate([
            m.contigDimensionHolder.getFractionalPxOfBp(column+.5,d.bpResolution)*d.pixelResolution,
            -m.contigDimensionHolder.getFractionalPxOfBp(row+.5,d.bpResolution)*d.pixelResolution]);
        }""",
            {"column": column, "row": row},
        )
        image = page.locator("#hic-contact-map").screenshot()
        (out / f"{name}.png").write_bytes(image)
        pixels = np.array(Image.open(io.BytesIO(image)).convert("RGB")).astype(int)
        x, y = map(round, point)
        area = pixels[y - 4 : y + 5, x - 4 : x + 5]
        assert (
            (area[:, :, 0] - area[:, :, 1] > 50) & (area[:, :, 1] - area[:, :, 2] > 20)
        ).any(), (name, point)

    original_point = page.evaluate("""()=>{
      const m=window.__hictBenchmark.manager(),d=m.viewAndLayersManager.getActiveVectorResolutionDescriptor();
      return m.getMap().getPixelFromCoordinate([m.contigDimensionHolder.getFractionalPxOfBp(1100.5,d.bpResolution)*d.pixelResolution,
        -m.contigDimensionHolder.getFractionalPxOfBp(100.5,d.bpResolution)*d.pixelResolution]);
    }""")
    page.evaluate(
        "window.__hictBenchmark.manager().viewAndLayersManager.setEnabledBpResolutions([10])"
    )
    check_marker("coarse-level-at-letter-zoom", 1100, 100)
    changed_point = page.evaluate("""()=>{
      const m=window.__hictBenchmark.manager(),d=m.viewAndLayersManager.getActiveVectorResolutionDescriptor();
      return m.getMap().getPixelFromCoordinate([m.contigDimensionHolder.getFractionalPxOfBp(1100.5,d.bpResolution)*d.pixelResolution,
        -m.contigDimensionHolder.getFractionalPxOfBp(100.5,d.bpResolution)*d.pixelResolution]);
    }""")
    assert max(abs(a - b) for a, b in zip(original_point, changed_point)) < 1, (
        original_point,
        changed_point,
    )
    page.evaluate(
        "window.__hictBenchmark.manager().viewAndLayersManager.setEnabledBpResolutions([4])"
    )
    check_marker("fine-level-at-letter-zoom", 1100, 100)
    with page.expect_response(
        lambda r: r.url.endswith("/split_contig_at_bin") and r.ok
    ):
        page.evaluate(
            "window.__hictBenchmark.manager().eventManager.onClickInScissorsMode([31,31],4)"
        )
    page.wait_for_function(
        "window.__hictBenchmark.manager().contigDimensionHolder.contig_count===2"
    )
    check_marker("after-split", 1096, 100)
    page.evaluate(
        "window.__hictBenchmark.manager().networkManager.requestManager.restoreAssemblyHistory('undo')"
    )
    page.wait_for_function(
        "window.__hictBenchmark.manager().contigDimensionHolder.contig_count===1"
    )
    check_marker("after-undo-split", 1100, 100)
    with page.expect_response(
        lambda r: r.url.endswith("/reverse_selection_range") and r.ok
    ):
        page.evaluate("""()=>{
          const e=window.__hictBenchmark.manager().eventManager,select=e.getSelectedContigBpRange;
          e.getSelectedContigBpRange=()=>({startBP:0,endBP:12017});
          e.onReverseSelectionClicked();e.getSelectedContigBpRange=select;
        }""")
    page.wait_for_function(
        "window.__hictBenchmark.manager().contigDimensionHolder.contigDescriptors[0].direction===0"
    )
    check_marker("after-reverse", 10916, 11916)
    page.evaluate(
        "window.__hictBenchmark.manager().networkManager.requestManager.restoreAssemblyHistory('undo')"
    )
    check_marker("after-undo-reverse", 1100, 100)
    print("Exact substitution follows Split, Reverse and Undo", flush=True)
    held = {}

    def hold_detail(route):
        if "jobId" in route.request.post_data_json:
            route.continue_()
            return
        response = route.fetch().json()
        deadline = time.monotonic() + 20
        while (
            response["status"] in ("submitted", "running")
            and time.monotonic() < deadline
        ):
            time.sleep(0.02)
            response = page.request.post(
                a.api + "/sequence/alignment_detail", data={"jobId": response["jobId"]}
            ).json()
        held["route"] = route
        held["response"] = response

    page.route("**/sequence/alignment_detail", hold_detail)
    page.evaluate(
        "window.__hictBenchmark.manager().networkManager.requestManager.bumpSequenceRevision()"
    )
    page.wait_for_timeout(300)
    assert held, "No alignment request was intercepted"
    page.evaluate("""()=>{
      const m=window.__hictBenchmark.manager(),d=m.viewAndLayersManager.getActiveVectorResolutionDescriptor();
      m.getView().setCenter([m.contigDimensionHolder.getFractionalPxOfBp(7050,d.bpResolution)*d.pixelResolution,
        -m.contigDimensionHolder.getFractionalPxOfBp(50,d.bpResolution)*d.pixelResolution]);
    }""")
    page.unroute("**/sequence/alignment_detail", hold_detail)
    try:
        held["route"].fulfill(
            status=200,
            content_type="application/json",
            body=json.dumps(held["response"]),
        )
    except Exception as failed_fulfil:
        assert (
            "Target" in str(failed_fulfil) or "abort" in str(failed_fulfil).lower()
        ), failed_fulfil
    page.wait_for_function(
        """()=>{
      const l=window.__hictBenchmark.manager().alignmentDetail;
      return l.source.getFeatures().length===0 && l.state.message.value==='No alignment found in the selected intervals';
    }""",
        timeout=30000,
    )
    print(
        "Stale alignment response discarded and no-alignment message shown", flush=True
    )
    assert not errors, errors
    browser.close()
