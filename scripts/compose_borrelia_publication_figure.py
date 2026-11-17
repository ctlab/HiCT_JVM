#!/usr/bin/env python3
"""Compose verified immutable Borrelia captures at 178 mm without resampling panels.

This is figure authoring, not an assembly transformation. Full scientific
provenance remains in each panel's metadata and in the accompanying receipt.
"""
import argparse
import copy
import hashlib
import json
import os
import re
from pathlib import Path
import subprocess
import xml.etree.ElementTree as ET
from playwright.sync_api import sync_playwright
from pypdf import PdfReader, PdfWriter
from pypdf.generic import NameObject

NS = "http://www.w3.org/2000/svg"
ET.register_namespace("", NS)


def node(tag, **attributes):
    return ET.Element(f"{{{NS}}}{tag}", {k.replace("_", "-"): str(v) for k, v in attributes.items()})


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--captures", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--font-dir", required=True, help="HiCT_WebUI/src/assets/fonts, for deterministic PDF rendering")
    args = parser.parse_args()
    source = Path(args.captures).resolve()
    receipt = json.loads((source / "results.json").read_text())
    assert receipt["status"] == "passed" and not receipt["browserErrors"]
    panels = receipt["captures"]
    assert [p["name"] for p in panels] == ["A-original", "B-exact-cut", "C-scaffolded"]
    graph = panels[0]["metadata"]["provenance"]["renderPipeline"]
    assert all(p["metadata"]["provenance"]["renderPipeline"] == graph for p in panels)
    assert all(p["metadata"]["wholeMapSizing"] == "NATIVE_GRID" for p in panels)
    assert graph["upperExpression"]["mode"] == "LINEAR" and graph["lowerExpression"]["mode"] == "MODDOTPLOT_IDENTITY"
    assert graph["lowerExpression"]["minSignal"] == 80 and graph["lowerExpression"]["maxSignal"] == 100
    expected_input = {"type": "binary", "op": "MUL", "left": {"type": "source", "source": "PRIMARY"}, "right": {"type": "binary", "op": "MUL", "left": {"type": "track1d", "trackId": "__builtin_cooler_weights__", "axis": "ROW"}, "right": {"type": "track1d", "trackId": "__builtin_cooler_weights__", "axis": "COL"}}}
    assert graph["upperExpression"]["input"] == expected_input
    assert graph["lowerExpression"]["input"] == {"type": "binary", "op": "DIV", "left": {"type": "source", "source": "SECONDARY"}, "right": {"type": "constant", "value": 10000}}
    assert all(p["metadata"]["provenance"]["visualizationOptions"]["coolerWeightsNaNPolicy"] == "REPLACE_NANS_WITH_ZERO" for p in panels)
    out = Path(args.output).absolute()
    assert out == out.resolve() and out.is_relative_to("/tmp")
    out.mkdir(exist_ok=False)
    padding, caption = 8, 32
    pw = max(p["width"] for p in panels)
    ph = max(p["height"] for p in panels)
    width, height = 2 * pw + 3 * padding, 2 * (ph + caption) + 3 * padding
    root = node("svg", width="178mm", height=f"{178 * height / width:.9f}mm", viewBox=f"0 0 {width:g} {height:g}")
    root.append(node("rect", width=width, height=height, fill="#ffffff"))
    defs = node("defs")
    root.append(defs)
    fonts = set()
    raster_records = []
    def text(value, x, y, size=11, bold=False):
        item = node("text", x=x, y=y, font_family="HiCT Sans", font_size=size, fill="#172033", font_weight="bold" if bold else "normal")
        item.text = value
        root.append(item)
    for i, panel in enumerate(panels):
        path = source / (panel["name"] + ".svg")
        assert digest(path) == panel["files"]["svg"]
        scene = ET.parse(path).getroot()
        assert all(n.get("text-anchor") == "start" for n in scene.iter() if n.tag.endswith("}text") and n.text in ("GC fraction", "Cut marker"))
        x = padding if i < 2 else pw + 2 * padding
        y = padding + caption if i == 0 else ph + 2 * caption + 2 * padding
        title = ["A  Original assembly", "B  Exact cut", "C  Scaffolded children"][i]
        text(title, x, y - 10, 12, True)
        for parent in scene.iter():
            for child in list(parent):
                if child.tag == f"{{{NS}}}metadata":
                    parent.remove(child)
                elif child.tag == f"{{{NS}}}style":
                    if child.text not in fonts:
                        defs.append(copy.deepcopy(child))
                        fonts.add(child.text)
                    parent.remove(child)
        images_before = []
        for element in scene.iter():
            if element.tag == f"{{{NS}}}image":
                images_before.append(dict(element.attrib))
            if "id" in element.attrib:
                element.set("id", f"p{i}-" + element.get("id"))
            if "clip-path" in element.attrib:
                element.set("clip-path", element.get("clip-path").replace("url(#", f"url(#p{i}-"))
        # Nested width/height/viewBox remain unchanged: every panel keeps pixel scale one.
        scene.set("x", str(x))
        scene.set("y", str(y))
        root.append(scene)
        assert images_before == [dict(e.attrib) for e in scene.iter() if e.tag == f"{{{NS}}}image"]
        raster_records.append({"name": panel["name"], "images": len(images_before), "pixelScale": 1, "offset": [x, y], "sourceSvgSha256": digest(path), "provenance": panel["metadata"]})
    lx, ly = pw + 2 * padding, padding + 20
    text("Data and rendering", lx, ly, 12, True)
    for row, value in enumerate([
        "Borrelia burgdorferi · S9 / B31",
        f"GSM7056007 · native grid: {receipt['resolution'] // 1000} kb",
        "Upper triangle: balanced Hi-C",
    ]):
        text(value, lx, ly + 20 + 17 * row)
    def bar(colors, x, y):
        for i, color in enumerate(colors):
            root.append(node("rect", x=x + i * 8, y=y, width=8, height=10, fill=color))
    upper = graph["upperExpression"]
    assert upper["startColor"] == "#ffffffff" and upper["endColor"] == "#e80000ff" and upper["minSignal"] == 0
    bar([f"#{255 - round(i * 23 / 10):02x}{255 - round(i * 255 / 10):02x}{255 - round(i * 255 / 10):02x}" for i in range(11)], lx + 20, ly + 62)
    text("0", lx, ly + 72)
    text(f"≥ {graph['upperExpression']['maxSignal']:.4g}", lx + 116, ly + 72)
    text("Lower: estimated sequence identity", lx, ly + 94)
    bar(["#313695", "#4575b4", "#74add1", "#abd9e9", "#e0f3f8", "#ffffbf", "#fee090", "#fdae61", "#f46d43", "#d73027", "#a50026"], lx + 20, ly + 103)
    text("80", lx, ly + 113)
    text("100%", lx + 116, ly + 113)
    for row, value in enumerate([
        "Below 80%: not displayed.",
        "GC: called-base fraction, 10 kb.",
        "Marker: illustrative cut region.",
        "Cut: Ch1 at boundary 450,123 bp.",
        "Crossing source bins are retained.",
        "Native matrix and FASTA unchanged.",
        "Excluded balancing bins are masked.",
        "Panels share one rendering pipeline.",
    ]):
        text(value, lx, ly + 138 + row * 17)
    metadata = node("metadata")
    metadata.text = json.dumps({"format": "hict-figure", "version": 1, "publicationComposition": True, "widthMm": 178, "layout": "three-panels-and-rendering-key", "panels": raster_records, "sourceHashes": receipt["sources"], "captureReceiptSha256": digest(source / "results.json")}, separators=(",", ":"))
    root.insert(0, metadata)
    svg = out / "borrelia-editing.svg"
    ET.ElementTree(root).write(svg, encoding="utf-8", xml_declaration=False)
    font_dir = Path(args.font_dir).resolve()
    font_files = [font_dir / name for name in ("DejaVuSans.ttf", "DejaVuSans-Bold.ttf", "DejaVuSansMono.ttf")]
    assert all(path.is_file() for path in font_files)
    config = ET.Element("fontconfig")
    ET.SubElement(config, "dir").text = str(font_dir)
    ET.SubElement(config, "cachedir").text = str(out / "font-cache")
    for source_family, target_family in (("HiCT Sans", "DejaVu Sans"), ("HiCT Mono", "DejaVu Sans Mono")):
        alias = ET.SubElement(config, "alias")
        ET.SubElement(alias, "family").text = source_family
        ET.SubElement(ET.SubElement(alias, "prefer"), "family").text = target_family
    font_config = out / "fonts.conf"
    ET.ElementTree(config).write(font_config, encoding="utf-8")
    environment = {**os.environ, "FONTCONFIG_FILE": str(font_config)}
    subprocess.run(["rsvg-convert", "--format", "pdf", "--output", str(out / "borrelia-editing.pdf"), str(svg)], check=True, env=environment)
    subprocess.run(["rsvg-convert", "--width", "2102", "--output", str(out / "borrelia-editing-preview.png"), str(svg)], check=True, env=environment)
    pdf_path = out / "borrelia-editing.pdf"
    original_pdf = PdfReader(pdf_path)
    writer = PdfWriter(clone_from=original_pdf)
    xmp = ET.Element("{adobe:ns:meta/}xmpmeta")
    rdf = ET.SubElement(xmp, "{http://www.w3.org/1999/02/22-rdf-syntax-ns#}RDF")
    description = ET.SubElement(rdf, "{http://www.w3.org/1999/02/22-rdf-syntax-ns#}Description", {"{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about": ""})
    ET.SubElement(description, "{urn:hict:figure#}metadata").text = metadata.text
    writer.xmp_metadata = ET.tostring(xmp, encoding="utf-8")
    writer.root_object["/Metadata"].get_object().update({NameObject("/Type"): NameObject("/Metadata"), NameObject("/Subtype"): NameObject("/XML")})
    writer.add_attachment("hict-provenance.json", metadata.text.encode("utf-8"))
    writer.add_metadata({"/Title": "HiCT Borrelia editing", "/Creator": "HiCT publication figure composition", "/Subject": "GSM7056007 public-data editing demonstration"})
    temporary_pdf = out / "borrelia-editing-with-provenance.pdf"
    with temporary_pdf.open("wb") as stream:
        writer.write(stream)
    with_provenance = PdfReader(temporary_pdf)
    assert len(with_provenance.pages) == 1
    assert original_pdf.pages[0].get_contents().get_data() == with_provenance.pages[0].get_contents().get_data()
    assert with_provenance.attachments["hict-provenance.json"] == [metadata.text.encode("utf-8")]
    xmp_record = ET.fromstring(with_provenance.trailer["/Root"]["/Metadata"].get_object().get_data())
    assert json.loads(xmp_record.find(".//{urn:hict:figure#}metadata").text) == json.loads(metadata.text)
    temporary_pdf.replace(pdf_path)
    info = subprocess.check_output(["pdfinfo", str(out / "borrelia-editing.pdf")], text=True)
    (out / "pdfinfo.txt").write_text(info)
    page_size = re.search(r"Page size:\s*([\d.]+) x ([\d.]+) pts", info)
    assert page_size and abs(float(page_size[1]) * 25.4 / 72 - 178) < 0.01
    assert abs(float(page_size[2]) * 25.4 / 72 - 178 * height / width) < 0.01
    font_info = subprocess.check_output(["pdffonts", str(out / "borrelia-editing.pdf")], text=True)
    (out / "pdf-fonts.txt").write_text(font_info)
    assert all("DejaVu" in line.split()[0] and line.split()[-5] == "yes" for line in font_info.splitlines()[2:])
    extracted = subprocess.check_output(["pdftotext", str(out / "borrelia-editing.pdf"), "-"], text=True)
    assert all(s in extracted for s in ("Original assembly", "Exact cut", "Scaffolded children", "450,123", "GC fraction")), extracted
    (out / "pdf-text.txt").write_text(extracted)
    assert "0.478" in extracted
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 900, "height": 900})
        page.set_content(svg.read_text())
        page.evaluate("document.fonts.ready")
        sizes = page.evaluate("""()=>Array.from(document.querySelectorAll('text')).filter(t=>t.textContent.trim()).map(t=>{const m=t.getScreenCTM();return parseFloat(getComputedStyle(t).fontSize)*Math.hypot(m.c,m.d)*72/96;})""")
        assert sizes and min(sizes) >= 7, min(sizes)
        page.screenshot(path=str(out / "print-preview-browser.png"), full_page=True)
        browser.close()
    (out / "capture-results.json").write_text(json.dumps(receipt, indent=2) + "\n")
    (out / "composition.json").write_text(json.dumps({"status": "passed", "widthMm": 178, "heightMm": 178 * height / width, "widthUnits": width, "heightUnits": height, "smallestVectorTextPoints": min(sizes), "compositionScriptSha256": digest(Path(__file__).resolve()), "rsvgVersion": subprocess.check_output(["rsvg-convert", "--version"], text=True).strip(), "fontFiles": {p.name: digest(p) for p in font_files}, "panels": [{k: v for k, v in p.items() if k != "provenance"} for p in raster_records]}, indent=2) + "\n")
    print("Composed", width, height, "units at 178 mm; original embedded image attributes/bytes retained")


if __name__ == "__main__":
    main()
