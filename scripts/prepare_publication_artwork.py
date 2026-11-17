#!/usr/bin/env python3
"""Create vector-cell artwork and a 900-dpi TIFF from an immutable HiCT capture.

Each decoded native PNG pixel becomes an exact same-colour rectangle. This is
an alternative encoding of the captured colour field, not new genomic detail.
Source PNGs and scientific metadata remain available alongside the artwork.
"""
import argparse
import base64
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import xml.etree.ElementTree as ET

import PIL
import pypdf
from PIL import Image, ImageDraw
from pypdf import PdfReader, PdfWriter
from pypdf.generic import NameObject

NS = "http://www.w3.org/2000/svg"
XLINK = "http://www.w3.org/1999/xlink"
ET.register_namespace("", NS)
MAX_PIXELS = 1_000_000


def sha(data):
    return hashlib.sha256(data).hexdigest()


def pixel_rectangles(image):
    """Merge identical horizontal runs only when they continue on the next row."""
    pixels = image.load()
    active, rectangles = {}, []
    for y in range(image.height):
        current = {}
        x = 0
        while x < image.width:
            color, start = pixels[x, y], x
            while x < image.width and pixels[x, y] == color:
                x += 1
            key = (start, x - start, color)
            rectangle = active.pop(key, None)
            if rectangle is None:
                rectangle = [start, y, x - start, 1, color]
            else:
                rectangle[3] += 1
            current[key] = rectangle
        rectangles.extend(active.values())
        active = current
    rectangles.extend(active.values())
    return rectangles


def vectorize_images(root, out):
    records, total = [], 0
    parents = {child: parent for parent in root.iter() for child in parent}
    images = list(root.iter(f"{{{NS}}}image"))
    if not images:
        raise ValueError("The source has no native contact images")
    allowed = {"x", "y", "width", "height", "href", f"{{{XLINK}}}href", "transform", "opacity", "clip-path", "preserveAspectRatio"}
    for index, element in enumerate(images):
        if set(element.attrib) - allowed:
            raise ValueError("Unsupported image attributes; use an unmodified native-grid capture")
        href = element.get("href", element.get(f"{{{XLINK}}}href", ""))
        if element.get(f"{{{XLINK}}}href", href) != href or not href.startswith("data:image/png;base64,"):
            raise ValueError("Only matching embedded native PNG references are supported")
        encoded = base64.b64decode(href.split(",", 1)[1], validate=True)
        with Image.open(io.BytesIO(encoded)) as original:
            total += original.width * original.height
            if total > MAX_PIXELS:
                raise ValueError("Vector artwork is limited to one million captured pixels; capture a smaller area")
            if float(element.get("width", "0")) != original.width or float(element.get("height", "0")) != original.height:
                raise ValueError("Use native pixel dimensions; this tool does not resample images")
            image = original.convert("RGBA")
        rectangles = pixel_rectangles(image)
        rebuilt = Image.new("RGBA", image.size)
        draw = ImageDraw.Draw(rebuilt)
        coverage = bytearray(image.width * image.height)
        for x, y, w, h, color in rectangles:
            for row in range(y, y + h):
                start = row * image.width + x
                if any(coverage[start:start + w]):
                    raise AssertionError("Native cells overlap in the rectangle encoding")
                coverage[start:start + w] = b"\1" * w
            draw.rectangle((x, y, x + w - 1, y + h - 1), fill=color)
        if not all(coverage) or rebuilt.tobytes() != image.tobytes():
            raise AssertionError("Rectangle reconstruction changed the decoded RGBA field")
        source_name = f"source-contact-{index + 1}.png"
        (out / source_name).write_bytes(encoded)
        group = ET.Element(f"{{{NS}}}g", {"shape-rendering": "crispEdges"})
        for name in ("transform", "opacity", "clip-path"):
            if name in element.attrib:
                group.set(name, element.get(name))
        ox, oy = float(element.get("x", "0")), float(element.get("y", "0"))
        count = 0
        for x, y, w, h, color in rectangles:
            if not color[3]:
                continue
            attributes = {"x": f"{x + ox:g}", "y": f"{y + oy:g}", "width": str(w), "height": str(h), "fill": "#%02x%02x%02x" % color[:3]}
            if color[3] != 255:
                attributes["fill-opacity"] = f"{color[3] / 255:.12g}"
            ET.SubElement(group, f"{{{NS}}}rect", attributes)
            count += 1
        parent = parents[element]
        position = list(parent).index(element)
        parent.remove(element)
        parent.insert(position, group)
        records.append({"sourcePng": source_name, "pngSha256": sha(encoded), "rgbaSha256": sha(image.tobytes()), "width": image.width, "height": image.height, "rgbaPixelsVerified": image.width * image.height, "visibleRectangles": count, "exactRgbaReconstruction": True, "placement": dict(element.attrib, href="retained separately", **{f"{{{XLINK}}}href": "retained separately"})})
    return records


def font_environment(font_dir, out):
    font_dir = Path(font_dir).resolve()
    files = [font_dir / n for n in ("DejaVuSans.ttf", "DejaVuSans-Bold.ttf", "DejaVuSansMono.ttf")]
    if not all(p.is_file() for p in files):
        raise ValueError("The bundled HiCT TTF directory is required")
    root = ET.Element("fontconfig")
    ET.SubElement(root, "dir").text = str(font_dir)
    ET.SubElement(root, "cachedir").text = str(out / "font-cache")
    for name, family in (("HiCT Sans", "DejaVu Sans"), ("HiCT Mono", "DejaVu Sans Mono")):
        alias = ET.SubElement(root, "alias")
        ET.SubElement(alias, "family").text = name
        ET.SubElement(ET.SubElement(alias, "prefer"), "family").text = family
    path = out / "fonts.conf"
    ET.ElementTree(root).write(path, encoding="utf-8")
    return {**os.environ, "FONTCONFIG_FILE": str(path)}, {p.name: sha(p.read_bytes()) for p in files}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--figure", required=True)
    parser.add_argument("--font-dir", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    source = Path(args.figure).resolve()
    data = source.read_bytes()
    if len(data) > 24 * 1024 * 1024 or b"<!DOCTYPE" in data or b"<!ENTITY" in data:
        raise ValueError("Use a bounded standalone HiCT SVG capture")
    root = ET.fromstring(data)
    metadata = root.find(f"{{{NS}}}metadata")
    if root.tag != f"{{{NS}}}svg" or metadata is None:
        raise ValueError("Expected a standalone HiCT SVG with scientific provenance")
    provenance = json.loads(metadata.text)
    if provenance.get("format") != "hict-figure" or provenance.get("version") != 1:
        raise ValueError("Expected HiCT figure provenance")
    dimensions = [root.get(name, "") for name in ("width", "height")]
    if not all(re.fullmatch(r"[0-9]+(?:\.[0-9]+)?mm", value) for value in dimensions):
        raise ValueError("Choose a physical print size for the SVG before preparing artwork")
    width_mm, height_mm = [float(value[:-2]) for value in dimensions]
    viewbox = [float(value) for value in root.get("viewBox", "").split()]
    if len(viewbox) != 4 or viewbox[:2] != [0, 0] or any(n <= 0 or n != int(n) for n in viewbox[2:]) or viewbox[2] * viewbox[3] > 4 * MAX_PIXELS:
        raise ValueError("Use a bounded whole-pixel native capture")
    dpi = 900
    pixel_width, pixel_height = round(width_mm * dpi / 25.4), round(height_mm * dpi / 25.4)
    if min(pixel_width, pixel_height) < 1 or pixel_width * pixel_height > 64 * 1024 * 1024:
        raise ValueError("The 900-dpi TIFF exceeds the 64-megapixel budget; choose a smaller print size")
    out = Path(args.output).absolute()
    if out != out.resolve() or not out.is_relative_to("/tmp"):
        parser.error("Use a fresh nonsymlinked output directory under /tmp")
    out.mkdir(exist_ok=False)
    records = vectorize_images(root, out)
    provenance["artwork"] = {"version": 1, "method": "NATIVE_RGBA_RECTANGLES_V1", "sourceSvgSha256": sha(data), "resampling": "NONE", "text": "PRESERVED_VECTOR_TEXT", "nativeImages": records}
    metadata.text = json.dumps(provenance, separators=(",", ":"))
    svg = out / "borrelia-editing.svg"
    ET.ElementTree(root).write(svg, encoding="utf-8")
    environment, fonts = font_environment(args.font_dir, out)
    width, height = [int(v) for v in root.get("viewBox").split()[2:]]
    render = lambda src, dst, w, h: subprocess.run(["rsvg-convert", "--width", str(w), "--height", str(h), "--output", str(dst), str(src)], env=environment, check=True)
    render(source, out / "source-at-native-units.png", width, height)
    render(svg, out / "vector-at-native-units.png", width, height)
    with Image.open(out / "source-at-native-units.png") as a, Image.open(out / "vector-at-native-units.png") as b:
        if a.convert("RGBA").tobytes() != b.convert("RGBA").tobytes():
            raise AssertionError("Full-scene native-unit rendering differs from the immutable source")
        native_scene_sha = sha(a.convert("RGBA").tobytes())
    pdf = out / "borrelia-editing.pdf"
    subprocess.run(["rsvg-convert", "--format", "pdf", "--output", str(pdf), str(svg)], env=environment, check=True)
    reader = PdfReader(pdf)
    if len(reader.pages) != 1 or list(reader.pages[0].images):
        raise AssertionError("The publication PDF must have one page and no raster images")
    writer = PdfWriter(clone_from=reader)
    xmp = ET.Element("{adobe:ns:meta/}xmpmeta")
    rdf = ET.SubElement(xmp, "{http://www.w3.org/1999/02/22-rdf-syntax-ns#}RDF")
    description = ET.SubElement(rdf, "{http://www.w3.org/1999/02/22-rdf-syntax-ns#}Description", {"{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about": ""})
    ET.SubElement(description, "{urn:hict:figure#}metadata").text = metadata.text
    writer.xmp_metadata = ET.tostring(xmp, encoding="utf-8")
    writer.root_object["/Metadata"].get_object().update({NameObject("/Type"): NameObject("/Metadata"), NameObject("/Subtype"): NameObject("/XML")})
    writer.add_attachment("hict-provenance.json", metadata.text.encode())
    for record in records:
        writer.add_attachment(record["sourcePng"], (out / record["sourcePng"]).read_bytes())
    writer.add_metadata({"/Title": "HiCT Borrelia editing", "/Creator": "HiCT vector-cell artwork"})
    temporary = out / "with-provenance.pdf"
    with temporary.open("wb") as stream:
        writer.write(stream)
    verified = PdfReader(temporary)
    assert not list(verified.pages[0].images)
    assert verified.pages[0].get_contents().get_data() == reader.pages[0].get_contents().get_data()
    assert verified.attachments["hict-provenance.json"] == [metadata.text.encode()]
    for record in records:
        assert sha(verified.attachments[record["sourcePng"]][0]) == record["pngSha256"]
    temporary.replace(pdf)
    info = subprocess.check_output(["pdfinfo", str(pdf)], text=True)
    (out / "pdfinfo.txt").write_text(info)
    page_size = re.search(r"Page size:\s*([\d.]+) x ([\d.]+) pts", info)
    assert page_size and abs(float(page_size[1]) * 25.4 / 72 - width_mm) < 0.01
    assert abs(float(page_size[2]) * 25.4 / 72 - height_mm) < 0.01
    font_info = subprocess.check_output(["pdffonts", str(pdf)], text=True)
    (out / "pdf-fonts.txt").write_text(font_info)
    assert all("DejaVu" in line.split()[0] and line.split()[-5] == "yes" for line in font_info.splitlines()[2:])
    extracted = subprocess.check_output(["pdftotext", str(pdf), "-"], text=True)
    assert all(text in extracted for text in ("Original assembly", "Scaffolded children", "GC fraction", "0.478"))
    assert not list(root.iter(f"{{{NS}}}image"))
    # Raster delivery is generated from the vector geometry, with no added data samples.
    render(svg, out / "at-900dpi.png", pixel_width, pixel_height)
    tiff = out / "borrelia-editing-900dpi.tif"
    with Image.open(out / "at-900dpi.png") as image:
        paper = Image.new("RGBA", image.size, "white")
        paper.alpha_composite(image.convert("RGBA"))
        paper.convert("RGB").save(tiff, compression="raw", dpi=(dpi, dpi), description=metadata.text)
    with Image.open(tiff) as image:
        assert image.size == (pixel_width, pixel_height) and image.info["dpi"] == (dpi, dpi)
        assert image.tag_v2[270] == metadata.text
        tiff_pixel_sha = sha(image.tobytes())
    tiff_bytes_sha = hashlib.sha256()
    with tiff.open("rb") as original, (out / "borrelia-editing-900dpi.tif.gz").open("wb") as raw_gzip, gzip.GzipFile(filename="", mode="wb", fileobj=raw_gzip, mtime=0) as compressed:
        while chunk := original.read(1024 * 1024):
            tiff_bytes_sha.update(chunk)
            compressed.write(chunk)
    unwrapped_sha = hashlib.sha256()
    with gzip.open(out / "borrelia-editing-900dpi.tif.gz", "rb") as unwrapped:
        while chunk := unwrapped.read(1024 * 1024):
            unwrapped_sha.update(chunk)
    assert unwrapped_sha.digest() == tiff_bytes_sha.digest()
    render(svg, out / "borrelia-editing-preview.png", 2102, round(2102 * height / width))
    result = {"status": "passed", "method": "NATIVE_RGBA_RECTANGLES_V1", "sourceSvgSha256": sha(data), "scriptSha256": sha(Path(__file__).read_bytes()), "runtime": {"python": sys.version.split()[0], "pillow": PIL.__version__, "pypdf": pypdf.__version__, "rsvg": subprocess.check_output(["rsvg-convert", "--version"], text=True).strip()}, "fontFiles": fonts, "images": records, "nativeUnitFullScenePixelEquality": True, "nativeUnitRenderedRgbaSha256": native_scene_sha, "pdfRasterImages": 0, "pdfEmbeddedSourcePngs": len(records), "widthMm": width_mm, "heightMm": height_mm, "tiff": {"dpi": dpi, "widthPixels": pixel_width, "heightPixels": pixel_height, "compression": "UNCOMPRESSED", "storage": "GZIP_WRAPPED_TIFF", "decodedPixelSha256": tiff_pixel_sha, "uncompressedFileSha256": tiff_bytes_sha.hexdigest(), "gzipRoundTripExact": True}, "files": {name: sha((out / name).read_bytes()) for name in (svg.name, pdf.name, "borrelia-editing-900dpi.tif.gz", "borrelia-editing-preview.png")}}
    (out / "artwork-validation.json").write_text(json.dumps(result, indent=2) + "\n")
    print("Passed vector-cell reconstruction, full-scene pixel equality, zero-raster PDF and 900-dpi TIFF")


if __name__ == "__main__":
    main()
