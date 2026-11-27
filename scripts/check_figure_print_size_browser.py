#!/usr/bin/env python3
"""Verify actual SVG/PDF/PNG physical sizes without resampling captured contacts."""

import argparse
import functools
import hashlib
import http.server
import json
from pathlib import Path
import re
import struct
import subprocess
import threading
import xml.etree.ElementTree as ET
import zlib
from PIL import Image
from playwright.sync_api import expect, sync_playwright
from sequence_browser_harness import BrowserCheck, HictTestServer, configure_test_api


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("jar", "webui", "output"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--warning-recovery", action="store_true")
    parser.add_argument("--fixture-java", default="java")
    args = parser.parse_args()
    out = Path(args.output).absolute()
    assert out == out.resolve() and out.is_relative_to("/tmp")
    out.mkdir(exist_ok=False)
    data = out / "data"
    data.mkdir()
    jar = Path(args.jar).resolve()
    with (out / "fixture.log").open("w") as log:
        subprocess.run(
            [
                args.fixture_java,
                "--class-path",
                str(jar),
                "src/test/java/ru/itmo/ctlab/hict/fixtures/ReinforcementHdf5Fixture.java",
                str(data),
            ],
            check=True,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
    (data / "features.bed").write_text(
        "tiny\t0\t5\tFirst feature\ntiny\t8\t15\tSecond feature\n"
    )
    native = data / "tiny-primary.hict.hdf5"
    digest = hashlib.sha256(native.read_bytes()).hexdigest()

    class Quiet(http.server.SimpleHTTPRequestHandler):
        def log_message(self, *_):
            pass

    web = http.server.ThreadingHTTPServer(
        ("127.0.0.1", 0),
        functools.partial(Quiet, directory=str(Path(args.webui).resolve())),
    )
    threading.Thread(target=web.serve_forever, daemon=True).start()
    try:
        with HictTestServer(
            jar, data, out / "server.log"
        ) as api, sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            page = browser.new_page(viewport={"width": 1440, "height": 1000})
            page.set_default_timeout(90000)
            page.add_init_script("window.__HICT_BENCHMARK_ENABLED__=true")
            configure_test_api(page, api.url)
            check = BrowserCheck(page, out)
            checks = []
            dialog = page.get_by_role("dialog", name="Export figure", exact=True)

            def save(stem, extension):
                with page.expect_download(timeout=90000) as download:
                    dialog.locator("#export-" + extension + "-button").click()
                path = out / (stem + "." + extension)
                download.value.save_as(path)
                return path

            def metadata(path):
                if path.suffix == ".png":
                    with Image.open(path) as image:
                        return json.loads(image.text["HiCT provenance"])
                if path.suffix == ".svg":
                    return json.loads(
                        ET.parse(path)
                        .getroot()
                        .find("{http://www.w3.org/2000/svg}metadata")
                        .text
                    )
                root = ET.fromstring(
                    subprocess.check_output(["pdfinfo", "-meta", str(path)])
                )
                return json.loads(root.find(".//{urn:hict:figure#}metadata").text)

            def record(name, **details):
                checks.append({"test": name, "status": "passed", **details})
                page.screenshot(path=str(out / (name + "-ui.png")))
                print("Passed", name, flush=True)

            def png_chunks(path):
                data = path.read_bytes()
                assert data[:8] == b"\x89PNG\r\n\x1a\n"
                chunks = []
                offset = 8
                while offset < len(data):
                    length = struct.unpack(">I", data[offset : offset + 4])[0]
                    kind = data[offset + 4 : offset + 8]
                    payload = data[offset + 8 : offset + 8 + length]
                    crc = struct.unpack(
                        ">I", data[offset + 8 + length : offset + 12 + length]
                    )[0]
                    assert crc == zlib.crc32(kind + payload)
                    chunks.append((kind, payload))
                    offset += length + 12
                assert offset == len(data)
                return chunks

            try:
                page.goto(f"http://127.0.0.1:{web.server_port}/")
                page.evaluate(
                    "async api=>await window.__hictBenchmark.open('tiny-primary.hict.hdf5',api)",
                    api.url,
                )
                page.evaluate(
                    "async()=>await window.__hictBenchmark.manager().linearTrackManager.openTrack('features.bed')"
                )
                check.settle()
                page.locator("#export-dropdown-toggle").click()
                expect(dialog.locator("#export-png-button")).to_be_enabled()
                expect(dialog).to_contain_text("Smallest export text: 6.0 pt")
                auto = save("auto-label-size", "svg")
                assert metadata(auto)["outputSize"]["minimumTextPoints"] == 6
                def smallest_svg_label_points(path):
                    return page.evaluate("""source => {
                    const doc = new DOMParser().parseFromString(source, "image/svg+xml");
                    const svg = document.importNode(doc.documentElement, true);
                    for (const image of svg.querySelectorAll("image")) image.remove();
                    svg.style.position = "fixed";
                    svg.style.opacity = "0";
                    document.body.append(svg);
                    try {
                        const sizes = [...svg.querySelectorAll("text,tspan")]
                            .filter(node => [...node.childNodes].some(child =>
                                child.nodeType === Node.TEXT_NODE && child.textContent.trim()))
                            .map(node => {
                                const matrix = node.getScreenCTM();
                                return matrix ? Number.parseFloat(getComputedStyle(node).fontSize) *
                                    Math.hypot(matrix.c, matrix.d) * 0.75 : 0;
                            }).filter(size => size > 0);
                        return Math.min(...sizes);
                    } finally { svg.remove(); }
                }""", path.read_text())

                printed = smallest_svg_label_points(auto)
                assert 5.95 <= printed <= 6.05, printed
                check.advanced()
                label_size = dialog.get_by_label("Label size")
                print_size = dialog.get_by_label("Figure print size", exact=True)
                if args.warning_recovery:
                    label_size.select_option("captured")
                    print_size.select_option("custom")
                    dialog.get_by_label("Width (mm)", exact=True).fill("22")
                    warning = dialog.get_by_role("status").filter(
                        has_text="It may be unreadable in print."
                    )
                    expect(warning).to_be_visible()
                    expect(dialog.locator("#export-svg-button")).to_be_enabled()
                    larger_labels = warning.get_by_role("button", name="Use larger labels", exact=True)
                    larger_labels.focus()
                    larger_labels.press("Enter")
                    expect(label_size).to_have_value("auto")
                    readable = dialog.get_by_role("status").filter(has_text="Smallest export text:")
                    expect(readable).to_contain_text("6.0 pt")
                    expect(readable).to_be_focused()
                    enlarged = save("readable-22", "svg")
                    assert 5.95 <= smallest_svg_label_points(enlarged) <= 6.05
                    checks.append({
                        "test": "small-print-text-warning-offers-keyboard-recovery-and-keeps-save-available",
                        "status": "passed",
                    })
                label_size.select_option("captured")
                print_size.select_option("0")
                original = save("captured", "png")
                original_metadata = metadata(original)
                original_chunks = png_chunks(original)
                with Image.open(original) as image:
                    original_pixels, dimensions = (
                        image.convert("RGBA").tobytes(),
                        image.size,
                    )
                for mm in (86, 178):
                    dialog.get_by_label("Figure print size", exact=True).select_option(
                        str(mm)
                    )
                    paths = {
                        extension: save(f"print-{mm}", extension)
                        for extension in ("png", "svg", "pdf")
                    }
                    records = [metadata(p) for p in paths.values()]
                    assert records[0] == records[1] == records[2]
                    info = records[0].pop("outputSize")
                    assert records[0] == original_metadata
                    assert info["widthMm"] == mm
                    assert (
                        abs(info["heightMm"] - mm * dimensions[1] / dimensions[0])
                        < 1e-9
                    )
                    assert info["rasterPolicy"] == "PRESERVE_CAPTURE_PIXELS"
                    assert (
                        abs(info["effectiveRasterDpi"] - dimensions[0] * 25.4 / mm)
                        < 1e-9
                    )
                    with Image.open(paths["png"]) as image:
                        assert image.size == dimensions
                        assert image.convert("RGBA").tobytes() == original_pixels
                    chunks = png_chunks(paths["png"])
                    assert [x for x in chunks if x[0] == b"IDAT"] == [
                        x for x in original_chunks if x[0] == b"IDAT"
                    ]
                    physical = [payload for kind, payload in chunks if kind == b"pHYs"]
                    assert len(physical) == 1
                    x, y, unit = struct.unpack(">IIB", physical[0])
                    assert unit == 1 and x == y
                    assert abs(dimensions[0] * 1000 / x - mm) < 0.005
                    assert (
                        len(
                            [
                                x
                                for x in chunks
                                if x[0] == b"iTXt"
                                and x[1].startswith(b"HiCT provenance\0")
                            ]
                        )
                        == 1
                    )
                    root = ET.parse(paths["svg"]).getroot()
                    expected_points = smallest_svg_label_points(paths["svg"])
                    expect(dialog).to_contain_text(
                        f"Smallest export text: {expected_points:.1f} pt at this width."
                    )
                    assert root.attrib["width"] == str(mm) + "mm"
                    assert (
                        abs(float(root.attrib["height"][:-2]) - info["heightMm"]) < 1e-9
                    )
                    assert (
                        root.attrib["viewBox"] == f"0 0 {dimensions[0]} {dimensions[1]}"
                    )
                    pdfinfo = subprocess.check_output(
                        ["pdfinfo", str(paths["pdf"])], text=True
                    )
                    (out / f"print-{mm}-pdfinfo.txt").write_text(pdfinfo)
                    size = re.search(r"Page size:\s*([\d.]+) x ([\d.]+) pts", pdfinfo)
                    assert size, pdfinfo
                    assert abs(float(size[1]) * 25.4 / 72 - mm) < 0.01
                    assert abs(float(size[2]) * 25.4 / 72 - info["heightMm"]) < 0.01
                    text = subprocess.check_output(
                        ["pdftotext", str(paths["pdf"]), "-"], text=True
                    )
                    assert "features.bed" in text
                    subprocess.run(
                        [
                            "rsvg-convert",
                            "--width",
                            str(dimensions[0]),
                            "--height",
                            str(dimensions[1]),
                            "--output",
                            str(out / f"print-{mm}-svg.png"),
                            str(paths["svg"]),
                        ],
                        check=True,
                    )
                    subprocess.run(
                        [
                            "pdftoppm",
                            "-scale-to-x",
                            str(dimensions[0]),
                            "-scale-to-y",
                            str(dimensions[1]),
                            "-singlefile",
                            "-png",
                            str(paths["pdf"]),
                            str(out / f"print-{mm}-pdf"),
                        ],
                        check=True,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                    )
                    with Image.open(original) as ref, Image.open(
                        out / f"print-{mm}-svg.png"
                    ) as svg, Image.open(out / f"print-{mm}-pdf.png") as pdf:
                        ref, svg, pdf = [im.convert("RGBA") for im in (ref, svg, pdf)]
                        assert ref.size == svg.size == pdf.size
                        samples = 0
                        for py in range(4, dimensions[1] - 4, 13):
                            for px in range(4, dimensions[0] - 4, 13):
                                c = ref.getpixel((px, py))
                                if not (c[0] > c[1] + 10 and c[0] > c[2] + 10):
                                    continue
                                if any(
                                    ref.getpixel((px + dx, py + dy)) != c
                                    for dx, dy in ((-2, 0), (2, 0), (0, -2), (0, 2))
                                ):
                                    continue
                                assert all(
                                    max(
                                        abs(a - b)
                                        for a, b in zip(c, im.getpixel((px, py)))
                                    )
                                    <= 4
                                    for im in (svg, pdf)
                                ), (
                                    mm,
                                    px,
                                    py,
                                    c,
                                    svg.getpixel((px, py)),
                                    pdf.getpixel((px, py)),
                                )
                                samples += 1
                        assert samples > 100, samples
                    record(
                        f"print-{mm}-physical-size-provenance-vector-labels-and-unchanged-pixels",
                        flatInteriorMatches=samples,
                        outputSize=info,
                    )
                page.set_viewport_size({"width": 640, "height": 700})
                expect(
                    dialog.get_by_label("Figure print size", exact=True)
                ).to_be_visible()
                assert page.evaluate("document.documentElement.scrollWidth<=innerWidth")
                record("print-size-controls-fit-a-narrow-window")
                dialog.get_by_label("Figure print size", exact=True).select_option("0")
                again = save("captured-again", "png")
                assert again.read_bytes() == original.read_bytes()
                assert hashlib.sha256(native.read_bytes()).hexdigest() == digest
                record(
                    "return-to-captured-size-keeps-original-frozen-png-bytes-and-native-source"
                )
                page.set_viewport_size({"width": 1440, "height": 1000})
                dialog.get_by_label("Figure print size", exact=True).select_option(
                    "178"
                )
                dialog.get_by_label("Snapshot name", exact=True).fill("Reference panel")
                dialog.get_by_role("button", name="Save snapshot", exact=True).click()
                expect(
                    dialog.get_by_role("button", name="Snapshots (1)", exact=True)
                ).to_be_enabled()
                stored = page.evaluate(
                    "()=>window.__hictBenchmark.manager().figureSnapshots.documents()[0]"
                )
                assert [stored["width"], stored["height"]] == list(dimensions)
                assert "outputSize" not in json.loads(
                    ET.fromstring(stored["svg"])
                    .find("{http://www.w3.org/2000/svg}metadata")
                    .text
                )
                assert ET.fromstring(stored["svg"]).attrib["width"] == str(
                    dimensions[0]
                )
                dialog.get_by_role("button", name="Snapshots (1)", exact=True).click()
                dialog.get_by_role(
                    "button", name="Compare to snapshot", exact=True
                ).click()
                dialog.get_by_label("Comparison layout", exact=True).select_option(
                    "vertical"
                )
                dialog.get_by_role(
                    "button", name="Update comparison", exact=True
                ).click()
                expect(dialog.locator("#export-pdf-button")).to_be_enabled()
                portrait = {
                    extension: save("portrait-comparison", extension)
                    for extension in ("png", "svg", "pdf")
                }
                portrait_meta = [metadata(path) for path in portrait.values()]
                assert portrait_meta[0] == portrait_meta[1] == portrait_meta[2]
                info = portrait_meta[0]["outputSize"]
                assert info["widthMm"] == 178 and info["heightMm"] > 178
                assert portrait_meta[0]["comparison"]["layout"] == "vertical"
                pdfinfo = subprocess.check_output(
                    ["pdfinfo", str(portrait["pdf"])], text=True
                )
                size = re.search(r"Page size:\s*([\d.]+) x ([\d.]+) pts", pdfinfo)
                assert size and abs(float(size[1]) * 25.4 / 72 - 178) < 0.01
                assert abs(float(size[2]) * 25.4 / 72 - info["heightMm"]) < 0.01
                text = subprocess.check_output(
                    ["pdftotext", str(portrait["pdf"]), "-"], text=True
                )
                assert "Reference panel" in text and "features.bed" in text
                assert hashlib.sha256(native.read_bytes()).hexdigest() == digest
                record(
                    "portrait-comparison-physical-page-and-captured-snapshot-independence",
                    outputSize=info,
                )
                assert not check.errors, check.errors
                (out / "results.json").write_text(
                    json.dumps(
                        {
                            "checks": checks,
                            "errors": check.errors,
                            "sourceSha256": digest,
                        },
                        indent=2,
                    )
                )
            finally:
                page.screenshot(path=str(out / "final-ui.png"))
                browser.close()
    finally:
        web.shutdown()
        web.server_close()


if __name__ == "__main__":
    main()
