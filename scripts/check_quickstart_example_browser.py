#!/usr/bin/env python3
"""Open the public quick-start example in three clicks in a fresh browser profile."""

import argparse
import gzip
import io
import hashlib
import json
import re
import time
from pathlib import Path

import numpy as np
from PIL import Image
from playwright.sync_api import sync_playwright


def rendered_signal_pixels(page):
    viewport = page.evaluate_handle(
        "() => window.__hictBenchmark.manager().getMap().getViewport()"
    ).as_element()
    pixels = np.array(
        Image.open(io.BytesIO(viewport.screenshot())).convert("RGB")
    ).astype(int)
    return int(
        (
            (pixels[:, :, 0] > pixels[:, :, 1] + 20)
            & (pixels[:, :, 0] > pixels[:, :, 2] + 20)
        ).sum()
    )


parser = argparse.ArgumentParser()
for name in ("url", "api", "output"):
    parser.add_argument("--" + name, required=True)
parser.add_argument(
    "--journey",
    action="store_true",
    help="Continue through edits, session restore and AGP export",
)
parser.add_argument(
    "--example",
    choices=["dnazoo-tardigrade", "geo-borrelia", "geo-yeast-microc"],
    default="dnazoo-tardigrade",
)
parser.add_argument(
    "--data-dir",
    help="Disposable server data folder for independent reference verification",
)
args = parser.parse_args()
examples = {
    "dnazoo-tardigrade": ("nHd_3.1_HiC.hict.hdf5", 5),
    "geo-borrelia": ("borrelia.hict.hdf5", 19),
    "geo-yeast-microc": ("yeast-wt3.hict.hdf5", 17),
}
expected_matrix, expected_contigs = examples[args.example]
if args.example != "dnazoo-tardigrade" and not args.data_dir:
    parser.error(
        "Selected examples require the disposable --data-dir for reference verification"
    )
out = Path(args.output)
out.mkdir(parents=True, exist_ok=True)
with sync_playwright() as pw:
    browser = pw.chromium.launch(headless=True)
    page = browser.new_page(viewport={"width": 1440, "height": 1000})
    errors, console_errors = [], []
    page.on("pageerror", lambda error: errors.append(str(error)))
    page.on(
        "crash",
        lambda: (out / "crash.json").write_text(
            json.dumps({"errors": errors, "consoleErrors": console_errors})
        ),
    )
    page.on(
        "console",
        lambda message: (
            console_errors.append(message.text) if message.type == "error" else None
        ),
    )
    page.add_init_script(
        """window.__HICT_BENCHMARK_ENABLED__=true;
      window.quickStartClicks=[];
      document.addEventListener('click',event=>{if(event.isTrusted)
        window.quickStartClicks.push((event.target.textContent||event.target.ariaLabel||'').trim());},true);"""
    )
    page.route(
        "http://localhost:5000/**",
        lambda route: route.continue_(
            url=args.api + route.request.url.removeprefix("http://localhost:5000")
        ),
    )
    try:
        page.goto(args.url)
        page.wait_for_function("window.__hictBenchmark")
        started = time.monotonic()
        page.get_by_role("link", name="Help", exact=True).click()
        page.get_by_role("link", name="Examples...", exact=True).click()
        dialog = page.get_by_role("dialog", name="Examples", exact=True)
        card = dialog.locator(f'[data-example="{args.example}"]')
        card.get_by_role("button", name="Open example", exact=True).click()
        page.wait_for_function(
            "expected => window.__hictBenchmark.manager()?.getOptions().filename === expected",
            arg=f"examples/{args.example}/{expected_matrix}",
            timeout=300000,
        )
        page.wait_for_function(
            "!document.querySelector('.open-progress-modal')", timeout=60000
        )
        page.wait_for_timeout(1500)
        clicks = page.evaluate("window.quickStartClicks")
        assert len(clicks) == 3, clicks
        assert (
            page.get_by_role("dialog", name="Open File Wizard", exact=True).count() == 0
        )
        state = page.evaluate(
            """() => {const m=window.__hictBenchmark.manager();return {
          filename:m.getOptions().filename,fasta:m.getOptions().fastaFilename,
          contigs:m.contigDimensionHolder.contig_count,
          resolutions:m.viewAndLayersManager.getAllNavigationBpResolutions()};}"""
        )
        assert state["contigs"] == expected_contigs, state
        response = page.request.post(
            args.api + "/sequence/region", data={"startBp": 0, "endBp": 1000}
        )
        assert response.ok, response.text()
        sequence = response.json()["sequence"]
        assert (
            len(sequence) >= 1000 and sum(base in "ACGTacgt" for base in sequence) > 900
        )
        if args.data_dir:
            import cooler

            def fasta_records(path):
                records = {}
                opener = gzip.open if path.suffix == ".gz" else open
                name = None
                with opener(path, "rt") as stream:
                    for line in stream:
                        if line.startswith(">"):
                            name = line[1:].split()[0]
                            assert name not in records
                            records[name] = []
                        elif name:
                            records[name].append(line.strip())
                return {name: "".join(parts) for name, parts in records.items()}

            folder = Path(args.data_dir) / "examples" / args.example
            manifest = json.loads((folder / "manifest.json").read_text())
            reference = fasta_records(Path(args.data_dir) / state["fasta"])
            preparation = manifest["openingRecipe"].get("fastaPreparation")
            if preparation:
                original = fasta_records(folder / "borrelia-original.fa.gz")
                assert list(reference) == [
                    record["target"] for record in preparation["records"]
                ]
                for record in preparation["records"]:
                    assert reference[record["target"]] == original[record["source"]]
                    assert len(reference[record["target"]]) == record["length"]
            if args.example.startswith("geo-"):
                matrix = folder / (
                    "borrelia.mcool"
                    if args.example == "geo-borrelia"
                    else "yeast-wt3.mcool"
                )
                levels = cooler.fileops.list_coolers(str(matrix))
                chromosomes = cooler.Cooler(str(matrix) + "::" + levels[0]).chromsizes
                assert len(chromosomes) == expected_contigs
                for name, length in chromosomes.items():
                    assert name in reference and len(reference[name]) == int(length)
                assert sequence[:1000] == reference[chromosomes.index[0]][:1000]
            else:
                contigs = page.evaluate("""() => window.__hictBenchmark.manager()
                    .contigDimensionHolder.contigDescriptors.map(c =>
                        [c.contigName, c.contigLengthBp, c.direction])""")
                for name, length, direction in contigs:
                    assert name in reference and len(reference[name]) == length
                    assert direction == 1, (name, direction)
                assert sequence[:1000] == reference[contigs[0][0]][:1000]
        red_pixels = rendered_signal_pixels(page)
        assert red_pixels > 1000, red_pixels
        assert not errors, errors
        assert not console_errors, console_errors
        page.screenshot(path=str(out / "quick-start.png"))
        seconds_to_map = time.monotonic() - started
        restored_signal_pixels = None
        journey = []
        if args.journey:
            initial_resolution = page.evaluate(
                "window.__hictBenchmark.manager().getView().getResolution()"
            )
            page.get_by_role("button", name="Zoom in", exact=True).click()
            page.get_by_role("button", name="Zoom in", exact=True).click()
            page.wait_for_function(
                "before => window.__hictBenchmark.manager().getView().getResolution()<before",
                arg=initial_resolution,
            )
            page.wait_for_timeout(600)
            page.evaluate(
                "() => {window.__hictBenchmark.manager().viewAndLayersManager.fitViewToActiveExtent();}"
            )
            page.wait_for_timeout(300)
            journey.append("navigation changes scale and returns to the map extent")

            def snapshot():
                return page.evaluate(
                    "async () => await window.__hictBenchmark.manager().networkManager.requestManager.getSessionSnapshot(true,true,true,true)"
                )

            def file_action(name):
                page.get_by_role("link", name="File", exact=True).click()
                page.get_by_role("link", name=name, exact=True).click()

            def point(fraction):
                return page.evaluate(
                    """fraction => {
                    const m=window.__hictBenchmark.manager(),l=m.viewAndLayersManager;
                    const d=l.currentViewState.resolutionDesciptor,holder=m.contigDimensionHolder;
                    const bp=holder.contigDescriptors[0].contigLengthBp*fraction;
                    const px=holder.getFractionalPxOfBp(bp,d.bpResolution)*d.pixelResolution;
                    const pixel=m.getMap().getPixelFromCoordinate([px,-px]);
                    const rect=m.getMap().getViewport().getBoundingClientRect();
                    return {x:rect.left+pixel[0],y:rect.top+pixel[1]};
                }""",
                    fraction,
                )

            initial = snapshot()["primary"]
            selected, selection_end = point(0.25), point(0.65)
            page.keyboard.down("Shift")
            page.mouse.move(selected["x"], selected["y"])
            page.mouse.down()
            page.mouse.move(selection_end["x"], selection_end["y"], steps=12)
            page.mouse.up()
            page.keyboard.up("Shift")
            page.wait_for_function(
                "window.__hictBenchmark.manager().viewAndLayersManager.currentViewState.selectionBorders.leftContigDescriptorInclusive!==undefined"
            )
            with page.expect_response(
                "**/reverse_selection_range"
            ) as reversed_response:
                page.get_by_role("button", name="Reverse Selection", exact=True).click()
            assert reversed_response.value.ok, reversed_response.value.text()
            page.wait_for_function(
                "!window.__hictBenchmark.manager().networkManager.requestManager.history.busy"
            )
            reversed_state = snapshot()["primary"]
            assert reversed_state["agpSha256"] != initial["agpSha256"]
            journey.append("Reverse changes the assembly")
            # Clear the selection handles before choosing a split point inside it.
            page.keyboard.press("Escape")
            page.get_by_role("button", name=re.compile("^Split contig:")).click()
            cut = point(0.35)
            page.wait_for_function(
                """point => {
                    const m=window.__hictBenchmark.manager();
                    const rect=m.getMap().getViewport().getBoundingClientRect();
                    const pixel=[point.x-rect.left,point.y-rect.top];
                    return !m.getView().getAnimating() &&
                        m.viewAndLayersManager.layersHolder.hicDataLayers.some(layer=>layer.getData(pixel));
                }""",
                arg=cut,
            )
            with page.expect_response("**/split_contig_at_bin") as split_response:
                page.mouse.dblclick(cut["x"], cut["y"])
            assert split_response.value.ok, split_response.value.text()
            page.wait_for_function(
                "count => window.__hictBenchmark.manager().contigDimensionHolder.contig_count === count",
                arg=expected_contigs + 1,
            )
            split_state = snapshot()["primary"]
            assert len(split_state["splitEdits"]) == 1
            with page.expect_response("**/assembly/history/restore") as undo_response:
                page.get_by_role(
                    "button", name="Undo assembly edit", exact=True
                ).click()
            assert undo_response.value.ok, undo_response.value.text()
            page.wait_for_function(
                "count => window.__hictBenchmark.manager().contigDimensionHolder.contig_count === count",
                arg=expected_contigs,
            )
            expected = snapshot()["primary"]
            assert expected["agpSha256"] == reversed_state["agpSha256"]
            journey.append(
                "Split adds a native component and Undo restores the reversed layout"
            )
            with page.expect_download() as session_download:
                file_action("Save session")
            session_path = out / "first-run-session.json"
            session_download.value.save_as(session_path)
            saved = json.loads(session_path.read_text())["snapshot"]["primary"]
            assert saved["agpSha256"] == expected["agpSha256"]
            file_action("Close")
            page.get_by_role(
                "heading", name="No contact map open", exact=True
            ).wait_for()
            with page.expect_file_chooser() as chooser:
                file_action("Open session")
            chooser.value.set_files(session_path)
            page.get_by_text(
                "Session restored. Undo history starts from the restored assembly.",
                exact=True,
            ).wait_for()
            restored = snapshot()["primary"]
            assert restored["agpSha256"] == expected["agpSha256"]
            assert restored["assemblyInfo"] == expected["assemblyInfo"]
            assert restored["fastaFilename"] == state["fasta"]
            journey.append("Save, Close and Open session retain the layout and FASTA")
            page.get_by_role("link", name="Assembly", exact=True).click()
            with page.expect_download() as agp_download:
                page.get_by_role("link", name="Export to AGP", exact=True).click()
            agp_path = out / "first-run.agp"
            agp_download.value.save_as(agp_path)
            components = [
                line.split()
                for line in agp_path.read_text().splitlines()
                if line and not line.startswith("#")
            ]
            assert len(components) == expected_contigs and all(
                len(row) == 9 for row in components
            ), components
            assert (
                hashlib.sha256(agp_path.read_bytes()).hexdigest()
                == expected["agpSha256"]
            )
            journey.append("Exported AGP matches the saved assembly exactly")
            assert not errors and not console_errors, {
                "errors": errors,
                "consoleErrors": console_errors,
            }
            page.wait_for_timeout(1000)
            restored_signal_pixels = rendered_signal_pixels(page)
            assert restored_signal_pixels > 1000, restored_signal_pixels
            page.screenshot(path=str(out / "first-run-restored.png"))
        result = {
            "secondsToMap": seconds_to_map,
            "totalJourneySeconds": time.monotonic() - started,
            "restoredSignalPixels": restored_signal_pixels,
            "example": args.example,
            "clicks": clicks,
            "map": state,
            "redSignalPixels": red_pixels,
            "sequenceStart": sequence[:100],
            "errors": errors,
            "consoleErrors": console_errors,
            "jobs": page.request.get(args.api + "/jobs").json(),
            "journey": journey,
        }
        (out / "results.json").write_text(json.dumps(result, indent=2))
        print(json.dumps(result), flush=True)
    except BaseException:
        import traceback

        (out / "failure.txt").write_text(traceback.format_exc())
        (out / "errors.json").write_text(
            json.dumps({"errors": errors, "consoleErrors": console_errors})
        )
        try:
            page.screenshot(path=str(out / "failure.png"))
            (out / "failure.html").write_text(page.content())
        except Exception:
            pass
        raise
    finally:
        browser.close()
