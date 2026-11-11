#!/usr/bin/env python3
"""Verify toolbar edits and real Move arrows with exact native Undo/Redo states."""

import argparse
import json
import re
from pathlib import Path
from playwright.sync_api import sync_playwright
from sequence_browser_harness import BrowserCheck


def prepare_fixture(directory, jar):
    """Build an independent four-contig, two-level map through the real converter."""
    import subprocess
    import cooler
    import numpy as np
    import pandas as pd

    directory = Path(directory).absolute()
    if not directory.is_relative_to("/tmp") or directory != directory.resolve():
        raise ValueError("Use a nonsymlinked disposable DATA directory under /tmp")
    matrix = directory / "structural.mcool"
    target = directory / "structural.hict.hdf5"
    if matrix.exists() or target.exists():
        raise ValueError("Fixture generation requires fresh output paths")
    for resolution in (100, 200):
        bins_per_contig = 400000 // resolution
        count = bins_per_contig * 4
        bins = pd.DataFrame(
            {
                "chrom": np.repeat(["one", "two", "three", "four"], bins_per_contig),
                "start": np.tile(np.arange(bins_per_contig) * resolution, 4),
                "end": np.tile(np.arange(1, bins_per_contig + 1) * resolution, 4),
            }
        )
        pixels = pd.DataFrame(
            {
                "bin1_id": np.arange(count),
                "bin2_id": np.arange(count),
                "count": np.arange(count) % 100 + 1,
            }
        )
        cooler.create_cooler(
            str(matrix) + f"::resolutions/{resolution}",
            bins,
            pixels,
            ordered=True,
            mode="a",
        )
    converter = directory / "StructuralFixture.java"
    converter.write_text("""import java.nio.file.Path;
import java.util.List;
import ru.itmo.ctlab.hict.hict_library.converters.*;
class StructuralFixture {
    public static void main(String[] args) {
        new McoolToHictConverter().convert(new ConversionOptions(
            Path.of(args[0]), Path.of(args[1]), List.of(100L, 200L), 8192, 1,
            ConversionOptions.CompressionAlgorithm.DEFLATE, "", false, 4,
            true, false, false, false, ConversionOptions.ExportMode.INTERNAL),
            System.err::println);
    }
}
""")
    with (directory / "fixture-conversion.log").open("w") as log:
        subprocess.run(
            [
                "java",
                "--class-path",
                str(Path(jar).resolve()),
                str(converter),
                str(matrix),
                str(target),
            ],
            check=True,
            stdout=log,
            stderr=subprocess.STDOUT,
        )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("url", "api", "output"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument(
        "--data-dir", help="Fresh disposable server DATA_DIR for fixture generation"
    )
    parser.add_argument("--jar", help="Immutable JAR used to convert the fixture")
    args = parser.parse_args()
    if bool(args.data_dir) != bool(args.jar):
        parser.error("--data-dir and --jar must be supplied together")
    if args.data_dir:
        prepare_fixture(args.data_dir, args.jar)
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    results, errors = [], []
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1440, "height": 1000})
        page.set_default_timeout(60000)
        page.add_init_script("window.__HICT_BENCHMARK_ENABLED__=true")
        page.route(
            "http://localhost:5000/**",
            lambda route: route.continue_(
                url=args.api + route.request.url.removeprefix("http://localhost:5000")
            ),
        )
        page.on("pageerror", lambda error: errors.append(str(error)))
        page.on(
            "console",
            lambda message: (
                errors.append(message.text) if message.type == "error" else None
            ),
        )
        check = BrowserCheck(page, output)

        def snapshot():
            return page.evaluate(
                "async () => (await window.__hictBenchmark.manager().networkManager.requestManager.getSessionSnapshot(true)).primary"
            )

        def select_orders(first=0, last=0):
            page.evaluate(
                "() => {window.__hictBenchmark.manager().viewAndLayersManager.fitViewToActiveExtent();}"
            )
            check.settle()
            points = page.evaluate(
                """({first,last}) => {
                const m=window.__hictBenchmark.manager(),h=m.contigDimensionHolder;
                const d=m.viewAndLayersManager.currentViewState.resolutionDesciptor;
                const bounds=m.getMap().getViewport().getBoundingClientRect();
                return [[first,.25],[last,.65]].map(([order,fraction])=>{
                    const bp=h.prefix_sum_bp[order]+h.contigDescriptors[order].contigLengthBp*fraction;
                    const px=h.getFractionalPxOfBp(bp,d.bpResolution)*d.pixelResolution;
                    const p=m.getMap().getPixelFromCoordinate([px,-px]);return [bounds.left+p[0],bounds.top+p[1]];
                });
            }""",
                {"first": first, "last": last},
            )
            page.keyboard.down("Shift")
            page.mouse.move(*points[0])
            page.mouse.down()
            page.mouse.move(*points[1], steps=12)
            page.mouse.up()
            page.keyboard.up("Shift")
            page.wait_for_function(
                """({first,last}) => {
                const s=window.__hictBenchmark.manager().viewAndLayersManager.currentViewState.selectionBorders;
                return s.leftContigOrderInclusive===first && s.rightContigOrderInclusive===last;
            }""",
                arg={"first": first, "last": last},
            )

        def button_action(name, endpoint):
            with page.expect_response(
                lambda response: response.url.endswith(endpoint)
            ) as response:
                page.get_by_role("button", name=name, exact=True).click()
            assert response.value.ok, response.value.text()
            check.settle()

        def round_trip(name, action):
            before = snapshot()
            action()
            after = snapshot()
            assert after["agpSha256"] != before["agpSha256"], name
            for button, expected in [
                ("Undo assembly edit", before),
                ("Redo assembly edit", after),
                ("Undo assembly edit", before),
            ]:
                button_action(button, "/assembly/history/restore")
                actual = snapshot()
                assert actual["agpSha256"] == expected["agpSha256"], (name, button)
                assert actual["assemblyInfo"] == expected["assemblyInfo"], (
                    name,
                    button,
                )
            results.append(
                {
                    "test": name,
                    "before": before["agpSha256"],
                    "after": after["agpSha256"],
                    "undoRedoExact": True,
                }
            )
            page.screenshot(path=str(output / (name + ".png")))

        try:
            page.goto(args.url)
            page.wait_for_function("window.__hictBenchmark")
            page.evaluate(
                "async api => {await window.__hictBenchmark.open('structural.hict.hdf5',api);}",
                args.api,
            )
            check.settle()
            assert (
                page.evaluate(
                    "window.__hictBenchmark.manager().contigDimensionHolder.contig_count"
                )
                == 4
            )
            button_action("Unscaffold all", "/unscaffold_all")
            select_orders()
            round_trip(
                "reverse",
                lambda: button_action("Reverse Selection", "/reverse_selection_range"),
            )
            select_orders(0, 1)
            round_trip(
                "group",
                lambda: button_action(
                    "Group selection into scaffold", "/group_contigs_into_scaffold"
                ),
            )
            select_orders(0, 1)
            button_action(
                "Group selection into scaffold", "/group_contigs_into_scaffold"
            )
            select_orders(0, 1)
            round_trip(
                "ungroup",
                lambda: button_action(
                    "Ungroup selection into singleton scaffolds",
                    "/ungroup_contigs_from_scaffold",
                ),
            )
            round_trip(
                "unscaffold-all",
                lambda: button_action("Unscaffold all", "/unscaffold_all"),
            )
            button_action("Unscaffold all", "/unscaffold_all")
            select_orders()
            round_trip(
                "debris",
                lambda: button_action(
                    "Move selection to debris", "/move_selection_to_debris"
                ),
            )

            def move_by_arrow():
                select_orders()
                order = page.evaluate(
                    "window.__hictBenchmark.manager().contigDimensionHolder.contigDescriptors.map(c=>c.contigId)"
                )
                # Start at the coarser native level so the subsequent zoom crosses a level.
                page.evaluate("""() => {
                    const m=window.__hictBenchmark.manager(),l=m.viewAndLayersManager;
                    m.getView().setResolution(Math.max(...l.pixelResolutionSet));
                }""")
                check.settle()
                old_resolution = page.evaluate(
                    "window.__hictBenchmark.manager().viewAndLayersManager.currentViewState.resolutionDesciptor.bpResolution"
                )
                page.get_by_role(
                    "button", name="Enter translocation mode", exact=True
                ).click()
                before_escape = snapshot()
                assert page.evaluate(
                    "window.__hictBenchmark.manager().viewAndLayersManager.currentViewState.activeTool"
                ) == 0
                page.keyboard.press("Escape")
                assert page.evaluate(
                    "window.__hictBenchmark.manager().viewAndLayersManager.currentViewState.activeTool"
                ) is None
                assert snapshot() == before_escape
                assert page.evaluate("""() => window.__hictBenchmark.manager().viewAndLayersManager
                    .layersHolder.contigTranslocationArrowsLayers.every(layer=>!layer.getVisible())""")
                select_orders()
                page.get_by_role(
                    "button", name="Enter translocation mode", exact=True
                ).click()
                for _ in range(12):
                    page.get_by_role("button", name="Zoom in", exact=True).click()
                    check.settle()
                    if (
                        page.evaluate(
                            "window.__hictBenchmark.manager().viewAndLayersManager.currentViewState.resolutionDesciptor.bpResolution"
                        )
                        != old_resolution
                    ):
                        break
                page.wait_for_function(
                    "bp => window.__hictBenchmark.manager().viewAndLayersManager.currentViewState.resolutionDesciptor.bpResolution!==bp",
                    arg=old_resolution,
                )
                page.evaluate("""() => {
                    const m=window.__hictBenchmark.manager(),l=m.viewAndLayersManager,h=m.contigDimensionHolder;
                    const d=l.currentViewState.resolutionDesciptor;
                    const p=h.getFractionalPxOfBp(h.prefix_sum_bp[3],d.bpResolution)*d.pixelResolution;
                    m.getView().setCenter([p,-p]);m.getMap().renderSync();
                }""")
                check.settle()
                page.wait_for_function(
                    """id => {
                    const l=window.__hictBenchmark.manager().viewAndLayersManager;
                    const source=l.layersHolder.bpResolutionToContigTranslocationArrowsLayer.get(l.currentViewState.resolutionDesciptor.bpResolution)?.getSource();
                    return source?.getFeatures().some(f=>f.get('rightContigDescriptor')?.contigId===id);
                }""",
                    arg=order[3],
                )
                target = page.evaluate(
                    """id => {
                    const m=window.__hictBenchmark.manager(),l=m.viewAndLayersManager;
                    const f=l.layersHolder.bpResolutionToContigTranslocationArrowsLayer.get(l.currentViewState.resolutionDesciptor.bpResolution).getSource().getFeatures()
                        .find(f=>f.get('rightContigDescriptor')?.contigId===id);
                    const coords=f.getGeometry().getInteriorPoints().getCoordinates()[0];
                    const p=m.getMap().getPixelFromCoordinate(coords),r=m.getMap().getViewport().getBoundingClientRect();
                    return [r.left+p[0],r.top+p[1]];
                }""",
                    order[3],
                )
                with page.expect_response("**/move_selection_range") as response:
                    page.mouse.move(*target)
                    page.mouse.click(*target)
                assert response.value.ok, response.value.text()
                check.settle()
                actual = page.evaluate(
                    "window.__hictBenchmark.manager().contigDimensionHolder.contigDescriptors.map(c=>c.contigId)"
                )
                assert actual == [order[1], order[2], order[0], order[3]], actual
                assert (
                    page.get_by_role(
                        "button", name="Enter translocation mode", exact=True
                    ).count()
                    == 1
                )

            round_trip("move-arrow-after-resolution-change", move_by_arrow)
            select_orders()

            def split():
                # Existing selection handles consume double-clicks at their border.
                page.evaluate(
                    "() => {window.__hictBenchmark.manager().eventManager.resetSelection();}"
                )
                page.get_by_role("button", name=re.compile("^Split contig:")).click()
                target = page.evaluate("""() => {
                    const m=window.__hictBenchmark.manager(),l=m.viewAndLayersManager,h=m.contigDimensionHolder;
                    const d=l.currentViewState.resolutionDesciptor;
                    const px=h.getFractionalPxOfBp(h.contigDescriptors[0].contigLengthBp*.4,d.bpResolution)*d.pixelResolution;
                    const p=m.getMap().getPixelFromCoordinate([px,-px]),r=m.getMap().getViewport().getBoundingClientRect();
                    return [r.left+p[0],r.top+p[1]];
                }""")
                with page.expect_response("**/split_contig_at_bin") as response:
                    page.mouse.dblclick(*target)
                assert response.value.ok, response.value.text()
                page.wait_for_function(
                    "window.__hictBenchmark.manager().contigDimensionHolder.contig_count===5"
                )
                check.settle()

            round_trip("split", split)
            assert not errors and not check.errors, errors + check.errors
        finally:
            page.screenshot(path=str(output / "last-state.png"))
            (output / "last-state.json").write_text(
                json.dumps(
                    page.evaluate("""() => {
                const m=window.__hictBenchmark?.manager(),map=m?.getMap();
                return {view:map?.getView().getProperties(),complete:map?.renderComplete_,loading:map?.tileQueue_.getTilesLoading(),queued:map?.tileQueue_.getCount()};
            }"""),
                    default=str,
                    indent=2,
                )
            )
            (output / "results.json").write_text(
                json.dumps(
                    {"checks": results, "errors": errors + check.errors}, indent=2
                )
            )
            browser.close()
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
