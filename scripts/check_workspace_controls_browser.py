#!/usr/bin/env python3
"""Check navigation, bookmarks, presets, OSD fields and notification outcomes."""

import argparse
import json
import re
import xml.etree.ElementTree as ET
from pathlib import Path

from playwright.sync_api import sync_playwright
from sequence_browser_harness import BrowserCheck


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("url", "api", "output"):
        parser.add_argument("--" + name, required=True)
    args = parser.parse_args()
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1440, "height": 1000})
        page.set_default_timeout(30000)
        page.add_init_script("window.__HICT_BENCHMARK_ENABLED__=true")
        page.route(
            "http://localhost:5000/**",
            lambda route: route.continue_(
                url=args.api + route.request.url.removeprefix("http://localhost:5000")
            ),
        )
        check = BrowserCheck(page, output)

        def view():
            return page.evaluate(
                "() => {const v=window.__hictBenchmark.manager().getView();return {center:v.getCenter(),resolution:v.getResolution(),rotation:v.getRotation()};}"
            )

        def navigate():
            original = view()
            assembly = page.evaluate(
                "async () => (await window.__hictBenchmark.manager().networkManager.requestManager.getSessionSnapshot(true)).primary.agpSha256"
            )
            page.locator("a.navbar-brand").click()
            assert view() == original
            assert (
                page.evaluate(
                    "async () => (await window.__hictBenchmark.manager().networkManager.requestManager.getSessionSnapshot(true)).primary.agpSha256"
                )
                == assembly
            )
            page.evaluate("""() => {
                const m=window.__hictBenchmark.manager();
                m.getView().setResolution(m.viewAndLayersManager.pixelResolutionSet[0]);
                m.getView().setCenter([3000,-6000]);
            }""")
            check.settle()
            for label in ("vertically", "horizontally", "over counter-diagonal"):
                page.evaluate(
                    "() => {window.__hictBenchmark.manager().getView().setCenter([3000,-6000]);}"
                )
                check.settle()
                before = view()["center"]
                page.get_by_role(
                    "button", name="Slide to the main diagonal " + label, exact=True
                ).click()
                check.settle()
                after = view()["center"]
                expected = (
                    [before[0], -before[0]]
                    if label == "vertically"
                    else (
                        [-before[1], before[1]]
                        if label == "horizontally"
                        else [(before[0] - before[1]) / 2, -(before[0] - before[1]) / 2]
                    )
                )
                assert max(abs(a - b) for a, b in zip(after, expected)) < 1, (
                    label,
                    before,
                    after,
                    expected,
                )
            page.evaluate(
                "() => {window.__hictBenchmark.manager().getView().setRotation(Math.PI/4);}"
            )
            center = view()["center"]
            page.locator(".ol-rotate-reset").click()
            check.settle()
            assert abs(view()["rotation"]) < 1e-8 and view()["center"] == center
            scissors = page.locator("button").filter(has=page.locator("i.bi-scissors"))
            scissors.click()
            assert scissors.get_attribute("aria-pressed") == "true"
            page.keyboard.press("Escape")
            assert scissors.get_attribute("aria-pressed") == "false"
            before = view()
            thumb = page.locator(".ol-zoomslider-thumb").bounding_box()
            page.mouse.move(
                thumb["x"] + thumb["width"] / 2, thumb["y"] + thumb["height"] / 2
            )
            page.mouse.down()
            page.mouse.move(
                thumb["x"] + thumb["width"] / 2,
                thumb["y"] + thumb["height"] / 2 - 30,
                steps=12,
            )
            page.mouse.up()
            check.settle()
            assert 0 < view()["resolution"] < before["resolution"]
            assert view()["center"] == before["center"]

            before_modes = view()
            ruler = page.locator("#ruler-mode-button")
            assert ruler.inner_text().strip() == "Ruler: Global"
            for mode, label in (("contig", "In contig"), ("scaffold", "In scaffold"), ("global", "Global")):
                ruler.click()
                check.settle()
                assert ruler.inner_text().strip() == "Ruler: " + label
                assert page.evaluate("""() => document.querySelector('#app').__vue_app__.config.globalProperties
                    .$pinia._s.get('uiSettings').rulerCoordinateMode""") == mode
                assert view() == before_modes
                page.screenshot(path=str(output / ("ruler-mode-" + mode + ".png")))

        def caption_sides():
            page.evaluate(
                "window.__hictBenchmark.manager().viewAndLayersManager.fitViewToActiveExtent()"
            )
            check.settle()
            original = view()
            toggle = page.locator('button[aria-controls="layers-block"]')
            toggle.click()
            collapsed = toggle.get_attribute("aria-expanded") == "false"
            if not collapsed:
                toggle.click()
            assert not page.locator("#layers-block").is_visible()
            toggle.click()
            assert page.locator("#layers-block").is_visible()
            positions = {}
            for label, value in (
                ("Above", "2"),
                ("Below", "1"),
                ("Left", "4"),
                ("Right", "5"),
                ("Inside", "0"),
            ):
                for kind in ("Contig", "Scaffold"):
                    page.get_by_label(
                        kind + " names caption side", exact=True
                    ).select_option(value)
                check.settle()
                assert view() == original
                with page.expect_download() as download:
                    page.evaluate(
                        "window.__hictBenchmark.manager().exportCurrentMapSvg(undefined,{scope:'viewport',cropToMap:false})"
                    )
                target = output / ("caption-side-" + label.lower() + ".svg")
                download.value.save_as(target)
                root = ET.parse(target).getroot()
                positions[label] = {}
                for name in ("two", "scf_two"):
                    matches = [
                        element
                        for element in root.iter()
                        if element.tag.endswith("}text")
                        and element.text == name
                        and "12px" in element.get("style", "")
                        and element.get("fill") != "none"
                    ]
                    assert matches, (label, name)
                    parents = {
                        child: parent for parent in root.iter() for child in parent
                    }
                    points = set()
                    for element in matches:
                        x, y = float(element.attrib["x"]), float(element.attrib["y"])
                        node = element
                        while node is not None:
                            transform = node.get("transform")
                            if transform:
                                assert transform.startswith("matrix("), transform
                                a, b, c, d, e, f = map(
                                    float,
                                    re.findall(
                                        r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?", transform
                                    ),
                                )
                                x, y = a * x + c * y + e, b * x + d * y + f
                            node = parents.get(node)
                        points.add((x, y))
                    assert len(points) == 1, (label, name, points)
                    positions[label][name] = list(points.pop())
            for name in ("two", "scf_two"):
                assert (
                    positions["Above"][name][1]
                    < positions["Inside"][name][1]
                    < positions["Below"][name][1]
                ), (name, positions)
                assert (
                    positions["Left"][name][0]
                    < positions["Inside"][name][0]
                    < positions["Right"][name][0]
                ), (name, positions)
            (output / "caption-positions.json").write_text(
                json.dumps(positions, indent=2) + "\n"
            )

        def scopes_and_search():
            def bounds():
                return page.evaluate(
                    "window.__hictBenchmark.manager().getLayersManager().getActiveMapPixelBounds(100)"
                )

            def choose(label, query, kind, name):
                field = page.get_by_role("textbox", name=label, exact=True)
                field.fill(query)
                options = field.locator("..").locator(".scope-result")
                options.filter(has=page.get_by_text(kind, exact=True)).filter(
                    has=page.get_by_text(name, exact=True)
                ).click()
                check.settle()

            choose("Rows scope", "two", "Contig", "two")
            assert bounds() == {
                "rowStartPx": 4000,
                "rowEndPx": 8000,
                "colStartPx": 0,
                "colEndPx": 16000,
            }
            choose("Columns scope", "one", "Contig", "one")
            assert bounds() == {
                "rowStartPx": 4000,
                "rowEndPx": 8000,
                "colStartPx": 0,
                "colEndPx": 4000,
            }
            values = page.evaluate(
                """async () => Array.from((await window.__hictBenchmark.manager().networkManager.requestManager.queryMatrixFloat32({
                bpResolution:100,startRowPx:4000,endRowPx:4002,startColPx:0,endColPx:2,source:'PRIMARY',signalMode:'RAW_COUNTS'})).values)"""
            )
            assert values == [0, 0, 0, 0], values
            page.get_by_role(
                "button", name="Copy row scope to columns", exact=True
            ).click()
            check.settle()
            assert bounds() == {
                "rowStartPx": 4000,
                "rowEndPx": 8000,
                "colStartPx": 4000,
                "colEndPx": 8000,
            }
            values = page.evaluate(
                """async () => Array.from((await window.__hictBenchmark.manager().networkManager.requestManager.queryMatrixFloat32({
                bpResolution:100,startRowPx:4000,endRowPx:4002,startColPx:4000,endColPx:4002,source:'PRIMARY',signalMode:'RAW_COUNTS'})).values)"""
            )
            assert values == [1, 0, 0, 2], values
            before, before_bounds = view(), bounds()
            row = page.get_by_role("textbox", name="Rows scope", exact=True)
            row.fill("missing scope")
            row.press("Enter")
            assert bounds() == before_bounds and view() == before
            row.press("Escape")
            assert row.input_value() == "two"
            for label, query in (
                ("Rows scope", "All Rows"),
                ("Columns scope", "All Columns"),
            ):
                field = page.get_by_role("textbox", name=label, exact=True)
                field.fill(query)
                field.locator("..").locator(".scope-result").filter(
                    has_text=query
                ).click()
                check.settle()
            assert bounds() == {
                "rowStartPx": 0,
                "rowEndPx": 16000,
                "colStartPx": 0,
                "colEndPx": 16000,
            }
            search = page.locator("#global-search-input")
            search.fill("three")
            page.locator(".search-result").filter(
                has=page.get_by_text("Contig", exact=True)
            ).filter(has=page.get_by_text("three", exact=True)).click()
            check.settle()
            assert (
                max(abs(a - b) for a, b in zip(view()["center"], [10000, -10000])) < 1
            )
            search.fill("scf_two")
            page.locator(".search-result").filter(
                has=page.get_by_text("scf_two", exact=True)
            ).wait_for()
            page.get_by_role("button", name="Go to", exact=True).click()
            check.settle()
            assert max(abs(a - b) for a, b in zip(view()["center"], [6000, -6000])) < 1
            before_missing = view()
            search.fill("__absent_contig_scaffold_feature__")
            page.wait_for_timeout(700)
            assert page.locator(".search-result").count() == 0
            page.get_by_role("button", name="Go to", exact=True).click()
            check.settle()
            assert view() == before_missing

        def entity_navigation():
            page.get_by_role("button", name="Entities", exact=True).click()
            for level in (100, 200):
                page.evaluate(
                    "bp=>{const m=window.__hictBenchmark.manager();m.getView().setResolution(m.viewAndLayersManager.getPixelResolutionForBpResolution(bp));m.getView().setCenter([8000,-8000]);}",
                    level,
                )
                check.settle()
                for tab, name, button, expected in (
                    ("contigs", "three", "Go to contig", [10000, -10000]),
                    ("scaffolds", "scf_two", "Go to scaffold", [6000, -6000]),
                ):
                    page.locator(f"label[for={tab}-btn]").click()
                    row = next(
                        row
                        for row in page.locator(".name-row").all()
                        if row.locator("input").input_value() == name
                    )
                    row.get_by_role("button", name=button, exact=True).click()
                    check.settle()
                    assert (
                        max(abs(a - b) for a, b in zip(view()["center"], expected)) < 1
                    ), (level, name, view(), expected)

        def bookmarks():
            expected = view()
            page.get_by_role(
                "button", name="Bookmark current location", exact=True
            ).click()
            toast = page.locator("[data-sonner-toast]").filter(has_text="bookmarked")
            toast.locator("[data-close-button]").click()
            toast.wait_for(state="detached")
            page.get_by_role("button", name="Entities", exact=True).click()
            page.locator("label[for=bookmarks-btn]").click()
            page.get_by_role(
                "textbox", name="Name of the saved location", exact=True
            ).fill("Release landmark")
            page.get_by_role("button", name="Zoom in", exact=True).click()
            check.settle()
            assert view()["resolution"] != expected["resolution"]
            page.locator(
                'button[title="Go to saved location"], button[data-bs-original-title="Go to saved location"]'
            ).last.click()
            check.settle()
            actual = view()
            assert (
                actual["center"] == expected["center"]
                and actual["resolution"] == expected["resolution"]
                and actual["rotation"] == expected["rotation"]
            ), (expected, actual)
            page.locator("#save-button").click()
            go_buttons = page.locator(
                'button[title="Go to saved location"], button[data-bs-original-title="Go to saved location"]'
            )
            assert go_buttons.count() == 2
            page.locator(
                'button[title="Remove saved location"], button[data-bs-original-title="Remove saved location"]'
            ).last.click()
            assert go_buttons.count() == 1
            with page.expect_download() as download:
                page.locator(".bookmark-actions").get_by_role(
                    "button", name="Export", exact=True
                ).click()
            target = output / "bookmarks.json"
            download.value.save_as(target)
            exported = json.loads(target.read_text())
            assert exported and exported[0]["name"] == "Release landmark", exported
            page.locator(
                'button[title="Remove saved location"], button[data-bs-original-title="Remove saved location"]'
            ).last.click()
            assert (
                page.locator(
                    'button[title="Go to saved location"], button[data-bs-original-title="Go to saved location"]'
                ).count()
                == 0
            )
            page.locator(".bookmark-actions input[type=file]").set_input_files(target)
            page.locator(
                'button[title="Go to saved location"], button[data-bs-original-title="Go to saved location"]'
            ).last.wait_for()
            assert (
                page.get_by_role(
                    "textbox", name="Name of the saved location", exact=True
                ).input_value()
                == "Release landmark"
            )
            page.get_by_role("button", name="Visualization", exact=True).click()
            page.get_by_role("button", name="Entities", exact=True).click()
            page.locator("label[for=bookmarks-btn]").click()
            assert (
                page.get_by_role(
                    "textbox", name="Name of the saved location", exact=True
                ).input_value()
                == "Release landmark"
            )
            with page.expect_download() as session_download:
                check.menu("File", "Save session")
            session_path = output / "named-bookmark-session.json"
            session_download.value.save_as(session_path)
            saved = json.loads(session_path.read_text())
            assert saved["client"]["savedLocations"][0]["name"] == "Release landmark"
            check.menu("File", "Close")
            page.get_by_role(
                "heading", name="No contact map open", exact=True
            ).wait_for()
            page.reload()
            page.wait_for_function("window.__hictBenchmark")
            assert (
                page.evaluate(
                    "document.querySelector('#app').__vue_app__.config.globalProperties"
                    ".$pinia._s.get('sessionStore').savedLocations.length"
                )
                == 0
            )
            with page.expect_file_chooser() as chooser:
                check.menu("File", "Open session")
            chooser.value.set_files(session_path)
            page.get_by_text(
                "Session restored. Undo history starts from the restored assembly.",
                exact=True,
            ).wait_for()
            page.get_by_role("button", name="Entities", exact=True).click()
            page.locator("label[for=bookmarks-btn]").click()
            assert (
                page.get_by_role(
                    "textbox", name="Name of the saved location", exact=True
                ).input_value()
                == "Release landmark"
            )
            legacy_path = output / "legacy-bookmarks.json"
            legacy = [
                {key: value for key, value in entry.items() if key != "name"}
                for entry in exported
            ]
            legacy_path.write_text(json.dumps(legacy))
            page.locator(".bookmark-actions input[type=file]").set_input_files(
                legacy_path
            )
            page.wait_for_function(
                "document.querySelector('input[aria-label=\"Name of the saved location\"]').value === '0 unnamed location'"
            )
            page.get_by_role("button", name="Visualization", exact=True).click()

        def presets():
            page.get_by_role("button", name="Manage…", exact=True).click()
            group = page.get_by_role("group", name="Visualization presets", exact=True)
            count = (
                page.get_by_role("combobox", name="Visualization preset", exact=True)
                .locator("option")
                .count()
            )
            group.get_by_title(
                "Save applied visualization and complete rendering graph", exact=True
            ).click()
            page.wait_for_function(
                "count => document.querySelector('select[aria-label=\"Visualization preset\"]').options.length === count + 1",
                arg=count,
            )
            # Bind the saved row through its owner, keeping duplicate action labels scoped.
            rename = page.get_by_role("button", name="Rename preset", exact=True).last
            rename.click()
            name = page.get_by_role(
                "textbox", name="Name of visualization preset", exact=True
            )
            name.fill("Release verification preset")
            name.press("Enter")
            assert (
                page.get_by_role("combobox", name="Visualization preset", exact=True)
                .locator("option")
                .count()
                == count + 1
            )
            page.get_by_role(
                "button", name="Release verification preset", exact=True
            ).wait_for()
            rename.click()
            name.fill("Cancelled rename")
            name.press("Escape")
            page.get_by_role(
                "button", name="Release verification preset", exact=True
            ).wait_for()
            with page.expect_download() as download:
                group.get_by_role("button", name="Export", exact=True).click()
            target = output / "presets.json"
            download.value.save_as(target)
            assert "Release verification preset" in target.read_text()
            page.get_by_role(
                "button", name="Remove visualization preset", exact=True
            ).last.click()
            assert (
                page.get_by_role("combobox", name="Visualization preset", exact=True)
                .locator("option")
                .count()
                == count
            )
            with page.expect_file_chooser() as chooser:
                group.get_by_role("button", name="Import", exact=True).click()
            chooser.value.set_files(target)
            page.get_by_role(
                "button", name="Release verification preset", exact=True
            ).wait_for()
            before = page.evaluate(
                "async () => await window.__hictBenchmark.manager().networkManager.requestManager.getRenderPipelineConfig()"
            )
            page.get_by_role(
                "combobox", name="Visualization preset", exact=True
            ).select_option(label="Release verification preset")
            check.settle()
            after = page.evaluate(
                "async () => await window.__hictBenchmark.manager().networkManager.requestManager.getRenderPipelineConfig()"
            )
            assert (
                before["upperExpression"] == after["upperExpression"]
                and before["lowerExpression"] == after["lowerExpression"]
            ), (before, after)

            labels_before = (
                page.get_by_role("combobox", name="Visualization preset", exact=True)
                .locator("option")
                .all_text_contents()
            )
            candidate = json.loads(target.read_text())["data"][
                "savedVisualizationPresets"
            ][-1]
            candidate["option_id"] = 1000000
            candidate["name"] = "Rejected partial preset"
            candidate["signalThresholds"] = {
                "lowerSignalBound": 11,
                "upperSignalBound": 13,
            }
            thresholds_before = page.evaluate(
                "() => {const c=document.querySelector('#app').__vue_app__.config.globalProperties.$pinia._s.get('visualizationOptions').colormap;return [c.minSignal,c.maxSignal];}"
            )
            malformed = output / "malformed-presets.json"
            malformed.write_text(
                json.dumps(
                    {
                        "exportType": "visualizationOptions",
                        "data": {"savedVisualizationPresets": [candidate, None]},
                    }
                )
            )
            with page.expect_file_chooser() as chooser:
                group.get_by_role("button", name="Import", exact=True).click()
            chooser.value.set_files(malformed)
            page.locator("[data-sonner-toast]").filter(
                has_text="Cannot import visualization options"
            ).last.wait_for()
            assert (
                page.get_by_role("combobox", name="Visualization preset", exact=True)
                .locator("option")
                .all_text_contents()
                == labels_before
            )
            assert (
                page.evaluate(
                    "async()=>await window.__hictBenchmark.manager().networkManager.requestManager.getRenderPipelineConfig()"
                )
                == after
            )

            assert (
                page.evaluate(
                    "() => {const c=document.querySelector('#app').__vue_app__.config.globalProperties.$pinia._s.get('visualizationOptions').colormap;return [c.minSignal,c.maxSignal];}"
                )
                == thresholds_before
            )
            originals = json.loads(target.read_text())["data"][
                "savedVisualizationPresets"
            ]
            accepted = [
                dict(originals[-1], name="Atomic import one"),
                dict(originals[-1], name="Atomic import two"),
            ]
            valid_batch = output / "two-valid-presets.json"
            valid_batch.write_text(
                json.dumps(
                    {
                        "exportType": "visualizationOptions",
                        "data": {"savedVisualizationPresets": accepted},
                    }
                )
            )
            with page.expect_file_chooser() as chooser:
                group.get_by_role("button", name="Import", exact=True).click()
            chooser.value.set_files(valid_batch)
            for label in ("Atomic import one", "Atomic import two"):
                page.get_by_role("button", name=label, exact=True).wait_for()
            select = page.get_by_role(
                "combobox", name="Visualization preset", exact=True
            )
            assert select.locator("option").count() == len(labels_before) + 2
            ids = select.locator("option").evaluate_all(
                "nodes=>nodes.map(n=>n.value).filter(Boolean).map(Number)"
            )
            assert len(set(ids)) == len(ids)
            legacy_id = max(ids) + 1
            legacy_candidate = dict(originals[-1])
            legacy_candidate.pop("name", None)
            legacy_file = output / "legacy-unnamed-presets.json"
            legacy_file.write_text(
                json.dumps(
                    {
                        "exportType": "visualizationOptions",
                        "data": {"savedLocations": [legacy_candidate]},
                    }
                )
            )
            with page.expect_file_chooser() as chooser:
                group.get_by_role("button", name="Import", exact=True).click()
            chooser.value.set_files(legacy_file)
            page.get_by_role(
                "button", name=f"Imported preset {legacy_id}", exact=True
            ).wait_for()
            assert select.locator("option").count() == len(labels_before) + 3

        def osd():
            check.menu("View", "Info settings…")
            dialog = page.get_by_role(
                "dialog", name="On-screen display settings", exact=True
            )
            dialog.get_by_label("Show overlay", exact=True).check()
            fields = dialog.locator('input[id^="osd-field-"]').evaluate_all(
                "(inputs) => inputs.map(input=>input.id)"
            )
            for field in fields:
                dialog.locator("#" + field).check()
            dialog.get_by_role("button", name="Done", exact=True).click()
            viewport = page.evaluate(
                "() => window.__hictBenchmark.manager().getMap().getViewport().getBoundingClientRect().toJSON()"
            )

            def hover():
                page.mouse.move(
                    viewport["x"] + viewport["width"] * 0.45,
                    viewport["y"] + viewport["height"] * 0.45,
                )
                page.wait_for_timeout(150)

            hover()
            overlay = page.locator(".ol-mouse-position")
            baseline = overlay.inner_text().splitlines()
            assert len(baseline) == len(fields), (baseline, fields)
            for index, field in enumerate(fields):
                check.menu("View", "Info settings…")
                dialog.locator("#" + field).uncheck()
                dialog.get_by_role("button", name="Done", exact=True).click()
                hover()
                actual = overlay.inner_text().splitlines()
                assert actual == baseline[:index] + baseline[index + 1 :], (
                    field,
                    baseline,
                    actual,
                )
                check.menu("View", "Info settings…")
                dialog.locator("#" + field).check()
                dialog.get_by_role("button", name="Done", exact=True).click()
                hover()
            check.menu("View", "Info settings…")
            dialog.locator(".osd-field-row").first.get_by_role(
                "button", name="Down", exact=True
            ).click()
            dialog.get_by_label("Position", exact=True).select_option("bottom-left")
            dialog.get_by_role("button", name="Done", exact=True).click()
            hover()
            assert overlay.inner_text().splitlines() == [
                baseline[1],
                baseline[0],
                *baseline[2:],
            ]
            rect = overlay.locator("div").first.bounding_box()
            assert abs(rect["x"] - viewport["x"] - 12) < 2
            assert (
                abs(
                    rect["y"] + rect["height"] - viewport["y"] - viewport["height"] + 12
                )
                < 2
            )
            page.locator("#toggle-osd-button").click()
            hover()
            assert overlay.inner_text() == ""
            page.locator("#toggle-osd-button").click()
            hover()
            assert len(overlay.inner_text().splitlines()) == len(fields)
            page.wait_for_timeout(300)
            assert (
                json.loads(
                    page.evaluate('localStorage.getItem("hict.ui.osd.position")')
                )
                == "bottom-left"
            )

        def notifications():
            graph = page.evaluate(
                "async () => await window.__hictBenchmark.manager().networkManager.requestManager.getRenderPipelineConfig()"
            )
            page.get_by_role(
                "combobox", name="Visualization preset", exact=True
            ).select_option(label="ModDotPlot: multicolor overlay")
            page.wait_for_function(
                "() => document.querySelector('#app').__vue_app__.config.globalProperties.$pinia._s.get('notificationCenter').entries.length > 0"
            )
            assert (
                page.evaluate(
                    "async () => await window.__hictBenchmark.manager().networkManager.requestManager.getRenderPipelineConfig()"
                )
                == graph
            )
            before = page.evaluate(
                "async () => (await window.__hictBenchmark.manager().networkManager.requestManager.getSessionSnapshot(true)).primary.agpSha256"
            )
            page.get_by_role("button", name="Notifications", exact=False).first.click()
            dialog = page.get_by_role("dialog").filter(
                has=page.get_by_role("heading", name="Notifications", exact=True)
            )
            dialog.locator(".notification-entry").first.wait_for(state="visible")
            assert "Attach a generated overlay" in dialog.inner_text()
            dialog.get_by_role("button", name="Clear", exact=True).click()
            dialog.locator(".notification-entry").wait_for(state="detached")
            dialog.locator(".btn-close").click()
            after = page.evaluate(
                "async () => (await window.__hictBenchmark.manager().networkManager.requestManager.getSessionSnapshot(true)).primary.agpSha256"
            )
            assert before == after

        try:
            page.goto(args.url)
            page.wait_for_function("window.__hictBenchmark")
            page.evaluate(
                'async api => {await window.__hictBenchmark.open("structural.hict.hdf5",api);}',
                args.api,
            )
            check.settle()
            check.step("diagonal-navigation-and-rotation-reset", navigate)
            check.step("border-panel-and-each-caption-side-export", caption_sides)
            check.step(
                "independent-row-column-scopes-and-genomic-search", scopes_and_search
            )
            check.step(
                "sidebar-entity-navigation-at-two-native-levels", entity_navigation
            )
            check.step("bookmark-save-go-export-remove-import", bookmarks)
            check.step("preset-save-rename-cancel-export-remove-import-apply", presets)
            check.step("each-osd-field-order-position-toggle-persistence", osd)
            check.step("notification-history-clear-preserves-assembly", notifications)
        finally:
            (output / "results.json").write_text(
                json.dumps({"checks": check.rows, "errors": check.errors}, indent=2)
                + "\n"
            )
            browser.close()


if __name__ == "__main__":
    main()
