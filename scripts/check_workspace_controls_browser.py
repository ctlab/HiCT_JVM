#!/usr/bin/env python3
"""Check navigation, bookmarks, presets, OSD fields and notification outcomes."""

import argparse
import json
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

        def bookmarks():
            page.get_by_role("button", name="Entities", exact=True).click()
            page.locator("label[for=bookmarks-btn]").click()
            expected = view()
            page.get_by_title("Save current viewport", exact=True).click()
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
            assert page.evaluate(
                "document.querySelector('#app').__vue_app__.config.globalProperties"
                ".$pinia._s.get('sessionStore').savedLocations.length"
            ) == 0
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
            group.get_by_title("Save current visualization options", exact=True).click()
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

        def osd():
            check.menu("View", "OSD settings...")
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
                check.menu("View", "OSD settings...")
                dialog.locator("#" + field).uncheck()
                dialog.get_by_role("button", name="Done", exact=True).click()
                hover()
                actual = overlay.inner_text().splitlines()
                assert actual == baseline[:index] + baseline[index + 1 :], (
                    field,
                    baseline,
                    actual,
                )
                check.menu("View", "OSD settings...")
                dialog.locator("#" + field).check()
                dialog.get_by_role("button", name="Done", exact=True).click()
                hover()
            check.menu("View", "OSD settings...")
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
