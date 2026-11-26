#!/usr/bin/env python3
"""Check desktop layouts and region analysis defaults at the supported projector sizes."""

import argparse
import json
from pathlib import Path

from playwright.sync_api import expect, sync_playwright
from sequence_browser_harness import BrowserCheck, configure_test_api


def check_layout(page, check, args):
    ribbon = page.locator(".header-ribbon")
    toolbar = page.locator("#workspace-map-tools")
    expect(page.get_by_role("heading", name="No contact map open")).to_be_visible()
    disabled_controls = page.locator(
        ".header-ribbon :is(button,input,select):visible, #workspace-map-tools button:visible"
    ).evaluate_all("""controls => controls.map(control => ({
        label: control.getAttribute('aria-label') || control.textContent.trim() || control.id,
        disabled: control.matches(':disabled, [aria-disabled="true"]'),
        reason: control.closest('[title]')?.title
    }))""")
    assert disabled_controls and all(
        control["disabled"] and control["reason"] for control in disabled_controls
    ), disabled_controls
    page.evaluate(
        "async ({file,api,fasta}) => window.__hictBenchmark.open(file,api,fasta)",
        {"file": args.file, "api": args.api, "fasta": args.fasta},
    )
    check.settle()

    def toolbar_hit_test():
        buttons = toolbar.get_by_role("button")
        assert buttons.count() > 5
        for button in buttons.all():
            assert button.evaluate("""button => {
                const rect = button.getBoundingClientRect();
                const viewport = button.closest('#workspace-map-tools').getBoundingClientRect();
                if (rect.top < viewport.top-1 || rect.bottom > viewport.bottom+1) return false;
                if (button.disabled) return true;
                const target = document.elementFromPoint(rect.x + rect.width/2, rect.y + rect.height/2);
                return button === target || button.contains(target);
            }"""), button.get_attribute("aria-label") or button.text_content()

    check.step("toolbar-buttons-can-be-reached", toolbar_hit_test)

    def normalization_field():
        select = page.get_by_role("combobox", name="Normalization", exact=True)
        expect(select).to_be_in_viewport(ratio=1)
        assert select.evaluate("""select => {
            const label = select.labels[0].getBoundingClientRect();
            const field = select.parentElement.getBoundingClientRect();
            const control = select.getBoundingClientRect();
            return control.top >= label.bottom
                && control.top - label.bottom <= 4
                && Math.abs(control.left - label.left) <= 1
                && Math.abs(control.width - field.width) <= 1;
        }""")
        expect(page.locator("#gradient-apply-button")).to_be_in_viewport(ratio=1)

    check.step("normalization-label-above-full-width-field", normalization_field)

    def set_region(row, column, length):
        page.evaluate("""([row, column, length]) => {
            const manager = window.__hictBenchmark.manager();
            const bounds = {rowStart:row, rowEnd:row+length, columnStart:column, columnEnd:column+length};
            manager.viewAndLayersManager.setRegionBounds(bounds);
        }""", [row, column, length])
        return map_point(row + length/2, column + length/2)

    def map_point(row, column):
        return page.evaluate("""([row, column]) => {
            const manager = window.__hictBenchmark.manager();
            const grid = manager.viewAndLayersManager.currentViewState.resolutionDesciptor;
            const project = bp => manager.contigDimensionHolder.getFractionalPxOfBp(bp, grid.bpResolution)
                * grid.pixelResolution;
            const map = manager.getMap();
            const middle = map.getPixelFromCoordinate([project(column), -project(row)]);
            const viewport = map.getViewport().getBoundingClientRect();
            return [viewport.left+middle[0], viewport.top+middle[1]];
        }""", [row, column])

    def complete_region_label():
        set_region(0, 0, 364000)
        label = ribbon.locator(".selection-label span")
        expect(label).to_have_text("Region · 364 kb × 364 kb")
        assert label.evaluate("node => node.scrollWidth <= node.clientWidth+1")
        assert ribbon.evaluate("node => node.getBoundingClientRect().height <= 56")
        assert page.evaluate("document.documentElement.scrollWidth <= innerWidth+1")

    check.step("complete-region-label-on-one-line", complete_region_label)

    ribbon_positions = {}

    def measure_ribbon(state):
        selection = ribbon.locator(".selection-label")
        assert selection.get_attribute("aria-label") == selection.get_attribute("title")
        geometry = ribbon.evaluate("""ribbon => {
            const left = ribbon.querySelector('#left-header-block');
            const right = ribbon.querySelector('#right-header-block');
            const controls = ['#global-search-input', '[aria-label="Rows scope"]',
              '[aria-label="Columns scope"]', '[aria-label="Signal display"]', '.selection-label']
              .map(selector => ribbon.querySelector(selector).getBoundingClientRect());
            return {
                ordered: controls.every((box, index) => index === 0 || controls[index-1].right <= box.left),
                fits: left.lastElementChild.matches('.selection-tools')
                  && left.lastElementChild.getBoundingClientRect().right <= right.getBoundingClientRect().left
                  && ribbon.getBoundingClientRect().height <= 56,
                width: controls[4].width,
                requiredWidth: 13 * parseFloat(getComputedStyle(document.documentElement).fontSize),
                x: controls.map(box => box.left),
            };
        }""")
        assert geometry["ordered"] and geometry["fits"], geometry
        assert geometry["width"] == geometry["requiredWidth"], geometry
        ribbon_positions[state] = geometry["x"]
        assert geometry["x"] == ribbon_positions["none"], ribbon_positions

    def ribbon_selection_states():
        page.evaluate("""() => window.__hictBenchmark.manager().viewAndLayersManager.clearSelections()""")
        measure_ribbon("none")
        page.keyboard.down("Shift")
        page.mouse.click(*map_point(100000, 100000))
        page.keyboard.up("Shift")
        expect(ribbon.locator(".selection-label span")).to_have_text("Selection · 1 contig")
        measure_ribbon("one-contig")
        page.keyboard.down("Control")
        page.mouse.move(*map_point(100000, 100000))
        page.mouse.down()
        page.mouse.move(*map_point(400000, 350000), steps=6)
        page.mouse.up()
        page.keyboard.up("Control")
        expect(ribbon.locator(".selection-label span")).to_contain_text("Region ·")
        measure_ribbon("region")

    check.step("ribbon-clear-contig-and-region-positions", ribbon_selection_states)

    def menu_geometry(menu):
        geometry = menu.evaluate("""menu => {
            const bounds = menu.getBoundingClientRect();
            const rows = menu.querySelectorAll(':scope > section:not(.child-menu) > [role=menuitem]');
            return {
                fits: bounds.left >= 7 && bounds.right <= innerWidth-7 && bounds.top >= 7
                    && bounds.bottom <= innerHeight-7 && menu.scrollHeight <= menu.clientHeight+1,
                width: bounds.width,
                rows: rows.length,
                rowHeights: [...rows].map(row => row.getBoundingClientRect().height),
                labels: [...rows].map(row => row.getAttribute('aria-label') ??
                    row.innerText.trim().replace(/\\s*›$/, "")),
            };
        }""")
        assert geometry["fits"] and geometry["rows"] <= 9, geometry
        return geometry

    def selection_analysis():
        position = set_region(100000, 100000, 80000)
        page.mouse.click(*position, button="right")
        menu = page.get_by_role("menu", name="Assembly actions")
        analyse = menu.get_by_role("menuitem", name="Analyse", exact=True)
        expect(analyse).to_be_visible()
        geometry = menu_geometry(menu)
        (check.output / "context-menu.json").write_text(json.dumps(geometry, indent=2))
        assert geometry["labels"][:6] == [
            "Zoom to region", "Isolate view", "Edit", "Copy", "Export", "Analyse",
        ], geometry
        assert not menu.locator('[data-submenu="scaffold"]').count()
        expect(menu.get_by_role("menuitem").last).to_have_text("Move to unplaced (debris)…")
        copy = menu.get_by_role("menuitem", name="Copy", exact=True)
        copy.hover()
        expect(page.get_by_role("menu", name="Copy").get_by_role(
            "menuitem", name="Contig name", exact=True,
        )).to_be_visible()
        assert analyse.evaluate("""row => {
            const box = row.getBoundingClientRect();
            return row.contains(document.elementFromPoint(box.x+box.width/2,box.y+box.height/2));
        }"""), geometry
        analyse.hover()
        submenu = page.get_by_role("menu", name="Analyse")
        expect(menu).to_be_visible()
        expect(submenu).to_be_visible()
        assert submenu.evaluate("""menu => {
            const bounds = menu.getBoundingClientRect();
            return bounds.left >= 0 && bounds.right <= innerWidth+1
                && bounds.top >= 0 && bounds.bottom <= innerHeight+1;
        }""")
        submenu.get_by_role("menuitem", name="ModDotPlot of region…").click()
        dialog = page.get_by_role("dialog", name="ModDotPlot", exact=True)
        expect(dialog.get_by_role("button", name="Generate", exact=True)).to_be_enabled()
        resolutions = dialog.locator(".resolution-options input:checked").evaluate_all(
            "inputs => inputs.map(input => Number(input.value))"
        )
        assert resolutions == [21, 25, 50, 100, 200], resolutions
        assert_dialog_fits(dialog)
        page.keyboard.press("Escape")
        expect(dialog).to_be_hidden()

    check.step("bounded-region-menu-and-moddotplot-defaults", selection_analysis)

    def multi_entity_context():
        manager = "window.__hictBenchmark.manager()"
        for start, end in ((0, 800000), (800000, 1600000)):
            page.evaluate(
                f"([start,end]) => {manager}.viewAndLayersManager.selectEntityRangeByBp(start,end,{{expand:false}})",
                [start, end],
            )
            with page.expect_response("**/group_contigs_into_scaffold") as grouped:
                page.get_by_role("button", name="Group selection into scaffold", exact=True).click()
            assert grouped.value.ok
            check.settle()
            if start == 0:
                page.keyboard.down("Shift")
                page.mouse.click(*map_point(100000, 100000))
                page.keyboard.up("Shift")
                expect(ribbon.locator(".selection-label span")).to_have_text("Selection · 2 contigs in 1 scaffold")
                measure_ribbon("one-scaffold")
                (check.output / "ribbon-positions.json").write_text(json.dumps(ribbon_positions, indent=2))
                page.screenshot(path=str(check.output / "ribbon-selection.png"))
                ribbon.locator(".selection-label").click()
                page.screenshot(path=str(check.output / "ribbon-selection-dropdown.png"))
                page.keyboard.press("Escape")
        page.mouse.click(*set_region(100000, 100000, 80000), button="right")
        menu = page.get_by_role("menu", name="Assembly actions")
        diagonal = menu_geometry(menu)
        assert diagonal["rows"] == 9
        assert abs(diagonal["width"] - 300) <= 4, diagonal
        assert all(abs(height - 34.25) <= 4 for height in diagonal["rowHeights"]), diagonal
        assert diagonal["labels"][6] == "Contig · one", diagonal
        assert diagonal["labels"][7].startswith("Scaffold · "), diagonal
        page.screenshot(path=str(check.output / "single-entity-region-menu.png"))
        page.keyboard.press("Escape")
        point = set_region(100000, 900000, 80000)
        page.mouse.click(*point, button="right")
        geometry = menu_geometry(menu)
        assert geometry["rows"] == 9
        assert geometry["labels"][-3:] == ["Contigs", "Scaffolds", "Move to unplaced (debris)…"], geometry
        for group, selector in (("Contigs", "contig"), ("Scaffolds", "scaffold")):
            trigger = menu.locator(f'[data-submenu="{selector}"]')
            trigger.focus()
            page.keyboard.press("ArrowRight")
            child = menu.get_by_role("menu", name=group, exact=True)
            expect(child).to_be_visible()
            assert child.locator(".entity-title").count() == 2
            expect(child.get_by_role("menuitem").first).to_be_focused()
            assert_dialog_fits(child)
            page.screenshot(path=str(check.output / f"{selector}-submenu.png"))
            page.keyboard.press("ArrowLeft")
            expect(child).to_be_hidden()
            expect(trigger).to_be_focused()
        danger = menu.get_by_role("menuitem", name="Move to unplaced (debris)…", exact=True)
        danger.hover()
        expect(menu.get_by_role("menu", name="Move to unplaced (debris)…", exact=True)
               .get_by_role("menuitem")).to_have_count(2)
        menu.get_by_role("menuitem", name="Isolate view", exact=True).click()
        check.settle()
        page.mouse.click(*map_point(140000, 940000), button="right")
        menu.get_by_role("menuitem", name="Show whole map", exact=True).click()
        check.settle()
        assert page.evaluate(f"""() => Object.values({manager}.viewAndLayersManager.getAxisScopes())
            .every(scope => scope.kind === 'all')""")
        page.mouse.click(*map_point(140000, 140000), button="right")
        without_region = menu_geometry(menu)
        menu.locator('[data-submenu="contig"]').hover()
        expect(menu.locator(".child-menu").get_by_role(
            "menuitem", name="Zoom to contig", exact=True
        )).to_be_visible()
        menu.locator('[data-submenu="scaffold"]').hover()
        expect(menu.locator(".child-menu").get_by_role(
            "menuitem", name="Ungroup into singletons", exact=True
        )).to_be_visible()
        page.keyboard.press("Escape")
        assert without_region["rows"] < geometry["rows"]
        (check.output / "multi-entity-context-menu.json").write_text(json.dumps(geometry, indent=2))

    check.step("multi-entity-submenus-and-restore-whole-map", multi_entity_context)

    def dialogs_fit():
        for menu, action in (
            ("File", "Open with options…"), ("File", "Convert matrices…"),
            ("File", "Export matrix…"), ("View", "1D tracks…"),
            ("View", "Sources and layers…"), ("View", "Info settings…"),
            ("View", "Rendering pipeline…"), ("Tools", "Dotplot from FASTA files…"),
            ("Tools", "Find sequences in linked FASTA…"),
            ("Help", "User guide and shortcuts…"), ("Help", "About HiCT…"),
        ):
            check.menu(menu, action)
            dialog = page.get_by_role("dialog").last
            try:
                expect(dialog).to_be_visible()
                assert_dialog_fits(dialog)
                if action in {
                    "Open with options…", "Convert matrices…",
                    "Export matrix…", "Dotplot from FASTA files…",
                }:
                    footer = dialog.locator(".hict-modal-footer")
                    cancel = footer.get_by_role("button", name="Cancel", exact=True)
                    expect(cancel).to_have_class("btn btn-outline-secondary")
                    primary = footer.locator(".btn-primary").last.bounding_box()
                    cancellation = cancel.bounding_box()
                    assert primary["x"] > cancellation["x"]
                    link = footer.locator(".btn-link")
                    if link.count():
                        assert link.bounding_box()["x"] < cancellation["x"]
                page.keyboard.press("Escape")
                expect(dialog).to_be_hidden()
            except AssertionError as failure:
                raise AssertionError(f"{menu} → {action}: {failure}") from failure

    check.step("dialogs-and-footers-fit", dialogs_fit)


def assert_dialog_fits(dialog):
    assert dialog.evaluate("""dialog => {
        const shell = dialog.querySelector('.modal-dialog') || dialog;
        return [shell, ...dialog.querySelectorAll('.modal-footer')].every(node => {
            const bounds = node.getBoundingClientRect();
            return bounds.left >= -1 && bounds.right <= innerWidth+1
                && bounds.top >= -1 && bounds.bottom <= innerHeight+1;
        });
    }"""), dialog.get_attribute("aria-label") or dialog.locator(".modal-title").all_text_contents()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ("api", "url", "file", "fasta", "output"):
        parser.add_argument("--" + key, required=True)
    args = parser.parse_args()
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        for width, height in ((1280, 720), (1366, 768), (1440, 900), (1920, 1080)):
            output = Path(args.output) / f"{width}x{height}"
            output.mkdir(parents=True)
            page = browser.new_page(viewport={"width": width, "height": height})
            page.add_init_script("window.__HICT_BENCHMARK_ENABLED__ = true")
            configure_test_api(page, args.api)
            check = BrowserCheck(page, output)
            try:
                assert page.request.post(args.api + "/close", data={}).ok
                page.goto(args.url)
                check_layout(page, check, args)
            finally:
                (output / "layout.json").write_text(json.dumps({
                    "checks": check.rows, "errors": check.errors,
                }, indent=2))
                page.close()
        browser.close()


if __name__ == "__main__":
    main()
