#!/usr/bin/env python3
"""Check desktop layouts and region analysis defaults at the supported projector sizes."""

import argparse
import json
from pathlib import Path

from playwright.sync_api import expect, sync_playwright
from sequence_browser_harness import BrowserCheck


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

    def set_region(row, column, length):
        return page.evaluate("""([row, column, length]) => {
            const manager = window.__hictBenchmark.manager();
            const layers = manager.viewAndLayersManager;
            const bounds = {rowStart:row, rowEnd:row+length, columnStart:column, columnEnd:column+length};
            layers.setRegionBounds(bounds);
            const grid = layers.currentViewState.resolutionDesciptor;
            const project = bp => manager.contigDimensionHolder.getFractionalPxOfBp(bp, grid.bpResolution)
                * grid.pixelResolution;
            const map = manager.getMap();
            const middle = map.getPixelFromCoordinate([project(column + length/2), -project(row + length/2)]);
            const viewport = map.getViewport().getBoundingClientRect();
            return [viewport.left+middle[0], viewport.top+middle[1]];
        }""", [row, column, length])

    def complete_region_label():
        set_region(0, 0, 364000)
        label = ribbon.locator(".selection-label span")
        expect(label).to_have_text("Region · 364 kb × 364 kb")
        assert label.evaluate("node => node.scrollWidth <= node.clientWidth+1")
        assert ribbon.evaluate("node => node.getBoundingClientRect().height <= 56")
        assert page.evaluate("document.documentElement.scrollWidth <= innerWidth+1")

    check.step("complete-region-label-on-one-line", complete_region_label)

    def selection_analysis():
        position = set_region(100000, 100000, 80000)
        page.mouse.click(*position, button="right")
        menu = page.get_by_role("menu", name="Assembly actions")
        analyse = menu.get_by_role("menuitem", name="Analyse", exact=True)
        expect(analyse).to_be_visible()
        geometry = menu.evaluate("""menu => {
            const bounds = menu.getBoundingClientRect();
            const groups = [...menu.querySelectorAll(':scope > section')];
            const rows = group => group.querySelectorAll(':scope > [role=menuitem]').length;
            return {
                fits: bounds.left >= 0 && bounds.right <= innerWidth+1 && bounds.top >= 0
                    && bounds.bottom <= innerHeight+1 && menu.scrollHeight <= menu.clientHeight+1,
                rows: groups.reduce((count, group) => count+rows(group), 0),
                regionRows: rows(menu.querySelector('[data-submenu=analyse]').parentElement),
            };
        }""")
        (check.output / "context-menu.json").write_text(json.dumps(geometry, indent=2))
        assert geometry["fits"] and geometry["rows"] <= 11 and geometry["regionRows"] <= 6, geometry
        assert not menu.get_by_role("menuitem", name="Zoom to scaffold").count()
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
            expect(dialog).to_be_visible()
            assert_dialog_fits(dialog)
            page.keyboard.press("Escape")
            expect(dialog).to_be_hidden()

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
        for width, height in ((1280, 720), (1440, 900)):
            output = Path(args.output) / f"{width}x{height}"
            output.mkdir(parents=True)
            page = browser.new_page(viewport={"width": width, "height": height})
            page.add_init_script("window.__HICT_BENCHMARK_ENABLED__ = true")
            page.route("http://localhost:5000/**", lambda route: route.continue_(
                url=args.api + route.request.url.removeprefix("http://localhost:5000")))
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
