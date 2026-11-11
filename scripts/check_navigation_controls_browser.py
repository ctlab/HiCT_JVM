#!/usr/bin/env python3
"""Verify menu keyboard navigation, reload, sidebar and resolution restrictions."""

import argparse
import json
from pathlib import Path

from playwright.sync_api import expect, sync_playwright
from sequence_browser_harness import BrowserCheck


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("url", "api", "output"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--file", default="combined_ind2_4DN.hict.hdf5")
    args = parser.parse_args()
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    results = []
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

        def enabled():
            return page.evaluate(
                "window.__hictBenchmark.manager().getLayersManager().getEnabledBpResolutions()"
            )

        try:
            page.goto(args.url)
            page.wait_for_function("window.__hictBenchmark")
            page.evaluate(
                "async a=>{await window.__hictBenchmark.open(a.file,a.api)}", vars(args)
            )
            check.settle()
            manager = "window.__hictBenchmark.manager()"
            before = view()
            graph = page.evaluate(
                f"async()=>await {manager}.networkManager.requestManager.getRenderPipelineConfig()"
            )
            menus = page.locator('nav a[data-bs-toggle="dropdown"]')
            assert menus.count() == 6
            for menu in menus.all():
                menu.focus()
                menu.press("ArrowDown")
                expect(menu).to_have_attribute("aria-expanded", "true")
                page.keyboard.press("Escape")
                expect(menu).to_have_attribute("aria-expanded", "false")
                expect(menu).to_be_focused()
            assert view() == before
            results.append(
                {"test": "Menu keyboard opening and Escape restore focus and view"}
            )
            page.locator("#reload-tiles-button").click()
            check.settle()
            assert view() == before
            assert (
                page.evaluate(
                    f"async()=>await {manager}.networkManager.requestManager.getRenderPipelineConfig()"
                )
                == graph
            )
            results.append({"test": "Reload preserves exact graph and viewport"})
            page.get_by_role("button", name="Hide side panel", exact=True).click()
            expect(page.locator("aside.sidebar")).to_have_class(
                "sidebar sidebar-collapsed"
            )
            page.get_by_role("button", name="Show side panel", exact=True).click()
            check.settle()
            assert view() == before
            results.append({"test": "Sidebar collapse and restore preserve viewport"})
            check.menu("View", "Restrict Resolutions...")
            dialog = page.get_by_role("dialog").filter(
                has=page.get_by_role("heading", name="Restrict Resolutions", exact=True)
            )
            switches = dialog.get_by_role("switch")
            levels = [
                int(switch.get_attribute("id").removeprefix("resolution-modal-"))
                for switch in switches.all()
            ]
            assert len(levels) >= 2
            finest, coarsest = min(levels), max(levels)
            for level in sorted(levels)[1:]:
                dialog.locator(f"#resolution-modal-{level}").uncheck()
                assert level not in enabled()
                assert finest in enabled()
            assert enabled() == [finest]
            expect(dialog.locator(f"#resolution-modal-{finest}")).to_be_disabled()
            assert enabled() == [finest]
            expect(dialog.locator(f"#resolution-modal-{finest}")).to_be_checked()
            dialog.locator("#resolution-finest-limit").fill(str(finest))
            dialog.locator("#resolution-coarsest-limit").fill(str(coarsest))
            dialog.get_by_role("button", name="Set limits", exact=True).click()
            assert enabled() == sorted(levels)
            dialog.locator("#resolution-finest-limit").fill(str(coarsest))
            dialog.locator("#resolution-coarsest-limit").fill(str(finest))
            dialog.get_by_role("button", name="Set limits", exact=True).click()
            assert enabled() == sorted(levels)
            dialog.locator("#resolution-finest-limit").fill(str(coarsest))
            dialog.locator("#resolution-coarsest-limit").fill(str(coarsest))
            dialog.get_by_role("button", name="Set limits", exact=True).click()
            assert enabled() == [coarsest]
            dialog.get_by_role("button", name="Done", exact=True).click()
            check.settle()
            assert (
                page.evaluate(
                    f"{manager}.getLayersManager().currentViewState.resolutionDesciptor.bpResolution"
                )
                == coarsest
            )
            check.menu("View", "Restrict Resolutions...")
            dialog.locator("#resolution-finest-limit").fill(str(finest))
            dialog.locator("#resolution-coarsest-limit").fill(str(coarsest))
            dialog.get_by_role("button", name="Set limits", exact=True).click()
            dialog.get_by_role("button", name="Done", exact=True).click()
            check.settle()
            results.append(
                {
                    "test": "Each resolution switch, last-level guard, limits and reversed-limit rejection",
                    "levels": levels,
                }
            )
            assert not check.errors, check.errors
        finally:
            page.screenshot(path=str(output / "last-state.png"))
            (output / "results.json").write_text(
                json.dumps({"checks": results, "errors": check.errors}, indent=2) + "\n"
            )
            browser.close()
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
