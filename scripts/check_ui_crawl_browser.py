#!/usr/bin/env python3
"""Check assembly safeguards, region tools, scrollable tracks, and core workspace surfaces."""
import argparse
import json
from pathlib import Path

from playwright.sync_api import expect, sync_playwright
from sequence_browser_harness import BrowserCheck


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ("api", "url", "file", "output"):
        parser.add_argument("--" + key, required=True)
    parser.add_argument("--track", help="A feature track with overlapping features")
    parser.add_argument("--width", type=int, default=1440)
    parser.add_argument("--height", type=int, default=900)
    parser.add_argument("--dpr", type=float, default=1)
    args = parser.parse_args()
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": args.width, "height": args.height},
                                device_scale_factor=args.dpr)
        page.set_default_timeout(30_000)
        page.add_init_script("window.__HICT_BENCHMARK_ENABLED__ = true")
        page.route("http://localhost:5000/**", lambda route: route.continue_(
            url=args.api + route.request.url.removeprefix("http://localhost:5000")))
        check = BrowserCheck(page, output)
        try:
            assert page.request.post(args.api + "/close", data={}).ok
            page.goto(args.url)
            expect(page.get_by_role("heading", name="No contact map open")).to_be_visible()
            for name in ("Undo", "Redo", "Map Info"):
                expect(page.get_by_role("button", name=name, exact=True)).to_be_disabled()
            expect(page.get_by_label("Square selection", exact=True)).to_be_disabled()
            expect(page.get_by_label("Signal display")).to_be_disabled()
            page.screenshot(path=str(output / "no-map.png"))

            with page.expect_response("**/session/availability") as availability:
                check.menu("File", "Join running session")
            assert availability.value.json() == {"available": False}
            assert not check.errors, check.errors

            page.keyboard.press("Control+o")
            expect(page.get_by_role("heading", name="Open Hi-C dataset")).to_be_visible()
            check.choose_file(args.file)
            coverage_requests = []

            def observe_coverage_status(request):
                if request.url.endswith("/normalization/coverage/status"):
                    coverage_requests.append(request.url)

            page.on("request", observe_coverage_status)
            page.get_by_role("button", name="Load", exact=True).click()
            page.wait_for_function("window.__hictBenchmark?.manager()")
            check.settle()
            expect(page.get_by_role("button", name="Undo", exact=True)).to_be_disabled()

            def idle_coverage_status():
                page.wait_for_timeout(10_000)
                page.remove_listener("request", observe_coverage_status)
                assert 1 <= len(coverage_requests) <= 3, len(coverage_requests)

            check.step("idle-coverage-status-is-bounded", idle_coverage_status)

            def agp():
                response = page.request.post(args.api + "/get_agp_for_assembly", data={})
                assert response.ok
                return response.text()

            initial = agp()

            def undo_redo():
                check.reverse()
                expect(page.get_by_role("button", name="Undo", exact=True)).to_be_enabled()
                assert agp() != initial
                page.get_by_role("link", name="Edit", exact=True).click()
                expect(page.get_by_role("button", name="Undo Reverse selection", exact=True)).to_be_enabled()
                page.keyboard.press("Escape")
                page.get_by_role("button", name="Undo", exact=True).click()
                check.settle()
                assert agp() == initial
                expect(page.get_by_role("button", name="Redo", exact=True)).to_be_enabled()

            check.step("undo-and-redo-at-left", undo_redo)

            def confirm_removal():
                page.get_by_role("button", name="Remove all scaffolds…", exact=True).click()
                confirmation = page.get_by_role("alertdialog", name="Remove all scaffolds?")
                expect(confirmation).to_contain_text("You can undo this")
                expect(confirmation.get_by_role("button", name="Cancel")).to_be_focused()
                page.screenshot(path=str(output / "confirm-remove.png"))
                confirmation.get_by_role("button", name="Cancel").click()
                expect(confirmation).to_be_hidden()
                assert agp() == initial

            check.step("cancel-scaffold-removal", confirm_removal)

            def region_tools():
                page.evaluate("""() => window.__hictBenchmark.manager().viewAndLayersManager.setRegionBounds(
                    {rowStart:40,rowEnd:125,columnStart:80,columnEnd:140})""")
                read = "() => window.__hictBenchmark.manager().viewAndLayersManager.getRegionBounds()"
                before = page.evaluate(read)
                assert before
                square = page.get_by_label("Square selection", exact=True)
                square.check()
                assert before == page.evaluate(read)
                square.uncheck()

            check.step("square-applies-to-new-selections", region_tools)

            def help_guide():
                check.menu("Help", "User guide and shortcuts…")
                dialog = page.get_by_role("dialog", name="User guide and shortcuts")
                expect(dialog.get_by_role("heading", name="HiCT user guide")).to_be_visible()
                page.screenshot(path=str(output / "user-guide.png"))
                page.keyboard.press("Escape")
                expect(dialog).to_be_hidden()

            check.step("help-renders-guide", help_guide)

            if args.track:
                def wheel_track():
                    page.evaluate("file => window.__hictBenchmark.manager().linearTrackManager.openTrack(file)", args.track)
                    track = page.locator(".horizontal-track-canvas-host .track-scroll").first
                    expect(track).to_be_visible()
                    page.wait_for_function("""() => {
                       const node = document.querySelector(".horizontal-track-canvas-host .track-scroll");
                       return node?.scrollHeight > node?.clientHeight;
                    }""")
                    read_zoom = "() => window.__hictBenchmark.manager().getView().getResolution()"
                    view_menu = page.get_by_role("link", name="View", exact=True)
                    wheel_scroll = view_menu.locator("..").locator("button[role=menuitemcheckbox]").filter(
                        has_text="Scroll 1D tracks with mouse wheel"
                    )
                    view_menu.click()
                    expect(view_menu).to_have_attribute("aria-expanded", "true")
                    can_choose_wheel_mode = wheel_scroll.count() > 0
                    if can_choose_wheel_mode:
                        expect(wheel_scroll).to_have_attribute("aria-checked", "false")
                        wheel_scroll.click()
                        expect(wheel_scroll).to_have_attribute("aria-checked", "true")
                    else:
                        page.keyboard.press("Escape")

                    zoom = page.evaluate(read_zoom)
                    track.hover()
                    page.mouse.wheel(0, 120)
                    page.wait_for_function("document.querySelector('.horizontal-track-canvas-host .track-scroll').scrollTop > 0")
                    assert page.evaluate(read_zoom) == zoom

                    if can_choose_wheel_mode:
                        view_menu.click()
                        wheel_scroll.click()
                        expect(wheel_scroll).to_have_attribute("aria-checked", "false")
                        track.evaluate("lane => { lane.scrollTop = 0; }")
                        track.hover()
                        page.mouse.wheel(0, -120)
                        page.wait_for_function(f"before => ({read_zoom})() < before", arg=zoom)
                        assert track.evaluate("lane => lane.scrollTop") == 0

                check.step("wheel-zooms-or-scrolls-feature-lanes", wheel_track)
        finally:
            (output / "crawl.json").write_text(json.dumps({
                "checks": check.rows, "errors": check.errors
            }, indent=2))
            browser.close()


if __name__ == "__main__":
    main()
