#!/usr/bin/env python3
"""Exercise navigation, signal controls and sidebar scrolling at desktop/narrow widths."""

import argparse
import functools
import hashlib
import http.server
import io
import json
from pathlib import Path
import re
import shutil
import subprocess
import threading
from PIL import Image
from playwright.sync_api import expect, sync_playwright
from sequence_browser_harness import BrowserCheck, HictTestServer, configure_test_api


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ("jar", "webui", "data-dir", "output"):
        parser.add_argument("--" + key, required=True)
    args = parser.parse_args()
    data, out = Path(args.data_dir).absolute(), Path(args.output).absolute()
    jar = Path(args.jar).resolve()
    if any(p != p.resolve() or not p.is_relative_to("/tmp") for p in (data, out)):
        parser.error("Use fresh nonsymlinked disposable /tmp paths")
    data.mkdir(parents=True, exist_ok=False)
    out.mkdir(parents=True, exist_ok=False)
    scripts = Path(__file__).resolve().parent
    with (out / "fixture.log").open("w") as log:
        subprocess.run(
            [
                "java",
                "--class-path",
                str(jar),
                str(
                    scripts.parent
                    / "src/test/java/ru/itmo/ctlab/hict/fixtures/ReinforcementHdf5Fixture.java"
                ),
                str(data),
            ],
            check=True,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
    primary = "biology_reference_primary_contact_matrix_with_a_long_filename.hict.hdf5"
    secondary = (
        "biology_reference_secondary_contact_matrix_with_a_long_filename.hict.hdf5"
    )
    fasta = "biology_reference_assembly_sequence_with_a_long_filename.fa"
    for name in [primary, secondary]:
        shutil.copyfile(data / "tiny-primary.hict.hdf5", data / name)
    (data / fasta).write_text(">tiny\nACGTACGTACGTACGTA\n")
    (data / "annotations.bed").write_text(
        "tiny\t0\t3\tShort feature\t42\t+\ntiny\t7\t16\tLong feature\t24\t-\n"
    )
    hashes = {
        name: hashlib.sha256((data / name).read_bytes()).hexdigest()
        for name in [primary, secondary, fasta]
    }

    class Quiet(http.server.SimpleHTTPRequestHandler):
        def log_message(self, *_):
            pass

    web = http.server.ThreadingHTTPServer(
        ("127.0.0.1", 0),
        functools.partial(Quiet, directory=str(Path(args.webui).resolve())),
    )
    threading.Thread(target=web.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{web.server_port}/?dev=1"
    measurements = []
    window_errors = []
    try:
        with HictTestServer(
            jar, data, out / "server.log"
        ) as api, sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            context = browser.new_context(viewport={"width": 1440, "height": 1000})
            context.expose_binding(
                "__captureLayoutError",
                lambda source, message: window_errors.append(message),
            )
            context.add_init_script(
                "window.__HICT_BENCHMARK_ENABLED__=true;window.__layoutErrors=[];window.addEventListener('error',e=>{if(e instanceof ErrorEvent){window.__layoutErrors.push(e.message);window.__captureLayoutError(e.message);}},true)"
            )
            configure_test_api(context, api.url)
            page = context.new_page()
            page.set_default_timeout(30000)
            check = BrowserCheck(page, out)

            def show_sidebar():
                button = page.get_by_role("button", name="Show side panel", exact=True)
                if button.is_visible():
                    button.click()

            def raw():
                return page.evaluate(
                    "async()=>Array.from((await window.__hictBenchmark.manager().networkManager.requestManager.queryMatrixFloat32({bpResolution:4,startRowPx:0,startColPx:0,endRowPx:5,endColPx:5,source:'PRIMARY',signalMode:'RAW_COUNTS'})).values)"
                )

            def bounds():
                return page.evaluate(
                    """()=>{const box=e=>{const r=e.getBoundingClientRect();return {x:r.x,y:r.y,w:r.width,h:r.height,right:r.right,bottom:r.bottom,scroll:e.scrollWidth,client:e.clientWidth}};
                    const sidebar=document.querySelector('.sidebar'),r=sidebar.getBoundingClientRect();
                    const visible=e=>e.getClientRects().length&&getComputedStyle(e).visibility!=='hidden';
                    return {width:innerWidth,root:box(document.querySelector('.main-ui-component')),workspace:box(document.querySelector('.workspace-component')),sidebar:box(sidebar),
                      outside:[...sidebar.querySelectorAll('input,button,select,summary')].filter(e=>visible(e)&&(e.getBoundingClientRect().right>r.right+1||e.getBoundingClientRect().left<r.left-1)).map(e=>({label:e.getAttribute('aria-label')||e.textContent.trim().slice(0,40),...box(e)})),
                      navOutside:[...document.querySelectorAll('.navbar-bar .nav-link,.navbar-bar .file-chip,.navbar-bar .jobs-button')].filter(e=>visible(e)&&(e.getBoundingClientRect().right>innerWidth+1||e.getBoundingClientRect().left<0)).map(e=>({label:e.textContent.trim(),...box(e)}))};}"""
                )

            def menus():
                for name in [
                    "File",
                    "Assembly",
                    "Selection",
                    "View",
                    "Tools",
                    "Dev",
                    "Help",
                ]:
                    if name == "Selection":
                        toggle = page.locator(".header-ribbon").get_by_role(
                            "button", name=name, exact=True
                        )
                    else:
                        toggle = page.locator(".navbar-bar .navbar-nav").get_by_role(
                            "link", name=name, exact=True
                        )
                    toggle.click()
                    menu = toggle.locator("..").locator(".dropdown-menu.show")
                    menu.wait_for()
                    assert menu.evaluate("""element => {
                        const bounds = element.getBoundingClientRect();
                        return bounds.left >= 0 && bounds.right <= innerWidth + 1 &&
                            bounds.top >= 0 && bounds.bottom <= innerHeight + 1;
                    }"""), name
                    items = menu.locator(
                        'a.dropdown-item:not(.disabled):not([aria-disabled="true"]),button.dropdown-item:not([disabled]):not(.disabled),input:not([disabled])'
                    )
                    last = items.last
                    if last.count():
                        last.scroll_into_view_if_needed()
                        assert last.evaluate(
                            "e=>{const b=e.getBoundingClientRect(),p=document.elementFromPoint(b.x+b.width/2,b.y+b.height/2);return b.x>=0&&b.right<=innerWidth+1&&b.y>=0&&b.bottom<=innerHeight+1&&(p===e||e.contains(p));}"
                        ), name
                    if page.viewport_size["width"] == 640:
                        page.screenshot(
                            path=str(out / ("menu-640-" + name.lower() + ".png"))
                        )
                    page.keyboard.press("Escape")
                    menu.wait_for(state="hidden")

            def pixel(label):
                page.locator(
                    ".interactive-workspace_content"
                ).scroll_into_view_if_needed()
                check.settle()
                point = page.evaluate(
                    """()=>{const m=window.__hictBenchmark.manager(),base=m.viewAndLayersManager.coordinateBaseBp,p=m.getMap().getPixelFromCoordinate([10/base,-6/base]),b=m.getMap().getViewport().getBoundingClientRect();return [Math.round(b.x+p[0]),Math.round(b.y+p[1])];}"""
                )
                content = page.screenshot(path=str(out / (label + ".png")))
                picture = Image.open(io.BytesIO(content)).convert("RGB")
                assert (
                    0 <= point[0] < picture.width and 0 <= point[1] < picture.height
                ), point
                return list(picture.getpixel(tuple(point)))

            def notification_access():
                notification = page.get_by_role("button", name="Open notifications", exact=True)
                assert notification.evaluate("""button => {
                    const bounds = button.getBoundingClientRect();
                    const hit = document.elementFromPoint(
                        bounds.left + bounds.width / 2, bounds.top + bounds.height / 2
                    );
                    return bounds.bottom <= innerHeight && button.contains(hit);
                }""")
                upper = page.locator(".toolbar-upper")
                if upper.evaluate("pane => pane.scrollHeight > pane.clientHeight"):
                    before = notification.bounding_box()
                    upper.evaluate("pane => { pane.scrollTop = pane.scrollHeight; }")
                    assert upper.evaluate("pane => pane.scrollTop > 0")
                    assert notification.bounding_box() == before
                    upper.evaluate("pane => { pane.scrollTop = 0; }")

            def desktop_toolbar_fit():
                for width, height in ((1280, 720), (1366, 768), (1440, 900), (1920, 1080)):
                    page.set_viewport_size({"width": width, "height": height})
                    toolbar = page.locator(".toolbar-outer")
                    problems = toolbar.evaluate("""toolbar => {
                        const outside = [];
                        const column = toolbar.querySelector(".toolbar-upper");
                        const viewport = column.getBoundingClientRect();
                        for (const button of toolbar.querySelectorAll("button")) {
                            if (!button.getClientRects().length) continue;
                            const bounds = button.getBoundingClientRect();
                            const name = button.getAttribute("aria-label") || button.title;
                            const limit = column.contains(button) ? viewport.bottom : innerHeight;
                            if (bounds.bottom > limit + 1 || bounds.top < 0) {
                                outside.push([name, "clipped", Math.ceil(Math.max(bounds.bottom - limit, -bounds.top))]);
                            }
                            if (button.matches(":disabled, [aria-disabled=true]")) continue;
                            const hit = document.elementFromPoint(bounds.x + bounds.width / 2, bounds.y + bounds.height / 2);
                            if (!button.contains(hit)) outside.push([name, "obscured"]);
                        }
                        return outside;
                    }""")
                    assert not problems, (width, height, problems)

                page.set_viewport_size({"width": 900, "height": 520})
                upper = page.locator(".toolbar-upper")
                expect(upper).to_have_class(re.compile("toolbar-upper--more-below"))
                upper.evaluate("pane => { pane.scrollTop = pane.scrollHeight; }")
                expect(upper).not_to_have_class(re.compile("toolbar-upper--more-below"))
                expect(page.get_by_role("button", name="Open notifications", exact=True)).to_be_in_viewport()
                page.set_viewport_size({"width": 1440, "height": 1000})

            try:
                page.goto(url)

                def empty_ribbon_search():
                    ribbon = page.locator(".header-ribbon")
                    selection = ribbon.get_by_role("button", name="Selection", exact=True)
                    expect(selection).to_be_disabled()
                    opacity = selection.evaluate("button => getComputedStyle(button).opacity")
                    assert float(opacity) < 1, opacity
                    search = ribbon.get_by_role(
                        "textbox", name="Find a contig, scaffold or feature", exact=True
                    )
                    go_to = ribbon.get_by_role("button", name="Go to", exact=True)
                    for control in (search, go_to):
                        expect(control).to_be_disabled()
                        expect(control).to_have_css("opacity", opacity)

                page.set_viewport_size({"width": 1440, "height": 900})
                check.step("empty-ribbon-search-matches-disabled-controls", empty_ribbon_search)
                for height in (720, 900):
                    page.set_viewport_size({"width": 1440, "height": height})
                    notification_access()
                page.set_viewport_size({"width": 1440, "height": 1000})
                page.evaluate(
                    "a=>window.__hictBenchmark.open(a.primary,a.api,a.fasta)",
                    {"primary": primary, "api": api.url, "fasta": fasta},
                )
                check.settle()
                check.step("desktop-toolbar-controls-fit-and-scroll-fade", desktop_toolbar_fit)
                page.set_viewport_size({"width": 1280, "height": 720})
                notification_access()
                page.evaluate("window.__hictBenchmark.manager().viewAndLayersManager.fitViewToActiveExtent()")
                check.settle()
                ribbon = page.locator(".header-ribbon")
                expect(ribbon.get_by_role("button", name="Selection", exact=True)).to_be_visible()
                ribbon_height = ribbon.bounding_box()["height"]
                square_box = ribbon.get_by_text("Square selection", exact=True).bounding_box()
                check.select_range()
                selection = ribbon.get_by_role("button", name=re.compile(r"^Region · "))
                expect(selection).to_have_attribute("title", re.compile(r"^Region:"))
                assert ribbon.bounding_box()["height"] == ribbon_height <= 56, (
                    ribbon_height,
                    ribbon.evaluate("""ribbon => Array.from(ribbon.querySelectorAll(
                        '.header-block, #left-header-block > *, #right-header-block > *'
                    ), element => [element.id || element.className,
                        element.getBoundingClientRect().toJSON()])"""),
                )
                assert ribbon.get_by_text("Square selection", exact=True).bounding_box() == square_box
                for _ in range(3):
                    ribbon.locator("#ruler-mode-button").click()
                    assert ribbon.bounding_box()["height"] == ribbon_height
                page.keyboard.press("Escape")
                page.set_viewport_size({"width": 1440, "height": 1000})

                def prepare():
                    check.menu("View", "Sources and layers…")
                    dialog = page.get_by_role(
                        "dialog", name="Sources and layers", exact=True
                    )
                    dialog.get_by_role("button", name="Browse…", exact=True).click()
                    picker = page.get_by_role(
                        "dialog", name="Select secondary data source", exact=True
                    )
                    picker.locator(".file-name-cell").get_by_text(
                        secondary, exact=True
                    ).dblclick()
                    picker.wait_for(state="hidden")
                    dialog.get_by_role("button", name="Attach", exact=True).click()
                    expect(
                        dialog.get_by_text("Attached: " + secondary, exact=True)
                    ).to_be_visible()
                    dialog.get_by_role("button",name="Close",exact=True).click()
                    check.menu("View","1D tracks…")
                    dialog=page.get_by_role("dialog",name="1D tracks",exact=True)
                    dialog.get_by_role("button", name="Browse…", exact=True).click()
                    picker = page.get_by_role(
                        "dialog", name="Select 1D track file", exact=True
                    )
                    picker.locator(".file-name-cell").get_by_text(
                        "annotations.bed", exact=True
                    ).dblclick()
                    picker.wait_for(state="hidden")
                    dialog.get_by_role("button", name="Add file", exact=True).click()
                    expect(
                        dialog.get_by_role(
                            "textbox", name="Track name for annotations.bed", exact=True
                        )
                    ).to_be_visible()
                    dialog.get_by_role("button", name="Close", exact=True).click()
                    dialog.wait_for(state="hidden")
                    check.settle()
                    page.locator("#normalization-mode").select_option("none")
                    page.locator("#signal-transform-linear").click()
                    details = page.locator("#signal-advanced")
                    if details.get_attribute("open") is None:
                        details.locator("summary").click()
                    page.locator("#scaling-across-zoom").select_option("none")
                    page.get_by_label("Lower signal threshold", exact=True
                    ).fill("0")
                    page.get_by_label("Upper signal threshold", exact=True
                    ).fill("11")
                    page.locator("#gradient-apply-button").click()
                    check.settle()
                    page.locator(".navbar-bar .navbar-nav").get_by_role(
                        "link", name="View", exact=True
                    ).click()
                    alignment = page.get_by_role(
                        "menuitemcheckbox",
                        name="Align visible FASTA automatically",
                        exact=True,
                    )
                    if alignment.get_attribute("aria-checked") == "true":
                        alignment.click()
                    else:
                        page.keyboard.press("Escape")
                    check.settle()

                check.step(
                    "long-reference-names-fasta-secondary-and-annotations", prepare
                )
                known = [
                    1,
                    3,
                    0,
                    0,
                    0,
                    3,
                    0,
                    7,
                    0,
                    0,
                    0,
                    7,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    11,
                ]
                assert raw() == known
                for index, (width, height) in enumerate(
                    [
                        (1440, 1000),
                        (1280, 900),
                        (1024, 768),
                        (900, 700),
                        (800, 600),
                        (640, 800),
                    ]
                ):

                    def layout(width=width, height=height):
                        page.set_viewport_size({"width": width, "height": height})
                        show_sidebar()
                        check.settle()
                        page.wait_for_function(
                            """()=>{const r=document.querySelector('.main-ui-component');return r.scrollWidth<=innerWidth+1;}"""
                        )
                        result = bounds()
                        assert (
                            not result["outside"] and not result["navOutside"]
                        ), result
                        assert result["sidebar"]["h"] >= 250, result
                        assert page.locator(".file-chip--static").count() == 2
                        assert (
                            page.locator(".file-chip--static")
                            .nth(0)
                            .get_attribute("title")
                            == primary
                        )
                        assert (
                            page.locator(".file-chip--static")
                            .nth(1)
                            .get_attribute("title")
                            == secondary
                        )
                        source_toggle = page.get_by_role("button", name="Sources", exact=True)
                        compact = source_toggle.is_visible()
                        if compact:
                            source_toggle.press("Enter")
                            expect(source_toggle).to_have_attribute("aria-expanded", "true")
                        assert (
                            page.get_by_role(
                                "button", name="Relink FASTA", exact=True
                            ).get_attribute("title")
                            == fasta
                        )
                        if compact:
                            page.keyboard.press("Escape")
                            expect(source_toggle).to_have_attribute("aria-expanded", "false")
                        menus()
                        measurements.append(result)
                        pixel("workspace-" + str(width))

                    check.step(f"controls-and-all-menus-fit-{width}", layout)

                    def change_range(index=index, width=width):
                        before = pixel("before-" + str(width))
                        upper = page.get_by_label("Upper signal threshold", exact=True
                        )
                        upper.fill(str(22 + index * 11))
                        page.locator("#gradient-apply-button").click()
                        check.settle()
                        options = page.request.post(
                            api.url + "/get_visualization_options", data={}
                        ).json()
                        assert (
                            options["colormap"]["maxSignal"] == 22 + index * 11
                        ), options
                        check.settle()
                        after = pixel("after-" + str(width))
                        assert before != after, (width, before, after)
                        assert raw() == known

                    check.step(
                        f"visible-range-controls-change-pixels-and-preserve-contacts-{width}",
                        change_range,
                    )

                def compact_map():
                    page.set_viewport_size({"width": 390, "height": 844})
                    hide_sidebar = page.get_by_role("button", name="Hide side panel", exact=True)
                    if hide_sidebar.is_visible():
                        hide_sidebar.click()
                    map_area = page.locator(".interactive-workspace_content")
                    tools_toggle = page.get_by_role("button", name="Map tools", exact=True)
                    tools = page.get_by_role("region", name="Map tools", exact=True)
                    tracks_toggle = page.get_by_role("button", name="Tracks", exact=True)
                    tracks = page.locator("#workspace-horizontal-tracks")
                    expect(tools).to_be_hidden()
                    expect(tracks).to_be_hidden()
                    assert map_area.bounding_box()["width"] >= 390 * 0.7
                    tools_toggle.press("Enter")
                    expect(tools).to_be_visible()
                    tools_toggle.press("Tab")
                    expect(tools).to_be_focused()
                    page.keyboard.press("Escape")
                    expect(tools_toggle).to_be_focused()
                    expect(tools).to_be_hidden()
                    tracks_toggle.press("Enter")
                    expect(tracks).to_be_visible()
                    tracks_toggle.press("Enter")
                    expect(tracks).to_be_hidden()
                    assert raw() == known

                check.step("map-and-keyboard-panels-at-390", compact_map)

                def restore_choice():
                    page.set_viewport_size({"width": 800, "height": 600})
                    show_sidebar()
                    page.get_by_role(
                        "button", name="Hide side panel", exact=True
                    ).click()
                    show_sidebar()
                    page.set_viewport_size({"width": 1024, "height": 768})
                    page.set_viewport_size({"width": 800, "height": 600})
                    page.reload()
                    check.menu("File", "Join running session")
                    check.settle()
                    expect(
                        page.get_by_role("button", name="Hide side panel", exact=True)
                    ).to_be_visible()
                    page.get_by_role(
                        "button", name="Hide side panel", exact=True
                    ).click()
                    page.reload()
                    check.menu("File", "Join running session")
                    check.settle()
                    expect(
                        page.get_by_role("button", name="Show side panel", exact=True)
                    ).to_be_visible()

                check.step(
                    "explicit-sidebar-choice-survives-viewport-changes-and-reload",
                    restore_choice,
                )
                fresh = browser.new_context(viewport={"width": 640, "height": 800})
                configure_test_api(fresh, api.url)
                empty = fresh.new_page()
                empty.on("pageerror", lambda error: check.errors.append(str(error)))
                empty.goto(url)
                empty.get_by_role(
                    "heading", name="No contact map open", exact=True
                ).wait_for()
                expect(
                    empty.get_by_role("button", name="Show side panel", exact=True)
                ).to_be_visible()
                empty.get_by_role("button", name="Show side panel", exact=True).click()
                hint = empty.locator(".sidebar-empty-hint")
                hint.scroll_into_view_if_needed()
                expect(hint).to_be_visible()
                empty.screenshot(path=str(out / "empty-640.png"))
                fresh.close()
                check.rows.append(
                    {
                        "test": "fresh-narrow-window-starts-collapsed-and-opens-its-empty-panel",
                        "status": "passed",
                    }
                )
                assert not check.errors, check.errors
                errors = window_errors
                assert not errors, errors
                assert all(
                    hashlib.sha256((data / name).read_bytes()).hexdigest() == digest
                    for name, digest in hashes.items()
                )
                (out / "results.json").write_text(
                    json.dumps(
                        {
                            "status": "ok",
                            "checks": check.rows,
                            "measurements": measurements,
                            "windowErrors": errors,
                            "pageErrors": check.errors,
                            "sourceHashes": hashes,
                        },
                        indent=2,
                    )
                )
            except Exception:
                page.screenshot(path=str(out / "failed.png"))
                raise
            finally:
                browser.close()
    finally:
        web.shutdown()
        web.server_close()


if __name__ == "__main__":
    main()
