#!/usr/bin/env python3
"""Exercise real wheel zoom across pyramid levels and preserve explicit contrast.

Each step is three wheel events (roughly a doubling). The gate measures red
contact signal along the visible diagonal, excluding borders and entity labels.
Fixtures must be disposable copies in the server data directory.
"""

import argparse
import io
import json
from pathlib import Path

import numpy as np
from PIL import Image
from playwright.sync_api import sync_playwright


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("url", "api", "output"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--browser-executable")
    parser.add_argument(
        "--files",
        nargs="+",
        default=["combined_ind2_4DN.hict.hdf5", "arabiensis.0.hict.hdf5"],
    )
    parser.add_argument(
        "--normalizations",
        nargs="+",
        choices=["file", "sqrt"],
        default=["file", "sqrt"],
    )
    parser.add_argument("--floor", type=float, default=0.5)
    args = parser.parse_args()
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    report = {"checks": [], "pageErrors": [], "consoleErrors": [], "frames": []}

    def check(name, ok, **detail):
        report["checks"].append(dict(name=name, ok=bool(ok), **detail))
        print(
            ("PASS " if ok else "FAIL ") + name + " " + json.dumps(detail), flush=True
        )

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=True, executable_path=args.browser_executable
        )
        context = browser.new_context(viewport={"width": 1440, "height": 900})
        context.add_init_script("window.__HICT_BENCHMARK_ENABLED__=true")
        context.route(
            "http://localhost:5000/**",
            lambda r: r.continue_(
                url=args.api + r.request.url.removeprefix("http://localhost:5000")
            ),
        )
        page = context.new_page()
        page.set_default_timeout(120000)
        page.on("pageerror", lambda e: report["pageErrors"].append(str(e)))
        page.on(
            "console",
            lambda m: (
                report["consoleErrors"].append(m.text) if m.type == "error" else None
            ),
        )

        def settle():
            page.wait_for_timeout(2200)
            page.wait_for_function(
                "()=>!window.__hictBenchmark.manager().visualizationManager.openingRangeRefreshInFlight"
            )
            page.wait_for_timeout(700)

        def state():
            return page.evaluate("""async()=>{
                const m=window.__hictBenchmark.manager();
                return {resolution:m.viewAndLayersManager.currentViewState.resolutionDesciptor.bpResolution};
            }""")

        def measure(tag):
            g = page.evaluate(
                """()=>{const m=window.__hictBenchmark.manager(), map=m.getMap(), e=map.getView().getProjection().getExtent(),r=map.getViewport().getBoundingClientRect();return {a:map.getPixelFromCoordinate([e[0],e[3]]), b:map.getPixelFromCoordinate([e[2],e[1]]),x:r.x,y:r.y,width:r.width,height:r.height}}"""
            )
            shot = page.screenshot()
            (output / (tag + ".png")).write_bytes(shot)
            rgb = np.asarray(Image.open(io.BytesIO(shot)).convert("RGB")).astype(int)
            a, b = np.array(g["a"]), np.array(g["b"])
            delta = b - a
            low, high = 0.0, 1.0
            # Keep rulers, minimap and zoom controls out of the pixel oracle.
            for origin, span, size, margin in zip(
                a, delta, [g["width"], g["height"]], [150, 90]
            ):
                entry, exit = sorted(
                    ((margin - origin) / span, (size - 10 - origin) / span)
                )
                low, high = max(low, entry), min(high, exit)
            drawn = total = 0
            if high > low:
                for t in np.linspace(low, high, 200):
                    x, y = a + t * delta + np.array([g["x"], g["y"]])
                    pixels = rgb[
                        int(y) - 3 : int(y) + 4, int(x) - 3 : int(x) + 4
                    ].reshape(-1, 3)
                    signal = (pixels[:, 0] - pixels[:, 1] > 25) & (
                        pixels[:, 0] - pixels[:, 2] > 25
                    )
                    drawn += signal.any()
                    total += 1
            share = float(drawn / total) if total else 0
            check(
                tag + " diagonal",
                total >= 20 and share >= args.floor,
                share=share,
                samples=total,
                resolution=state()["resolution"],
            )

        def wheel():
            box = page.locator(".ol-viewport").first.bounding_box()
            page.mouse.move(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2)
            page.mouse.wheel(0, -300)
            page.wait_for_timeout(300)

        try:
            for fixture in args.files:
                for normalization in args.normalizations:
                    context.request.post(args.api + "/close", data={})
                    page.goto(args.url)
                    page.wait_for_function("window.__hictBenchmark")
                    page.get_by_text("Open map…", exact=False).first.click()
                    d = page.get_by_role("dialog")
                    d.get_by_text(fixture, exact=False).first.click()
                    d.get_by_role("button", name="Load", exact=True).click()
                    page.wait_for_function("window.__hictBenchmark.manager?.()")
                    settle()
                    if normalization == "sqrt":
                        page.locator("#normalization-mode").select_option("sqrt")
                        page.wait_for_function(
                            '()=>!document.querySelector("#normalization-progress")',
                            timeout=240000,
                        )
                        page.get_by_role("button", name="Apply", exact=True).click()
                        settle()
                    stem = fixture.split(".")[0] + "-" + normalization
                    initial = state()
                    page.evaluate(
                        """()=>{window.__zoomFrames=[];let last=performance.now();window.__zoomFrameId=requestAnimationFrame(function frame(now){window.__zoomFrames.push(now-last);last=now;window.__zoomFrameId=requestAnimationFrame(frame)})}"""
                    )
                    measure(stem + "-0")
                    for step in range(1, 9):
                        for _ in range(3):
                            wheel()
                        settle()
                        if step in (4, 8):
                            measure(stem + "-" + str(step))
                        else:
                            page.screenshot(
                                path=str(output / (stem + "-" + str(step) + ".png"))
                            )
                    frames = page.evaluate(
                        "()=>{cancelAnimationFrame(window.__zoomFrameId);return window.__zoomFrames}"
                    )
                    p95 = float(np.percentile(frames, 95))
                    report["frames"].append(
                        {"fixture": fixture, "normalization": normalization, "p95": p95}
                    )
                    check(stem + " frame pacing", p95 <= 33, p95=p95)
                    check(
                        stem + " crosses pyramid levels",
                        state()["resolution"] < initial["resolution"],
                    )
                    display = page.locator("select").filter(
                        has=page.locator('option[value="OBSERVED_OVER_EXPECTED"]')
                    )
                    observed = context.request.post(
                        args.api + "/get_visualization_options", data={}
                    ).json()["colormap"]
                    display.select_option("OBSERVED_OVER_EXPECTED")
                    settle()
                    oe = context.request.post(
                        args.api + "/get_visualization_options", data={}
                    ).json()["colormap"]
                    check(
                        stem + " O/E diverging range",
                        (oe["minSignal"], oe["maxSignal"]) == (0.5, 2),
                    )
                    page.screenshot(path=str(output / (stem + "-oe.png")))
                    display.select_option("OBSERVED")
                    settle()
                    restored = context.request.post(
                        args.api + "/get_visualization_options", data={}
                    ).json()["colormap"]
                    check(stem + " Observed range restored", restored == observed)
                    # Explicit Lower/Upper must survive the next resolution change.
                    upper = page.get_by_label("Upper signal threshold")
                    upper.fill("0.123")
                    upper.press("Tab")
                    page.get_by_role("button", name="Apply", exact=True).click()
                    settle()
                    for _ in range(4):
                        wheel()
                    settle()
                    check(
                        stem + " manual upper retained",
                        abs(float(upper.input_value()) - 0.123) < 1e-9,
                    )
        finally:
            check(
                "no page errors", not report["pageErrors"], errors=report["pageErrors"]
            )
            check(
                "no console errors",
                not report["consoleErrors"],
                errors=report["consoleErrors"],
            )
            (output / "result.json").write_text(json.dumps(report, indent=2))
            browser.close()
    raise SystemExit(1 if any(not c["ok"] for c in report["checks"]) else 0)


if __name__ == "__main__":
    main()
