#!/usr/bin/env python3
"""Exercise optimistic workspace saving with independent real browser clients."""
import argparse
import json
from pathlib import Path

from playwright.sync_api import sync_playwright


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:8187")
    parser.add_argument("--api", default="http://127.0.0.1:5107")
    parser.add_argument("--data-dir", required=True)
    parser.add_argument("--output", default="/tmp/hict-release-physical/concurrent-attach")
    args = parser.parse_args()
    data = Path(args.data_dir).absolute()
    if data != data.resolve() or not data.is_relative_to("/tmp"):
        raise ValueError("Use a dedicated /tmp directory with physical copies")
    filename = "combined_ind2_4DN.hict.hdf5"
    path = data / filename
    if not path.is_file() or path.is_symlink() or path.stat().st_nlink != 1:
        raise ValueError("Use a physical map copy")
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    results, errors = [], []
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        contexts = [browser.new_context(viewport={"width": 1440, "height": 1000}) for _ in range(2)]
        pages = [context.new_page() for context in contexts]
        a, b = pages
        b_saves = []
        b.on("request", lambda request: b_saves.append(request.post_data_json)
             if request.url.endswith("/workspace/state") else None)
        for page in pages:
            page.set_default_timeout(60000)
            page.on("pageerror", lambda error: errors.append(str(error)))
            page.add_init_script("window.__HICT_BENCHMARK_ENABLED__=true")
            page.route("http://localhost:5000/**", lambda route: route.continue_(
                url=args.api + route.request.url.removeprefix("http://localhost:5000")))

        def stored():
            response = a.request.post(args.api + "/attach", data={})
            assert response.ok, response.text()
            return response.json()

        def wait_width(width):
            for _ in range(60):
                state = stored().get("workspaceState")
                if state and state["trackStyles"]["contigs"]["width"] == width:
                    return state
                a.wait_for_timeout(100)
            raise AssertionError(f"Workspace width {width} was not saved")

        def attach(page):
            page.get_by_role("link", name="File", exact=True).click()
            with page.expect_response("**/attach"):
                page.get_by_role("link", name="Attach", exact=True).click()
            page.wait_for_timeout(1800)
            prompt = page.locator(".cooler-weights-nan-modal.show .btn-close")
            if prompt.is_visible():
                prompt.click()

        def width(page):
            return page.evaluate("window.__hictBenchmark.manager().viewAndLayersManager.getTrackStylePreset().contigs.width")

        try:
            a.goto(args.url)
            a.wait_for_function("window.__hictBenchmark")
            a.evaluate("arg=>window.__hictBenchmark.open(arg.filename,arg.api)", {"filename": filename, "api": args.api})
            a.wait_for_timeout(1500)
            button = a.get_by_role("button", name="Do not use Cooler weights", exact=True)
            if button.is_visible():
                button.click()
            a.evaluate("""() => {const m=window.__hictBenchmark.manager(),l=m.viewAndLayersManager;
                const p=l.getTrackStylePreset();p.contigs.width=2;p.contigs.borderOutline=false;
                l.applyTrackStylePreset(p);m.getView().setResolution(m.getView().getResolution()/2);} """)
            wait_width(2)
            b.goto(args.url)
            b.wait_for_function("window.__hictBenchmark")
            attach(b)
            assert width(b) == 2
            results.append({"test": "second browser restores first browser workspace"})
            a.evaluate("""() => {const l=window.__hictBenchmark.manager().viewAndLayersManager,
                p=l.getTrackStylePreset();p.contigs.width=7;l.applyTrackStylePreset(p);}""")
            latest = wait_width(7)
            b_saves.clear()
            b.evaluate("""() => {const v=window.__hictBenchmark.manager().getView(),c=v.getCenter();
                v.setCenter([c[0]+30*v.getResolution(),c[1]]);}""")
            b.wait_for_timeout(6000)
            actual = stored()["workspaceState"]
            results.append({"test": "stale browser pan cannot overwrite newer styles",
                            "expectedWidth": 7, "actualWidth": actual["trackStyles"]["contigs"]["width"]})
            assert actual == latest, "An older client replaced newer workspace settings"
            assert len(b_saves) == 1, "A rejected autosave must not keep retrying"
            assert width(b) == 2, "Conflict must not silently replace local settings"
            b.get_by_text("Workspace settings changed in another window. Attach again to restore the latest saved view.", exact=True).wait_for()
            b.screenshot(path=str(output / "conflict-preserves-local-view.png"))
            attach(b)
            assert width(b) == 7
            assert b.get_by_text("Workspace settings changed in another window. Attach again to restore the latest saved view.", exact=True).count() == 0
            results.append({"test": "explicit reattach resolves workspace conflict"})
            old = stored()
            a.evaluate("arg=>window.__hictBenchmark.open(arg.filename,arg.api)", {"filename": filename, "api": args.api})
            new = stored()
            assert new["workspaceVersion"]["session"] != old["workspaceVersion"]["session"]
            stale = a.request.post(args.api + "/workspace/state", data={"filename": filename,
                "state": old["workspaceState"], "expectedVersion": old["workspaceVersion"]})
            assert stale.status == 409, stale.text()
            current = stored()
            assert current["workspaceVersion"]["session"] == new["workspaceVersion"]["session"]
            assert current.get("workspaceState") != old["workspaceState"]
            results.append({"test": "reopening same filename rejects stale session saves"})
            assert not errors, errors
        finally:
            (output / "results.json").write_text(json.dumps({"results": results, "errors": errors}, indent=2))
            browser.close()
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
