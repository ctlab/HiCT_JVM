#!/usr/bin/env python3
"""Verify local map editing, session recovery, exports and API docs without WAN requests."""

import argparse
import hashlib
import json
from pathlib import Path
from urllib.parse import urlsplit

from playwright.sync_api import sync_playwright
from sequence_browser_harness import BrowserCheck


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("url", "api", "output"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--filename", default="structural.hict.hdf5")
    args = parser.parse_args()
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    evidence = {"externalRequests": [], "exports": [], "checks": []}
    errors = []
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        context = browser.new_context(viewport={"width": 1440, "height": 1000})

        def local_only(route):
            url = route.request.url
            if url.startswith("http://localhost:5000/"):
                route.continue_(
                    url=args.api + url.removeprefix("http://localhost:5000")
                )
            elif urlsplit(url).hostname in ("127.0.0.1", "localhost"):
                route.continue_()
            else:
                evidence["externalRequests"].append(url)
                route.abort()

        context.route("**/*", local_only)
        page = context.new_page()
        page.set_default_timeout(60000)
        page.add_init_script("window.__HICT_BENCHMARK_ENABLED__=true")
        page.on("pageerror", lambda error: errors.append(str(error)))
        check = BrowserCheck(page, output)

        def assembly():
            return page.evaluate(
                "async () => (await window.__hictBenchmark.manager().networkManager.requestManager.getSessionSnapshot(true)).primary"
            )

        try:
            page.goto(args.url)
            page.wait_for_function("window.__hictBenchmark")
            check.menu("File", "Open map…")
            check.choose_file(args.filename)
            page.get_by_role("button", name="Load", exact=True).click()
            check.settle()
            before = assembly()
            check.reverse()
            changed = assembly()
            assert before["agpSha256"] != changed["agpSha256"]
            check.menu("File", "Close")
            prompt = page.get_by_role("alertdialog", name="Unsaved assembly edits")
            prompt.get_by_role("button", name="Cancel", exact=True).click()
            assert assembly()["agpSha256"] == changed["agpSha256"]
            check.menu("File", "Close")
            with page.expect_download() as saved:
                prompt.get_by_role("button", name="Save session", exact=True).click()
            session = output / "offline-session.json"
            saved.value.save_as(session)
            page.get_by_role(
                "heading", name="No contact map open", exact=True
            ).wait_for()
            page.reload()
            with page.expect_file_chooser() as chooser:
                check.menu("File", "Open session")
            chooser.value.set_files(session)
            page.get_by_text(
                "Session restored. Undo history starts from the restored assembly.",
                exact=True,
            ).wait_for()
            check.settle()
            restored = assembly()
            assert restored["agpSha256"] == changed["agpSha256"]
            assert restored["assemblyInfo"] == changed["assemblyInfo"]
            evidence["checks"].append(
                "Unsaved Close Cancel preserves edits, Save closes, browser restart and session restore retain exact assembly"
            )
            for extension in ("Png", "Svg", "Pdf"):
                with page.expect_download() as download:
                    page.evaluate(
                        'async format => {await window.__hictBenchmark.manager()["exportCurrentMap" + format](undefined,{scope:"viewport",cropToMap:true});}',
                        extension,
                    )
                target = output / ("offline." + extension.lower())
                download.value.save_as(target)
                assert target.stat().st_size > 1000
                evidence["exports"].append(
                    {
                        "format": extension,
                        "bytes": target.stat().st_size,
                        "sha256": hashlib.sha256(target.read_bytes()).hexdigest(),
                    }
                )
            docs = context.new_page()
            docs.on("pageerror", lambda error: errors.append(str(error)))
            docs.goto(args.api + "/api/v1/")
            docs.locator(".swagger-ui .opblock").first.wait_for()
            assert docs.locator(".swagger-ui .opblock").count() > 20
            docs.screenshot(path=str(output / "offline-api.png"))
            evidence["checks"].append(
                "Bundled Swagger renders API operations with WAN requests denied"
            )
            page.screenshot(path=str(output / "offline-map.png"))
            assert not evidence["externalRequests"], evidence["externalRequests"]
            assert not errors and not check.errors, errors + check.errors
            evidence["status"] = "passed"
        finally:
            evidence["errors"] = errors + check.errors
            (output / "results.json").write_text(json.dumps(evidence, indent=2) + "\n")
            browser.close()
    print(json.dumps(evidence, indent=2))


if __name__ == "__main__":
    main()
