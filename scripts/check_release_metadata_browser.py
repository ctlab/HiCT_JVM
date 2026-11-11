#!/usr/bin/env python3
"""Verify the built WebUI metadata, citation and guide entry in a fresh browser."""

import argparse
import json
from pathlib import Path
from playwright.sync_api import sync_playwright


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("url", "api", "output", "webui-version", "webui-commit"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument(
        "--file", help="Optional disposable map to verify workspace preservation"
    )
    args = parser.parse_args()
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        context = browser.new_context(viewport={"width": 1440, "height": 1000})
        errors = []
        page = context.new_page()
        page.add_init_script("window.__HICT_BENCHMARK_ENABLED__=true")
        page.on("pageerror", lambda error: errors.append(str(error)))
        page.on(
            "console",
            lambda message: (
                errors.append(message.text) if message.type == "error" else None
            ),
        )
        page.route(
            "http://localhost:5000/**",
            lambda route: route.continue_(
                url=args.api + route.request.url.removeprefix("http://localhost:5000")
            ),
        )
        page.goto(args.url)
        assembly_before = None
        if args.file:
            page.wait_for_function("window.__hictBenchmark")
            page.evaluate(
                "async a=>{await window.__hictBenchmark.open(a.file,a.api)}", vars(args)
            )
            assembly_before = page.evaluate(
                "async ()=>(await window.__hictBenchmark.manager().networkManager.requestManager.getSessionSnapshot(true)).primary.agpSha256"
            )
        page.get_by_role("link", name="Help", exact=True).click()
        guide = page.get_by_role("link", name="User guide and shortcuts", exact=True)
        guide_href = guide.get_attribute("href")
        assert guide_href.endswith("/docs/user-guide.md")
        page.get_by_role("link", name="About", exact=True).click()
        dialog = page.get_by_role("dialog", name="About HiCT")
        dialog.get_by_role("heading", name="How to cite", exact=True).wait_for()
        page.wait_for_function("""() => {
            const text = document.querySelector('.about-versions')?.textContent ?? '';
            return text.length > 0 && !text.includes('loading');
        }""")
        versions = dialog.locator(".about-versions").inner_text()
        assert f"WebUI: {args.webui_version}" in versions, versions
        assert f"Commit: {args.webui_commit}" in versions.splitlines(), versions
        assert "unknown" not in versions and "loading" not in versions, versions
        citation = dialog.locator(".how-to-cite").inner_text()
        assert args.webui_version in citation and args.webui_commit in citation
        assert "Serdiukov" in citation and "Zamyatin" in citation
        assert (
            dialog.get_by_role("link", name="Machine-readable software citation")
            .get_attribute("href")
            .endswith("/CITATION.cff")
        )
        contacts = dialog.locator('a[href^="mailto:"]')
        assert contacts.count() == 4
        mail_targets = []
        for contact in contacts.all():
            address = contact.inner_text().strip()
            assert contact.get_attribute("href") == "mailto:" + address
            mail_targets.append(address)
        page.screenshot(path=str(output / "about-citation.png"))
        dialog.get_by_role("button", name="Attribution", exact=True).click()
        attribution = dialog.locator(".attribution-panel").inner_text()
        for tool in ("hictk", "BLAST", "minimap2"):
            assert tool.lower() in attribution.lower(), tool
        assert "MIT" in attribution
        page.screenshot(path=str(output / "about-attribution.png"))
        dialog.get_by_role("button", name="Changelog", exact=True).click()
        dialog.locator(".about-changelog").get_by_role(
            "heading", name=args.webui_version, exact=True
        ).wait_for()
        page.screenshot(path=str(output / "about-changelog.png"))
        dialog.get_by_role("button", name="About", exact=True).click()
        assert dialog.locator(".about-versions").inner_text() == versions
        if assembly_before:
            assembly_after = page.evaluate(
                "async ()=>(await window.__hictBenchmark.manager().networkManager.requestManager.getSessionSnapshot(true)).primary.agpSha256"
            )
            assert assembly_after == assembly_before

        assert not errors, errors
        (output / "results.json").write_text(
            json.dumps(
                {
                    "versions": versions,
                    "citation": citation,
                    "guide": guide_href,
                    "mailTargets": mail_targets,
                    "tabs": ["About", "Attribution", "Changelog"],
                    "workspaceAgpSha256": assembly_before,
                    "errors": errors,
                },
                indent=2,
            )
        )
        browser.close()


if __name__ == "__main__":
    main()
