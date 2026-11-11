"""UI actions shared by reinforcement acceptance checks on disposable servers."""
import re
from pathlib import PurePosixPath


def expand_slider_limits(page):
    details = page.locator("details.range-details")
    if details.get_attribute("open") is None:
        details.locator("summary").click()


def link_fasta(page, filename):
    """Use the same linkage path as the user, including frontend capability updates."""
    page.get_by_role("link", name="Assembly", exact=True).click()
    page.get_by_role("link", name="Link FASTA", exact=True).click()
    dialog = page.locator(".modal.show").filter(has_text="Select FASTA file")
    file = PurePosixPath(filename)
    if str(file.parent) != ".":
        path = dialog.get_by_placeholder("Relative folder or file path under DATA_DIR")
        path.fill(str(file.parent))
        with page.expect_response(lambda response: response.url.endswith("/list_directory")):
            path.press("Enter")
    # Enter on a file path can submit it immediately; choose the row explicitly
    # so the response listener is installed before the separate Load action.
    name_pattern = re.compile("^" + re.escape(file.name) + "$")
    dialog.locator("span[title]").filter(has_text=name_pattern).click()
    with page.expect_response(lambda response: response.url.endswith("/link_fasta")) as linked:
        dialog.get_by_role("button", name="Load", exact=True).click()
    assert linked.value.ok, linked.value.text()
    dialog.wait_for(state="hidden")


def select_visualization_source(page, source):
    if source not in ("PRIMARY", "SECONDARY"):
        raise ValueError(f"Unknown visualization source: {source}")
    button = page.get_by_role("group", name="Visualization source", exact=True).get_by_role(
        "button", name=source.title(), exact=True)
    button.click()
    assert button.get_attribute("aria-pressed") == "true", source
