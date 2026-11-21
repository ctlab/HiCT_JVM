#!/usr/bin/env python3
"""Check export controls, source extrema, and edited search boundaries in a running candidate."""
import argparse
import json
import time
from pathlib import Path

from playwright.sync_api import sync_playwright
from sequence_browser_harness import BrowserCheck


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ("api", "url", "data-dir", "output"):
        parser.add_argument("--" + key, required=True)
    args = parser.parse_args()
    data, output = Path(args.data_dir), Path(args.output)
    assert data.resolve() == data and data.is_relative_to("/tmp")
    output.mkdir(parents=True, exist_ok=True)
    source = "tiny-primary.hict.hdf5"
    results = []
    with sync_playwright() as pw:
        browser = pw.chromium.launch()
        page = browser.new_page(viewport={"width": 1440, "height": 1000})
        page.add_init_script("window.__HICT_BENCHMARK_ENABLED__=true")
        page.route("http://localhost:5000/**", lambda r: r.continue_(
            url=args.api + r.request.url.removeprefix("http://localhost:5000")))
        def post(path, body):
            res = page.request.post(args.api+path, data=body)
            assert res.ok, (path, res.text())
            return res.json()

        page.goto(args.url)
        page.wait_for_function("window.__hictBenchmark")
        page.evaluate("([name,api])=>window.__hictBenchmark.open(name,api,'tiny.fa')", [source,args.api])
        check = BrowserCheck(page, output)
        check.settle()
        check.menu("File", "Export matrix")
        dialog = page.get_by_role("dialog").filter(has=page.get_by_role("heading", name="Export matrix to Cooler"))
        inputs = dialog.locator("input[type=number]")
        submit = dialog.get_by_role("button", name="Export Cooler", exact=True)
        for idx, key, invalid, valid in (
            (0, "compression", ["", "-1", "10", "2.5"], ["0", "9"]),
            (1, "parallelism", ["", "-2", "1.5", "2147483648"], ["-1", "0", "1", "2147483647"]),
        ):
            for value in invalid:
                inputs.nth(idx).fill(value)
                assert submit.is_disabled() and dialog.get_by_role("alert").is_visible(), (key,value)
                if value:
                    response = page.request.post(args.api+"/convert/jobs", data={"filename":source,key:float(value)})
                    assert 400 <= response.status < 500 and key in response.text(), response.text()
            for value in valid:
                inputs.nth(idx).fill(value)
                assert submit.is_enabled(), (key,value)
            results.append({"field":key,"rejected":invalid,"accepted":valid})
        page.keyboard.press("Escape")
        post("/secondary/open", {"filename":"tiny-extrema.hict.hdf5"})
        for side, maximum in (("PRIMARY",11),("SECONDARY",83)):
            stats = post("/matrix/statistics", {"bpResolution":4, "startRowPx":0, "startColPx":0,
                "expression":{"type":"source","source":side}})
            assert [stats["count"],stats["min"],stats["max"]] == [25,0,maximum], stats
            results.append({"source":side,"expected":[0,maximum],"observed":[stats["min"],stats["max"]]})
        post("/open", {"filename":source})
        post("/link_fasta", {"fastaFilename":"tiny.fa"})
        raw = "ACGTRYMKSWBDHVNAC"
        complement = str.maketrans("ACGTRYMKSWBDHVN","TGCAYRKMSWVHDBN")
        (data/"tiny-reversed.agp").write_text("edited\t1\t17\t1\tW\ttiny\t1\t17\t-\n")
        for reversed_source, seq in ((False,raw), (True,raw.translate(complement)[::-1])):
            if reversed_source:
                post("/load_agp", {"agpFilename":"tiny-reversed.agp"})
            assert post("/sequence/region", {"startBp":0,"endBp":17})["sequence"] == seq
            for start,end in ((0,17),(0,2),(1,16),(15,17)):
                path = f"bounds-{int(reversed_source)}-{start}-{end}.bed"
                submission = post("/sequence/search/jobs", {"scope":"selection","startBp":start,"endBp":end,
                    "queries":"AC","bothStrands":True,"mode":"exact","threads":1,"outputFilename":path})
                deadline = time.monotonic()+30
                while time.monotonic()<deadline:
                    job = next(j for j in post("/sequence/jobs/list",{}) if j["jobId"] == submission["jobId"])
                    if job["status"] in ("finished","failed","cancelled"):
                        break
                    time.sleep(.1)
                assert job["status"] == "finished",job
                expected = []
                for i in range(start,end-1):
                    word = seq[i:i+2]
                    if word not in ("AC","GT"):
                        continue
                    neg = (word == "GT") != reversed_source
                    source_start = 17-i-2 if reversed_source else i
                    expected.append([source_start,source_start+2,"-" if neg else "+"])
                actual = []
                for line in (data/path).read_text().splitlines():
                    if line and not line.startswith(("#","track","browser")):
                        fields = line.split("\t")
                        actual.append([int(fields[1]),int(fields[2]),fields[5]])
                assert sorted(actual)==sorted(expected), (seq,start,end,actual,expected)
                results.append({"reversed":reversed_source,"range":[start,end],"expected":expected,"observed":actual})
        (output/"results.json").write_text(json.dumps(results, indent=2)+"\n")
        browser.close()


if __name__ == "__main__":
    main()
