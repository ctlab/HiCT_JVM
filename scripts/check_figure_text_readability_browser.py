#!/usr/bin/env python3
"""Check print-text measurement against independent SVG transforms in real Chromium."""
import argparse
import json
from pathlib import Path
import subprocess
from playwright.sync_api import sync_playwright


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--webui-project", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    project = Path(args.webui_project).resolve()
    out = Path(args.output).absolute()
    assert out == out.resolve() and out.is_relative_to("/tmp")
    out.mkdir(exist_ok=False)
    module = out / "readability.js"
    subprocess.run([
        "node", str(project / "node_modules/esbuild/bin/esbuild"),
        str(project / "src/app/core/export/FigurePrintReadability.ts"),
        "--bundle", "--format=iife", "--global-name=PrintReadability",
        "--loader:.woff2=dataurl", "--loader:.ttf=dataurl",
        "--outfile=" + str(module),
    ], check=True)
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_page()
        page.set_content("<main>Print text measurement</main>")
        page.add_script_tag(path=module)
        results = page.evaluate("""()=>{
          const m=PrintReadability;
          const figure=(content,root='')=>({width:200,height:100,svg:`<svg xmlns="http://www.w3.org/2000/svg" width="200" height="100" viewBox="0 0 200 100" ${root}><metadata>{"format":"hict-figure","version":1}</metadata>${content}</svg>`});
          const cases=[
            ['plain 12px', '<text font-size="12">Label</text>',12],
            ['inherited 14px', '<g font-size="14"><text>Label</text></g>',14],
            ['scaled and rotated', '<g transform="rotate(90) scale(2)"><text font-size="9">Label</text></g>',18],
            ['nested viewport', '<svg width="100" height="50" viewBox="0 0 200 100"><text font-size="12">Label</text></svg>',6],
            ['nested viewport inside a scaled outer group', '<g transform="scale(2)"><svg width="100" height="50" viewBox="0 0 200 100"><text font-size="12">Label</text></svg></g>',12],
            ['tspan override', '<text font-size="14">Label<tspan font-size="8">small</tspan></text>',8],
            ['no text', '<rect width="20" height="20"/>',null],
          ];
          const initial=document.body.childElementCount;
          if(m.smallestFigureTextPx(figure('<text>Root font</text>','font-size="13"'))!==13) throw Error('root font overwritten');
          return cases.map(([name,source,expected])=>{
            const actual=m.smallestFigureTextPx(figure(source));
            if(actual!==expected) throw Error(`${name}: ${actual} != ${expected}`);
            if(document.body.childElementCount!==initial) throw Error('measurement DOM leaked');
            if(actual!==null){
              const points=m.printedTextPoints(actual,200,178);
              if(Math.abs(points-actual*178/200/25.4*72)>1e-10) throw Error('incorrect physical points');
            }
            return {name,status:'passed',smallestTextPx:actual};
          });
        }""")
        (out / "results.json").write_text(json.dumps(results, indent=2))
        browser.close()
    print("Passed", len(results), "independent browser text/transform cases")


if __name__ == "__main__":
    main()
