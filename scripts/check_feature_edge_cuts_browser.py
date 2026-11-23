#!/usr/bin/env python3
"""Exercise exact GTF/GFF feature-edge cuts through both real track context menus."""
import argparse
import functools
import hashlib
import http.server
import json
from pathlib import Path
import subprocess
import threading

from playwright.sync_api import sync_playwright
from sequence_browser_harness import HictTestServer, BrowserCheck


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ('jar', 'webui', 'data-dir', 'output'):
        parser.add_argument('--'+key, required=True)
    args = parser.parse_args()
    data, output = Path(args.data_dir).absolute(), Path(args.output).absolute()
    if any(p != p.resolve() or not p.is_relative_to('/tmp') for p in (data, output)):
        parser.error('Use fresh nonsymlinked disposable directories under /tmp')
    data.mkdir(parents=True, exist_ok=False)
    output.mkdir(parents=True, exist_ok=False)
    with (output/'fixture.log').open('w') as log:
        subprocess.run(['java', '--class-path', str(Path(args.jar).resolve()),
                        str(Path(__file__).with_name('SequenceViewBrowserFixture.java')), str(data)],
                       check=True, stdout=log, stderr=subprocess.STDOUT)
    (data/'genes.gtf').write_text('tiny\tfixture\tgene\t4\t6\t.\t+\t.\tgene_id "marker"; gene_name "Marker";\n'
        'tiny\tfixture\tgene\t1034\t1066\t.\t+\t.\tgene_id "anchor"; gene_name "Anchor";\n')
    (data/'bases.gff3').write_text('##gff-version 3\ntiny\tfixture\tgene\t1\t1\t.\t+\t.\tID=one;Name=OneBase\n')
    native = data/'sequence.hict.hdf5'
    digest = hashlib.sha256(native.read_bytes()).hexdigest()
    original = ''.join((data/'sequence.fa').read_text().splitlines()[1:])
    reverse = original.translate(str.maketrans('ACGTRYMKSWBDHVN', 'TGCAYRKMSWVHDBN'))[::-1]
    class Quiet(http.server.SimpleHTTPRequestHandler):
        def log_message(self, *_): pass
    static = http.server.ThreadingHTTPServer(('127.0.0.1', 0),
        functools.partial(Quiet, directory=str(Path(args.webui).resolve())))
    threading.Thread(target=static.serve_forever, daemon=True).start()
    try:
        with HictTestServer(args.jar, data, output/'server.log') as app, sync_playwright() as pw:
            app.http('/open', {'filename': native.name})
            app.http('/link_fasta', {'fastaFilename': 'sequence.fa'})
            browser = pw.chromium.launch(headless=True)
            page = browser.new_page(viewport={'width': 1600, 'height': 1100}, accept_downloads=True)
            page.context.grant_permissions(['clipboard-read', 'clipboard-write'])
            page.set_default_timeout(30000)
            page.add_init_script('window.__HICT_BENCHMARK_ENABLED__=true')
            page.route('http://localhost:5000/**', lambda route: route.continue_(
                url=app.url+route.request.url.removeprefix('http://localhost:5000')))
            check = BrowserCheck(page, output)
            requests = []
            view_positions = []
            page.on('request', lambda request: requests.append(request.post_data_json)
                    if request.url.endswith('/split_contig_at_bp') else None)
            page.goto(f'http://127.0.0.1:{static.server_port}/')
            check.menu('File', 'Join running session'); check.settle()
            def lengths():
                return page.evaluate('window.__hictBenchmark.manager().contigDimensionHolder.contigDescriptors.map(d=>d.contigLengthBp)')
            def focus(bp, resolution=1/2000):
                page.evaluate('''p=>window.__hictBenchmark.manager().viewAndLayersManager.restoreViewPosition({centerBp:[p[0],p[0]],resolution:p[1],rotation:0})''', [bp, resolution])
                check.settle()
            def sequence(expected):
                actual = json.loads(app.http('/sequence/region', {'startBp': 0, 'endBp': 4000}))['sequence']
                assert actual == expected
            def feature_menu(axis, track, start, end):
                page.mouse.move(800, 80)
                page.wait_for_function('''p=>{
                    const t=window.__hictBenchmark.manager().linearTrackManager,s=t.lastRenderedSnapshot[p.axis];
                    const id=t.tracks.find(t=>t.name===p.track)?.trackId;
                    return s?.featureHits.some(h=>h.trackId===id&&h.bin.startBp===p.start&&h.bin.endBp===p.end);
                }''', arg={'axis': axis, 'track': track, 'start': start, 'end': end})
                info = page.evaluate('''p=>{const t=window.__hictBenchmark.manager().linearTrackManager;
                    const id=t.tracks.find(t=>t.name===p.track).trackId,s=t.lastRenderedSnapshot[p.axis];
                    const h=s.featureHits.find(h=>h.trackId===id&&h.bin.startBp===p.start&&h.bin.endBp===p.end);
                    const r=document.querySelector('#'+p.axis+'-igv-track-div').getBoundingClientRect();
                    const along=(h.axisStart+h.axisEnd)/2,cross=(h.crossStart+h.crossEnd)/2;
                    return {point:p.axis==='horizontal'?[r.left+along,r.top+cross]:[r.left+cross,r.top+along],
                        label:h.bin.label,strand:h.bin.strand,start:h.bin.startBp,end:h.bin.endBp};
                }''', {'axis': axis, 'track': track, 'start': start, 'end': end})
                page.mouse.click(*info['point'], button='right')
                menu = page.get_by_role('menu', name=track, exact=True)
                menu.wait_for()
                return menu, info
            def cut(menu, edge, expected_offset, expected_lengths):
                before = page.evaluate('window.__hictBenchmark.manager().viewAndLayersManager.captureViewPosition()')
                with page.expect_response('**/split_contig_at_bp') as response:
                    menu.get_by_role('menuitem', name='Cut at feature '+edge, exact=True).click()
                assert response.value.ok, response.value.text()
                body = response.value.request.post_data_json
                assert body['offsetBp'] == expected_offset, body
                page.wait_for_function('expected=>JSON.stringify(window.__hictBenchmark.manager().contigDimensionHolder.contigDescriptors.map(d=>d.contigLengthBp))===JSON.stringify(expected)', arg=expected_lengths)
                check.settle()
                assert lengths() == expected_lengths
                after = page.evaluate('window.__hictBenchmark.manager().viewAndLayersManager.captureViewPosition()')
                assert before['displayGrid'] == after['displayGrid'], (before, after)
                assert before['resolution'] == after['resolution'], (before, after)
                assert max(abs(a-b) for a,b in zip(before['centerBp'], after['centerBp'])) < 1e-6, (before, after)
                view_positions.append({'before': before, 'after': after})
            page.evaluate('''async()=>{const t=window.__hictBenchmark.manager().linearTrackManager;await t.openTrack('genes.gtf','GTF genes');}''')
            focus(4.5, .006)
            menu, forward = feature_menu('horizontal', 'GTF genes', 3, 6)
            assert forward['strand'] == '+', forward
            cut(menu, 'start', 3, [3, 3997])
            focus(1050, .006)
            menu, _ = feature_menu('horizontal', 'GTF genes', 1033, 1066)
            menu.get_by_role('menuitem', name='Copy coordinates', exact=True).click()
            page.get_by_text('Coordinates copied', exact=True).last.wait_for()
            copied = page.evaluate('navigator.clipboard.readText()')
            assert copied == 'tiny_3_4000:1031-1063', copied
            menu, _ = feature_menu('horizontal', 'GTF genes', 1033, 1066)
            menu.get_by_role('menuitem', name='Center on feature', exact=True).click(); check.settle()
            centered = page.evaluate('window.__hictBenchmark.manager().viewAndLayersManager.captureViewPosition()')
            assert centered['resolution'] == .006 and centered['displayGrid'] == 'NATIVE', centered
            assert max(abs(bp-1049.5) for bp in centered['centerBp']) < 1e-6, centered
            menu, _ = feature_menu('horizontal', 'GTF genes', 1033, 1066)
            menu.get_by_role('menuitem', name='Zoom to feature', exact=True).click(); check.settle()
            zoomed = page.evaluate('window.__hictBenchmark.manager().viewAndLayersManager.captureViewPosition()')
            assert zoomed['resolution'] < .006 and zoomed['displayGrid'] == 'SEQUENCE_BP', zoomed
            assert max(abs(bp-1049.5) for bp in zoomed['centerBp']) < 1e-6, zoomed
            page.locator('canvas.sequence-axis.horizontal').wait_for(state='visible')
            page.screenshot(path=str(output/'zoom-to-feature-after-cut.png'))
            focus(4.5)
            menu, _ = feature_menu('horizontal', 'GTF genes', 3, 6)
            cut(menu, 'end', 3, [3, 3, 3994])
            sequence(original)
            menu, _ = feature_menu('horizontal', 'GTF genes', 3, 6)
            assert menu.get_by_role('menuitem', name='Cut at feature start', exact=True).is_disabled()
            assert menu.get_by_role('menuitem', name='Cut at feature end', exact=True).is_disabled()
            page.screenshot(path=str(output/'existing-feature-boundaries.png'))
            page.keyboard.press('Escape')
            for _ in range(2):
                page.keyboard.press('Control+z'); check.settle()
            assert lengths() == [4000], lengths()
            page.evaluate('window.__hictBenchmark.manager().viewAndLayersManager.fitViewToActiveExtent()'); check.settle()
            page.evaluate('''()=>{const m=window.__hictBenchmark.manager(),h=m.contigDimensionHolder;
                m.viewAndLayersManager.selectEntityRangeByBp(0,h.prefix_sum_bp[h.contig_count]);}''')
            with page.expect_response('**/reverse_selection_range') as response:
                page.get_by_role('button', name='Reverse Selection', exact=True).click()
            assert response.value.ok
            check.settle(); focus(3995.5)
            menu, reversed_feature = feature_menu('vertical', 'GTF genes', 3994, 3997)
            assert reversed_feature['strand'] == '-', reversed_feature
            cut(menu, 'start', 3994, [3994, 6])
            menu, _ = feature_menu('vertical', 'GTF genes', 3994, 3997)
            cut(menu, 'end', 3, [3994, 3, 3])
            sequence(reverse)
            # A delayed action must retain its opening revision across undo.
            stale = page.evaluate('window.__hictBenchmark.manager().eventManager.getExactCutTarget(3999)')
            page.keyboard.press('Control+z'); check.settle()
            sent = len(requests)
            assert page.evaluate('target=>window.__hictBenchmark.manager().eventManager.cutAtExactTarget(target)', stale) is False
            assert len(requests) == sent
            assert lengths() == [3994, 6]
            page.keyboard.press('Control+y'); check.settle()
            assert lengths() == [3994, 3, 3]
            page.evaluate('''async()=>{const t=window.__hictBenchmark.manager().linearTrackManager;await t.openTrack('bases.gff3','GFF bases');}''')
            focus(3999.5)
            menu, one_base = feature_menu('horizontal', 'GFF bases', 3999, 4000)
            assert menu.get_by_role('menuitem', name='Cut at feature end', exact=True).is_disabled()
            cut(menu, 'start', 2, [3994, 3, 2, 1])
            sequence(reverse)
            page.screenshot(path=str(output/'one-base-gff-cut.png'))
            with page.expect_download() as saved:
                check.menu('File', 'Save session…')
            session = output/'feature-cuts-session.json'; saved.value.save_as(session)
            check.menu('File', 'Close map')
            with page.expect_file_chooser() as chooser:
                check.menu('File', 'Open session…')
            chooser.value.set_files(session)
            page.get_by_text('Session restored. Undo history starts from the restored assembly.', exact=True).last.wait_for()
            check.settle()
            assert lengths() == [3994, 3, 2, 1], lengths()
            sequence(reverse)
            menu, _ = feature_menu('horizontal', 'GFF bases', 3999, 4000)
            assert menu.get_by_role('menuitem', name='Cut at feature start', exact=True).is_disabled()
            assert menu.get_by_role('menuitem', name='Cut at feature end', exact=True).is_disabled()
            page.screenshot(path=str(output/'restored-feature-cuts.png'))
            assert hashlib.sha256(native.read_bytes()).hexdigest() == digest
            assert not check.errors, check.errors
            (output/'results.json').write_text(json.dumps({'status': 'ok', 'requests': requests,
                'forwardGtf': forward, 'reversedGtf': reversed_feature, 'oneBaseGff': one_base,
                'lengths': lengths(), 'viewPositions': view_positions, 'centeredFeature': centered, 'zoomedFeature': zoomed,
                'copiedCoordinates': copied, 'sourceSha256': digest, 'pageErrors': check.errors,
                'checks': ['horizontal GTF start/end', 'zero-based half-open edges', 'existing joins disabled',
                    'keyboard undo/redo', 'reversed vertical GTF start/end', 'stale action rejected without request',
                    'one-base GFF3 feature', 'terminal assembly edge disabled', 'all IUPAC bases retained',
                    'session/feature restore', 'native/base BP view preserved after every cut',
                    'Center keeps native scale', 'Zoom reaches letters at exact feature after cut',
                    'copy coordinates uses ungrouped 1-based inclusive digits', 'native bytes unchanged']}, indent=2))
            browser.close()
    finally:
        static.shutdown(); static.server_close()


if __name__ == '__main__':
    main()
