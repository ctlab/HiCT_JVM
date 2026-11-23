#!/usr/bin/env python3
"""Qualify VCF spans and BigWig aggregation through real controls, edits and sessions.

Needs Python 3.12 with Playwright, NumPy and pyBigWig; fixtures use the supplied
JAR and a host JDK. Uses only fresh physical /tmp inputs and owned processes.
"""
import argparse
import base64
import functools
import gzip
import hashlib
import http.server
import json
import math
from pathlib import Path
import subprocess
import threading
import time

from playwright.sync_api import sync_playwright
from sequence_browser_harness import HictTestServer, BrowserCheck


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('jar', 'webui', 'data-dir', 'output'):
        parser.add_argument('--' + name, required=True)
    parser.add_argument('--signed', action='store_true', help='Qualify signed BigWig values and drawing')
    args = parser.parse_args()
    data, out = Path(args.data_dir).absolute(), Path(args.output).absolute()
    for path in (data, out):
        if path != path.resolve() or not path.is_relative_to('/tmp'):
            parser.error('Use fresh physical directories under /tmp')
    data.mkdir(parents=True, exist_ok=False)
    out.mkdir(parents=True, exist_ok=False)
    import pyBigWig
    with (out / 'fixture.log').open('w') as log:
        subprocess.run(['java', '--class-path', str(Path(args.jar).resolve()),
                        str(Path(__file__).resolve().parents[1] / 'src/test/java/ru/itmo/ctlab/hict/fixtures/ReinforcementHdf5Fixture.java'),
                        str(data)], check=True, stdout=log, stderr=subprocess.STDOUT)
        subprocess.run(['java', '--class-path', str(Path(args.jar).resolve()),
                        str(Path(__file__).with_name('BamTrackBrowserFixture.java')), str(data)],
                       check=True, stdout=log, stderr=subprocess.STDOUT)
    vcf = '''##fileformat=VCFv4.3
##contig=<ID=tiny,length=17>
##INFO=<ID=END,Number=1,Type=Integer,Description="Last reference position">
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
tiny\t3\tdeletion\tN\t<DEL>\t.\tPASS\tEND=14
tiny\t5\tlandmark\tA\tG\t.\tPASS\t.
'''
    (data / 'variants.vcf').write_text(vcf)
    with gzip.open(data / 'variants.vcf.gz', 'wt') as handle:
        handle.write(vcf)
    with pyBigWig.open(str(data / 'signal.bw'), 'w') as handle:
        handle.addHeader([('tiny', 17)])
        if args.signed:
            handle.addEntries(['tiny'] * 5, [0, 2, 3, 7, 10], ends=[1, 3, 5, 9, 11], values=[-7., -9., -4., 6., 0.])
        else:
            handle.addEntries(['tiny'] * 3, [0, 2, 3], ends=[1, 3, 5], values=[7.0, 9.0, 4.0])
    with pyBigWig.open(str(data / 'signal.bw')) as handle:
        independent = handle.values('tiny', 0, 17)
    oracle = [None if math.isnan(value) else value for value in independent]
    assert oracle == ([-7, None, -9, -4, -4, None, None, 6, 6, None, 0] + [None] * 6
                      if args.signed else [7, None, 9, 4, 4] + [None] * 12)
    hashes = {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
              for p in data.iterdir() if p.is_file()}

    class Quiet(http.server.SimpleHTTPRequestHandler):
        def log_message(self, *_):
            pass

    static = http.server.ThreadingHTTPServer(('127.0.0.1', 0),
        functools.partial(Quiet, directory=str(Path(args.webui).resolve())))
    threading.Thread(target=static.serve_forever, daemon=True).start()
    results = []
    try:
        for sequence, filename, resolution, vcf_name in (
            (False, 'tiny-fine.hict.hdf5', 1, 'variants.vcf'),
            (True, 'tiny-primary.hict.hdf5', 4, 'variants.vcf.gz')):
            case = out / ('sequence-grid' if sequence else 'native-grid')
            case.mkdir()
            with HictTestServer(args.jar, data, case / 'server.log') as app, sync_playwright() as pw:
                app.http('/open', {'filename': filename})
                app.http('/link_fasta', {'fastaFilename': 'tiny.fa'})
                browser = pw.chromium.launch(headless=True)
                page = browser.new_page(viewport={'width': 1600, 'height': 1100}, accept_downloads=True)
                page.set_default_timeout(30000)
                page.add_init_script('window.__HICT_BENCHMARK_ENABLED__=true')
                page.route('http://localhost:5000/**', lambda route: route.continue_(
                    url=app.url + route.request.url.removeprefix('http://localhost:5000')))
                check = BrowserCheck(page, case)
                page.goto(f'http://127.0.0.1:{static.server_port}/')
                check.menu('File', 'Join running session')
                check.settle()
                dialog = page.get_by_role('dialog', name='1D tracks', exact=True)

                def open_manager():
                    check.menu('View', '1D tracks…')
                    dialog.wait_for()

                def close_manager():
                    dialog.get_by_role('button', name='Close manager', exact=True).click()
                    dialog.wait_for(state='hidden')
                    check.settle()

                def add_file(name, display):
                    open_manager()
                    dialog.locator('#track-add-kind').select_option('file')
                    dialog.get_by_role('button', name='Browse…', exact=True).click()
                    picker = page.get_by_role('dialog', name='Select 1D track file', exact=True)
                    picker.wait_for()
                    picker.locator('.file-name-cell').get_by_text(name, exact=True).dblclick()
                    picker.wait_for(state='hidden')
                    dialog.locator('#track-display-name').fill(display)
                    with page.expect_response('**/tracks/open') as response:
                        dialog.get_by_role('button', name='Add file', exact=True).click()
                    assert response.value.ok, response.value.text()
                    dialog.get_by_role('group', name='Track ' + display, exact=True).wait_for()
                    close_manager()

                check.step('add-vcf-through-picker', lambda: add_file(vcf_name, 'Variants'))
                check.step('add-bigwig-through-picker', lambda: add_file('signal.bw', 'Signal'))
                check.step('add-indexed-bam-through-picker', lambda: add_file('reads.bam', 'Reads'))

                def query(start=0, end=17, width=17):
                    deadline = time.monotonic() + 30
                    while True:
                        payload = json.loads(app.http('/tracks/query_1d', {'unit': 'BP', 'startBP': start,
                            'endBP': end, 'widthPx': width, 'bpResolution': resolution, 'sequenceScale': sequence}))
                        tracks = {track['name']: track for track in payload['tracks']}
                        preparing = any((t.get('statusMessage') or '').startswith('Preparing') for t in tracks.values())
                        if not preparing:
                            return tracks
                        assert time.monotonic() < deadline, tracks
                        time.sleep(0.05)

                def values(track):
                    assert track['error'] is None and track['statusMessage'] is None, track
                    return [next((b['value'] for b in track['bins'] if b['startBp'] <= p < b['endBp']), None)
                            for p in range(17)]

                def verify_features(reversed=False):
                    track = query()['Variants']
                    for label, start, end in (('deletion', 2, 14), ('landmark', 4, 5)):
                        for p in range(17):
                            source = 16 - p if reversed else p
                            found = any(b['label'] == label and b['startBp'] <= p < b['endBp'] for b in track['bins'])
                            assert found == (start <= source < end), (label, p, reversed, track)

                def choose_mode(mode):
                    open_manager()
                    row = dialog.get_by_role('group', name='Track Signal', exact=True)
                    row.locator('summary').filter(has_text='Signal settings').click()
                    with page.expect_response('**/tracks/update') as response:
                        row.get_by_role('combobox', name='Aggregation for Signal', exact=True).select_option(mode)
                    assert response.value.ok, response.value.text()
                    close_manager()

                def choose_bam_mode(mode):
                    open_manager()
                    row = dialog.get_by_role('group', name='Track Reads', exact=True)
                    row.locator('summary').filter(has_text='Signal settings').click()
                    with page.expect_response('**/tracks/update') as response:
                        row.get_by_role('combobox', name='Rendering for Reads', exact=True).select_option(mode)
                    assert response.value.ok, response.value.text()
                    close_manager()

                depth = [1, 2, 1, 0, 0, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 0, 1]
                centers = [0] * 17
                for position in (1, 4, 11, 16):
                    centers[position] = 1

                def verify_bam(expected, reverse=False):
                    actual = [0 if v is None else v for v in values(query()['Reads'])]
                    assert actual == (expected[::-1] if reverse else expected), actual

                check.step('bam-aligned-depth-excludes-cigar-gaps', lambda: verify_bam(depth))
                choose_bam_mode('READ_DENSITY')
                check.step('bam-original-source-centers', lambda: verify_bam(centers))
                choose_bam_mode('COVERAGE')

                aggregates = {}
                for mode, expected in (('MAX', -4), ('MEAN', -6), ('SUM', -4.8)) if args.signed else (('MAX', 9), ('MEAN', 6), ('SUM', 4.8)):
                    if mode != 'MAX':
                        choose_mode(mode)
                    verify_features()
                    assert values(query()['Signal']) == oracle
                    coarse = query(0, 5, 1)['Signal']
                    bins = coarse['bins']
                    assert bins and all(abs(b['value'] - expected) < 1e-9 for b in bins), (mode, coarse)
                    aggregates[mode] = coarse
                check.step('independent-bigwig-all-modes', verify_features)

                def capture_signal_controls():
                    open_manager()
                    for name in ('Signal', 'Reads'):
                        dialog.get_by_role('group', name='Track ' + name, exact=True).locator('summary').filter(
                            has_text='Signal settings').click()
                    descriptions = {}
                    for width, height in ((1600, 1100), (640, 900), (375, 812)):
                        page.set_viewport_size({'width': width, 'height': height})
                        for label in ('Aggregation for Signal', 'Rendering for Reads'):
                            select = dialog.get_by_role('combobox', name=label, exact=True)
                            select.scroll_into_view_if_needed()
                            descriptions[label] = select.locator('option').all_text_contents()
                            bounds = select.bounding_box()
                            assert bounds and bounds['x'] >= 0 and bounds['x'] + bounds['width'] <= width, bounds
                            metrics = select.evaluate('''select=>{
                                const style=getComputedStyle(select),ctx=document.createElement('canvas').getContext('2d');
                                ctx.font=style.font||`${style.fontSize} ${style.fontFamily}`;
                                return {available:select.clientWidth-parseFloat(style.paddingLeft)-parseFloat(style.paddingRight),
                                    required:Math.max(...Array.from(select.options,o=>ctx.measureText(o.label).width))};
                            }''')
                            assert metrics['required'] <= metrics['available'] + .5, (width, label, metrics)
                        page.screenshot(path=str(case / f'signal-mode-labels-{width}.png'))
                    close_manager()
                    page.set_viewport_size({'width': 1600, 'height': 1100})
                    check.settle()
                    return descriptions

                mode_descriptions = capture_signal_controls()

                def focus(bp):
                    page.evaluate('''bp=>window.__hictBenchmark.manager().viewAndLayersManager.restoreViewPosition(
                        {centerBp:[bp,bp],resolution:0.01,rotation:0})''', bp)
                    check.settle()

                def feature_menu(label, edge):
                    focus(8 if label == 'deletion' else 4.5)
                    page.wait_for_function('''label=>window.__hictBenchmark.manager().linearTrackManager.lastRenderedSnapshot.horizontal?.featureHits.some(h=>h.bin.label===label)''', arg=label)
                    point = page.evaluate('''label=>{const s=window.__hictBenchmark.manager().linearTrackManager.lastRenderedSnapshot.horizontal;
                        const h=s.featureHits.find(h=>h.bin.label===label),r=document.querySelector('#horizontal-igv-track-div').getBoundingClientRect();
                        return [r.left+(h.axisStart+h.axisEnd)/2,r.top+(h.crossStart+h.crossEnd)/2]}''', label)
                    page.mouse.click(*point, button='right')
                    menu = page.get_by_role('menu', name='Variants', exact=True)
                    with page.expect_response('**/split_contig_at_bp') as response:
                        menu.get_by_role('menuitem', name='Cut at feature ' + edge, exact=True).click()
                    assert response.value.ok, response.value.text()
                    check.settle()

                check.step('cut-vcf-structural-end', lambda: feature_menu('deletion', 'end'))
                check.step('cut-vcf-landmark-start', lambda: feature_menu('landmark', 'start'))
                verify_features()
                assert values(query()['Signal']) == oracle
                verify_bam(depth)

                page.evaluate('window.__hictBenchmark.manager().viewAndLayersManager.selectEntityRangeByBp(0,17)')
                with page.expect_response('**/reverse_selection_range') as response:
                    page.get_by_role('button', name='Reverse Selection', exact=True).click()
                assert response.value.ok
                check.settle()
                verify_features(True)
                assert values(query()['Signal']) == oracle[::-1]
                verify_bam(depth, True)
                choose_bam_mode('READ_DENSITY')
                verify_bam(centers, True)
                check.step('reversed-source-values-and-vcf-spans', lambda: verify_features(True))

                with page.expect_download() as download:
                    check.menu('File', 'Save session…')
                session = case / 'tracks-session.json'
                download.value.save_as(session)
                check.menu('File', 'Close map')
                with page.expect_file_chooser() as chooser:
                    check.menu('File', 'Open session…')
                chooser.value.set_files(session)
                page.get_by_text('Session restored. Undo history starts from the restored assembly.', exact=True).last.wait_for()
                check.settle()
                verify_features(True)
                assert values(query()['Signal']) == oracle[::-1]
                definitions = json.loads(app.http('/tracks/list', {}))
                assert next(t for t in definitions if t['name'] == 'Signal')['aggregationMode'] == 'SUM'
                assert next(t for t in definitions if t['name'] == 'Reads')['renderMode'] == 'READ_DENSITY'
                verify_bam(centers, True)
                check.step('restored-vcf-bigwig-session', lambda: verify_features(True))
                coordinates = page.evaluate('''()=>{
                    const m=window.__hictBenchmark.manager(),h=m.getContigDimensionHolder();
                    const r=m.viewAndLayersManager.currentViewState.resolutionDesciptor.bpResolution;
                    return {sequenceGrid:h.usesSequenceCoordinates,resolution:r,centers:Array.from({length:17},(_,i)=>{
                        const bp=i+.5,px=h.getFractionalPxOfBp(bp,r);
                        return {bp,px,roundTrip:h.getFractionalBpOfPx(px,r)};
                    })};
                }''')
                for point in coordinates['centers']:
                    assert abs(point['roundTrip'] - point['bp']) < 1e-9, coordinates
                    if coordinates['sequenceGrid']:
                        assert abs(point['px'] - point['bp']) < 1e-9, coordinates
                if args.signed:
                    drawings = []
                    for log in (False, True):
                        open_manager()
                        row = dialog.get_by_role('group', name='Track Signal', exact=True)
                        row.locator('summary').filter(has_text='Signal settings').click()
                        row.get_by_role('checkbox', name='log', exact=True).set_checked(log)
                        close_manager()
                        drawing = page.evaluate('''({track,log})=>{
                            const linear=window.__hictBenchmark.manager().linearTrackManager;
                            const summary=linear.tracks.find(t=>t.trackId===track.trackId);
                            if(summary.logScale!==log) throw Error('Log checkbox did not persist');
                            const result=[];
                            for(const orientation of ['horizontal','vertical']) {
                                const canvas=document.createElement('canvas'),scale=24,cross=220;
                                canvas.width=orientation==='horizontal'?17*scale:cross;
                                canvas.height=orientation==='horizontal'?cross:17*scale;
                                const ctx=canvas.getContext('2d'),fills=[],original=ctx.fillRect.bind(ctx),color='#9b27c5';
                                ctx.fillRect=(x,y,w,h)=>{if(ctx.fillStyle===color)fills.push({x,y,w,h});original(x,y,w,h)};
                                const viewport={startPx:0,endPx:17,startBp:0,endBp:17,bpResolution:1,
                                    visibleWidthPx:17*scale,scopeStartPx:0,scopeEndPx:17,pxToScreen:p=>p*scale};
                                linear.drawTrackCanvas(canvas,orientation,{tracks:[{...track,color}]},viewport,undefined,false);
                                const range=linear.resolveSignalRange(summary,track.bins,viewport,1);
                                const transform=linear.buildScaleTransform(range.min,range.max,log,10);
                                // The plot reserves a 16 px title strip and a 2 px inset on both edges.
                                result.push({orientation,fills,zero:cross-2-(cross-20)*transform.normalize(0),range,
                                    png:canvas.toDataURL('image/png')});
                            }
                            return result;
                        }''', {'track': query()['Signal'], 'log': log})
                        for axis in drawing:
                            assert axis['range'] == {'min': -9, 'max': 6}, axis
                            for position, value in enumerate(oracle[::-1]):
                                def covers(rect):
                                    start, size = (rect['x'], rect['w']) if axis['orientation'] == 'horizontal' else (rect['y'], rect['h'])
                                    return start <= (position + .5) * 24 < start + size
                                bars = [r for r in axis['fills'] if covers(r)]
                                if value is None or value == 0:
                                    assert not bars, (position, value, bars)
                                    continue
                                assert len(bars) == 1, (position, value, bars)
                                rect = bars[0]
                                start, size = (rect['y'], rect['h']) if axis['orientation'] == 'horizontal' else (rect['x'], rect['w'])
                                assert start >= axis['zero'] - 1e-9 if value < 0 else start + size <= axis['zero'] + 1e-9, (value, axis)
                            png = axis.pop('png').split(',', 1)[1]
                            (case / f"signed-{'log' if log else 'linear'}-{axis['orientation']}.png").write_bytes(base64.b64decode(png))
                        drawings.append({'log': log, 'axes': drawing})
                        check.step('signed-' + ('log' if log else 'linear') + '-both-axis-bars', lambda: None)
                        page.screenshot(path=str(case / f"signed-{'log' if log else 'linear'}-restored.png"))
                    (case / 'signed-drawing-results.json').write_text(json.dumps(drawings, indent=2))
                assert not check.errors, check.errors
                results.append({'grid': 'sequence' if sequence else 'native', 'checks': check.rows,
                    'aggregates': aggregates, 'coordinates': coordinates, 'modeDescriptions': mode_descriptions,
                    'pageErrors': check.errors})
                browser.close()
        for name, digest in hashes.items():
            assert hashlib.sha256((data / name).read_bytes()).hexdigest() == digest, name
        (out / 'results.json').write_text(json.dumps({'status': 'passed', 'cases': results,
            'independentReader': 'pyBigWig ' + pyBigWig.__version__, 'signed': args.signed, 'sourceValues': oracle,
            'sourceHashesUnchanged': hashes}, indent=2))
    finally:
        static.shutdown()
        static.server_close()


if __name__ == '__main__':
    main()
