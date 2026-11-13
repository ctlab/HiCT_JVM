#!/usr/bin/env python3
"""Sequence-search DOM test with deterministic FASTA and retained full diagnostics."""
import argparse, functools, http.server, json, os, random, shutil, sys, threading, subprocess
from pathlib import Path
import time
from playwright.sync_api import sync_playwright
from sequence_browser_harness import HictTestServer, BrowserCheck


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--suite', help='Optional legacy benchmark directory for its font bundle')
    parser.add_argument('--jar', required=True)
    parser.add_argument('--webui', required=True)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    suite = Path(args.suite).resolve() if args.suite else Path(__file__).resolve().parents[1]
    out = Path(args.output).resolve(); out.mkdir(parents=True, exist_ok=False)
    data = out / 'data'; data.mkdir()
    # Multiple resolutions are essential: generated sidecars may contain a subset.
    import cooler, numpy as np, pandas as pd
    mcool = data / 'known.mcool'
    for resolution in (100, 200):
        n = 4000 // resolution
        bins = pd.DataFrame(dict(chrom=['assembly'] * n, start=np.arange(n) * resolution, end=np.arange(1, n+1) * resolution))
        pixels = pd.DataFrame([(i, j, (i+1)*43+j+1) for i in range(n) for j in range(i, n)], columns=['bin1_id','bin2_id','count'])
        cooler.create_cooler(str(mcool) + f'::resolutions/{resolution}', bins, pixels, ordered=True, mode='a')
    converter = out / 'OverlayFixture.java'
    converter.write_text("""import java.nio.file.Path; import java.util.List;
import ru.itmo.ctlab.hict.hict_library.converters.*;
class OverlayFixture { public static void main(String[] a) {
 new McoolToHictConverter().convert(new ConversionOptions(Path.of(a[0]), Path.of(a[1]), List.of(100L,200L),8192,1,
 ConversionOptions.CompressionAlgorithm.DEFLATE,"",false,4,true,false,false,false,ConversionOptions.ExportMode.INTERNAL),System.err::println);
}}""")
    with (out/'fixture-conversion.log').open('w') as log:
        subprocess.run(['java','--class-path',str(Path(args.jar).resolve()),str(converter),str(mcool),str(data/'known.hict.hdf5')], check=True, stdout=log, stderr=subprocess.STDOUT)
    rng = random.Random(721)
    sequence = ''.join(rng.choice('ACGT') for _ in range(4000))
    (data / 'known.fasta').write_text('>assembly\n' + sequence + '\n')
    (data / 'split.agp').write_text('s1 1 2000 1 W assembly 1 2000 +\ns2 1 2000 1 W assembly 2001 4000 -\n')
    reverse = lambda s: s.translate(str.maketrans('ACGT', 'TGCA'))[::-1]
    motif = sequence[1990:2000] + reverse(sequence[3990:4000])
    search_track_id = None
    class Quiet(http.server.SimpleHTTPRequestHandler):
        def log_message(self, *_): pass
    static = http.server.ThreadingHTTPServer(('127.0.0.1', 0), functools.partial(Quiet, directory=str(Path(args.webui).resolve())))
    threading.Thread(target=static.serve_forever, daemon=True).start()
    font = out / 'fonts.conf'
    fonts = Path(os.environ.get('HICT_TEST_FONTS', str(suite / 'runtime/fonts/dejavu')))
    if not fonts.is_dir(): fonts = Path('/usr/share/fonts/truetype/dejavu')
    font.write_text(f'<fontconfig><dir>{fonts}</dir><cachedir>{out}/font-cache</cachedir></fontconfig>')
    with HictTestServer(args.jar, data, out / 'server.log') as app, sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True, env={**os.environ, 'FONTCONFIG_FILE': str(font)})
        page = browser.new_page(viewport=dict(width=1440, height=1000)); page.set_default_timeout(60000)
        page.add_init_script('window.__HICT_BENCHMARK_ENABLED__ = true')
        page.route('http://localhost:5000/**', lambda route: route.continue_(url=app.url + route.request.url.removeprefix('http://localhost:5000')))
        check = BrowserCheck(page, out); success = False
        def load_file(menu, action, name, endpoint):
            check.menu(menu, action); check.choose_file(name)
            with page.expect_response(lambda r: endpoint in r.url) as response:
                page.get_by_role('button', name='Load', exact=True).click()
            assert response.value.status == 200, response.value.text()
            page.get_by_placeholder('Filter current folder by file name or path').wait_for(state='hidden')
        def missing_fasta():
            settings = json.loads(app.http('/sequence/settings', {'checkTools': True}))
            assert settings['mapOpen'] and not settings['fastaLinked'], settings
            assert all(settings['tools'][name]['available'] for name in ('seqkit','blastn','dotplot','moddotplot','hict-moddotplot')), settings
            for action, title, button in (
                ('Generate ModDotPlot…', 'Generate ModDotPlot', 'Generate ModDotPlot'),
                ('Generate dotplot from active assembly…', 'Generate dotplot from active assembly', 'Generate dotplot from active assembly'),
                ('Find sequences in linked FASTA…', 'Find sequences in the linked FASTA', 'Find sequences')):
                page.get_by_role('link', name='Tools', exact=True).click()
                page.get_by_role('button', name=action, exact=True).click()
                dialog = page.get_by_role('dialog', name=title, exact=True)
                dialog.get_by_text('Link the FASTA for the active map', exact=False).wait_for()
                assert dialog.get_by_role('button', name=button, exact=True).is_disabled()
                dialog.get_by_role('button', name='Close', exact=True).last.click()
            # Exercise the server guard independently, keeping expected 409
            # responses out of the browser's unexpected-error audit.
            import urllib.request, urllib.error
            request = urllib.request.Request(app.url + '/sequence/moddotplot/jobs', data=json.dumps({'scope':'assembly','resolutions':[100],'mergeCompatible':True}).encode(), headers={'Content-Type':'application/json'})
            try: urllib.request.urlopen(request); raise AssertionError('Missing FASTA accepted')
            except urllib.error.HTTPError as failure:
                payload = json.loads(failure.read())
                assert failure.code == 409 and 'Link the FASTA' in payload['error'], payload
                (out/'missing-fasta-response.json').write_text(json.dumps(payload,indent=2))
        def missing_tools():
            # Stub only the readiness response to exercise incomplete builds;
            # all successful computations below still use the actual JAR tools.
            def unavailable(route):
                state = json.loads(app.http('/sequence/settings', route.request.post_data_json or {}))
                state['tools'] = {name:dict(available=False,reason='This installation is missing '+name+'. Install the complete HiCT package.') for name in ('seqkit','blastn','dotplot','moddotplot','hict-moddotplot')}
                route.fulfill(json=state)
            page.route('**/sequence/settings', unavailable)
            try:
                page.get_by_role('link', name='Tools', exact=True).click()
                page.get_by_role('button', name='Find sequences in linked FASTA…', exact=True).click()
                dialog=page.get_by_role('dialog',name='Find sequences in the linked FASTA',exact=True)
                dialog.get_by_text('This installation is missing seqkit.',exact=False).wait_for()
                assert dialog.get_by_role('button',name='Find sequences',exact=True).is_disabled()
                assert dialog.locator('#search-mode option:disabled').count()==3
                dialog.get_by_role('button',name='Close',exact=True).last.click()
            finally: page.unroute('**/sequence/settings', unavailable)
        def search():
            nonlocal search_track_id
            page.get_by_role('link', name='Tools', exact=True).click()
            page.get_by_role('button', name='Find sequences in linked FASTA…', exact=True).click()
            page.get_by_label('FASTA records, or one DNA sequence per line').fill('>cross_join α\n' + motif)
            assert page.get_by_label('Search scope').input_value() == 'assembly'
            assert page.get_by_label('Matching', exact=True).input_value() == 'exact'
            check.advanced()
            assert page.get_by_label('Forward strand color').input_value() == '#dc2626'
            assert page.get_by_label('Reverse strand color').input_value() == '#2563eb'
            page.get_by_label('Forward strand color').fill('#12ab34')
            page.get_by_label('Reverse strand color').fill('#3456ef')
            page.get_by_label('Save reusable BED under DATA_DIR').check()
            page.get_by_label('BED output path').fill('published/search-results.bed')
            page.get_by_label('Default gap length (Ns)', exact=True).fill('0')
            with page.expect_response(lambda r: '/sequence/search/jobs' in r.url) as response:
                page.get_by_role('button', name='Find sequences', exact=True).click()
            assert response.value.status == 202, response.value.text()
            request = response.value.request.post_data_json
            assert request['forwardColor'] == '#12ab34' and request['reverseColor'] == '#3456ef', request
            assert request['outputFilename'] == 'published/search-results.bed', request
            page.get_by_role('dialog', name='Find sequences in the linked FASTA', exact=True).wait_for(state='hidden')
            check.settle()
            jobs = json.loads(app.http('/sequence/jobs/list', {}))
            assert len(jobs) == 1 and jobs[0]['status'] == 'finished', jobs
            assert jobs[0]['hits'] == 1 and jobs[0]['fragments'] == 2, jobs
            tracks = json.loads(app.http('/tracks/list', {}))
            assert any('Sequence search (1 matches)' == t['name'] for t in tracks), tracks
            search_track_id = next(t['trackId'] for t in tracks if t['name'] == 'Sequence search (1 matches)')
            published = data / 'published/search-results.bed'
            assert published.is_file(), published
            bed = published.read_text()
            assert '\t18,171,52\t' in bed and '\t52,86,239\t' in bed, bed
            (out / 'jobs.json').write_text(json.dumps(jobs, indent=2))
            (out / 'tracks.json').write_text(json.dumps(tracks, indent=2))
        def track_axis_visibility():
            assert search_track_id
            name = 'Sequence search (1 matches)'
            page.get_by_role('button', name=f'Hide {name} horizontally', exact=True).click()
            hidden = page.evaluate("trackId => window.__hictBenchmark.manager().linearTrackManager.isTrackVisibleOnAxis(trackId, 'horizontal')", search_track_id)
            assert not hidden
            check.menu('View', 'Tracks and layers…')
            dialog = page.get_by_role('dialog').filter(has=page.get_by_role('heading', name='Tracks and layers', exact=True))
            dialog.get_by_role('button', name=f'Show {name} horizontally', exact=True).click()
            restored = page.evaluate("trackId => window.__hictBenchmark.manager().linearTrackManager.isTrackVisibleOnAxis(trackId, 'horizontal')", search_track_id)
            assert restored
            dialog.get_by_role('button', name='Close', exact=True).click()
            dialog.wait_for(state='hidden')
        def moddotplot():
            import h5py
            versions = []
            for iteration in range(2):
                page.get_by_role('link', name='Tools', exact=True).click()
                page.get_by_role('button', name='Generate ModDotPlot…', exact=True).click()
                dialog = page.get_by_role('dialog', name='Generate ModDotPlot', exact=True)
                check.advanced()
                assert page.get_by_label('Analysis scope').input_value() == 'assembly'
                [checkbox.uncheck() for checkbox in dialog.locator('.resolution-options input').all()]
                assert dialog.get_by_role('button', name='Generate ModDotPlot', exact=True).is_disabled()
                dialog.locator('.resolution-options input[value="100"]').check()
                assert page.get_by_label('Worker threads').input_value() in ('-1', '2')
                page.get_by_label('Worker threads').fill('2')
                page.get_by_label('k-mer length', exact=True).fill('15')
                page.get_by_label('Target sketch size', exact=True).fill('128')
                delta = page.get_by_label('Neighbor expansion (fraction of window)', exact=True)
                delta.fill('0.25')
                assert delta.evaluate('element=>element.validity.stepMismatch')
                before_invalid = json.loads(app.http('/sequence/jobs/list', {}))
                dialog.get_by_role('button', name='Generate ModDotPlot', exact=True).click()
                assert json.loads(app.http('/sequence/jobs/list', {})) == before_invalid
                delta.fill('0.2')
                assert delta.evaluate('element=>element.checkValidity()')
                if iteration == 0:
                    page.get_by_label('Save a reusable copy under DATA_DIR').check()
                    page.get_by_label('Output path').fill('published/moddotplot.hict.hdf5')
                page.get_by_label('Display completed result').select_option('overlay' if iteration else 'below')
                assert page.get_by_label('Accumulate compatible regions; replace scores within this rectangle').is_checked()
                if iteration == 0:
                    page.screenshot(path=str(out / 'moddotplot-dialog.png'))
                def review_once(route):
                    page.unroute('**/secondary/open', review_once)
                    status = json.loads(app.http('/secondary/status', {}))
                    status.update(requiresConfirmation=True, warnings=['Test: review a requested overlay before replacing the current layer.'])
                    route.fulfill(json=status)
                if iteration:
                    page.route('**/secondary/open', review_once)
                with page.expect_response(lambda r: '/sequence/moddotplot/jobs' in r.url) as response:
                    dialog.get_by_role('button', name='Generate ModDotPlot', exact=True).click()
                assert response.value.status == 202, response.value.text()
                if iteration:
                    dialog.get_by_text('Review overlay compatibility', exact=True).wait_for()
                    page.screenshot(path=str(out/'inline-overlay-confirmation.png'))
                    dialog.get_by_role('button', name='Attach despite differences', exact=True).click()
                dialog.wait_for(state='hidden')
                check.settle()
                pipeline = json.loads(app.http('/render_pipeline/get', {}))
                assert pipeline['enabled'] and not pipeline['swapUpperLower'], pipeline
                if iteration:
                    assert pipeline['upperExpression']['type'] == 'pixel_blend' and pipeline['lowerExpression']['type'] == 'pixel_blend', pipeline
                else:
                    assert pipeline['upperExpression']['type'] == 'colormap' and pipeline['lowerExpression']['type'] == 'colormap' and pipeline['lowerExpression']['mode'] == 'MODDOTPLOT_IDENTITY', pipeline
                status = json.loads(app.http('/secondary/status', {}))
                assert status['attached'] and not status.get('requiresConfirmation'), status
                assert status['compatibility']['primaryResolutions'] == [200, 100], status
                assert status['compatibility']['secondaryResolutions'] == [100], status
                assert status['compatibility']['sameMatrixSizes'] and not status['compatibility']['mismatchedResolutionOrders'], status
                assert status['sequenceAnalysis'] == dict(kind='moddotplot',scoreUnit='identity_percent',scoreScale=10000.0,minimumIdentity=80.0), status
                (out/f'moddotplot-status-{iteration}.json').write_text(json.dumps(status,indent=2))
                jobs = [j for j in json.loads(app.http('/sequence/jobs/list', {})) if j['kind'] == 'moddotplot']
                assert len(jobs) == iteration + 1 and all(j['status'] == 'finished' for j in jobs), jobs
                latest = sorted(jobs, key=lambda j: j['createdAtMs'])[-1]
                assert latest['requestedThreads'] == 2 and 1 <= latest['effectiveThreads'] <= 2, latest
                path = data / latest['outputFile']
                versions.append(path)
                with h5py.File(path, 'r') as f:
                    assert f['sequence_analysis'].attrs['complete']
                    compatibility = json.loads(f['sequence_analysis'].attrs['compatibility'])
                    assert compatibility['settings']['parameters'] == {
                        'ambiguous': False, 'delta': 0.2, 'identity': 80.0, 'k': 15, 'sketch_size': 128
                    }, compatibility
                    assert len(f['sequence_analysis/updates']) == iteration + 1
                    assert f['resolutions/100/treap_coo/block_length'][:].sum() > 0
                if iteration == 0:
                    assert latest['outputFile'] == 'published/moddotplot.hict.hdf5' and path.is_file(), latest
                page.get_by_role('button', name='Secondary', exact=True).click()
                assert page.evaluate("() => document.querySelector('#app').__vue_app__.config.globalProperties.$pinia._s.get('matrixViewStore').activeVisualizationSource") == 'SECONDARY'
                page.get_by_role('button', name='Primary', exact=True).click()
                if iteration:
                    # The inspector now scopes colormaps to the selected source and half.
                    # Each blended half has one colormap for each source.
                    link = page.get_by_role('button', name='Link upper and lower triangles', exact=True)
                    if link.get_attribute('aria-pressed') == 'true': link.click()
                    for half in ('Upper', 'Lower'):
                        page.get_by_role('button', name=half + ' triangle', exact=True).click()
                        assert page.get_by_role('button', name=half + ' triangle', exact=True).get_attribute('aria-pressed') == 'true'
            assert versions[0] != versions[1] and versions[0].exists(), versions
            assert versions[1].is_file()
            (out / 'moddotplot-jobs.json').write_text(json.dumps(jobs, indent=2))
        def identity_presets():
            page.get_by_role('combobox', name='Visualization preset', exact=True).select_option(label='ModDotPlot: Hi-C above, identity below')
            check.settle()
            expected = json.loads(app.http('/render_pipeline/get', {}))
            assert expected['lowerExpression']['mode'] == 'MODDOTPLOT_IDENTITY', expected
            # Editing and reapplying the graph must not silently downgrade it to LINEAR.
            check.menu('View', 'Rendering pipeline…')
            dialog = page.get_by_role('dialog').filter(has=page.get_by_role('heading', name='Rendering Pipeline', exact=True))
            with page.expect_response(lambda r: '/render_pipeline/set' in r.url):
                dialog.get_by_role('button', name='Apply', exact=True).click()
            dialog.get_by_role('button', name='Close', exact=True).click()
            dialog.wait_for(state='hidden'); check.settle()
            actual = json.loads(app.http('/render_pipeline/get', {}))
            assert actual['lowerExpression']['mode'] == 'MODDOTPLOT_IDENTITY', actual
            page.get_by_role('combobox', name='Visualization preset', exact=True).select_option(label='ModDotPlot: multicolor overlay')
            check.settle()
            actual = json.loads(app.http('/render_pipeline/get', {}))
            assert actual['upperExpression']['top']['mode'] == 'MODDOTPLOT_IDENTITY', actual
            query = dict(bpResolution=100, startRowPx=0, endRowPx=2,
                         startColPx=0, endColPx=2, signalMode='RAW_COUNTS', format='JSON')
            primary_values = json.loads(app.http('/matrix/query', {**query, 'source': 'PRIMARY'}))['values']
            secondary_values = json.loads(app.http('/matrix/query', {**query, 'source': 'SECONDARY'}))['values']
            preset_results = []
            for preset in ('primary_only', 'dotplot_overlay', 'moddotplot_below', 'moddotplot_overlay'):
                check.menu('View', 'Rendering pipeline…')
                dialog = page.get_by_role('dialog').filter(has=page.get_by_role('heading', name='Rendering Pipeline', exact=True))
                dialog.locator('.pipeline-preset-select').select_option(preset)
                before_load = json.loads(app.http('/render_pipeline/get', {}))
                dialog.get_by_role('button', name='Load preset', exact=True).click()
                assert json.loads(app.http('/render_pipeline/get', {})) == before_load
                with page.expect_response(lambda response: '/render_pipeline/set' in response.url):
                    dialog.get_by_role('button', name='Apply', exact=True).click()
                check.settle()
                applied = json.loads(app.http('/render_pipeline/get', {}))
                assert applied['enabled'] and not applied['swapUpperLower'], applied
                upper, lower = applied['upperExpression'], applied['lowerExpression']
                if preset == 'primary_only':
                    assert upper == lower and upper['type'] == 'colormap', applied
                elif preset == 'dotplot_overlay':
                    assert upper == lower and upper['type'] == 'pixel_blend', applied
                elif preset == 'moddotplot_below':
                    assert upper['type'] == 'colormap' and lower['mode'] == 'MODDOTPLOT_IDENTITY', applied
                else:
                    assert upper == lower and upper['top']['mode'] == 'MODDOTPLOT_IDENTITY', applied
                assert json.loads(app.http('/matrix/query', {**query, 'source': 'PRIMARY'}))['values'] == primary_values
                assert json.loads(app.http('/matrix/query', {**query, 'source': 'SECONDARY'}))['values'] == secondary_values
                dialog.get_by_role('button', name='Close', exact=True).click()
                dialog.wait_for(state='hidden')
                page.wait_for_function("""()=>{
                    const m=window.__hictBenchmark.manager(),map=m.getMap();
                    const [left,bottom,right,top]=map.getView().getProjection().getExtent();
                    const layers=[...m.viewAndLayersManager.layersHolder.primaryHiCDataLayers,
                        ...m.viewAndLayersManager.layersHolder.secondaryHiCDataLayers];
                    return Array.from({length:31},(_,i)=>(i+1)/32).some(f=>{
                        const pixel=map.getPixelFromCoordinate([left+f*(right-left),top+f*(bottom-top)]);
                        return layers.filter(layer=>layer.getVisible()).some(layer=>{
                            const rgba=layer.getData(pixel);
                            return rgba?.[3]>0 && Math.min(...rgba.slice(0,3))<240;
                        });
                    });
                }""")
                preset_results.append({'preset': preset, 'config': applied,
                                       'primaryValues': primary_values, 'secondaryValues': secondary_values})
            (out / 'editor-preset-results.json').write_text(json.dumps(preset_results, indent=2))
        def selection_actions():
            check.select_range()
            box = page.locator('#hic-contact-map').bounding_box()
            point = dict(x=box['x'] + min(box['width'] * .17, 150) + 40,
                         y=box['y'] + min(box['height'] * .17, 150) + 40)
            def menu():
                page.mouse.click(**point, button='right')
                context = page.get_by_role('menu', name='Assembly actions')
                context.wait_for()
                assert 'Selection: active rectangle' in context.inner_text()
                return context
            for format in ('PNG', 'SVG', 'PDF'):
                context = menu()
                with page.expect_download(timeout=90000) as download:
                    context.get_by_role('menuitem', name=f'Export selection as {format}', exact=True).click()
                path = out / ('selection.' + format.lower())
                download.value.save_as(path)
                assert path.stat().st_size > 200, str(path)
                check.settle()
            # Vector PDF rasterization should preserve the same selected scene.
            if shutil.which('pdftoppm'):
                from PIL import Image
                import numpy as np
                subprocess.run(['pdftoppm','-r','96','-singlefile','-png',str(out/'selection.pdf'),str(out/'selection-pdf-render')],check=True,capture_output=True)
                png=np.array(Image.open(out/'selection.png').convert('RGB')).astype(float)
                pdf=np.array(Image.open(out/'selection-pdf-render.png').convert('RGB')).astype(float)
                assert png.shape==pdf.shape,(png.shape,pdf.shape)
                diff=np.abs(png-pdf)
                comparison=dict(mean_channel_error=float(diff.mean()),pixels_over_10=float((diff.max(axis=2)>10).mean()))
                (out/'export-comparison.json').write_text(json.dumps(comparison,indent=2))
                assert comparison['mean_channel_error']<3 and comparison['pixels_over_10']<0.06,comparison
                text=subprocess.check_output(['pdftotext',str(out/'selection.pdf'),'-'],text=True)
                assert len(text.strip())>15, 'PDF labels must remain editable text'
            menu().get_by_role('menuitem', name='Generate ModDotPlot for selection…', exact=True).click()
            assert page.get_by_label('Analysis scope').input_value() == 'selection'
            page.get_by_label('Display completed result').select_option('none')
            previous_pipeline = json.loads(app.http('/render_pipeline/get', {}))
            with page.expect_response(lambda r: '/sequence/moddotplot/jobs' in r.url) as response:
                page.get_by_role('dialog', name='Generate ModDotPlot', exact=True).get_by_role('button', name='Generate ModDotPlot', exact=True).click()
            assert response.value.status == 202, response.value.text()
            request = response.value.request.post_data_json
            assert request['scope'] == 'selection' and request['region']['rowEnd'] < 4000, request
            page.get_by_role('dialog', name='Generate ModDotPlot', exact=True).wait_for(state='hidden')
            check.settle()
            assert json.loads(app.http('/render_pipeline/get', {})) == previous_pipeline, 'Do not link now must retain the current display'
        def alignment_dotplot():
            import h5py
            check.select_range()
            box=page.locator('#hic-contact-map').bounding_box()
            page.mouse.click(x=box['x']+min(box['width']*.17,150)+40,y=box['y']+min(box['height']*.17,150)+40,button='right')
            page.get_by_role('menu',name='Assembly actions').get_by_role('menuitem',name='Generate dotplot for selection…',exact=True).click()
            dialog=page.get_by_role('dialog',name='Generate dotplot from active assembly',exact=True)
            assert page.get_by_label('Analysis scope').input_value()=='selection'
            [checkbox.uncheck() for checkbox in dialog.locator('.resolution-options input').all()]
            assert dialog.get_by_role('button', name='Generate dotplot from active assembly', exact=True).is_disabled()
            dialog.locator('.resolution-options input[value="100"]').check()
            check.advanced()
            for name,value in [('Minimizer k','7'),('Minimizer window','1'),('Minimum chain score','5'),('Minimum alignment length (bp)','20'),('Sample every (bp)','50'),('Alignment threads','2')]:
                page.get_by_label(name,exact=True).fill(value)
            page.get_by_label('Save a reusable copy under DATA_DIR').check()
            page.get_by_label('Output path').fill('published/selection-dotplot.hict.hdf5')
            with page.expect_response(lambda r:'/sequence/dotplot/jobs' in r.url) as response:
                dialog.get_by_role('button',name='Generate dotplot from active assembly',exact=True).click()
            assert response.value.status==202,response.value.text()
            dialog.wait_for(state='hidden');check.settle()
            jobs=[j for j in json.loads(app.http('/sequence/jobs/list',{})) if j['kind']=='dotplot']
            assert len(jobs)==1 and jobs[0]['status']=='finished',jobs
            assert jobs[0]['outputFile']=='published/selection-dotplot.hict.hdf5' and (data/jobs[0]['outputFile']).is_file(),jobs
            assert jobs[0]['requestedThreads'] == 2 and 1 <= jobs[0]['effectiveThreads'] <= 2, jobs
            with h5py.File(data/jobs[0]['outputFile']) as f:
                assert f['sequence_analysis'].attrs['score_unit']=='alignment_sample_count'
                assert f['resolutions/100/treap_coo/block_length'][:].sum()>0
                compatibility = json.loads(f['sequence_analysis'].attrs['compatibility'])
                parameters = compatibility['settings']['parameters']
                assert parameters == {
                    'k': 7, 'window': 1, 'chainScore': 5, 'minAlignmentLength': 20,
                    'sampleBp': 50, 'threads': jobs[0]['effectiveThreads']
                }, parameters
            # Independently count scalar endpoint samples in the captured axes.
            # Reading the result in a separate server preserves this UI workspace.
            import math
            job_root = data / 'processed' / 'sequence-analysis' / jobs[0]['jobId']
            plan = json.loads((job_root / 'analysis-plan.json').read_text())
            level = next(level for level in plan['levels'] if level['resolution'] == 100)
            rows, columns = level['rows'], level['columns']
            assert all(window[0] == 0 for window in rows + columns), level
            row_offset = min(window[2] for window in rows)
            col_offset = min(window[2] for window in columns)
            expected_counts = {}
            for line in (job_root / 'alignment.paf').read_text().splitlines():
                fields = line.split('\t')
                query_start, query_end = int(fields[2]), int(fields[3])
                target_start, target_end, length = int(fields[7]), int(fields[8]), int(fields[10])
                if length < 20:
                    continue
                steps = max(1, length // 50)
                for index in range(steps + 1):
                    fraction = index / steps
                    query = query_start + math.floor(fraction * (query_end - query_start - 1) + 0.5) + row_offset
                    offset = math.floor(fraction * (target_end - target_start - 1) + 0.5)
                    target = (target_start + offset if fields[4] == '+' else target_end - 1 - offset) + col_offset
                    row = next((window[1] for window in rows if window[2] <= query < window[3]), None)
                    column = next((window[1] for window in columns if window[2] <= target < window[3]), None)
                    if row is not None and column is not None:
                        pair = tuple(sorted((row, column)))
                        expected_counts[pair] = expected_counts.get(pair, 0) + 1
            assert expected_counts, 'Alignment produced no owned sample pairs'
            with HictTestServer(args.jar, data, out / 'alignment-sample-reader.log') as reader:
                reader.http('/open', {'filename': jobs[0]['outputFile']})
                actual = json.loads(reader.http('/matrix/query', {
                    'bpResolution': 100, 'startRowPx': 0, 'endRowPx': 40,
                    'startColPx': 0, 'endColPx': 40, 'source': 'PRIMARY',
                    'signalMode': 'RAW_COUNTS', 'format': 'JSON'
                }))['values']
            expected = [expected_counts.get(tuple(sorted((row, column))), 0)
                        for row in range(40) for column in range(40)]
            assert actual == expected, (actual, expected)
            (out / 'alignment-sample-oracle.json').write_text(json.dumps({
                'sampleBp': 50, 'ownedSamples': sum(expected_counts.values()),
                'matrixValuesCompared': len(expected), 'values': actual
            }, indent=2))
            (out/'dotplot-jobs.json').write_text(json.dumps(jobs,indent=2))
        def blast_search():
            page.get_by_role('link', name='Tools', exact=True).click()
            page.get_by_role('button', name='Find sequences in linked FASTA…', exact=True).click()
            page.get_by_label('FASTA records, or one DNA sequence per line').fill('>indel alignment\n' + sequence[100:190] + 'AAAAA' + sequence[190:310])
            page.get_by_label('Matching', exact=True).select_option('blast')
            check.advanced()
            page.get_by_label('Default gap length (Ns)', exact=True).fill('1000')
            with page.expect_response(lambda r: '/sequence/search/jobs' in r.url) as response:
                page.get_by_role('button', name='Find sequences', exact=True).click()
            assert response.value.status == 202, response.value.text()
            page.get_by_role('dialog', name='Find sequences in the linked FASTA', exact=True).wait_for(state='hidden')
            check.settle()
            jobs = sorted([j for j in json.loads(app.http('/sequence/jobs/list', {})) if j['kind'] == 'search'], key=lambda j:j['createdAtMs'])[-1:]
            assert len(jobs) == 1 and jobs[0]['status'] == 'finished' and jobs[0]['hits'] >= 1, jobs
            raw = (data / jobs[0]['outputFile']).parent / 'blast-hits.tsv'
            assert any(int(line.split('\t')[9]) > 0 for line in raw.read_text().splitlines()), raw.read_text()
            (out/'blast-job.json').write_text(json.dumps(jobs,indent=2))
        def search_warning():
            check.reverse(); check.settle()
            warnings = page.evaluate("() => Array.from(window.__hictBenchmark.manager().linearTrackManager.analysisWarnings.values())")
            assert any('New matches might have formed' in w for w in warnings), warnings
            (out/'search-warnings.json').write_text(json.dumps(warnings,indent=2))
        def minimap_clipping():
            state = page.evaluate("""() => {
              const m=window.__hictBenchmark.manager(), map=m.map, v=map.getView();
              const original={center:v.getCenter().slice(),resolution:v.getResolution()}, results=[];
              const bounds=m.minimap.getView().getProjection().getExtent();
              for (const center of [[bounds[0],bounds[1]],[bounds[2],bounds[3]],[(bounds[0]+bounds[2])/2,(bounds[1]+bounds[3])/2]]) {
                v.setCenter(center);m.syncMinimapViewport();
                const extent=m.minimapViewportFeature.getGeometry()?.getExtent();
                if (extent && (extent[0]<bounds[0] || extent[1]<bounds[1] || extent[2]>bounds[2] || extent[3]>bounds[3])) throw new Error('Viewport escapes minimap');
                results.push({center,extent,bounds});
              }
              v.setCenter([(bounds[0]+bounds[2])/2,(bounds[1]+bounds[3])/2]);v.setResolution(original.resolution/3);m.syncMinimapViewport();m.minimap.renderSync();
              const canvases=Array.from(m.minimap.getTargetElement().querySelectorAll('canvas')).map(c=>{
                const scratch=document.createElement('canvas');scratch.width=c.width;scratch.height=c.height;
                const ctx=scratch.getContext('2d',{willReadFrequently:true});ctx.drawImage(c,0,0);
                const a=ctx.getImageData(0,0,c.width,c.height).data;let blue=0;
                for(let i=0;i<a.length;i+=4)if(a[i]<30&&a[i+1]>60&&a[i+1]<160&&a[i+2]>220&&a[i+3]>100)blue++;
                return {width:c.width,height:c.height,blue};
              });
              results.push({canvases});
              v.fit(bounds,{size:map.getSize(),padding:[0,0,0,0]});m.syncMinimapViewport();m.minimap.renderSync();
              const fullCanvases=Array.from(m.minimap.getTargetElement().querySelectorAll('canvas')).map(c=>{
                const scratch=document.createElement('canvas');scratch.width=c.width;scratch.height=c.height;
                const ctx=scratch.getContext('2d',{willReadFrequently:true});ctx.drawImage(c,0,0);
                const a=ctx.getImageData(0,0,c.width,c.height).data;let blue=0;
                for(let i=0;i<a.length;i+=4)if(a[i]<30&&a[i+1]>60&&a[i+1]<160&&a[i+2]>220&&a[i+3]>100)blue++;
                return {width:c.width,height:c.height,blue};
              });
              results.push({canvases:fullCanvases,fullExtent:m.minimapViewportFeature.getGeometry()?.getExtent()});
              v.setCenter(original.center);v.setResolution(original.resolution);m.syncMinimapViewport();
              if(getComputedStyle(m.minimap.getTargetElement()).overflow!=='hidden')throw new Error('Missing minimap clipping');
              return results;
            }""")
            (out/'minimap-clipping.json').write_text(json.dumps(state,indent=2))
            assert all(sum(c['blue'] for c in row['canvases'])>10 for row in state[-2:]),state[-2:]
        def search_control_variants():
            page.evaluate(
                "args => window.__hictBenchmark.open('known.hict.hdf5', args.api, 'known.fasta')",
                {"api": app.url},
            )
            check.settle()
            original = sequence[80:104]
            ambiguous = "N" + original[1:]
            mutated = ("A" if original[0] != "A" else "C") + original[1:]
            cases = [
                ("literal-ambiguity", ambiguous, "exact", False, False, 0),
                ("expanded-ambiguity", ambiguous, "exact", False, True, 0),
                ("forward-only-reverse-query", reverse(original), "exact", False, False, 0),
                ("both-strands-reverse-query", reverse(original), "exact", True, False, 0),
                ("one-substitution", mutated, "substitutions", False, False, 1),
            ]
            records = []
            for label, query, mode, both, degenerate, mismatches in cases:
                expected = set()
                for start in range(len(sequence) - len(query) + 1):
                    candidate = sequence[start : start + len(query)]
                    for strand, bases in [("+", candidate)] + (
                        [("-", reverse(candidate))] if both else []
                    ):
                        differences = sum(
                            a != b and not (degenerate and a == "N")
                            for a, b in zip(query, bases)
                        )
                        if differences <= mismatches:
                            expected.add((start, start + len(query), strand))
                check.menu("Tools", "Find sequences in linked FASTA…")
                dialog = page.get_by_role(
                    "dialog", name="Find sequences in the linked FASTA", exact=True
                )
                page.wait_for_function(
                    "!document.querySelector('#search-mode option[value=exact]').disabled"
                )
                dialog.get_by_label("FASTA records, or one DNA sequence per line").fill(query)
                dialog.get_by_label("Search scope", exact=True).select_option("assembly")
                dialog.get_by_label("DNA strands", exact=True).select_option(
                    "true" if both else "false"
                )
                dialog.get_by_label("Matching", exact=True).select_option(mode)
                if mode == "exact":
                    dialog.get_by_label(
                        "Expand IUPAC ambiguity codes in queries", exact=True
                    ).set_checked(degenerate)
                else:
                    dialog.get_by_label("Maximum substitutions per match", exact=True).fill(
                        str(mismatches)
                    )
                check.advanced()
                dialog.get_by_label("Save reusable BED under DATA_DIR").check()
                filename = "published/control-" + label + ".bed"
                dialog.get_by_label("BED output path").fill(filename)
                with page.expect_response(
                    lambda response: response.url.endswith("/sequence/search/jobs")
                ) as submitted:
                    dialog.get_by_role("button", name="Find sequences", exact=True).click()
                assert submitted.value.status == 202, submitted.value.text()
                settings = submitted.value.request.post_data_json
                assert (
                    settings["mode"] == mode
                    and settings["bothStrands"] == both
                    and settings["degenerate"] == degenerate
                ), settings
                assert (
                    settings["scope"] == "assembly" and settings["outputFilename"] == filename
                ), settings
                if mode == "substitutions":
                    assert settings["mismatches"] == mismatches, settings
                job_id = submitted.value.json()["jobId"]
                deadline = time.monotonic() + 60
                while time.monotonic() < deadline:
                    job = next(
                        j
                        for j in json.loads(app.http("/sequence/jobs/list", {}))
                        if j["jobId"] == job_id
                    )
                    if job["status"] in ("finished", "failed", "cancelled"):
                        break
                    page.wait_for_timeout(100)
                assert job["status"] == "finished" and job["hits"] == len(expected), (
                    label,
                    expected,
                    job,
                )
                bed = data / filename
                assert bed.is_file(), (label, job)
                lines = [
                    line.split("\t")
                    for line in bed.read_text().splitlines()
                    if line and not line.startswith(("#", "track", "browser"))
                ]
                actual = {(int(row[1]), int(row[2]), row[5]) for row in lines}
                assert actual == expected, (label, actual, expected)
                if expected:
                    dialog.wait_for(state="hidden")
                else:
                    dialog.get_by_role("button", name="Close", exact=True).last.click()
                records.append(
                    {
                        "case": label,
                        "expected": sorted(expected),
                        "actual": sorted(actual),
                        "job": job_id,
                    }
                )
            (out / "search-control-variants.json").write_text(
                json.dumps(records, indent=2) + "\n"
            )
        def reattach_to_original_order():
            jobs = sorted([j for j in json.loads(app.http('/sequence/jobs/list', {})) if j['kind']=='dotplot'], key=lambda j:j['createdAtMs'])
            filename=jobs[-1]['outputFile']
            check.menu('File','Close')
            page.get_by_role('alertdialog', name='Unsaved assembly edits').get_by_role('button', name='Discard edits', exact=True).click()
            page.get_by_role('heading', name='No contact map open', exact=True).wait_for()
            load_file('File','Open…','known.hict.hdf5','/open')
            result=page.evaluate("""async filename => {
              const m=window.__hictBenchmark.manager(), api=m.networkManager.requestManager;
              const opened=await api.openSecondarySource(filename);
              if(opened.requiresConfirmation)throw new Error('Generated overlay should match original physical bins');
              const secondary=await api.setAssemblyInfoSource('SECONDARY');
              const primary=await api.setAssemblyInfoSource('PRIMARY');
              m.viewAndLayersManager.setSecondaryResolutionModel(opened.compatibility);
              await m.reloadTilesFromBackend();
              return {opened,primary:primary.assemblyInfo,secondary:secondary.assemblyInfo};
            }""",filename)
            assert result['primary']['contigDescriptors'] == result['secondary']['contigDescriptors'], result
            (out/'original-order-reattachment.json').write_text(json.dumps(result,indent=2))
        try:
            page.goto(f'http://127.0.0.1:{static.server_port}/')
            check.step('open', lambda: load_file('File', 'Open…', 'known.hict.hdf5', '/open'))
            check.step('missing-fasta-guidance', missing_fasta)
            check.step('link-fasta', lambda: load_file('Assembly', 'Link FASTA', 'known.fasta', '/link_fasta'))
            check.step('split-and-invert-layout', lambda: load_file('Assembly', 'Load AGP', 'split.agp', '/load_agp'))
            check.step('missing-tools-disabled', missing_tools)
            check.step('search-cross-join-and-open-track', search)
            check.step('hide-and-restore-track-axis', track_axis_visibility)
            check.step('blast-gapped-local-search', blast_search)
            check.step('search-warning-after-edit', search_warning)
            check.step('minimap-clipping', minimap_clipping)
            check.step('generate-display-and-accumulate-moddotplot', moddotplot)
            check.step('identity-presets-and-pipeline-roundtrip', identity_presets)
            check.step('selection-menu-exports-and-moddotplot', selection_actions)
            check.step('selection-alignment-dotplot',alignment_dotplot)
            check.step('reattach-generated-overlay-to-original-order', reattach_to_original_order)
            check.step('search-modes-iupac-strands-and-exact-bed-positions', search_control_variants)
            notifications = page.evaluate("()=>document.querySelector('#app').__vue_app__.config.globalProperties.$pinia._s.get('notificationCenter').entries")
            (out / 'notifications.json').write_text(json.dumps(notifications, indent=2))
            assert not [e for e in notifications if e['level'] == 'error'], notifications
            assert any('Sequence search completed: 1 matches' in e['message'] for e in notifications)
            success = True
        finally:
            (out / 'summary.json').write_text(json.dumps(dict(status='ok' if success else 'error', cases=check.rows, browser=browser.version), indent=2))
            browser.close(); static.shutdown()


if __name__ == '__main__': main()
