#!/usr/bin/env python3
"""Sequence-search DOM test with deterministic FASTA and retained full diagnostics."""
import argparse, functools, http.server, json, os, random, shutil, sys, threading, subprocess
from pathlib import Path
from playwright.sync_api import sync_playwright


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--suite', required=True, help='Benchmark suite containing the fixture and BrowserCheck harness')
    parser.add_argument('--jar', required=True)
    parser.add_argument('--webui', required=True)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    suite = Path(args.suite).resolve()
    sys.path[:0] = [str(suite), str(suite / 'scripts')]
    from bench.application import HictApplication
    from check_g3_browser import BrowserCheck
    out = Path(args.output).resolve(); out.mkdir(parents=True, exist_ok=False)
    data = out / 'data'; data.mkdir()
    fixture = suite / 'prepared/fixture/known.hict.hdf5'
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
    class Quiet(http.server.SimpleHTTPRequestHandler):
        def log_message(self, *_): pass
    static = http.server.ThreadingHTTPServer(('127.0.0.1', 0), functools.partial(Quiet, directory=str(Path(args.webui).resolve())))
    threading.Thread(target=static.serve_forever, daemon=True).start()
    font = out / 'fonts.conf'
    fonts = Path(os.environ.get('HICT_TEST_FONTS', str(suite / 'runtime/fonts/dejavu')))
    if not fonts.is_dir(): fonts = Path('/usr/share/fonts/truetype/dejavu')
    font.write_text(f'<fontconfig><dir>{fonts}</dir><cachedir>{out}/font-cache</cachedir></fontconfig>')
    config = dict(hict_dependency_jar=str(Path(args.jar).resolve()), hict_application_jar_only=True, java_args=['-Xmx2g'], sampling_ms=5)
    with HictApplication(config, data / 'known.hict.hdf5', out / 'server.log') as app, sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True, env={**os.environ, 'FONTCONFIG_FILE': str(font)})
        page = browser.new_page(viewport=dict(width=1440, height=1000)); page.set_default_timeout(60000)
        page.add_init_script('window.__HICT_BENCHMARK_ENABLED__ = true')
        page.route('http://localhost:5000/**', lambda route: route.continue_(url=app.url + route.request.url.removeprefix('http://localhost:5000')))
        check = BrowserCheck(args, app, page, out); success = False
        def load_file(menu, action, name, endpoint):
            check.menu(menu, action); check.choose_file(name)
            with page.expect_response(lambda r: endpoint in r.url) as response:
                page.get_by_role('button', name='Load', exact=True).click()
            assert response.value.status == 200, response.value.text()
            page.get_by_placeholder('Filter current folder by file name or path').wait_for(state='hidden')
        def missing_fasta():
            settings = json.loads(app.http('/sequence/settings', {'checkTools': True}))
            assert settings['mapOpen'] and not settings['fastaLinked'], settings
            assert all(settings['tools'][name]['available'] for name in ('seqkit','blastn','dotplot','hict-moddotplot')), settings
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
                state['tools'] = {name:dict(available=False,reason='This installation is missing '+name+'. Install the complete HiCT package.') for name in ('seqkit','blastn','dotplot','hict-moddotplot')}
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
            page.get_by_role('link', name='Tools', exact=True).click()
            page.get_by_role('button', name='Find sequences in linked FASTA…', exact=True).click()
            page.get_by_label('FASTA records, or one DNA sequence per line').fill('>cross_join α\n' + motif)
            assert page.get_by_label('Search scope').input_value() == 'assembly'
            assert page.get_by_label('Matching', exact=True).input_value() == 'exact'
            page.get_by_label('Default gap length (Ns)', exact=True).fill('0')
            with page.expect_response(lambda r: '/sequence/search/jobs' in r.url) as response:
                page.get_by_role('button', name='Find sequences', exact=True).click()
            assert response.value.status == 202, response.value.text()
            page.get_by_role('dialog', name='Find sequences in the linked FASTA', exact=True).wait_for(state='hidden')
            check.settle()
            jobs = json.loads(app.http('/sequence/jobs/list', {}))
            assert len(jobs) == 1 and jobs[0]['status'] == 'finished', jobs
            assert jobs[0]['hits'] == 1 and jobs[0]['fragments'] == 2, jobs
            tracks = json.loads(app.http('/tracks/list', {}))
            assert any('Sequence search (1 matches)' == t['name'] for t in tracks), tracks
            (out / 'jobs.json').write_text(json.dumps(jobs, indent=2))
            (out / 'tracks.json').write_text(json.dumps(tracks, indent=2))
        def moddotplot():
            import h5py
            versions = []
            for iteration in range(2):
                page.get_by_role('link', name='Tools', exact=True).click()
                page.get_by_role('button', name='Generate ModDotPlot…', exact=True).click()
                dialog = page.get_by_role('dialog', name='Generate ModDotPlot', exact=True)
                assert page.get_by_label('Analysis scope').input_value() == 'assembly'
                [checkbox.uncheck() for checkbox in dialog.locator('.resolution-options input').all()]
                dialog.locator('.resolution-options input[value="100"]').check()
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
                path = data / latest['outputFile']
                versions.append(path)
                with h5py.File(path, 'r') as f:
                    assert f['sequence_analysis'].attrs['complete']
                    assert len(f['sequence_analysis/updates']) == iteration + 1
                    assert f['resolutions/100/treap_coo/block_length'][:].sum() > 0
            assert versions[0] != versions[1] and not versions[0].exists(), versions
            assert versions[1].is_file()
            (out / 'moddotplot-jobs.json').write_text(json.dumps(jobs, indent=2))
        def identity_presets():
            page.get_by_role('button', name='ModDotPlot: Hi-C above, identity below', exact=True).click()
            check.settle()
            expected = json.loads(app.http('/render_pipeline/get', {}))
            assert expected['lowerExpression']['mode'] == 'MODDOTPLOT_IDENTITY', expected
            # Editing and reapplying the graph must not silently downgrade it to LINEAR.
            check.menu('View', 'Rendering pipeline...')
            dialog = page.get_by_role('dialog').filter(has=page.get_by_role('heading', name='Rendering Pipeline', exact=True))
            with page.expect_response(lambda r: '/render_pipeline/set' in r.url):
                dialog.get_by_role('button', name='Apply', exact=True).click()
            dialog.get_by_role('button', name='Close', exact=True).click()
            dialog.wait_for(state='hidden'); check.settle()
            actual = json.loads(app.http('/render_pipeline/get', {}))
            assert actual['lowerExpression']['mode'] == 'MODDOTPLOT_IDENTITY', actual
            page.get_by_role('button', name='ModDotPlot: multicolor overlay', exact=True).click()
            check.settle()
            actual = json.loads(app.http('/render_pipeline/get', {}))
            assert actual['upperExpression']['top']['mode'] == 'MODDOTPLOT_IDENTITY', actual
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
            dialog.locator('.resolution-options input[value="100"]').check()
            for name,value in [('Minimizer k','7'),('Minimizer window','1'),('Minimum chain score','5'),('Minimum alignment length (bp)','20')]:
                page.get_by_label(name,exact=True).fill(value)
            with page.expect_response(lambda r:'/sequence/dotplot/jobs' in r.url) as response:
                dialog.get_by_role('button',name='Generate dotplot from active assembly',exact=True).click()
            assert response.value.status==202,response.value.text()
            dialog.wait_for(state='hidden');check.settle()
            jobs=[j for j in json.loads(app.http('/sequence/jobs/list',{})) if j['kind']=='dotplot']
            assert len(jobs)==1 and jobs[0]['status']=='finished',jobs
            with h5py.File(data/jobs[0]['outputFile']) as f:
                assert f['sequence_analysis'].attrs['score_unit']=='alignment_sample_count'
                assert f['resolutions/100/treap_coo/block_length'][:].sum()>0
            (out/'dotplot-jobs.json').write_text(json.dumps(jobs,indent=2))
        def blast_search():
            page.get_by_role('link', name='Tools', exact=True).click()
            page.get_by_role('button', name='Find sequences in linked FASTA…', exact=True).click()
            page.get_by_label('FASTA records, or one DNA sequence per line').fill('>indel alignment\n' + sequence[100:190] + 'AAAAA' + sequence[190:310])
            page.get_by_label('Matching', exact=True).select_option('blast')
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
        def reattach_to_original_order():
            jobs = sorted([j for j in json.loads(app.http('/sequence/jobs/list', {})) if j['kind']=='dotplot'], key=lambda j:j['createdAtMs'])
            filename=jobs[-1]['outputFile']
            check.menu('File','Close'); check.settle()
            load_file('File','Manual Open...','known.hict.hdf5','/open')
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
            check.step('open', lambda: load_file('File', 'Manual Open...', 'known.hict.hdf5', '/open'))
            check.step('missing-fasta-guidance', missing_fasta)
            check.step('link-fasta', lambda: load_file('Assembly', 'Link FASTA', 'known.fasta', '/link_fasta'))
            check.step('split-and-invert-layout', lambda: load_file('Assembly', 'Load AGP', 'split.agp', '/load_agp'))
            check.step('missing-tools-disabled', missing_tools)
            check.step('search-cross-join-and-open-track', search)
            check.step('blast-gapped-local-search', blast_search)
            check.step('search-warning-after-edit', search_warning)
            check.step('minimap-clipping', minimap_clipping)
            check.step('generate-display-and-accumulate-moddotplot', moddotplot)
            check.step('identity-presets-and-pipeline-roundtrip', identity_presets)
            check.step('selection-menu-exports-and-moddotplot', selection_actions)
            check.step('selection-alignment-dotplot',alignment_dotplot)
            check.step('reattach-generated-overlay-to-original-order', reattach_to_original_order)
            notifications = page.evaluate("()=>document.querySelector('#app').__vue_app__.config.globalProperties.$pinia._s.get('notificationCenter').entries")
            (out / 'notifications.json').write_text(json.dumps(notifications, indent=2))
            assert not [e for e in notifications if e['level'] == 'error'], notifications
            assert any('Sequence search completed: 1 matches' in e['message'] for e in notifications)
            success = True
        finally:
            (out / 'summary.json').write_text(json.dumps(dict(status='ok' if success else 'error', cases=check.rows, browser=browser.version), indent=2))
            browser.close(); static.shutdown()


if __name__ == '__main__': main()
