#!/usr/bin/env python3
"""Quad-map acceptance tests on disposable physical copies, including generation.

Run against a dedicated server whose DATA_DIR is --data-dir. Sequence jobs write
managed artifacts below that directory; browser exports go to --output.
"""
import argparse
import json
import os
import time
from pathlib import Path
from playwright.sync_api import sync_playwright


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--url', default='http://127.0.0.1:8185')
    parser.add_argument('--api', default='http://127.0.0.1:5105')
    parser.add_argument('--output', default='/tmp/hict-review-20261018/browser')
    parser.add_argument('--data-dir', required=True, help='Dedicated server DATA_DIR with physical input copies')
    args = parser.parse_args()
    data = Path(args.data_dir).absolute()
    if data != data.resolve() or not data.is_relative_to('/tmp'):
        raise ValueError('Acceptance DATA must be a nonsymlinked directory under /tmp')
    for name in ('combined_ind2_4DN.hict.hdf5', 'ind2.self.hict.hdf5', 'quad_combined_ind2.fasta'):
        path = data / name
        if not path.is_file() or path.is_symlink() or path.stat().st_nlink != 1:
            raise ValueError(f'Use a physical input copy, not a symlink/hardlink: {path}')
    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=True)
    errors, results, network_errors = [], [], []
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_page(viewport={'width': 1440, 'height': 1000})
        page.set_default_timeout(90000)
        page.on('pageerror', lambda error: errors.append(str(error)))
        page.on('requestfailed', lambda request: network_errors.append({'url': request.url, 'error': request.failure}))
        page.on('response', lambda response: network_errors.append({'url': response.url, 'status': response.status}) if response.status >= 400 else None)
        page.add_init_script('window.__HICT_BENCHMARK_ENABLED__ = true')
        page.route('http://localhost:5000/**', lambda route: route.continue_(url=args.api + route.request.url.removeprefix('http://localhost:5000')))
        try:
            page.goto(args.url)
            page.wait_for_function('window.__hictBenchmark')
            start = time.monotonic()
            page.evaluate('(api) => window.__hictBenchmark.open("combined_ind2_4DN.hict.hdf5", api)', args.api)
            page.wait_for_function('window.__hictBenchmark.manager()?.getView().getCenter()?.every(Number.isFinite)')
            page.wait_for_timeout(2000)
            nan_button = page.get_by_role('button', name='Do not use Cooler weights', exact=True)
            if nan_button.is_visible(): nan_button.click()
            results.append({'test': 'primary open', 'seconds': time.monotonic() - start})
            # Noninteractive capture still exercises the real renderer, not a mock map.
            page.evaluate('''async () => {
                const m = window.__hictBenchmark.manager();
                const status = await m.networkManager.requestManager.openSecondarySource('ind2.self.hict.hdf5', true);
                m.viewAndLayersManager.setSecondaryResolutionModel(status.compatibility);
                const source = s => ({type:'source', source:s});
                const color = (s,c,max) => ({type:'colormap', mode:'LINEAR', input:source(s), startColor:'#ffffff00', endColor:c, minSignal:0, maxSignal:max});
                const overlay = {type:'pixel_blend', mode:'OVER', top:color('SECONDARY','#000000ff',20), bottom:color('PRIMARY','#ff0000ff',100), topOpacity:0.65, bottomOpacity:1};
                await m.networkManager.requestManager.setRenderPipelineConfig({enabled:true, swapUpperLower:false,upperExpression:overlay,lowerExpression:overlay});
                await m.reloadTilesFromBackend();
                m.viewAndLayersManager.fitViewToActiveExtent();
            }''')
            page.wait_for_timeout(2000)
            page.screenshot(path=str(out/'quad-overlay.png'))
            for scope in ('viewport', 'whole-map'):
                before = page.evaluate('''() => {const v=window.__hictBenchmark.manager().getView();return [v.getCenter(),v.getResolution(),v.getRotation()]}''')
                for fmt in ('Svg', 'Png', 'Pdf'):
                    start = time.monotonic()
                    with page.expect_download(timeout=120000) as download:
                        page.evaluate('''async ({scope,fmt}) => {
                            window.exportProgress=[];
                            await window.__hictBenchmark.manager()['exportCurrentMap'+fmt](p=>window.exportProgress.push(p), {scope,cropToMap:true});
                        }''', {'scope': scope, 'fmt': fmt})
                    target = out/f'{scope}.{fmt.lower()}'
                    download.value.save_as(target)
                    progress = page.evaluate('window.exportProgress')
                    assert progress[0] > 0 and progress[-1] == 1, progress
                    after = page.evaluate('''() => {const v=window.__hictBenchmark.manager().getView();return [v.getCenter(),v.getResolution(),v.getRotation()]}''')
                    assert before == after, (before, after)
                    results.append({'test': f'{scope} {fmt}', 'bytes': target.stat().st_size, 'seconds': time.monotonic()-start, 'progress': progress})
            numeric = page.evaluate('''async () => {
                const api=window.__hictBenchmark.manager().networkManager.requestManager;
                const bounds={bpResolution:250000,startRowPx:0,endRowPx:12,startColPx:0,endColPx:12};
                const raw=await api.queryMatrixFloat32({...bounds,signalMode:'RAW_COUNTS'});
                const processed=await api.queryMatrixFloat32({...bounds,signalMode:'PIPELINE_SIGNAL',expression:{type:'binary',op:'MUL',left:{type:'source',source:'PRIMARY'},right:{type:'constant',value:2}}});
                return {rows:raw.rows,cols:raw.cols,raw:Array.from(raw.values),processed:Array.from(processed.values)};
            }''')
            assert numeric['rows'] == numeric['cols'] == 12, 'Matrix dimensions must be exposed to cross-origin browsers'
            assert numeric['processed'] == [2*x for x in numeric['raw']], 'Numeric histogram expression changed raw counts'
            results.append({'test': 'numeric expression query', 'values': len(numeric['raw'])})

            page.get_by_role('link', name='View', exact=True).click()
            page.get_by_role('link', name='Rendering pipeline...', exact=True).click()
            page.locator('.graph-canvas').wait_for(state='visible')
            page.wait_for_timeout(500)
            graph = page.evaluate('''() => {const c=document.querySelector('.graph-canvas').data;
                c.ds.scale=0.7;c.ds.offset=[123,45];
                return {nodes:c.graph._nodes.map(n=>n.type),viewport:[c.ds.scale,Array.from(c.ds.offset)]};}''')
            assert 'hict/signal_range' in graph['nodes'], graph
            page.evaluate('''() => {const g=document.querySelector('.graph-canvas').data.graph;
                window.dispatchEvent(new CustomEvent('hict:pipeline-histogram',{detail:g._nodes.find(n=>n.type==='hict/source')}));}''')
            page.locator('.histogram-chart svg').wait_for(state='visible')
            page.screenshot(path=str(out/'histogram.png'))
            page.locator('.histogram-dialog').get_by_role('button', name='Close', exact=True).click()
            results.append({'test':'graph histogram dialog renders sampled distribution'})
            page.locator('.pipeline-root .modal-header button').first.click()
            page.locator('.pipeline-preview').wait_for()
            page.wait_for_timeout(500)
            preview_resolution = page.evaluate('window.__hictBenchmark.manager().getView().getResolution()')
            map_box = page.locator('#hic-contact-map').bounding_box()
            x, y = map_box['x']+map_box['width']/2, map_box['y']+map_box['height']/2
            page.mouse.move(x, y)
            page.mouse.wheel(0, -250)
            page.wait_for_timeout(700)
            assert page.evaluate('window.__hictBenchmark.manager().getView().getResolution()') < preview_resolution, 'Preview blocked map zoom'
            page.locator('.pipeline-root .modal-header button').first.click()
            restored_graph = page.evaluate('''() => {const c=document.querySelector('.graph-canvas').data;return [c.ds.scale,Array.from(c.ds.offset)]}''')
            assert restored_graph == graph['viewport'], (graph, restored_graph)
            page.screenshot(path=str(out/'pipeline.png'))
            page.locator('.modal-footer').get_by_role('button', name='Close', exact=True).click()
            results.append({'test': 'pipeline range nodes and Preview viewport', 'nodes': graph['nodes']})

            track_id = page.evaluate('''async () => {
                const t=window.__hictBenchmark.manager().linearTrackManager;
                await t.openCoolerWeightsTrack('Acceptance weights');
                const id=t.getTracksSnapshot().find(t=>t.name==='Acceptance weights').trackId;
                t.setTrackVisibleOnAxis(id,'horizontal',false);t.setTrackLogBase(id,2);return id;
            }''')
            page.wait_for_timeout(1500)
            pipeline_before_attach = page.evaluate('window.__hictBenchmark.manager().networkManager.requestManager.getRenderPipelineConfig()')
            attached_before = page.evaluate('''() => {const m=window.__hictBenchmark.manager();return {view:[m.getView().getCenter(),m.getView().getResolution()],styles:m.viewAndLayersManager.getTrackStylePreset()}}''')
            page.get_by_role('link', name='File', exact=True).click()
            page.get_by_role('link', name='Attach', exact=True).click()
            page.wait_for_timeout(2500)
            attached_after = page.evaluate('''() => {const m=window.__hictBenchmark.manager();return {view:[m.getView().getCenter(),m.getView().getResolution()],styles:m.viewAndLayersManager.getTrackStylePreset()}}''')
            assert attached_before == attached_after, (attached_before, attached_after)
            pipeline_after_attach = page.evaluate('window.__hictBenchmark.manager().networkManager.requestManager.getRenderPipelineConfig()')
            assert pipeline_before_attach == pipeline_after_attach, 'Attach changed rendering pipeline'
            assert float(page.locator('.threshold-number-input').nth(1).input_value()) == 100, 'Attach replaced graph thresholds with legacy options'
            track_state = page.evaluate('''id => {const t=window.__hictBenchmark.manager().linearTrackManager;
                return [t.isTrackVisibleOnAxis(id,'horizontal'),t.isTrackVisibleOnAxis(id,'vertical'),t.getTrackLogBase(id)];}''',track_id)
            assert track_state == [False,True,2], track_state
            page.evaluate('id=>window.__hictBenchmark.manager().linearTrackManager.removeTrack(id)',track_id)
            results.append({'test':'Attach preserves viewport and styles'})

            link = page.request.post(args.api+'/link_fasta', data={'fastaFilename':'quad_combined_ind2.fasta'})
            assert link.ok, link.text()
            region = page.request.post(args.api+'/sequence/region',data={'startBp':0,'endBp':100})
            assert region.ok and region.json()['linked'], region.text()
            first_contig = page.evaluate('window.__hictBenchmark.manager().getContigDimensionHolder().contigDescriptors[0].contigName')
            with (data / 'quad_combined_ind2.fasta').open('rb') as fasta:
                prefix = fasta.read(512).decode('ascii').splitlines()
            assert prefix[0] == '>'+first_contig, first_contig
            assert region.json()['sequence'].upper() == prefix[1][:100].upper()
            page.evaluate('''() => {const m=window.__hictBenchmark.manager(),v=m.getView();
                v.setResolution(v.getMinResolution());v.setCenter([0.1,-0.1]);m.getMap().renderSync();}''')
            page.locator('.sequence-axis.horizontal').wait_for(state='visible')
            page.locator('.sequence-axis.vertical').wait_for(state='visible')
            page.screenshot(path=str(out/'fasta-bases.png'))
            results.append({'test':'FASTA exact source bases and both axis canvases', 'bases':100})
            page.evaluate('window.__hictBenchmark.manager().viewAndLayersManager.fitViewToActiveExtent()')
            for iteration in range(2):
                primary_before = page.evaluate('''async () => {const p=await window.__hictBenchmark.manager().networkManager.requestManager.getRenderPipelineConfig();
                    return p.upperExpression.type==='pixel_blend'?p.upperExpression.bottom:p.upperExpression;}''')
                page.evaluate('''i => {const s=window.__hictBenchmark.manager().viewAndLayersManager.currentViewState.selectionBorders;
                    s.leftBP=[i*40000,i*40000];s.rightBP=[(i+1)*40000,(i+1)*40000];}''', iteration)
                page.get_by_role('link', name='Tools', exact=True).click()
                page.get_by_role('button', name='Generate ModDotPlot…', exact=True).click()
                dialog = page.get_by_role('dialog', name='Generate ModDotPlot', exact=True)
                page.get_by_label('Analysis scope').select_option('selection')
                for checkbox in dialog.locator('.resolution-options input').all(): checkbox.uncheck()
                dialog.locator('.resolution-options input[value="5000"]').check()
                page.get_by_label('Worker threads').fill('4')
                dialog.get_by_role('button', name='Generate ModDotPlot', exact=True).click()
                dialog.wait_for(state='hidden', timeout=180000)
                page.wait_for_timeout(1500)
                primary_after = page.evaluate('window.__hictBenchmark.manager().networkManager.requestManager.getRenderPipelineConfig().then(p=>p.upperExpression)')
                assert primary_before == primary_after, (primary_before, primary_after)
                results.append({'test':f'ModDotPlot selection {iteration+1} retains primary graph'})
            page.screenshot(path=str(out/'moddotplot-repeated.png'))
            for width in (900, 390):
                page.set_viewport_size({'width':width,'height':1000})
                page.wait_for_timeout(600)
                box = page.locator('#hic-contact-map').bounding_box()
                assert box['width'] > 200 and box['height'] > 300, box
                page.screenshot(path=str(out/f'workspace-{width}.png'))
            assert not errors, errors
        finally:
            diagnostics = page.evaluate('''() => {
              const m=window.__hictBenchmark?.manager()?.getMap();
              if (!m) return null;
              return {loading:m.tileQueue_.getTilesLoading(),queued:m.tileQueue_.getCount(),complete:m.renderComplete_,
                animate:m.frameState_?.animate,hints:m.frameState_?.viewHints,
                layers:m.getAllLayers().map(l=>({visible:l.getVisible(),min:l.getMinResolution(),max:l.getMaxResolution(),ready:l.getRenderer()?.ready,loading:l.getSource()?.loading,type:l.constructor.name}))};
            }''')
            (out/'diagnostics.json').write_text(json.dumps(diagnostics, indent=2))
            page.screenshot(path=str(out/'last-state.png'))
            (out/'results.json').write_text(json.dumps({'results': results, 'browserErrors': errors, 'networkErrors': network_errors}, indent=2))
            browser.close()
    print(json.dumps(results, indent=2))


if __name__ == '__main__':
    main()
