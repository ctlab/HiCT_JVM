#!/usr/bin/env python3
"""Exercise independent entity/region gestures, exact coordinate entry and scoped borders."""
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
    parser=argparse.ArgumentParser(description=__doc__)
    for key in ('jar','webui','data-dir','output'):parser.add_argument('--'+key,required=True)
    args=parser.parse_args();data,out=Path(args.data_dir).absolute(),Path(args.output).absolute()
    if any(p!=p.resolve() or not p.is_relative_to('/tmp') for p in (data,out)):parser.error('Use fresh disposable nonsymlinked /tmp paths')
    data.mkdir(parents=True,exist_ok=False);out.mkdir(parents=True,exist_ok=False)
    with (out/'fixture.log').open('w') as log:
        subprocess.run(['java','--class-path',str(Path(args.jar).resolve()),str(Path(__file__).with_name('SequenceViewBrowserFixture.java')),str(data)],check=True,stdout=log,stderr=subprocess.STDOUT)
    native=data/'sequence.hict.hdf5';digest=hashlib.sha256(native.read_bytes()).hexdigest()
    original = ''.join((data / 'sequence.fa').read_text().splitlines()[1:])
    (data / 'selection-gap.agp').write_text(
        'joined\t1\t1000\t1\tW\ttiny\t1\t1000\t-\n'
        'joined\t1001\t1037\t2\tN\t37\tscaffold\tyes\tproximity_ligation\n'
        'joined\t1038\t2037\t3\tW\ttiny\t1001\t2000\t+\n'
        'tail\t1\t2000\t1\tW\ttiny\t2001\t4000\t+\n'
    )
    class Quiet(http.server.SimpleHTTPRequestHandler):
        def log_message(self,*_):pass
    static=http.server.ThreadingHTTPServer(('127.0.0.1',0),functools.partial(Quiet,directory=str(Path(args.webui).resolve())))
    threading.Thread(target=static.serve_forever,daemon=True).start()
    try:
        with HictTestServer(args.jar,data,out/'server.log') as app,sync_playwright() as pw:
            app.http('/open',{'filename':native.name});app.http('/link_fasta',{'fastaFilename':'sequence.fa'})
            browser=pw.chromium.launch(headless=True)
            page=browser.new_page(viewport={'width':1600,'height':1100},accept_downloads=True)
            page.context.grant_permissions(
                ['clipboard-read', 'clipboard-write'],
                origin=f'http://127.0.0.1:{static.server_port}',
            )
            page.set_default_timeout(30000);page.add_init_script('window.__HICT_BENCHMARK_ENABLED__=true')
            page.route('http://localhost:5000/**',lambda route:route.continue_(url=app.url+route.request.url.removeprefix('http://localhost:5000')))
            check=BrowserCheck(page,out);edits=[]
            page.on('request',lambda r:edits.append(r.post_data_json) if r.url.endswith('/reverse_selection_range') else None)
            page.goto(f'http://127.0.0.1:{static.server_port}/');check.menu('File','Join running session');check.settle()
            page.evaluate('''async()=>{const m=window.__hictBenchmark.manager(),a=m.networkManager.requestManager;
                m.eventManager.applyAssemblyInfo(await a.splitContigAtBp(0,1000));
                const id=m.contigDimensionHolder.contigDescriptors[1].contigId;
                m.eventManager.applyAssemblyInfo(await a.splitContigAtBp(id,1000));m.reloadVisuals();}''');check.settle()
            def state():
                return page.evaluate('''()=>{const m=window.__hictBenchmark.manager(),l=m.viewAndLayersManager;
                    return {ids:l.currentViewState.selectedContigIds,scaffolds:l.currentViewState.selectedScaffoldIds,region:l.getRegionBounds()??null,
                        extent:l.selectionInteractions.contigSelectExtent.getExtent()??null,entityRange:l.getSelectedEntityRange(),highlights:l.entityHighlightLayer.getSource().getFeatures().length,
                        view:l.captureViewPosition(),kind:l.currentViewState.entitySelectionKind};}''')
            def point(x,y):
                return page.evaluate('''bp=>{const m=window.__hictBenchmark.manager(),d=m.viewAndLayersManager.currentViewState.resolutionDesciptor,h=m.contigDimensionHolder;
                    const p=m.getMap().getPixelFromCoordinate([h.getFractionalPxOfBp(bp[0],d.bpResolution)*d.pixelResolution,-h.getFractionalPxOfBp(bp[1],d.bpResolution)*d.pixelResolution]);
                    const r=m.getMap().getViewport().getBoundingClientRect();return [r.left+p[0],r.top+p[1]];}''',[x,y])
            def drag(modifier,start,end):
                page.keyboard.down(modifier);page.mouse.move(*point(*start));page.mouse.down();page.mouse.move(*point(*end),steps=12);page.mouse.up();page.keyboard.up(modifier);check.settle()
            def choose(action):check.menu('Selection',action);check.settle()
            def coordinates():
                choose('Edit region coordinates…' if state()['region'] else 'Select region by coordinates…')
                dialog=page.get_by_role('dialog',name='Select region by coordinates',exact=True);dialog.wait_for();return dialog
            def fill(dialog,values):
                for key,value in values.items():dialog.locator('#region-'+key).fill(str(value))
            def export_fasta(filename, coords):
                with page.expect_response('**/get_fasta_for_selection') as response, page.expect_download() as download:
                    page.get_by_role('button', name='Export FASTA for selection', exact=True).click()
                assert response.value.ok, response.value.text()
                request = response.value.request.post_data_json
                assert [request[key] for key in ['fromBpX', 'fromBpY', 'toBpX', 'toBpY']] == coords, request
                path = out / filename
                download.value.save_as(path)
                records = {}
                for line in path.read_text().splitlines():
                    if line.startswith('>'):
                        name = line[1:]
                        records[name] = ''
                    else:
                        records[name] += line
                return records
            def scope(axis,label):
                field=page.get_by_role('textbox',name='Rows scope' if axis=='row' else 'Columns scope',exact=True)
                field.fill(label);page.locator('.scope-result:visible').filter(has_text=label).first.click();check.settle()
            def entity_drag():
                drag('Shift',(200,200),(1800,1800));s=state()
                assert len(s['ids'])==2 and s['region'] is None and s['extent'] is None,s
                assert s['entityRange']=={'startBP':0,'endBP':2000} and s['highlights']==2,s
                assert page.locator('.hict-entity-drag').count()==0
                assert page.evaluate('window.__hictBenchmark.manager().viewAndLayersManager.entityHighlightLayer.getStyle().every(s=>!s.getFill())')
            check.step('shift-entities',entity_drag)

            def isolate_entities():
                page.mouse.click(*point(500,500),button='right')
                menu=page.get_by_role('menu',name='Assembly actions',exact=True)
                menu.get_by_role('menuitem',name='Isolate view to selection',exact=True).click()
                check.settle()
                read_scopes = 'window.__hictBenchmark.manager().viewAndLayersManager.getAxisScopes()'
                selected = page.evaluate(read_scopes)
                assert all(selected[axis]['kind'] == 'selection' and len(selected[axis]['contigIds']) == 2
                           for axis in ('row','column')), selected
                scope('row','All rows')
                scope('column','All columns')
                restored = page.evaluate(read_scopes)
                assert all(restored[axis]['kind'] == 'all' for axis in ('row','column')), restored

            check.step('shift-selection-isolates-both-axes',isolate_entities)
            entity_drag()
            with page.expect_response('**/group_contigs_into_scaffold') as response:
                page.get_by_role('button',name='Group selection into scaffold',exact=True).click()
            assert response.value.ok;check.settle()
            scaffold=page.evaluate('''()=>{const m=window.__hictBenchmark.manager();return [...m.scaffoldHolder.scaffoldTable.values()].find(s=>s.scaffoldBordersBP?.startBP===0&&s.scaffoldBordersBP?.endBP===2000);}''')
            assert scaffold,scaffold
            def scaffold_pick():
                choose('Scaffolds')
                page.keyboard.down('Shift');page.mouse.click(*point(500,500));page.keyboard.up('Shift');check.settle()
                s=state();assert len(s['ids'])==2 and len(s['scaffolds'])==1 and s['highlights']==1,s
                page.evaluate('''()=>window.__hictBenchmark.manager().viewAndLayersManager.restoreViewPosition({centerBp:[1000,1000],resolution:.008})''');check.settle()
                assert state()['scaffolds']==s['scaffolds']
                page.evaluate('window.__hictBenchmark.manager().viewAndLayersManager.fitViewToActiveExtent()');check.settle()
                assert state()['scaffolds']==s['scaffolds'] and state()['highlights']==1,state()
            check.step('scaffold-and-reprojection',scaffold_pick)
            def region_drag():
                drag('Control',(300,250),(750,900));s=state()
                # Pointer events are quantized to screen pixels at the native map scale.
                # Exact base endpoints are checked independently through coordinate entry below.
                for key,target in {'rowStart':250,'rowEnd':900,'columnStart':300,'columnEnd':750}.items():
                    assert abs(s['region'][key]-target)<=5,s
                assert not s['ids'] and s['entityRange'] is None and s['highlights']==0,s
                square = page.get_by_label('Square selection', exact=True)
                square.check()
                assert state()['region'] == s['region']
                square.uncheck()
                before=len(edits);page.get_by_role('button',name='Reverse Selection',exact=True).click();check.settle();assert len(edits)==before
                page.mouse.click(*point(500,500));check.settle();assert state()['region']==s['region']
                page.mouse.click(*point(2200,2200));check.settle();assert state()['region'] is None,state()
            check.step('ctrl-region-and-edit-isolation',region_drag)
            names=page.evaluate('window.__hictBenchmark.manager().contigDimensionHolder.contigDescriptors.map(d=>d.contigName)')
            scope('row',scaffold['scaffoldName']);scope('column',names[1])
            def hide_scoped():
                choose('Hide borders in scoped view')
                assert page.evaluate('''()=>{const l=window.__hictBenchmark.manager().viewAndLayersManager;return [l.getActiveContigBordersLayer(),l.getActiveScaffoldBordersLayer()].every(layer=>layer.getSource().getFeatures().every(f=>!['contigBorders','scaffoldBorders'].includes(f.get('trackType'))));}''')
                choose('Contigs')
                page.keyboard.down('Shift');page.mouse.click(*point(1200,1200));page.keyboard.up('Shift');check.settle()
                assert len(state()['ids'])==1 and state()['highlights']==1,state()
            check.step('scoped-borders-and-visible-selection',hide_scoped)
            def typed_region():
                dialog=coordinates();fill(dialog,{'row-start':'0'});dialog.get_by_role('button',name='Select region',exact=True).click()
                dialog.get_by_role('alert').wait_for();assert state()['region'] is None
                fill(dialog,{'row-start':names[0]+':2','row-end':names[1]+':10','column-start':names[1]+':4','column-end':names[1]+':80'})
                page.screenshot(path=str(out/'coordinate-dialog.png'))
                dialog.get_by_role('button',name='Select region',exact=True).click();dialog.wait_for(state='hidden');check.settle()
                expected={'rowStart':1,'rowEnd':1010,'columnStart':1003,'columnEnd':1080}
                assert state()['region']==expected and not state()['ids'],state()
                dialog=coordinates();fill(dialog,{'column-start':names[0]+':1'});dialog.get_by_role('button',name='Select region',exact=True).click()
                dialog.get_by_role('alert').wait_for();assert state()['region']==expected
                page.keyboard.press('Escape');assert state()['region']==expected
                # The sequence export must use exact BP endpoints, never inverse native-bin rounding.
                records = export_fasta('selected.fasta', [1003, 1, 1080, 1010])
                assert records=={'selection_horizontal_1003_1080_primary':original[1003:1080],
                                 # Selection coordinates are gapless assembly BP; FASTA preserves
                                 # the scaffold separator (the fixture uses the default 1000 bp).
                                 'selection_vertical_1_1010_primary':original[1:1000]+'N'*1000+original[1000:1010]},records
                page.evaluate('''()=>window.__hictBenchmark.manager().viewAndLayersManager.restoreViewPosition({centerBp:[1040,500],resolution:1/2000})''');check.settle()
                assert state()['region']==expected,state()
                dialog=coordinates();dialog.locator('#region-convention').select_option('zero-based')
                assert dialog.locator('#region-row-start').input_value()=='1'
                assert dialog.locator('#region-column-start').input_value()=='1003'
                dialog.get_by_label('Coordinate frame').select_option('scaffold')
                scaffold_name = scaffold['scaffoldName']
                assert dialog.locator('#region-row-start').input_value() == f'scf:{scaffold_name}:1'
                dialog.get_by_role('button', name='Copy coordinates', exact=True).click()
                dialog.get_by_role('status').get_by_text('Coordinates copied.', exact=True).wait_for()
                assert page.evaluate('navigator.clipboard.readText()') == (
                    f'Scaffold coordinates (bp, 0-based, end excluded)\n'
                    f'Rows\tscf:{scaffold_name}:1\tscf:{scaffold_name}:1010\n'
                    f'Columns\tscf:{scaffold_name}:1003\tscf:{scaffold_name}:1080'
                )
                dialog.get_by_label('Coordinate frame').select_option('contig')
                assert dialog.locator('#region-column-start').input_value() == f'ctg:{names[1]}:3'
                fill(dialog, {'row-start': f'scf:{scaffold_name}:1', 'row-end': f'ctg:{names[1]}:10'})
                dialog.get_by_role('button',name='Select region',exact=True).click();check.settle();assert state()['region']==expected,state()
                page.keyboard.press('Escape');check.settle();assert state()['region'] is None
            check.step('typed-scoped-coordinates-export-and-escape',typed_region)
            scope('row','All rows');scope('column','All columns')
            assert page.evaluate('''()=>window.__hictBenchmark.manager().viewAndLayersManager.getActiveContigBordersLayer().getSource().getFeatures().some(f=>f.get('trackType')==='contigBorders')''')
            def export_without_overlays():
                def export(name):
                    with page.expect_download() as download:
                        page.evaluate("window.__hictBenchmark.manager().exportCurrentMapPng(undefined,{scope:'viewport',cropToMap:true})")
                    path=out/(name+'.png');download.value.save_as(path);check.settle();return path.read_bytes()
                page.evaluate('window.__hictBenchmark.manager().viewAndLayersManager.clearSelections()');check.settle()
                clean=export('clean-export')
                page.evaluate('window.__hictBenchmark.manager().viewAndLayersManager.selectEntityRangeByBp(0,2000)');check.settle()
                assert state()['highlights']==2
                assert export('entity-export')==clean
                assert len(state()['ids'])==2 and page.evaluate('window.__hictBenchmark.manager().viewAndLayersManager.entityHighlightLayer.getVisible()')
                page.evaluate('window.__hictBenchmark.manager().viewAndLayersManager.setRegionBounds({rowStart:100,rowEnd:1000,columnStart:300,columnEnd:2000})');check.settle()
                selected=state()['region'];assert export('region-export')==clean
                assert state()['region']==selected
                assert page.evaluate('window.__hictBenchmark.manager().viewAndLayersManager.selectionInteractions.contigSelectExtent.extentOverlay_.getVisible()')
            check.step('selection-free-export-and-restoration',export_without_overlays)
            def clipboard_selection():
                dialog = coordinates()
                fill(dialog, {
                    'row-start': 101,
                    'row-end': 900,
                    'column-start': 201,
                    'column-end': 1200,
                })
                dialog.get_by_role('button', name='Select region', exact=True).click()
                dialog.wait_for(state='hidden')
                check.settle()
                before = state()
                choose('Copy coordinates')
                page.get_by_text('Selection coordinates copied', exact=True).wait_for()
                assert page.evaluate('navigator.clipboard.readText()') == (
                    'Assembly coordinates (bp, 1-based inclusive)\n'
                    'Rows\t101\t900\nColumns\t201\t1200'
                )
                page.mouse.click(*point(500, 500), button='right')
                menu = page.get_by_role('menu', name='Assembly actions')
                menu.get_by_role('menuitem', name='Copy', exact=True).click()
                page.get_by_role('menu', name='Copy', exact=True).get_by_role(
                    'menuitem', name='Coordinates in contig').click()
                page.wait_for_function("async () => (await navigator.clipboard.readText()).startsWith('Contig coordinates')")
                assert page.evaluate('navigator.clipboard.readText()') == (
                    f'Contig coordinates (bp, 1-based inclusive)\nRows\tctg:{names[0]}:101\tctg:{names[0]}:900\n'
                    f'Columns\tctg:{names[0]}:201\tctg:{names[1]}:200'
                )
                page.mouse.click(*point(500, 500), button='right')
                menu = page.get_by_role('menu', name='Assembly actions')
                menu.get_by_role('menuitem', name='Copy', exact=True).click()
                page.get_by_role('menu', name='Copy', exact=True).get_by_role(
                    'menuitem', name='Coordinates in scaffold').click()
                page.wait_for_function("async () => (await navigator.clipboard.readText()).startsWith('Scaffold coordinates')")
                scaffold_name = scaffold['scaffoldName']
                assert page.evaluate('navigator.clipboard.readText()') == (
                    f'Scaffold coordinates (bp, 1-based inclusive)\nRows\tscf:{scaffold_name}:101\tscf:{scaffold_name}:900\n'
                    f'Columns\tscf:{scaffold_name}:201\tscf:{scaffold_name}:1200'
                )

                page.mouse.click(*point(500, 500), button='right')
                menu = page.get_by_role('menu', name='Assembly actions')
                menu.get_by_role('menuitem', name='Copy', exact=True).click()
                page.get_by_role('menu', name='Copy', exact=True).get_by_role(
                    'menuitem', name='Region as image', exact=True).click()
                page.get_by_text('Selection copied as PNG', exact=True).wait_for()
                png = page.evaluate('''async () => {
                    const [item] = await navigator.clipboard.read();
                    const image = await item.getType('image/png');
                    const bitmap = await createImageBitmap(image);
                    const dimensions = [bitmap.width, bitmap.height];
                    bitmap.close();
                    return dimensions;
                }''')
                assert min(png) > 0, png
                after = state()
                assert after['region'] == before['region'] and after['view'] == before['view']
            check.step('selection-clipboard-text-and-png', clipboard_selection)
            def stale_dialog():
                dialog=coordinates()
                dialog.get_by_text('Cut at a coordinate', exact=True).click()
                dialog.get_by_label('Cut after position', exact=True).fill('350')
                page.evaluate('''async()=>{const m=window.__hictBenchmark.manager();m.eventManager.applyAssemblyInfo(await m.networkManager.requestManager.splitContigAtBp(m.contigDimensionHolder.contigDescriptors[0].contigId,10));m.reloadVisuals();}''')
                dialog.get_by_role('button', name='Cut contig', exact=True).click()
                assert 'changed' in dialog.get_by_role('alert').inner_text()
                dialog.get_by_role('button',name='Select region',exact=True).click()
                alerts = dialog.get_by_role('alert').all_inner_texts()
                assert len(alerts) == 2 and all('changed' in message for message in alerts), alerts
                assert state()['region'] is None and not state()['ids']
                page.keyboard.press('Escape');check.settle()
            check.step('stale-coordinates-rejected',stale_dialog)

            def selected_fasta_with_explicit_gap():
                check.menu('Assembly', 'Load AGP…')
                check.choose_file('selection-gap.agp')
                with page.expect_response('**/load_agp') as loaded:
                    page.get_by_role('button', name='Load', exact=True).click()
                assert loaded.value.ok, loaded.value.text()
                page.get_by_placeholder('Filter current folder by file name or path').wait_for(state='hidden')
                check.settle()

                dialog = coordinates()
                dialog.locator('#region-convention').select_option('one-based')
                fill(dialog, {'row-start': 991, 'row-end': 1010, 'column-start': 1004, 'column-end': 1080})
                dialog.get_by_role('button', name='Select region', exact=True).click()
                dialog.wait_for(state='hidden')
                check.settle()

                records = export_fasta('selected-explicit-gap.fasta', [1003, 990, 1080, 1010])
                complement = str.maketrans('ACGTRYMKSWBDHVN', 'TGCAYRKMSWVHDBN')
                reversed_tail = original[:10].translate(complement)[::-1]
                assert records == {
                    'selection_horizontal_1003_1080_primary': original[1003:1080],
                    'selection_vertical_990_1010_primary': reversed_tail + 'N' * 37 + original[1000:1010],
                }, records

            check.step('selected-fasta-spans-reversed-fragment-and-explicit-agp-gap', selected_fasta_with_explicit_gap)

            def cut_at_typed_coordinate():
                before_agp = app.http('/get_agp_for_assembly', {})
                before_sequence = json.loads(app.http('/sequence/region', {'startBp': 0, 'endBp': 1000}))['sequence']
                first = page.evaluate('''() => {
                    const descriptor = window.__hictBenchmark.manager().contigDimensionHolder.contigDescriptors[0];
                    return {name: descriptor.contigName, length: descriptor.contigLengthBp,
                            id: descriptor.contigId, direction: descriptor.direction};
                }''')
                assert first['length'] == 1000 and first['direction'] != 1, first
                page.set_viewport_size({'width': 1280, 'height': 720})
                dialog = coordinates()
                position = dialog.get_by_label('Cut after position', exact=True)
                assert not position.is_visible()
                dialog.get_by_text('Cut at a coordinate', exact=True).click()
                position.fill(f"{first['name']}:1000")
                dialog.get_by_role('button', name='Cut contig', exact=True).click()
                assert 'inside an editable contig' in dialog.get_by_role('alert').inner_text()
                assert app.http('/get_agp_for_assembly', {}) == before_agp
                scaffold_name = page.evaluate('window.__hictBenchmark.manager().scaffoldHolder.getScaffoldLocusByBp(0).scaffoldName')
                position.fill(f"scf:{scaffold_name}:350")
                assert dialog.get_by_role('alert').count() == 0
                bounds = dialog.evaluate('''element => {
                    const {top, bottom, left, right} = element.getBoundingClientRect();
                    return {top, bottom, left, right, scrollable: element.scrollHeight > element.clientHeight};
                }''')
                assert 0 <= bounds['top'] < bounds['bottom'] <= 720, bounds
                assert 0 <= bounds['left'] < bounds['right'] <= 1280, bounds
                assert bounds['scrollable'], bounds
                page.screenshot(path=str(out / 'coordinate-cut-dialog.png'))
                with page.expect_response('**/split_contig_at_bp') as response:
                    position.press('Enter')
                assert response.value.ok, response.value.text()
                request = response.value.request.post_data_json
                assert request['contigId'] == first['id'] and request['offsetBp'] == 350, request
                dialog.wait_for(state='hidden')
                check.settle()
                lengths = page.evaluate('''() => window.__hictBenchmark.manager().contigDimensionHolder
                    .contigDescriptors.map(contig => contig.contigLengthBp)''')
                assert lengths[:2] == [350, 650] and sum(lengths) == 4000, lengths
                after_sequence = json.loads(app.http('/sequence/region', {'startBp': 0, 'endBp': 1000}))['sequence']
                assert after_sequence == before_sequence
                page.get_by_role('button', name='Journal', exact=True).click()
                with page.expect_response('**/assembly/history/restore') as undo:
                    page.get_by_role('button', name='Undo assembly edit', exact=True).click()
                assert undo.value.ok, undo.value.text()
                assert app.http('/get_agp_for_assembly', {}) == before_agp

            check.step('typed-exact-cut-preserves-oriented-sequence-and-undo', cut_at_typed_coordinate)
            assert hashlib.sha256(native.read_bytes()).hexdigest()==digest
            assert not check.errors,check.errors
            (out/'results.json').write_text(json.dumps({'status':'ok','checks':check.rows,'state':state(),'sourceSha256':digest,'pageErrors':check.errors},indent=2))
            browser.close()
    finally:static.shutdown();static.server_close()


if __name__=='__main__':main()
