#!/usr/bin/env python3
"""Exercise real native/base view transitions, exact scissors, tracks and saved positions."""
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
    for key in ('jar','webui','data-dir','output'): parser.add_argument('--'+key,required=True)
    args=parser.parse_args()
    data,output=Path(args.data_dir).absolute(),Path(args.output).absolute()
    if data!=data.resolve() or not data.is_relative_to('/tmp') or not output.is_relative_to('/tmp'):
        parser.error('Use fresh nonsymlinked disposable directories under /tmp')
    data.mkdir(parents=True,exist_ok=False);output.mkdir(parents=True,exist_ok=False)
    with (output/'fixture.log').open('w') as log:
        subprocess.run(['java','--class-path',str(Path(args.jar).resolve()),
            str(Path(__file__).with_name('SequenceViewBrowserFixture.java')),str(data)],check=True,stdout=log,stderr=subprocess.STDOUT)
    native=data/'sequence.hict.hdf5';digest=hashlib.sha256(native.read_bytes()).hexdigest()
    class Quiet(http.server.SimpleHTTPRequestHandler):
        def log_message(self,*_): pass
    static=http.server.ThreadingHTTPServer(('127.0.0.1',0),functools.partial(Quiet,directory=str(Path(args.webui).resolve())))
    threading.Thread(target=static.serve_forever,daemon=True).start()
    try:
        with HictTestServer(args.jar,data,output/'server.log') as app,sync_playwright() as pw:
            app.http('/open',{'filename':native.name});app.http('/link_fasta',{'fastaFilename':'sequence.fa'})
            browser=pw.chromium.launch(headless=True)
            page=browser.new_page(viewport={'width':1600,'height':1100},accept_downloads=True)
            page.set_default_timeout(30000);page.add_init_script('window.__HICT_BENCHMARK_ENABLED__=true')
            page.route('http://localhost:5000/**',lambda route:route.continue_(url=app.url+route.request.url.removeprefix('http://localhost:5000')))
            check=BrowserCheck(page,output)
            page.goto(f'http://127.0.0.1:{static.server_port}/');check.menu('File','Attach');check.settle()
            def state():
                return page.evaluate('''() => {const m=window.__hictBenchmark.manager(),l=m.viewAndLayersManager;
                    return {grid:l.currentViewState.resolutionDesciptor,view:l.captureViewPosition(),lengths:m.contigDimensionHolder.contigDescriptors.map(d=>d.contigLengthBp),
                        sizes:[...l.imageSizes],sequence:l.isSequenceScale};}''')
            def focus(bp,resolution=1/2000):
                page.evaluate('''p=>{const m=window.__hictBenchmark.manager();m.viewAndLayersManager.restoreViewPosition({centerBp:[p[0],p[0]],resolution:p[1],rotation:0});}''',[bp,resolution])
                check.settle()
            def scissors(bp):
                focus(bp)
                button=page.get_by_role('button',name='Split contig: double-click a base boundary',exact=False)
                if button.count(): button.click()
                point=page.evaluate('''bp=>{const m=window.__hictBenchmark.manager(),d=m.viewAndLayersManager.currentViewState.resolutionDesciptor;
                    const x=m.contigDimensionHolder.getFractionalPxOfBp(bp,d.bpResolution)*d.pixelResolution;
                    const p=m.getMap().getPixelFromCoordinate([x,-x]),r=m.getMap().getViewport().getBoundingClientRect();
                    return [r.left+p[0],r.top+r.height/2];}''',bp)
                print('Scissors',bp,point,state(),flush=True)
                page.screenshot(path=str(output/f'before-cut-{bp}.png'))
                with page.expect_response('**/split_contig_at_bp') as response: page.mouse.dblclick(*point,delay=80)
                assert response.value.ok,response.value.text()
                page.keyboard.press('Escape');check.settle()
                return response.value.request.post_data_json
            assert not state()['sequence'],state()
            before=state()
            first=scissors(253);assert first['offsetBp']==253,first
            assert state()['lengths']==[253,3747],state()
            second=scissors(1);assert second['offsetBp']==1,second
            assert state()['lengths']==[1,252,3747],state()
            assert sorted(state()['sizes'])==[22,42],state()
            page.keyboard.press('Control+z');check.settle()
            assert state()['lengths']==[253,3747],state()
            page.keyboard.press('Control+y');check.settle()
            assert state()['lengths']==[1,252,3747],state()
            page.evaluate('''()=>window.__hictBenchmark.manager().viewAndLayersManager.fitViewToActiveExtent()''');check.settle()
            page.evaluate('''()=>{const l=window.__hictBenchmark.manager().viewAndLayersManager;
                const source=l.layersHolder.bpResolutionToContigBordersLayer.get(l.currentViewState.resolutionDesciptor.bpResolution).getSource();
                l.selectionCollections.selectedContigFeatures.extend(source.getFeatures().filter(f=>f.get('trackType')==='contigBorders'));}''')
            with page.expect_response('**/reverse_selection_range') as response:
                page.get_by_role('button',name='Reverse Selection',exact=True).click()
            assert response.value.ok,response.value.text()
            focus(3990)
            assert state()['lengths']==[3747,252,1],state()
            source_sequence=''.join((data/'sequence.fa').read_text().splitlines()[1:])
            expected_sequence=source_sequence.translate(str.maketrans('ACGTRYMKSWBDHVN','TGCAYRKMSWVHDBN'))[::-1]
            assert json.loads(app.http('/sequence/region',{'startBp':0,'endBp':4000}))['sequence']==expected_sequence
            assert state()['sequence'] and state()['grid']['sourceBpResolution']==100,state()
            page.get_by_test_id('sequence-scale-status').wait_for()
            assert 'Base scale' in page.get_by_test_id('sequence-scale-status').inner_text()
            geometry=page.evaluate('''()=>{const m=window.__hictBenchmark.manager(),l=m.viewAndLayersManager,h=m.contigDimensionHolder,d=l.currentViewState.resolutionDesciptor;
                const screen=bp=>m.getMap().getPixelFromCoordinate([h.getFractionalPxOfBp(bp,1)*d.pixelResolution,0])[0];
                return {baseWidths:[3997,3998,3999].map(bp=>screen(bp+1)-screen(bp)),center:l.captureViewPosition().centerBp,
                    native:h.getNativeCoordinates().prefix_sum_px.get(100).at(-1)};}''')
            assert all(abs(width-20)<1e-6 for width in geometry['baseWidths']),geometry
            assert geometry['native']==42,geometry
            page.evaluate('''async()=>{const t=window.__hictBenchmark.manager().linearTrackManager;await t.openTrack('landmark.bed','Landmarks');await t.clearCachesAndRender();}''')
            check.settle();focus(3990)
            page.screenshot(path=str(output/'tracks-loaded.png'))
            (output/'track-diagnostics.json').write_text(json.dumps(page.evaluate('''()=>{
                const t=window.__hictBenchmark.manager().linearTrackManager;
                return {snapshots:t.lastRenderedSnapshot,errors:[]};}'''),indent=2))
            page.wait_for_function('''()=>window.__hictBenchmark.manager().linearTrackManager.lastRenderedSnapshot.horizontal?.featureHits.some(h=>h.bin.label==='landmark')''')
            hits=page.evaluate('''()=>window.__hictBenchmark.manager().linearTrackManager.lastRenderedSnapshot.horizontal.featureHits.map(h=>({label:h.bin.label,start:h.bin.startBp,end:h.bin.endBp,width:h.axisEnd-h.axisStart}))''')
            landmark=next(hit for hit in hits if hit['label']=='landmark')
            assert (landmark['start'],landmark['end'])==(3994,3997),hits
            assert abs(landmark['width']-60)<=2,hits
            page.screenshot(path=str(output/'one-base-fragment-and-tracks.png'))
            saved_view=state()['view']
            focus(2000);interior_view=state()['view']
            focus(2000,.006);assert not state()['sequence'],state()
            native_view=state()['view'];assert max(abs(a-b) for a,b in zip(native_view['centerBp'],interior_view['centerBp']))<1e-7,(native_view,interior_view)
            page.evaluate('''saved=>window.__hictBenchmark.manager().viewAndLayersManager.restoreViewPosition(saved)''',saved_view);check.settle()
            assert state()['sequence'],state()
            assert max(abs(a-b) for a,b in zip(state()['view']['centerBp'],saved_view['centerBp']))<1e-7,state()
            focus(2000,.006)
            pointer=page.evaluate('''()=>{const m=window.__hictBenchmark.manager(),r=m.getMap().getViewport().getBoundingClientRect();
                const pixel=[r.width/2+61,r.height/2+43];window.__sequenceWheelClient=[Math.round(r.left+pixel[0]),Math.round(r.top+pixel[1])];
                return window.__sequenceWheelClient;}''')
            def pointer_bp():
                return page.evaluate('''()=>{const m=window.__hictBenchmark.manager(),d=m.viewAndLayersManager.currentViewState.resolutionDesciptor;
                    const r=m.getMap().getViewport().getBoundingClientRect(),c=window.__sequenceWheelClient;
                    return m.getMap().getCoordinateFromPixel([c[0]-r.left,c[1]-r.top]).map((x,i)=>m.contigDimensionHolder.getFractionalBpOfPx((i?-x:x)/d.pixelResolution,d.bpResolution));}''')
            anchor=pointer_bp();page.mouse.move(*pointer)
            wheel_steps=0
            for _ in range(16):
                previous=state()['view']['resolution']
                page.mouse.wheel(0,-120)
                page.wait_for_function('previous=>window.__hictBenchmark.manager().getView().getResolution()!==previous',arg=previous)
                check.settle()
                wheel_steps+=1
                print('Wheel anchor',pointer_bp(),anchor,flush=True)
                assert max(abs(a-b) for a,b in zip(pointer_bp(),anchor))<1e-5,(pointer_bp(),anchor)
                if page.locator('canvas.sequence-axis.horizontal').is_visible():break
            assert state()['sequence'],state()
            assert max(abs(a-b) for a,b in zip(pointer_bp(),anchor))<1e-5,(pointer_bp(),anchor)
            page.locator('canvas.sequence-axis.horizontal').wait_for(state='visible')
            wheel_view=state()
            # The overview keeps its native source extent and translates clicks through assembly BP.
            mini=page.evaluate('''()=>{const m=window.__hictBenchmark.manager(),l=m.viewAndLayersManager,d=m.minimapResolutionDescriptor;
                const h=m.contigDimensionHolder.getNativeCoordinates(),x=h.getFractionalPxOfBp(3800,d.bpResolution)*d.pixelResolution;
                const p=m.minimap.getPixelFromCoordinate([x,-x]),r=m.minimap.getViewport().getBoundingClientRect();
                return {point:[r.left+p[0],r.top+p[1]],nativeExtent:m.minimap.getView().getProjection().getExtent(),
                    separateProjection:m.minimap.getView().getProjection()!==m.getView().getProjection()};}''')
            assert mini['separateProjection'],mini
            page.mouse.click(*mini['point']);check.settle()
            # Chromium's fractional overview click coordinates have subpixel rounding.
            assert max(abs(bp-3800) for bp in state()['view']['centerBp'])<1e-3,state()
            page.evaluate('''saved=>window.__hictBenchmark.manager().viewAndLayersManager.restoreViewPosition(saved)''',saved_view);check.settle()
            mode=page.locator('select').filter(has=page.locator('option[value="EXPECTED"]'))
            mode.select_option('OBSERVED_OVER_EXPECTED');check.settle()
            mode.select_option('OBSERVED');check.settle()
            exports=[]
            for format in ('Png','Svg','Pdf'):
                with page.expect_download() as download:
                    page.evaluate('''format=>window.__hictBenchmark.manager()['exportCurrentMap'+format](undefined,{scope:'viewport',cropToMap:true})''',format)
                target=output/('sequence-view.'+format.lower());download.value.save_as(target)
                content=target.read_bytes()
                if format=='Png':assert content.startswith(b'\x89PNG\r\n\x1a\n')
                if format=='Svg':assert b'<svg' in content
                if format=='Pdf':assert content.startswith(b'%PDF-')
                assert len(content)>1000
                check.settle()
                assert state()['sequence'],state()
                assert max(abs(a-b) for a,b in zip(state()['view']['centerBp'],saved_view['centerBp']))<1e-7,state()
                exports.append({'format':format,'bytes':len(content),'sha256':hashlib.sha256(content).hexdigest()})
            with page.expect_download() as download: check.menu('File','Save session')
            session=output/'sequence-view-session.json';download.value.save_as(session)
            saved=json.loads(session.read_text())
            assert saved['client']['workspace']['view']['displayGrid']=='SEQUENCE_BP'
            check.menu('File','Close')
            with page.expect_file_chooser() as chooser:check.menu('File','Open session')
            chooser.value.set_files(session)
            page.get_by_text('Session restored. Undo history starts from the restored assembly.',exact=True).last.wait_for();check.settle()
            restored=state()
            assert restored['lengths']==[3747,252,1] and restored['sequence'],restored
            assert json.loads(app.http('/sequence/region',{'startBp':0,'endBp':4000}))['sequence']==expected_sequence
            assert max(abs(a-b) for a,b in zip(restored['view']['centerBp'],saved['client']['workspace']['view']['centerBp']))<1e-7,restored
            assert hashlib.sha256(native.read_bytes()).hexdigest()==digest
            assert not check.errors,check.errors
            page.screenshot(path=str(output/'restored-sequence-view.png'))
            (output/'results.json').write_text(json.dumps({'status':'ok','initial':before,'geometry':geometry,'hits':hits,
                'restored':restored,'wheelView':wheel_view,'wheelStepsToLetters':wheel_steps,'overview':mini,'exports':exports,'pageErrors':check.errors,'sourceSha256':digest,
                'checks':['native/base transitions','real exact scissors','one-base fragment','reversal','uniform base widths',
                          'native raster extents','1D feature alignment','native/base saved view anchors','wheel pointer anchor','visible FASTA letters',
                          'native overview projection and click','keyboard undo/redo','O/E display','PNG/SVG/PDF viewport export and restored view',
                          'session save/restore','all 4000 IUPAC bases retained in reverse complement','native data unchanged']},indent=2))
            browser.close()
    finally:static.shutdown();static.server_close()


if __name__=='__main__':main()
