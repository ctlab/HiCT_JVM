#!/usr/bin/env python3
"""Qualify separated track/source controls, keyboard access and resized canvas pixels."""
import argparse
import functools
import hashlib
import http.server
import json
import math
from pathlib import Path
import shutil
import subprocess
import sys
import threading
from playwright.sync_api import expect,sync_playwright
from sequence_browser_harness import BrowserCheck,HictTestServer


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    for key in ('jar','webui','data-dir','output'):parser.add_argument('--'+key,required=True)
    parser.add_argument('--hdf-python',default=sys.executable)
    args=parser.parse_args();data,out=Path(args.data_dir).absolute(),Path(args.output).absolute();jar=Path(args.jar).resolve()
    if any(p!=p.resolve() or not p.is_relative_to('/tmp') for p in (data,out)):parser.error('Use fresh nonsymlinked disposable /tmp paths')
    data.mkdir(parents=True,exist_ok=False);out.mkdir(parents=True,exist_ok=False)
    scripts=Path(__file__).resolve().parent
    with (out/'fixture.log').open('w') as log:
        subprocess.run(['java','--class-path',str(jar),str(scripts.parent/'src/test/java/ru/itmo/ctlab/hict/fixtures/ReinforcementHdf5Fixture.java'),str(data)],check=True,stdout=log,stderr=subprocess.STDOUT)
        subprocess.run([args.hdf_python,str(scripts/'prepare_reinforcement_sources.py'),'--data-dir',str(data)],check=True,stdout=log,stderr=subprocess.STDOUT)
    legacy=data/'legacy-unknown.hict.hdf5';shutil.copyfile(data/'tiny-primary.hict.hdf5',legacy)
    subprocess.run([args.hdf_python,'-c',"""import h5py,sys
with h5py.File(sys.argv[1],'r+') as f:
 for key in ['contact_data_state','hict_score_scale','hict_source_metadata_version','hict_source_units']:
  if key in f.attrs:del f.attrs[key]
 for resolution in f['resolutions'].values():
  if 'hict_imported_weights' in resolution.attrs:del resolution.attrs['hict_imported_weights']
""",str(legacy)],check=True)
    (data/'workspace-annotations.bed').write_text('tiny\t0\t3\tShort feature\t42\t+\ntiny\t7\t16\tLong feature\t24\t-\n')
    shutil.copyfile(data/'tiny-weighted.hict.hdf5',data/'workspace-secondary.hict.hdf5')
    hashes={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in data.glob('*.hict.hdf5')}
    class Quiet(http.server.SimpleHTTPRequestHandler):
        def log_message(self,*_):pass
    web=http.server.ThreadingHTTPServer(('127.0.0.1',0),functools.partial(Quiet,directory=str(Path(args.webui).resolve())))
    threading.Thread(target=web.serve_forever,daemon=True).start();url=f'http://127.0.0.1:{web.server_port}/'
    rows=[];geometry=[];coverage_oracle={}
    try:
        with HictTestServer(jar,data,out/'server.log') as api:
            # Existing numeric/feature/background/precompute/error acceptance remains independent.
            with (out/'controls.log').open('w') as log:
                subprocess.run([sys.executable,str(scripts/'check_track_controls_browser.py'),'--url',url,'--api',api.url,'--data-dir',str(data),'--output',str(out/'controls')],check=True,stdout=log,stderr=subprocess.STDOUT)
            with sync_playwright() as pw:
                browser=pw.chromium.launch(headless=True);page=browser.new_page(viewport={'width':1440,'height':1000})
                page.set_default_timeout(30000);page.add_init_script('window.__HICT_BENCHMARK_ENABLED__=true')
                page.add_init_script("window.__workspaceErrors=[];window.addEventListener('error',event=>{if(event instanceof ErrorEvent)window.__workspaceErrors.push(event.message);},true)")
                page.route('http://localhost:5000/**',lambda r:r.continue_(url=api.url+r.request.url.removeprefix('http://localhost:5000')))
                check=BrowserCheck(page,out);dialog=page.locator('.track-manager-root [role=dialog]').first
                def manager():check.menu('View','1D tracks…');dialog.wait_for()
                def tab(name):
                    if dialog.get_by_role('heading',name=name,exact=True).is_visible(): return
                    close();check.menu('View',name+'…');dialog.wait_for()
                    if name=='1D tracks': dialog.get_by_label('Add track',exact=True).select_option('weights')
                def close():dialog.get_by_role('button',name='Close',exact=True).click();dialog.wait_for(state='hidden');check.settle()
                def browse(title,filename):
                    dialog.get_by_role('button',name='Browse…',exact=True).click()
                    picker=page.get_by_role('dialog',name=title,exact=True)
                    picker.locator('.file-name-cell').get_by_text(filename,exact=True).dblclick();picker.wait_for(state='hidden')
                def hold_real_list():
                    page.evaluate("""async()=>{const m=window.__hictBenchmark.manager().linearTrackManager,rm=window.__hictBenchmark.manager().networkManager.requestManager;
                        const original=rm.listTracks,snapshot=await original.call(rm),probe={original};window.__heldTrackList=probe;
                        rm.listTracks=()=>{rm.listTracks=original;return new Promise(resolve=>{probe.release=()=>resolve(snapshot);});};
                        probe.pending=m.refreshTrackList(false);
                    }""")
                def release_real_list():
                    page.evaluate("""async()=>{const probe=window.__heldTrackList;if(!probe)return;
                        window.__hictBenchmark.manager().networkManager.requestManager.listTracks=probe.original;
                        probe.release?.();await probe.pending;delete window.__heldTrackList;}""")
                try:
                    page.goto(url);page.evaluate("api=>window.__hictBenchmark.open('tiny-weighted.hict.hdf5',api)",api.url);check.settle();manager()
                    def tabs():
                        expect(dialog.get_by_role('tab')).to_have_count(0)
                        expect(dialog.get_by_role('button',name='Attach',exact=True)).not_to_be_visible()
                        tab('Sources and layers')
                        expect(dialog.get_by_role('button',name='Attach',exact=True)).to_be_visible()
                        expect(dialog.get_by_label('Track file',exact=True)).not_to_be_visible()
                        tab('1D tracks')
                        dialog.get_by_label('Add track',exact=True).select_option('file')
                        dialog.get_by_label('Display name',exact=True).fill('Workspace annotations')
                    check.step('separate-track-and-source-managers',tabs)
                    def add_file():
                        browse('Select 1D track file','workspace-annotations.bed')
                        dialog.get_by_role('button',name='Add file',exact=True).click()
                        expect(dialog.get_by_role('textbox',name='Track name for Workspace annotations',exact=True)).to_be_visible()
                        assert len(json.loads(api.http('/tracks/list',{})))==1
                        page.screenshot(path=str(out/'manager-file-add.png'))
                    check.step('file-add-uses-the-1d-tab',add_file)
                    def source_and_weights():
                        dialog.get_by_label('Add track',exact=True).select_option('weights')
                        expect(dialog.locator('#track-weight-source option[value=SECONDARY]')).to_be_disabled()
                        button=dialog.get_by_role('button',name='Add weights',exact=True);button.click();expect(button).to_be_disabled();expect(dialog.get_by_role('textbox',name='Track name for Map weights - Primary',exact=True)).to_be_visible()
                        tab('Sources and layers');browse('Select secondary data source','workspace-secondary.hict.hdf5')
                        dialog.get_by_role('button',name='Attach',exact=True).click()
                        expect(dialog.get_by_text('Attached: workspace-secondary.hict.hdf5',exact=True)).to_be_visible()
                        tab('1D tracks');dialog.get_by_label('Weights from',exact=True).select_option('SECONDARY');button.click();expect(button).to_be_disabled();expect(dialog.get_by_role('textbox',name='Track name for Map weights - Secondary',exact=True)).to_be_visible()
                        assert len(json.loads(api.http('/tracks/list',{})))==3
                    check.step('source-attachment-and-add-weights-for-both-maps',source_and_weights)
                    def signal():
                        row=dialog.locator('.track-row').filter(has=page.get_by_role('textbox',name='Track name for Map weights - Primary',exact=True))
                        expect(row.locator('input[title="Signal range minimum"]')).not_to_be_visible()
                        row.locator('summary').click();expect(row.locator('input[title="Signal range minimum"]')).to_be_visible()
                        log=row.locator('input[id^=track-log-scale-]');log.check()
                        base=row.locator('input[title="Log base for log(1+x)"]');base.fill('2');base.press('Tab')
                        auto=row.locator('input[id^=track-range-auto-]');auto.uncheck()
                        page.wait_for_function("()=>window.__hictBenchmark.manager().linearTrackManager.getTracksSnapshot().find(t=>t.name==='Map weights - Primary')?.rangeAuto===false")
                        hold_real_list()
                        try:
                            maximum=row.locator('input[title="Signal range maximum"]');maximum.fill('8')
                            release_real_list();expect(maximum).to_have_value('8')
                        finally:release_real_list()
                        maximum.press('Tab')
                        page.wait_for_function("()=>{const m=window.__hictBenchmark.manager().linearTrackManager,t=m.getTracksSnapshot().find(t=>t.name==='Map weights - Primary');return t?.logScale===true&&t?.rangeAuto===false&&t?.rangeMax===8&&m.getTrackLogBase(t.trackId)===2;}")
                        auto.check();log.uncheck();row.locator('summary').click()
                    check.step('signal-settings-have-a-deliberate-disclosure',signal)
                    def track_cards():
                        row=dialog.get_by_role('group',name='Track Workspace annotations',exact=True)
                        field=row.get_by_role('textbox',name='Track name for Workspace annotations',exact=True)
                        field.fill('Renamed annotations');field.press('Tab')
                        row=dialog.get_by_role('group',name='Track Renamed annotations',exact=True)
                        expect(row).to_be_visible()
                        toggle=row.get_by_role('checkbox',name='Show Renamed annotations',exact=True);toggle.uncheck()
                        page.wait_for_function("()=>window.__hictBenchmark.manager().linearTrackManager.getTracksSnapshot().find(t=>t.name==='Renamed annotations')?.visible===false")
                        toggle.check()
                        for axis in ['horizontal','vertical']:
                            row.get_by_role('button',name=f'Hide Renamed annotations {axis}ly',exact=True).click()
                            expect(row.get_by_role('button',name=f'Show Renamed annotations {axis}ly',exact=True)).to_be_visible()
                            assert page.evaluate("axis=>!window.__hictBenchmark.manager().linearTrackManager.getVisibleTracksForAxis(axis).some(t=>t.name==='Renamed annotations')",axis)
                            row.get_by_role('button',name=f'Show Renamed annotations {axis}ly',exact=True).click()
                        color=row.get_by_label('Color for Renamed annotations',exact=True)
                        color.evaluate("element=>{element.value='#16a34a';element.dispatchEvent(new Event('change',{bubbles:true}));}")
                        page.wait_for_function("()=>window.__hictBenchmark.manager().linearTrackManager.getTracksSnapshot().find(t=>t.name==='Renamed annotations')?.color==='#16a34a'")
                        row.get_by_role('button',name='Move Renamed annotations down',exact=True).click()
                        page.wait_for_function("()=>window.__hictBenchmark.manager().linearTrackManager.getTracksSnapshot()[1]?.name==='Renamed annotations'")
                        row.get_by_role('button',name='Move Renamed annotations up',exact=True).click()
                        page.wait_for_function("()=>window.__hictBenchmark.manager().linearTrackManager.getTracksSnapshot()[0]?.name==='Renamed annotations'")
                        page.evaluate("async()=>{const m=window.__hictBenchmark.manager().linearTrackManager;const t=m.getTracksSnapshot().find(t=>t.name==='Renamed annotations');await m.updateTrack(t.trackId,{name:'External annotation'});}")
                        row=dialog.get_by_role('group',name='Track External annotation',exact=True);expect(row).to_be_visible()
                        field=row.get_by_role('textbox',name='Track name for External annotation',exact=True);field.fill('Workspace annotations');field.press('Tab')
                        expect(dialog.get_by_role('group',name='Track Workspace annotations',exact=True)).to_be_visible()
                        dialog.get_by_label('Weights from',exact=True).select_option('PRIMARY')
                        dialog.get_by_role('group',name='Track Map weights - Primary',exact=True).get_by_role('button',name='Remove',exact=True).click()
                        button=dialog.get_by_role('button',name='Add weights',exact=True);expect(button).to_be_enabled();button.click()
                        expect(dialog.get_by_role('group',name='Track Map weights - Primary',exact=True)).to_be_visible()
                        assert len(json.loads(api.http('/tracks/list',{})))==3
                    check.step('track-card-rename-visibility-axes-color-order-remove-and-live-updates',track_cards)
                    def old_list():
                        hold_real_list()
                        try:
                            page.evaluate("async()=>{const m=window.__hictBenchmark.manager().linearTrackManager,t=m.getTracksSnapshot().find(t=>t.name==='Workspace annotations');await m.updateTrack(t.trackId,{name:'Newer annotations'});}")
                            expect(dialog.get_by_role('group',name='Track Newer annotations',exact=True)).to_be_visible()
                            release_real_list()
                            row=dialog.get_by_role('group',name='Track Newer annotations',exact=True);expect(row).to_be_visible()
                            field=row.get_by_role('textbox',name='Track name for Newer annotations',exact=True);field.fill('Workspace annotations');field.press('Tab')
                            expect(dialog.get_by_role('group',name='Track Workspace annotations',exact=True)).to_be_visible()
                        finally:release_real_list()
                    check.step('late-real-list-cannot-revert-a-newer-track-edit',old_list)


                    def fitting():
                        for width,height in [(1440,1000),(1024,768),(800,600)]:
                            page.set_viewport_size({'width':width,'height':height})
                            for name in ['1D tracks','Sources and layers']:
                                tab(name)
                                page.wait_for_function("""()=>{const d=document.querySelector('.track-manager-root .modal-content'),b=d?.querySelector('.modal-body');if(!d||!b)return false;const r=d.getBoundingClientRect();return r.top>=0&&r.bottom<=innerHeight+1&&r.left>=0&&r.right<=innerWidth+1&&b.scrollWidth<=b.clientWidth+1;}""")
                                page.screenshot(path=str(out/f'manager-{width}-{name.split()[0]}.png'))
                        page.set_viewport_size({'width':1440,'height':1000})
                    check.step('manager-fits-three-window-sizes-without-horizontal-clipping',fitting)
                    def layers():
                        tab('Sources and layers');dialog.get_by_role('button',name='Borders and labels…',exact=True).click();dialog.wait_for(state='hidden')
                        control=page.get_by_role('button',name='Borders and labels',exact=True)
                        expect(control).to_have_attribute('aria-expanded','true');expect(control).to_be_focused()
                        control.click();page.get_by_role('button',name='Hide side panel',exact=True).click()
                        manager();tab('Sources and layers');dialog.get_by_role('button',name='Borders and labels…',exact=True).click();dialog.wait_for(state='hidden')
                        expect(control).to_have_attribute('aria-expanded','true');expect(control).to_be_focused()
                        control.click();check.settle()
                    check.step('map-layer-action-opens-and-focuses-the-existing-sidebar',layers)
                    page.get_by_role('combobox',name='Visualization preset',exact=True).select_option(label='Red on black');check.settle()
                    def geometry_matches():
                        page.wait_for_function("""()=>{const m=window.__hictBenchmark.manager(),t=m.getMap().getTargetElement(),s=m.getMap().getSize();return s[0]===t.clientWidth&&s[1]===t.clientHeight&&['horizontal','vertical'].every(a=>{const h=document.querySelector('#'+a+'-igv-track-div'),c=h?.querySelector('canvas');return c&&c.width===h.clientWidth&&c.height===h.clientHeight;});}""")
                        return page.evaluate("""()=>{const m=window.__hictBenchmark.manager();return {map:m.getMap().getSize(),corners:['.interactive-workspace_corner_tracks_ruler','.interactive-workspace_corner_ruler_tracks'].map(s=>getComputedStyle(document.querySelector(s)).backgroundColor),axes:['horizontal','vertical'].map(a=>{const h=document.querySelector('#'+a+'-igv-track-div'),c=h.querySelector('canvas'),ctx=c.getContext('2d');const pts=a==='horizontal'?[[5,c.height-3],[Math.floor(c.width/2),c.height-3],[c.width-5,c.height-3]]:[[c.width-3,5],[c.width-3,Math.floor(c.height/2)],[c.width-3,c.height-5]];return {axis:a,host:[h.clientWidth,h.clientHeight],canvas:[c.width,c.height],pixels:pts.map(p=>Array.from(ctx.getImageData(...p,1,1).data))};})};}""")
                    def resizes():
                        for size in [80,240,140]:
                            for axis in ['horizontal','vertical']:
                                handle=page.get_by_role('separator',name='Horizontal tracks panel height' if axis=='horizontal' else 'Vertical tracks panel width',exact=True)
                                previous=float(handle.get_attribute('aria-valuenow'));box=handle.bounding_box();x,y=box['x']+box['width']/2,box['y']+box['height']/2
                                page.mouse.move(x,y);page.mouse.down();page.mouse.move(x+(size-previous if axis=='vertical' else 0),y+(size-previous if axis=='horizontal' else 0),steps=12);page.mouse.up();check.settle()
                            item=geometry_matches();geometry.append({'size':size,**item})
                            assert all(pixel[3]==255 and pixel[:3]!=[255,255,255] for a in item['axes'] for pixel in a['pixels']),item
                            assert item['axes'][0]['canvas'][0]==item['map'][0] and item['axes'][1]['canvas'][1]==item['map'][1],item
                            assert item['corners']==['rgb(0, 0, 0)','rgb(0, 0, 0)'],item
                            page.screenshot(path=str(out/f'resized-{size}.png'))
                        for axis in ['horizontal','vertical']:
                            handle=page.get_by_role('separator',name='Horizontal tracks panel height' if axis=='horizontal' else 'Vertical tracks panel width',exact=True)
                            handle.focus();page.keyboard.press('End')
                        page.set_viewport_size({'width':800,'height':600})
                        page.wait_for_function("()=>[...document.querySelectorAll('[role=separator][aria-valuenow]')].every(e=>Number(e.getAttribute('aria-valuenow'))<=Number(e.getAttribute('aria-valuemax')))")
                        geometry_matches();page.screenshot(path=str(out/'resized-window-800.png'))
                        page.set_viewport_size({'width':1440,'height':1000});check.settle()
                        horizontal=page.get_by_role('separator',name='Horizontal tracks panel height',exact=True);horizontal.focus();before=int(horizontal.get_attribute('aria-valuenow'));page.keyboard.press('ArrowDown');expect(horizontal).to_have_attribute('aria-valuenow',str(before+10));geometry_matches()
                        page.keyboard.press('Home');expect(horizontal).to_have_attribute('aria-valuenow','60');geometry_matches()
                        vertical=page.get_by_role('separator',name='Vertical tracks panel width',exact=True);vertical.focus();before=int(vertical.get_attribute('aria-valuenow'));page.keyboard.press('ArrowRight');expect(vertical).to_have_attribute('aria-valuenow',str(before+10));geometry_matches()
                        box=vertical.bounding_box();page.mouse.move(box['x']+2,box['y']+2);page.mouse.down();page.mouse.move(box['x']+22,box['y']+2);page.evaluate("window.dispatchEvent(new Event('blur'))");page.mouse.up();geometry_matches();assert page.evaluate('document.body.style.cursor')==''
                    check.step('three-panel-sizes-dark-edge-pixels-keyboard-and-blur-cleanup',resizes)
                    def weight_sources():
                        page.locator('#normalization-mode').select_option('sqrt')
                        page.wait_for_function("window.__hictBenchmark.manager().networkManager.requestManager.getNormalizationWeightsMode('PRIMARY')==='SQRT_COVERAGE'")
                        assert json.loads(api.http('/normalization/coverage/status', {'source': 'SECONDARY'}))['mode'] == 'FILE'
                        check.settle();manager();dialog.get_by_label('Add track',exact=True).select_option('weights')
                        expect(dialog.get_by_text('Square-root coverage weights from the selected map.',exact=False)).to_be_visible()
                        expect(dialog.get_by_text('Coverage weights · Primary',exact=True)).to_be_visible()
                        page.screenshot(path=str(out/'coverage-manager.png'))
                        values=json.loads(api.http('/tracks/query_1d',{'unit':'BP','startBP':0,'endBP':20,'widthPx':5,'bpResolution':4}))
                        weight=next(t for t in values['tracks'] if t['trackId']==next(t['trackId'] for t in json.loads(api.http('/tracks/list',{})) if t['sourceFile']=='__internal__/cooler_weights/PRIMARY'))
                        raw=[[1,3,0,0,0],[3,0,7,0,0],[0,7,0,0,0],[0,0,0,0,0],[0,0,0,0,11]]
                        row_sums=[sum(row) for row in raw];total=sum(row_sums)
                        unscaled=sum(raw[i][j]/math.sqrt(row_sums[i]*row_sums[j]) for i in range(5) for j in range(5) if row_sums[i]>0 and row_sums[j]>0)
                        scale=total/unscaled
                        expected_weights=[math.sqrt(scale/r) for r in row_sums if r>0]
                        actual_weights=[b['value'] for b in weight['bins']]
                        assert len(actual_weights)==len(expected_weights)==4,actual_weights
                        assert all(math.isclose(a,b,rel_tol=1e-12) for a,b in zip(actual_weights,expected_weights)),(actual_weights,expected_weights)
                        close()
                        requests = 'window.__hictBenchmark.manager().networkManager.requestManager'
                        page.evaluate(f"{requests}.prepareCoverageNormalization('SECONDARY')")
                        page.wait_for_function(f"async () => (await {requests}.coverageNormalizationStatus('SECONDARY')).state === 'READY'")
                        page.evaluate(f"{requests}.selectNormalizationWeights('SQRT_COVERAGE', 'SECONDARY')")
                        for source in ['PRIMARY','SECONDARY']:
                            queries=page.evaluate("""async source=>{const q={bpResolution:4,startRowPx:0,endRowPx:5,startColPx:0,endColPx:5,source},api=window.__hictBenchmark.manager().networkManager.requestManager;
                                return {raw:Array.from((await api.queryMatrixFloat32({...q,signalMode:'RAW_COUNTS'})).values),normalized:Array.from((await api.queryMatrixFloat32({...q,signalMode:'COOLER_WEIGHTED'})).values)};}""",source)
                            assert queries['raw']==[v for row in raw for v in row],queries
                            assert len(queries['normalized'])==25,queries
                            expected=[scale*raw[i][j]/math.sqrt(row_sums[i]*row_sums[j]) if row_sums[i]>0 and row_sums[j]>0 else 0 for i in range(5) for j in range(5)]
                            assert all(math.isclose(a,b,rel_tol=1e-6,abs_tol=1e-6) for a,b in zip(queries['normalized'],expected)),queries
                            assert math.isclose(sum(queries['normalized']),total,rel_tol=1e-6),queries
                            coverage_oracle[source]=queries
                        coverage_oracle.update(rowSums=row_sums,total=total,scale=scale,weights=actual_weights)
                        explanation=page.locator('.coverage-explanation');summary=explanation.locator('summary')
                        expect(page.locator('#normalization-help')).to_be_visible()
                        summary.focus();page.keyboard.press('Enter');expect(explanation).to_have_attribute('open','')
                        for width,height in [(1440,1000),(800,600)]:
                            page.set_viewport_size({'width':width,'height':height});explanation.scroll_into_view_if_needed()
                            assert explanation.evaluate("e=>{const r=e.getBoundingClientRect();return r.left>=0&&r.right<=innerWidth;}"),width
                            page.screenshot(path=str(out/f'coverage-explanation-{width}.png'))
                        summary.focus();page.keyboard.press('Enter');expect(explanation).not_to_have_attribute('open','')
                        page.set_viewport_size({'width':1440,'height':1000})
                        page.screenshot(path=str(out/'coverage-map.png'));page.locator('#normalization-mode').select_option('file')
                        expect(explanation).to_have_count(0)
                        page.wait_for_function("window.__hictBenchmark.manager().networkManager.requestManager.getNormalizationWeightsMode('PRIMARY')==='FILE'")
                        assert json.loads(api.http('/normalization/coverage/status', {'source': 'SECONDARY'}))['mode'] == 'SQRT_COVERAGE'
                        check.settle();manager();dialog.get_by_label('Add track',exact=True).select_option('weights')
                        expect(dialog.get_by_text('Stored Cooler weights from the selected map.',exact=False)).to_be_visible()
                        expect(dialog.get_by_text('File weights · Primary',exact=True)).to_be_visible();close()
                    check.step('weight-hints-and-records-identify-file-versus-computed-source',weight_sources)
                    def late_poll():
                        held=[];requests=[]
                        def hold(route):requests.append(route.request.url);held.append(route)
                        page.route('**/tracks/precompute/status',hold)
                        try:
                            manager();page.wait_for_timeout(100);assert len(held)==1
                            close();held.pop().continue_();page.wait_for_timeout(2700)
                            assert len(requests)==1,requests
                        finally:
                            for route in held:route.abort()
                            page.unroute('**/tracks/precompute/status',hold)
                    check.step('closed-manager-does-not-start-a-late-polling-loop',late_poll)
                    def unavailable_weights():
                        page.evaluate("api=>window.__hictBenchmark.open('tiny-identity.hict.hdf5',api)",api.url);check.settle();manager()
                        dialog.get_by_label('Add track',exact=True).select_option('weights')
                        expect(dialog.get_by_role('button',name='Add weights',exact=True)).to_be_disabled()
                        reason=page.evaluate('window.__hictBenchmark.manager().networkManager.requestManager.sourceCapabilities.PRIMARY.importedWeights.reason')
                        assert reason;expect(dialog.get_by_text(reason,exact=True)).to_be_visible()
                        assert json.loads(api.http('/tracks/list',{}))==[]
                        close()
                    check.step('known-unavailable-source-weights-have-a-readable-reason',unavailable_weights)
                    def legacy_weights():
                        page.evaluate("api=>window.__hictBenchmark.open('legacy-unknown.hict.hdf5',api)",api.url);check.settle();manager()
                        dialog.get_by_label('Add track',exact=True).select_option('weights')
                        expect(dialog.get_by_role('button',name='Add weights',exact=True)).to_be_enabled()
                        expect(dialog.get_by_text('Weight provenance is unknown for this file.',exact=False)).to_be_visible()
                        dialog.get_by_role('button',name='Add weights',exact=True).click()
                        expect(dialog.get_by_role('group',name='Track Map weights - Primary',exact=True)).to_be_visible()
                        advanced = dialog.locator('details[data-advanced-id="tracks"]')
                        if advanced.get_attribute('open') is None:
                            advanced.locator(':scope > summary').click()
                        expect(dialog.get_by_text('Map weights are read directly and need no preparation.',exact=True)).to_be_visible()
                        expect(dialog.get_by_role('button',name='Precompute all',exact=True)).to_be_disabled()
                        close()
                    check.step('legacy-unknown-weight-provenance-remains-available',legacy_weights)

                    window_errors=page.evaluate('window.__workspaceErrors');assert not window_errors,window_errors
                    assert not check.errors,check.errors
                    assert all(hashlib.sha256((data/name).read_bytes()).hexdigest()==digest for name,digest in hashes.items())
                    rows=check.rows
                    (out/'results.json').write_text(json.dumps({'status':'ok','checks':rows,'geometry':geometry,'coverageOracle':coverage_oracle,'sourceHashes':hashes,'pageErrors':check.errors,'windowErrors':window_errors},indent=2))
                except Exception:
                    page.screenshot(path=str(out/'failed.png'));raise
                finally:browser.close()
    finally:web.shutdown();web.server_close()


if __name__=='__main__':main()
