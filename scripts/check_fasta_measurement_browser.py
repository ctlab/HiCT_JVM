#!/usr/bin/env python3
"""Real FASTA-strip selection, strand copy, repeat jobs and edit invalidation."""
import argparse
import functools
import hashlib
import http.server
import json
from pathlib import Path
import random
import subprocess
import threading
from playwright.sync_api import sync_playwright
from sequence_browser_harness import HictTestServer, BrowserCheck


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    for key in ('jar','webui','data-dir','output'):parser.add_argument('--'+key,required=True)
    args=parser.parse_args();data,out=Path(args.data_dir).absolute(),Path(args.output).absolute()
    if any(p!=p.resolve() or not p.is_relative_to('/tmp') for p in (data,out)):parser.error('Use fresh nonsymlinked disposable /tmp directories')
    data.mkdir(parents=True,exist_ok=False);out.mkdir(parents=True,exist_ok=False)
    with (out/'fixture.log').open('w') as log:
        subprocess.run(['java','--class-path',str(Path(args.jar).resolve()),str(Path(__file__).with_name('SequenceViewBrowserFixture.java')),str(data)],check=True,stdout=log,stderr=subprocess.STDOUT)
    rng=random.Random(731);sequence=list(''.join(rng.choice('ACGT') for _ in range(4000)))
    for start,bases in [(100,'CTCTCTCTCTCT'),(200,'CTCTATCTCTCT'),(300,'NNNNNNNNNNNN')]:sequence[start:start+len(bases)]=bases
    original=''.join(sequence);(data/'sequence.fa').write_text('>tiny\n'+original+'\n')
    native=data/'sequence.hict.hdf5';digest=hashlib.sha256(native.read_bytes()).hexdigest()
    class Quiet(http.server.SimpleHTTPRequestHandler):
        def log_message(self,*_):pass
    static=http.server.ThreadingHTTPServer(('127.0.0.1',0),functools.partial(Quiet,directory=str(Path(args.webui).resolve())))
    threading.Thread(target=static.serve_forever,daemon=True).start()
    try:
        with HictTestServer(args.jar,data,out/'server.log') as app,sync_playwright() as pw:
            app.http('/open',{'filename':native.name});app.http('/link_fasta',{'fastaFilename':'sequence.fa'})
            browser=pw.chromium.launch(headless=True)
            page=browser.new_page(viewport={'width':1600,'height':1100},accept_downloads=True)
            page.context.grant_permissions(['clipboard-read','clipboard-write'])
            page.set_default_timeout(30000);page.add_init_script('window.__HICT_BENCHMARK_ENABLED__=true')
            page.route('http://localhost:5000/**',lambda r:r.continue_(url=app.url+r.request.url.removeprefix('http://localhost:5000')))
            check=BrowserCheck(page,out);submitted=[];results=[]
            page.on('request',lambda r:submitted.append({'body':r.post_data_json,'headers':r.headers}) if r.url.endswith('/sequence/repeat_period') and 'startBp' in r.post_data_json else None)
            page.goto(f'http://127.0.0.1:{static.server_port}/');check.menu('File','Join running session');check.settle()
            def state():return page.evaluate('''()=>{const l=window.__hictBenchmark.manager().viewAndLayersManager;return {selection:l.currentViewState.sequenceMeasurement??null,region:l.getRegionBounds()??null,entities:l.currentViewState.selectedContigIds,view:l.captureViewPosition()};}''')
            def focus(bp):
                page.evaluate('''bp=>window.__hictBenchmark.manager().viewAndLayersManager.restoreViewPosition({centerBp:[bp,bp],resolution:1/2000})''',bp);check.settle()
                page.wait_for_function('''bp=>{const c=document.querySelector('canvas.sequence-axis.horizontal');return c?.dataset.bases?.length>0 && +c.dataset.firstBp<=bp && +c.dataset.endBp>bp;}''',arg=bp)
            def point(bp,axis='horizontal',reverse=False):
                return page.evaluate('''p=>{const m=window.__hictBenchmark.manager(),d=m.viewAndLayersManager.currentViewState.resolutionDesciptor;
                    const v=m.contigDimensionHolder.getFractionalPxOfBp(p.bp+.5,d.bpResolution)*d.pixelResolution;
                    const pixel=m.getMap().getPixelFromCoordinate(p.axis==='horizontal'?[v,0]:[0,-v]);
                    const r=document.querySelector('canvas.sequence-axis.'+p.axis).getBoundingClientRect(),cross=p.reverse?42:14;
                    return p.axis==='horizontal'?[r.left+pixel[0],r.top+cross]:[r.left+cross,r.top+pixel[1]];}''',{'bp':bp,'axis':axis,'reverse':reverse})
            def select(start,end,axis='horizontal',reverse=False,modifier='Control'):
                if modifier:page.keyboard.down(modifier)
                page.mouse.move(*point(start,axis,reverse));page.mouse.down();page.mouse.move(*point(end-1,axis,reverse),steps=12);page.mouse.up()
                if modifier:page.keyboard.up(modifier)
                check.settle()
                selected=state()['selection'];assert selected and (selected['startBp'],selected['endBp'],selected['axis'],selected['reverse'])==(start,end,axis,reverse),state()
            def card():return page.get_by_role('region',name='FASTA measurement',exact=True)
            def estimate(period):
                with page.expect_response('**/sequence/repeat_period') as response:card().get_by_role('button',name='Estimate period',exact=True).click()
                assert response.value.ok,response.value.text()
                job=json.loads(response.value.text())
                for _ in range(100):
                    if job['status'] in ('finished','failed','cancelled'):break
                    page.wait_for_timeout(50);job=json.loads(app.http('/sequence/repeat_period',{'jobId':job['jobId']}))
                assert job['status']=='finished' and job['result']['periodBp']==period,job
                frozen=state()['selection'];bases=frozen['sequence']
                if frozen['reverse']:bases=bases.translate(str.maketrans('ACGTRYMKSWBDHVN','TGCAYRKMSWVHDBN'))[::-1]
                assert job['result']['inputSha256']==hashlib.sha256(bases.encode()).hexdigest(),job
                card().get_by_text('No supported period' if period is None else f'Period: {period} bp',exact=True).wait_for()
                results.append(job);return job
            def copied(button,expected):
                card().get_by_role('button',name=button,exact=True).click();page.wait_for_timeout(100)
                assert page.evaluate('navigator.clipboard.readText()')==expected
            def forward():
                focus(106);before=state()['view'];select(100,112)
                assert state()['selection']['sequence']=='CT'*6 and not state()['entities'] and state()['region'] is None,state()
                after=state()['view'];assert before['resolution']==after['resolution'] and before['centerBp']==after['centerBp'],(before,after)
                copied('Copy selected sequence','CT'*6);copied('Copy assembly coordinates','101-112')
                job=estimate(2);assert job['result']['identity']==1 and job['result']['copies']==6
                assert submitted[-1]['body']=={'startBp':100,'endBp':112}
                assert 'x-hict-history-session' in submitted[-1]['headers']
                assert page.locator('.sequence-measurement-highlight').count()==1
            check.step('forward-repeat-selection-and-copy',forward)
            def jobs():
                page.get_by_role('button',name='Jobs',exact=False).click()
                dialog=page.get_by_role('dialog',name='Background jobs',exact=True)
                dialog.get_by_text('Repeat measurement',exact=True).wait_for()
                assert 'Period 2 bp' in dialog.inner_text() and '101–112' in dialog.inner_text()
                page.screenshot(path=str(out/'repeat-job.png'));page.keyboard.press('Escape');assert card().is_visible()
                page.keyboard.press('Escape');check.settle();assert state()['selection'] is None
            check.step('jobs-and-escape',jobs)
            def reverse():
                page.mouse.click(*point(106),button='right');page.get_by_role('menuitem',name='Show reverse strand',exact=True).click();check.settle()
                page.mouse.click(*point(106),button='right');page.get_by_role('menuitem',name='Drag to select bases',exact=True).click();check.settle()
                select(100,112,reverse=True,modifier=None)
                assert state()['selection']['sequence']=='AG'*6
                copied('Copy selected sequence','AG'*6);estimate(2)
                page.mouse.click(*point(106),button='right');page.get_by_role('menuitem',name='Drag to pan',exact=True).click();check.settle()
            check.step('reverse-strand-and-plain-selection-mode',reverse)
            def mutation():
                focus(206);select(200,212,axis='vertical');assert state()['selection']['sequence']=='CTCTATCTCTCT'
                job=estimate(2);assert job['result']['matches']==8 and job['result']['comparedPairs']==10 and job['result']['identity']==.8,job
                copied('Copy selected sequence','CTCTATCTCTCT')
            check.step('vertical-substitution-period',mutation)
            def ambiguous():
                focus(306);select(300,312);job=estimate(None);assert job['result']['ambiguousBases']==12
            check.step('ambiguous-bases-do-not-create-repeat-evidence',ambiguous)
            def keyboard():
                focus(106);select(100,101);assert len(state()['selection']['sequence'])==1
                page.keyboard.press('Shift+ArrowRight');check.settle()
                assert state()['selection']['startBp']==100 and state()['selection']['endBp']==102,state()
                page.keyboard.press('Escape');check.settle();assert state()['selection'] is None
            check.step('one-base-and-keyboard-extension',keyboard)
            def cancel_gesture():
                page.keyboard.down('Control');page.mouse.move(*point(100));page.mouse.down();page.mouse.move(*point(111),steps=8)
                page.keyboard.press('Escape');page.mouse.up();page.keyboard.up('Control');check.settle()
                assert state()['selection'] is None and not page.locator('.sequence-measurement-highlight').count(),state()
            check.step('escape-cancels-an-incomplete-drag',cancel_gesture)
            def late_results():
                select(100,112);pending=[]
                def hold(route):pending.append(route)
                page.route('**/sequence/repeat_period',hold)
                try:
                    card().get_by_role('button',name='Estimate period',exact=True).click()
                    card().get_by_role('button',name='Cancel measurement',exact=True).wait_for()
                    assert len(pending)==1
                    card().get_by_role('button',name='Cancel measurement',exact=True).click()
                    with page.expect_response('**/sequence/repeat_period') as response:pending.pop(0).continue_(url=app.url+'/sequence/repeat_period')
                    assert response.value.ok;check.settle()
                    assert not card().get_by_text('Period: 2 bp',exact=True).count()
                    card().get_by_role('button',name='Estimate period',exact=True).click()
                    card().get_by_role('button',name='Cancel measurement',exact=True).wait_for();assert len(pending)==1
                    card().get_by_role('button',name='Clear FASTA measurement',exact=True).click()
                    with page.expect_response('**/sequence/repeat_period') as response:pending.pop(0).continue_(url=app.url+'/sequence/repeat_period')
                    assert response.value.ok;check.settle();assert not card().count() and state()['selection'] is None
                finally:page.unroute('**/sequence/repeat_period',hold)
            check.step('late-submission-after-cancel-and-close-is-ignored',late_results)
            def edits():
                select(100,112);estimate(2)
                page.evaluate('''async()=>{const m=window.__hictBenchmark.manager();m.eventManager.applyAssemblyInfo(await m.networkManager.requestManager.splitContigAtBp(0,103));m.reloadVisuals();}''');check.settle()
                assert state()['selection'] is None and not card().count()
                focus(106);select(100,112);assert state()['selection']['sequence']=='CT'*6;estimate(2)
                page.evaluate('window.__hictBenchmark.manager().viewAndLayersManager.selectEntityRangeByBp(0,4000)')
                with page.expect_response('**/reverse_selection_range') as response:page.get_by_role('button',name='Reverse Selection',exact=True).click()
                assert response.value.ok;check.settle();assert state()['selection'] is None
                focus(3894);select(3888,3900);assert state()['selection']['sequence']=='AG'*6;estimate(2)
                assert json.loads(app.http('/sequence/region',{'startBp':3888,'endBp':3900}))['sequence']=='AG'*6
            check.step('exact-cut-and-reversed-assembly-invalidation',edits)
            def layout():
                selected=state()['selection'];view=state()['view']
                page.mouse.move(*point(3894));page.mouse.down();page.mouse.move(*point(3889),steps=8);page.mouse.up();check.settle()
                assert state()['selection']==selected and state()['view']['centerBp']!=view['centerBp']
                page.set_viewport_size({'width':1024,'height':768});check.settle()
                box=card().bounding_box();assert box and box['x']>=0 and box['y']>=0 and box['x']+box['width']<=1024 and box['y']+box['height']<=768,box
                page.screenshot(path=str(out/'measurement-1024x768.png'))
                page.set_viewport_size({'width':1600,'height':1100});check.settle()
                assert state()['selection']==selected
            check.step('pan-preserves-measurement-and-compact-layout',layout)
            def clean_export():
                def export(name):
                    with page.expect_download() as download:page.evaluate("window.__hictBenchmark.manager().exportCurrentMapPng(undefined,{scope:'viewport',cropToMap:true})")
                    path=out/(name+'.png');download.value.save_as(path);check.settle();return path.read_bytes()
                selected=state()['selection'];before=export('measured-export');assert state()['selection']==selected
                card().get_by_role('button',name='Clear FASTA measurement',exact=True).click();check.settle()
                assert export('clean-export')==before
            check.step('measurement-free-image-export',clean_export)
            def session():
                focus(3894);select(3888,3900)
                with page.expect_download() as download:check.menu('File','Save session…')
                path=out/'measured-session.json';download.value.save_as(path)
                assert 'sequenceMeasurement' not in path.read_text()
                check.menu('File','Close map')
                with page.expect_file_chooser() as chooser:check.menu('File','Open session…')
                chooser.value.set_files(path)
                page.get_by_text('Session restored. Undo history starts from the restored assembly.',exact=True).last.wait_for();check.settle()
                assert state()['selection'] is None and not card().count()
                assert json.loads(app.http('/sequence/region',{'startBp':3888,'endBp':3900}))['sequence']=='AG'*6
            check.step('measurement-is-transient-across-session-restore',session)
            assert hashlib.sha256(native.read_bytes()).hexdigest()==digest
            assert not check.errors,check.errors
            (out/'results.json').write_text(json.dumps({'status':'ok','checks':check.rows,'jobs':results,'submitted':submitted,'sourceSha256':digest,'pageErrors':check.errors},indent=2))
            browser.close()
    finally:static.shutdown();static.server_close()


if __name__=='__main__':main()
