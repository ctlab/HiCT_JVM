#!/usr/bin/env python3
"""Exercise real HiCT actions with controlled writable-file picker outcomes."""
import argparse
import base64
import functools
import hashlib
import http.server
import json
from pathlib import Path
import shutil
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
    for name in ('maps','seqs'):(data/name).mkdir()
    shutil.copyfile(data/'sequence.hict.hdf5',data/'maps/sequence.hict.hdf5');shutil.copyfile(data/'sequence.fa',data/'seqs/sequence.fa')
    native=data/'sequence.hict.hdf5';digest=hashlib.sha256(native.read_bytes()).hexdigest()
    class Quiet(http.server.SimpleHTTPRequestHandler):
        def log_message(self,*_):pass
    static=http.server.ThreadingHTTPServer(('127.0.0.1',0),functools.partial(Quiet,directory=str(Path(args.webui).resolve())))
    threading.Thread(target=static.serve_forever,daemon=True).start()
    try:
        with HictTestServer(args.jar,data,out/'server.log') as app,sync_playwright() as pw:
            app.http('/open',{'filename':native.name});app.http('/link_fasta',{'fastaFilename':'sequence.fa'})
            browser=pw.chromium.launch(headless=True);page=browser.new_page(viewport={'width':1600,'height':1100},accept_downloads=True)
            page.set_default_timeout(30000);page.add_init_script('window.__HICT_BENCHMARK_ENABLED__=true')
            page.route('http://localhost:5000/**',lambda r:r.continue_(url=app.url+r.request.url.removeprefix('http://localhost:5000')))
            check=BrowserCheck(page,out);requests=[];expected_errors=[];downloads=[]
            page.on('download',lambda item:downloads.append(item.suggested_filename))
            page.on('request',lambda r:requests.append(r.url))
            page.goto(f'http://127.0.0.1:{static.server_port}/');check.menu('File','Join running session');check.settle()
            page.evaluate('''()=>{
              const q=window.__fileQA={pickerMode:'save',writeMode:'save',files:{},calls:[],opened:[],readbacks:0,aborts:0};
              window.__qaSavePicker=async options=>{
                q.calls.push({id:options.id,name:options.suggestedName,startIn:options.startIn?.name??null,activation:navigator.userActivation.isActive});
                if(q.pickerMode==='cancel')throw new DOMException('Cancelled','AbortError');
                if(q.pickerMode==='deny')throw new DOMException('Picker denied','NotAllowedError');
                const name='chosen-'+options.suggestedName;
                const handle={name,getFile:async()=>{q.readbacks++;return q.files[name];},createWritable:async()=>{
                  const chunks=[];return {write:async value=>{if(q.writeMode==='fail-write')throw Error('Simulated write failure');chunks.push(value);},
                    close:async()=>{if(q.writeMode==='fail-close')throw Error('Simulated close failure');
                      const commit=()=>{q.files[name]=new File(chunks,name,{type:chunks[0]?.type||'text/plain'});q.last=name;};
                      if(q.writeMode==='wait-close')await new Promise(resolve=>{q.releaseWrite=()=>{commit();q.releaseWrite=null;resolve();};});else commit();},
                    abort:async()=>{q.aborts++;}};
                }};
                if(q.pickerMode==='wait')await new Promise(resolve=>{q.releasePicker=()=>{q.releasePicker=null;resolve();};});
                return handle;
              };
              window.showSaveFilePicker=window.__qaSavePicker;
              window.showOpenFilePicker=async options=>{q.opened.push({id:options.id,startIn:options.startIn?.name??null});return [{getFile:async()=>q.files[q.lastSession]}];};
            }''')
            def mode(picker='save',write='save'):page.evaluate('v=>Object.assign(window.__fileQA,{pickerMode:v[0],writeMode:v[1]})',[picker,write])
            def dirty():return page.evaluate('window.__hictBenchmark.manager().networkManager.requestManager.hasUnsavedAssemblyEdits()')
            def reverse():
                page.evaluate('window.__hictBenchmark.manager().viewAndLayersManager.selectEntityRangeByBp(0,4000)')
                with page.expect_response('**/reverse_selection_range') as response:page.get_by_role('button',name='Reverse Selection',exact=True).click()
                assert response.value.ok;check.settle();assert dirty()
            def saved_count():return page.evaluate('Object.keys(window.__fileQA.files).length')
            def save_session():
                mode();check.menu('File','Save session…')
                page.get_by_role('button',name='Open saved session',exact=True).last.wait_for();check.settle();assert not dirty()
                page.evaluate('window.__fileQA.lastSession=window.__fileQA.last')
                return page.evaluate('async()=>JSON.parse(await window.__fileQA.files[window.__fileQA.last].text())')
            def artifact(name):
                value=page.evaluate('''async()=>{const file=window.__fileQA.files[window.__fileQA.last];return await new Promise(resolve=>{const r=new FileReader();r.onload=()=>resolve(r.result.split(',')[1]);r.readAsDataURL(file);});}''')
                path=out/name;path.write_bytes(base64.b64decode(value));return path
            def error_dialog():
                dialog=page.get_by_role('alertdialog',name='Session could not be saved',exact=True);dialog.wait_for();return dialog
            def examples():
                check.menu('File','Examples…');dialog=page.get_by_role('dialog',name='Examples',exact=True);dialog.wait_for()
                page.screenshot(path=str(out/'file-examples.png'));page.keyboard.press('Escape')
            check.step('file-open-examples',examples)
            reverse()
            def cancelled():
                mode('cancel');before=saved_count();snapshots=sum(url.endswith('/session/snapshot') for url in requests)
                check.menu('File','Save session…');check.settle()
                assert dirty() and saved_count()==before and sum(url.endswith('/session/snapshot') for url in requests)==snapshots
                check.menu('File','Close map');dialog=page.get_by_role('alertdialog',name='Unsaved assembly edits',exact=True)
                dialog.get_by_role('button',name='Save session',exact=True).click();check.settle();assert dialog.is_visible() and dirty()
                dialog.get_by_role('button',name='Cancel',exact=True).click()
            check.step('cancelled-session-does-not-save-or-close',cancelled)
            def denied():
                mode('deny');check.menu('File','Save session…');dialog=error_dialog();assert 'Picker denied' in dialog.inner_text()
                assert dirty();page.screenshot(path=str(out/'picker-denied-dialog.png'));dialog.get_by_role('button',name='OK',exact=True).click()
            check.step('denied-picker-does-not-download',denied)
            def failed_close():
                mode(write='fail-close');before=saved_count();check.menu('File','Save session…');dialog=error_dialog()
                assert 'Simulated close failure' in dialog.inner_text() and dirty() and saved_count()==before
                assert page.evaluate('window.__fileQA.aborts')==1;dialog.get_by_role('button',name='OK',exact=True).click()
            check.step('failed-write-close-keeps-edits-unsaved',failed_close)
            def failed_guard_save():
                mode(write='fail-close');check.menu('File','Close map')
                dialog=page.get_by_role('alertdialog',name='Unsaved assembly edits',exact=True)
                dialog.get_by_role('button',name='Save session',exact=True).click();check.settle()
                dialog.get_by_role('alert').wait_for();assert 'Simulated close failure' in dialog.inner_text()
                assert dirty() and not page.get_by_role('alertdialog',name='Session could not be saved',exact=True).count()
                page.screenshot(path=str(out/'failed-guard-save-dialog.png'))
                dialog.get_by_role('button',name='Cancel',exact=True).click()
            check.step('failed-guard-save-stays-in-one-dialog',failed_guard_save)
            def confirmed():
                saved=save_session();assert saved['version']==4 and saved['snapshot']['assemblyVerified']
                artifact('session.json');assert page.evaluate('window.__fileQA.calls.at(-1).id')=='hict-session'
                assert page.evaluate('window.__fileQA.calls.at(-1).activation')
                check.menu('File','Close map');check.menu('File','Open session…')
                page.get_by_text('Session restored. Undo history starts from the restored assembly.',exact=True).last.wait_for();check.settle()
                assert not dirty() and page.evaluate('window.__fileQA.opened.at(-1).id')=='hict-session'
                assert page.evaluate('window.__fileQA.opened.at(-1).startIn')=='chosen-hict_session.json'
            check.step('confirmed-session-picker-write-and-open',confirmed)
            def notification():
                reverse();save_session();before=page.evaluate('window.__fileQA.readbacks')
                previous=page.evaluate('window.__hictBenchmark.manager().networkManager.requestManager.history.summary.session')
                page.get_by_role('button',name='Open saved session',exact=True).last.click()
                page.get_by_text('Session restored. Undo history starts from the restored assembly.',exact=True).last.wait_for();check.settle()
                page.wait_for_function('old=>window.__hictBenchmark.manager().networkManager.requestManager.history.summary.session!==old',arg=previous)
                assert page.evaluate('window.__fileQA.readbacks')>before and not dirty()
            check.step('saved-session-notification-opens-written-file',notification)
            def concurrent_edit():
                reverse();mode(write='wait-close');check.menu('File','Save session…')
                page.wait_for_function('typeof window.__fileQA.releaseWrite==="function"')
                reverse();page.evaluate('window.__fileQA.releaseWrite()');check.settle();assert dirty()
                page.get_by_text('The session was saved, but newer edits remain unsaved. Save again before continuing.',exact=True).last.wait_for()
                save_session()
            check.step('later-edits-are-not-marked-saved',concurrent_edit)
            def assembly_files():
                expected=json.loads(app.http('/sequence/region',{'startBp':0,'endBp':4000}))['sequence']
                mode();check.menu('Assembly','Export FASTA…');page.wait_for_function('window.__fileQA.last==="chosen-assembly.fasta"')
                path=artifact('assembly.fasta');actual=''.join(line for line in path.read_text().splitlines() if not line.startswith('>'))
                assert actual==expected
                check.menu('Assembly','Export AGP…');page.wait_for_function('window.__fileQA.last==="chosen-assembly.agp"');artifact('assembly.agp')
                calls=page.evaluate('window.__fileQA.calls');assert any(c['id']=='hict-fasta' for c in calls) and any(c['id']=='hict-agp' for c in calls)
            check.step('assembly-fasta-agp-use-save-destinations',assembly_files)
            def select_region():page.evaluate('window.__hictBenchmark.manager().viewAndLayersManager.setRegionBounds({columnStart:8,columnEnd:36,rowStart:8,rowEnd:36})')
            def selection():
                expected=json.loads(app.http('/sequence/region',{'startBp':8,'endBp':36}))['sequence'];select_region()
                with page.expect_response('**/get_fasta_for_selection') as response:page.get_by_role('button',name='Export FASTA for selection',exact=True).click()
                assert response.value.ok;check.settle();path=artifact('selection.fasta')
                assert ''.join(line for line in path.read_text().splitlines() if not line.startswith('>'))==expected
                assert page.evaluate('window.__fileQA.calls.at(-1).startIn')=='chosen-assembly.fasta'
            check.step('selection-fasta-preserves-exact-endpoints-and-kind-folder',selection)
            def stale_selection():
                select_region();mode('wait');before=saved_count();before_errors=len(check.errors)
                page.get_by_role('button',name='Export FASTA for selection',exact=True).click();page.wait_for_function('typeof window.__fileQA.releasePicker==="function"')
                reverse()
                with page.expect_response('**/get_fasta_for_selection') as response:page.evaluate('window.__fileQA.releasePicker()')
                assert response.value.status==409;check.settle();assert saved_count()==before
                for message in check.errors[before_errors:]:assert '409' in message,message
                expected_errors.extend(check.errors[before_errors:]);del check.errors[before_errors:]
                mode();save_session()
            check.step('stale-selection-is-rejected-after-picker-delay',stale_selection)
            def figures():
                for extension in ('svg','png','pdf'):
                    if not page.locator('#export-'+extension+'-button').is_visible():page.get_by_role('button',name='Export',exact=True).click()
                    page.locator('#export-'+extension+'-button').click()
                    page.wait_for_function('ext=>window.__fileQA.last.endsWith("."+ext)',arg=extension);check.settle()
                    path=artifact('figure.'+extension);assert path.stat().st_size>1000
                    assert page.evaluate('window.__fileQA.calls.at(-1).id')=='hict-figure'
                page.get_by_role('dialog',name='Export figure',exact=True).get_by_role('button',name='Close',exact=True).last.click()
            check.step('png-svg-pdf-user-actions-use-destinations',figures)
            def fallback():
                reverse();page.evaluate('window.showSaveFilePicker=undefined')
                with page.expect_download() as download:check.menu('File','Save session…')
                download.value.save_as(out/'fallback-session.json');check.settle();assert dirty()
                assert json.loads((out/'fallback-session.json').read_text())['version']==4
                page.get_by_text('Session download started.',exact=False).last.wait_for()
                check.menu('File','Close map');dialog=page.get_by_role('alertdialog',name='Unsaved assembly edits',exact=True)
                with page.expect_download() as download:dialog.get_by_role('button',name='Save session',exact=True).click()
                download.value.save_as(out/'fallback-guard-session.json');check.settle();assert dialog.is_visible() and dirty()
                page.screenshot(path=str(out/'fallback-save-guard.png'));dialog.get_by_role('button',name='Cancel',exact=True).click()
                page.evaluate('()=>{window.showSaveFilePicker=window.__qaSavePicker;}');save_session()
            check.step('browser-download-fallback-is-not-confirmed-save',fallback)
            def folders():
                field=page.get_by_placeholder('Relative folder or file path under DATA_DIR')
                check.menu('File','Open map…');field.fill('maps');field.press('Enter');check.settle();check.choose_file('sequence.hict.hdf5')
                with page.expect_response('**/open') as response:page.get_by_role('button',name='Load',exact=True).click()
                assert response.value.ok;check.settle()
                check.menu('File','Open map…');field.wait_for();check.settle();assert field.input_value()=='maps'
                assert page.get_by_role('button',name='Load',exact=True).is_enabled()
                page.screenshot(path=str(out/'remembered-map-file.png'))
                page.get_by_role('button',name='Dismiss',exact=True).click()
                check.menu('Assembly','Link FASTA…');field.wait_for();check.settle();assert field.input_value()==''
                field.fill('seqs');field.press('Enter');check.settle();check.choose_file('sequence.fa')
                with page.expect_response('**/link_fasta') as response:page.get_by_role('button',name='Load',exact=True).click()
                assert response.value.ok;check.settle()
                check.menu('Assembly','Link FASTA…');field.wait_for();check.settle();assert field.input_value()=='seqs';page.get_by_role('button',name='Dismiss',exact=True).click()
            check.step('server-file-folders-remembered-per-kind',folders)
            assert hashlib.sha256(native.read_bytes()).hexdigest()==digest
            assert not check.errors,check.errors
            assert len(downloads)==2,downloads
            (out/'results.json').write_text(json.dumps({'status':'ok','checks':check.rows,'pickerCalls':page.evaluate('window.__fileQA.calls'),'expectedConsoleErrors':expected_errors,'pageErrors':check.errors,'sourceSha256':digest},indent=2))
            browser.close()
    finally:static.shutdown();static.server_close()


if __name__=='__main__':main()
