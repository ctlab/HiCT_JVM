#!/usr/bin/env python3
"""Qualify packaged offline startup/retry and edit protection on a private display."""
import argparse
import functools
import hashlib
import http.server
import json
import os
from pathlib import Path
import select
import socket
import subprocess
import threading
import time
from urllib.request import urlopen
from urllib.parse import parse_qs,unquote,urlsplit
from playwright.sync_api import sync_playwright
from sequence_browser_harness import BrowserCheck,HictTestServer


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    for key in ('jar','payload','data-dir','output'):parser.add_argument('--'+key,required=True)
    parser.add_argument('--runtime',default='/tmp/hict-native-stream-runtime/runtime')
    args=parser.parse_args();data,out=Path(args.data_dir).absolute(),Path(args.output).absolute();payload=Path(args.payload).resolve();runtime=Path(args.runtime).resolve()
    if any(p!=p.resolve() or not p.is_relative_to('/tmp') for p in (data,out)):parser.error('Use fresh nonsymlinked /tmp directories')
    data.mkdir(parents=True,exist_ok=False);out.mkdir(parents=True,exist_ok=False)
    with (out/'fixture.log').open('w') as log:
        subprocess.run(['java','--class-path',str(Path(args.jar).resolve()),str(Path(__file__).with_name('SequenceViewBrowserFixture.java')),str(data)],check=True,stdout=log,stderr=subprocess.STDOUT)
    digest=hashlib.sha256((data/'sequence.hict.hdf5').read_bytes()).hexdigest()
    reservation=socket.socket();reservation.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1);reservation.bind(('127.0.0.1',0));port=reservation.getsockname()[1]
    url=f'http://127.0.0.1:{port}/';fallback=None
    state={'status':503,'rootRequests':0};web=None;electron=xvfb=None;dialogs=[];errors=[]
    class Results(list):
        def append(self,value):
            super().append(value);print(value,flush=True)
            (out/'partial-results.json').write_text(json.dumps(self,indent=2))
    rows=Results()
    class Handler(http.server.SimpleHTTPRequestHandler):
        def log_message(self,*_):pass
        def do_GET(self):
            if self.path=='/':state['rootRequests']+=1
            if state['status']!=200:
                self.send_response(state['status']);self.send_header('Content-Type','text/html');self.end_headers();self.wfile.write(b'<h1>Starting</h1>');return
            super().do_GET()
    env={**os.environ,'GSETTINGS_BACKEND':'memory','DBUS_SESSION_BUS_ADDRESS':'disabled:'};env.pop('ELECTRON_RUN_AS_NODE',None)
    for key in ('XDG_CONFIG_HOME','XDG_CACHE_HOME','XDG_DATA_HOME','XDG_STATE_HOME','XDG_RUNTIME_DIR'):
        path=out/key.lower();path.mkdir(mode=0o700);env[key]=str(path)
    env['LD_LIBRARY_PATH']=str(runtime/'usr/lib/x86_64-linux-gnu')+((':'+env['LD_LIBRARY_PATH']) if env.get('LD_LIBRARY_PATH') else '')
    readfd,writefd=os.pipe()
    try:
        with HictTestServer(args.jar,data,out/'server.log') as api,(out/'xvfb.log').open('w') as xlog,(out/'electron.log').open('w') as elog:
            api.http('/open',{'filename':'sequence.hict.hdf5'});api.http('/link_fasta',{'fastaFilename':'sequence.fa'})
            xvfb=subprocess.Popen([str(runtime/'usr/bin/Xvfb'),'-displayfd',str(writefd),'-screen','0','1600x1100x24','-nolisten','tcp'],pass_fds=(writefd,),env=env,stdout=xlog,stderr=xlog)
            os.close(writefd);writefd=None
            assert select.select([readfd],[],[],15)[0];display=os.read(readfd,32).decode().strip();assert display.isdecimal();env['DISPLAY']=':'+display
            manifest=json.loads((payload/'manifest.json').read_text())
            def launch(reset=False):
                nonlocal electron
                with socket.socket() as probe:probe.bind(('127.0.0.1',0));debug=probe.getsockname()[1]
                child_env={**env,'HICT_RESET_WEBUI':'1' if reset else '0'}
                command=[str(payload/manifest['command']),'--no-sandbox','--disable-gpu','--ozone-platform=x11','--remote-debugging-port='+str(debug),'--user-data-dir='+str(out/'profile'),str(payload/'app'),url]
                electron=subprocess.Popen(command,env=child_env,cwd=payload,stdout=elog,stderr=elog)
                endpoint=f'http://127.0.0.1:{debug}';deadline=time.monotonic()+30
                while True:
                    assert electron.poll() is None,'Electron exited before recovery; see electron.log'
                    try:
                        with urlopen(endpoint+'/json/version',timeout=1):return endpoint
                    except OSError:
                        assert time.monotonic()<deadline;time.sleep(.1)
            with sync_playwright() as pw:
                def connect(reset=False):
                    browser=pw.chromium.connect_over_cdp(launch(reset=reset));context=browser.contexts[0]
                    page=context.pages[0] if context.pages else context.wait_for_event('page',timeout=30000)
                    page.set_default_timeout(30000)
                    page.on('pageerror',lambda error:errors.append(str(error)))
                    page.on('dialog',lambda d:(dialogs.append({'type':d.type,'message':d.message}),d.dismiss()))
                    page.on('console',lambda message:print('renderer',message.type,message.text,flush=True))
                    return browser,context,page
                browser,context,page=connect()
                page.wait_for_url('**/index.html?api=*',wait_until='commit');fallback=page.url
                assert parse_qs(urlsplit(fallback).query)["api"] == [url.rstrip("/")]
                try:page.get_by_role('button',name='Reconnect',exact=True).wait_for()
                except Exception:
                    subprocess.run(['import','-display',env['DISPLAY'],'-window','root',str(out/'bootstrap-display.png')],env=env,check=True,timeout=10)
                    page.screenshot(path=str(out/'bootstrap-failed.png'))
                    print(page.evaluate("({body:document.body.innerText,desktop:Object.keys(window.hictDesktop||{}),scripts:[...document.scripts].map(s=>s.src)})"),errors,flush=True)
                    raise
                page.screenshot(path=str(out/'offline-start.png'));assert page.get_by_role('heading',name='No contact map open',exact=True).is_visible()
                assert page.evaluate("typeof window.hictDesktop.reconnect==='function' && typeof require==='undefined' && typeof process==='undefined'")
                for href in page.locator('link[rel*=icon]').evaluate_all('nodes=>nodes.map(node=>node.href)'):
                    assert (payload/'app'/unquote(urlsplit(href).path).lstrip('/')).is_file(),href
                rows.append('packaged offline startup, native preload and public icons')
                context.add_init_script('window.__HICT_BENCHMARK_ENABLED__=true')
                def forward_api(route):
                    if route.request.is_navigation_request():
                        route.fallback()
                    else:
                        route.continue_(url=api.url + route.request.url.removeprefix(url.rstrip("/")))

                page.route(url + "**", forward_api)
                page.reload();page.get_by_role('button',name='Reconnect',exact=True).wait_for()
                page.evaluate("localStorage.setItem('hict.recovery.probe','keep')")
                def retry():
                    previous=page.evaluate('performance.timeOrigin')
                    page.get_by_role('button',name='Reconnect',exact=True).click()
                    page.wait_for_function("before=>performance.timeOrigin!==before && location.pathname==='/index.html' && document.querySelector('.desktop-recovery button')?.textContent.trim()==='Reconnect'",arg=previous)
                retry();assert page.url==fallback and page.evaluate("localStorage.getItem('hict.recovery.probe')")=='keep'
                rows.append('connection-refused retry returns to usable bundled UI')
                reservation.close();reservation=None
                web=http.server.ThreadingHTTPServer(('127.0.0.1',port),functools.partial(Handler,directory=str(payload/'app')))
                threading.Thread(target=web.serve_forever,daemon=True).start()
                before=state['rootRequests'];retry();assert state['rootRequests']==before+1
                rows.append('HTTP 503 returns to bundled UI without a navigation loop')
                check=BrowserCheck(page,out);check.menu('File','Join running session');check.settle()
                assert page.evaluate('Boolean(window.__hictBenchmark.manager())')
                page.evaluate('window.__hictBenchmark.manager().viewAndLayersManager.selectEntityRangeByBp(0,4000)')
                page.get_by_role('button',name='Reverse Selection',exact=True).click();check.settle()
                assert page.evaluate('window.__hictBenchmark.manager().networkManager.requestManager.hasUnsavedAssemblyEdits()')
                before=state['rootRequests'];page.get_by_role('button',name='Reconnect',exact=True).click()
                prompt=page.get_by_role('alertdialog',name='Unsaved assembly edits',exact=True);prompt.wait_for();page.screenshot(path=str(out/'reconnect-edits.png'));prompt.get_by_role('button',name='Cancel',exact=True).click()
                assert state['rootRequests']==before and page.url==fallback
                assert page.evaluate('window.__hictBenchmark.manager().networkManager.requestManager.hasUnsavedAssemblyEdits()')
                rows.append('bundled interface edits and cancelled reconnect preserve live work')
                previous=page.evaluate('performance.timeOrigin');page.get_by_role('button',name='Reconnect',exact=True).click()
                page.get_by_role('alertdialog',name='Unsaved assembly edits',exact=True).get_by_role('button',name='Discard edits',exact=True).click()
                page.wait_for_function("before=>performance.timeOrigin!==before && location.pathname==='/index.html' && document.querySelector('.desktop-recovery')",arg=previous)
                assert state['rootRequests']==before+1 and not dialogs,dialogs
                rows.append('approved reconnect has no duplicate unload prompt')
                state['status']=200;page.get_by_role('button',name='Reconnect',exact=True).click();page.wait_for_url(url)
                page.get_by_role('heading',name='No contact map open',exact=True).wait_for();assert not page.locator('.desktop-recovery').count()
                before=state['rootRequests'];assert page.evaluate('window.hictDesktop.reconnect()')=={'connected':False};assert state['rootRequests']==before
                page.evaluate("localStorage.setItem('hict.recovery.probe','online')")
                page.screenshot(path=str(out/'reconnected.png'));rows.append('server recovery and recovery-only IPC ownership')
                page.close();assert electron.wait(timeout=10)==0
                web.shutdown();web.server_close();web=None
                reservation=socket.socket();reservation.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1);reservation.bind(('127.0.0.1',port))
                browser,context,page=connect()
                try:page.wait_for_url(fallback)
                except Exception:
                    print('restart state',page.url,page.locator('body').inner_text(),flush=True)
                    page.screenshot(path=str(out/'restart-failed.png'));raise
                page.get_by_role('button',name='Reconnect',exact=True).wait_for();assert page.evaluate("localStorage.getItem('hict.recovery.probe')")=='keep'
                page.close();assert electron.wait(timeout=10)==0;rows.append('bundled-origin preferences survive offline restart')
                browser,context,page=connect(reset=True);page.wait_for_url(fallback)
                page.get_by_role('button',name='Reconnect',exact=True).wait_for();assert page.evaluate("localStorage.getItem('hict.recovery.probe')") is None
                assert (out/'electron.log').read_text().count('HICT_WEBUI_RESET_COMPLETE')==1
                page.set_viewport_size({'width':1024,'height':768});page.screenshot(path=str(out/'offline-1024.png'))
                box=page.locator('.desktop-recovery').bounding_box();assert box and box['x']>=0 and box['x']+box['width']<=1024,box
                page.close();assert electron.wait(timeout=10)==0;rows.append('explicit storage reset and compact recovery notice')
                assert not errors and not dialogs,(errors,dialogs)
                assert hashlib.sha256((data/'sequence.hict.hdf5').read_bytes()).hexdigest()==digest
                (out/'results.json').write_text(json.dumps({'status':'ok','checks':rows,'sourceSha256':digest,'pageErrors':errors,'unexpectedDialogs':dialogs,'rootRequests':state['rootRequests'],'payloadManifest':manifest},indent=2))
    except Exception:
        try:
            if not page.is_closed():
                page.screenshot(path=str(out/'failed.png'))
                print(page.locator('body').inner_text(),flush=True)
        except Exception:pass
        raise
    finally:
        if reservation:reservation.close()
        if web:web.shutdown();web.server_close()
        os.close(readfd)
        if writefd is not None:os.close(writefd)
        if electron is not None and electron.poll() is None:
            electron.terminate()
            try:electron.wait(timeout=10)
            except subprocess.TimeoutExpired:electron.kill();electron.wait()
        if xvfb is not None:xvfb.terminate();xvfb.wait(timeout=10)


if __name__=='__main__':main()
