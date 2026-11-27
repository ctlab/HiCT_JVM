#!/usr/bin/env python3
"""Real Electron/native save-dialog acceptance on an owned private X display."""
import argparse
import functools
import hashlib
import http.server
import json
import math
import os
from pathlib import Path
import select
import socket
import subprocess
import threading
import time
from urllib.request import Request, urlopen
from urllib.error import HTTPError
from playwright.sync_api import sync_playwright
from sequence_browser_harness import BrowserCheck, HictTestServer, configure_test_api


def sha(path):
    h=hashlib.sha256()
    with path.open('rb') as source:
        while block:=source.read(1024*1024):h.update(block)
    return h.hexdigest()


def main():
    p=argparse.ArgumentParser(description=__doc__)
    for key in ('jar','webui','data-dir','output'):p.add_argument('--'+key,required=True)
    p.add_argument('--runtime',default='/tmp/hict-native-stream-runtime/runtime')
    p.add_argument('--payload',help='Use a built Electron browser payload instead of the development executable')
    a=p.parse_args();data,out=Path(a.data_dir).absolute(),Path(a.output).absolute();web=Path(a.webui).resolve();runtime=Path(a.runtime).resolve()
    if any(x!=x.resolve() or not x.is_relative_to('/tmp') for x in (data,out)):p.error('Use fresh nonsymlinked /tmp directories')
    data.mkdir(parents=True,exist_ok=False);out.mkdir(parents=True,exist_ok=False)
    chosen=out/'chosen';chosen.mkdir();blocked=out/'blocked';blocked.mkdir()
    length=512_000_000
    with (out/'fixture.log').open('w') as log:
        subprocess.run(['java','--class-path',str(Path(a.jar).resolve()),str(Path(__file__).with_name('SequenceViewBrowserFixture.java')),str(data),str(length)],check=True,stdout=log,stderr=subprocess.STDOUT)
    original={name:sha(data/name) for name in ('sequence.fa','sequence.hict.hdf5')}
    class Static(http.server.SimpleHTTPRequestHandler):
        def log_message(self,*_):pass
    static=http.server.ThreadingHTTPServer(('127.0.0.1',0),functools.partial(Static,directory=str(web/'dist')))
    threading.Thread(target=static.serve_forever,daemon=True).start()
    native_requests=[];mode={'value':'normal','requests':0};received=threading.Event();release=threading.Event()
    env={**os.environ,'GSETTINGS_BACKEND':'memory','DBUS_SESSION_BUS_ADDRESS':'disabled:'}
    env.pop('ELECTRON_RUN_AS_NODE',None)
    for key in ('XDG_CONFIG_HOME','XDG_CACHE_HOME','XDG_DATA_HOME','XDG_STATE_HOME','XDG_RUNTIME_DIR'):
        directory=out/key.lower();directory.mkdir(mode=0o700);env[key]=str(directory)
    env['LD_LIBRARY_PATH']=str(runtime/'usr/lib/x86_64-linux-gnu')+((':'+env['LD_LIBRARY_PATH']) if env.get('LD_LIBRARY_PATH') else '')
    class Results(list):
        def append(self,value):
            super().append(value);print(value,flush=True)
            (out/'partial-results.json').write_text(json.dumps(self,indent=2))
    electron=xvfb=None;proxy=None;readfd,writefd=os.pipe();results=Results()
    try:
        with HictTestServer(a.jar,data,out/'server.log') as server,(out/'xvfb.log').open('w') as xlog,(out/'electron.log').open('w') as elog:
            server.http('/open',{'filename':'sequence.hict.hdf5'});server.http('/link_fasta',{'fastaFilename':'sequence.fa'})
            class Proxy(http.server.BaseHTTPRequestHandler):
                def log_message(self,*_):pass
                def do_OPTIONS(self):
                    self.send_response(204);self.send_header('Access-Control-Allow-Origin','*');self.send_header('Access-Control-Allow-Headers','*');self.end_headers()
                def do_GET(self):self.forward()
                def do_POST(self):self.forward()
                def forward(self):
                    body=self.rfile.read(int(self.headers.get('Content-Length','0'))) if self.command=='POST' else None
                    headers={k:v for k,v in self.headers.items() if k.lower() in ('content-type','accept','x-hict-history-session','x-hict-history-revision')}
                    coordinate=self.path in ('/get_fasta_for_assembly','/get_fasta_for_selection','/get_agp_for_assembly')
                    if coordinate:
                        native_requests.append({'path':self.path,'headers':headers,'body':body.decode()});mode['requests']+=1;received.set()
                        if mode['value']=='gate':release.wait(15)
                    try:
                        try:response=urlopen(Request(server.url+self.path,data=body,headers=headers,method=self.command),timeout=60)
                        except HTTPError as error:response=error
                        with response:
                            self.send_response(response.status)
                            for key in ('Content-Type','Content-Length','X-HiCT-History'):
                                if response.headers.get(key):self.send_header(key,response.headers[key])
                            self.send_header('Access-Control-Allow-Origin','*')
                            self.send_header('Access-Control-Expose-Headers','X-HiCT-History, X-HiCT-Workspace, X-HiCT-Workspace-Token, X-HiCT-Cancelled')
                            self.end_headers()
                            while block:=response.read(65536):
                                self.wfile.write(block);self.wfile.flush()
                                if coordinate:time.sleep(.002 if mode['value']=='slow' else .0005)
                    except (BrokenPipeError,ConnectionResetError):pass
            proxy=http.server.ThreadingHTTPServer(('127.0.0.1',0),Proxy);threading.Thread(target=proxy.serve_forever,daemon=True).start()
            api=f'http://localhost:{proxy.server_port}'
            xvfb=subprocess.Popen([str(runtime/'usr/bin/Xvfb'),'-displayfd',str(writefd),'-screen','0','1600x1100x24','-nolisten','tcp'],pass_fds=(writefd,),env=env,stdout=xlog,stderr=xlog)
            os.close(writefd);writefd=None
            if not select.select([readfd],[],[],15)[0]:raise RuntimeError('Private X display did not start')
            display=os.read(readfd,32).decode().strip();assert display.isdecimal();env['DISPLAY']=':'+display
            with socket.socket() as port:port.bind(('127.0.0.1',0));debug_port=port.getsockname()[1]
            url=f'http://127.0.0.1:{static.server_port}/';endpoint=f'http://127.0.0.1:{debug_port}'
            executable=web/'node_modules/electron/dist/electron';entry=web/'dist/electron/main/main.js'
            if a.payload:
                payload=Path(a.payload).resolve();manifest=json.loads((payload/'manifest.json').read_text())
                executable=payload/manifest['command'];entry=payload/'app/main/main.js'
            electron=subprocess.Popen([str(executable),'--no-sandbox','--disable-gpu','--ozone-platform=x11','--remote-debugging-port='+str(debug_port),'--user-data-dir='+str(out/'profile'),str(entry),url],env=env,stdout=elog,stderr=elog)
            deadline=time.monotonic()+30
            while True:
                if electron.poll() is not None:raise RuntimeError('Electron exited; see electron.log')
                try:
                    with urlopen(endpoint+'/json/version',timeout=1):break
                except OSError:
                    assert time.monotonic()<deadline;time.sleep(.1)
            def xdo(*args):
                return subprocess.run([str(runtime/'usr/bin/xdotool'),*args],env=env,capture_output=True,text=True,timeout=10,check=False)
            def dialog(page,target=None):
                deadline=time.monotonic()+20
                while True:
                    windows=xdo('search','--onlyvisible','--name','^Save HiCT export$').stdout.splitlines()
                    if windows:break
                    if time.monotonic()>=deadline:
                        page.screenshot(path=str(out/'missing-dialog.png'))
                        raise AssertionError((page.locator('[data-sonner-toast]').all_text_contents(),page.locator('[role=dialog]:visible').all_text_contents()))
                    page.wait_for_timeout(50)
                window=windows[-1];assert xdo('windowfocus','--sync',window).returncode==0
                if target is None:assert xdo('key','Escape').returncode==0;return
                subprocess.run(['import','-display',env['DISPLAY'],'-window','root',str(out/'native-dialog.png')],env=env,check=True,timeout=10)
                for keys in [('key','ctrl+l'),('key','ctrl+a'),('type','--clearmodifiers','--delay','1',str(target)),('key','Return')]:assert xdo(*keys).returncode==0
                page.wait_for_timeout(500)
                if xdo('search','--onlyvisible','--name','^Save HiCT export$').stdout.strip():assert xdo('key','alt+s').returncode==0
                deadline=time.monotonic()+5
                while xdo('search','--onlyvisible','--name','^Save HiCT export$').stdout.strip():
                    if time.monotonic()>deadline:
                        subprocess.run(['import','-display',env['DISPLAY'],'-window','root',str(out/'unclosed-dialog.png')],env=env,check=True,timeout=10)
                        raise AssertionError('Native save dialog did not close')
                    page.wait_for_timeout(100)
            def memory():
                status=Path('/proc',str(electron.pid),'status').read_text();return next(int(line.split()[1])*1024 for line in status.splitlines() if line.startswith('VmRSS:'))
            with sync_playwright() as pw:
                browser=pw.chromium.connect_over_cdp(endpoint);context=browser.contexts[0];context.add_init_script('window.__HICT_BENCHMARK_ENABLED__=true')
                page=context.pages[0];page.set_default_timeout(30000);check=BrowserCheck(page,out)
                configure_test_api(page, api)
                page.reload(wait_until='domcontentloaded')
                check.menu('File','Join running session');check.settle()
                page.evaluate('api=>window.__hictBenchmark.manager().networkManager.onHostChanged(api)',api)
                assert page.evaluate("typeof window.hictDesktop.saveCoordinateExport==='function' && typeof require==='undefined' && typeof process==='undefined'")
                renderer_requests=[];page.on('request',lambda r:renderer_requests.append(r.url) if '/get_fasta_for_' in r.url or '/get_agp_for_' in r.url else None)
                baseline=memory();samples=[];target=chosen/'assembly.fasta'
                check.menu('Assembly','Export FASTA…');dialog(page,target)
                while not target.exists():page.wait_for_timeout(50);samples.append(memory())
                page.get_by_text('Assembly FASTA saved: assembly.fasta',exact=True).last.wait_for();page.screenshot(path=str(out/'saved.png'))
                h=hashlib.sha256();bases=0
                with target.open('rb') as file:
                    for line in file:
                        if line.startswith(b'>'):continue
                        line=line.strip();h.update(line);bases+=len(line)
                expected=hashlib.sha256();pattern=b'ACGTRYMKSWBDHVNAC'*1024;remaining=length
                while remaining:block=pattern[:min(remaining,len(pattern))];expected.update(block);remaining-=len(block)
                assert bases==length and h.digest()==expected.digest();assert renderer_requests==[],renderer_requests
                peak=max(samples);assert peak-baseline<256*1024*1024,(baseline,peak)
                results.append({'test':'native dialog and 512 Mb main-process streaming','bytes':target.stat().st_size,'sha256':sha(target),'mainBaselineRss':baseline,'mainPeakRss':peak,'rendererCoordinateRequests':renderer_requests})
                requests_before=mode['requests'];check.menu('Assembly','Export FASTA…');dialog(page)
                page.get_by_text('Assembly FASTA export cancelled.',exact=True).last.wait_for();assert mode['requests']==requests_before
                results.append({'test':'native chooser cancellation does not fetch'})
                page.evaluate('window.__hictBenchmark.manager().viewAndLayersManager.setRegionBounds({columnStart:8,columnEnd:36,rowStart:8,rowEnd:36})')
                page.get_by_role('button',name='Export FASTA for selection',exact=True).click();selection=chosen/'selection.fasta';dialog(page,selection)
                page.get_by_text('Selection FASTA saved: selection.fasta',exact=True).last.wait_for()
                sequence=''.join(line for line in selection.read_text().splitlines() if not line.startswith('>'));assert sequence==('ACGTRYMKSWBDHVNAC'*3)[8:36]
                results.append({'test':'native exact-region FASTA','sequence':sequence})
                check.menu('Assembly','Export AGP…');agp=chosen/'assembly.agp';dialog(page,agp);page.get_by_text('Assembly AGP saved: assembly.agp',exact=True).last.wait_for();assert str(length) in agp.read_text()
                results.append({'test':'native AGP save'})
                mode['value']='slow';cancelled=chosen/'cancelled.fasta';check.menu('Assembly','Export FASTA…');dialog(page,cancelled)
                page.locator('[data-sonner-toast]').filter(has=page.get_by_text('Exporting Assembly FASTA…',exact=True)).get_by_role('button',name='Cancel',exact=True).click()
                page.get_by_text('Assembly FASTA export cancelled.',exact=True).last.wait_for();assert not cancelled.exists()
                page.get_by_text('Exporting Assembly FASTA…',exact=True).wait_for(state='hidden')
                assert not list(chosen.glob('.hict-save-*.tmp'));results.append({'test':'progress cancellation cleans native staging'})
                mode['value']='gate';received.clear();release.clear();failure=blocked/'failure.fasta'
                check.menu('Assembly','Export FASTA…');dialog(page,failure)
                deadline=time.monotonic()+10
                while not received.is_set():
                    assert time.monotonic()<deadline,page.locator('[data-sonner-toast]').all_text_contents()
                    page.wait_for_timeout(50)
                blocked.chmod(0o500);release.set()
                page.get_by_text('Cannot save the export in this folder. Choose a writable folder.',exact=True).last.wait_for();blocked.chmod(0o700);assert not failure.exists();assert not list(blocked.glob('.hict-save-*.tmp'))
                assert (out/'profile/last-fasta-directory.txt').read_text()==str(chosen)
                page.screenshot(path=str(out/'failed.png'));results.append({'test':'native disk permission failure remains unsaved'})
                mode['value']='normal'
                page.evaluate('window.__hictBenchmark.manager().viewAndLayersManager.selectEntityRangeByBp(0,512000000)')
                page.get_by_role('button',name='Reverse Selection',exact=True).click();check.settle()
                check.menu('File','Save session…');dialog(page);assert page.evaluate('window.__hictBenchmark.manager().networkManager.requestManager.hasUnsavedAssemblyEdits()')
                check.menu('File','Save session…');session=chosen/'session.json';dialog(page,session)
                page.get_by_role('button',name='Open saved session',exact=True).last.wait_for();assert not page.evaluate('window.__hictBenchmark.manager().networkManager.requestManager.hasUnsavedAssemblyEdits()')
                assert json.loads(session.read_text())['version']==4
                page.get_by_role('button',name='Open saved session',exact=True).last.click();page.get_by_text('Session restored. Undo history starts from the restored assembly.',exact=True).last.wait_for()
                results.append({'test':'native Blob session cancellation, confirmation and readback restore'})
                mode['value']='slow';navigation=chosen/'navigation.fasta';check.menu('Assembly','Export FASTA…');dialog(page,navigation)
                page.reload(wait_until='domcontentloaded');page.wait_for_timeout(500)
                assert not navigation.exists() and not list(chosen.glob('.hict-save-*.tmp'));results.append({'test':'navigation aborts owned native transfer'})
                check.menu('File','Join running session');check.settle()
                page.evaluate('api=>window.__hictBenchmark.manager().networkManager.onHostChanged(api)',api)
                closing=chosen/'window-close.fasta';check.menu('Assembly','Export FASTA…');dialog(page,closing)
                deadline=time.monotonic()+10
                while not list(chosen.glob('.hict-save-*.tmp')):
                    assert time.monotonic()<deadline;page.wait_for_timeout(50)
                page.close();assert electron.wait(timeout=10)==0
                assert not closing.exists() and not list(chosen.glob('.hict-save-*.tmp'))
                results.append({'test':'last-window close waits for native stage cleanup','exitCode':electron.returncode})
                assert {name:sha(data/name) for name in original}==original
                assert not check.errors,check.errors
                (out/'results.json').write_text(json.dumps({'status':'ok','checks':results,'nativeRequests':native_requests,'sourceHashes':original,'pageErrors':check.errors},indent=2))
                if browser.is_connected():browser.close()
    finally:
        release.set();blocked.chmod(0o700)
        os.close(readfd)
        if writefd is not None:os.close(writefd)
        if electron is not None and electron.poll() is None:
            electron.terminate()
            try:electron.wait(timeout=10)
            except subprocess.TimeoutExpired:electron.kill();electron.wait()
        if xvfb is not None:xvfb.terminate();xvfb.wait(timeout=10)
        if proxy:proxy.shutdown();proxy.server_close()
        static.shutdown();static.server_close()


if __name__=='__main__':main()
