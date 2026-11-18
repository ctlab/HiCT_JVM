#!/usr/bin/env python3
"""Real JVM/UI large FASTA transfer through Chromium's native private filesystem writer."""
import argparse
import functools
import hashlib
import http.server
import json
import os
import math
from pathlib import Path
import subprocess
import threading
import time
from playwright.sync_api import sync_playwright
from sequence_browser_harness import BrowserCheck, HictTestServer


def digest(path):
    h=hashlib.sha256()
    with path.open('rb') as source:
        while block:=source.read(1024*1024):h.update(block)
    return h.hexdigest()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    for key in ('jar','webui','data-dir','output'):parser.add_argument('--'+key,required=True)
    args=parser.parse_args();data,out=Path(args.data_dir).absolute(),Path(args.output).absolute()
    if any(p!=p.resolve() or not p.is_relative_to('/tmp') for p in (data,out)):parser.error('Use fresh nonsymlinked /tmp directories')
    data.mkdir(parents=True,exist_ok=False);out.mkdir(parents=True,exist_ok=False)
    length=512_000_000
    with (out/'fixture.log').open('w') as log:
        subprocess.run(['java','--class-path',str(Path(args.jar).resolve()),str(Path(__file__).with_name('SequenceViewBrowserFixture.java')),str(data),str(length)],check=True,stdout=log,stderr=subprocess.STDOUT)
    native=data/'sequence.hict.hdf5';source_hash=digest(native);fasta_hash=digest(data/'sequence.fa')
    class Handler(http.server.SimpleHTTPRequestHandler):
        def log_message(self,*_):pass
    static=http.server.ThreadingHTTPServer(('127.0.0.1',0),functools.partial(Handler,directory=str(Path(args.webui).resolve())))
    threading.Thread(target=static.serve_forever,daemon=True).start()
    try:
        with HictTestServer(args.jar,data,out/'server.log') as app,sync_playwright() as pw:
            app.http('/open',{'filename':native.name});app.http('/link_fasta',{'fastaFilename':'sequence.fa'})
            context=pw.chromium.launch_persistent_context(str(out/'chromium-profile'),headless=True,viewport={'width':1600,'height':1100},accept_downloads=True);page=context.pages[0]
            page.set_default_timeout(30000);page.add_init_script('window.__HICT_BENCHMARK_ENABLED__=true')
            forwarding=lambda r:r.continue_(url=app.url+r.request.url.removeprefix('http://localhost:5000'))
            page.route('http://localhost:5000/**',forwarding)
            check=BrowserCheck(page,out);page.goto(f'http://127.0.0.1:{static.server_port}/');check.menu('File','Attach');check.settle()
            # Do not intercept the large response: qualify the actual fetch network path.
            page.evaluate('host=>window.__hictBenchmark.manager().networkManager.onHostChanged(host)',app.url)
            page.unroute('http://localhost:5000/**',forwarding)
            page.evaluate('''async()=>{
              const root=await navigator.storage.getDirectory();
              window.__streamHandle=await root.getFileHandle('assembly.fa',{create:true});
              window.__streamQA={mode:'save',bytes:0,chunks:0,maxChunk:0,closed:false,aborted:false};
              window.showSaveFilePicker=async()=>({name:'assembly.fa',getFile:()=>window.__streamHandle.getFile(),createWritable:async()=>{
                const sink=await window.__streamHandle.createWritable();
                return {
                  write:async chunk=>{
                    const q=window.__streamQA;
                    if(!(chunk instanceof Uint8Array))throw Error('Whole-Blob path used');
                    if(q.mode==='fail' && q.bytes>=1000000)throw Error('Injected disk write failure');
                    await sink.write(chunk);q.bytes+=chunk.byteLength;q.chunks++;q.maxChunk=Math.max(q.maxChunk,chunk.byteLength);
                    if(q.mode==='cancel')await new Promise(resolve=>setTimeout(resolve,20));
                  },
                  close:async()=>{await sink.close();window.__streamQA.closed=true;},
                  abort:async()=>{await sink.abort();window.__streamQA.aborted=true;}
                };
              }});
            }''')
            session=page.context.new_cdp_session(page)
            session.send('HeapProfiler.collectGarbage')
            def memory():
                heap=session.send('Runtime.getHeapUsage');renderer=0;total=0
                pending=[os.getpid()]
                while pending:
                    pid=pending.pop()
                    try:
                        proc=Path('/proc',str(pid));pending.extend(map(int,(proc/'task'/str(pid)/'children').read_text().split()))
                        name=(proc/'comm').read_text().strip()
                        if 'chrome' not in name and 'headless' not in name:continue
                        status=(proc/'status').read_text();command=(proc/'cmdline').read_bytes()
                        rss=next(int(line.split()[1])*1024 for line in status.splitlines() if line.startswith('VmRSS:'))
                    except (OSError,StopIteration):continue
                    total+=rss
                    if b'--type=renderer' in command:renderer+=rss
                return {'jsHeap':heap['usedSize'],'rendererRss':renderer,'browserProcessesRss':total}
            baseline=memory();samples=[]
            page.evaluate('''()=>{const q=window.__transferLatency={active:true,frames:[],inputs:[],last:performance.now()};
              requestAnimationFrame(function tick(now){if(!q.active)return;q.frames.push(now-q.last);q.last=now;requestAnimationFrame(tick);});
              window.addEventListener('pointermove',event=>{if(q.active)q.inputs.push(performance.now()-event.timeStamp);},{passive:true});}''')
            def state():return page.evaluate('window.__streamQA')
            def reset(mode):page.evaluate('mode=>{window.__streamQA={mode,bytes:0,chunks:0,maxChunk:0,closed:false,aborted:false};}',mode)
            def begin():check.menu('Assembly','Export FASTA…')
            def wait_for(key):
                deadline=time.monotonic()+90
                while not state()[key]:
                    if time.monotonic()>=deadline:
                        page.screenshot(path=str(out/'timeout.png'))
                        raise AssertionError((state(),key,check.errors,page.locator('[data-sonner-toast]').all_text_contents()))
                    page.wait_for_timeout(50);page.mouse.move(600+len(samples)%50,400);samples.append(memory())
            def readback(name):
                with page.expect_download() as download:
                    page.evaluate('''async()=>{const file=await window.__streamHandle.getFile();const url=URL.createObjectURL(file),a=document.createElement('a');a.href=url;a.download='readback.fa';a.click();setTimeout(()=>URL.revokeObjectURL(url),1000);}''')
                path=out/name;download.value.save_as(path);return path
            begin();wait_for('closed');check.settle();success=state()
            assert success['bytes']>length and success['chunks']>10,success
            peak={key:max(sample[key] for sample in samples) for key in baseline}
            delta={key:peak[key]-baseline[key] for key in baseline}
            assert delta['jsHeap']<64*1024*1024,(baseline,peak)
            assert baseline['rendererRss']>0 and delta['rendererRss']<256*1024*1024,(baseline,peak)
            assert delta['browserProcessesRss']<384*1024*1024,(baseline,peak)
            latency=page.evaluate('''()=>{window.__transferLatency.active=false;return window.__transferLatency;}''')
            percentile=lambda values:sorted(values)[max(0,math.ceil(len(values)*.95)-1)]
            assert len(latency['frames'])>=10 and len(latency['inputs'])>=5,latency
            frame_p95,input_p95=percentile(latency['frames']),percentile(latency['inputs'])
            assert frame_p95<=33 and input_p95<=100,(frame_p95,input_p95)
            page.get_by_text('Assembly FASTA saved: assembly.fa',exact=True).last.wait_for()
            page.screenshot(path=str(out/'large-export-saved.png'))
            target=readback('assembly.fa');sequence=hashlib.sha256();bases=0;headers=0
            with target.open('rb') as exported:
                for line in exported:
                    if line.startswith(b'>'):headers+=1;continue
                    line=line.strip();sequence.update(line);bases+=len(line)
            expected=hashlib.sha256();pattern=b'ACGTRYMKSWBDHVNAC'*1024;remaining=length
            while remaining:
                block=pattern[:min(remaining,len(pattern))];expected.update(block);remaining-=len(block)
            assert bases==length and headers==1 and sequence.digest()==expected.digest(),(bases,headers)
            completed_hash=digest(target)
            reset('cancel');begin();deadline=time.monotonic()+60
            while state()['bytes']<1_000_000:
                assert time.monotonic()<deadline,(state(),check.errors)
                page.wait_for_timeout(50)
            page.locator('[data-sonner-toast]').filter(has=page.get_by_text('Exporting Assembly FASTA…',exact=True)).get_by_role('button',name='Cancel',exact=True).click()
            wait_for('aborted');check.settle();cancelled=state();assert not cancelled['closed']
            assert digest(readback('after-cancel.fa'))==completed_hash
            reset('fail');begin();wait_for('aborted');check.settle();failed=state();assert not failed['closed']
            assert digest(readback('after-failure.fa'))==completed_hash
            page.get_by_text('Injected disk write failure',exact=True).last.wait_for();page.screenshot(path=str(out/'failed-stream-export.png'))
            assert not check.errors,check.errors
            assert digest(native)==source_hash and digest(data/'sequence.fa')==fasta_hash
            (out/'results.json').write_text(json.dumps({'status':'ok','checks':['512 Mb ordered exact-sequence transfer through native OPFS writer','sampled persistent Chromium JS heap and Linux process RSS','frame/input acknowledgement during transfer','cancel aborts writer and preserves previous file','injected write failure aborts and preserves previous file','unchanged native/FASTA inputs'],'success':success,'cancelled':cancelled,'failed':failed,'sourceSha256':source_hash,'fastaSha256':fasta_hash,'outputSha256':completed_hash,'baselineMemoryBytes':baseline,'peakMemoryBytes':peak,'peakDeltaBytes':delta,'frameP95Ms':frame_p95,'inputAcknowledgementP95Ms':input_p95,'frameSamples':len(latency['frames']),'inputSamples':len(latency['inputs']),'pageErrors':[]},indent=2))
            context.close()
    finally:static.shutdown();static.server_close()


if __name__=='__main__':main()
