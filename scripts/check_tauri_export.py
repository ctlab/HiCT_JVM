#!/usr/bin/env python3
"""Real Tauri save/cancel and storage-reset acceptance on an isolated X display.

Reuse quad-map exports to test IPC, or render the real WebUI inside WebKit.
Requires Xvfb, xdotool and the GTK desktop portal backend; integrated rendering
also requires Pillow, numpy, Inkscape and Poppler's pdftoppm.
"""
import argparse
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import mimetypes
import os
from pathlib import Path
import queue
import select
import shutil
import subprocess
import threading
import time
from urllib.parse import urlsplit, unquote

PAGE = br"""<!doctype html><meta charset="utf-8"><title>HiCT native acceptance</title>
<h1>HiCT native export acceptance</h1><script>
const report=value=>fetch('/event',{method:'POST',body:JSON.stringify(value)});
let ticks=0;setInterval(()=>ticks++,100);
window.onerror=message=>report({error:String(message)});
window.onunhandledrejection=event=>report({error:String(event.reason)});
const originalWarn=console.warn;
console.warn=(...args)=>{const message=args.map(String).join(' ');
 if(message.startsWith('Could not load image'))report({error:message.slice(0,80)+' ... '+message.slice(message.lastIndexOf('\n'))});
 originalWarn.apply(console,args);};
async function state(){return {local:localStorage.getItem('hict-test'),cookie:document.cookie,
 databases:(await indexedDB.databases()).map(d=>d.name).filter(n=>n==='hict-test'),
 caches:(await caches.keys()).filter(n=>n==='hict-test')};}
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
async function until(check){const end=Date.now()+60000;while(!check()){
 if(Date.now()>end)throw Error('WebUI initialization timed out');await sleep(100);}}
function viewState(){const v=window.__hictBenchmark.manager().getView();
 return [v.getCenter(),v.getResolution(),v.getRotation()];}
function mapPixels(){let red=0,dark=0;
 for(const c of document.querySelectorAll('#hic-contact-map .ol-layer canvas')){
  const ctx=c.getContext('2d');if(!ctx)continue;
  const a=ctx.getImageData(0,0,c.width,c.height).data;
  for(let i=0;i<a.length;i+=4){if(a[i+3]<100)continue;
   if(a[i]>a[i+1]+30&&a[i]>a[i+2]+30)red++;
   if(a[i]<100&&a[i+1]<100&&a[i+2]<100)dark++;}}
 return {red,dark};}
async function openMap(){
 await until(()=>window.__hictBenchmark);
 await window.__hictBenchmark.open('combined_ind2_4DN.hict.hdf5',window.__HICT_ACCEPTANCE__.api);
 await sleep(1200);
 const nan=Array.from(document.querySelectorAll('button')).find(b=>b.textContent.trim()==='Do not use Cooler weights');
 if(nan)nan.click();
 const m=window.__hictBenchmark.manager();
 const status=await m.networkManager.requestManager.openSecondarySource('ind2.self.hict.hdf5',true);
 m.viewAndLayersManager.setSecondaryResolutionModel(status.compatibility);
 const color=(source,endColor,maxSignal)=>({type:'colormap',mode:'LINEAR',input:{type:'source',source},
  minSignal:0,maxSignal,startColor:'#ffffff00',endColor});
 const overlay={type:'pixel_blend',mode:'OVER',top:color('SECONDARY','#000000ff',20),
  bottom:color('PRIMARY','#ff0000ff',100),topOpacity:0.65,bottomOpacity:1};
 await m.networkManager.requestManager.setRenderPipelineConfig({enabled:true,swapUpperLower:false,
  upperExpression:overlay,lowerExpression:overlay});
 await m.reloadTilesFromBackend();m.viewAndLayersManager.fitViewToActiveExtent();
 m.getView().setResolution(m.getView().getResolution()/2);
 await until(()=>{const p=mapPixels();return p.red>200&&p.dark>200});
 return mapPixels();}
async function seed(){
 localStorage.setItem('hict-test','retained');document.cookie='hict_test=retained;max-age=3600;path=/';
 await new Promise((resolve,reject)=>{const r=indexedDB.open('hict-test',1);
 r.onupgradeneeded=()=>r.result.createObjectStore('values');
 r.onsuccess=()=>{r.result.close();resolve()};r.onerror=()=>reject(r.error);});
 await (await caches.open('hict-test')).put('/cached',new Response('retained'));}
(async()=>{
 const pixels=window.__HICT_ACCEPTANCE__?await openMap():null;
 await report({ready:await state(),bridge:Boolean(window.__TAURI__?.core?.invoke),pixels});
 while(true){
  const c=await (await fetch('/command')).json();
  if(!c){await new Promise(r=>setTimeout(r,100));continue;}
  try{
   if(c.action==='seed'){await seed();await report({seeded:await state()});continue;}
   if(c.action==='quit'){await window.__TAURI__.core.invoke('quit_app');return;}
   if(window.__HICT_ACCEPTANCE__){
    const [scope,extension]=c.name.split('.');const start=ticks,before=viewState(),progress=[];
    const format={svg:'Svg',png:'Png',pdf:'Pdf'}[extension];
    await window.__hictBenchmark.manager()['exportCurrentMap'+format](p=>progress.push(p),{scope,cropToMap:true});
    await report({saved:c.name,result:null,ticks:ticks-start,before,after:viewState(),progress,pixels:mapPixels()});
    continue;
   }
   const bytes=Array.from(new Uint8Array(await (await fetch('/asset/'+c.name)).arrayBuffer()));
   const start=ticks;
   const result=await window.__TAURI__.core.invoke('save_export',{filename:c.name,bytes});
   await report({saved:c.name,result,ticks:ticks-start});
  }catch(error){await report({error:String(error)});}
 }
})();</script>"""


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--binary', required=True)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('--exports', help='Reuse already-rendered exports for native bridge testing')
    mode.add_argument('--webui', help='Built WebUI dist directory for integrated rendering tests')
    parser.add_argument('--api', default='http://127.0.0.1:5107')
    parser.add_argument('--data-dir', help='Dedicated backend DATA_DIR with physical quad-map copies')
    parser.add_argument('--output', required=True, help='Fresh directory under /tmp')
    parser.add_argument('--reset-runs', type=int, default=3)
    parser.add_argument('--xvfb', default='/tmp/hict-desktop-test/runtime/usr/bin/Xvfb')
    parser.add_argument('--xdotool', default='/tmp/hict-desktop-test/runtime/usr/bin/xdotool')
    args = parser.parse_args()
    if args.reset_runs < 1:
        raise ValueError('At least one reset run is required')
    output = Path(args.output).absolute()
    if output != output.resolve() or not output.is_relative_to('/tmp'):
        raise ValueError('Use an isolated /tmp output directory')
    output.mkdir(exist_ok=False)
    names = [f'{scope}.{fmt}' for scope in ('viewport', 'whole-map') for fmt in ('svg', 'png', 'pdf')]
    assets = {name: Path(args.exports) / name for name in names} if args.exports else {}
    if args.exports and not all(p.is_file() for p in assets.values()):
        raise ValueError('All six rendered image artifacts are required')
    webui = Path(args.webui).resolve() if args.webui else None
    page = PAGE
    if webui:
        # Restarts must serve the same hashed assets even if another build runs.
        snapshot = output / 'webui-snapshot'
        shutil.copytree(webui, snapshot)
        webui = snapshot
        data = Path(args.data_dir or '').absolute()
        if data != data.resolve() or not data.is_relative_to('/tmp'):
            raise ValueError('Use a dedicated /tmp backend DATA_DIR')
        for name in ('combined_ind2_4DN.hict.hdf5', 'ind2.self.hict.hdf5'):
            path = data / name
            if not path.is_file() or path.is_symlink() or path.stat().st_nlink != 1:
                raise ValueError(f'Use a physical input copy: {path}')
        config = json.dumps({'api': args.api})
        head = f'<script>window.__HICT_BENCHMARK_ENABLED__=true;window.__HICT_ACCEPTANCE__={config};</script>'.encode()
        page = (webui / 'index.html').read_bytes().replace(b'</head>', head + b'</head>')
        page = page.replace(b'</body>', PAGE[PAGE.index(b'<script>'):] + b'</body>')
    commands, events = queue.Queue(), queue.Queue()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            if self.path == '/command':
                try:
                    value = commands.get_nowait()
                except queue.Empty:
                    value = None
                body, mime = json.dumps(value).encode(), 'application/json'
            elif self.path.startswith('/asset/') and self.path[7:] in assets:
                body, mime = assets[self.path[7:]].read_bytes(), 'application/octet-stream'
            elif webui and urlsplit(self.path).path != '/':
                path = (webui / unquote(urlsplit(self.path).path).lstrip('/')).resolve()
                if not path.is_relative_to(webui) or not path.is_file():
                    self.send_error(404)
                    return
                body, mime = path.read_bytes(), mimetypes.guess_type(str(path))[0] or 'application/octet-stream'
            else:
                body, mime = page, 'text/html'
            self.send_response(200)
            self.send_header('Content-Type', mime)
            self.send_header('Content-Length', str(len(body)))
            self.send_header('Cache-Control', 'no-store')
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self):
            events.put(json.loads(self.rfile.read(int(self.headers['Content-Length']))))
            self.send_response(204)
            self.end_headers()

    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    env = os.environ.copy()
    for key in ('HOME', 'XDG_CONFIG_HOME', 'XDG_CACHE_HOME', 'XDG_DATA_HOME', 'XDG_RUNTIME_DIR'):
        path = output / key.lower()
        path.mkdir(mode=0o700)
        env[key] = str(path)
    env.update(GSETTINGS_BACKEND='memory', XDG_CURRENT_DESKTOP='GNOME', GDK_BACKEND='x11',
               LIBGL_ALWAYS_SOFTWARE='1', HICT_EXPORT_DIR=str(output),
               LD_LIBRARY_PATH='/tmp/hict-desktop-test/runtime/usr/lib/x86_64-linux-gnu')
    for key in ('WAYLAND_DISPLAY', 'DBUS_SESSION_BUS_ADDRESS', 'HICT_RESET_WEBUI'):
        env.pop(key, None)
    processes, results = [], []

    def event():
        value = events.get(timeout=60)
        if 'error' in value:
            raise AssertionError(value)
        return value

    def xdo(*arguments):
        return subprocess.run([args.xdotool, *arguments], env=env, capture_output=True,
                              text=True, check=False, timeout=10)

    with (output / 'native.log').open('w') as log:
        try:
            read_fd, write_fd = os.pipe()
            xvfb = subprocess.Popen([args.xvfb, '-displayfd', str(write_fd), '-screen', '0',
                                     '1600x1100x24', '-nolisten', 'tcp'], env=env,
                                    pass_fds=(write_fd,), stdout=log, stderr=log)
            processes.append(xvfb)
            os.close(write_fd)
            try:
                if not select.select([read_fd], [], [], 15)[0]:
                    raise RuntimeError('Xvfb did not start')
                display = os.read(read_fd, 32).decode().strip()
                if not display.isdecimal():
                    raise RuntimeError('Xvfb exited without a display')
                env['DISPLAY'] = ':' + display
            finally:
                os.close(read_fd)
            bus = subprocess.Popen(['dbus-daemon', '--session', '--nofork', '--print-address=1'],
                                   env=env, stdout=subprocess.PIPE, stderr=log, text=True)
            processes.append(bus)
            if not select.select([bus.stdout], [], [], 15)[0]:
                raise RuntimeError('D-Bus did not start')
            env['DBUS_SESSION_BUS_ADDRESS'] = bus.stdout.readline().strip()

            def launch(reset=False):
                acknowledgements = (output / 'native.log').read_text().count('HICT_WEBUI_RESET_COMPLETE')
                child_env = dict(env, HICT_RESET_WEBUI='1' if reset else '0')
                app = subprocess.Popen([str(Path(args.binary).resolve()),
                                        f'http://127.0.0.1:{server.server_port}'],
                                       env=child_env, stdout=log, stderr=log)
                processes.append(app)
                ready = event()
                assert ready.get('bridge'), ready
                assert (output / 'native.log').read_text().count('HICT_WEBUI_RESET_COMPLETE') == acknowledgements + int(reset), \
                    'Reset acknowledgement must occur exactly once, before the page loads'
                if webui:
                    assert ready['pixels']['red'] > 200 and ready['pixels']['dark'] > 200, ready
                    results.append({'test': 'WebKit renders Hi-C and dotplot', 'pixels': ready['pixels']})
                return app, ready['ready']

            def quit_app(app):
                commands.put({'action': 'quit'})
                app.wait(timeout=15)
                assert app.returncode == 0

            app, state = launch()
            empty = {'local': None, 'cookie': '', 'databases': [], 'caches': []}
            assert state == empty, state
            commands.put({'action': 'seed'})
            seeded = event()['seeded']
            assert seeded['local'] == 'retained' and 'hict-test' in seeded['databases']
            for name in [*names, 'viewport.pdf']:
                cancel = (output / name).exists()
                previous = hashlib.sha256((output / name).read_bytes()).digest() if cancel else None
                commands.put({'action': 'save', 'name': name})
                deadline = time.monotonic() + 45
                while True:
                    windows = xdo('search', '--onlyvisible', '--name', '^Save HiCT export$').stdout.splitlines()
                    if windows:
                        break
                    if not events.empty():
                        raise AssertionError(('Save completed without a dialog', event()))
                    if time.monotonic() > deadline:
                        raise RuntimeError('No native dialog; inspect native.log')
                    time.sleep(0.1)
                window = windows[-1]
                transient = subprocess.check_output(['xprop', '-id', window, 'WM_TRANSIENT_FOR'], env=env, text=True)
                assert 'window id #' in transient and not transient.rstrip().endswith('0x0'), transient
                subprocess.run(['import', '-display', env['DISPLAY'], '-window', 'root',
                                str(output / f'{name}{"-cancel" if cancel else ""}-dialog.png')],
                               env=env, check=True, timeout=20)
                assert xdo('windowfocus', '--sync', window).returncode == 0
                if cancel:
                    assert xdo('key', 'Escape').returncode == 0
                else:
                    if webui:
                        # Keep the chooser's preset directory but use a stable
                        # artifact name instead of the app's timestamped default.
                        assert xdo('key', 'ctrl+a').returncode == 0
                        assert xdo('type', '--clearmodifiers', '--delay', '1', name).returncode == 0
                    # The production dialog already has the requested name and
                    # HICT_EXPORT_DIR. Avoid GTK filename completion selecting a
                    # similarly named screenshot when entering an absolute path.
                    geometry = dict(line.split('=', 1) for line in
                                    xdo('getwindowgeometry', '--shell', window).stdout.splitlines())
                    assert xdo('mousemove', '--window', window,
                               str(int(geometry['WIDTH']) - 50),
                               str(int(geometry['HEIGHT']) - 24), 'click', '1').returncode == 0
                saved = event()
                assert saved['saved'] == name and saved['ticks'] > 0, saved
                deadline = time.monotonic() + 10
                while xdo('search', '--onlyvisible', '--name', '^Save HiCT export$').stdout.strip():
                    if time.monotonic() > deadline:
                        raise RuntimeError('Native chooser did not close after save/cancel')
                    time.sleep(0.1)
                actual = hashlib.sha256((output / name).read_bytes()).digest()
                if webui:
                    assert saved['before'] == saved['after'], saved
                    assert saved['progress'][0] > 0 and saved['progress'][-1] == 1, saved
                    assert (output / name).stat().st_size > 1000
                else:
                    assert saved['result'] == (None if cancel else str(output / name)), saved
                    assert actual == hashlib.sha256(assets[name].read_bytes()).digest()
                if cancel:
                    assert actual == previous, 'Cancellation changed an existing export'
                results.append({'name': name, 'cancel': cancel, 'heartbeatTicks': saved['ticks'],
                                'bytes': (output / name).stat().st_size, 'details': saved})
            subprocess.run(['import', '-display', env['DISPLAY'], '-window', 'root',
                            str(output / 'completed.png')], env=env, check=True, timeout=20)
            for run in range(args.reset_runs):
                quit_app(app)
                app, state = launch()
                assert state == seeded, (state, seeded)
                results.append({'test': 'storage persists across restart', 'run': run + 1, 'state': state})
                quit_app(app)
                app, state = launch(reset=True)
                assert state == empty, state
                results.append({'test': 'reset clears storage before page load', 'run': run + 1, 'state': state})
                if run + 1 < args.reset_runs:
                    commands.put({'action': 'seed'})
                    assert event()['seeded'] == seeded
            quit_app(app)
            if webui:
                results.extend(validate_rendered_exports(output))
        finally:
            for process in reversed(processes):
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()
            server.shutdown()
            server.server_close()
            (output / 'results.json').write_text(json.dumps(results, indent=2))
    print(json.dumps(results, indent=2))


def validate_rendered_exports(output, require_dark=True):
    """Independent consumers must see the same heatmap, not merely a valid file."""
    import numpy as np
    from PIL import Image

    results = []
    with (output / 'rasterization.log').open('w') as log:
        for scope in ('viewport', 'whole-map'):
            subprocess.run(['inkscape', str(output / f'{scope}.svg'), '--export-type=png',
                            f'--export-filename={output / (scope + "-svg.png")}'],
                           stdout=log, stderr=log, check=True, timeout=120)
            subprocess.run(['pdftoppm', '-singlefile', '-r', '96', '-png',
                            str(output / f'{scope}.pdf'), str(output / f'{scope}-pdf')],
                           stdout=log, stderr=log, check=True, timeout=120)
            reference = np.array(Image.open(output / f'{scope}.png').convert('RGB')).astype(np.int16)
            if scope == 'whole-map':
                assert abs(reference.shape[0] - reference.shape[1]) <= 1, reference.shape
            # Exclude the minimap, ruler labels and outside border, so those cannot
            # satisfy the content assertion when a heatmap layer is missing.
            for fmt in ('svg', 'pdf'):
                actual = np.array(Image.open(output / f'{scope}-{fmt}.png').convert('RGB')).astype(np.int16)
                # Inkscape rounds fractional SVG page dimensions; canvas and
                # Poppler round up. Compare common pixels without rescaling.
                assert all(abs(a-b) <= 1 for a, b in zip(reference.shape[:2], actual.shape[:2])), (scope, fmt, reference.shape, actual.shape)
                height, width = (min(a, b) for a, b in zip(reference.shape[:2], actual.shape[:2]))
                expected = reference[96:height-16, 96:width-16]
                actual = actual[96:height-16, 96:width-16]
                red = (expected[:, :, 0] > expected[:, :, 1] + 30) & (expected[:, :, 0] > expected[:, :, 2] + 30)
                dark = (expected < 100).all(axis=2)
                assert red.sum() > 2000, (scope, red.sum())
                if require_dark:
                    assert dark.sum() > 5000, (scope, dark.sum())
                actual_red = (actual[:, :, 0] > actual[:, :, 1] + 30) & (actual[:, :, 0] > actual[:, :, 2] + 30)
                actual_dark = (actual < 100).all(axis=2)
                metrics = {'scope': scope, 'format': fmt,
                           'meanAbsoluteRgbError': float(np.abs(expected - actual).mean()),
                           'redRecall': float((red & actual_red).sum() / red.sum()),
                           'darkRecall': float((dark & actual_dark).sum() / dark.sum()) if dark.any() else None}
                assert metrics['meanAbsoluteRgbError'] < 10, metrics
                assert metrics['redRecall'] > 0.8, metrics
                if require_dark:
                    assert metrics['darkRecall'] > 0.8, metrics
                results.append({'test': 'Independent rasterized heatmap matches PNG', **metrics})
    (output / 'rasterization-results.json').write_text(json.dumps(results, indent=2))
    return results


if __name__ == '__main__':
    main()
