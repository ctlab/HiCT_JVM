#!/usr/bin/env python3
"""Exercise real Electron/GTK save and cancel dialogs on a private Xvfb display.

Requires Xvfb and xdotool (paths configurable), a built WebUI, and an isolated
quad-data test server. No dialogs or input events reach the user's desktop.
"""
import argparse
import json
import os
from pathlib import Path
import select
import socket
import subprocess
import time
from urllib.request import urlopen

from playwright.sync_api import sync_playwright


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--webui', default=str(Path(__file__).resolve().parents[2] / 'HiCT_WebUI'))
    parser.add_argument('--url', default='http://127.0.0.1:8187')
    parser.add_argument('--api', default='http://127.0.0.1:5107')
    parser.add_argument('--output', default='/tmp/hict-release-physical/electron')
    parser.add_argument('--xvfb', default='/tmp/hict-desktop-test/runtime/usr/bin/Xvfb')
    parser.add_argument('--xdotool', default='/tmp/hict-desktop-test/runtime/usr/bin/xdotool')
    args = parser.parse_args()
    out = Path(args.output).absolute()
    out.mkdir(parents=True, exist_ok=True)
    if out != out.resolve() or not out.is_relative_to('/tmp'):
        raise ValueError('Use an isolated output/profile directory under /tmp')
    env = os.environ.copy()
    for key in ('HOME', 'XDG_CONFIG_HOME', 'XDG_CACHE_HOME', 'XDG_RUNTIME_DIR'):
        directory = out / key.lower()
        directory.mkdir(mode=0o700, exist_ok=True)
        env[key] = str(directory)
    env.pop('ELECTRON_RUN_AS_NODE', None)
    env['DBUS_SESSION_BUS_ADDRESS'] = 'disabled:'
    env['GSETTINGS_BACKEND'] = 'memory'
    env['LD_LIBRARY_PATH'] = '/tmp/hict-desktop-test/runtime/usr/lib/x86_64-linux-gnu'
    # Ask Xvfb to allocate an unused display; never use DISPLAY from the parent.
    read_fd, write_fd = os.pipe()
    results, errors = [], []
    electron = None
    with (out / 'xvfb.log').open('w') as xlog, (out / 'electron.log').open('w') as elog:
        xvfb = subprocess.Popen([args.xvfb, '-displayfd', str(write_fd), '-screen', '0', '1600x1100x24',
                                 '-nolisten', 'tcp'], pass_fds=(write_fd,), env=env, stdout=xlog, stderr=xlog)
        os.close(write_fd)
        try:
            if not select.select([read_fd], [], [], 15)[0]:
                raise RuntimeError('Xvfb did not allocate a display; inspect xvfb.log')
            display = os.read(read_fd, 32).decode().strip()
            if not display.isdecimal():
                raise RuntimeError('Xvfb exited without allocating a display')
            env['DISPLAY'] = ':' + display
            with socket.socket() as port_probe:
                port_probe.bind(('127.0.0.1', 0))
                port = port_probe.getsockname()[1]
            webui = Path(args.webui)
            command = [
                str(webui / 'node_modules/electron/dist/electron'),
                '--no-sandbox', '--disable-gpu', '--ozone-platform=x11',
                '--remote-debugging-port=' + str(port), '--user-data-dir=' + str(out / 'profile'),
                str(webui / 'dist/electron/main/main.js'), args.url,
            ]
            endpoint = f'http://127.0.0.1:{port}'

            def launch():
                nonlocal electron
                electron = subprocess.Popen(command, env=env, stdout=elog, stderr=elog)
                deadline = time.monotonic() + 30
                while True:
                    if electron.poll() is not None:
                        raise RuntimeError('Electron exited; inspect electron.log')
                    try:
                        with urlopen(endpoint + '/json/version', timeout=1):
                            return
                    except OSError:
                        if time.monotonic() > deadline:
                            raise RuntimeError('Electron debugging endpoint did not start')
                        time.sleep(0.1)

            launch()

            def xdo(*arguments):
                return subprocess.run([args.xdotool, *arguments], env=env, capture_output=True,
                                      text=True, timeout=10, check=False)

            with sync_playwright() as pw:
                browser = pw.chromium.connect_over_cdp(endpoint)
                context = browser.contexts[0]
                context.add_init_script('window.__HICT_BENCHMARK_ENABLED__ = true')
                page = context.pages[0] if context.pages else context.wait_for_event('page', timeout=30000)
                page.set_default_timeout(60000)
                page.on('pageerror', lambda error: errors.append(str(error)))
                page.on('console', lambda message: errors.append(message.text[:200])
                        if message.text.startswith('Could not load image') else None)
                page.route('http://localhost:5000/**', lambda route: route.continue_(
                    url=args.api + route.request.url.removeprefix('http://localhost:5000')))
                page.reload()
                page.wait_for_function('Boolean(window.__hictBenchmark && window.hictDesktop?.saveExport)')
                assert page.evaluate('''async () => {
                    const url=URL.createObjectURL(new Blob(['local PDF raster']));
                    try { return await (await fetch(url)).text(); }
                    finally { URL.revokeObjectURL(url); }
                }''') == 'local PDF raster', 'CSP must permit local raster blob reads'
                page.evaluate('api => window.__hictBenchmark.open("combined_ind2_4DN.hict.hdf5", api)', args.api)
                page.wait_for_timeout(1500)
                nan = page.get_by_role('button', name='Do not use Cooler weights', exact=True)
                if nan.is_visible():
                    nan.click()
                page.evaluate('''async () => {
                    const m=window.__hictBenchmark.manager();
                    const color={type:'colormap',mode:'LINEAR',input:{type:'source',source:'PRIMARY'},
                        minSignal:0,maxSignal:100,startColor:'#ffffffff',endColor:'#ff0000ff'};
                    await m.networkManager.requestManager.setRenderPipelineConfig({enabled:true,
                        swapUpperLower:false,upperExpression:color,lowerExpression:color});
                    await m.reloadTilesFromBackend();
                }''')
                page.wait_for_timeout(1000)
                cases = [(scope, fmt, False) for scope in ('viewport', 'whole-map') for fmt in ('Svg', 'Png', 'Pdf')]
                cases.append(('whole-map', 'Pdf', True))
                for scope, fmt, cancel in cases:
                    target = out / f'{scope}{"-cancel" if cancel else ""}.{fmt.lower()}'
                    if target.exists():
                        raise FileExistsError(f'Use a fresh output directory: {target}')
                    page.evaluate('''({scope,fmt}) => {
                        window.exportProgress=[];window.nativeExportDone=false;window.nativeExportError=null;
                        window.__hictBenchmark.manager()['exportCurrentMap'+fmt](
                            p=>window.exportProgress.push(p),{scope,cropToMap:true})
                            .catch(e=>window.nativeExportError=String(e))
                            .finally(()=>window.nativeExportDone=true);
                    }''', {'scope': scope, 'fmt': fmt})
                    deadline = time.monotonic() + 60
                    while True:
                        windows = xdo('search', '--onlyvisible', '--name', '^Save HiCT export$').stdout.splitlines()
                        if windows:
                            break
                        export_error = page.evaluate('window.nativeExportError')
                        if export_error:
                            raise RuntimeError('Export failed before native save: ' + export_error)
                        if time.monotonic() > deadline:
                            raise RuntimeError('Native save dialog did not appear: ' + str(page.evaluate('window.nativeExportError')))
                        page.wait_for_timeout(100)
                    window = windows[-1]
                    transient = subprocess.check_output(['xprop', '-id', window, 'WM_TRANSIENT_FOR'], env=env, text=True)
                    assert 'window id #' in transient and not transient.rstrip().endswith('0x0'), transient
                    subprocess.run(['import', '-display', env['DISPLAY'], '-window', 'root',
                                    str(out / f'{scope}-{fmt.lower()}-dialog.png')], env=env, check=True, timeout=20)
                    assert xdo('windowfocus', '--sync', window).returncode == 0
                    if cancel:
                        assert xdo('key', 'Escape').returncode == 0
                    else:
                        assert xdo('key', 'ctrl+l').returncode == 0
                        assert xdo('key', 'ctrl+a').returncode == 0
                        assert xdo('type', '--clearmodifiers', '--delay', '1', str(target)).returncode == 0
                        assert xdo('key', 'Return').returncode == 0
                        time.sleep(0.5)
                        subprocess.run(['import', '-display', env['DISPLAY'], '-window', 'root',
                                        str(out / f'{scope}-{fmt.lower()}-location.png')], env=env, check=True, timeout=20)
                        if xdo('search', '--onlyvisible', '--name', '^Save HiCT export$').stdout.strip():
                            assert xdo('key', 'alt+s').returncode == 0
                    page.wait_for_function('window.nativeExportDone', timeout=30000)
                    assert page.evaluate('window.nativeExportError') is None
                    progress = page.evaluate('window.exportProgress')
                    assert progress[0] > 0 and progress[-1] == 1, progress
                    if cancel:
                        assert not target.exists()
                    else:
                        assert target.stat().st_size > 1000, target
                    results.append({'scope': scope, 'format': fmt, 'cancel': cancel,
                                    'bytes': target.stat().st_size if target.exists() else 0,
                                    'progress': progress, 'parent': transient.strip()})
                assert not errors, errors
                from check_tauri_export import validate_rendered_exports
                results.extend(validate_rendered_exports(out, require_dark=False))
                page.screenshot(path=str(out / 'completed.png'))
                await_probe = '''async () => {
                    localStorage.setItem('hict-reset-probe','keep');
                    document.cookie='hict_reset_probe=keep; path=/; max-age=3600';
                    await new Promise((resolve,reject)=>{
                        const r=indexedDB.open('hict-reset-probe',1);
                        r.onupgradeneeded=()=>r.result.createObjectStore('probe');
                        r.onsuccess=()=>{r.result.close();resolve();};r.onerror=()=>reject(r.error);
                    });
                    await caches.open('hict-reset-probe');
                }'''
                page.evaluate(await_probe)
                storage_probe = '''async () => ({
                    local:localStorage.getItem('hict-reset-probe'),
                    cookie:document.cookie.includes('hict_reset_probe=keep'),
                    database:(await indexedDB.databases()).some(d=>d.name==='hict-reset-probe'),
                    cache:(await caches.keys()).includes('hict-reset-probe')
                })'''
                for reset in (False, True):
                    browser.close()
                    if electron.poll() is None:
                        electron.terminate()
                    electron.wait(timeout=10)
                    env['HICT_RESET_WEBUI'] = '1' if reset else '0'
                    launch()
                    browser = pw.chromium.connect_over_cdp(endpoint)
                    context = browser.contexts[0]
                    page = context.pages[0] if context.pages else context.wait_for_event('page', timeout=30000)
                    page.wait_for_url(args.url.rstrip('/') + '/**')
                    page.wait_for_load_state('domcontentloaded')
                    actual = page.evaluate(storage_probe)
                    expected = {'local': None if reset else 'keep', 'cookie': not reset,
                                'database': not reset, 'cache': not reset}
                    assert actual == expected, (reset, actual, expected)
                    assert (out / 'electron.log').read_text().count('HICT_WEBUI_RESET_COMPLETE') == int(reset), \
                        'Only completed storage resets may be acknowledged'
                    results.append({'test': 'browser storage reset' if reset else 'storage persists across restart',
                                    'storage': actual})
                (out / 'results.json').write_text(json.dumps(results, indent=2) + '\n')
                browser.close()
        finally:
            os.close(read_fd)
            if electron is not None and electron.poll() is None:
                electron.terminate()
                try:
                    electron.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    electron.kill()
                    electron.wait()
            xvfb.terminate()
            xvfb.wait(timeout=10)
    print(json.dumps(results, indent=2))


if __name__ == '__main__':
    main()
