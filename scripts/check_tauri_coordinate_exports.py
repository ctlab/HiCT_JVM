#!/usr/bin/env python3
"""Exercise the real Linux Tauri WebUI, native GTK choosers and coordinate writes.

Use an already running disposable JVM/WebUI pair and tauri-driver in a private
X11 container. The environment JSON supplies api, webdriver, display and the
nativeSha256. The container must mount the output's parent at the same path,
contain xdotool, ImageMagick and Python 3, and expose the snapshotted executable.
A single-record FASTA must already be linked to the open assembly. No source
assembly edits or external submissions are made. This is not installer or other
platform qualification. WebDriver's DELETE /window can terminate WebKit without
normal cleanup; this test sends the native WM_DELETE_WINDOW message instead.
"""
import argparse
import hashlib
import json
from pathlib import Path
import statistics
import subprocess
import time
from urllib.error import HTTPError
from urllib.request import Request, urlopen

ELEMENT = 'element-6066-11e4-a52e-4f735466cecf'
# A normal window-manager close, without a window manager in the private X server.
WM_DELETE = '''import ctypes as C,sys
X=C.CDLL('libX11.so.6')
class Data(C.Union):_fields_=[('b',C.c_char*20),('s',C.c_short*10),('l',C.c_long*5)]
class Client(C.Structure):_fields_=[('type',C.c_int),('serial',C.c_ulong),('send_event',C.c_int),('display',C.c_void_p),('window',C.c_ulong),('message_type',C.c_ulong),('format',C.c_int),('data',Data)]
class Event(C.Union):_fields_=[('xclient',Client),('pad',C.c_long*24)]
X.XOpenDisplay.argtypes=[C.c_char_p];X.XOpenDisplay.restype=C.c_void_p
X.XInternAtom.argtypes=[C.c_void_p,C.c_char_p,C.c_int];X.XInternAtom.restype=C.c_ulong
X.XSendEvent.argtypes=[C.c_void_p,C.c_ulong,C.c_int,C.c_long,C.POINTER(Event)];X.XFlush.argtypes=[C.c_void_p];X.XCloseDisplay.argtypes=[C.c_void_p]
d=X.XOpenDisplay(None);assert d;w=int(sys.argv[1]);e=Event();e.xclient.type=33;e.xclient.display=d;e.xclient.window=w;e.xclient.send_event=1;e.xclient.message_type=X.XInternAtom(d,b'WM_PROTOCOLS',0);e.xclient.format=32;e.xclient.data.l[0]=X.XInternAtom(d,b'WM_DELETE_WINDOW',0)
assert X.XSendEvent(d,w,0,0,C.byref(e));X.XFlush(d);X.XCloseDisplay(d)
'''


def until(check, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = check()
        if value:
            return value
        time.sleep(.05)
    raise TimeoutError(getattr(check, '__name__', 'condition'))


def digest(path):
    result = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(4 * 1024 * 1024), b''):
            result.update(block)
    return result.hexdigest()


def sequence(path):
    result, bases, records = hashlib.sha256(), 0, 0
    with path.open('rb') as stream:
        for line in stream:
            if line.startswith(b'>'):
                records += 1
            else:
                value = line.strip()
                result.update(value)
                bases += len(value)
    return {'records': records, 'bases': bases, 'sha256': result.hexdigest()}


class NativeCheck:
    def __init__(self, args):
        self.args = args
        self.environment = json.loads(Path(args.environment).read_text())
        assert digest(Path(args.binary)) == self.environment['nativeSha256'], 'Native snapshot hash mismatch'
        self.output = Path(args.output).absolute()
        if self.output != self.output.resolve() or not self.output.is_relative_to('/tmp'):
            raise ValueError('Use a fresh physical /tmp output directory')
        self.output.mkdir(exist_ok=args.resume)
        self.saved = self.output / 'exports with spaces'
        self.saved.mkdir(exist_ok=args.resume)
        self.results = json.loads((self.output / 'results.json').read_text()) if args.resume else []
        self.session = json.loads((self.output / 'session.json').read_text())['sessionId'] if args.resume else None
        self.base = ['docker', 'exec', '--user', '1000:1000', '-e',
                     'DISPLAY=' + self.environment['display'], args.container]

    def docker(self, *arguments):
        return subprocess.check_output(self.base + list(arguments), text=True, timeout=30)

    def wd(self, method, path, body=None):
        request = Request(self.environment['webdriver'] + path, method=method,
                          data=None if body is None else json.dumps(body).encode(),
                          headers={'Content-Type': 'application/json'})
        try:
            with urlopen(request, timeout=120) as response:
                value = json.load(response)['value']
        except HTTPError as error:
            raise RuntimeError(error.read().decode()) from None
        if isinstance(value, dict) and value.get('error'):
            raise RuntimeError(value)
        return value

    def command(self, method, path, body=None):
        return self.wd(method, '/session/' + self.session + path, body)

    def js(self, script, *arguments):
        return self.command('POST', '/execute/sync', {'script': script, 'args': list(arguments)})

    def element(self, using, value):
        return self.command('POST', '/element', {'using': using, 'value': value})[ELEMENT]

    def click(self, text):
        assert "'" not in text
        matches = self.command('POST', '/elements', {'using': 'xpath', 'value':
            "//*[self::a or self::button][normalize-space(.)='" + text + "']"})
        for match in matches:
            identifier = match[ELEMENT]
            if self.command('GET', '/element/' + identifier + '/displayed'):
                return self.command('POST', '/element/' + identifier + '/click', {})
        raise AssertionError('No visible control: ' + text)

    def entries(self):
        return self.js('return document.querySelector("#app").__vue_app__.config.globalProperties.$pinia._s.get("notificationCenter").entries;')

    def floor(self):
        return max([entry['id'] for entry in self.entries()], default=0)

    def outcome(self, floor):
        return until(lambda: next((entry for entry in self.entries() if entry['id'] > floor and
            (entry['level'] == 'error' or any(value in entry['message'] for value in
             ('export cancelled.', ' saved:', 'Session saved:', 'Session restored.')))), None))

    def record(self, case, **details):
        value = {'case': case, 'status': 'passed', **details}
        self.results.append(value)
        (self.output / 'results.json').write_text(json.dumps(self.results, indent=2) + '\n')
        print(json.dumps(value), flush=True)

    def screenshot(self, name):
        # WebDriver screenshots can omit WebKit composited images/icon fonts.
        self.docker('import', '-window', 'root', str(self.output / name))

    def attach(self):
        if self.session:
            try:
                self.wd('DELETE', '/session/' + self.session)
            except RuntimeError:
                pass
        value = self.wd('POST', '/session', {'capabilities': {'alwaysMatch': {
            'tauri:options': {'application': self.args.binary}}}})
        self.session = value['sessionId']
        (self.output / 'session.json').write_text(json.dumps(value, indent=2) + '\n')
        until(lambda: self.js('return document.body.innerText.includes("Attach to open session");'))
        self.click('Help')
        self.click('Connection…')
        element = self.element('css selector', '#api-gateway-input')
        self.command('POST', '/element/' + element + '/clear', {})
        api = self.environment['api']
        self.command('POST', '/element/' + element + '/value', {'text': api, 'value': list(api)})
        self.click('Connect')
        self.click('Attach to open session')
        until(lambda: self.js('return document.body.innerText.includes("Assembly:");'))
        assert self.js('return window.__HICT_TAURI_EXPORT_STREAM_VERSION__') == 1

    def dialog(self, menu, label, target=None, replace=False):
        self.click(menu)
        self.click(label)
        self.select_destination(target, replace)

    def visible_save_dialog(self):
        try:
            return self.docker('xdotool', 'search', '--onlyvisible', '--name', '^Save HiCT export$').splitlines()[-1]
        except (subprocess.CalledProcessError, IndexError):
            return None

    def select_destination(self, target=None, replace=False):
        window = until(self.visible_save_dialog)
        self.docker('xdotool', 'windowfocus', window)
        if target is None:
            self.docker('xdotool', 'key', '--clearmodifiers', 'Escape')
            return
        self.docker('xdotool', 'key', '--clearmodifiers', 'ctrl+a')
        self.docker('xdotool', 'type', '--clearmodifiers', str(target))
        self.docker('xdotool', 'key', 'Return')
        if replace:
            time.sleep(.5)
            self.screenshot(target.name + '-overwrite.png')
            self.docker('xdotool', 'key', '--clearmodifiers', 'alt+r')
        until(lambda: not self.visible_save_dialog())

    def clean(self):
        until(lambda: not list(self.saved.glob('.hict-save-*.tmp')))
        until(lambda: not list(Path(self.args.environment).parent.joinpath('java-tmp').glob('hict-fasta-export-*.fa')))

    def rss(self):
        raw = subprocess.check_output(['docker', 'top', self.args.container, '-eo', 'pid,comm,rss'], text=True)
        return sum(int(row.split()[-1]) * 1024 for row in raw.splitlines()[1:])

    def interrupted(self, mode):
        target = self.saved / (mode + '.fasta')
        floor = self.floor()
        self.dialog('Assembly', 'Export FASTA…', target)
        def arrived():
            assert not target.exists(), 'Transfer finished before cancellation was exercised'
            return max((path.stat().st_size for path in self.saved.glob('.hict-save-*.tmp')), default=0)
        amount = until(lambda: (value if (value := arrived()) >= 65536 else 0))
        started = time.monotonic()
        if mode == 'cancel':
            self.click('Cancel')
            notice = self.outcome(floor)
            assert 'cancelled' in notice['message'].lower(), notice
        elif mode == 'quit':
            self.click('File')
            try:
                self.click('Quit')
            except RuntimeError:
                # Native exit can beat WebDriver's click acknowledgement. The
                # process-exit and filesystem checks below must still succeed.
                pass
        elif mode == 'navigation':
            self.command('POST', '/refresh', {})
        elif mode == 'sigterm':
            # Match the Unix launcher's Process.destroy(), not SIGKILL or the
            # WebDriver browser-close command. Identify only this test binary.
            code = ('from pathlib import Path;import os,signal,json;found=[]\n'
                    'for proc in Path("/proc").iterdir():\n'
                    ' if proc.name.isdecimal():\n'
                    '  try:\n'
                    '   first=(proc/"cmdline").read_bytes().split(b"\\0")[0].decode()\n'
                    '   if first==' + repr(self.args.binary) + ':found.append(int(proc.name))\n'
                    '  except (OSError,UnicodeError):pass\n'
                    'assert len(found)==1,found\n'
                    'os.kill(found[0],signal.SIGTERM)\n')
            self.docker('python3', '-c', code)
        elif mode == 'native-window-close':
            window = self.docker('xdotool', 'search', '--onlyvisible', '--name', '^HiCT$').splitlines()[-1]
            self.docker('python3', '-c', WM_DELETE, window)
        else:
            raise ValueError('Unknown interruption mode: ' + mode)
        if mode not in ('cancel', 'navigation'):
            until(lambda: 'hict-tauri-brow' not in subprocess.check_output(
                ['docker', 'top', self.args.container, '-eo', 'pid,comm'], text=True))
        self.clean()
        assert not target.exists()
        elapsed = time.monotonic() - started
        if mode == 'sigterm':
            assert elapsed < 2, 'Cleanup exceeded the Unix launcher\'s graceful-stop window'
        self.record(mode + ' during transfer', observedStagingBytes=amount,
                    cleanupSeconds=elapsed)

    def run(self):
        source = Path(self.args.source_fasta)
        assert source.is_file() and source == source.resolve() and source.is_relative_to('/tmp')
        assert source.stat().st_nlink == 1, 'Use a physical disposable FASTA copy'
        before = digest(source)
        assert before == self.environment['fastaSha256'], 'Fixture hash mismatch'
        expected = sequence(source)
        assert expected['records'] == 1 and expected['bases'] >= 16_000_000
        if self.args.resume:
            completed = {result['case'] for result in self.results if result['status'] == 'passed'}
            required = {'native chooser Cancel', 'native AGP overwrite',
                        'native FASTA bytes and responsiveness', 'full destination preserves original',
                        'native session save and actual file restore', 'cancel during transfer'}
            assert required.issubset(completed), 'Resume only the remaining graceful-exit cases'
            assert before == self.environment['fastaSha256'], 'The fixture changed before resume'
            for mode in ('quit', 'native-window-close'):
                if mode + ' during transfer' not in completed:
                    self.attach()
                    self.interrupted(mode)
            self.qualify(source, before)
            return
        self.attach()
        floor = self.floor()
        self.dialog('Assembly', 'Export FASTA…')
        assert 'cancelled' in self.outcome(floor)['message'].lower()
        self.clean()
        self.record('native chooser Cancel')
        target = self.saved / 'overwritten.agp'
        original = b'original must survive until confirmed replacement\n'
        target.write_bytes(original)
        floor = self.floor()
        self.dialog('Assembly', 'Export to AGP', target, replace=True)
        assert self.outcome(floor)['level'] == 'success'
        assert target.read_bytes() != original and b'\tW\t' in target.read_bytes()
        self.clean()
        self.record('native AGP overwrite', bytes=target.stat().st_size, sha256=digest(target))
        target = self.saved / 'assembly.fasta'
        floor, samples, acknowledgements = self.floor(), [], []
        self.js('window.__nativeQA={active:true,frames:[],last:performance.now()};const tick=t=>{const q=window.__nativeQA;if(!q.active)return;q.frames.push(t-q.last);q.last=t;requestAnimationFrame(tick)};requestAnimationFrame(tick);')
        self.dialog('Assembly', 'Export FASTA…', target)
        started, deadline = time.monotonic(), time.monotonic() + 240
        while not target.exists():
            assert time.monotonic() < deadline
            assert not any(e['id'] > floor and e['level'] == 'error' for e in self.entries())
            samples.append(self.rss())
            tick = time.monotonic()
            self.js('return performance.now()')
            acknowledgements.append((time.monotonic() - tick) * 1000)
            time.sleep(.05)
        transfer_seconds = time.monotonic() - started
        frames = self.js('window.__nativeQA.active=false;return window.__nativeQA.frames;')
        assert self.outcome(floor)['level'] == 'success'
        assert sequence(target) == expected
        self.clean()
        self.screenshot('native-fasta-saved.png')
        self.record('native FASTA bytes and responsiveness', bytes=target.stat().st_size,
                    sha256=digest(target), sourceSequence=expected, secondsAfterChooserClosed=transfer_seconds,
                    rssSamples=samples, frameP95Ms=statistics.quantiles(frames, n=100)[94],
                    acknowledgementP95Ms=statistics.quantiles(acknowledgements, n=100)[94] if len(acknowledgements)>1 else acknowledgements)
        if self.args.full_volume:
            path = Path(self.args.full_volume) / 'protected.fasta'
            self.docker('python3', '-c', 'from pathlib import Path;Path(' + repr(str(path)) + ').write_text("original\\n")')
            floor = self.floor()
            self.dialog('Assembly', 'Export FASTA…', path, replace=True)
            notice = self.outcome(floor)
            assert notice['level'] == 'error' and 'storage space' in notice['message'], notice
            self.docker('python3', '-c', 'from pathlib import Path;p=Path(' + repr(str(path)) + ');assert p.read_text()=="original\\n";assert not list(p.parent.glob(".hict-save-*.tmp"))')
            self.clean()
            self.record('full destination preserves original', notification=notice)
        target = self.saved / 'session.json'
        floor = self.floor()
        self.dialog('File', 'Save session', target)
        assert self.outcome(floor)['level'] == 'success'
        assert json.loads(target.read_text())['format'] == 'hict-session'
        floor = self.floor()
        self.click('File')
        self.click('Open session')
        element = self.element('css selector', 'input[type="file"][accept="application/json"]')
        self.command('POST', '/element/' + element + '/value', {'text': str(target), 'value': list(str(target))})
        notice = self.outcome(floor)
        assert 'restored' in notice['message'].lower(), notice
        self.record('native session save and actual file restore', bytes=target.stat().st_size,
                    note='Automated WebKit suppresses the HTML open chooser; WebDriver supplies the actual saved file')
        self.interrupted('cancel')
        self.interrupted('quit')
        self.attach()
        self.interrupted('native-window-close')
        self.qualify(source, before)

    def qualify(self, source, before):
        assert digest(source) == before
        (self.output / 'qualification.json').write_text(json.dumps({
            'status': 'passed', 'environment': self.environment, 'sourceSha256': before,
            'cases': len(self.results), 'results': self.results}, indent=2) + '\n')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('environment', 'container', 'binary', 'source-fasta', 'output'):
        parser.add_argument('--' + name, required=True)
    parser.add_argument('--resume', action='store_true', help='Resume only graceful-exit cases after the first six cases passed')
    parser.add_argument('--full-volume', help='An existing bounded tmpfs inside the container')
    args = parser.parse_args()
    check = NativeCheck(args)
    try:
        check.run()
    except BaseException as error:
        (check.output / 'failure.json').write_text(json.dumps({'status': 'failed', 'error': repr(error)}, indent=2) + '\n')
        raise


if __name__ == '__main__':
    main()
