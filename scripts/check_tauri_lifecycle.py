#!/usr/bin/env python3
"""Real native Tauri page-replacement and Unix launcher-stop regression checks.

Use the same private driver/container/physical FASTA prerequisites as
check_tauri_coordinate_exports.py. Blob probes deliberately pause after one
native chunk, with actual GTK choosers; HTTP probes use the actual WebUI and JVM.
No original destination, assembly structure or external service is modified.
"""
import argparse
import json
from pathlib import Path
import time

from check_tauri_coordinate_exports import NativeCheck, digest, until


class LifecycleCheck(NativeCheck):
    def start_blob_probe(self, filename):
        self.js('''
            window.__hictLifecycleProbe = { state: "choosing" };
            window.__TAURI__.core.invoke("begin_export_stream", {
                filename: arguments[0], kind: "figure"
            }).then(async picked => {
                const probe = window.__hictLifecycleProbe;
                probe.picked = picked;
                if (!picked) { probe.state = "cancelled"; return; }
                await window.__TAURI__.core.invoke("write_export_chunk",
                    new Uint8Array(65536).fill(65), { headers: {
                        "x-hict-export-id": picked[0],
                        "x-hict-export-sequence": "0"
                    }});
                probe.state = "written";
            }).catch(error => { window.__hictLifecycleProbe.error = String(error); });
        ''', filename)

    def blob_navigation(self):
        target = self.saved / 'blob-navigation.bin'
        self.start_blob_probe(target.name)
        self.select_destination(target)
        def written():
            probe = self.js('return window.__hictLifecycleProbe;')
            assert not probe.get('error'), probe
            return probe if probe.get('state') == 'written' else None
        previous = until(written)
        staged = list(self.saved.glob('.hict-save-*.tmp'))
        assert len(staged) == 1 and staged[0].stat().st_size == 65536
        self.command('POST', '/refresh', {})
        self.clean()
        assert not target.exists()
        self.record('document refresh revokes a partial Blob', bytesBeforeRefresh=65536,
                    scope='controlled paused native producer after actual GTK selection',
                    priorOperation=previous['picked'][0])

    def late_chooser(self):
        target = self.saved / 'stale-chooser.bin'
        self.start_blob_probe(target.name)
        until(self.visible_save_dialog)
        self.screenshot('chooser-before-document-refresh.png')
        self.command('POST', '/refresh', {})
        # This answers the old page's native chooser after the replacement page
        # has started. The native command must reject its captured page token.
        self.select_destination(target)
        time.sleep(.2)
        self.clean()
        assert not target.exists()
        self.record('old page chooser answer cannot create a staged file',
                    scope='controlled native command with a real late GTK save answer')

    def run(self):
        source = Path(self.args.source_fasta)
        assert source == source.resolve() and source.is_relative_to('/tmp')
        assert source.is_file() and source.stat().st_nlink == 1 and source.stat().st_size >= 16_000_000
        before = digest(source)
        assert before == self.environment['fastaSha256']
        self.attach()
        self.blob_navigation()
        self.attach()
        self.late_chooser()
        self.attach()
        self.interrupted('navigation')
        self.attach()
        self.interrupted('sigterm')
        self.qualify(source, before)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('environment', 'container', 'binary', 'source-fasta', 'output'):
        parser.add_argument('--' + name, required=True)
    parser.set_defaults(resume=False)
    check = LifecycleCheck(parser.parse_args())
    try:
        check.run()
    except BaseException as error:
        (check.output / 'failure.json').write_text(json.dumps({
            'status': 'failed', 'error': repr(error)}, indent=2) + '\n')
        raise
    finally:
        check.close_session()


if __name__ == '__main__':
    main()
