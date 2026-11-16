#!/usr/bin/env python3
"""Opt in to approximate export through the real UI and download its completed Jobs result."""
import argparse
import functools
import hashlib
import http.server
import json
from pathlib import Path
import subprocess
import shutil
import threading
import time

from playwright.sync_api import sync_playwright
from sequence_browser_harness import HictTestServer, BrowserCheck


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ('jar', 'webui', 'data-dir', 'output', 'cooler-python'):
        parser.add_argument('--' + key, required=True)
    args = parser.parse_args()
    data, output = Path(args.data_dir).absolute(), Path(args.output).absolute()
    if data != data.resolve() or not data.is_relative_to('/tmp') or not output.is_relative_to('/tmp'):
        parser.error('Use nonsymlinked disposable fixture paths under /tmp')
    source = data/'tiny-primary.hict.hdf5'
    if source.is_symlink() or source.stat().st_nlink != 1:
        parser.error('Use a physical 17 bp reinforcement fixture copy')
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    output.mkdir(parents=True, exist_ok=False)

    class Quiet(http.server.SimpleHTTPRequestHandler):
        def log_message(self, *_):
            pass

    static = http.server.ThreadingHTTPServer(('127.0.0.1', 0),
        functools.partial(Quiet, directory=str(Path(args.webui).resolve())))
    threading.Thread(target=static.serve_forever, daemon=True).start()
    try:
        with HictTestServer(args.jar, data, output/'server.log') as app, sync_playwright() as pw:
            app.http('/open', {'filename': source.name})
            original = {r:json.loads(app.http('/matrix/query', {'bpResolution':r,'rows':n,'cols':n,
                'signalMode':'RAW_COUNTS','format':'JSON'})) for r,n in ((4,5),(10,2))}
            browser = pw.chromium.launch(headless=True)
            page = browser.new_page(viewport={'width':1440,'height':1000}, accept_downloads=True)
            page.set_default_timeout(30000)
            page.add_init_script('window.__HICT_BENCHMARK_ENABLED__=true')
            page.route('http://localhost:5000/**', lambda route: route.continue_(
                url=app.url+route.request.url.removeprefix('http://localhost:5000')))
            check = BrowserCheck(page,output)
            page.goto(f'http://127.0.0.1:{static.server_port}/')
            check.menu('File','Attach'); check.settle()
            for offset in (5,3):
                page.evaluate('''async offset => {
                    const m=window.__hictBenchmark.manager();
                    const id=m.contigDimensionHolder.contigDescriptors[0].contigId;
                    m.eventManager.applyAssemblyInfo(await m.networkManager.requestManager.splitContigAtBp(id,offset));
                    await m.reloadVisuals();
                }''',offset)
                check.settle()
            app.http('/reverse_selection_range', {'startBP':0,'endBP':17})
            # Refresh the browser history and oriented descriptors before exporting.
            page.reload(); check.menu('File','Attach'); check.settle()
            snapshot = json.loads(app.http('/session/snapshot', {'verifyAssemblyRoundTrip':True}))['primary']
            oracle = output/'oracle.json'
            oracle.write_text(json.dumps({'contacts':original,'agp':snapshot['agp']}))
            check.menu('File','Export matrix')
            dialog = page.get_by_role('dialog').filter(has=page.get_by_text('Export matrix to Cooler',exact=True))
            checkbox = dialog.locator('#export-approximate-rebin')
            assert not checkbox.is_checked()
            dialog.get_by_role('button',name='Export Cooler',exact=True).click()
            dialog.get_by_text('Enable approximate rebinning to export this sequence-cut assembly, or turn off current assembly to export the saved matrix.',exact=True).wait_for()
            assert not json.loads(app.http('/convert/jobs/list', {}))
            checkbox.check()
            dialog.locator('.alert-danger').wait_for(state='hidden')
            assert dialog.locator('select').first.input_value() == 'internal'
            assert dialog.locator('select').first.is_disabled()
            assert not dialog.locator('#export-balance-coolers').is_checked()
            assert dialog.locator('#export-balance-coolers').is_disabled()
            checkbox.uncheck()
            assert dialog.locator('select').first.input_value() == 'auto'
            assert dialog.locator('#export-balance-coolers').is_checked()
            checkbox.check()
            dialog.locator('#export-all-resolutions').check()
            name = 'approximate-'+output.name+'.mcool'
            assert not (data/name).exists()
            dialog.locator('input[placeholder="matrix.mcool"]').fill(name)
            page.screenshot(path=str(output/'opt-in-export.png'))
            with page.expect_response('**/convert/jobs') as submitted:
                dialog.get_by_role('button',name='Export Cooler',exact=True).click()
            assert submitted.value.ok, submitted.value.text()
            job_id = submitted.value.json()['jobId']
            request = submitted.value.request.post_data_json
            assert request['approximateRebin'] is True and request['balanceExportedCoolers'] is False
            dialog.get_by_role('button',name='Dismiss',exact=True).click()
            page.locator('.jobs-button').click()
            job = page.locator('.job-row').filter(has=page.get_by_text('Download result',exact=True))
            deadline = time.monotonic()+60
            while time.monotonic()<deadline:
                result = json.loads(app.http('/convert/jobs/'+job_id))
                if result['status'] in ('finished','failed','cancelled'):
                    break
                page.wait_for_timeout(100)
            assert result['status']=='finished', result
            download_link = job.get_by_role('link',name='Download result',exact=True)
            download_link.wait_for()
            assert job.locator('.job-path').inner_text() == name
            artifact = output/'approximate.mcool'
            with page.expect_download() as download:
                download_link.click()
            download.value.save_as(artifact)
            page.screenshot(path=str(output/'finished-export-job.png'))
            assert hashlib.sha256(source.read_bytes()).hexdigest()==digest
            assert not check.errors, check.errors
            subprocess.run([args.cooler_python,str(Path(__file__).with_name('check_approximate_cooler.py')),
                '--artifact',str(artifact),'--oracle',str(oracle),'--output',str(output/'cooler-parity.json')],check=True)
            batch_agp = data/(output.name+'.agp')
            assert not batch_agp.exists() and not (data/'tiny-primary.mcool').exists()
            batch_agp.write_text(snapshot['agp'])
            batch = json.loads(app.http('/convert/jobs/batch', {'files':[source.name],
                'assemblyFilename':batch_agp.name,'approximateRebin':True,'exportAllResolutions':True}))
            batch_id = batch['jobIds'][0]
            deadline = time.monotonic()+60
            while time.monotonic()<deadline:
                batch_result = json.loads(app.http('/convert/jobs/'+batch_id))
                if batch_result['status'] in ('finished','failed','cancelled'):
                    break
                page.wait_for_timeout(100)
            assert batch_result['status']=='finished', batch_result
            batch_artifact = output/'batch.mcool'
            shutil.copyfile(data/'tiny-primary.mcool',batch_artifact)
            subprocess.run([args.cooler_python,str(Path(__file__).with_name('check_approximate_cooler.py')),
                '--artifact',str(batch_artifact),'--oracle',str(oracle),'--output',str(output/'batch-cooler-parity.json')],check=True)
            assert hashlib.sha256(source.read_bytes()).hexdigest()==digest
            (output/'results.json').write_text(json.dumps({'status':'ok','jobId':job_id,'sourceSha256':digest,
                'checks':['explicit opt-in','balanced output disabled','staged job flag propagation','Jobs delivery after dialog dismissal',
                          'actual result download','independent Cooler/base-endpoint parity','native file unchanged','batch opt-in and default unbalanced output'],
                'pageErrors':check.errors},indent=2))
            browser.close()
    finally:
        static.shutdown(); static.server_close()


if __name__=='__main__':
    main()
