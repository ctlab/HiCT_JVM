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
from sequence_browser_harness import HictTestServer, BrowserCheck, configure_test_api


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
            configure_test_api(page, app.url)
            check = BrowserCheck(page,output)
            page.goto(f'http://127.0.0.1:{static.server_port}/')
            check.menu('File','Join running session'); check.settle()
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
            page.reload(); check.menu('File','Join running session'); check.settle()
            snapshot = json.loads(app.http('/session/snapshot', {'verifyAssemblyRoundTrip':True}))['primary']
            oracle = output/'oracle.json'
            oracle.write_text(json.dumps({'contacts':original,'agp':snapshot['agp']}))
            check.menu('File','Export matrix…')
            dialog = page.get_by_role('dialog').filter(has=page.get_by_text('Export matrix to Cooler',exact=True))
            dialog.wait_for()
            check.advanced()
            checkbox = dialog.locator('#export-approximate-rebin')
            assert not checkbox.is_checked()
            dialog.get_by_role('button',name='Export',exact=True).click()
            dialog.get_by_text('Enable approximate rebinning under Advanced to export the edited assembly, or choose Original order in the file.',exact=True).wait_for()
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
                dialog.get_by_role('button',name='Export',exact=True).click()
            assert submitted.value.ok, submitted.value.text()
            job_id = submitted.value.json()['jobId']
            request = submitted.value.request.post_data_json
            assert request['approximateRebin'] is True and request['balanceExportedCoolers'] is False
            dialog.get_by_role('button',name='Cancel',exact=True).click()
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
            # Reopen the real exported file, then save and restore its provenance.
            page.get_by_role('dialog',name='Background jobs').get_by_role('button',name='Close',exact=True).click()
            with page.expect_download() as saved_before_import:
                check.menu('File','Save session…')
            saved_before_import.value.save_as(output/'before-reimport-session.json')
            imported_name = 'reopened-'+output.name+'.hict.hdf5'
            imported_job = json.loads(app.http('/convert/jobs',{'filename':name,'outputFilename':imported_name,
                'direction':'mcool-to-hict','exportAllResolutions':True,'buildResolutionPyramid':False,
                'balanceInputCoolers':False}))['jobId']
            page.locator('.jobs-button').click()
            reopened_row = page.locator('.job-row').filter(has=page.get_by_role('button',name='Open result',exact=True))
            deadline = time.monotonic()+60
            while time.monotonic()<deadline:
                imported_result = json.loads(app.http('/convert/jobs/'+imported_job))
                if imported_result['status'] in ('finished','failed','cancelled'):
                    break
                page.wait_for_timeout(100)
            assert imported_result['status']=='finished',imported_result
            reopened_row.get_by_role('button',name='Open result',exact=True).click()
            estimated_chip = page.locator('.file-chip--static').filter(has_text='Estimated:').first
            estimated_chip.wait_for()
            assert imported_name in estimated_chip.get_attribute('title')
            assert 'uniform-independent-endpoints-v1' in estimated_chip.get_attribute('title')
            check.settle()
            opened = json.loads(app.http('/attach',{}))['openFileResponse']
            caps = opened['capabilities']
            assert caps['approximation']['method']=='uniform-independent-endpoints-v1'
            assert caps['approximation']['upstreamApproximate'] is False
            reopened_values = {}
            for resolution in original:
                count = sum(d['contigLengthBins'][str(resolution)] for d in opened['assemblyInfo']['contigDescriptors'])
                reopened_values[resolution] = json.loads(app.http('/matrix/query',{'bpResolution':resolution,
                    'unit':'BINS','rows':count,'cols':count,'signalMode':'RAW_COUNTS','format':'JSON'}))
            reopened_json = output/'reopened-values.json'; reopened_json.write_text(json.dumps(reopened_values))
            subprocess.run([args.cooler_python,str(Path(__file__).with_name('check_approximate_cooler.py')),
                '--artifact',str(artifact),'--oracle',str(oracle),'--reopened-json',str(reopened_json),
                '--output',str(output/'reimport-parity.json')],check=True)
            secondary_name = 'secondary-'+imported_name
            assert not (data/secondary_name).exists()
            shutil.copyfile(data/imported_name,data/secondary_name)
            app.http('/secondary/open',{'filename':secondary_name})
            page.reload(); check.menu('File','Join running session'); check.settle()
            secondary_chip = page.locator('.file-chip--static').filter(has_text='Secondary (estimated):')
            secondary_chip.wait_for()
            assert 'uniform-independent-endpoints-v1' in secondary_chip.get_attribute('title')
            with page.expect_download() as downloaded_session:
                check.menu('File','Save session…')
            imported_session = output/'reopened-session.json'; downloaded_session.value.save_as(imported_session)
            saved_caps = json.loads(imported_session.read_text())['snapshot']['primary']['capabilities']
            assert saved_caps['approximation']==caps['approximation']
            assert json.loads(imported_session.read_text())['snapshot']['secondary']['capabilities']['approximation']==caps['approximation']
            check.menu('File','Close map')
            with page.expect_file_chooser() as chooser:
                check.menu('File','Open session…')
            chooser.value.set_files(imported_session)
            page.get_by_text('Session restored. Undo history starts from the restored assembly.',exact=True).last.wait_for()
            estimated_chip.wait_for(); check.settle()
            secondary_chip.wait_for()
            assert json.loads(app.http('/attach',{}))['openFileResponse']['capabilities']['approximation']==caps['approximation']
            page.screenshot(path=str(output/'reopened-estimated-session.png'))
            # Ordinary export preserves these stored estimates without applying another model.
            check.menu('File','Export matrix…')
            dialog = page.get_by_role('dialog').filter(has=page.get_by_text('Export matrix to Cooler',exact=True))
            assert not dialog.locator('#export-approximate-rebin').is_checked()
            dialog.locator('#export-balance-coolers').uncheck()
            dialog.locator('#export-all-resolutions').check()
            ordinary_name = 'stored-'+output.name+'.mcool'
            dialog.locator('input[placeholder="matrix.mcool"]').fill(ordinary_name)
            with page.expect_response('**/convert/jobs') as submitted:
                dialog.get_by_role('button',name='Export',exact=True).click()
            assert submitted.value.ok, submitted.value.text()
            ordinary_id = submitted.value.json()['jobId']
            assert submitted.value.request.post_data_json.get('approximateRebin',False) is False
            dialog.get_by_role('button',name='Cancel',exact=True).click()
            page.locator('.jobs-button').click()
            deadline = time.monotonic()+60
            while time.monotonic()<deadline:
                ordinary_result = json.loads(app.http('/convert/jobs/'+ordinary_id))
                if ordinary_result['status'] in ('finished','failed','cancelled'):
                    break
                page.wait_for_timeout(100)
            assert ordinary_result['status']=='finished',ordinary_result
            ordinary_row = page.locator('.job-row').filter(has=page.get_by_text(ordinary_name,exact=True))
            with page.expect_download() as download:
                ordinary_row.get_by_role('link',name='Download result',exact=True).click()
            ordinary_artifact = output/'stored.mcool'; download.value.save_as(ordinary_artifact)
            page.screenshot(path=str(output/'stored-export-job.png'))
            subprocess.run([args.cooler_python,str(Path(__file__).with_name('check_stored_float_cooler.py')),
                '--original',str(artifact),'--exported',str(ordinary_artifact),
                '--session',str(imported_session),
                '--output',str(output/'stored-parity.json')],check=True)
            assert not check.errors,check.errors
            assert hashlib.sha256(source.read_bytes()).hexdigest()==digest
            (output/'results.json').write_text(json.dumps({'status':'ok','jobId':job_id,'sourceSha256':digest,
                'checks':['explicit opt-in','balanced output disabled','staged job flag propagation','Jobs delivery after dialog dismissal',
                          'actual result download','independent Cooler/base-endpoint parity','native file unchanged','batch opt-in and default unbalanced output',
                          'Jobs open imported result','primary and secondary estimated labels','fractional reimport parity','session provenance save/restore',
                          'ordinary export without another approximation','exact stored float Cooler parity'],
                'pageErrors':check.errors},indent=2))
            browser.close()
    finally:
        static.shutdown(); static.server_close()


if __name__=='__main__':
    main()
