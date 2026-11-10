#!/usr/bin/env python3
"""Verify the session capture API through a real attached client and stale-client conflict."""
import argparse
import hashlib
import json
from pathlib import Path
from playwright.sync_api import sync_playwright


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('url', 'api', 'output'):
        parser.add_argument('--' + name, required=True)
    parser.add_argument('--data-dir', help='Disposable fixture directory for independent content hash checks')
    args = parser.parse_args()
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    result = {'checks': [], 'pageErrors': []}
    graph = json.loads((Path(__file__).resolve().parent.parent / 'src/test/resources/reinforcement/custom-pipeline.json').read_text())
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(viewport={'width': 1440, 'height': 1000})
        context.add_init_script('window.__HICT_BENCHMARK_ENABLED__=true')
        context.route('http://localhost:5000/**', lambda route: route.continue_(url=args.api + route.request.url.removeprefix('http://localhost:5000')))
        page = context.new_page()
        page.set_default_timeout(15000)
        page.on('pageerror', lambda error: result['pageErrors'].append(str(error)))
        def post(path, data=None):
            response = context.request.post(args.api + path, data=data or {})
            assert response.ok, response.text()
            return response.json()
        def attach():
            page.reload()
            page.get_by_role('link', name='File', exact=True).click()
            page.get_by_role('link', name='Attach', exact=True).click()
            page.get_by_text('Attached to session tiny-primary.hict.hdf5', exact=True).wait_for(state='visible')
        def capture():
            return page.evaluate('()=>window.__hictBenchmark.manager().networkManager.requestManager.getSessionSnapshot(true,true,true)')
        try:
            post('/close')
            empty = context.request.post(args.api + '/session/snapshot', data={})
            assert empty.status == 404, empty.text()
            page.goto(args.url)
            page.wait_for_function('window.__hictBenchmark')
            page.evaluate("api=>window.__hictBenchmark.open('tiny-primary.hict.hdf5',api)", args.api)
            post('/secondary/open', {'filename': 'old-secondary.hict.hdf5'})
            post('/link_fasta', {'fastaFilename': 'tiny.fa'})
            bed = post('/tracks/open', {'filename': 'session-track.bed', 'name': 'Session track'})
            graph['lowerExpression']['left'] = {'type': 'binary', 'op': 'ADD', 'left': graph['lowerExpression']['left'],
                                               'right': {'type': 'track1d', 'trackId': bed['trackId'], 'axis': 'ROW'}}
            post('/tracks/open_cooler_weights', {'source': 'PRIMARY'})
            post('/reverse_selection_range', {'startBP': 0, 'endBP': 17})
            attach()
            page.evaluate('graph=>window.__hictBenchmark.manager().networkManager.requestManager.setRenderPipelineConfig(graph)', graph)
            with page.expect_response(lambda response: response.url.endswith('/workspace/state') and response.status == 200):
                page.evaluate('''()=>{const view=window.__hictBenchmark.manager().getView();
                    view.setResolution(view.getResolution()/2);}''')
            history = post('/assembly/history')
            first = capture()
            assert first['assemblyVerified'] is True
            assert first['filesFingerprinted'] is True
            assert first['tracksVerified'] is True
            assert first['renderPipeline'] == graph
            assert first['primary']['filename'] == 'tiny-primary.hict.hdf5'
            assert first['primary']['fastaFilename'] == 'tiny.fa'
            assert first['secondary']['filename'] == 'old-secondary.hict.hdf5'
            assert first['tracks'][0]['name'] == 'Session track'
            assert first['tracks'][0]['sourceFile'] == 'session-track.bed'
            assert first['tracks'][1]['sourceReference'] == 'PRIMARY'
            if args.data_dir:
                resources = [
                    ('tiny-primary.hict.hdf5', first['primary']['fileFingerprint']),
                    ('old-secondary.hict.hdf5', first['secondary']['fileFingerprint']),
                    ('tiny.fa', first['primary']['fastaFingerprint']),
                    ('session-track.bed', first['tracks'][0]['fileFingerprint']),
                ]
                for filename, fingerprint in resources:
                    content = (Path(args.data_dir) / filename).read_bytes()
                    assert fingerprint == {'algorithm': 'SHA-256', 'sizeBytes': len(content), 'sha256': hashlib.sha256(content).hexdigest()}
                result['checks'].append('matrix, FASTA and BED fingerprints match independent content hashes; virtual weights reference primary')
                track_path = Path(args.data_dir) / 'session-track.bed'
                backup = track_path.with_suffix('.snapshot-test-backup')
                assert not backup.exists()
                track_path.rename(backup)
                try:
                    failed = page.evaluate('''async()=>{try{await window.__hictBenchmark.manager().networkManager.requestManager.getSessionSnapshot(true,true,true);return null;}catch(error){return error.response?.status;}}''')
                    assert failed is not None and failed >= 400, failed
                    assert post('/assembly/history') == history
                finally:
                    backup.rename(track_path)
                recovered = capture()
                assert recovered['primary'] == first['primary']
                assert recovered['secondary'] == first['secondary']
                assert recovered['tracks'] == first['tracks']
                result['checks'].append('missing resource rejects capture without changing the live assembly and recovers after restoration')
            assert first['workspace']['workspaceState']['version'] == 1
            assert 'axisScopes' in first['workspace']['workspaceState']
            assert first['primary']['assemblyInfo']['contigDescriptors'][0]['contigDirection'] == 0
            for key in ('primary', 'secondary'):
                source = first[key]
                assert hashlib.sha256(source['agp'].encode()).hexdigest() == source['agpSha256']
            assert post('/assembly/history') == history
            result['checks'].append('capture retains edited strand, both sources, FASTA and exact custom graph without adding history')
            result['checks'].append('capture includes track definitions and the persisted browser workspace')
            missing_reference = json.loads(json.dumps(graph))
            missing_reference['lowerExpression']['left']['right']['trackId'] = 'trk_missing'
            try:
                page.evaluate('graph=>window.__hictBenchmark.manager().networkManager.requestManager.setRenderPipelineConfig(graph)', missing_reference)
                rejected = page.evaluate('''async()=>{try{await window.__hictBenchmark.manager().networkManager.requestManager.getSessionSnapshot(false,false,true);return null;}catch(error){return error.response?.status;}}''')
                assert rejected is not None and rejected >= 400, rejected
                assert post('/assembly/history') == history
            finally:
                page.evaluate('graph=>window.__hictBenchmark.manager().networkManager.requestManager.setRenderPipelineConfig(graph)', graph)
            result['checks'].append('track verification rejects missing graph references without changing assembly history')
            post('/reverse_selection_range', {'startBP': 0, 'endBP': 17})
            conflict = page.evaluate('''async()=>{try{await window.__hictBenchmark.manager().networkManager.requestManager.getSessionSnapshot(true,true,true);return null;}catch(error){return error.response?.status;}}''')
            assert conflict == 409, conflict
            result['checks'].append('stale displayed assembly cannot be captured')
            attach()
            second = capture()
            assert second['primary']['assemblyInfo']['contigDescriptors'][0]['contigDirection'] == 1
            assert second['primary']['agpSha256'] != first['primary']['agpSha256']
            result['checks'].append('fresh Attach captures the new assembly hash and strand')
            unverified = dict(second)
            unverified.pop('assemblyVerified')
            def legacy_snapshot(route):
                route.fulfill(status=200, content_type='application/json', body=json.dumps(unverified))
            page.route('**/session/snapshot', legacy_snapshot)
            try:
                message = page.evaluate('''async()=>{try{await window.__hictBenchmark.manager().networkManager.requestManager.getSessionSnapshot(true,true,true);return null;}catch(error){return error.message;}}''')
                assert message == 'Server did not verify assembly reconstruction', message
                unverified.clear()
                unverified.update(second)
                unverified.pop('filesFingerprinted')
                message = page.evaluate('''async()=>{try{await window.__hictBenchmark.manager().networkManager.requestManager.getSessionSnapshot(true,true,true);return null;}catch(error){return error.message;}}''')
                assert message == 'Server did not fingerprint session resources', message
                unverified.clear()
                unverified.update(second)
                unverified.pop('tracksVerified')
                message = page.evaluate('''async()=>{try{await window.__hictBenchmark.manager().networkManager.requestManager.getSessionSnapshot(true,true,true);return null;}catch(error){return error.message;}}''')
                assert message == 'Server did not verify session track definitions', message
            finally:
                page.unroute('**/session/snapshot', legacy_snapshot)
            result['checks'].append('explicit verification rejects an unverified older-server response')
            result['first'], result['second'] = first, second
            page.screenshot(path=str(output / 'snapshot.png'))
            assert not result['pageErrors'], result['pageErrors']
        finally:
            (output / 'results.json').write_text(json.dumps(result, indent=2) + '\n')
            browser.close()


if __name__ == '__main__':
    main()
