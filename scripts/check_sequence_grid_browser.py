#!/usr/bin/env python3
"""Decode real sequence-scale PNG tiles in Chromium and compare every native reference color."""
import argparse
import base64
import functools
import hashlib
import http.server
import json
from pathlib import Path
import shutil
import threading
import time
from urllib.parse import urlencode

from playwright.sync_api import sync_playwright
from sequence_browser_harness import HictTestServer, BrowserCheck, configure_test_api


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ('jar','webui','data-dir','output'):
        parser.add_argument('--'+key,required=True)
    args = parser.parse_args()
    data, output = Path(args.data_dir).absolute(), Path(args.output).absolute()
    if data != data.resolve() or not data.is_relative_to('/tmp') or not output.is_relative_to('/tmp'):
        parser.error('Use isolated nonsymlinked disposable paths under /tmp')
    source = data/'tiny-primary.hict.hdf5'
    if source.is_symlink() or source.stat().st_nlink != 1:
        parser.error('Use a physical 17 bp reinforcement fixture copy')
    secondary = data/'tiny-secondary.hict.hdf5'
    if secondary.exists(): parser.error('Use a fresh data directory')
    shutil.copyfile(source,secondary)
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    output.mkdir(parents=True,exist_ok=False)

    class Quiet(http.server.SimpleHTTPRequestHandler):
        def log_message(self,*_): pass

    static = http.server.ThreadingHTTPServer(('127.0.0.1',0),
        functools.partial(Quiet,directory=str(Path(args.webui).resolve())))
    threading.Thread(target=static.serve_forever,daemon=True).start()
    try:
        with HictTestServer(args.jar,data,output/'server.log') as app, sync_playwright() as pw:
            app.http('/open',{'filename':source.name})
            app.http('/secondary/open',{'filename':secondary.name})
            browser = pw.chromium.launch(headless=True)
            page = browser.new_page(viewport={'width':1440,'height':1000})
            page.set_default_timeout(30000)
            page.add_init_script('window.__HICT_BENCHMARK_ENABLED__=true')
            configure_test_api(page, app.url)
            check = BrowserCheck(page,output)
            page.goto(f'http://127.0.0.1:{static.server_port}/')
            check.menu('File','Join running session'); check.settle()

            def decode(url):
                return page.evaluate('''async url => {
                    const image=new Image();image.src=url;await image.decode();
                    const canvas=document.createElement('canvas');canvas.width=image.width;canvas.height=image.height;
                    const context=canvas.getContext('2d');context.drawImage(image,0,0);
                    return {width:canvas.width,height:canvas.height,pixels:Array.from(context.getImageData(0,0,canvas.width,canvas.height).data)};
                }''',url)

            def png(params):
                start = time.monotonic()
                response = page.request.get(app.url+'/get_tile?'+urlencode(params))
                assert response.ok,(response.status,response.text())
                duration = (time.monotonic()-start)*1000
                if params.get('format') == 'PNG_BY_PIXELS':
                    url = 'data:image/png;base64,'+base64.b64encode(response.body()).decode()
                    payload = None
                else:
                    payload = response.json(); url = payload['image']
                return decode(url),url,payload,response.headers,duration

            def pipeline(enabled,split=False,swap=False):
                def colors(source):
                    return {'type':'colormap','mode':'LINEAR','input':{'type':'source','source':source},
                            'minSignal':0,'maxSignal':11,'startColor':'#ffffffff','endColor':'#ff0000ff'}
                branch = {'type':'pixel_blend','mode':'ADD','top':colors('PRIMARY'),'bottom':colors('SECONDARY')}
                app.http('/render_pipeline/set',{'enabled':enabled,'upperExpression':colors('PRIMARY') if split else branch,
                    'lowerExpression':colors('SECONDARY') if split else branch,'swapUpperLower':swap})

            options = json.loads(app.http('/get_visualization_options',{}))
            options.update(preLogBase=-1,postLogBase=-1,applyCoolerWeights=False,
                           resolutionScaling=False,resolutionLinearScaling=False,signalDisplayMode='OBSERVED')
            app.http('/set_visualization_options',options)
            originals = {}
            for enabled in (False,True):
                pipeline(enabled)
                for source_name in ('PRIMARY','SECONDARY'):
                    for resolution,count in ((4,5),(10,2)):
                        originals[enabled,source_name,resolution] = png({'source':source_name,'bpResolution':resolution,
                            'format':'PNG_BY_PIXELS','row':0,'col':0,'rows':count,'cols':count})[0]
            for offset in (5,1):
                page.evaluate('''async offset => {
                    const m=window.__hictBenchmark.manager();
                    m.eventManager.applyAssemblyInfo(await m.networkManager.requestManager.splitContigAtBp(
                        m.contigDimensionHolder.contigDescriptors[0].contigId,offset));
                    await m.reloadVisuals();
                }''',offset)
                check.settle()
            app.http('/reverse_selection_range',{'startBP':0,'endBP':17})
            page.reload(); check.menu('File','Join running session'); check.settle()
            results = []
            preview_native = preview_sequence = None
            for enabled in (False,True):
                pipeline(enabled)
                for source_name in ('PRIMARY','SECONDARY'):
                    for resolution,ids in ((4,[4,3,2,1,1,0,0]),(10,[1,0,0,0])):
                        original = originals[enabled,source_name,resolution]
                        params = {'source':source_name,'bpResolution':resolution,'format':'PNG_BY_PIXELS',
                                  'row':-2,'col':-1,'rows':21,'cols':19,'sequenceScale':'true'}
                        actual,url,_,headers,ms = png(params)
                        assert headers['x-hict-display-grid']=='SEQUENCE_BP'
                        assert headers['x-hict-source-bp-resolution']==str(resolution)
                        for row in range(21):
                            for col in range(19):
                                r,c = row-2,col-1
                                expected = [0,0,0,0]
                                if 0<=r<17 and 0<=c<17:
                                    a,b = (16-r)//resolution,(16-c)//resolution
                                    index = 4*(a*original['width']+b)
                                    expected = original['pixels'][index:index+4]
                                index = 4*(row*19+col)
                                assert actual['pixels'][index:index+4]==expected,(enabled,source_name,resolution,row,col,actual['pixels'][index:index+4],expected)
                        # Native references beyond the original physical count must stay colored too.
                        native,native_url,_,_,_ = png({'source':source_name,'bpResolution':resolution,
                            'format':'PNG_BY_PIXELS','row':0,'col':0,'rows':len(ids),'cols':len(ids)})
                        for row,a in enumerate(ids):
                            for col,b in enumerate(ids):
                                at = 4*(row*len(ids)+col); before = 4*(a*original['width']+b)
                                assert native['pixels'][at:at+4]==original['pixels'][before:before+4]
                        grid,_,payload,_,_ = png({'source':source_name,'bpResolution':resolution,'tile_size':32,
                            'row':0,'col':0,'sequenceScale':'true'})
                        assert payload['displayGrid']=={'unit':'BASE_PAIRS','bpPerPixel':1,'sourceBpResolution':resolution,
                            'cellSemantics':'SOURCE_BIN_REFERENCE_COLORS','contactValuesInterpolated':False,
                            'startRowBp':0,'endRowBp':32,'startColBp':0,'endColBp':32}
                        assert not payload.get('signals')
                        assert grid['width']==grid['height']==32
                        results.append({'pipeline':enabled,'source':source_name,'bpResolution':resolution,
                                        'checkedPixels':21*19+len(ids)**2,'maxChannelError':0,'requestMs':ms})
                        if enabled and source_name=='PRIMARY' and resolution==4:
                            preview_native = native_url
                            preview_sequence = png(dict(params,row=0,col=0,rows=17,cols=17))[1]
            for swap in (False,True):
                pipeline(True,split=True,swap=swap)
                for source_name in ('PRIMARY','SECONDARY'):
                    original = originals[True,source_name,4]
                    actual,_,_,_,_ = png({'source':source_name,'bpResolution':4,'format':'PNG_BY_PIXELS',
                        'row':0,'col':0,'rows':17,'cols':17,'sequenceScale':'true'})
                    for row in range(17):
                        for col in range(17):
                            visible = (row<=col) != swap
                            if source_name=='SECONDARY': visible = not visible
                            expected = [0,0,0,0]
                            if visible:
                                a,b = (16-row)//4,(16-col)//4
                                before = 4*(a*original['width']+b)
                                expected = original['pixels'][before:before+4]
                            at = 4*(row*17+col)
                            assert actual['pixels'][at:at+4]==expected,(swap,source_name,row,col)
            for unit in ('PIXELS','BINS'):
                raw = json.loads(app.http('/matrix/query',{'bpResolution':4,'rows':7,'cols':7,'unit':unit,
                    'format':'JSON','signalMode':'PIPELINE_SIGNAL','expression':{'type':'constant','value':7}}))
                assert raw['values']==[7]*49,(unit,raw)
            for extras in ({'sequenceScale':'yes'},{'sequenceScale':'true','source':''},
                           {'sequenceScale':'true','gridBpResolution':4},{'sequenceScale':'true','gridSampleFactor':2}):
                params = {'source':'PRIMARY','bpResolution':4,'tile_size':32,**extras}
                response = page.request.get(app.url+'/get_tile?'+urlencode(params))
                assert response.status==400,(params,response.status,response.text())
            assert not check.errors,check.errors
            assert hashlib.sha256(source.read_bytes()).hexdigest()==digest
            assert hashlib.sha256(secondary.read_bytes()).hexdigest()==digest
            page.screenshot(path=str(output/'native-ui-still-pending.png'))
            # Diagnostic preview of the actual server images, not a product UI feature.
            preview = browser.new_page(viewport={'width':1080,'height':660})
            preview.set_content(f'''<html><style>body{{font:16px sans-serif;margin:30px;color:#17212b}}
                main{{display:flex;gap:40px}}img{{width:408px;height:408px;image-rendering:pixelated;border:1px solid #aaa}}
                h2{{font-size:18px}}p{{max-width:900px;line-height:1.5}}</style>
                <h1>Sequence-grid backend qualification</h1><p>Same native 4 bp contact values after exact cuts and reversal.
                The retained fragments are 12, 4 and 1 bases long. The right image uses uniform base widths.</p>
                <main><section><h2>Native bin references (7 × 7)</h2><img src="{preview_native}"></section>
                <section><h2>One-base display grid (17 × 17)</h2><img src="{preview_sequence}"></section></main>
                <p>Contact resolution remains 4 bp. Main map transition, rulers and FASTA integration are pending.</p></html>''')
            preview.locator('img').last.wait_for(); preview.screenshot(path=str(output/'sequence-grid-comparison.png'))
            (output/'results.json').write_text(json.dumps({'status':'ok','cases':results,
                'checks':['both sources','both stored levels','standard and pipeline renderers','exact source-color parity',
                          'clipped/reversed shared bins','transparent padding','all native reference colors',
                          'numeric pipeline bounds','base-grid triangles and swap','explicit grid metadata','invalid combinations rejected','native bytes unchanged'],
                'pageErrors':check.errors,'sourceSha256':digest,'uiIntegration':'pending'},indent=2))
            browser.close()
    finally:
        static.shutdown(); static.server_close()


if __name__=='__main__': main()
