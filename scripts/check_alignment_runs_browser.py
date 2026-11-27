#!/usr/bin/env python3
"""Qualify short-repeat CIGARs, continuous exact alignment geometry and exports."""
import argparse
import functools
import hashlib
import http.server
import json
from pathlib import Path
import random
import subprocess
import threading
import time
from playwright.sync_api import sync_playwright
from sequence_browser_harness import HictTestServer, BrowserCheck, configure_test_api


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    for key in ('jar','webui','data-dir','output'):parser.add_argument('--'+key,required=True)
    args=parser.parse_args();data,out=Path(args.data_dir).absolute(),Path(args.output).absolute()
    if any(p!=p.resolve() or not p.is_relative_to('/tmp') for p in (data,out)):parser.error('Use fresh disposable nonsymlinked /tmp paths')
    data.mkdir(parents=True,exist_ok=False);out.mkdir(parents=True,exist_ok=False)
    with (out/'fixture.log').open('w') as log:
        subprocess.run(['java','--class-path',str(Path(args.jar).resolve()),str(Path(__file__).with_name('SequenceViewBrowserFixture.java')),str(data)],check=True,stdout=log,stderr=subprocess.STDOUT)
    rng=random.Random(904);bases=list(''.join(rng.choice('ACGT') for _ in range(4000)))
    row=''.join(bases[500:540]);longrow=''.join(bases[1500:2012])
    rc=lambda sequence:sequence.translate(str.maketrans('ACGT','TGCA'))[::-1]
    insertion=next(b for b in 'ACGT' if b not in row[19:21])
    for start,sequence in [(100,'CT'*6),(200,'CT'*5),(300,'CTCTATCTCT'),
        (600,row[:20]+insertion+row[20:]),(700,row[:20]+row[21:]),(800,rc(row)),
        (900,row[:20]+'N'+row[21:]),(1000,'N'*40),
        (2200,longrow[:257]+next(b for b in 'ACGT' if b not in longrow[256:258])+longrow[257:]),
        (2800,longrow[:100]+next(b for b in 'ACGT' if b!=longrow[100])+longrow[101:])]:bases[start:start+len(sequence)]=sequence
    original=''.join(bases);(data/'sequence.fa').write_text('>tiny\n'+original+'\n')
    native=data/'sequence.hict.hdf5';digest=hashlib.sha256(native.read_bytes()).hexdigest()
    class Quiet(http.server.SimpleHTTPRequestHandler):
        def log_message(self,*_):pass
    static=http.server.ThreadingHTTPServer(('127.0.0.1',0),functools.partial(Quiet,directory=str(Path(args.webui).resolve())))
    threading.Thread(target=static.serve_forever,daemon=True).start()
    try:
        with HictTestServer(args.jar,data,out/'server.log') as app,sync_playwright() as pw:
            app.http('/open',{'filename':native.name});app.http('/link_fasta',{'fastaFilename':'sequence.fa'})
            browser=pw.chromium.launch(headless=True)
            page=browser.new_page(viewport={'width':1600,'height':1100},accept_downloads=True)
            page.set_default_timeout(30000);page.add_init_script('window.__HICT_BENCHMARK_ENABLED__=true')
            configure_test_api(page, app.url)
            check=BrowserCheck(page,out);api_results=[]
            page.goto(f'http://127.0.0.1:{static.server_port}/');check.menu('File','Join running session');check.settle()
            def alignment(rs,re,cs,ce):
                response=json.loads(app.http('/sequence/alignment_detail',{'rowStart':rs,'rowEnd':re,'columnStart':cs,'columnEnd':ce}))
                deadline=time.monotonic()+30
                while response['status'] in ('submitted','running') and time.monotonic()<deadline:
                    time.sleep(.03);response=json.loads(app.http('/sequence/alignment_detail',{'jobId':response['jobId']}))
                assert response['status']=='finished',response
                result=response['result']
                assert result['rowSha256']==hashlib.sha256(original[rs:re].encode()).hexdigest(),result
                assert result['columnSha256']==hashlib.sha256(original[cs:ce].encode()).hexdigest(),result
                assert result['aligner'] and len(result['alignerSha256'])==64 and result['snapshot'],result
                api_results.append(result);return result
            def exact_repeats():
                result=alignment(100,112,200,210)
                assert result['profile']['name']=='short-window-v1' and result['profile']['kmer']==2,result
                # Independent enumeration of maximal exact diagonals, not minimap's own output.
                expected=set()
                r,c='CT'*6,'CT'*5
                for offset in range(-len(c)+1,len(r)):
                    i,j=max(0,offset),max(0,-offset);start=(i,j)
                    while i<len(r) and j<len(c) and r[i]==c[j]:i+=1;j+=1
                    if i-start[0]>=2:expected.add((100+start[0],100+i,200+start[1],200+j))
                actual={(s['rowStart'],s['rowEnd'],s['columnStart'],s['columnEnd']) for s in result['segments'] if not s['reverse'] and s['operation']=='MATCH'}
                assert actual==expected and len(expected)==10 and result['alignmentCount']==10,result
                assert len({s['alignment'] for s in result['segments']})==10
            check.step('all-ten-short-repeat-diagonals',exact_repeats)
            def substitution():
                result=alignment(100,112,300,310)
                assert any(s['operation']=='SUBSTITUTION' and s['columnStart']==304 and s['columnEnd']==305 for s in result['segments']),result
            check.step('short-repeat-substitution-cigar',substitution)
            def gaps_and_reverse():
                for start,end,operation in [(600,641,'INSERTION'),(700,739,'DELETION'),(900,940,'AMBIGUOUS')]:
                    result=alignment(500,540,start,end)
                    assert any(s['operation']==operation and not s['reverse'] for s in result['segments']),(operation,result)
                assert any(s['reverse'] and s['operation']=='MATCH' for s in alignment(500,540,800,840)['segments'])
                assert alignment(500,540,1000,1040)['alignmentCount']==0
            check.step('short-gaps-reverse-ambiguity-and-no-hit',gaps_and_reverse)
            def long_profile():
                result=alignment(1500,2012,2200,2713)
                assert result['profile']['name']=='seeded-window-v1' and result['profile']['kmer']==7,result
                assert any(s['operation']=='INSERTION' and not s['reverse'] for s in result['segments']),result
                assert any(s['operation']=='SUBSTITUTION' and not s['reverse'] for s in alignment(1500,2012,2800,3312)['segments'])
            check.step('larger-window-profile-retains-gapped-alignments',long_profile)
            page.evaluate('''async()=>{const m=window.__hictBenchmark.manager();for(const bp of [100,112,200,210,300,310,1000,1040]) {
                const t=m.eventManager.getExactCutTarget(bp);if(!t)throw Error('missing cut '+bp);
                m.eventManager.applyAssemblyInfo(await m.networkManager.requestManager.splitContigAtBp(t.contigId,t.offsetBp));
                }m.reloadVisuals();}''');check.settle()
            names=page.evaluate('''()=>{const m=window.__hictBenchmark.manager(),h=m.contigDimensionHolder;return Object.fromEntries(h.contigDescriptors.map((d,i)=>[h.prefix_sum_bp[i],d.contigName]));}''')
            def scope(axis,label):
                field=page.get_by_role('textbox',name='Rows scope' if axis=='row' else 'Columns scope',exact=True)
                field.fill(label);page.locator('.scope-result:visible').filter(has_text=label).first.click();check.settle()
            def focus(column,row,resolution=1/8000):
                page.evaluate('''v=>window.__hictBenchmark.manager().viewAndLayersManager.restoreViewPosition({centerBp:[v[0],v[1]],resolution:v[2]})''',[column,row,resolution]);check.settle()
            def rendered():
                return page.evaluate('''()=>{const m=window.__hictBenchmark.manager(),a=m.alignmentDetail;return {message:a.state.message.value,visible:a.state.visible.value,
                    features:a.source.getFeatures().map(f=>({type:f.getGeometry().getType(),coordinates:f.getGeometry().getCoordinates(),segment:f.get('alignmentSegment'),color:f.getStyle().at(-1).getStroke().getColor()}))};}''')
            scope('row',names['100']);scope('column',names['200']);focus(205,106)
            def regular_lines():
                minimum = page.get_by_role("spinbutton", name="Minimum alignment length (bp)")
                assert minimum.input_value() == "10"
                page.wait_for_function("""()=>window.__hictBenchmark.manager().alignmentDetail.source.getFeatures().length===2""")
                assert all(f["segment"]["rowEnd"] - f["segment"]["rowStart"] == 10 for f in rendered()["features"])
                minimum.fill("2")
                minimum.press("Tab")
                page.wait_for_function('''()=>{const a=window.__hictBenchmark.manager().alignmentDetail;return a.state.message.value===''&&a.source.getFeatures().length===10;}''')
                state=rendered();assert all(f['type']=='LineString' and f['color']=='#047857' for f in state['features']),state
                assert all(len(f['coordinates'])==f['segment']['rowEnd']-f['segment']['rowStart']+1 for f in state['features']),state
                # Every vertex uses a true base boundary in the active coordinate model.
                assert page.evaluate('''()=>{const m=window.__hictBenchmark.manager(),d=m.viewAndLayersManager.getActiveVectorResolutionDescriptor(),h=m.contigDimensionHolder;
                    return m.alignmentDetail.source.getFeatures().every(f=>{const s=f.get('alignmentSegment');return f.getGeometry().getCoordinates().every((p,i)=>
                        Math.abs(p[0]-h.getFractionalPxOfBp(s.columnStart+i,d.bpResolution)*d.pixelResolution)<1e-9&&Math.abs(p[1]+h.getFractionalPxOfBp(s.rowStart+i,d.bpResolution)*d.pixelResolution)<1e-9);});}''')
                (out/'regular-lines.json').write_text(json.dumps(state,indent=2))
            check.step('regular-lines-align-with-letter-boundaries',regular_lines)
            scope('column',names['300']);focus(305,106)
            def rendered_substitution():
                has_substitution = '''()=>window.__hictBenchmark.manager().alignmentDetail.source.getFeatures().some(f=>f.get('alignmentSegment').operation==='SUBSTITUTION')'''
                page.wait_for_function(has_substitution)
                state=rendered();assert any(f['color']=='#c2410c' for f in state['features']),state
                assert all(f['type']=='LineString' for f in state['features'])
                # Keep both aligner windows in view while zooming. Trimming this
                # repetitive fixture can legitimately turn a mismatch into an indel.
                page.set_viewport_size({'width':1600,'height':1750})
                focus(305,106,1/12000)
                page.wait_for_function(has_substitution)
                assert any(f['segment']['operation']=='SUBSTITUTION' for f in rendered()['features'])
            check.step('substitution-lines-survive-further-zoom',rendered_substitution)
            def exports():
                for format in ('Png','Svg','Pdf'):
                    with page.expect_download() as download:page.evaluate('''format=>window.__hictBenchmark.manager()['exportCurrentMap'+format](undefined,{scope:'viewport',cropToMap:true})''',format)
                    target=out/('alignment-lines.'+format.lower());download.value.save_as(target)
                    assert target.stat().st_size>1000
                svg=(out/'alignment-lines.svg').read_text().lower()
                assert '#c2410c' in svg or '194,65,12' in svg or '194, 65, 12' in svg
                assert '<path' in svg and 'image/svg+xml' not in svg
                assert any(f['segment']['operation']=='SUBSTITUTION' for f in rendered()['features'])
            check.step('vector-png-svg-pdf-exports-and-restoration',exports)
            def toggle():
                check.menu('View','Align visible FASTA automatically');check.settle()
                assert not rendered()['visible'] and not rendered()['features']
                check.menu('View','Align visible FASTA automatically');check.settle()
                page.wait_for_function('''()=>window.__hictBenchmark.manager().alignmentDetail.source.getFeatures().length>0''')
            check.step('disable-and-resume-automatic-alignment',toggle)
            def no_hit():
                scope('column',names['1000']);focus(1020,106)
                page.wait_for_function('''()=>window.__hictBenchmark.manager().alignmentDetail.state.message.value.startsWith('No alignment reported')''')
                assert not rendered()['features']
            check.step('empty-alignment-is-explicit',no_hit)
            def reverse_assembly():
                scope('row','All rows');scope('column','All columns')
                page.evaluate('window.__hictBenchmark.manager().viewAndLayersManager.selectEntityRangeByBp(0,4000)')
                with page.expect_response('**/reverse_selection_range') as response:page.get_by_role('button',name='Reverse Selection',exact=True).click()
                assert response.value.ok;check.settle()
                scope('row',names['100']);scope('column',names['200']);focus(3795,3894)
                page.wait_for_function('''()=>{const a=window.__hictBenchmark.manager().alignmentDetail;return a.state.message.value===''&&a.source.getFeatures().length===10;}''')
                assert all(f['segment']['rowStart']>=3888 and f['segment']['rowEnd']<=3900 and f['segment']['columnStart']>=3790 and f['segment']['columnEnd']<=3800 for f in rendered()['features'])
                assert json.loads(app.http('/sequence/region',{'startBp':0,'endBp':4000}))['sequence']==rc(original)
            check.step('exact-fragments-and-reversed-assembly',reverse_assembly)
            def undo_redo():
                page.locator('#hic-contact-map').click(position={'x':70,'y':70})
                page.keyboard.press('Control+z');check.settle();focus(205,106)
                page.wait_for_function('''()=>{const a=window.__hictBenchmark.manager().alignmentDetail;return a.state.message.value===''&&a.source.getFeatures().length===10&&a.source.getFeatures().every(f=>f.get('alignmentSegment').rowStart>=100&&f.get('alignmentSegment').rowEnd<=112);}''')
                page.keyboard.press('Control+y');check.settle();focus(3795,3894)
                page.wait_for_function('''()=>{const a=window.__hictBenchmark.manager().alignmentDetail;return a.state.message.value===''&&a.source.getFeatures().length===10&&a.source.getFeatures().every(f=>f.get('alignmentSegment').rowStart>=3888&&f.get('alignmentSegment').rowEnd<=3900);}''')
            check.step('undo-redo-refreshes-alignment-geometry',undo_redo)
            def session():
                with page.expect_download() as download:check.menu('File','Save session…')
                path=out/'alignment-session.json';download.value.save_as(path)
                check.menu('File','Close map')
                with page.expect_file_chooser() as chooser:check.menu('File','Open session…')
                chooser.value.set_files(path)
                page.get_by_text('Session restored. Undo history starts from the restored assembly.',exact=True).last.wait_for();check.settle()
                page.wait_for_function('''()=>{const a=window.__hictBenchmark.manager().alignmentDetail;return a.state.message.value===''&&a.source.getFeatures().length===10;}''')
                assert all(f['type']=='LineString' for f in rendered()['features'])
                assert json.loads(app.http('/sequence/region',{'startBp':0,'endBp':4000}))['sequence']==rc(original)
            check.step('session-restores-scoped-lines-and-sequence',session)
            assert hashlib.sha256(native.read_bytes()).hexdigest()==digest
            assert not check.errors,check.errors
            (out/'results.json').write_text(json.dumps({'status':'ok','checks':check.rows,'apiResults':api_results,'rendered':rendered(),'sourceSha256':digest,'pageErrors':check.errors},indent=2))
            browser.close()
    finally:static.shutdown();static.server_close()


if __name__=='__main__':main()
