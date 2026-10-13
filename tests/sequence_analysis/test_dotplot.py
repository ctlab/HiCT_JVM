import os
import random
import math
import sys
from pathlib import Path
import h5py
import numpy as np
import pytest
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'scripts/sequence_analysis'))
from dotplot_kernel import sample_paf, run
from overlay_store import OverlayWriter
from test_overlay import template


def test_sampled_forward_reverse_alignments_match_scalar_oracle_with_unowned_gaps(tmp_path):
    rows = np.array([[1, 3, 0, 13], [1, 2, 13, 20], [0, 1, 24, 33], [0, 0, 33, 40]])
    cols = np.array([[0, 2, 0, 8], [0, 3, 10, 20], [1, 0, 20, 31], [1, 1, 31, 40]])
    rng = random.Random(721)
    lines, expected = [], []
    for _ in range(200):
        qs, qe = sorted(rng.sample(range(41), 2))
        ts, te = sorted(rng.sample(range(41), 2))
        length, strand = max(qe-qs,te-ts), rng.choice(['+', '-'])
        lines.append(f'rows\t40\t{qs}\t{qe}\t{strand}\tcolumns\t40\t{ts}\t{te}\t1\t{length}\t60')
        if length < 3:
            continue
        steps = max(1,length//3)
        for i in range(steps+1):
            q = qs + math.floor((i/steps)*(qe-qs-1)+0.5)
            delta = math.floor((i/steps)*(te-ts-1)+0.5)
            t = ts+delta if strand=='+' else te-1-delta
            r = next((i for i,v in enumerate(rows) if v[2] <= q < v[3]), None)
            c = next((i for i,v in enumerate(cols) if v[2] <= t < v[3]), None)
            if r is not None and c is not None:
                expected.append((r,c))
    paf=tmp_path/'oracle.paf';paf.write_text('\n'.join(lines)+'\n')
    got=[]
    sample_paf(paf,rows,cols,0,0,3,3,lambda r,c:got.extend(zip(r,c)))
    assert got == expected


def test_large_physical_block_remains_sparse_and_capacity_growth_is_bounded(tmp_path):
    source=template(tmp_path/'template.hdf5')
    with h5py.File(source,'r+') as f:
        f['resolutions/2/treap_coo'].attrs['dense_submatrix_size']=8192
        f['resolutions/2/stripes/stripe_length_bins'][:]=8192
    output=tmp_path/'out.hdf5'
    with OverlayWriter(source,output,[2],dict(kind='alignment-dotplot'),unit='alignment_sample_count',scale=1) as out:
        # More than 100/1e4: counts must retain their own units and full magnitude.
        for column in range(200):
            out.patch(2,[[0,2]],[[1,column]],[[123456]])
        coo=out.handle['resolutions/2/treap_coo']
        assert coo['block_length'][1] == 200
        assert len(coo['block_vals']) < 4*200
        assert np.all(coo['block_vals'][int(coo['block_offset'][1]):int(coo['block_offset'][1])+200] == 123456)
    assert output.stat().st_size < 1_000_000


def test_native_alignment_dotplot_and_zero_replacement(tmp_path):
    aligner=os.environ.get('HICT_MINIMAP2_BIN')
    if not aligner:
        pytest.skip('Set HICT_MINIMAP2_BIN for native alignment test')
    # Metadata uses the small existing fixture; its physical bins are attached
    # explicitly to 100-bp windows of a non-repetitive current sequence.
    source=template(tmp_path/'template.hdf5')
    rng=random.Random(481)
    sequence=''.join(rng.choices('ACGT',k=800))
    fasta=tmp_path/'input.fa'
    fasta.write_text('>analysis\n'+'\n'.join(sequence[i:i+80] for i in range(0,800,80))+'\n')
    axis=[[i//4,i%4,i*100,(i+1)*100] for i in range(8)]
    first_dir=tmp_path/'first';first_dir.mkdir()
    plan=dict(settings=dict(k=7,window=1,chainScore=5,sampleBp=25,minAlignmentLength=100,threads=1),
        levels=[dict(resolution=2,rows=axis,columns=axis)],template=str(source),output=str(first_dir/'out.hdf5'),
        aligner=aligner,input=str(fasta),inputLength=800,inputOffset=0,sourceFastaIdentity='fixture',
        snapshot='original',sourceFragments=[],inputSha256='fixture')
    run(plan,log=lambda _:None)
    with h5py.File(plan['output']) as f:
        assert f['sequence_analysis'].attrs['score_unit']=='alignment_sample_count'
        coo=f['resolutions/2/treap_coo']
        for stripe in range(2):
            block=OverlayWriter._read_block(coo,stripe*2+stripe,4)
            assert np.all(np.diag(block)>0)
    # Incompatible parameters refuse accumulation before changing the old file.
    import hashlib
    before=hashlib.sha256(Path(plan['output']).read_bytes()).hexdigest()
    next_dir=tmp_path/'second';next_dir.mkdir()
    previous=plan['output'];plan.update(previous=previous,output=str(next_dir/'out.hdf5'))
    plan['settings']['k']=9
    with pytest.raises(ValueError,match='differ'):
        run(plan,log=lambda _:None)
    assert hashlib.sha256(Path(previous).read_bytes()).hexdigest()==before
