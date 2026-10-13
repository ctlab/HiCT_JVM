#!/usr/bin/env python3
"""Execute bundled native code and verify known answers (not only --help)."""
import argparse
import json
from pathlib import Path
import subprocess
import tempfile
import h5py
import numpy as np


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--seqkit', type=Path, required=True)
    parser.add_argument('--adapter', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    work = Path(tempfile.mkdtemp(prefix='hict-sequence-smoke-'))
    fasta = work / 'search.fa'
    fasta.write_text('>fixture\nACGTAAGGACGT\n')
    result = subprocess.run([str(args.seqkit.resolve()), 'locate', '--bed', '--only-positive-strand',
                             '--pattern', 'ACGT', str(fasta)], check=True, capture_output=True, text=True)
    intervals = sorted((int(line.split('\t')[1]), int(line.split('\t')[2]))
                       for line in result.stdout.splitlines() if line)
    assert intervals == [(0, 4), (8, 12)], intervals
    template = work / 'template.hdf5'
    with h5py.File(template, 'w') as f:
        info = f.create_group('contig_info')
        info['contig_name'] = np.array(['A'], dtype=h5py.string_dtype())
        info['contig_length_bp'] = [200]
        r = f.create_group('resolutions/100')
        r.create_group('contigs')['contig_length_bins'] = [2]
        r.create_group('atl')['basis_atu'] = [[0, 0, 2, 0]]
        r.create_group('stripes')['stripe_length_bins'] = [2]
        r['stripes']['stripes_bin_weights'] = [[1., 1.]]
        r.create_group('treap_coo').attrs.update(dense_submatrix_size=2, stripes_count=1, bins_count=2)
    sequence = 'A' * 100 + 'C' * 100
    prepared = work / 'prepared.fa'
    # Match FASTAProcessor's LF-only, 80-column prepared export on every OS.
    # write_text's platform translation would create CRLF offsets on Windows.
    prepared.write_bytes(('>analysis\n' + '\n'.join(sequence[i:i+80] for i in range(0, 200, 80)) + '\n').encode('ascii'))
    axis = [[0, 0, 0, 100], [0, 1, 100, 200]]
    plan = dict(template=str(template), output=str(work / 'overlay.hdf5'), input=str(prepared),
                inputOffset=0, inputLength=200, sequenceLength=200, snapshot='fixture',
                sourceFragments=[dict(source='A', start=0, end=200, reversed=False)],
                sourceFastaIdentity='fixture', inputSha256='fixture',
                settings=dict(k=7, identity=80, sketch_size=100, delta=0, ambiguous=False),
                levels=[dict(resolution=100, rows=axis, columns=axis)])
    (work / 'plan.json').write_text(json.dumps(plan))
    subprocess.run([str(args.adapter.resolve()), '--plan', str(work / 'plan.json')], check=True)
    with h5py.File(plan['output']) as f:
        assert f['sequence_analysis'].attrs['complete']
        coo = f['resolutions/100/treap_coo']
        n, start = int(coo['block_length'][0]), int(coo['block_offset'][0])
        cells = dict(zip(zip(coo['block_rows'][start:start+n], coo['block_cols'][start:start+n]),
                         coo['block_vals'][start:start+n]))
        assert cells == {(0, 0): 1_000_000, (1, 1): 1_000_000}, cells
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(dict(passed=True, searchIntervals=intervals,
        identityCells=3, work=str(work), adapter=json.loads(subprocess.check_output(
            [str(args.adapter.resolve()), '--version'], text=True))), indent=2))


if __name__ == '__main__':
    main()
