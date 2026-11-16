#!/usr/bin/env python3
"""Independently check an approximate 17 bp fixture export using Python Cooler."""
import argparse
import json
from pathlib import Path
import cooler
import numpy as np


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ('artifact', 'oracle', 'output'):
        parser.add_argument('--' + key, required=True)
    args = parser.parse_args()
    oracle = json.loads(Path(args.oracle).read_text())
    results = []
    for resolution, source in oracle['contacts'].items():
        resolution = int(resolution)
        matrix = cooler.Cooler(f'{args.artifact}::/resolutions/{resolution}')
        bins = matrix.bins()[:]
        offsets = {}
        count = 0
        for name, length in matrix.chromsizes.items():
            offsets[name] = count
            count += (int(length) + resolution - 1) // resolution
        assert count == matrix.shape[0]
        targets = [-1] * 17
        for line in oracle['agp'].strip().splitlines():
            row = line.split('\t')
            if row[4] in ('N', 'U'):
                continue
            assert row[5] == 'tiny' and row[8] in ('+', '-')
            start, end, target = int(row[6])-1, int(row[7]), int(row[1])-1
            for bp in range(start, end):
                local = bp-start if row[8] == '+' else end-bp-1
                assert targets[bp] == -1
                targets[bp] = offsets[row[0]] + (target+local)//resolution
        expected = np.zeros(matrix.shape, dtype=float)
        native = np.array(source['values']).reshape(source['rows'], source['cols'])
        for r in range(source['rows']):
            for c in range(r, source['cols']):
                a, b = r*resolution, min(17, (r+1)*resolution)
                x, y = c*resolution, min(17, (c+1)*resolution)
                value = native[r,c]/((b-a)*(y-x))
                for i in range(a,b):
                    for j in range(x,y):
                        ti, tj = targets[i], targets[j]
                        if ti >= 0 and tj >= 0:
                            expected[min(ti,tj),max(ti,tj)] += value
        pixels = matrix.pixels()[:]
        assert pixels['count'].dtype == np.dtype('float64')
        assert not pixels.duplicated(['bin1_id', 'bin2_id']).any()
        actual = np.zeros(matrix.shape, dtype=float)
        for pixel in pixels.itertuples(index=False):
            assert pixel.bin1_id <= pixel.bin2_id
            actual[pixel.bin1_id,pixel.bin2_id] = pixel.count
        np.testing.assert_allclose(actual, expected, rtol=1e-12, atol=1e-12)
        np.testing.assert_allclose(matrix.matrix(balance=False)[:], expected+expected.T-np.diag(np.diag(expected)), rtol=1e-12, atol=1e-12)
        metadata = matrix.info['metadata']
        if isinstance(metadata,str):
            metadata = json.loads(metadata)
        assert metadata['hict_approximate_export'] is True
        assert metadata['method'] == 'uniform-independent-endpoints-v1'
        assert metadata['resolution_policy'] == 'independent_stored_levels'
        assert metadata['native_contacts_modified'] is False
        assert 'weight' not in bins.columns
        assert bins['hict_sequence_fraction'].between(0,1).all()
        assert abs(pixels['count'].sum()-22) < 1e-12
        assert np.any(np.abs(pixels['count']-np.rint(pixels['count'])) > 1e-9)
        results.append({'resolution': resolution, 'bins': len(bins), 'pixels': len(pixels),
                        'contactSum': float(pixels['count'].sum()), 'maxCellError': float(np.max(np.abs(actual-expected)))})
    Path(args.output).write_text(json.dumps({'status': 'ok', 'levels': results}, indent=2))


if __name__ == '__main__':
    main()
