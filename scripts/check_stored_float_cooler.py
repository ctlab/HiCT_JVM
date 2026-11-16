#!/usr/bin/env python3
"""Compare stored float Cooler exports with an independent Cooler reader."""
import argparse
import json
from pathlib import Path

import cooler
import h5py
import numpy as np
import pandas as pd


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ('original', 'exported', 'output'):
        parser.add_argument('--'+key, required=True)
    parser.add_argument('--session', help='Expected current-assembly chromosome names from a saved session')
    args = parser.parse_args()
    groups = cooler.fileops.list_coolers(args.original)
    assert groups == cooler.fileops.list_coolers(args.exported)
    checks = []
    with h5py.File(args.original) as original, h5py.File(args.exported) as exported:
        for group in groups:
            before = cooler.Cooler(f'{args.original}::{group}')
            after = cooler.Cooler(f'{args.exported}::{group}')
            np.testing.assert_array_equal(before.chromsizes.values, after.chromsizes.values)
            if args.session:
                agp = json.loads(Path(args.session).read_text())['snapshot']['primary']['agp']
                names = list(dict.fromkeys(line.split('\t')[0] for line in agp.splitlines() if line and not line.startswith('#')))
                assert list(after.chromsizes.index) == names
            else:
                pd.testing.assert_series_equal(before.chromsizes, after.chromsizes)
            # AGP may rename singleton scaffolds; bin indices and coordinates must be unchanged.
            pd.testing.assert_frame_equal(before.bins()[:][['start','end']], after.bins()[:][['start','end']])
            np.testing.assert_array_equal(before.bins()[:]['chrom'].cat.codes, after.bins()[:]['chrom'].cat.codes)
            pd.testing.assert_frame_equal(before.pixels()[:], after.pixels()[:])
            np.testing.assert_array_equal(before.matrix(balance=False)[:], after.matrix(balance=False)[:])
            assert after.pixels()[:]['count'].dtype == np.dtype('float64')
            assert 'weight' not in after.bins()[:].columns
            assert after.info['sum'] == before.info['sum']
            metadata = after.info['metadata']
            if isinstance(metadata,str):
                metadata = json.loads(metadata)
            assert metadata['export_method'] == 'source_bin_remapping'
            assert bool(exported.attrs['hict-approximate-export'])
            for attribute in ('method','resolution_policy','upstream_approximate'):
                assert original['hict/approximate_export'].attrs[attribute] == exported['hict/approximate_export'].attrs[attribute]
            checks.append({'group':group,'pixels':int(after.info['nnz']),'sum':float(after.info['sum']),'maxCellError':0})
    Path(args.output).write_text(json.dumps({'status':'ok','levels':checks},indent=2))


if __name__ == '__main__':
    main()
