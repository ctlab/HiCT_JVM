import sys
from pathlib import Path
import hashlib
import h5py
import numpy as np
import pytest
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'scripts/sequence_analysis'))
from overlay_store import OverlayWriter, SCALE


def template(path, stripes=2):
    with h5py.File(path, 'w') as f:
        contigs = f.create_group('contig_info')
        contigs['contig_name'] = np.array(['A', 'B'], dtype=h5py.string_dtype())
        contigs['contig_length_bp'] = [8, 8]
        r = f.create_group('resolutions/2')
        r.create_group('contigs')['contig_length_bins'] = [4, 4]
        r.create_group('atl')['basis_atu'] = [[0, 0, 4, 0], [1, 0, 4, 0]]
        r.create_group('stripes')['stripe_length_bins'] = [4] * stripes
        r['stripes']['stripes_bin_weights'] = np.full((stripes, 4), np.nan)
        coo = r.create_group('treap_coo')
        coo.attrs.update(dense_submatrix_size=4, stripes_count=stripes, bins_count=stripes * 4, hdf5_max_chunk_size=8192)
    return path


def read_matrix(path, stripes=2):
    matrix = np.zeros((stripes * 4, stripes * 4), dtype=np.int64)
    with h5py.File(path, 'r') as f:
        root = f['resolutions/2/treap_coo']
        for a in range(stripes):
            for b in range(a, stripes):
                block = OverlayWriter._read_block(root, a * stripes + b, 4)
                for r, c in zip(*np.nonzero(block)):
                    matrix[a * 4 + r, b * 4 + c] = block[r, c]
                    matrix[b * 4 + c, a * 4 + r] = block[r, c]
    return matrix / SCALE


def test_accumulated_rectangles_replace_even_zero_and_preserve_previous(tmp_path):
    source = template(tmp_path / 'primary.hdf5')
    first, second = tmp_path / 'one.hdf5', tmp_path / 'two.hdf5'
    rows, cols = np.array([[1, 2], [0, 1]]), np.array([[1, 0], [0, 2], [0, 3]])
    scores = np.array([[99.81234, 0, 90], [88, 91, 92]])
    with OverlayWriter(source, first, [2], {'k': 21}) as out:
        out.record_coverage(2, rows, cols, {'snapshot': 'one'})
        out.patch(2, rows, cols, scores)
    before = hashlib.sha256(first.read_bytes()).hexdigest()
    expected = np.zeros((8, 8))
    for i, (rs, rb) in enumerate(rows):
        for j, (cs, cb) in enumerate(cols):
            expected[rs * 4 + rb, cs * 4 + cb] = expected[cs * 4 + cb, rs * 4 + rb] = round(scores[i, j], 4)
    np.testing.assert_array_equal(read_matrix(first), expected)
    with OverlayWriter(source, second, [2], {'k': 21}, previous=first) as out:
        out.record_coverage(2, rows[:1], cols[2:], {'snapshot': 'two'})
        out.patch(2, rows[:1], cols[2:], [[0]])
    expected[6, 3] = expected[3, 6] = 0
    np.testing.assert_array_equal(read_matrix(second), expected)
    assert hashlib.sha256(first.read_bytes()).hexdigest() == before
    with h5py.File(second) as f:
        assert len(f['sequence_analysis/updates']) == 2
        assert f['sequence_analysis'].attrs['complete']
        assert np.all(f['resolutions/2/stripes/stripes_bin_weights'][:] == 1)


def test_reject_incompatible_overwrite_and_invalid_cells(tmp_path):
    source = template(tmp_path / 'primary.hdf5')
    first, second = tmp_path / 'one.hdf5', tmp_path / 'two.hdf5'
    with OverlayWriter(source, first, [2], {'k': 21}) as out:
        with pytest.raises(ValueError, match='symmetric'):
            out.patch(2, [[0, 0], [0, 1]], [[0, 0], [0, 1]], [[100, 10], [20, 100]])
        for scores in ([[np.nan]], [[-1]], [[101]]):
            with pytest.raises(ValueError):
                out.patch(2, [[0, 0]], [[0, 1]], scores)
        with pytest.raises(ValueError):
            out.patch(2, [[0, 8]], [[0, 1]], [[3]])
    with pytest.raises(ValueError, match='differ'):
        OverlayWriter(source, second, [2], {'k': 31}, previous=first)
    assert not second.exists()
    with pytest.raises(FileExistsError):
        OverlayWriter(source, first, [2], {'k': 21})


def test_sparse_empty_index_and_failure_keeps_old_overlay(tmp_path):
    source = template(tmp_path / 'primary.hdf5', 2000)
    first, broken = tmp_path / 'one.hdf5', tmp_path / 'broken.hdf5'
    with OverlayWriter(source, first, [2], {}) as out:
        out.patch(2, [[1999, 3]], [[0, 1]], [[90]])
    with h5py.File(first) as f:
        assert f['resolutions/2/treap_coo/block_offset'].id.get_storage_size() < 100_000
    assert first.stat().st_size < 1_000_000
    with pytest.raises(RuntimeError):
        with OverlayWriter(source, broken, [2], {}, previous=first):
            raise RuntimeError('cancelled')
    with h5py.File(first) as old, h5py.File(broken) as failed:
        assert old['sequence_analysis'].attrs['complete']
        assert not failed['sequence_analysis'].attrs['complete']
