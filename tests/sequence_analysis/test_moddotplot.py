import sys
from pathlib import Path
import numpy as np
import pytest
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'scripts/sequence_analysis'))
from moddotplot_kernel import Settings, PreparedFasta, prepare_windows, calculate_blocks
from moddotplot.estimate_identity import prepare_sequence_sketches, selfContainmentMatrix
from moddotplot.parse_fasta import _hash_sequence


def test_aligned_windows_match_upstream_and_rectangular_blocks_have_correct_axes():
    random = np.random.default_rng(721)
    part = ''.join(random.choice(list('ACGT'), 350))
    sequence = (part + part[:200] + 'N' * 150 + part[::-1] + 'N' * 50).encode()
    settings = Settings(k=11, identity=45, sketch_size=64, delta=0.5)
    resolution = 100
    windows = [(s, min(s + resolution, len(sequence))) for s in range(0, len(sequence), resolution)]
    actual = prepare_windows(lambda a, b: sequence[a:b], len(sequence), windows, resolution, settings)
    sparse, expectation = settings.sparsity(resolution)
    expected = prepare_sequence_sketches(sequence, resolution, sparse, settings.delta, settings.k, False, expectation)
    for i, (core, neighbor, _) in enumerate(actual):
        np.testing.assert_array_equal(core, expected.core[i])
        np.testing.assert_array_equal(neighbor, expected.neighbors[i])
    oracle = selfContainmentMatrix(expected.core, expected.neighbors, settings.k, settings.identity, False)
    assembled = np.zeros(oracle.shape)
    calculate_blocks(actual, actual, settings,
        lambda r, c, v: assembled.__setitem__((slice(r, r + v.shape[0]), slice(c, c + v.shape[1])), v), block_size=3)
    np.testing.assert_allclose(assembled, oracle, atol=1e-12)
    rows, cols = [3, 1, 8], [0, 1, 7, 2, 6]
    rectangular = np.zeros((len(rows), len(cols)))
    calculate_blocks([actual[i] for i in rows], [actual[i] for i in cols], settings,
        lambda r, c, v: rectangular.__setitem__((slice(r, r + v.shape[0]), slice(c, c + v.shape[1])), v), block_size=2)
    np.testing.assert_allclose(rectangular, oracle[np.ix_(rows, cols)], atol=1e-12)


def test_nonuniform_windows_and_reverse_complements_match_exhaustive_hash_oracle():
    random = np.random.default_rng(97)
    part = ''.join(random.choice(list('ACGT'), 503))
    rc = part.translate(str.maketrans('ACGT', 'TGCA'))[::-1]
    sequence = (part + rc + 'N' * 90 + part).encode()
    windows = [(420, 550), (21, 83), (530, 711), (1006, 1096), (0, 503), (503, 1006)]
    settings = Settings(k=13, identity=0, sketch_size=10000, delta=0)
    actual = prepare_windows(lambda a, b: sequence[a:b], len(sequence), windows, 503, settings)
    sets = []
    for i, (a, b) in enumerate(windows):
        hashed = _hash_sequence(sequence[a:b].decode(), settings.k, False)
        expected = set(map(int, hashed.compressed()))
        sets.append(expected)
        assert set(map(int, actual[i][0])) == expected
    result = np.zeros((len(windows), len(windows)))
    calculate_blocks(actual, actual, settings,
        lambda r, c, v: result.__setitem__((slice(r, r + len(v)), slice(c, c + v.shape[1])), v), block_size=2)
    for r, a in enumerate(sets):
        for c, b in enumerate(sets):
            overlap = len(a & b)
            containment = max(overlap / len(a) if a else 0, overlap / len(b) if b else 0)
            assert result[r, c] == pytest.approx(100 * containment ** (1 / settings.k))
    assert result[4, 5] == 100
    assert np.all(result[3] == 0)


def test_prepared_fasta_index_at_wrapping_edges(tmp_path):
    sequence = ('ACGTRYN' * 59).encode()
    path = tmp_path / 'prepared.fa'
    path.write_bytes(b'>assembly\n' + b'\n'.join(sequence[i:i + 80] for i in range(0, len(sequence), 80)) + b'\n')
    fasta = PreparedFasta(path, len(sequence))
    try:
        for a in [0, 1, 79, 80, 81, 400, len(sequence)]:
            for b in [a, min(a + 1, len(sequence)), len(sequence)]:
                assert fasta.read(a, b) == sequence[a:b]
    finally:
        fasta.close()


def test_invalid_scientific_parameters_are_rejected():
    for options in ({'k': 0}, {'delta': 2}, {'identity': float('nan')}, {'sketch_size': 0}):
        with pytest.raises(ValueError):
            Settings(**options)
