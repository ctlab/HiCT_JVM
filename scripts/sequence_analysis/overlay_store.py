"""Source-bin anchored numeric overlays in the existing HiCT HDF5 layout.

Writers operate on a new immutable version. Callers publish the version only
when close succeeds; failure never changes a currently open overlay. Identity
is percent * 10_000, not RGB. The optional analysis group records computed
coverage independently of nonzero values.
"""
from __future__ import annotations
import hashlib
import json
import uuid
from pathlib import Path
import h5py
import numpy as np

SCALE = 10_000
SCHEMA = "hict-sequence-overlay-v1"


def canonical_json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _metadata_digest(handle, resolutions):
    digest = hashlib.sha256()
    roots = ["contig_info"]
    roots += [f"resolutions/{r}/{name}" for r in resolutions for name in ("contigs", "atl", "stripes/stripe_length_bins")]
    def visit(name, obj):
        digest.update(name.encode())
        if isinstance(obj, h5py.Dataset):
            digest.update(str(obj.shape).encode())
            # Decode strings independently of variable/fixed HDF5 storage types.
            for start in range(0, obj.shape[0] if obj.ndim else 1, 8192):
                data = obj[start:start + 8192] if obj.ndim else obj[()]
                if h5py.check_string_dtype(obj.dtype):
                    data = np.asarray(data).flatten()
                    digest.update(canonical_json([x.decode() if isinstance(x, bytes) else str(x) for x in data]).encode())
                else:
                    digest.update(np.asarray(data).astype(obj.dtype.newbyteorder('<'), copy=False).tobytes())
    for root in roots:
        obj = handle[root]
        visit(root, obj)
        if isinstance(obj, h5py.Group):
            obj.visititems(lambda name, child: visit(root + '/' + name, child))
    return digest.hexdigest()


def _create_empty(source, dest, resolutions):
    source.copy('contig_info', dest)
    for resolution in resolutions:
        name = f'resolutions/{resolution}'
        base, src = dest.require_group(name), source[name]
        for key in ('contigs', 'atl', 'stripes'):
            src.copy(key, base)
        # A sequence score has no balancing weights. Keep metadata dimensions.
        weights = base['stripes/stripes_bin_weights']
        for start in range(0, weights.shape[0], 1024):
            weights[start:start + 1024] = 1.0
        old = src['treap_coo']
        treap = base.create_group('treap_coo')
        for key, value in old.attrs.items():
            treap.attrs[key] = value
        width = int(old.attrs['dense_submatrix_size'])
        stripes = len(base['stripes/stripe_length_bins'])
        total = stripes * stripes
        for key in ('block_offset', 'block_length'):
            # Unwritten compressed chunks remain fill-zero; no quadratic write.
            treap.create_dataset(key, shape=(total,), dtype='<i8', chunks=(min(max(1, total), 8192),), compression='gzip', fillvalue=0)
        for key in ('block_rows', 'block_cols', 'block_vals'):
            treap.create_dataset(key, shape=(1,), maxshape=(None,), dtype='<i8', chunks=(8192,), compression='gzip')
        treap.create_dataset('dense_blocks', shape=(1, 1, width, width), dtype='<i8',
                             chunks=(1, 1, width, width), compression='gzip', fillvalue=0)


class OverlayWriter:
    """Append bounded physical-block patches to a never-before-existing file."""
    def __init__(self, source_path, output_path, resolutions, settings, previous=None, unit='identity_percent', scale=SCALE):
        self.path = Path(output_path)
        self.handle = None
        if unit not in ('identity_percent', 'alignment_sample_count') or scale < 1:
            raise ValueError('Unsupported overlay score unit/scale')
        self.unit, self.scale = unit, scale
        self.resolutions = sorted(set(map(int, resolutions)))
        if not self.resolutions or any(r <= 0 for r in self.resolutions):
            raise ValueError('Choose positive map resolutions')
        # h5py mode x makes accidental overwrite impossible, even after a race.
        with h5py.File(source_path, 'r') as source:
            identity = _metadata_digest(source, self.resolutions)
            compatibility = dict(schema=SCHEMA, sourceMetadataSha256=identity,
                                 resolutions=self.resolutions, settings=settings, scale=scale, unit=unit)
            self.compatibility = canonical_json(compatibility)
            if previous:
                with h5py.File(previous, 'r') as old:
                    if not old['sequence_analysis'].attrs.get('complete', False):
                        raise ValueError('Cannot accumulate an incomplete overlay')
                    if old['sequence_analysis'].attrs.get('compatibility') != self.compatibility:
                        raise ValueError('Overlay settings/source differ; create a separate overlay')
                    with h5py.File(self.path, 'x') as out:
                        for key in old:
                            old.copy(key, out)
                        for key, value in old.attrs.items():
                            out.attrs[key] = value
            else:
                with h5py.File(self.path, 'x') as out:
                    _create_empty(source, out, self.resolutions)
                    meta = out.create_group('sequence_analysis')
                    meta.attrs['compatibility'] = self.compatibility
                    meta.attrs['score_unit'] = unit
                    meta.attrs['score_scale'] = scale
                    meta.attrs['kind'] = settings.get('kind', 'moddotplot')
                    meta.create_group('updates')
        self.handle = h5py.File(self.path, 'r+')
        self.handle['sequence_analysis'].attrs['complete'] = False
        self.update = self.handle['sequence_analysis/updates'].create_group(str(uuid.uuid4()))
        self.cursors = {}
        self.capacities = {}

    def record_coverage(self, resolution, rows, columns, provenance):
        """Axes contain physical (stripe, bin) pairs, in frozen assembly order."""
        rows, columns = self._axis(resolution, rows), self._axis(resolution, columns)
        group = self.update.create_group(str(int(resolution)))
        group.create_dataset('provenance', data=canonical_json(provenance), dtype=h5py.string_dtype('utf-8'))
        for name, axis in (('row_bins', rows), ('column_bins', columns)):
            group.create_dataset(name, data=axis, compression='gzip', shuffle=True)

    def _axis(self, resolution, axis):
        if int(resolution) not in self.resolutions:
            raise ValueError('Resolution absent from overlay')
        raw = np.asarray(axis)
        if raw.ndim != 2 or raw.shape[1] != 2 or not np.issubdtype(raw.dtype, np.integer):
            raise ValueError('Axis must contain integer (stripe, bin) pairs')
        axis = raw.astype(np.int64, copy=False)
        lengths = self.handle[f'resolutions/{resolution}/stripes/stripe_length_bins'][:]
        if np.any(axis < 0) or np.any(axis[:, 0] >= len(lengths)) or np.any(axis[:, 1] >= lengths[axis[:, 0]]):
            raise ValueError('Physical bin outside source map')
        if len(np.unique(axis, axis=0)) != len(axis):
            raise ValueError('An axis cannot contain duplicated contact bins')
        return axis

    def patch(self, resolution, rows, columns, identity):
        rows, columns = self._axis(resolution, rows), self._axis(resolution, columns)
        values = np.asarray(identity, dtype=float)
        limit = 100 if self.unit == 'identity_percent' else 2**52
        if values.shape != (len(rows), len(columns)) or not np.isfinite(values).all() or np.any(values < 0) or np.any(values > limit):
            raise ValueError('Score matrix must match axes and contain finite nonnegative values in range')
        if self.unit == 'alignment_sample_count' and np.any(values != np.floor(values)):
            raise ValueError('Alignment sample counts must be integers')
        values = np.rint(values * self.scale).astype(np.int64)
        root = self.handle[f'resolutions/{resolution}/treap_coo']
        width, stripes = int(root.attrs['dense_submatrix_size']), int(root.attrs['stripes_count'])
        for row_stripe in np.unique(rows[:, 0]):
            ri = np.flatnonzero(rows[:, 0] == row_stripe)
            for col_stripe in np.unique(columns[:, 0]):
                ci = np.flatnonzero(columns[:, 0] == col_stripe)
                r, c = rows[ri, 1], columns[ci, 1]
                patch = values[np.ix_(ri, ci)]
                lo, hi = int(row_stripe), int(col_stripe)
                if lo > hi:
                    lo, hi, r, c, patch = hi, lo, c, r, patch.T
                index = lo * stripes + hi
                # Same-stripe blocks are stored upper triangular, as contact maps.
                rr, cc = np.broadcast_arrays(r[:, None], c[None, :])
                if lo == hi:
                    rr, cc = np.minimum(rr, cc), np.maximum(rr, cc)
                    # Contradictory duplicate scores cannot be silently selected.
                    keys = (rr * width + cc).ravel()
                    order = np.argsort(keys, kind='stable')
                    ordered_values = patch.ravel()[order]
                    if np.any((np.diff(keys[order]) == 0) & (np.diff(ordered_values) != 0)):
                        raise ValueError('Identity is not symmetric for repeated physical cells')
                keys = (rr * width + cc).ravel()
                keys, positions = np.unique(keys, return_index=True)
                scores = patch.ravel()[positions]
                n, offset = int(root['block_length'][index]), int(root['block_offset'][index])
                if n and offset < 0:
                    raise ValueError('Unexpected dense block in generated overlay')
                area = slice(offset, offset + n)
                old_keys = root['block_rows'][area] * width + root['block_cols'][area]
                old_scores = root['block_vals'][area]
                keep = ~np.isin(old_keys, keys, kind='sort')
                nonzero = scores != 0
                merged_keys = np.concatenate((old_keys[keep], keys[nonzero]))
                merged_scores = np.concatenate((old_scores[keep], scores[nonzero]))
                order = np.argsort(merged_keys)
                merged_keys, merged_scores = merged_keys[order], merged_scores[order]
                self._write_entries(root, index, merged_keys // width, merged_keys % width, merged_scores)

    @staticmethod
    def _read_block(root, index, width):
        n, offset = int(root['block_length'][index]), int(root['block_offset'][index])
        block = np.zeros((width, width), dtype=np.int64)
        if n:
            if offset < 0:
                raise ValueError('Unexpected dense block in generated overlay')
            area = slice(offset, offset + n)
            block[root['block_rows'][area], root['block_cols'][area]] = root['block_vals'][area]
        return block

    def _write_entries(self, root, index, rows, cols, values):
        n = len(rows)
        previous_n = int(root['block_length'][index])
        previous_offset = int(root['block_offset'][index])
        # Geometric capacity bounds append amplification when successive score
        # tiles touch the same large physical block. Only this new version is
        # mutated; previous published versions remain immutable.
        capacity = self.capacities.get((root.name, index), previous_n)
        offset = previous_offset
        if n > capacity:
            capacity = max(64, 1 << (n - 1).bit_length())
            offset = len(root['block_vals'])
            for name in ('block_rows', 'block_cols', 'block_vals'):
                root[name].resize((offset + capacity,))
            self.capacities[root.name, index] = capacity
        if n:
            for name, data in (('block_rows', rows), ('block_cols', cols), ('block_vals', values)):
                target = root[name]
                target[offset:offset + n] = data
        root['block_offset'][index] = offset
        root['block_length'][index] = n

    def close(self, success=True):
        if self.handle is not None:
            self.handle['sequence_analysis'].attrs['complete'] = bool(success)
            self.handle.flush()
            self.handle.close()
            self.handle = None

    def __enter__(self):
        return self

    def __exit__(self, kind, value, traceback):
        self.close(success=kind is None)
