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
# Keep in sync with AssemblyAnalysisPlan's custom source-bin layout.
SEQUENCE_STRIPE_WIDTH = 256
MAX_STRIPES = 1_000_000  # Bound metadata allocations in standalone adapter calls.


def canonical_json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _metadata_digest(handle, resolutions):
    digest = hashlib.sha256()
    roots = ["contig_info"]
    for resolution in resolutions:
        root = f"resolutions/{resolution}"
        if root in handle:
            roots.extend(f"{root}/{name}" for name in ("contigs", "atl", "stripes/stripe_length_bins"))
        else:
            digest.update(f"sequence-grid-v1/{resolution}/{SEQUENCE_STRIPE_WIDTH}\n".encode())
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


def _sequence_grid(source, base, resolution):
    """Lay out fixed source bins in contig ID order without reading matrix data."""
    lengths = [int(value) for value in source['contig_info/contig_length_bp']]
    counts = [(length + resolution - 1) // resolution for length in lengths]
    size = sum(counts)
    if not 0 < size <= MAX_STRIPES * SEQUENCE_STRIPE_WIDTH:
        raise ValueError('Custom resolution exceeds the source bin budget')
    width = SEQUENCE_STRIPE_WIDTH
    stripe_count = (size + width - 1) // width
    stripe_lengths = np.full(stripe_count, width, dtype='<i8')
    stripe_lengths[-1] = size - (stripe_count - 1) * width
    stripes = base.create_group('stripes')
    stripes.create_dataset('stripe_length_bins', data=stripe_lengths)
    stripes.create_dataset('stripes_bin_weights', shape=(stripe_count, width), dtype='<f8',
                           chunks=(min(stripe_count, 128), width), compression='gzip', fillvalue=1.0)
    contigs = base.create_group('contigs')
    contigs.create_dataset('contig_length_bins', data=np.asarray(counts, dtype='<i8'))
    contigs.create_dataset('contig_hide_type', data=np.ones(len(counts), dtype='i1'))
    basis, mapping = [], []
    start = 0
    for contig, count in enumerate(counts):
        end = start + count
        while start < end:
            stripe, offset = divmod(start, width)
            length = min(end - start, width - offset)
            mapping.append((contig, len(basis)))
            basis.append((stripe, offset, offset + length, 1))
            start += length
    contigs.create_dataset('atl', data=np.asarray(mapping, dtype='<i8'))
    base.create_group('atl').create_dataset('basis_atu', data=np.asarray(basis, dtype='<i8'))
    return dict(dense_submatrix_size=width, bins_count=size, stripes_count=stripe_count,
                hdf5_max_chunk_size=268435456)


def _create_empty(source, dest, resolutions):
    if 'contig_info' not in dest:
        source.copy('contig_info', dest)
    for resolution in resolutions:
        name = f'resolutions/{resolution}'
        base = dest.require_group(name)
        if name in source:
            src = source[name]
            for key in ('contigs', 'atl', 'stripes'):
                src.copy(key, base)
            # A sequence score has no balancing weights. Keep metadata dimensions.
            weights = base['stripes/stripes_bin_weights']
            for start in range(0, weights.shape[0], 1024):
                weights[start:start + 1024] = 1.0
            attributes = src['treap_coo'].attrs
        else:
            attributes = _sequence_grid(source, base, resolution)
        treap = base.create_group('treap_coo')
        for key, value in attributes.items():
            treap.attrs[key] = value
        width = int(attributes['dense_submatrix_size'])
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
    def __init__(self, source_path, output_path, resolutions, settings, previous=None,
                 unit='identity_percent', scale=SCALE, overviews=(), template_name=None, job_id=None):
        self.path = Path(output_path)
        self.handle = None
        if unit not in ('identity_percent', 'alignment_sample_count') or scale < 1:
            raise ValueError('Unsupported overlay score unit/scale')
        self.unit, self.scale = unit, scale
        self.direct = sorted(set(map(int, resolutions)))
        self.overviews = sorted(set(overviews) - set(self.direct))
        run_levels = sorted(self.direct + self.overviews)
        if not run_levels or any(r <= 0 for r in run_levels):
            raise ValueError('Choose positive map resolutions')
        # h5py mode x makes accidental overwrite impossible, even after a race.
        with h5py.File(source_path, 'r') as source:
            native_levels = sorted(map(int, source.get('resolutions', ())))
            identity = _metadata_digest(source, native_levels)
            compatibility = dict(schema=SCHEMA, sourceMetadataSha256=identity,
                                 settings=settings, scale=scale, unit=unit)
            self.compatibility = canonical_json(compatibility)
            if previous:
                with h5py.File(previous, 'r') as old:
                    old_meta = old['sequence_analysis']
                    if not old_meta.attrs.get('complete', False):
                        raise ValueError('Cannot accumulate an incomplete overlay')
                    old_compatibility = json.loads(old_meta.attrs.get('compatibility', '{}'))
                    if 'resolutions' in old_compatibility:
                        # Older files hashed only the selected levels. Verify that
                        # hash against this template before adopting the new key.
                        old_levels = old_compatibility.pop('resolutions')
                        old_compatibility.pop('analysisLevels', None)
                        if old_compatibility.get('sourceMetadataSha256') == _metadata_digest(source, old_levels):
                            old_compatibility['sourceMetadataSha256'] = identity
                        previous_settings = old_compatibility.get('settings', {})
                        if previous_settings.get('kind') == 'alignment-dotplot':
                            previous_settings.get('parameters', {}).pop('threads', None)
                    if old_compatibility != compatibility:
                        raise ValueError('Overlay settings/source differ; create a separate overlay')
                    previous_levels = set(map(int, old['resolutions']))
                    with h5py.File(self.path, 'x') as out:
                        for key in old:
                            old.copy(key, out)
                        for key, value in old.attrs.items():
                            out.attrs[key] = value
                        _create_empty(source, out, sorted(set(run_levels) - previous_levels))
            else:
                with h5py.File(self.path, 'x') as out:
                    _create_empty(source, out, run_levels)
                    meta = out.create_group('sequence_analysis')
                    meta.attrs['score_unit'] = unit
                    meta.attrs['score_scale'] = scale
                    meta.attrs['kind'] = settings.get('kind', 'moddotplot')
                    meta.create_group('updates')
        self.handle = h5py.File(self.path, 'r+')
        self.resolutions = sorted(map(int, self.handle['resolutions']))
        meta = self.handle['sequence_analysis']
        meta.attrs['complete'] = False
        meta.attrs['compatibility'] = self.compatibility
        meta.attrs['resolutions'] = self.resolutions
        meta.attrs['explicit_coverage'] = True
        self.handle.attrs['template_name'] = template_name or Path(source_path).name
        updates = meta['updates']
        self.update = updates.create_group(str(uuid.uuid4()))
        self.update.attrs['ordinal'] = len(updates)
        self.update.attrs['analysis_levels'] = self.direct
        self.update.attrs['overview_levels'] = np.asarray(self.overviews, dtype=np.int64)
        if job_id:
            self.update.attrs['job_id'] = job_id
        for bp in self.direct:
            self.handle[f"resolutions/{bp}"].attrs["method"] = "direct"
        self.capacities = {}

    def record_coverage(self, resolution, rows, columns, provenance, update=None):
        """Axes contain physical (stripe, bin) pairs, in frozen assembly order."""
        rows, columns = self._axis(resolution, rows), self._axis(resolution, columns)
        group = (self.update if update is None else update).create_group(str(int(resolution)))
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
            if 'sequence_analysis' in self.handle:
                self.handle['sequence_analysis'].attrs['complete'] = bool(success)
            self.handle.flush()
            self.handle.close()
            self.handle = None

    def __enter__(self):
        return self

    def __exit__(self, kind, value, traceback):
        try:
            if kind is None and self.overviews:
                from overview import summarize
                for bp in self.overviews:
                    source = max(r for r in self.direct if r < bp)
                    print(f'Building {bp} bp overview from {source} bp scores', flush=True)
                    summarize(self, source, bp, patch_only=True)
        except BaseException:
            self.close(success=False)
            raise
        self.close(success=kind is None)
