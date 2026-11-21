"""Reduce stored scores in original contig coordinates, including fractional bin overlaps."""
from collections import defaultdict
from contextlib import closing
from functools import lru_cache
from itertools import groupby
import sqlite3

import numpy as np


class BinLayout:
    def __init__(self, level):
        self.bp = int(level.name.rsplit('/', 1)[1])
        self.width = int(level['treap_coo'].attrs['dense_submatrix_size'])
        self.lengths = level.file['contig_info/contig_length_bp'][:]
        self.by_stripe = defaultdict(list)
        self.by_contig = defaultdict(list)
        basis = level['atl/basis_atu'][:]
        ordinals = defaultdict(int)
        for contig, atu in level['contigs/atl'][:]:
            stripe, lo, hi, forward = map(int, basis[atu])
            ordinal = ordinals[contig]
            self.by_stripe[stripe].append((lo, hi, contig, ordinal, forward))
            self.by_contig[contig].append((ordinal, ordinal + hi-lo, stripe, lo, hi, forward))
            ordinals[contig] += hi-lo
        self.ends = {c: np.array([a[1] for a in spans]) for c, spans in self.by_contig.items()}

    def physical(self, contig, ordinal):
        spans = self.by_contig[contig]
        i = self.ends[contig].searchsorted(ordinal, side='right')
        if i == len(spans):
            return -1
        start, _, stripe, lo, hi, forward = spans[i]
        return stripe * self.width + (lo + ordinal - start if forward else hi-1-(ordinal-start))


def overlaps(fine, coarse):
    @lru_cache(maxsize=64)
    def stripe_bins(stripe):
        # A finer interval overlaps at most two coarser intervals on its contig.
        keys = np.full((fine.width, 2), -1, dtype=np.int64)
        weights = np.zeros_like(keys)
        for lo, hi, contig, ordinal, forward in fine.by_stripe[stripe]:
            for index in range(lo, hi):
                start = int(ordinal + (index-lo if forward else hi-1-index)) * fine.bp
                end = min(start + fine.bp, int(fine.lengths[contig]))
                for slot, target in enumerate(range(start // coarse.bp, (end-1) // coarse.bp + 1)):
                    keys[index, slot] = coarse.physical(contig, target)
                    weights[index, slot] = min(end, (target+1)*coarse.bp) - max(start, target*coarse.bp)
        return keys, weights
    return stripe_bins


def occupied(root):
    lengths = root['block_length']
    size = lengths.chunks[0] if lengths.chunks else 8192
    starts = (lengths.id.get_chunk_info(i).chunk_offset[0] for i in range(lengths.id.get_num_chunks())) if lengths.chunks else range(0, len(lengths), size)
    for start in starts:
        values = lengths[start:start + size]
        for offset in np.flatnonzero(values):
            yield int(start + offset), int(values[offset])


def project_axis(axis, project, width):
    keys = set()
    for stripe in np.unique(axis[:, 0]):
        mapped = project(int(stripe))[0][axis[axis[:, 0] == stripe, 1]]
        keys.update(np.unique(mapped[mapped >= 0]).tolist())
    return np.array([divmod(key, width) for key in sorted(keys)], dtype=np.int64).reshape(-1, 2)


def add_entries(db, left, right, area, scores, target, presence):
    """Keep the upper triangle. Both orientations of each off-diagonal input are visited."""
    valid = (left >= 0) & (right >= left)
    left, right, area, scores = left[valid], right[valid], area[valid], scores[valid]
    if not len(left):
        return
    width, stripes = target
    blocks = (left // width) * stripes + right // width
    cells = (left % width) * width + right % width
    keys, inverse = np.unique(np.column_stack((blocks, cells)), axis=0, return_inverse=True)
    if presence:
        db.executemany('INSERT OR IGNORE INTO totals VALUES (?, ?, 1, 1)', keys.tolist())
    else:
        sums = np.bincount(inverse, weights=scores*area)
        weights = np.bincount(inverse, weights=area)
        db.executemany('INSERT INTO totals VALUES (?,?,?,?) ON CONFLICT(block,cell) DO UPDATE SET '
                       'total=total+excluded.total, weight=weight+excluded.weight',
                       ((int(b), int(c), s, w) for (b,c), s,w in zip(keys,sums,weights)))


def entries(source, index, n, diagonal):
    offset = int(source['block_offset'][index])
    if offset >= 0:
        for pos in range(offset, offset+n, 8192):
            part = slice(pos, min(offset+n, pos+8192))
            yield tuple(source[key][part] for key in ('block_rows','block_cols','block_vals'))
    else:
        dataset = source['dense_blocks']
        width = int(source.attrs['dense_submatrix_size'])
        for start in range(0, width, 32):
            block = dataset[-offset-1, 0, start:start+32, :]
            r,c = np.nonzero(block)
            keep = (r+start <= c) if diagonal else slice(None)
            r,c = r[keep],c[keep]
            yield r+start,c,block[r,c]


def summarize(writer, source_bp, target_bp, updates=None):
    handle = writer.handle
    fine = BinLayout(handle[f'resolutions/{source_bp}'])
    coarse = BinLayout(handle[f'resolutions/{target_bp}'])
    project = overlaps(fine, coarse)
    source = handle[f'resolutions/{source_bp}/treap_coo']
    target = handle[f'resolutions/{target_bp}/treap_coo']
    presence = writer.unit in ('alignment_sample_count', 'alignment_presence')
    counts = writer.unit == 'counts'
    reduction = 'overlap_fraction_sum' if counts else 'any_alignment' if presence else 'positive_identity_area_mean'
    for update in [writer.update] if updates is None else updates:
        if str(source_bp) not in update:
            continue
        coverage = update[str(source_bp)]
        axes = [project_axis(coverage[name][:], project, coarse.width) for name in ('row_bins', 'column_bins')]
        writer.record_coverage(target_bp, *axes, dict(method=reduction, sourceResolution=source_bp), update=update)
    level = target.parent
    level.attrs.update(method=reduction, source_resolution=source_bp)
    shape = coarse.width, int(target.attrs['stripes_count'])
    # SQLite's private temporary database bounds the accumulator independently of map area.
    with closing(sqlite3.connect('')) as db:
        db.execute('CREATE TABLE totals(block INTEGER,cell INTEGER,total REAL,weight REAL,'
                   'PRIMARY KEY(block,cell)) WITHOUT ROWID')
        for index, n in occupied(source):
            a, b = divmod(index, int(source.attrs['stripes_count']))
            for rows, cols, scores in entries(source, index, n, a == b):
                valid = np.isfinite(scores) & ((scores != 0) if counts else (scores > 0))
                rows, cols, scores = rows[valid], cols[valid], scores[valid]
                rk, rw = project(a)
                ck, cw = project(b)
                distinct = (a != b) | (rows != cols)
                for i in range(2):
                    for j in range(2):
                        left, right = rk[rows,i], ck[cols,j]
                        area = rw[rows,i].astype(float)*cw[cols,j]
                        if counts:
                            area /= rw[rows].sum(axis=1) * cw[cols].sum(axis=1, dtype=float)
                        add_entries(db, left, right, area, scores, shape, presence)
                        add_entries(db, right[distinct], left[distinct], area[distinct], scores[distinct], shape, presence)
        # Re-read final direct scores after each update. This includes the old portion
        # of a partially overwritten coarse bin and clears bins whose last hit disappeared.
        for index, n in occupied(target):
            writer.capacities[target.name,index] = n
            target['block_length'][index] = 0
        expression = 'total' if counts else 'total/weight'
        cells_cursor = db.execute(f'SELECT block,cell,{expression} FROM totals ORDER BY block,cell')
        for block, group in groupby(cells_cursor, key=lambda entry: entry[0]):
            values = list(group)
            cells = np.array([entry[1] for entry in values])
            scores = np.array([entry[2] for entry in values])
            if target["block_vals"].dtype.kind != "f":
                scores = np.rint(scores).astype(np.int64)
            writer._write_entries(target, block, cells // coarse.width, cells % coarse.width, scores)


def cache_existing(plan):
    import h5py
    from overlay_store import OverlayWriter
    with h5py.File(plan['source'], 'r') as source:
        generated = 'sequence_analysis' in source
        template = plan['template'] if generated else plan['source']
        levels = plan['overviewLevels']
        with OverlayWriter(template, plan['output'], levels, {}) as writer:
            out = writer.handle
            del out['sequence_analysis']
            for name in source:
                if name == 'resolutions':
                    for bp in source[name]:
                        source[name].copy(bp, out[name])
                else:
                    if name in out:
                        del out[name]
                    source.copy(name, out)
            for key, value in source.attrs.items():
                out.attrs[key] = value
            writer.unit = plan['unit']
            writer.resolutions = sorted(map(int, out['resolutions']))
            updates = list(out['sequence_analysis/updates'].values()) if generated else []
            for index,bp in enumerate(levels):
                direct = max(r for r in plan['directLevels'] if r < bp)
                print(f'Building overview {index+1}/{len(levels)} at {bp} bp', flush=True)
                root = out[f'resolutions/{bp}/treap_coo']
                if writer.unit == 'counts' or source[f'resolutions/{direct}/treap_coo/block_vals'].dtype.kind == 'f':
                    del root['block_vals']
                    root.create_dataset('block_vals', shape=(1,), maxshape=(None,), dtype='<f8', chunks=(8192,), compression='gzip')
                summarize(writer, direct, bp, updates)
