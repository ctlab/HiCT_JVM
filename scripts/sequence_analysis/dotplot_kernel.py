"""minimap2-compatible selected-rectangle dotplots on frozen source-bin axes.

Alignment samples retain the existing HiCT PAF interpolation convention. A
disk-backed accumulator bounds RAM, and patching replaces zeros as well as hits.
This is an alignment dotplot, not a nucleotide-identity estimate.
"""
import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess
import threading
import numpy as np
from moddotplot_kernel import PreparedFasta
from overlay_store import OverlayWriter


def write_region(source, path, start, end, offset, name):
    with open(path, 'xb') as out:
        out.write(('>' + name + '\n').encode())
        for left in range(start, end, 80 * 8192):
            sequence = source.read(left - offset, min(end, left + 80 * 8192) - offset)
            out.write(b''.join(sequence[i:i+80] + b'\n' for i in range(0, len(sequence), 80)))


def sample_paf(path, rows, columns, row_offset, col_offset, step_bp, min_length, emit):
    """Emit canonical physical-bin pairs; reject points in unowned bin gaps."""
    if step_bp < 1 or min_length < 1:
        raise ValueError('Sample interval and minimum alignment length must be positive')
    with open(path) as stream:
        for line in stream:
            fields = line.rstrip().split('\t')
            if len(fields) < 12:
                raise ValueError('Malformed PAF record')
            qlen, qs, qe, tlen, ts, te, length = map(int, (fields[1], fields[2], fields[3], fields[6], fields[7], fields[8], fields[10]))
            if fields[0] != 'rows' or fields[5] != 'columns' or fields[4] not in ('+', '-') or not (0 <= qs < qe <= qlen and 0 <= ts < te <= tlen):
                raise ValueError('PAF coordinates do not match prepared axes')
            if length < min_length:
                continue
            steps = max(1, length // step_bp)
            for start in range(0, steps + 1, 8192):
                fraction = np.arange(start, min(steps + 1, start + 8192), dtype=np.float64) / steps
                q = qs + np.floor(fraction * (qe - qs - 1) + 0.5).astype(np.int64) + row_offset
                t = np.floor(fraction * (te - ts - 1) + 0.5).astype(np.int64)
                t = (ts + t if fields[4] == '+' else te - 1 - t) + col_offset
                ri = np.searchsorted(rows[:, 2], q, side='right') - 1
                ci = np.searchsorted(columns[:, 2], t, side='right') - 1
                valid = (ri >= 0) & (ci >= 0)
                valid &= (q < rows[np.maximum(ri, 0), 3]) & (t < columns[np.maximum(ci, 0), 3])
                emit(ri[valid], ci[valid])


def run(plan, log=print):
    options = plan['settings']
    for key in ('k', 'window', 'chainScore', 'sampleBp', 'minAlignmentLength', 'threads'):
        if type(options[key]) is not int or options[key] <= 0:
            raise ValueError('Invalid alignment setting: ' + key)
    if options['k'] > 28 or options['threads'] > 256:
        raise ValueError('Aligner k must be <=28 and threads <=256')
    root = Path(plan['output']).parent
    row_start = min(w[2] for level in plan['levels'] for w in level['rows'])
    row_end = max(w[3] for level in plan['levels'] for w in level['rows'])
    col_start = min(w[2] for level in plan['levels'] for w in level['columns'])
    col_end = max(w[3] for level in plan['levels'] for w in level['columns'])
    source = PreparedFasta(plan['input'], plan['inputLength'])
    try:
        write_region(source, root / 'rows.fasta', row_start, row_end, plan['inputOffset'], 'rows')
        write_region(source, root / 'columns.fasta', col_start, col_end, plan['inputOffset'], 'columns')
    finally:
        source.close()
    with open(plan['aligner'], 'rb') as stream:
        aligner_sha = hashlib.file_digest(stream, 'sha256').hexdigest()
    settings = dict(kind='alignment-dotplot', parameters=options, alignerSha256=aligner_sha,
                    sourceFasta=plan['sourceFastaIdentity'], sampling='paf-linear-endpoints-v1', gapPolicy=plan.get('gapPolicy', 'legacy-gap-free'))
    command = [plan['aligner'], '-t', str(options['threads']), '-k', str(options['k']), '-w', str(options['window']),
               '-m', str(options['chainScore']), '-P', '--no-long-join', str(root / 'columns.fasta'), str(root / 'rows.fasta')]
    log('Aligning selected sequence axes with ' + str(plan['aligner']))
    with open(root / 'alignment.paf', 'xb') as paf:
        process = subprocess.Popen(command, stdout=paf, stderr=subprocess.PIPE, text=True)
        try:
            for line in process.stderr:
                log(line.rstrip())
            if process.wait() != 0:
                raise RuntimeError('Alignment failed; see job log')
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()
    with OverlayWriter(plan['template'], plan['output'], [x['resolution'] for x in plan['levels']], settings,
                       previous=plan.get('previous'), unit='alignment_sample_count', scale=1) as writer:
        for level in plan['levels']:
            resolution = level['resolution']
            rows, cols = np.asarray(level['rows'], dtype=np.int64), np.asarray(level['columns'], dtype=np.int64)
            width = int(writer.handle[f'resolutions/{resolution}/treap_coo'].attrs['dense_submatrix_size'])
            rkeys, ckeys = rows[:, 0] * width + rows[:, 1], cols[:, 0] * width + cols[:, 1]
            db = sqlite3.connect(root / f'samples-{resolution}.sqlite')
            try:
                db.execute('CREATE TABLE samples(a INTEGER, b INTEGER, n INTEGER, PRIMARY KEY(a,b)) WITHOUT ROWID')
                db.execute('PRAGMA cache_size=-8192')
                def accept(ri, ci):
                    a, b = rkeys[ri], ckeys[ci]
                    keys, counts = np.unique(np.column_stack((np.minimum(a, b), np.maximum(a, b))), axis=0, return_counts=True)
                    db.executemany('INSERT INTO samples VALUES (?,?,?) ON CONFLICT(a,b) DO UPDATE SET n=n+excluded.n',
                                   [(int(a), int(b), int(n)) for (a,b),n in zip(keys,counts)])
                sample_paf(root / 'alignment.paf', rows, cols, row_start, col_start,
                           options['sampleBp'], options['minAlignmentLength'], accept)
                db.commit()
                writer.record_coverage(resolution, rows[:, :2], cols[:, :2], dict(snapshot=plan['snapshot'],
                    sourceFragments=plan['sourceFragments'], alignerSha256=aligner_sha, inputSha256=plan['inputSha256']))
                for a in range(0, len(rows), 128):
                    rk = rkeys[a:a+128]
                    for b in range(0, len(cols), 128):
                        ck = ckeys[b:b+128]
                        ri, ci = ','.join('?' for _ in rk), ','.join('?' for _ in ck)
                        values = {(int(x),int(y)): n for x,y,n in db.execute(
                            f'SELECT a,b,n FROM samples WHERE (a IN ({ri}) AND b IN ({ci})) OR (a IN ({ci}) AND b IN ({ri}))',
                            list(map(int,rk))+list(map(int,ck))+list(map(int,ck))+list(map(int,rk)))}
                        block = [[values.get((min(x,y),max(x,y)),0) for y in ck] for x in rk]
                        writer.patch(resolution, rows[a:a+len(rk), :2], cols[b:b+len(ck), :2], block)
                    log(f'Wrote {min(a+128,len(rows))}/{len(rows)} rows at {resolution} bp')
            finally:
                db.close()
