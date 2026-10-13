#!/usr/bin/env python3
"""HiCT adapter CLI, separate from upstream ModDotPlot's plotting command."""
import argparse
import json
import sys
import time
from pathlib import Path
import numpy as np
from moddotplot_kernel import UPSTREAM_COMMIT, Settings, PreparedFasta, prepare_windows, calculate_blocks
from overlay_store import OverlayWriter


def run(plan_path):
    plan_path = Path(plan_path)
    plan = json.loads(plan_path.read_text())
    if plan.get('kind') == 'dotplot':
        from dotplot_kernel import run as alignment_dotplot
        return alignment_dotplot(plan, log=lambda message: print(message, flush=True))
    options = Settings(**plan['settings'])
    log = lambda message: print(message, flush=True)
    start = time.perf_counter_ns()
    settings = dict(kind='moddotplot', upstream=UPSTREAM_COMMIT, parameters=plan['settings'],
                    sourceFasta=plan['sourceFastaIdentity'], windowPolicy='source-bin-current-sequence-v2', gapPolicy=plan.get('gapPolicy', 'legacy-gap-free'))
    resolutions = [level['resolution'] for level in plan['levels']]
    source = PreparedFasta(plan['input'], plan['inputLength'])
    try:
        with OverlayWriter(plan['template'], plan['output'], resolutions, settings, previous=plan.get('previous')) as writer:
            for level in plan['levels']:
                r, c = np.asarray(level['rows'], dtype=np.int64), np.asarray(level['columns'], dtype=np.int64)
                if r.ndim != 2 or c.ndim != 2 or r.shape[1] not in (4, 6) or c.shape[1] != r.shape[1]:
                    raise ValueError('Each axis needs stripe, bin, sequence start and sequence end')
                windows = list(dict.fromkeys([tuple(x) for x in r[:, 2:].tolist() + c[:, 2:].tolist()]))
                sketches = prepare_windows(lambda a, b: source.read(a - plan['inputOffset'], b - plan['inputOffset']),
                    plan['sequenceLength'], [w[:2] for w in windows], level['resolution'], options, log,
                    context_bounds=[w[2:] for w in windows] if r.shape[1] == 6 else None)
                by_window = dict(zip(windows, sketches))
                rows = [by_window[tuple(w)] for w in r[:, 2:]]
                cols = [by_window[tuple(w)] for w in c[:, 2:]]
                writer.record_coverage(level['resolution'], r[:, :2], c[:, :2],
                    dict(snapshot=plan['snapshot'], sourceFragments=plan['sourceFragments'],
                         inputSha256=plan['inputSha256'], rowWindows=r[:, 2:].tolist(), columnWindows=c[:, 2:].tolist()))
                calculate_blocks(rows, cols, options,
                    lambda a, b, values: writer.patch(level['resolution'], r[a:a + len(values), :2],
                        c[b:b + values.shape[1], :2], values), log=log)
    finally:
        source.close()
    log(f'Finished ModDotPlot in {(time.perf_counter_ns() - start) / 1e9:.3f} s: {plan["output"]}')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--version', action='store_true')
    parser.add_argument('--plan', type=Path)
    args = parser.parse_args()
    if args.version:
        print(json.dumps(dict(adapter='hict-moddotplot-v1', upstream=UPSTREAM_COMMIT)))
    elif args.plan:
        run(args.plan)
    else:
        parser.error('Provide --plan or --version')


if __name__ == '__main__':
    main()
