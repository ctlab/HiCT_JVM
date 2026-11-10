#!/usr/bin/env python3
"""Attach source search candidates to historical DOM signatures, never infer functional coverage."""
import argparse
import hashlib
import json
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--historical', type=Path, required=True)
    parser.add_argument('--webui', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    source = [(p.relative_to(args.webui).as_posix(), p.read_text().splitlines())
              for p in sorted((args.webui / 'src/app').rglob('*'))
              if p.suffix in ('.vue', '.ts')]
    records = []
    for old in json.loads(args.historical.read_text()):
        signature = old['signature']
        stable_id = hashlib.sha256(json.dumps(signature, sort_keys=True).encode()).hexdigest()[:16]
        # Prefer identifying attributes. Text matches are only leads, including
        # framework-generated controls and dynamic labels that need manual tracing.
        field = next((name for name in ('id', 'aria', 'title', 'text')
                      if signature.get(name)), None)
        needle = signature.get(field) if field else None
        candidates = []
        if needle and len(needle) >= 3:
            for path, lines in source:
                for line, text in enumerate(lines, 1):
                    if needle in text:
                        candidates.append({'path': path, 'line': line, 'matchedField': field})
        records.append({'id': 'control-' + stable_id, 'signature': signature,
                        'historicalSurfaces': old['surfaces'], 'sourceCandidates': candidates,
                        'coverage': 'pending', 'prerequisites': None, 'expectedEffect': None,
                        'reviewRequired': 'Resolve source ownership, prerequisites and semantic outcome before assigning coverage.'})
    assert len({r['id'] for r in records}) == len(records), 'Duplicate signatures or hash collision'
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps({'schemaVersion': 1,
        'historicalSha256': hashlib.sha256(args.historical.read_bytes()).hexdigest(),
        'controls': records}, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps({'signatures': len(records),
        'withSourceCandidates': sum(bool(r['sourceCandidates']) for r in records),
        'functionalOutcomesVerified': 0}))


if __name__ == '__main__':
    main()
