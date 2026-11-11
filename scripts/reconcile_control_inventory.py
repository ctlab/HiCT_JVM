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
    parser.add_argument('--contracts', type=Path, default=Path(__file__).resolve().parents[1]
                        / 'src/test/resources/reinforcement/control-contracts.json')
    args = parser.parse_args()
    source = [(p.relative_to(args.webui).as_posix(), p.read_text().splitlines())
              for p in sorted((args.webui / 'src/app').rglob('*'))
              if p.suffix in ('.vue', '.ts')]
    reviewed = json.loads(args.contracts.read_text())
    if reviewed.get('schemaVersion') != 1:
        raise ValueError('Unsupported control contract schema')
    contracts = {}
    source_by_path = dict(source)
    for contract in reviewed['contracts']:
        key = contract['id']
        if key in contracts:
            raise ValueError('Duplicate contract: ' + key)
        lines = source_by_path.get(contract['source'])
        if not lines or contract['anchor'] not in '\n'.join(lines):
            raise ValueError('Source anchor changed; review contract ' + key)
        if not contract['prerequisites'] or not contract['expectedEffect']:
            raise ValueError('Missing prerequisites or expected effect: ' + key)
        if contract['coverage'] not in ('inspected', 'pending') or contract['functionalCoverage'] != 'pending':
            raise ValueError('Static reconciliation cannot certify runtime outcomes: ' + key)
        contracts[key] = contract
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
        key = 'control-' + stable_id
        contract = contracts.pop(key, None)
        records.append({'id': key, 'signature': signature,
                        'historicalSurfaces': old['surfaces'], 'sourceCandidates': candidates,
                        'coverage': contract['coverage'] if contract else 'pending',
                        'contract': contract,
                        'reviewRequired': 'Verify the functional outcome at runtime; resolve pending source ambiguities.'})
    if contracts:
        raise ValueError('Contracts absent from historical fixture: ' + ', '.join(contracts))
    assert len({r['id'] for r in records}) == len(records), 'Duplicate signatures or hash collision'
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps({'schemaVersion': 1,
        'historicalSha256': hashlib.sha256(args.historical.read_bytes()).hexdigest(),
        'contractsSha256': hashlib.sha256(args.contracts.read_bytes()).hexdigest(),
        'reviewedWebuiRevision': reviewed['webuiRevision'],
        'controls': records}, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps({'signatures': len(records),
        'withSourceCandidates': sum(bool(r['sourceCandidates']) for r in records),
        'withContracts': sum(r['contract'] is not None for r in records),
        'sourceInspected': sum(r['coverage'] == 'inspected' for r in records),
        'functionalOutcomesVerified': 0}))


if __name__ == '__main__':
    main()
