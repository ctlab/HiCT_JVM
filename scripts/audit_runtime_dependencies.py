#!/usr/bin/env python3
"""Query OSV for resolved Maven runtime versions; report uncovered local artifacts separately."""
import argparse
import datetime
import json
from pathlib import Path
import urllib.request


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--inventory', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    inventory = json.loads(args.inventory.read_text())
    components = inventory['components']
    assert components, 'No runtime Maven components in inventory'
    queries = [{'package': {'ecosystem': 'Maven', 'name': row['group'] + ':' + row['name']},
                'version': row['version']} for row in components]
    report = {'checkedUtc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
              'source': 'https://api.osv.dev/v1/querybatch',
              'scope': 'Resolved Maven runtime versions; not reachability, native tools or local JAR coverage',
              'componentsChecked': len(components),
              'localFilesWithoutMavenArtifact': sorted(set(inventory['files']) - {row['file'] for row in components})}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    try:
        answers = []
        for offset in range(0, len(queries), 1000):
            batch = queries[offset:offset + 1000]
            request = urllib.request.Request(report['source'], json.dumps({'queries': batch}).encode(),
                {'Content-Type': 'application/json', 'User-Agent': 'HiCT-runtime-release-audit/1'})
            with urllib.request.urlopen(request, timeout=30) as response:
                result = json.load(response)['results']
            assert len(result) == len(batch), 'Incomplete OSV response'
            answers.extend(result)
        findings = [{'component': row, 'advisories': answer['vulns']}
                    for row, answer in zip(components, answers) if answer.get('vulns')]
        report.update(status='completed', flaggedComponents=findings)
    except Exception as error:
        report.update(status='incomplete', error=type(error).__name__ + ': ' + str(error))
        args.output.write_text(json.dumps(report, indent=2) + '\n')
        raise
    args.output.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({'status': report['status'], 'componentsChecked': len(components),
                      'flaggedComponents': len(findings), 'uncoveredLocalArtifacts': report['localFilesWithoutMavenArtifact']}))
    return 1 if findings else 0


if __name__ == '__main__':
    raise SystemExit(main())
