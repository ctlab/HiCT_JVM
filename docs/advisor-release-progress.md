# Advisor release implementation ledger

This ledger tracks implementation of the 2026-11-16 advisor/release plan agreed
in the owner conversation. Historical W/U/R sections in `plan-reinforce-final.md`
describe earlier releases; they must not override the decisions below.

## Owner decisions

- All 22 advisor requests are publication-release requirements.
- Preserve Dev accessibility, OSD design, wizard structure and bind/CORS behavior.
  Network deployment remains qualified for trusted environments.
- Exact cuts retain all sequence and source contacts. Children share crossing
  source bins through ATUs, with exact clipped FASTA intervals and no cut gap.
- Approximate Cooler export is opt-in, labeled and conservation-tested. Native
  contacts stay unchanged; shared references are not new contact observations.
- Target an Application Note. The supplied manuscript is read-only.
- Keep protected files and pre-existing dirty documents untouched. Commit by
  explicit paths per verified increment. Never push, merge or create release tags.

## Execution and coverage

| Package | Advisor items | Status |
|---|---|---|
| P1 Exact cuts, clipped bin geometry, sessions and exports | 21 | Pending |
| P2 Entity/region selection, coordinates, FASTA measurement | 2, 7, 13, 17 | Pending |
| P3 File destinations, saved/cancelled/failed results, sessions | 4, 6, 15 | Pending |
| P4 Tracks/sources separation, feature actions, resize | 9, 10, 11 | Pending |
| P5 Alignment runs/full cells and FASTA navigation | 5, 8 | Pending; existing wheel-to-letters verified |
| P6 O/E controls, coverage explanation, viewport histogram | 19, 20, 22 | Pending |
| P7 Jobs/examples, FASTA-only, OSD and diagonal provenance | 12, 14, 16 | Pending; baseline checks verified |
| P8 Opt-in remote BLAST with external-subject results | 18 | Pending |
| P9 Export viewer, immutable cut/scaffold figure snapshots | 3, 11 | Pending |
| P10 Launcher Stop returns to simple start view | 1 | Implemented; Linux private-display acceptance passed; wrapper/platform matrix pending |

P10 is independent and is being completed before the coordinated P1 changes.
No remaining package or platform is implicitly qualified by this ordering.

## Audit baseline

JVM `cd4f6540`, WebUI `f9d48ce`: 519 JVM tests with no failures/errors and
seven optional skips; WebUI build passed. Chromium exercised 18 menu destinations,
16 sequence lifecycle cases, and 20 zoom checks. Medium-map navigation p95 was
16.7–16.8 ms per frame and 29–31 ms input acknowledgement on the audit host.
FASTA-only AGP matched all 19 Borrelia records. Yeast raw 200x200 windows at
50/100 bp matched the source Cooler exactly, including its empty diagonal.
Evidence remains under `/tmp/hict-plan-audit`; archive release evidence before
claiming final qualification. Native and cross-platform GUI coverage is pending.

## Mandatory release gates

Each change needs a focused regression, applicable existing checks, and actual
UI/output verification on disposable data. Java changes run
`./gradlew test -x copyWebUI`; WebUI changes run `npm run build:web`.
Final checks use the packaged WebUI and exact artifact hashes. Test all edits,
undo/redo, session migration/restore, shared-bin geometry and conservation,
failure/cancellation, stale jobs, matrix/sequence/figure exports, 15 open/close
cycles and a 30-minute mixed soak. Preserve p95 input <=100 ms and frame <=33 ms
on declared hardware. All advertised platforms require actual package/native
dialog qualification; unrun checks remain pending.
