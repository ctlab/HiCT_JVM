# Branch CI and release validation

The owner authorized pushes and cancellation of superseded Actions runs on
2026-11-17. This supersedes the earlier no-push instruction. Check current branch
CI before every commit/push, monitor replacement runs to completion, and inspect
failures rather than treating a successful local Linux build as platform proof.
Do not merge or manually create release tags. Normal gated release workflows may
run after an authorized push.

Before pushing, fetch both branch heads, verify that the push is a fast-forward,
and push WebUI before JVM when both changed. Record the exact resulting SHAs.
Only queued/running runs on the same branch with an older superseded SHA may be
cancelled. Keep useful failure logs/artifacts. Do not cancel the current required
validation or unrelated branches, weaken tests or enable early publication.

## Initial audit

The WebUI head `2eb9f29` passes its four-platform build:
[WebUI CI](https://github.com/ctlab/HiCT_WebUI/actions/runs/44132027047).
The JVM head `2f492034` fails the Windows/macOS regression matrix:
[Java regressions](https://github.com/ctlab/HiCT_JVM/actions/runs/44132052957).
The preceding portable run built all four platforms successfully, then failed
its regression gate and skipped release publication:
[Portable Packages](https://github.com/ctlab/HiCT_JVM/actions/runs/44072316658).
The publication gate is correctly enforcing the failing regressions.

The shared failure is `ExampleSourceProvenanceTest` expecting converted-source
attribution. `capture()` canonicalized the data root before looking up metadata
whose cache key retained its supplied spelling. macOS `/var` aliases and Windows
short paths can therefore miss a valid conversion cache. Lookup now checks the
supplied and real-root spellings while retaining real-path output containment
and exact cache/source validation. A synthetic POSIX alias regression fails on
Linux before the change and passes afterward. The original cross-platform test
remains enabled; Windows/macOS CI must confirm their native path behavior.

The Windows sequence-track test also fails while deleting its temporary folder.
HTSJDK maps BAM indexes by default and its mapped-buffer `close()` drops the Java
reference without immediate unmapping. Windows now uses ordinary index I/O so
track closure can release the file deterministically. The existing Windows
regression and package matrix must confirm that fix; Linux retains its existing
mapping behavior.

Evidence is under `/tmp/hict-actions-audit-01`, including the downloaded Windows
JUnit XML, and the following local receipts:

- `/tmp/hict-ci-path-alias-red-01.log`: added alias regression fails before fix.
- `/tmp/hict-ci-path-and-track-green-01.log`: targeted provenance/track tests pass.
- `/tmp/hict-ci-release-regressions-01.log` and `.exit.json`: 642 JVM tests,
  zero failures/errors, two optional skips, plus one passing native-baseline test.
- `/tmp/hict-ci-release-workflow-contracts-01.log`: all 25 workflow, checkout,
  artifact-selection and publication-safeguard contracts pass.

Remote confirmation of the new commits is pending. A completed older run or a
successful package-build job does not qualify a newer SHA or its publication gate.


The next coordinated source increment is WebUI `3804e45` and JVM patch
`2.0.64-2f49203-webui_3804e45`, with desktop lifecycle hardening and both
cross-platform regression fixes. Local validation passed before commit/push.
Remote run conclusions must be recorded against the resulting exact SHAs.

## First pushed replacement and macOS fixture correction

WebUI `3804e455` passes all four jobs in [WebUI CI](https://github.com/ctlab/HiCT_WebUI/actions/runs/44138402420).
JVM `2f923de3e1e7ba66408a2b31889df419e9b11c29` passes Linux and Windows
in both push/PR matrices, including the original conversion-provenance and BAM
cleanup regressions. Its ARM macOS job fails only the newly added alias fixture:
`@TempDir` itself uses `/var`, so its second conversion root was another alias,
not the canonical root the test intended to exercise.

The fixture now explicitly uses `root.toRealPath()` for that branch and reports
the tested conversion-root spelling on failure. Both supplied-alias and canonical
branches remain exercised; production lookup and cache validation are unchanged.
The original all-platform test remains enabled. Focused provenance/cache tests
pass locally in `/tmp/hict-ci-macos-alias-fixture-01.log`. A new macOS run must
confirm the correction. The prior four active older-SHA JVM runs were cancelled
after their replacements started, as authorized; receipt:
`/tmp/hict-ci-superseded-cancellations-01.json`.
