# Reinforcement implementation evidence

## Scope and status

Started from `plan-reinforce-final.md`, in its prescribed W0-first order.
This change is a **W0 foundation increment**, not completion of W0 or a release.
No runtime defects are claimed fixed by adding regression tests. W1–W16 have
not been started. In particular, session loss, unsafe replacement, stale sequence,
armed scissors after Escape, settings persistence and mixed-resolution failures
still block release.

The direct instruction to commit now supersedes the earlier review-only ban.
All pre-existing changed/untracked files and `plan*.md` are protected. Only paths
introduced or edited for this increment are eligible for staging. No push or merge.

Baseline: JVM `0dbcb5f42a7d06eab178c0197e57d5478e138105`, WebUI
`8107d95fa6234cc67bed31652e8ad7543e2eba4a`; both indexes empty, WebUI clean.
SHA-256 of every pre-existing untracked/modified file and all plan files was
recorded locally in `/tmp/hict-reinforce-w0/baseline.json` before editing.

## Implemented foundation

- Shared synthetic matrix, FASTA, projection and graph fixtures, with zero rows,
  literal Log2 values, reverse slices, gaps, ambiguous bases and a custom multiplier.
- Production-code regression tests for every nonempty sequence subwindow, plain
  and gzip input, source coordinates, graph serialization and numerical values.
- Explicit coordinate, source capability and job contracts in
  `reinforcement-contracts.md`, with future contracts distinguished from runtime.
- Browser helpers for actual FASTA selection and source buttons, corrected
  user-triggered range scanning and preset selection, and order-independent ruler
  lane intersection checks.
- Changed-content FASTA relink oracle checks both stationary axes. A mere request
  or increased revision no longer proves success.
- Missing dotplot levels must retain a rendered layer. The former assertion that
  no layer was visible encoded the defect as expected behavior and is removed.
- Rotated exports use a rounded content envelope allowing captions and rulers,
  retaining signal checks, independent SVG/PDF rasterization and viewport restoration.
  This envelope is not a complete text-clipping/pixel-parity oracle; W16 still needs it.
- Reusable fresh-browser/repeat-open navigation diagnostic records action round-trip
  and settled time, frame distribution, long tasks, resource timings, heap and DOM.
  It does not label Playwright click latency as human input acknowledgement.

## Reproducibility

Run tests with:

```sh
./gradlew test -x copyWebUI --tests '*ReinforcementFixtureTest'
./gradlew test -x copyWebUI
```

Both passed: 315 Java tests, zero failures/errors, seven skipped optional tests.
The two added tests passed. WebUI application source was not changed, so no new
WebUI build is claimed. Browser runs use the previously built JAR
`hict_server-1.0.216-50ae800-webui_8107d95-fat.jar` and existing WebUI dist.
This is a baseline of current runtime, not qualification of a new distributable.

Machine: AMD Ryzen 9 7900X, 64,964,784 kB RAM, Linux 6.8.0-146-lowlatency,
1440×1000 headless Chromium. Playwright runs from the existing validation Python
environment. API `127.0.0.1:5202`, WebUI `127.0.0.1:8203`; physical/reflink copies
under `/tmp/hict-reinforce-w0/data`. No symlink or hardlink to original inputs.
The local `runtime-manifest.json` records the following inputs:

| Input | Bytes | SHA-256 |
|---|---:|---|
| combined_ind2_4DN.hict.hdf5 | 154317488 | `8cce355723881effce56ea8da89e25ca3087ebb30159ef13c7e85af20ca5f317` |
| ind2.self.hict.hdf5 | 51572875 | `d6b75919eacb551f35d92b218a8a67e97114297ff0909c491ab14c58c59fb6ed` |
| quad_combined_ind2.fasta | 294768927 | `6fdc6272679e228e485a2c782dbb5ad07cb294c49029613ed1ac90be55cc8106` |

These user-provided data are referenced by checksum and not redistributed in git.
Source-publication provenance is not asserted for these local test inputs.

## Interactive baseline results

Evidence: `/tmp/hict-reinforce-w0/`, including commands and exit codes in
`browser-results.json`, per-suite logs, screenshots, exported files and JSON.
Initial helper failures are preserved as `*.initial`; they are harness mistakes,
not additional application defects. Current helpers select file rows explicitly
and expand the range disclosure before scanning.

| Check | Result | Outcome verified |
|---|---|---|
| Exact signal range | Pass, 6.52 s | User-triggered primary scan, nine blocks, pan does not rescan, independent secondary bounds, return reuses cache |
| Edited view | Fails at missing repaint | FASTA is linked by UI; reversal succeeds but the stationary sequence axis never repaints. Later assertions remain unexecuted |
| Large FASTA | Fails at changed-content relink | Bounded 64 MiB Java indexing, 100 independent sequence intervals, 30 rapid pans, 76 letters/colors, 76 SVG letters and delayed-response recovery pass first |
| ModDotPlot zoom | Fails at missing 10K layer | Two real 5K computations preserve 339,729 primary pixels each; secondary threshold changes preserve primary pixels. Preset assertions after the failure remain unexecuted |
| Rotated export | Current assertions pass, 16.49 s | Four rotation/scope combinations × SVG/PNG/PDF, rounded content envelope, signal present in independent rasterizations, exact viewport restoration |
| Navigation diagnostic | Recorded separately | Forty actual map zoom-button actions across two opens; see controlled raw-count baseline below |

No browser page errors or server errors were recorded in the dotplot run. Its
failure is the intended W8 regression, not a generation failure. The export pass
does not establish complete glyph clipping or cross-format pixel equivalence.

The first navigation run reproduced nearly blank default rendering (R3). Retained
under `reinforcement_baseline/`, it is not the representative signal-rendering
measurement. `reinforcement_baseline_raw/` explicitly selects PRIMARY raw counts,
linear red, 0–100, both triangles before measurement. The measured opening duration
includes this setup. Fresh browser does not mean cold disk/server cache. These are
short local diagnostics, not W16 latency, memory or platform qualification.

Controlled baseline, Chromium 156.0.8078.4:

| Measurement | Fresh browser | Repeat open |
|---|---:|---:|
| Open + controlled graph + settled map | 412.1 ms | 335.3 ms |
| Frame interval p50 / p95 | 16.7 / 16.7 ms | 16.7 / 16.7 ms |
| Recorded frames | 365 | 364 |
| Observed long tasks over 50 ms | 0 | 0 |
| Tile/matrix resource entries | 65 | 36 |
| Tile/matrix resource duration p95 | 46.82 ms | 17.60 ms |
| Playwright click round-trip p95 | 36.38 ms | 36.12 ms |
| Zoom action to settled map p95 | 306.76 ms | 304.35 ms |
| JS heap after forced GC | 19,857,504 bytes | 25,694,328 bytes |
| DOM nodes / listeners | 1,746 / 669 | 1,844 / 675 |

The heap increase is not a plateau result; W6's repeated-open retention diagnosis
remains required. Timing includes browser instrumentation and only the declared
zoom workload. Visible red contact signal was checked in the saved screenshots.

## Remaining W0 work before W1

1. Generate a tiny on-disk HDF5 fixture and exercise real source I/O and disjoint
   pyramids, not only JSON signal/projection inputs.
2. Integrate the capability contract into persisted source metadata, `/open`, Attach,
   secondary status, frontend DTOs and accessible control availability.
3. Reconcile all 232 historic control signatures and 18 menu destinations against
   source with stable ids, prerequisites, outcome expectations and evidence status.
   A DOM inventory is not 232 passing functional scenarios.
4. Complete catalog and legacy/custom preset/session fixtures against the actual
   finalized schemas. Current catalog snippets are semantic cases, not schema passes.
5. Extend measured performance to pan/input acknowledgement and representative
   medium/large workflows. The navigation diagnostic is not a 30-minute soak or
   proof of a memory plateau, and OS/server caches are not cleared.
6. Finish the export-caption oracle and resolve any remaining harness failures
   before treating subsequent failures as application regressions.

W0 is intentionally left open. Later packages must not be marked complete, and
release readiness must not be inferred from the Java test pass.
