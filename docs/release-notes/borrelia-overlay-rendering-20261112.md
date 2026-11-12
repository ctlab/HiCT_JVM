# Borrelia overview and O/E rendering

The Borrelia pyramid contains coarse levels with no visible contigs. The minimap
now selects a nonempty primary level with at least 64 visible bins when available,
so the overview retains useful detail. When a finer secondary source changes the
coordinate base, the minimap also binds to the replacement primary tile source.

O/E uses a distance-dependent cis expected profile for each scaffold/contig.
Between separate replicons, or where no positive finite expected value exists,
the ratio is undefined. Render these pixels transparently instead of converting
them to saturated blue depletion. A measured zero with a defined positive
expected value remains blue; enrichment/depletion within a replicon remains
red/blue. The numeric matrix-query API retains its existing finite-zero behavior
for undefined ratios. Secondary identity processing and its palette are retained.
If the viewport profile is not yet available for a requested resolution, the
fallback is also restricted to scaffold/contig domains, preventing false blue
blocks during resolution transitions.

Optional binary signal tiles evaluate the observed numeric graph and do not
implement the Expected/O/E renderer. Expected and O/E therefore use server-rendered
PNG tiles even when that experimental transport is enabled.

**Map layout** is now above Primary/Secondary selection in the sidebar.

Validation uses copied Borrelia matrix/FASTA files and an isolated server, never
the owner's open session. `scripts/check_borrelia_overlay_browser.py` starts a real
selection ModDotPlot job, checks painted minimap pixels before/after attachment,
checks control ordering, verifies transparent trans O/E pixels with and without
a cached profile, and verifies stable tile
hashes after repeated zooms/source selection, and preserves the graph through
O/E → Expected → Observed. JVM regressions distinguish undefined ratios from valid
zero depletion and check transparent RGBA output. The resolution model regression
covers empty coarse levels and tiny/no-data fallbacks.

Example invocation after starting an isolated server on copied inputs:

```sh
python scripts/check_borrelia_overlay_browser.py \
  --url http://127.0.0.1:10073/ --api http://127.0.0.1:7073 \
  --data-dir /tmp/hict-borrelia-overlay-inputs --output /tmp/borrelia-evidence
```

Browser dependencies: Playwright (with Chromium) and Pillow. Both JVM and WebUI
changes must be included in the release build.
