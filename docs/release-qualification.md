# Current advisor-release qualification

Historical 2.0.0 release validation is retained separately. It does not qualify
the current advisor-release candidate or current-source platform CI.

The latest assembled Linux candidate contains backend version
`2.0.53-8d54602-webui_daea115` and WebUI commit
`daea115944446d62f6b77ee72999d1d0eddb15df`. Its integrated build uses
`./gradlew test shadowJar -PrequireBundledWebUI=true --offline`, without excluding
copyWebUI, and passes 625 JVM tests with zero failures/errors and seven skips.
The packager requires that version's exact JAR, with no directory-order fallback.

| Candidate artifact | SHA-256 |
|---|---|
| JAR `hict_server-2.0.53-8d54602-webui_daea115-fat.jar` | `e157d8c49d76c58d082e0b1a3b4d6fa4352ce7344af8515a550b292ddbef6e0f` |
| Linux `.run` | `33a813637f25d274ccf809e6ef2f61d1617cc6bef8963c62f1bbb9b09a90a4bb` |
| Linux `.tar.gz` | `346f51c9a0aaa077e4454897cacc1331c3ed29bbe5b3bdb5036b7ef28e92b2c1` |

The distribution basename is
`HiCT-2.0.53-8d54602-webui_daea115-linux-x86_64`. Artifacts remain local under
build/distributions; they are not published or tagged. Package generation and
checksum verification finish with exit zero. The actual .run self-extracts with
exit zero, and the extracted JAR matches the integrated build. Tested extraction:
`/tmp/hict-border-reuse-extracted-01/HiCT-2.0.53-8d54602-webui_daea115-linux-x86_64`.

## Sequential runtime qualification

`scripts/check_packaged_release_browser.py` launches the extracted package's
bin/hict with its own embedded Java runtime, packaged WebUI and bundled tools,
using fresh /tmp data/processed directories and owned loopback ports. It verifies
the served index and every asset against the package bytes before running browser
stages. It physically copies declared map inputs and generates independent mixed
and structural fixtures. Original inputs are never edited. Fixture preparation
needs Python with Cooler and NumPy, and a host JDK; browser subprocesses need
Playwright and NumPy. `--browser-python` supports a separate browser environment.

Example, from HiCT_JVM, with fresh output and an already verified extraction:

```sh
python scripts/check_packaged_release_browser.py \
  --app /tmp/extracted/HiCT-<version>-linux-x86_64 \
  --output /tmp/hict-packaged-qualification \
  --webui-commit <full-WebUI-commit> \
  --source-map /path/to/physical-copy/medium.hict.hdf5 \
  --source-map /path/to/physical-copy/large.hict.hdf5 \
  --soak-seconds 1800
```

Stages are sequential: version/citation/help metadata, declared menus with real
mouse/keyboard opening at two sizes, structural edits and exact Undo/Redo,
offline editing/session/export/API documentation, explicit navigation performance
gates for the structural and supplied maps, then the full-duration mixed soak.
The soak samples browser/Java heaps, RSS, threads, DOM/listener counts and native
memory while exercising navigation, edits, linked sequence, mixed layers and
exports. The existing numerical and memory limits remain enforced. A shorter
run is diagnostic and is not a thirty-minute qualification. Source and package
JAR hashes must still match after all stages. Server teardown targets only its
owned process group. Running stages update qualification.json and stage logs;
only terminal success supports completion.

Run `/tmp/hict-integrated-release-09` is terminal failed after 1,801.53 seconds
and 223 mixed cycles. All pre-soak gates pass. Soak frame p95 is 16.7 ms and
acknowledgement p95 47.18 ms; browser/JVM heap and RSS trends meet their limits.
DOM nodes stay at 632. Listener growth fails: early maximum 251, late maximum
282, beyond +20. No unexpected errors occur; real input bytes remain unchanged.
The notification dependency retains one document listener per toast. A guarded
production/dev bundler fix passes focused Node and real-browser lifecycle tests,
but is newer than this assembled artifact. A fresh integrated candidate and full
qualification are required. Driver receipts remain under
`/tmp/hict-border-reuse-qualification-02.{json,log}`.

Run `/tmp/hict-integrated-release-08` is terminal failed on its first soak cycle:
the driver used a persistent region instead of Shift entity selection before
Reverse. No edit request was sent; no memory sample or full soak completed.
Before that, every metadata/menu/structural/invalid-input/offline and strict
performance gate passed. All eight fresh/repeat samples across structural and
96-, 1,765- and 7,790-contig maps meet the unchanged limits: frame p95 <=16.8 ms,
acknowledgement p95 <=35.8 ms, zero long tasks. This proves packaged navigation
performance, not a completed soak.

Previous run `/tmp/hict-integrated-release-07` uses candidate 51 from the actual
.run extraction. Its checksums, self-extraction, embedded JAR identity, served
assets and metadata pass. It is terminal failed: menus, structural edits, invalid-input
recovery and offline workflows pass; navigation passes on the structural, 96- and
1,765-contig maps, but the 7,790-contig map reaches 33.4 ms frame p95 on both
fresh and repeat samples, exceeding the unchanged 33 ms limit. Input
acknowledgement p95 remains below 33 ms. Map completion is true with no pending
loads. The soak was not reached. Profile the large-map rendering before another
qualification attempt. Final publication figures, source-format coverage,
platform automation and native GUI checks also remain required.

Candidate-50 run `/tmp/hict-integrated-release-06` is retained as terminal failed.
It passed metadata/menus, structural edits, invalid-input recovery and offline
workflows, but exposed the obsolete-queue completion race before measuring its
complete 96-contig map. Candidate 51 contains the validated fix. Controlled
old/new browser evidence and twelve passing navigation samples are documented
in reinforcement-validation.md; they do not substitute for the full package run.
