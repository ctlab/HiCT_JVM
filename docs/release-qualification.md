# Current advisor-release qualification

Historical 2.0.0 release validation is retained separately. It does not qualify
the current advisor-release candidate or current-source platform CI.

The current Linux candidate contains backend version
`2.0.51-5cf6aaa-webui_1a37de6` and WebUI commit
`1a37de6d51caa90be21f8918b01c1b1dd4a532cf`. Its integrated build uses
`./gradlew test shadowJar -PrequireBundledWebUI=true --offline`, without excluding
copyWebUI. The packager requires that version's exact JAR. It must not choose an
arbitrary file from a directory containing earlier builds.

| Candidate artifact | SHA-256 |
|---|---|
| JAR `hict_server-2.0.51-5cf6aaa-webui_1a37de6-fat.jar` | `49170dd6284d691e1fbd88d20b09ab7b70fb009d3d5d41228b93395c49733092` |
| Linux `.run` | `2ff5bf621482bb4bd447563cf96b3cafaac778b05104a9249e991183097e51c5` |
| Linux `.tar.gz` | `012bc0d62b291a9770e0c2fc70c320496dc85e93bebe02495eda5c8ab8b33fbb` |

The distribution basename is
`HiCT-2.0.51-5cf6aaa-webui_1a37de6-linux-x86_64`. Artifacts remain local under
build/distributions; they are not published or tagged. Tested extracted package:
`/tmp/hict-render-completion-extracted-01/HiCT-2.0.51-5cf6aaa-webui_1a37de6-linux-x86_64`.

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

Current run `/tmp/hict-integrated-release-07` uses candidate 51 from the actual
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
