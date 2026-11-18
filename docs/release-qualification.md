# Current advisor-release qualification

Historical 2.0.0 release validation is retained separately. It does not qualify
the current advisor-release candidate or current-source platform CI.

## Published rolling package 2.0.364

The [published 2.0.364 build](https://github.com/ctlab/HiCT_JVM/actions/runs/44187130439)
contains JVM `9b07395e079ee3073ff779f73e12465ce0a6a454` and WebUI
`d6f771b3a5c6d70da1c52064946519df5db23ce4`. The release asset digest for
`HiCT-2.0.364-ci1-9b07395-x86_64.AppImage` is
`c44a06510a2fd4871e98d11b7f41005c4b167f33bf4ec8da2e4b9839b0b70299`.

The exact AppImage passed extract-and-run in an Ubuntu 22.04 desktop test
container with loopback as its only network interface. Its bundled hictk built
the resolution pyramid and balanced a synthetic four-contig Cooler. The
packaged server opened the resulting HiCT file in Chromium. A reverse edit
survived cancelling Close, saving, reloading and restoring the session. PNG,
SVG, PDF and bundled API documentation opened or exported without external
requests or page errors. The input and AppImage checksums were unchanged.
A separate run forced use of the bundled glibc loader and passed HDF5, native
processing and bundled command checks. This host lacks `/dev/fuse`. The
container checks cover extract-and-run.

The published JAR returned `unknown` for `webuiVersion` from `/version`.
Commit `c4595d06` moves the WebUI package metadata into Gradle's resource copy.
The packaged browser gate now checks the API field alongside the WebUI's About
dialog. A local test with Gradle's generated resource confirmed that the API
reports the WebUI version. The package gate also places conflicting
`version.txt` and `CHANGELOG.md` files in the disposable data directory.
The server now prefers the metadata embedded in the JAR for both endpoints.
The newer hosted package still requires these checks against its exact bytes.

Receipts and exports are under
`build/release-evidence/portable-publication-priority/hosted-linux-appimage/offline-qualification/`.
The previous Linux `.run` soak and the clipboard, accessibility, editing and
publication checks are recorded in [advisor release progress](advisor-release-progress.md).

## Earlier candidate 2.0.68

The qualified candidate contains JVM
`bf6ee7a3987487994ebd5046384458f209be8eed`, WebUI
`72a4d80991802e6cf40e8b93751dceaa37165e19`, version
`2.0.68-56bf2bd-webui_72a4d80`. Its integrated build passes 646 JVM tests,
zero failures/errors, two optional skips and native parity. The JAR contains
exactly the current 64 frontend files, with identical bytes and no stale chunks.

| Candidate artifact | SHA-256 |
|---|---|
| JAR | `9cd3899fa3e0fe787a7f24e4f699ecd3145fe6fc455d14dccc77e6c1ab0cce54` |
| Linux `.run` | `da9fc201d23557bc487b00045b3edbbd71f2add672923201ede6548331890c99` |
| Linux `.tar.gz` | `6095b14b7c8d31cda35a9e58003247f2d5e858e6c44c758275bb4fdcc202fa4d` |

The actual `.run` extraction and tar/staging comparison pass all 3,102 entries,
including bytes, modes and links. Final Linux Tauri passes eight native
coordinate/export cases and four lifecycle cases. Final Linux Electron passes
nine native export cases, eight recovery cases and fourteen figure/rendering/
storage cases. These 43 cases use the final packaged payloads and JRE; a full
JDK is used only for fixture generation. Receipts are retained under
`build/release-evidence/2.0.68/final-*`.

The complete serial package run `/tmp/hict-integrated-release-15` passes metadata,
menus, structural edits, incomplete-open recovery, offline workflows and all four
maps' strict fresh/repeat navigation gates. Its 1,803.74-second mixed soak passes
227 cycles: frame p95 16.7 ms, acknowledgement p95 44.34 ms, browser heap slope
0.0781 MiB/min and JVM heap slope 0.0606 MiB/min. JVM RSS trends downward; DOM
maxima remain 632 and listener maxima decrease from 228 to 224. There are no
unexpected errors, and source/JAR hashes remain unchanged. Other agent GUI QA
and compiler jobs stayed stopped throughout this run. Earlier failures remain
retained, including version 66's
33.3-ms frame p95 against an unchanged 33-ms limit during concurrent GUI QA.

Current-source Java push/PR matrices and WebUI CI pass all four platforms,
including sixteen shared native save tests per WebUI platform. The Windows
Portable Packages job fails a separate HTTP cancellation test while waiting for
directory-entry metadata to report staged bytes. A test-only live-handle metadata
change passes all 23 full native tests locally; replacement Windows validation
is still required. Both current Autobuild Release runs pass. Other portable
conclusions, AppImage qualification, broader formats and
accessibility remain open. Windows/macOS interactive GUI checks are explicitly
owner-deferred in `release-manual-checklist.md`; deferral is not a pass.

The coordinated `2.0.69-bf6ee7a-webui_ea5c2c7` source follow-up corrects VCF
reference intervals and reviews current
control-contract anchors. All five original VCF regressions fail meaningfully
before the fix; six VCF and three existing sequence-track cases pass afterward,
including fifteen rejected malformed/overflow inputs. The full updated source
suite passes 652 JVM tests (zero failures/errors, two optional skips), native
parity and the complete WebUI build. Its WebUI pin is
`ea5c2c76d80db1906c1202f7ac531bcbe72765e2`. These changes are not covered by
the version-68 candidate's packaged runtime results. Replacement platform CI
and packaged acceptance remain required. This is not an overall release sign-off.

## Historical packaged candidates

An earlier fully packaged Linux candidate contains backend version
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

Run `/tmp/hict-integrated-release-10` is terminal **passed** for candidate
`2.0.55-ecfd475-webui_95866d5`. The producer and driver both exit zero. Verified
`.run` extraction supplies the tested JAR, runtime, native tools and 53 served
assets. Metadata, two-size menu inspection, structural edits/Undo/Redo,
incomplete-open recovery, offline workflows and all strict fresh/repeat
performance samples pass. The mixed soak lasts 1,805.96 seconds/229 cycles:
frame p95 16.7 ms, edit acknowledgement p95 42.08 ms, browser heap slope
0.0898 MiB/min and JVM RSS slope 0.8516 MiB/min. DOM remains 632; listener
maxima are 224 then 228, within the unchanged +20 limit. No unexpected errors
occur, and native input/JAR hashes are unchanged after testing.

Artifact SHA-256 values:

- JAR: `ed1e66599d945319525dfd7ca326a1b3ed27ce72b57e42b858a5259713930170`
- Linux `.run`: `b2791e77116a3f24f22712e22b9e97396053ece26cab61e3d5be27182953356d`
- Linux `.tar.gz`: `5c288d6158feec6491286556fe83c1e444c59dd2cbb6d504dff0638f4150cc61`

Receipts: `/tmp/hict-sonner-cleanup-package-01.exit.json`,
`/tmp/hict-sonner-cleanup-qualification-01.json`,
`/tmp/hict-integrated-release-10.driver.exit.json`, and the run's
`qualification.json` and `soak/results.json`. Qualification applies to those
exact bytes. Subsequent RID-recovery development is not covered by this run.
Publication figures/readability, wider source formats and current native GUI/
Windows/macOS qualification remain open; this is not an overall release sign-off.

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
