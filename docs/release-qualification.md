# Current advisor-release qualification

Historical 2.0.0 release validation is retained separately. It does not qualify
the current advisor-release candidate or current-source platform CI.

## Published portable release 2.0.382

Portable Packages run 44241313376 pins JVM
`0c483e29a90067b086636cca2137d12a95a3c3d0` and WebUI
`332c67b4d227f9a58148338b74eea596a6e52230`. The Linux runfile from Actions
verified against SHA-256
`1585d4a63a9c997fa91096956a69336bd1301c44c31ec6acd8165c77945ace5a`.
The extracted JAR has SHA-256
`4fe67b0a7d1f6ad6178b8a2fcfecda951d76383639628bc21bcfe776c4079f5f`.
The package starts its bundled server and serves 53 verified WebUI files.
[Portable Packages run](https://github.com/ctlab/HiCT_JVM/actions/runs/44241313376)

On that Linux build, Chromium passed metadata and menu checks, exact undo
and redo for seven structural edits, incomplete-file recovery, and offline
session restore, PNG/SVG/PDF export and bundled API documentation. Across
four navigation fixtures, the maximum frame p95 was 16.8 ms and the maximum
input acknowledgement p95 was 32.5 ms. The release gates are 33 ms and
100 ms respectively.

The Linux runfile passed all ten qualification stages. The mixed soak ran
231 cycles in 1,804.75 seconds, with 16.7 ms frame p95 and 54.02 ms input
acknowledgement p95. Browser heap grew by 1.08 MiB, with a fitted rate of
0.049 MiB/min. Memory and DOM trend gates passed. The browser reported no
errors, all ten fixture files retained their checksums, and the runfile and
JAR hashes matched after the soak.

The exact published AppImage passed on an Ubuntu test container with only
loopback networking. Its bundled hictk converted the test Cooler. Chromium
preserved the edited assembly through session save and restore, exported PNG,
SVG and PDF, and opened bundled Swagger. Both the Cooler input and AppImage
retained their checksums. This test used extract-and-run mode.

The same Linux release passed native clipboard tests. Electron pasted the
exact rectangular coordinates and a 722 × 566 PNG. The Hi-C, FASTA and AGP
controls stayed beside Jobs at 640, 800, 1280 and 1440 px. Tauri pasted a
1068 × 844 PNG into a separate Electron window. The receiver decoded the
image and saved the clipboard bytes for verification.

Chromium passed 13 Tracks and Layers workflows on the published assets,
including keyboard tabs, file and Cooler-weight attachment, late-response
handling, and panel resizing with edge-pixel checks. Twelve FASTA measurement
workflows covered repeat detection, reverse strands, ambiguous bases, keyboard
selection, Jobs delivery, and invalidation after an exact cut. Receipts are in
`published-track-workflows/`.

The published assets passed 13 viewport-histogram and 11 O/E palette checks.
The slowest histogram refresh after a pan was 27.6 ms on the tested fixtures
against the 150 ms gate. Chromium transfer timestamps confirmed a peak of four
overlapping tile requests. The palette tests checked graph and sidebar edits,
secondary isolation, session restoration and exact RGBA values. During a
rendering change the pixel check retries only tiles explicitly marked as
cancelled and requires every successful image to match its requested dimensions.
Receipts are in `published-signal-final/` and `published-palette-retry/`.

In a loopback-only container, the three-click yeast Micro-C example reached
a readable map in 139.12 seconds from cached source files and a fresh
conversion. Reopening through the same File menu in a fresh browser profile
took 4.37 seconds with the converted map cached. Both runs verified 17
chromosomes against the source, compared the linked sequence with the FASTA,
and passed navigation, Reverse, exact Split, Undo, session restore and AGP
export. The input and converted-map hashes matched across the reopen. The
receipt is in `published-yeast/qualification.json`.

Windows passed startup from both `run.cmd` and the custom EXE. The Apple Silicon
and Intel macOS launchers also passed startup. The macOS 12.3 deployment target
audits checked 2,145 and 2,149 binaries respectively with zero errors.
These hosted checks serve the expected API version, WebUI and JavaScript
revision. All four platform package jobs passed. GitHub published all 17 assets
in 1h 10m 21s from run creation. The published provenance and checksum manifest
match the tested Linux runfile and its extracted JAR. The owner-deferred desktop
checks are documented in the
[platform checklist](release-manual-checklist.md).
[Public release](https://github.com/ctlab/HiCT_JVM/releases/tag/latest-jvm-sfx-native)

Receipts are in
`build/release-evidence/portable-publication-priority/run-44241313376/`.

## Published clipboard release 2.0.371

The Linux installer produced by [Portable Packages](https://github.com/ctlab/HiCT_JVM/actions/runs/44215590944)
contains JVM `b84ed77fad3eb23cb8a19b0a0dcfbbccfd951a59` and WebUI
`ed184e8b229e8132718e7facb09903726151ad4a`. The exact runfile
`HiCT-2.0.371-ci1-b84ed77-linux-x86_64.run` verified against SHA-256
`44b4ba8b9dc88e7f2be26067af899401969e9c1a9a87fa8e8d41dbc04d4adec3`.
The extracted JAR matched the independent workflow download. Its 64 embedded
WebUI files matched the exposed package files byte for byte. Bundled native
tool checks passed, and the About dialog and version endpoint reported the
package's expected versions and WebUI commit.

All eight Chromium selection stages passed, including the real coordinate
and image clipboard checks. The packaged Electron shell pasted a 722 × 566 PNG
and the exact rectangular coordinate text. The file controls stayed beside Jobs
at 640, 800, 1280 and 1440 px. The packaged Tauri shell wrote a 1068 × 844 PNG
that an independent GTK process read and saved. A separate Ctrl+V test pasted
the Tauri selection into an independent Electron editing field. The receiver
decoded and displayed the complete 1068 × 844 PNG. WebKit's own editable test
field received an empty image paste event on this Linux stack. These checks ran
in private Linux displays with loopback networking only. The cross-app receipt
and screenshot are in `native-tauri/cross-app/` below.

The released JAR and its bundled Java runtime also passed all 12 Linux
Launcher checks on a private display. The test started a real server, confirmed
its version endpoint, then used Stop to terminate the owned server and restore
the simple Launcher card. The post-Stop screenshot shows the enabled Start
action with the configuration and live log hidden. The Launcher test uses
isolated settings and data directories. Its browser lifecycle fixtures use
inert child processes. Screenshots and hashes are in `launcher-reset/` within
the release evidence directory below.

The exact Linux runfile completed all ten qualification stages, including
230 cycles in a 1,805.36-second mixed soak. Frame p95 was 16.8 ms and
input acknowledgement p95 was 53.90 ms. Browser heap grew by 0.93 MiB,
with a fitted rate of 0.051 MiB/min. The memory and DOM trend gates passed.
There were no browser errors, and all ten HDF5 files and the package JAR
retained their checksums.

The same installer rendered the synthetic substitution at the expected base
centre through resolution changes, exact Split, Reverse and Undo. The split
produced 124 and 11,893 bp fragments. All three export formats passed with
the shared writable-picker fixture. Native aligner cases and stale-response
rejection also passed. The AppImage used its bundled hictk offline through
conversion, edit/session restore, PNG/SVG/PDF export and bundled Swagger.

The published build converted the cached public yeast sources into a fresh
HiCT file. A repeat in a fresh Chromium profile opened the verified example
in three clicks, reaching the 17-contig map and linked FASTA in 6.09 s with
the converted cache. Navigation, Reverse, exact Split, Undo, session
restore and AGP export completed in 12.62 s with no console or page errors.
Exact Split preserved the assembly length and the AGP hash matched the
saved assembly. Both external source files retained their input hashes.

A separate fresh profile used **File → Open Examples… → Open example** with
checksum-verified local copies of the yeast sources. HiCT built a new native
matrix and reached the readable 17-chromosome map and linked FASTA in 139.09 s.
Both Jobs reached 100%. The full journey through zoom, Reverse, exact Split,
Undo, session restore and matching AGP export took 145.61 s. Chromium reported
zero page and console errors. The source hashes, screenshots and exports are
under `file-menu-quickstart/` in the release evidence directory below.

Eleven packaged Jobs checks also passed with synthetic inputs. Clearing the
notification badge and history preserved a completed search and its result.
Search and ModDotPlot results opened after their dialogs closed. Conversion,
both dotplot routes, coverage, track precompute and alignment completed. A
FASTA-only assembly reopened from Jobs and exported all 12,017 bases to AGP.
Chromium reported zero unexpected errors and the three inputs retained their
hashes. Evidence is in `jobs-and-notifications/` below.

The published JVM and WebUI also passed all 13 focused histogram checks.
The tests cover measured zeros, unsupported O/E bins, fit controls on each
source, compact layouts and failed-request recovery. Across seven measured
pans, the histogram updated in 4.9–27.8 ms against a 150 ms target. The
fixture matrices retained their original hashes.

All 13 display-mode checks passed on the same Linux package. An independent
5 × 5 contact fixture verifies the fitted Expected values and O/E ratios across
a scaffold boundary. The rendered tile gives three measured zeros the depletion
color and leaves 18 undefined ratios transparent. The matrix JSON represents
those undefined values as `null`. Evidence is in `expected-boundary/` below.

The Windows, Linux and both macOS package jobs passed. The hosted macOS 12.3
compatibility reports found zero errors in 1,109 arm64 and 1,113 Intel
binary entries. Interactive Windows and macOS validation remains deferred
as recorded in the [platform checklist](release-manual-checklist.md).

The rolling release published after 2h 17m 11s. The public build origin matches
the JVM and WebUI revisions above. All 17 public digests were verified using
the manifest and GitHub asset metadata. The Linux runfile and JAR digests
match the exact tested local files. [Public release](https://github.com/ctlab/HiCT_JVM/releases/tag/latest-jvm-sfx-native)

The receipt and screenshots are under
`build/release-evidence/portable-publication-priority/run-44215590944/`.

## Opening maps with unusable Cooler weights

The checksum-verified Linux Actions JAR `2.0.379-ci1-31655aa` intermittently
opened a nearly blank map in the manual picker. A synthetic fixture contains
30,236 real bins with no usable Cooler weights. The API and sidebar already
reported that file weights were off. The wizard drew the diagonal on the same
fixture.

A browser regression delayed the sidebar's pre-opening visualization response
until after the opening preset committed. The hosted WebUI then drew only
9% of the sampled manual-open diagonal. The revised WebUI ignores superseded
option reads and sends the opening options explicitly. With the same hosted
JVM and injected delay it drew all 240 sampled points in both opening routes,
displayed the fallback notice and passed all 17 assertions. The new WebUI
also passed all 17 assertions through the published Linux `2.0.382` package.
Both Manual Open and the wizard rendered all 240 sampled diagonal points,
with no page or console errors. The published JAR and fixture checksums
were unchanged.

The same package passed 16 browser measurements across finer, coarser,
disjoint and interleaved secondary resolution pyramids. The disjoint fixture
preserved its 12,017-bp assembly and the projected contact landmark through
Reverse, exact Split and contig scoping. All five input maps retained their
checksums. Receipts are in `published-opening-retry/` and
`published-mixed-pyramids-retry/` under the release evidence directory.

## Publication figure reproduction

The published `2.0.382` JAR and WebUI regenerated the three Borrelia panels
with the bundled ModDotPlot tool. The exact cut produced 450,123-bp and
460,601-bp children. The 259,811 contact-layer pixels and the complete native
scene matched the retained artwork. The locally regenerated track uses
pyBigWig 0.3.25. Its 155 genomic intervals and values match the retained
UCSC-generated track exactly. Source checksums were unchanged.

The 178-mm vector PDF contains zero raster page images and has a minimum
measured text size of 7.05 pt. The decoded 900-dpi TIFF pixels match the
retained artwork and the new TIFF passed its gzip round trip. The review
copies, retained caption, evidence and checksum manifest are under
`published-figure/review-artwork/` in the release evidence directory.

## Remote BLAST with a controlled provider

The published `2.0.382` JAR and packaged WebUI passed 18 Chromium checks
with the controlled provider executable in a loopback-only container. They
covered consent, searches without a map, cancellation and timeout, result
reopening, and export of all 20,001 fixture rows. Local sequence analysis
stayed available while the provider job was waiting. Browser errors were
empty, and all 13 fixture inputs retained their original checksums. The
receipt is in `published-remote-blast/browser/results.json`. The separate
live provider retrieval evidence is in the
[advisor ledger](advisor-release-progress.md#opt-in-remote-blast-and-independent-external-reports).

## Earlier rolling package 2.0.364

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
