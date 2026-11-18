# Current advisor-release qualification

Historical 2.0.0 release validation is retained separately. It does not qualify
the current advisor-release candidate or current-source platform CI.

## Published portable release 2.0.391

Run 44338895434 published 17 verified assets in 59m 20s from JVM
`befa3ae287e50500a419a4bfa4c414fb74038ec3` and WebUI
`534f3c467eaafc49cb61c18991e2abd14d4e4f9c`. All six JVM workflows and the
pinned WebUI matrix passed. Windows ZIP and EXE, Linux runfile and AppImage,
and both macOS architecture packages passed hosted startup.
[Portable Packages](https://github.com/ctlab/HiCT_JVM/actions/runs/44338895434)

The published Linux runfile passed ten selection checks and nine pre-soak
stages for metadata, menus, structural editing, incomplete-file recovery,
offline use, and navigation across four fixtures. The highest p95 among the
four navigation fixtures was 16.8 ms per frame and 32.23 ms for input
acknowledgement. The harness verified the two allocator arenas, 64 MiB
initial heap and 15–30% free-heap settings on the running Java process.
The 30-minute soak completed 228 cycles at 16.8 ms frame p95 and 61.02 ms
input p95. Server RSS trended −7.867 MiB/min. The measured JVM and browser
heap slopes were 0.082 and 0.063 MiB/min. The soak reported no browser errors.
The receipts are in
`build/release-evidence/portable-publication-priority/run-44338895434/`.

The exact runfile pasted the selection PNG into a separate Linux desktop app
from Tauri and Electron. Electron also pasted the exact coordinate text.
The Hi-C, FASTA and AGP controls stayed beside Jobs at all four tested widths.
Launcher Stop returned to the simple view, and the owned server received
both the expected default profile and the override in separate launches.
The Borrelia replay reproduced the contact matrix, GC track and exact cuts.
The public AppImage converted and reopened a Cooler, restored a session and
exported three figure formats with its network limited to loopback. Both the
normal and bundled-glibc launch paths used the verified Linux heap settings.

## Published portable release 2.0.390

Run 44313825161 uses JVM `2ee3744bb33fa226b4fadb0de6d4c8fc5d460b26` and
WebUI `534f3c467eaafc49cb61c18991e2abd14d4e4f9c`. GitHub published 17 verified
assets in 1h 38m. Windows ZIP and EXE, Linux runfile and AppImage, and both
macOS architectures passed hosted startup. The public manifest matches the
Linux runfile and JAR used in local tests. The companion Intel native PR
retry passed, completing the six JVM workflows for that commit.

The local runfile from Actions artifact 14436265218 passed all ten selection
scenarios and nine pre-soak stages. Those stages covered the packaged metadata,
18 menu destinations, structural editing, incomplete-file recovery, offline
operation and four navigation fixtures. Across fresh and repeat opens, the
worst measured p95 was 16.8 ms per frame and 32.28 ms for input acknowledgement.
The harness confirmed `MALLOC_ARENA_MAX=2` in the package's Java process
without an override. The 30-minute soak ran 232 cycles at 16.7 ms frame p95
and 55.99 ms input p95, with no browser errors. The server RSS trend was
1.540 MiB/min against the 1.0 MiB/min limit. Used JVM heap grew at
0.102 MiB/min. Native Memory Tracking's committed Java heap trended up by
1.327 MiB/min. The unchanged package passed a second 30-minute soak with a
64 MiB initial heap and 15–30% free-heap targets. Across 232 cycles it measured
0.358 MiB/min RSS growth, 16.7 ms frame p95 and 55.60 ms input p95. Median RSS
after warm-up fell from 637.89 to 314.50 MiB. The comparison kept the two-arena
setting and the same four fixtures. The next package uses the tested JVM
profile by default on Linux. Custom extra JVM options retain precedence.
Evidence is under
`build/release-evidence/portable-publication-priority/run-44313825161/`.

The exact runfile passed independent desktop clipboard checks. Electron pasted
the exact coordinates and a 722 × 566 PNG. Tauri pasted a 1,068 × 844 PNG into a
separate Electron window. The active files stayed beside Jobs at all four tested
widths. Launcher Stop returned to the simple view. The published runtime
reproduced all 259,811 contact pixels, 155 GC windows, exact cut lengths and
three snapshots in the Borrelia figure.
[Portable Packages run](https://github.com/ctlab/HiCT_JVM/actions/runs/44313825161)
· [Public release](https://github.com/ctlab/HiCT_JVM/releases/tag/latest-jvm-sfx-native)
· [JDK 21 heap sizing](https://docs.oracle.com/en/java/javase/21/gctuning/garbage-first-g1-garbage-collector1.html)

## Published portable release 2.0.389

Run 44293359089 uses JVM `ba27ce05b7076018be305b2fe290da32a5da2c36` and
WebUI `534f3c467eaafc49cb61c18991e2abd14d4e4f9c`. GitHub published 17 verified
assets in 1h 38m 45s. All six JVM workflows and the WebUI matrix passed.
The cold Intel BLAST build took 60 minutes in the portable workflow.
The official smoke script
passed on isolated checkouts of both revisions. Its JVM suite reported 664
tests, zero failures and nine environment-dependent skips. WebUI type-check,
production build and ESLint without fixes passed, with 187 lint warnings.
All seven publication artwork tests passed.

A separate run of the exact source used checksum-verified local tools and the
freshly compiled baseline JNI library. Seven of the skipped cases passed,
covering JNI parity, native HTTP sequence search and ModDotPlot, a BLAST gapped
alignment with raw scores, Cooler group import, minimap/Hictk conversion and
SeqKit results against an independent oracle. The other two source-smoke skips
require the optional large-map and external HIC assembly fixtures. Receipts are
in `build/release-evidence/portable-publication-priority/run-44293359089/`.
[Portable Packages run](https://github.com/ctlab/HiCT_JVM/actions/runs/44293359089)

The Windows ZIP and EXE, Linux runfile and AppImage, and both macOS
architectures passed hosted startup. The public release manifest matches the
runfile and JAR used for local tests. The Linux runfile passed all ten
selection checks and all 18 menu scenarios, including accessible names on all
17 tested dialog close controls. No page or HTTP errors were reported. The
four navigation fixtures passed on fresh browsers and repeat opens, with a
maximum frame p95 of 16.8 ms and input acknowledgement p95 of 31.93 ms. Java
received `MALLOC_ARENA_MAX=4` from the launcher. The 30-minute soak completed
232 cycles at 16.7 ms frame p95 and 55.42 ms input acknowledgement p95, with
no browser errors. Server RSS grew at 1.126 MiB/min against the 1.0 MiB/min
limit, almost entirely in anonymous memory. Used JVM heap grew at
0.073 MiB/min and the browser heap at 0.064 MiB/min. The exact Linux package
therefore has an open memory gate under its installed default. The same
runfile, JAR, inputs, JVM options and 30-minute test passed with the allocator
limit explicitly set to two, as verified in the Java process environment.
Across 232 cycles, server RSS trended −1.123 MiB/min, used JVM heap
0.085 MiB/min and browser heap 0.066 MiB/min. Frame p95 was 16.73 ms and
input acknowledgement p95 was 55.14 ms. All input hashes matched and the
browser reported no errors. The next Linux package uses two arenas by
default, with the environment override available. That package requires
its own default-setting soak.

The exact public runfile also passed independent desktop clipboard checks.
Electron pasted the tab-separated coordinates and a 722 × 566 PNG. Tauri
pasted a 1,068 × 844 PNG into a separate Electron window. The active files
stayed beside Jobs at 640, 800, 1280 and 1440 px. The bundled Launcher
started the real server and returned to its simple view after Stop. The
Borrelia figure replay matched all 259,811 contact pixels and 155 GC windows,
and reproduced the 450,123 and 460,601 bp cut, three snapshots and PNG, SVG
and PDF exports. The exact public AppImage passed offline Cooler conversion,
session restore, figure exports and bundled API documentation.

On the exact published JAR and WebUI, the offline yeast Micro-C suite passed
16 browser checks. Info followed a stationary pointer across four resolution
transitions with delayed tile responses and read no GPU tile pixels. Checks
covered map replacement, mixed primary and secondary resolution guidance,
and resetting the displayed coordinates outside the assembly. The independent
Cooler comparison matched all 669,584 raw counts in 17 windows over 50, 100 and
500 bp resolutions. The 50 bp source had zero cis contacts on offsets 0–3 and
1,365,532 at offset 4. Evidence is under
`build/release-evidence/portable-publication-priority/run-44313825161/published-389-yeast-info/`.

The published package also passed 13 track workspace checks, including keyboard
access to both tabs, feature centering and zoom, attaching weights to either map,
and pixel checks at three panel sizes. The signed-format suite passed 24 checks
across plain and gzip VCF, indexed BAM and BigWig. It compared BigWig samples
with an independent reader, excluded BAM alignment gaps from depth, and checked
the mapped features through cuts, reversals and session restore. Input hashes
matched. Track receipts are under
`build/release-evidence/portable-publication-priority/run-44313825161/published-389-tracks/`.

Thirteen viewport histogram checks and eleven O/E palette checks passed on the
same package. The histogram matched native bin counts at FASTA zoom, distinguished
measured zero from unsupported expected values and preserved source counts when
fitting levels. Refresh took at most 12.5 ms in the exact fixture and 25.5 ms in
the sampled fixture, against the 150 ms gate. Four simultaneous tile transfers
left capacity for interactive queries. The palette suite checked rendered RGBA,
keyboard range changes, separate range memory, session and graph round trips,
and recovery from a failed write. No browser errors were reported. Receipts are
under `build/release-evidence/portable-publication-priority/run-44313825161/published-389-rendering/`.

The exact published package passed 39 sequence and edit checks. GTF/GFF cuts
preserved all bases and source bytes, including on reversed contigs, through
Undo, Redo and session restore. Twelve FASTA measurement checks covered strand
copy, keyboard selection, imperfect repeats and a late job response after
cancellation. Twelve alignment checks verified all ten short-repeat diagonals,
substitution and indel CIGARs, letter-boundary alignment and PNG, SVG and PDF
output. None reported browser errors. Receipts are under
`build/release-evidence/portable-publication-priority/run-44313825161/published-389-sequence/`.

The published package passed 14 file-destination checks and 16 figure-viewer
checks. The destination suite exercised confirmed writes, cancellation, late
changes, remembered folders, save-notification reopening and download fallback.
It checked exact FASTA selection endpoints and session restoration. The viewer
reopened newly exported images and both retained publication previews, fitted
the controls at 640 px, and kept the current preview after cancellation or
invalid input. The browser reported no unexpected errors. Receipts and screenshots
are under `build/release-evidence/portable-publication-priority/run-44313825161/published-389-file-figures/`.

The published package passed 11 background job checks, including eight kinds
of work before the FASTA-only job. FASTA-only opened from Jobs after its setup
dialog closed. Its AGP export represented all 12,017 bases in a single forward
component. A separate controlled TLS Examples replay passed 34 checks, including
numeric stages, cancelling and retrying a running hictk conversion, applying a
FASTA/AGP/track recipe, and restoring frozen provenance without the catalog.
The replay needed a test-only wait for the catalog action to appear before opening
a cached recipe. Both suites completed without browser errors. Receipts are under
`build/release-evidence/portable-publication-priority/run-44313825161/` in
`published-389-background/` and `published-389-example-jobs-recheck/`.

The exact package passed 18 remote-search checks against a controlled provider
with networking restricted to loopback. These covered consent, cancellation,
timeouts, queue bounds, concurrent local search, accepted database names,
report reopening after a map change, and complete TSV export when the JSON
preview is capped. No browser errors were reported. Receipts are under
`build/release-evidence/portable-publication-priority/run-44313825161/published-389-remote-jobs/`.
[Public release](https://github.com/ctlab/HiCT_JVM/releases/tag/latest-jvm-sfx-native)

## Published portable release 2.0.388

GitHub published 17 assets in 1h 02m 53s. The JVM revision is
`f8f7cdc4ede8d0f8a8217964a44a0ed212bd00e0`, with WebUI
`02292b68d8b939c020d57ca36e9a1c86f0231a1d`. All six JVM workflows passed.
Hosted startup passed for both Windows launchers, the Linux runfile and
AppImage, and both macOS architectures.
[Portable Packages run](https://github.com/ctlab/HiCT_JVM/actions/runs/44283392789)

The public manifest matches the Linux runfile and JAR used in the browser tests.
All ten selection checks and all nine pre-soak stages passed, including four
navigation fixtures, structural editing and offline session/export workflows.
The packaged server's Java process received `MALLOC_ARENA_MAX=4` from the launcher.
The 30-minute soak passed after 231 cycles in 1,805.94 seconds. Frame p95 was
16.7 ms and input acknowledgement p95 was 55.69 ms. The fitted server RSS trend
was −4.214 MiB/min, JVM heap 0.102 MiB/min and browser heap 0.048 MiB/min.
DOM node counts stayed at 663 and all ten input files kept their checksums.
There were no browser errors.

The public Linux runfile also passed native clipboard checks. Electron pasted
the exact tab-separated region coordinates and a 722 × 566 PNG. The Hi-C,
FASTA, and AGP controls stayed beside Jobs at 640, 800, 1280, and 1440 px.
Tauri pasted a 1,068 × 844 PNG into an independent Electron window, which
decoded and displayed the image. The runner verified the runfile and JAR
checksums before and after the tests.

The exact public AppImage passed in extract-and-run mode with only loopback
networking. Bundled hictk converted a Cooler and the browser restored the edited
session, exported PNG, SVG and PDF, and displayed the bundled API documentation.
The AppImage and input retained their checksums, Java received the same allocator
default, and the browser reported no errors or external requests.

Receipts are under
`build/release-evidence/portable-publication-priority/run-44283392789/`.
[Public release](https://github.com/ctlab/HiCT_JVM/releases/tag/latest-jvm-sfx-native)

## Published portable release 2.0.387

All six JVM workflows succeeded for `145eafad6992b32800c473d933cd377c60f62263`,
with the portable build pinning WebUI `02292b68d8b939c020d57ca36e9a1c86f0231a1d`.
GitHub published 17 verified assets in 1h 07m 41s. Hosted startup passed for
the Windows command and EXE launchers, the Linux runfile and AppImage, and
both macOS architectures. The macOS 12.3 deployment target audits checked
1,109 Apple Silicon and 1,113 Intel binaries with zero errors.
[Portable Packages run](https://github.com/ctlab/HiCT_JVM/actions/runs/44272191879)

The public Linux checksum manifest matches the runfile that passed ten
Chromium selection workflows. These covered clipboard coordinates and PNG,
typed selection coordinates, and an exact cut in a reversed contig followed
by Undo. The packaged browser passed all nine stages before the soak, including
offline editing and navigation on four fixtures. The 30-minute soak completed
234 cycles at 16.7 ms frame p95 and 54.90 ms input acknowledgement p95, with no
browser errors. The server's RSS grew at 1.029 MiB/min against the 1.0 MiB/min
limit. Linux runfile qualification therefore remains open for the follow-up
launcher.

The exact public AppImage passed its standalone offline check in extract-and-run
mode with only loopback networking. Bundled hictk converted the Cooler fixture.
Chromium preserved edits through closing and restoring a session, exported PNG,
SVG and PDF, and opened the bundled API documentation with no external requests
or browser errors. The AppImage SHA-256 matches the public manifest.

Receipts are under
`build/release-evidence/portable-publication-priority/run-44272191879/`.
[Public release](https://github.com/ctlab/HiCT_JVM/releases/tag/latest-jvm-sfx-native)

## Unpublished portable candidate 2.0.385

Linux and Apple Silicon portable jobs passed packaged startup.
The Windows runner lost its workspace drive while building Tauri. The hosted
workflow was superseded, and the independent Linux qualification continued.
[Portable Packages](https://github.com/ctlab/HiCT_JVM/actions/runs/44263655268)

The exact Linux runfile passed all ten selection checks and all nine pre-soak
qualification stages. The 30-minute Ubuntu container soak completed 233 cycles
with 16.7 ms frame p95, 54.86 ms input acknowledgement p95 and zero browser
errors. Browser heap, JVM heap and DOM counts met their limits. Server RSS
grew at 1.649 MiB/min against the 1.0 MiB/min limit. The full qualification
therefore failed on this build. The Linux runfile SHA-256 is
`9bb78c65af322db1a6d6afbcc2662f97f90044844074ae0ba9625fdafd0564d1`,
and the extracted JAR SHA-256 is
`05ee6490ccc96caa269ed5c5ccb4905fe8fe1622d79301e70ee7881bf964fd13`.

A diagnostic replay used the identical runfile, JAR, fixtures, container and
duration with glibc `MALLOC_ARENA_MAX=4`. All stages passed. Its 30-minute
soak ran 234 cycles, with server RSS growth of 0.333 MiB/min, frame p95 of
16.7 ms and input acknowledgement p95 of 55.04 ms. There were zero browser
errors. The next Linux launchers apply the tested value as a default and honor
an explicit environment override. The default and diagnostic receipts are
under `build/release-evidence/portable-publication-priority/run-44263655268/`.

## Unpublished portable candidate 2.0.383

The exact Actions Linux runfile for `2.0.383-ci1-c29eaf5` passed ten browser
selection checks, including real clipboard reads and typed cuts in a reversed
contig. The coordinate dialog also stayed within a 1280 × 720 viewport and
could be scrolled and submitted by keyboard. The exact AGP was restored by
Undo. Metadata, menu, structural, incomplete-file, offline and four standalone
navigation stages passed.

The 30-minute loopback-only container soak completed 234 cycles with no reported
browser errors. Frame p95 was 16.7 ms and input acknowledgement p95 was 54.85 ms.
Browser, DOM and JVM heap measurements were within their limits. The fitted
server RSS slope was 1.039 MiB/min against the 1.0 MiB/min limit, so the soak
failed. The increase was in anonymous RSS. The `2.0.382` release had passed
on the Linux host, a different execution environment. All 18,130 JVM class
entries matched byte-for-byte between the two JARs. The only other runtime
archive difference was directory timestamps in the embedded JHDF5 payload.
The cause of the RSS trend remains under investigation.

All four platform packagers reached startup. Intel macOS artifact upload
failed on a hosted DNS lookup, so GitHub skipped publication. Receipts and
the RSS comparison are in `build/release-evidence/portable-publication-priority/`
`run-44255574234/`. [Portable Packages run](https://github.com/ctlab/HiCT_JVM/actions/runs/44255574234)

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
