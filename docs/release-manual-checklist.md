# Platform release checklist

The owner deferred these Windows and macOS GUI checks on 2026-11-11 for future
internal qualification work. Current-source platform CI and artifact receipts
must be recorded before claiming support for this advisor-release candidate;
historical 2.0.0 CI results do not qualify it. Complete this checklist on each
target when desktop access is available. Allow about ten minutes per platform with the small example. Use a
fresh writable data folder and retain the screenshots and logs.

## Final release acceptance

Record the result and attach evidence for the exact artifact. The Windows and
macOS desktop rows are pending the owner's run.

| Step | What to verify in the packaged app | Windows | macOS |
|---|---|---|---|
| B1 | Launch with one port and confirm the WebUI and API share an origin. Verify the legacy two-port configuration separately. | NOT TESTED | NOT TESTED |
| B2 | Shift-click and Shift-drag whole scaffolds and the contigs between them. Confirm Orlando chrX stays exact through Reverse, Undo and Redo. | NOT TESTED | NOT TESTED |
| B3 | Click the first and last translocation triangles. Confirm the selection moves to each end and View can hide the zoom controls. | NOT TESTED | NOT TESTED |
| B4 | Change the selection and check that the ribbon controls stay in place. | NOT TESTED | NOT TESTED |
| B5 | Generate both kinds of dotplot using Finest detail and Custom sizes. Verify storage estimates and disk-limit feedback. | NOT TESTED | NOT TESTED |
| B6 | Add a bookmark and confirm it appears first and is highlighted. Pan a focused, unlocked map with the arrow keys. | NOT TESTED | NOT TESTED |
| B7 | Re-test the Windows cases below. Verify FASTA retry, file-picker contrast and focus restoration on the desktop. | NOT TESTED | NOT TESTED |
| B8 (now B16) | Confirm release notes link to the exact platform downloads and include the compatibility and re-test notes. | NOT TESTED | NOT TESTED |
| B9 | Open the contact map with the operating system set to dark mode. The workspace stays white. | NOT TESTED | NOT TESTED |
| B10 | Convert a map. Check elapsed time, ETA and the log. Verify both automatic and manual opening. | NOT TESTED | NOT TESTED |
| B11 | Prepare square-root coverage on a large contact map. Reopen to reuse it, return to a visited zoom and verify its colour limits. | NOT TESTED | NOT TESTED |
| B12 | Generate two regions at different resolutions in one result set. Check transparency where detail is missing and reverse a contig to verify projection. | NOT TESTED | NOT TESTED |
| B13 | Show, hide and go to a result region. Save and reopen the set with the original map and verify the other-map error. | NOT TESTED | NOT TESTED |
| B14 | Add a Log-scale curve point and verify the inflection. Change Slider scale without changing colours and try Reset curve. | NOT TESTED | NOT TESTED |
| B15 | Narrow the histogram view. Type bounds, wheel-zoom and pan. Fit levels preserves the manual view and Whole data restores Auto. | NOT TESTED | NOT TESTED |

| Windows case | What to try | Result |
|---|---|---|
| F1 | Convert a valid MCOOL with a Unicode path. | NOT TESTED |
| F2 | Generate ModDotPlot from a FASTA with a Unicode path. | NOT TESTED |
| F3 | Launch from a Unicode working directory and verify that an early exit is reported as a failure. | NOT TESTED |
| F5 | Make an assembly edit in another client, then retry the export preview. | NOT TESTED |
| F8 | Generate ModDotPlot from a deeply nested portable installation. | NOT TESTED |

## Evidence record

| Field | Value |
|---|---|
| Tester and date | Pending |
| Operating system and architecture | Pending |
| JVM revision | Pending |
| WebUI revision | Pending |
| CI run and platform job URL | Pending |
| Artifact filename and SHA-256 | Pending |
| Result and evidence directory | Pending |

## Installation and first use

- [ ] Verify the published checksum and extract or start the platform package.
- [ ] Launch using the normal executable or wrapper and select the fresh folder.
- [ ] Start the server. Confirm the browser opens and About displays both versions and the WebUI commit.
- [ ] Open Help, Examples and the Borrelia card's Open example button. Confirm a readable 19-replicon map and linked FASTA.
- [ ] Confirm the bundled tool status lists conversion, alignment and sequence tools.

## Edit and preserve

- [ ] Zoom and pan. Confirm ruler positions and readable contact signal.
- [ ] Select a contig, Reverse, Undo and Redo. Confirm its orientation changes each time.
- [ ] Split a contig, Undo, and save a session.
- [ ] Close the map, restore the session and confirm the saved layout and linked sequence.
- [ ] Export AGP and a viewport PNG, SVG and PDF. Open each file independently.

## Tools, offline use and shutdown

- [ ] Run a short exact sequence search and open its resulting track from Jobs.
- [ ] Run a small alignment or identity job and confirm its result is visible.
- [ ] Disconnect networking and reopen the local example. Open API docs and export an image.
- [ ] Start a job, then Stop or Quit the Launcher. Confirm the server and its native children exit.
- [ ] Restart. Confirm Jobs contains no running work from the prior launch.

## Deferred internal follow-up

| Target | Status | Requested follow-up |
|---|---|---|
| Windows x86-64 | Deferred by owner on 2026-11-11 | Complete the interactive checklist and attach tester, revision, checksum and evidence records |
| macOS arm64 | Deferred by owner on 2026-11-11 | Complete the interactive checklist and attach tester, revision, checksum and evidence records |
| macOS x86-64 | Deferred by owner on 2026-11-11 | Complete the interactive checklist and attach tester, revision, checksum and evidence records |

## Hosted startup coverage

The `2.0.371` Windows job launched the portable executable and checked
hictk, minimap2 and the AVX2 aligner. The optional AVX-512 check produced an
illegal-instruction warning on its runner. The Apple Silicon job executed the
packaged launcher, discovered its bundled tools and inspected its signature
and the Tauri browser signature.
[Windows job](https://github.com/ctlab/HiCT_JVM/actions/runs/44215590944/job/134510476168)
· [Apple Silicon job](https://github.com/ctlab/HiCT_JVM/actions/runs/44215590944/job/134520940633)

The package scripts now start each hosted launcher with disposable data and
loopback-only listeners. The startup check waits for both the API and WebUI,
verifies the backend version and the selected WebUI revision from the served
JavaScript, and stops the test process. It covers the Windows ZIP entrypoint,
custom Windows EXE, both signed macOS launchers, Linux runfile and AppImage.
Both exact `2.0.371` Linux artifacts passed locally, with the AppImage
starting in extract-and-run mode in a network-isolated container. A wrong
expected WebUI revision failed the check. The hosted `2.0.382-ci1-0c483e2`
jobs confirmed successful API and WebUI startup for both Windows launchers
and both macOS architectures. The interactive checklist remains deferred.
[Windows job](https://github.com/ctlab/HiCT_JVM/actions/runs/44241313376/job/134595372604)
· [Apple Silicon job](https://github.com/ctlab/HiCT_JVM/actions/runs/44241313376/job/134595105195)
· [Intel macOS job](https://github.com/ctlab/HiCT_JVM/actions/runs/44241313376/job/134595651054)

The current candidate and local Linux evidence are recorded in
[release-qualification.md](release-qualification.md). Future checks must identify
their exact revisions and artifacts and record platform-specific desktop
interaction, installation and native-dialog evidence.

## Candidate desktop qualification

Use a clean browser profile and copies of the same inputs in each engine.
Record the artifact SHA-256 and both commits from About and `/version`. Repeat
launch from a writable path containing spaces and non-ASCII characters. On
Windows record the WebView2 version. On Linux record the display server,
desktop portal, and distribution. On macOS record the version of the OS and
whether the engine is native to the machine's architecture.

| Case | Expected behavior |
|---|---|
| Startup and provenance | The launcher starts the viewer and API on one port. About and the API report the same real JVM and WebUI commits as the package receipt. |
| Map and sequence | Open Examples, then open a native map or convert a pairs file. Link the matching FASTA. Use the FASTA chip to inspect its name, zoom to letters and change the alignment minimum. |
| Contrast and layers | Display two sources with distinct colors. Check exact limits across zoom and assembly edits. Square-root coverage remains readable. Observed and O/E each retain their own limits. |
| Editing and regions | Reverse, group, move and split. Use toolbar Undo and Redo. Cancel removal of all scaffolds and verify the AGP is unchanged. Select coordinates, check centering, resize a region and test Square selection on subsequent drags. |
| Tracks | Resize a dense BED track on both axes. Verify wheel zoom and turn on track scrolling in View to scroll the track. Verify strand direction, item colors, and readable signal ticks. |
| Analysis | Generate ModDotPlot and an alignment dotplot from small regions. Use Finest detail and Custom sizes. Confirm a result opens as a source. Try the signal histogram and local sequence search. |
| Guidance and errors | Request FASTA export without a linked FASTA. Verify guidance appears before the save picker. Exercise failed AGP import and a failed job. Check Details and Show log. |
| Guide and options | Open Help and read the bundled guide offline. Open each Advanced section, check its options, and reopen it to verify the remembered state. |
| Selection export | Open Edit, Copy, Export and Analyse with the keyboard. Export a region in three formats. Verify the cost warning with an expensive selection and cancel. Clipboard output is PNG. |
| Native export | Save viewport and whole-map PNG, SVG and PDF. Also test zoomed and isolated views, spaces and non-ASCII paths, cancel, overwrite decline, approved overwrite and denied destinations. Compare pixels and check editable vector labels in a viewer. |
| Figures | Verify physical sizes at single- and double-column widths. Preview rulers, annotations, map layers and snapshots. Check label legibility, the smallest-text reading and the warning below 5 pt. |
| Settings reset | Cancel and confirm browser reset in each wrapper. Verify the actual storage state on the app origin, including localStorage, cookies, Cache Storage and IndexedDB. Test a failed start before reset. |
| Launcher Stop | Start a job and stop HiCT from the Launcher. Check child processes exit and disposable job files are cleaned up. Open console logs for the stopped session, then restart. Repeat on Windows. |

Fill in each engine's result with **PASS**, **FAIL**, or **NOT TESTED** and
attach evidence for the exact artifact. Hosted startup checks have their own
receipts and do not substitute for these interactive cases.

### Linux x86-64

| Engine | Result | Evidence |
|---|---|---|
| Tauri (WebKitGTK, X11 or Wayland) | NOT TESTED | |
| Electron (X11 or Wayland) | NOT TESTED | |
| System browser | NOT TESTED | |

### Windows x86-64

| Engine | Result | Evidence |
|---|---|---|
| Tauri (WebView2) | NOT TESTED | |
| Electron | NOT TESTED | |
| System browser | NOT TESTED | |

### macOS arm64

| Engine | Result | Evidence |
|---|---|---|
| Tauri (WKWebView) | NOT TESTED | |
| Electron | NOT TESTED | |
| System browser | NOT TESTED | |

### macOS x86-64

| Engine | Result | Evidence |
|---|---|---|
| Tauri (WKWebView) | NOT TESTED | |
| Electron | NOT TESTED | |
| System browser | NOT TESTED | |
