# Platform release checklist

The owner deferred these Windows and macOS GUI checks on 2026-11-11 for future
internal qualification work. Current-source platform CI and artifact receipts
must be recorded before claiming support for this advisor-release candidate;
historical 2.0.0 CI results do not qualify it. Complete this checklist on each
target when desktop access is available. Allow about ten minutes per platform with the small example. Use a
fresh writable data folder and retain the screenshots and logs.

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
