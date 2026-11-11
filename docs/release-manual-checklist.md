# Platform release checklist

The owner deferred these Windows and macOS GUI checks on 2026-11-11 for future
internal qualification work. Automated CI evidence supports the current release
for these platforms. Complete this checklist on each target when desktop access
is available. Allow about ten minutes per platform with the small example. Use a
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

The release validation report records the current automated CI and local Linux
results. These future checks will add platform-specific desktop interaction,
installation and native-dialog evidence.
