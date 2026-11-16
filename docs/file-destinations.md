# File destinations and confirmed saves

The shared WebUI `FileSave` service distinguishes `saved`, `cancelled` and
`download-started`; failed writes reject. Sessions, assembly/selection FASTA,
AGP, PNG/SVG/PDF, bookmarks, presets and pipeline JSON use this service.

Browser writable-file pickers are invoked directly from the initiating click,
before asynchronous preparation, to retain user activation. A denied or failed
picker never silently falls back to downloading. Only a successful writer close
confirms a save; a write/close error attempts abort and preserves the error.
Successful handles are reused as start locations per export purpose during the
page lifetime. Stable purpose IDs allow the browser to retain its own chooser
history. Session opening uses the same purpose where that API is available.

Without the writable-file API, the ordinary browser download starts and the
notification explains that the browser controls the destination. HiCT cannot
confirm its completion and never marks the assembly saved from this outcome.
The unsaved-edits dialog stays open. Cancellation stays quiet; failures in that
dialog appear inline, avoiding a second modal behind the first.

Session preparation captures client state and the applied history identity before
requesting the guarded server snapshot. A confirmed write marks only that
captured history entry saved. Newer edits remain dirty; changing the open map
does not mark the replacement saved. The success action passes the saved file
through normal session restore and its unsaved-edits protection. Browser handles
read the saved file back; desktop adapters retain the successfully written Blob
for that action, rather than rereading later disk changes.

Electron and Tauri retain separate successful directories per purpose. They write
a uniquely named sibling temporary file, synchronize and close it, then rename
it over the chosen target. Failure cleans up owned temporary files and does not
advance remembered directories. This protects the previous file against a failed
write. Filesystem crash durability and cross-platform replacement behavior are
separate qualification requirements.

Server-side input selectors retain directory and filename per server/purpose in
local settings. Reopening verifies the folder and preselects the file only if it
is still visible. A missing folder falls back to the root. Browsing and dismissal
do not replace the last confirmed choice. Selection still requires a Load click.
Local session pickers and server-relative input selectors address different
filesystems and intentionally keep separate state.

FASTA/AGP requests capture the displayed history revision before a chooser can
delay submission. The server validates it on the assembly queue and freezes
records/layout before dispatching large sequence I/O to the export pool. Later
edits cannot change the frozen interval. Cancellation returns 409 instead of a
successful empty file. See [jvm_api_v1.md](jvm_api_v1.md).

## Qualification limits

Chromium workflow checks use controlled writable handles to exercise cancellation,
denial, failed close, delayed close, actual session restore and stale coordinates.
Other browser regressions use a controlled writer that emits real downloads so
the output bytes can be inspected. These checks do not qualify OS-native dialogs.
Electron's actual IPC handler is exercised against real temporary files, including
injected rename failure. The actual Tauri filesystem function is compiled and
tested independently; this does not qualify the complete Tauri wrapper.

The audit host has Rust 1.75, which cannot read the wrapper's v4 Cargo lockfile;
the project requires Rust 1.88. Full wrapper compilation and native GUI checks on
each advertised platform remain required. Whole-Blob buffering and desktop IPC
(including Tauri's per-byte array conversion) are not yet qualified for large
FASTA files. Streaming, bounded memory, all secondary-source/chooser routes,
large directories, accessibility and restart behavior remain P3 work. No release
readiness claim follows from these primary workflow checks.
