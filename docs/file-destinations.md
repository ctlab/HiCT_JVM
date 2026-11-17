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

For FASTA/AGP, writable browser handles consume the fetch response as a byte
stream. Each disk write completes before another chunk is pulled. Neither an
assembly-sized ArrayBuffer nor a Blob is created on this path. An AbortController
cancels preparation/transfer from the export notice. Errors cancel the response,
abort the writer and preserve the previous successful destination memory. Saved
is reported only after writer close. HTTP failures and unexpected content types
are rejected before opening a writer, without buffering the error body.

The export uses one promise-managed notice for progress and the final outcome,
recorded once in the notification journal. Cancel becomes Close after completion.
The installed notification library appends reused IDs and can loop layout during
manual dismissal/replacement. The export integration avoids both operations;
its lifecycle has focused and real-browser regression coverage.

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

Current Electron windows expose a request-transfer capability for FASTA/AGP.
The renderer sends only the endpoint, JSON options and captured history headers.
The main process opens the native chooser first, then fetches and writes chunks
sequentially to a staged sibling file. It validates the existing trusted-local
host policy and export endpoint allowlist, rejects redirects/error content, and
never sends the file bytes through renderer IPC. Older wrappers without this
capability retain their compatibility path. Tauri 0.1.2 provides the same
request-only path through its Rust HTTP client, with 64-KiB sequential disk writes,
normal TLS validation, no redirects or automatic retries, bounded idle waiting
and checks of the response type and complete byte count. It forwards only the
captured history headers and JSON options to the three allowed export endpoints.
Sequence bytes do not pass through the WebView.
Operation IDs use 128 random bits from `Crypto.getRandomValues`, which supports
HTTP contexts; this avoids adding a secure-context-only UUID dependency to the
trusted-LAN workflow ([API reference](https://developer.mozilla.org/en-US/docs/Web/API/Crypto/getRandomValues)).

Electron permits one coordinate transfer per window. Tauri permits at most four
active staged exports in the application, each owned by its choosing window. Cancellation requires
the matching window and operation ID. Electron navigation, renderer loss and destruction abort owned work. Tauri
user cancellation, native window-manager close and graceful application exit
remove owned files; navigation and renderer-process-loss qualification remain
separate gates. Graceful quit cancels transfers and waits for their cleanup;
an unanswered chooser owns no file and its aborted signal prevents later I/O.
Rename commits the file; cancellation after that point cannot turn a completed
save into a cancelled result. Permission and storage errors have concise messages;
the preload removes IPC method-name wrappers. Streamed native results do not
advertise browser readback. Sessions and locally generated figures still retain
their confirmed Blob readback capability.

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
injected rename failure. The Tauri filesystem implementation has independent native tests. Its complete
Linux wrapper is also compiled with Rust 1.88 against Ubuntu 22.04/WebKit 4.1;
real GTK dialog and transfer evidence is described below.

Nine acceptance checks pass with the built Linux Electron 41.10.7 payload and
real GTK save/cancel dialogs on a private X display, including 512 Mb FASTA,
selected FASTA, AGP, permission failure, cancellation, navigation, last-window
cleanup and session save/readback. A throttling proxy forwards actual JVM data
and preserves the journal/cancellation response headers. The direct desktop
transfer creates no renderer coordinate-download requests. Other platforms,
Wayland/portal variants, hard-crash durability and full installer integration
remain unqualified.

The host itself has Rust 1.75 and no WebKit 4.1 SDK. The isolated validation
SDK supplies Rust 1.88, WebKit 4.1 and tauri-driver; this does not change the host
or add those test dependencies to the application. See
[the native validation instructions](tauri-native-validation.md).

Tauri 0.1.2 was exercised with actual GTK save/cancel/overwrite dialogs, a 512 Mb
FASTA, AGP, an 8-MiB full destination volume, session save/restore, multi-chunk
SVG/PNG/PDF saves, user cancellation and graceful Quit/window-manager close.
The 518,400,014-byte FASTA matches the retained independent sequence checksum;
the final development binary saved it in 3.04 seconds on this host, with
17-ms frame p95. This is a development JVM/WebUI/native pair, not final installer
qualification. Automated WebKit suppresses HTML file-open choosers, so actual
saved session/SVG files were supplied through its file-input protocol. Its
DELETE /window operation left a partial file even with the application exit
guard; it must not be reported as equivalent to the passing native
WM_DELETE_WINDOW close. Hard process termination/orphan recovery, other
platforms and portal variants remain outside this qualification.
Whole-Blob buffering in older Electron/Tauri and download-only browsers
(including Tauri's per-byte array conversion) is not qualified for large FASTA.
Browser streaming is qualified on a 512 Mb synthetic assembly with
an independent sequence checksum and Chromium's actual private-filesystem
writable stream in a persistent profile. Only the chooser is substituted; the
large backend response is fetched directly. Cancellation and injected write
failure abort the real writer and retain the old output.
This qualifies the browser transfer integration, not OS-native file dialogs or
genome-scale behavior across all browsers. Tauri/download streaming, broader
bounded-memory qualification, all secondary-source/chooser routes,
large directories, accessibility and restart behavior remain P3 work. No release
readiness claim follows from these primary workflow checks.
