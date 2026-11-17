# Linux Tauri native export qualification

Tauri 0.1.3 uses request-only Rust HTTP downloads for coordinate FASTA/AGP and
64-KiB acknowledged binary IPC for locally generated Blobs. Choose a destination
before starting an HTTP request. Keep the original destination until an fsync and
rename succeed. Export IDs are owned by the choosing window; cancellation,
window-manager close and application exit remove staged files before a graceful
exit. A late file-dialog answer cannot start a write after shutdown. Legacy
wrappers keep their explicit compatibility path.

The complete native wrapper needs Rust 1.88 and the platform WebView SDK. The
portable Linux executable requires WebKit 4.1, GTK 3 and OpenSSL 3. The payload
manifest lists these libraries and distribution-specific installation hints.
No test driver, X server, compiler or test sandbox override is included in the
production payload.

## Build and native unit tests

`scripts/portable/tauri-validation.Dockerfile` is the validation SDK used on this
host: Ubuntu 22.04, Rust 1.88, WebKit 4.1 and tauri-driver 2.0.6. The built image
used here is `sha256:20611bc911ea8371adf3b6f2492527e227d1bd8617f24f71a3ec4350b91922bf`.
A validation-only writable Cargo cache/target avoids changing host Rust or its
registry. The payload builder runs the isolated file-writer contract and the
complete `cargo test --release --locked` suite before building the executable.

Example from the JVM checkout, with adjacent WebUI and an existing compatible
host Node 24 binary (the Ubuntu base's distribution Node is too old):

```bash
docker build -f scripts/portable/tauri-validation.Dockerfile -t hict-tauri-validation:rust188-ubuntu22 .
mkdir -p /tmp/hict-tauri-validation/cargo /tmp/hict-tauri-validation/target
docker run --rm --user 1000:1000 \
  -v /home/tux/Documents/HiCT/HiCT_WebUI:/home/tux/Documents/HiCT/HiCT_WebUI \
  -v /tmp/hict-tauri-validation:/tmp/hict-tauri-validation \
  -v /usr/bin/node:/usr/local/bin/node:ro \
  -e CARGO_HOME=/tmp/hict-tauri-validation/cargo \
  -e CARGO_TARGET_DIR=/tmp/hict-tauri-validation/target \
  -e CARGO_BUILD_JOBS=2 \
  -w /home/tux/Documents/HiCT/HiCT_WebUI \
  hict-tauri-validation:rust188-ubuntu22 \
  node scripts/build-tauri-browser-payload.mjs --platform linux_x86_64 \
  --output /tmp/hict-tauri-validation/browser-payload
```

Use the actual workspace paths and UID/GID when reproducing elsewhere. Build the
WebUI first. Snapshot its `dist` and the built executable before starting GUI
checks; subsequent builds must not replace executing binaries or served assets.
The validation SDK is not the shipping container or a replacement for the
normal platform payload builders.

## Real native GUI test

`check_tauri_coordinate_exports.py` uses Python's standard library and W3C
WebDriver, actual WebUI menu clicks, GTK save dialogs, real filesystem writes
and native X11 window-manager close events. Its explicit prerequisites are:

- A disposable JVM/WebUI pair with a **physical copy** of a single-record FASTA
  of at least 16 Mb, already open and linked. The recorded run uses 512 Mb;
  original user datasets must not be edited or linked through symlinks.
- A private Xvfb display at 1600×1100, tauri-driver/WebKitWebDriver, GTK desktop
  portal and a real named non-root user inside the SDK container. Export and
  profile directories are private, writable and mounted at the same host paths.
- The executable snapshot accessible inside that container; its WebUI URL set
  through `HICT_TAURI_URL`, and `TAURI_AUTOMATION=true` for the test driver only.
  Keep the application's existing bind/CORS policy. The test connects to the
  actual API using Help → Connection before attaching to the open session.
- A JSON environment receipt with `api`, `web`, `webdriver`, `display` and
  `nativeSha256`, plus artifact hashes/scope. In these runs this is written by
  the disposable pair supervisor. The optional full-volume test uses an 8-MiB
  tmpfs owned by that user, mounted at `/tmp/hict-tauri-full-volume`.

Run with a fresh output directory inside the same mounted trial directory:

```bash
python3 scripts/check_tauri_coordinate_exports.py \
  --environment /tmp/hict-tauri-streaming-native-04/environment.json \
  --container hict-tauri-gui-04 \
  --binary /tmp/hict-tauri-streaming-native-04/native-bin/hict-tauri-browser \
  --source-fasta /tmp/hict-tauri-streaming-native-01/data/assembly.fa \
  --output /tmp/hict-tauri-streaming-native-04/native-coordinate-qa-01 \
  --full-volume /tmp/hict-tauri-full-volume
```

The runner owns its newly created WebDriver session. Close any earlier session
before starting it. It records intermediate results/session identity and the
final qualification or failure receipt. It checks chooser cancellation,
confirmed AGP overwrite, full FASTA sequence identity and responsiveness,
full-volume preservation, session save/restore, user cancellation during
transfer, File → Quit and native WM_DELETE_WINDOW close during transfer. It
never submits BLAST queries or changes assembly structure.

Use ImageMagick root-window screenshots for actual desktop visual inspection:
WebDriver screenshots can omit composited WebKit images and icon fonts. Inspect
saved bytes independently. An HTML file-open chooser is suppressed by automated
WebKit; the session restore check supplies the **actual saved file** to the file
input. That check establishes restoration, not manual open-dialog behavior.

## Recorded scope and negative controls

`/tmp/hict-tauri-streaming-native-01` is the bounded-IPC-only red control: WebKit
still buffered the FASTA response, with about 1.49 GiB process-group RSS growth.
Do not qualify large coordinate exports from binary IPC alone.

Native HTTP trials 02/03 use an immutable version-62 JVM with copied new WebUI
assets and a new native wrapper: a **development pair**, not a final installer.
Trial 02 saved 518,400,014 bytes in 2.85 s, with 18.2 MiB additional RSS from the
chooser baseline, 17-ms frame p95 and 5.7-ms input acknowledgement p95. Trial 03's
final development executable is SHA-256
`0a9570bc260fd78455b3e247ccc986f2fd5add8e054a89bb40d71000e21553b9`;
it saved the same bytes in 3.04 s with 17-ms frame p95. The output file SHA-256 is
`8768c249260c53c2f10c479415672f5459a3868ca5dc631d29431c02b2dc3129`;
the independently read 512,000,000 sequence bases hash to
`1e077476b6d3eaf57a98b4ac46ed60aed534a4374e8d631c61d346b871d38858`.
These are host observations, not portable latency guarantees.

An initial trial encountered real insufficient space while the JVM was spooling
its FASTA and the chosen destination was being written. The original survived
and temporary files were removed. Repeating after verified duplicate-artifact
cleanup succeeded. Do not interpret that failed transfer as a successful save.

The WebDriver `DELETE /window` operation left a partial file in both prototypes,
even after adding an application `ExitRequested` cleanup guard. Native
WM_DELETE_WINDOW and File → Quit passed independently. Do not equate the
WebDriver operation with graceful window-manager close or claim hard-crash
orphan recovery. Interrupted/failed harness attempts remain recorded alongside
successful repeats; a corrected notification observer ignores late attach
notices rather than mistaking them for the export result.

Final packaged runtime/artifact identity, broader real map/track native workflows,
other platforms, Wayland/portal variants and hard-crash durability remain separate
release qualifications. Existing Chromium/Electron and publication-artwork
receipts retain their original exact version and scope.

## Page ownership and launcher termination follow-up

Tauri 0.1.3 adds page-generation cancellation, a per-window chooser guard and
Unix signal handling. Navigation invalidates the old generation immediately,
without waiting for the disk mutex. A queued cleanup removes only exports from
that generation. A chooser captures its generation before opening, so a late
answer cannot create a file for a replaced page. The chooser guard also rejects
overlapping IPC retries while the original dialog is pending. SIGTERM/SIGINT
enter the awaited application-exit cleanup path; the Unix launcher uses SIGTERM
and allows two seconds before forcing termination. Legacy byte-array clients now
use the same registered staged writer; their renderer transport remains buffered.

`check_tauri_lifecycle.py` reuses the coordinate runner's private-display setup:

```bash
python3 scripts/check_tauri_lifecycle.py \
  --environment /tmp/hict-tauri-streaming-native-08/environment.json \
  --container hict-tauri-gui-08 \
  --binary /tmp/hict-tauri-streaming-native-08/native-bin/hict-tauri-browser \
  --source-fasta /tmp/hict-tauri-streaming-native-01/data/assembly.fa \
  --output /tmp/hict-tauri-streaming-native-08/lifecycle-qa-01
```

The four cases are a paused 64-KiB native Blob followed by document refresh, a
real chooser answered after refresh, refresh during a real FASTA transfer, and
SIGTERM during a real FASTA transfer. They pass on native SHA-256
`a73cfa59c532d89c29f8218caab52415b85ecb21827455cdd7b87b1cc2c7749c`.
HTTP refresh/termination observe 20,242,432/6,438,912 staged bytes and clean up
in 0.078/0.111 s. This is a development pair with immutable version-63 JVM/WebUI,
not yet the updated final package.

The same native executable passes 23 Rust tests, eight ordinary native-coordinate
cases, 17 integrated real-map/figure/reset checks and nine legacy IPC checks.
The latter uses actual retained SVG/PNG/PDF bytes, including the 13-MB whole-map
SVG, through the older save command. The real-map harness injects its controlled
command page; its capture/compare results establish rendering and native I/O,
not unmodified startup navigation. Ordinary/lifecycle runners use the actual
WebUI. Full WebUI build and lock audit pass; all previously locked dependency
versions/checksums remain unchanged.

Negative controls remain under trials 05 and 07: version 63 leaves 64 KiB staged
after Blob refresh and an 87-MB FASTA after SIGTERM; the first lifecycle prototype
allows Tauri's interrupted custom-protocol request to retry through postMessage
and open another chooser. The final chooser guard closes that observed race.
SIGKILL, renderer hard crashes and platform variants remain separate gates.
