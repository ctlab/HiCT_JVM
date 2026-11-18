# Native browser dependency audit

The Tauri wrapper is delivered code. Audit its exact `HiCT_WebUI/src-tauri/Cargo.lock`
in addition to the JVM runtime and Electron payload. From the WebUI checkout:

```bash
python3 scripts/audit-native-dependencies.py --output ../HiCT_JVM/build/reports/native-osv-audit.json
node scripts/check-native-dependency-patches.mjs
cargo test --release --locked --manifest-path src-tauri/Cargo.toml
```

The Python command requires Python 3.11+ and sends only public crate names/versions
to OSV. It scans every registry entry, including inactive platform/feature and
build dependencies. Local packages are listed separately. A network/incomplete
response fails; findings produce exit 1 for triage, including maintenance notices.
The source verifier runs during ordinary frontend builds and uses the retained
upstream archive without network access. Linux native tests require the normal
GTK3/WebKitGTK SDK; the validated SDK uses Rust 1.88 and Ubuntu 22.04.

## Corrected dependencies

The November 18, 2026 lockfile audit finds 16 flagged registry components.
Reviewed advisory fixes support these compatible updates:

| Component | Before | Selected | Reason |
|---|---|---|---|
| anyhow | 1.0.102 | 1.0.103 | Mutable downcast soundness |
| Tokio | 1.52.3 | 1.53.3 | AsyncFd ownership/safety |
| rustix | 1.1.5 | 1.1.6 | Zero-length xattr buffer initialization |
| serde_with | 3.20.0 | 3.21.0 | Empty KeyValueMap serialization |
| plist | 1.9.0 | 1.10.4 | Selects fixed quick-xml 0.42.0 instead of 0.39.4 |

The Wayland scanner retains quick-xml 0.41.0, which is also fixed. Published
crate checksums/MSRV metadata and a complete optimized Linux build verify the
selected graph with Rust 1.88. The full native suite passes 24 tests, including
strict HTTP ownership, cancellation, byte accounting and failed-write cleanup.

GLib 0.18.5 is the last published compatible GTK3 release. Its string-variant
iterator passes an immutable C out-pointer, causing an optimized regression to
crash with SIGSEGV. The vendored original crate carries exactly the two-line
[upstream fix](https://github.com/gtk-rs/gtk-rs-core/commit/b5a4071e439bef2b5eea76c3aa25e5ae84839e34).
The same regression and complete suite pass after the patch. The original
archive/checksum, 121 source-file comparisons and license/copyright are retained
under `HiCT_WebUI/src-tauri/vendor/`. Cargo selects that exact local patch.
Version scanners may still flag GLib 0.18.5; the source mitigation is explicit.
See [RUSTSEC-2024-0429](https://rustsec.org/advisories/RUSTSEC-2024-0429.html).

## Remaining maintenance notices

The new OSV query completes for 508 registry components. Ten components still
carry **unmaintained** notices; none is hidden or reclassified as a fixed release.
The local application and patched GLib are explicitly outside that query.

| Components | Notice / disposition |
|---|---|
| chrono | RUSTSEC-2026-0543; present in the lockfile but absent from the actual Linux normal/build feature graph |
| proc-macro-error | RUSTSEC-2024-0370; GTK/GLib proc-macro build dependency; track the parent stack's migration |
| unic-char-property, unic-char-range, unic-common, unic-ucd-ident, unic-ucd-version | RUSTSEC-2025-0081/0075/0080/0100/0098; Tauri URL-pattern dependency chain; track upstream replacement |
| zbus_names, zvariant, zvariant_derive | RUSTSEC-2026-0371/0370/0372; D-Bus portal types/macros, merged upstream into zbus 6; track the portal stack's compatible migration |

These are maintenance findings with no vulnerability fix version supplied in
the advisories. Preserve their review separately from soundness/DoS fixes. Recheck
before releases; new vulnerability findings require fresh evaluation. The Linux
feature-graph result does not establish other platforms' reachability.

Receipts, exact upstream advisory JSON, release/MSRV metadata, the failing
optimized test, passing 24-case suite and Linux dependency graph are retained in
`build/release-evidence/2.0.72/` and
`build/release-evidence/native-dependency-update/`. Replacement Windows/macOS
builds and final native/package GUI qualification remain necessary.

## Bundled ModDotPlot dependencies

The portable plotting build pins Pillow 13.0.0, FontTools 4.60.2,
setuptools 84.2.1 and wheel 0.46.3 to address the advisories on the previous
versions. PyInstaller 6.17.0 and altgraph 0.17.5 support the setuptools release
after removal of `pkg_resources`. The OSV query covered 25 exact Python
package versions, including Windows/macOS-specific build requirements, and
returned zero findings. This query covers the Python package inventory.
[Pillow release](https://github.com/python-pillow/Pillow/releases/tag/13.0.0)
· [altgraph release notes](https://altgraph.readthedocs.io/en/latest/changelog.html#id1)
· [PyInstaller release notes](https://pyinstaller.org/en/stable/CHANGES.html)

The pinned Linux release image builds Pillow from its verified source archive
and links checksum-pinned libjpeg-turbo 3.2.1 statically. The build ships JPEG's
license files and records both source digests and the resolved local wheel.
This preserves the glibc 2.17 target. Windows and macOS use their compatible
Pillow wheels. The two frozen Linux ModDotPlot executables and their 376 ELF
files passed the glibc 2.17 audit. The upstream CLI rendered the full and
triangular PNG and SVG figures from a synthetic sequence. The HiCT adapter
returned the two expected identity cells and passed the parallel block check.
The same dependency set rendered PNG, JPEG, SVG, PDF, PostScript and TIFF in a
Matplotlib format check. CI runs the frozen plot test along with the existing
SeqKit and HiCT ModDotPlot identity checks.
[Pillow wheel files](https://pypi.org/project/pillow/13.0.0/#files)
· [libjpeg-turbo source release](https://github.com/libjpeg-turbo/libjpeg-turbo/releases/tag/3.2.1)
