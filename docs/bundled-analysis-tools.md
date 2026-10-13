# Building and checking the sequence tools

`./gradlew natives` now prepares **both** the HiCT JNI libraries and the complete
external toolchain for the current platform. `./gradlew clean build natives jar`
waits for this preparation before copying resources into the JAR. The first
source build is substantial: it builds hictk, minimap2, mm2-plus, SeqKit and the
HiCT sequence-analysis adapter, plus BLAST+ on Linux x86_64. Later runs reuse the
payload only if the build inputs, required commands and every file hash match.
Previous payloads and interrupted staging directories are retained.

The external-tool stage contains build scripts and dependency metadata, not a
Gradle checkout. Linux marks `gradlew` executable only for the JNI and fat-JAR
targets. CI exercises all three tool targets in this scripts-only layout with
real Docker bootstrap and small executable probes, in addition to the full
compiler and packaged-tool checks. This catches staging failures even when a
developer machine already has a valid native-tool cache.

Linux requires Docker and Python 3.10+ on the host. Tool builds run inside
manylinux2014 with their own Python 3.12 and pinned Go toolchain. Windows/macOS
require Python 3.12, Go 1.25.5 and the C/C++ build dependencies used by the
corresponding portable-release jobs. `HICT_TOOL_BUILD_PYTHON` selects the host
Python executable. Reserve ample disk space for compiler dependencies and source
builds (20 GiB or more recommended). Source-only Java tests and `shadowJar` do not
force an external-tool source build; use `natives shadowJar` for a complete local
application. Missing prerequisites fail the build rather than silently dropping
a feature. `compileNativeProcessing*` remains available for JNI-only development.

CI builds and merges sequence payloads **before** packaging, including macOS.
`scripts/toolchains/verify_packaged_tools.py --jar PATH` verifies all listed
files inside the actual JAR and starts the extracted tools using a fresh native
cache, with developer overrides removed. Release jobs run this check on their
native OS. SeqKit and the identity/alignment overlay adapter are required on all
four release platforms. Bundled BLAST is currently Linux x86_64 only; on other
platforms a compatible local BLAST can be selected with `HICT_BLASTN_BIN`.

`prepare_bundled_tools.py --seal` is intended for CI after its native build jobs
have produced and checked the payload. It verifies completeness and hashes and
records the current source key. It is not a way to establish the provenance of
an arbitrary downloaded binary.

# Selection analysis

Tools → Generate dotplot from active assembly uses minimap2/mm2-plus for
alignment. Tools → Generate ModDotPlot uses k-mer sketches and identity scores.
Both share a sidecar writer and source-bin mapping implementation; its executable
retains the name `hict-moddotplot` for compatibility. An alignment job dispatches
to the alignment kernel; it does not calculate ModDotPlot identities. The older
FASTA-file dotplot workflow remains separately available in Tools.

The analysis dialogs check the map, linked FASTA and each engine before enabling
submission. A missing FASTA explains **Assembly → Link FASTA**. Server validation
also retains the detailed explanation in the HTTP JSON response, including for
scripts that bypass the UI. Expected prerequisite errors are not server crashes.

The attachment choice is **Overlay**, **Below diagonal**, or **Do not link now**.
Overlay blends on both triangles; below-diagonal leaves Hi-C above. Attaching
replaces the secondary source and starts a new journal, as described in the
modal. Saving without attaching preserves the current display and journal.
