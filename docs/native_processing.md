# Optional Native Processing Backend

HiCT uses the Java implementation by default. The optional native backend is an
incremental JNI acceleration path for selected tile-processing hot spots; it is
safe to omit from builds and packages.

## Current Native Scope

When native processing is enabled, Java now opens a native backend session and
keeps only the opaque session handle. The session owns native backend state,
variant metadata, and operation counters; all JNI kernels are routed through it.
This is the compatibility layer for the planned native HDF5 resource ownership.
`nativeHdf5BackendAvailable=false` means the session is active but HDF5 files are
still owned by the Java/JHDF5 backend.

The current native library offloads:

- base signal preparation for `double` matrix tiles;
- base signal preparation for `long` matrix tiles;
- post-log tile transform after rendering-pipeline signal preparation;
- simple linear-gradient RGBA rasterization, including a hand-written AVX-512
  mapping loop in the AVX-512 binary;
- observed/expected and expected-only distance normalization for non-segmented
  diagonal profiles;
- dense/sparse stripe-block counting used by `.mcool -> .hict.hdf5`
  conversion;
- sparse stripe-block row-major sorting used by `.mcool -> .hict.hdf5`
  write preparation;
- 1D precomputed-track aggregation used by BED/GFF/GTF/BigWig/cache-backed
  track rendering;
- projected interval aggregation used by BigWig max/mean/sum, BED-style
  coverage, and read-density rendering;
- ASCII FASTA reverse-complement export.

Wholesale HDF5/JHDF5 replacement is intentionally staged. HDF5 file opening,
dataset layout traversal, assembly/scaffold mutations, segmented
expected-normalization profile construction, and full rendering-pipeline graph
evaluation still use the Java implementation. Native conversion/HDF5 hooks are
added only where the native output can be parity-tested against the Java
converters on representative `.mcool` and `.hict.hdf5` files; until then
conversion commands keep the proven Java I/O path and use native kernels only
for isolated hot loops. See `docs/native_hdf5_migration.md` for the file-backend
migration plan.

Two binary variants are built on x86-64 toolchains that support them:

- `avx2`: AVX2/FMA/SSE4.2/BMI/BMI2, the common target for Intel Core
  i7-1185G7, Ryzen 7900X, and Ryzen 8500G-class machines;
- `avx512`: AVX2 plus AVX-512F/DQ/BW/VL.

The runtime loader uses `avx512` only when Linux CPU flags advertise all
required AVX-512 core features. If the variant is missing, unsafe, or fails to
load, HiCT falls back to the AVX2 native library and then to Java.

## Building

On supported local platforms:

```bash
./gradlew natives describeNativeProcessing
```

`./gradlew natives` builds every native variant supported by the current machine
and compiler. Unsupported platforms/toolchains are skipped without failing the
regular Java build. Linux uses `g++`/`clang++`. Windows uses MSVC-compatible
`cl.exe`/`clang-cl` when the command is available in a Visual Studio x64 tools
shell.

OpenMP is disabled by default to avoid adding an implicit runtime dependency to
portable packages. It can be requested for local benchmarking builds:

```bash
./gradlew natives -PnativeOpenmp=true
```

or:

```bash
HICT_NATIVE_OPENMP=1 ./gradlew natives
```

`./gradlew jar` builds the regular JAR and also produces the fat Shadow JAR with
the available native libraries embedded under `natives/<platform>/`.

Legacy single-variant builds are still available:

```bash
./gradlew compileNativeProcessing
```

## Benchmarks

```bash
./gradlew benchmark
```

The benchmark runs one JVM per native variant so JNI symbol binding cannot mix
AVX2 and AVX-512 libraries in the same process. It verifies deterministic
tile-processing parity before reporting timing for the implemented native
kernels.

Reports are written to `build/reports/hict-native-benchmark/`:

- `benchmark.csv` contains requests/sec and mean latency for Java, AVX2 and
  AVX-512 variants.
- `requests_per_second.svg` and `index.html` provide a quick visual comparison.

For quick local smoke checks, reduce the data size and iteration count:

```bash
./gradlew benchmark \
  -Dhict.native.benchmark.rows=256 \
  -Dhict.native.benchmark.columns=256 \
  -Dhict.native.benchmark.iterations=3 \
  -Dhict.native.benchmark.warmup=1
```

## JHDF5 / HDF5 Native Builds

The companion `jhdf5-with-plugins-configuration-snapshot` repository keeps the
dynamic-linking HDF5/JHDF5 build layout required for compression plugins. It now
contains repeatable Linux amd64 variant scripts:

```bash
cd ../jhdf5-with-plugins-configuration-snapshot/source/c
curl -L -o CMake-hdf5-1.10.11.zip \
  https://support.hdfgroup.org/ftp/HDF5/releases/hdf5-1.10/hdf5-1.10.11/src/CMake-hdf5-1.10.11.zip
JAVA_HOME=/path/to/jdk ./build_linux_amd64_variants.sh generic avx2 avx512
```

The script deploys to `libs/native/jhdf5/amd64-Linux-generic`,
`libs/native/jhdf5/amd64-Linux-avx2`, and
`libs/native/jhdf5/amd64-Linux-avx512`. The generic build is the portable amd64
fallback. The AVX2 and AVX-512 builds are intentionally separate binary sets:
they must only be loaded on CPUs with the matching instruction-set support.
The Linux scripts build and deploy the compression plugins from the HDF5 plugin
archive first; legacy plugin copies are a fallback only when plugin targets were
not produced by the current build.
Windows amd64 has an analogous `build_windows_amd64_variants.ps1` entry point
for Visual Studio x64 Native Tools shells.

## Runtime Selection

Native processing is disabled unless explicitly requested:

```bash
HICT_NATIVE_PROCESSING=1
```

or:

```bash
-Dhict.native.processing=true
```

Optional library overrides:

```bash
HICT_NATIVE_LIBRARY_PATH=/absolute/path/to/libhict_native.so
HICT_NATIVE_LIBRARY_DIR=/directory/containing/mapped/library/name
HICT_NATIVE_VARIANT=auto|avx2|avx512
HICT_NATIVE_DISABLE_AVX512=1
```

`HICT_NATIVE_VARIANT=baseline` is still accepted as a legacy alias for `avx2`.

The WebUI exposes the same runtime switch under `Dev -> Use native code
processing`. If the library is missing or fails, HiCT reports that Java is active
and continues running.

## Threading Model

Native processing uses one process-local backend session per JVM. Java worker
threads may call the session concurrently; implemented kernels do not mutate
per-call shared data except relaxed atomic operation counters. Library loading
and session creation are synchronized only for the first attempt. The session is
kept alive for the process lifetime so worker threads are never racing with a
native-state destructor while tile work is in progress.

## Correctness Contract

Native output must match the Java implementation for the offloaded stage. All
native calls are optional: a missing native library, rejected input, or native
failure disables native processing and returns to Java for the current process.
The optional parity test can be run after compiling the native library:

```bash
./gradlew test --tests '*NativeProcessingServiceTest' \
  -Dhict.native.test.library="$PWD/build/native-processing/resources/natives/linux_64/libhict_native.so"
```
