# Optional Native Processing Backend

HiCT uses the Java implementation by default. The optional native backend is an
incremental JNI acceleration path for selected tile-processing hot spots; it is
safe to omit from builds and packages.

## Current Native Scope

The current native library offloads:

- base signal preparation for `double` matrix tiles;
- base signal preparation for `long` matrix tiles;
- simple linear-gradient RGBA rasterization.
- dense/sparse stripe-block counting used by `.mcool -> .hict.hdf5`
  conversion.

HDF5 I/O, assembly/scaffold mutations, expected-normalization profile
construction, and full rendering-pipeline graph evaluation still use the Java
implementation. Native conversion/HDF5 hooks should be added only when they can
be parity-tested against the Java converters on representative `.mcool` and
`.hict.hdf5` files; until then conversion commands stay on the proven Java path.

Two binary variants are built on x86-64 toolchains that support them:

- `baseline`: AVX2/FMA/SSE4.2/BMI/BMI2, the common target for Intel Core
  i7-1185G7, Ryzen 7900X, and Ryzen 8500G-class machines;
- `avx512`: baseline plus AVX-512F/DQ/BW/VL.

The runtime loader uses `avx512` only when Linux CPU flags advertise all
required AVX-512 core features. If the variant is missing, unsafe, or fails to
load, HiCT falls back to the baseline native library and then to Java.

## Building

On supported local platforms:

```bash
./gradlew natives describeNativeProcessing
```

`./gradlew natives` builds every native variant supported by the current machine
and compiler. Unsupported platforms/toolchains are skipped without failing the
regular Java build.

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
baseline and AVX-512 libraries in the same process. It verifies deterministic
tile-processing parity before reporting timing.

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
HICT_NATIVE_VARIANT=auto|baseline|avx512
HICT_NATIVE_DISABLE_AVX512=1
```

The WebUI exposes the same runtime switch under `Dev -> Use native code
processing`. If the library is missing or fails, HiCT reports that Java is active
and continues running.

## Threading Model

The native processing functions are stateless. Java worker threads may call them
concurrently. Library loading is synchronized only for the first load attempt;
after that, tile-processing calls use a volatile fast path and do not take a
coarse-grained loader lock.

## Correctness Contract

Native output must match the Java implementation for the offloaded stage. The
optional parity test can be run after compiling the native library:

```bash
./gradlew test --tests '*NativeProcessingServiceTest' \
  -Dhict.native.test.library="$PWD/build/native-processing/resources/natives/linux_64/libhict_native.so"
```
