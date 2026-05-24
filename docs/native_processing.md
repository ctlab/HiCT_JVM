# Optional Native Processing Backend

HiCT uses the Java implementation by default. The optional native backend is an
incremental JNI acceleration path for selected tile-processing hot spots; it is
safe to omit from builds and packages.

## Current Native Scope

The current native library offloads:

- base signal preparation for `double` matrix tiles;
- base signal preparation for `long` matrix tiles;
- simple linear-gradient RGBA rasterization.

HDF5 I/O, assembly/scaffold mutations, expected-normalization profile
construction, and full rendering-pipeline graph evaluation still use the Java
implementation.

## Building

On supported local platforms:

```bash
./gradlew compileNativeProcessing describeNativeProcessing
```

To bundle the compiled native library into Gradle resources:

```bash
./gradlew -PincludeNativeProcessing=true processResources
```

The equivalent environment flag is:

```bash
HICT_INCLUDE_NATIVE_PROCESSING=1
```

If the native toolchain or platform is unsupported, the task is skipped and Java
builds continue to work.

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
