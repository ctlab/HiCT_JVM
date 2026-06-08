/*
 * MIT License
 *
 * Copyright (c) 2021-2026. Aleksandr Serdiukov, Anton Zamyatin, Aleksandr Sinitsyn, Vitalii Dravgelis and Computer Technologies Laboratory ITMO University team.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ru.itmo.ctlab.hict.hict_library.nativeprocessing;

import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.scijava.nativelib.NativeLoader;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

@Slf4j
final class NativeTileProcessor {
  private static final @NotNull String SSE2_LIBRARY_BASE_NAME = "hict_native_sse2";
  private static final @NotNull String AVX2_LIBRARY_BASE_NAME = "hict_native";
  private static final @NotNull String AVX512_LIBRARY_BASE_NAME = "hict_native_avx512";
  private static final @NotNull String NATIVE_VARIANT_PROPERTY = "hict.native.variant";
  private static final @NotNull String NATIVE_VARIANT_ENV = "HICT_NATIVE_VARIANT";
  private volatile @NotNull LoadReport loadReport = LoadReport.notAttempted();
  private volatile long sessionHandle;

  @NotNull LoadReport ensureLoaded() {
    final var current = this.loadReport;
    if (current.state() == LoadState.LOADED || current.state() == LoadState.FAILED) {
      return current;
    }
    return loadOnce();
  }

  private synchronized @NotNull LoadReport loadOnce() {
    if (this.loadReport.state() == LoadState.LOADED || this.loadReport.state() == LoadState.FAILED) {
      return this.loadReport;
    }
    this.loadReport = tryLoad();
    return this.loadReport;
  }

  boolean computeBaseSignalDouble(final double @NotNull [] input,
                                  final double @Nullable [] rowWeights,
                                  final double @Nullable [] columnWeights,
                                  final int rows,
                                  final int columns,
                                  final double lnPreLogBase,
                                  final double resolutionScalingCoeff,
                                  final double resolutionLinearScalingCoeff,
                                  final boolean applyResolutionScaling,
                                  final boolean applyResolutionLinearScaling,
                                  final boolean applyCoolerWeights,
                                  final double @NotNull [] output) {
    final var sessionHandle = sessionHandle();
    if (sessionHandle == 0L) {
      return false;
    }
    return nativeComputeBaseSignalDouble(
      sessionHandle,
      input,
      rowWeights,
      columnWeights,
      rows,
      columns,
      lnPreLogBase,
      resolutionScalingCoeff,
      resolutionLinearScalingCoeff,
      applyResolutionScaling,
      applyResolutionLinearScaling,
      applyCoolerWeights,
      output
    );
  }

  boolean computeBaseSignalLong(final long @NotNull [] input,
                                final double @Nullable [] rowWeights,
                                final double @Nullable [] columnWeights,
                                final int rows,
                                final int columns,
                                final double lnPreLogBase,
                                final double resolutionScalingCoeff,
                                final double resolutionLinearScalingCoeff,
                                final boolean applyResolutionScaling,
                                final boolean applyResolutionLinearScaling,
                                final boolean applyCoolerWeights,
                                final double @NotNull [] output) {
    final var sessionHandle = sessionHandle();
    if (sessionHandle == 0L) {
      return false;
    }
    return nativeComputeBaseSignalLong(
      sessionHandle,
      input,
      rowWeights,
      columnWeights,
      rows,
      columns,
      lnPreLogBase,
      resolutionScalingCoeff,
      resolutionLinearScalingCoeff,
      applyResolutionScaling,
      applyResolutionLinearScaling,
      applyCoolerWeights,
      output
    );
  }

  boolean mapLinearGradientRgba(final double @NotNull [] signal,
                                final int rows,
                                final int columns,
                                final float @NotNull [] startRgba,
                                final float @NotNull [] endRgba,
                                final double minSignal,
                                final double maxSignal,
                                final byte @NotNull [] outputRgba) {
    final var sessionHandle = sessionHandle();
    if (sessionHandle == 0L) {
      return false;
    }
    return nativeMapLinearGradientRgba(
      sessionHandle,
      signal,
      rows,
      columns,
      startRgba,
      endRgba,
      minSignal,
      maxSignal,
      outputRgba
    );
  }

  boolean applyPostLog(final double @NotNull [] values,
                       final double lnPostLogBase) {
    final var sessionHandle = sessionHandle();
    return sessionHandle != 0L && nativeApplyPostLog(sessionHandle, values, lnPostLogBase);
  }

  boolean countStripeBlocks(final long @NotNull [] columnBins,
                            final int stripeCount,
                            final int submatrixSize,
                            final int denseThreshold,
                            final long @NotNull [] outputSparseDenseCounts) {
    final var sessionHandle = sessionHandle();
    if (sessionHandle == 0L) {
      return false;
    }
    return nativeCountStripeBlocks(
      sessionHandle,
      columnBins,
      stripeCount,
      submatrixSize,
      denseThreshold,
      outputSparseDenseCounts
    );
  }

  boolean aggregatePrecomputedSeries(final double @NotNull [] values,
                                     final long @NotNull [] support,
                                     final long queryStartPx,
                                     final long queryEndPx,
                                     final int bucketCount,
                                     final int strategyCode,
                                     final double @NotNull [] outputValues,
                                     final long @NotNull [] outputSupport) {
    final var sessionHandle = sessionHandle();
    if (sessionHandle == 0L) {
      return false;
    }
    return nativeAggregatePrecomputedSeries(
      sessionHandle,
      values,
      support,
      queryStartPx,
      queryEndPx,
      bucketCount,
      strategyCode,
      outputValues,
      outputSupport
    );
  }

  boolean aggregateIntervals(final long @NotNull [] starts,
                             final long @NotNull [] ends,
                             final double @Nullable [] values,
                             final long queryStartPx,
                             final long queryEndPx,
                             final int bucketCount,
                             final int modeCode,
                             final double @NotNull [] outputValues,
                             final long @NotNull [] outputCounts) {
    final var sessionHandle = sessionHandle();
    if (sessionHandle == 0L) {
      return false;
    }
    return nativeAggregateIntervals(
      sessionHandle,
      starts,
      ends,
      values,
      queryStartPx,
      queryEndPx,
      bucketCount,
      modeCode,
      outputValues,
      outputCounts
    );
  }

  boolean reverseComplementAscii(final byte @NotNull [] input,
                                 final byte @NotNull [] output) {
    final var sessionHandle = sessionHandle();
    return sessionHandle != 0L && nativeReverseComplementAscii(sessionHandle, input, output);
  }

  boolean sortSparseBlockDouble(final long @NotNull [] rows,
                                final long @NotNull [] columns,
                                final double @NotNull [] values,
                                final int submatrixSize) {
    final var sessionHandle = sessionHandle();
    return sessionHandle != 0L && nativeSortSparseBlockDouble(sessionHandle, rows, columns, values, submatrixSize);
  }

  boolean sortSparseBlockLong(final long @NotNull [] rows,
                              final long @NotNull [] columns,
                              final long @NotNull [] values,
                              final int submatrixSize) {
    final var sessionHandle = sessionHandle();
    return sessionHandle != 0L && nativeSortSparseBlockLong(sessionHandle, rows, columns, values, submatrixSize);
  }

  boolean transformExpectedSignal(final double @NotNull [] signal,
                                  final int rows,
                                  final int columns,
                                  final long startRowPx,
                                  final long startColPx,
                                  final int displayModeCode,
                                  final long minDiagonal,
                                  final double @NotNull [] diagonalMeans,
                                  final double @NotNull [] output) {
    final var sessionHandle = sessionHandle();
    if (sessionHandle == 0L) {
      return false;
    }
    return nativeTransformExpectedSignal(
      sessionHandle,
      signal,
      rows,
      columns,
      startRowPx,
      startColPx,
      displayModeCode,
      minDiagonal,
      diagonalMeans,
      output
    );
  }

  @NotNull SessionReport sessionReport() {
    final var loadReport = ensureLoaded();
    if (!loadReport.available()) {
      return SessionReport.inactive();
    }
    final var handle = this.sessionHandle;
    if (handle == 0L) {
      return SessionReport.inactive();
    }
    try {
      return new SessionReport(
        true,
        nativeSessionOperationCount(handle),
        nativeSessionFailedOperationCount(handle),
        nativeSessionHdf5Available(handle)
      );
    } catch (final Throwable err) {
      log.warn("Failed to query HiCT native processing session", err);
      return SessionReport.inactive();
    }
  }

  boolean ensureSessionOpen() {
    return sessionHandle() != 0L;
  }

  private long sessionHandle() {
    final var loadReport = ensureLoaded();
    if (!loadReport.available()) {
      return 0L;
    }
    final var current = this.sessionHandle;
    if (current != 0L) {
      return current;
    }
    return openSessionOnce();
  }

  private synchronized long openSessionOnce() {
    if (this.sessionHandle != 0L) {
      return this.sessionHandle;
    }
    try {
      final var handle = nativeOpenSession();
      if (handle == 0L) {
        this.loadReport = LoadReport.failed("HiCT native processing library could not create a backend session");
        return 0L;
      }
      this.sessionHandle = handle;
      return handle;
    } catch (final Throwable err) {
      this.loadReport = LoadReport.failed("HiCT native processing session initialization failed: " + err.getMessage());
      log.warn("HiCT native processing session initialization failed", err);
      return 0L;
    }
  }

  private static @NotNull LoadReport tryLoad() {
    final var explicitPath = NativeCpuFeatures.firstNonBlank(
      System.getProperty("hict.native.library.path"),
      System.getenv("HICT_NATIVE_LIBRARY_PATH")
    );
    if (explicitPath != null) {
      final var report = tryLoadFromPath(Path.of(explicitPath), "explicit HICT native library path");
      if (report.state() == LoadState.LOADED) {
        return report;
      }
      log.warn("Failed to load HiCT native processing library from explicit path {}: {}", explicitPath, report.reason());
      return report;
    }

    final var explicitDirectory = NativeCpuFeatures.firstNonBlank(
      System.getProperty("hict.native.library.dir"),
      System.getenv("HICT_NATIVE_LIBRARY_DIR")
    );
    if (explicitDirectory != null) {
      final var report = tryLoadFromDirectory(Path.of(explicitDirectory), "explicit HICT native library directory");
      if (report.state() == LoadState.LOADED) {
        return report;
      }
      log.warn("Failed to load HiCT native processing library from explicit directory {}: {}", explicitDirectory, report.reason());
    }

    final var resourceReport = tryLoadFromBundledResource();
    if (resourceReport.state() == LoadState.LOADED) {
      return resourceReport;
    }

    for (final var libraryBaseName : preferredLibraryBaseNames()) {
      try {
        NativeLoader.loadLibrary(libraryBaseName);
        return loaded("NativeLoader " + libraryBaseName, nativeVersion());
      } catch (final Throwable err) {
        log.debug("NativeLoader could not load {}", libraryBaseName, err);
      }
    }

    var lastFailure = "";
    for (final var libraryBaseName : preferredLibraryBaseNames()) {
      try {
        System.loadLibrary(libraryBaseName);
        return loaded("java.library.path " + libraryBaseName, nativeVersion());
      } catch (final Throwable err) {
        lastFailure = err.getMessage();
      }
    }
    return LoadReport.failed("HiCT native processing library is not available: " + lastFailure);
  }

  private static @NotNull LoadReport tryLoadFromDirectory(final @NotNull Path directory,
                                                          final @NotNull String sourceDescription) {
    var lastFailure = "";
    for (final var libraryBaseName : preferredLibraryBaseNames()) {
      final var mappedName = System.mapLibraryName(libraryBaseName);
      final var report = tryLoadFromPath(directory.resolve(mappedName), sourceDescription + " (" + libraryBaseName + ")");
      if (report.state() == LoadState.LOADED) {
        return report;
      }
      lastFailure = report.reason();
    }
    return LoadReport.failed(lastFailure.isBlank() ? sourceDescription + " did not contain a supported library" : lastFailure);
  }

  private static @NotNull LoadReport tryLoadFromPath(final @NotNull Path path,
                                                     final @NotNull String sourceDescription) {
    try {
      if (!Files.isRegularFile(path)) {
        return LoadReport.failed(sourceDescription + " does not point to a regular file: " + path);
      }
      System.load(path.toAbsolutePath().normalize().toString());
      return loaded(path.toAbsolutePath().normalize().toString(), nativeVersion());
    } catch (final Throwable err) {
      return LoadReport.failed(sourceDescription + " failed: " + err.getMessage());
    }
  }

  private static @NotNull LoadReport tryLoadFromBundledResource() {
    final var platformDirectory = platformDirectory();
    if (platformDirectory == null) {
      return LoadReport.failed("Unsupported native processing platform: " + System.getProperty("os.name") + " / " + System.getProperty("os.arch"));
    }
    var lastFailure = "";
    for (final var libraryBaseName : preferredLibraryBaseNames()) {
      final var mappedName = System.mapLibraryName(libraryBaseName);
      final var resourcePath = "/natives/" + platformDirectory + "/" + mappedName;
      try (InputStream stream = NativeTileProcessor.class.getResourceAsStream(resourcePath)) {
        if (stream == null) {
          lastFailure = "Bundled native processing library not found at " + resourcePath;
          continue;
        }
        final var extractionDirectory = Files.createTempDirectory("hict-native-processing-");
        final var extractedLibrary = extractionDirectory.resolve(mappedName);
        Files.copy(stream, extractedLibrary, StandardCopyOption.REPLACE_EXISTING);
        extractedLibrary.toFile().deleteOnExit();
        extractionDirectory.toFile().deleteOnExit();
        System.load(extractedLibrary.toAbsolutePath().normalize().toString());
        return loaded("bundled resource " + resourcePath, nativeVersion());
      } catch (final IOException err) {
        lastFailure = "Failed to extract bundled native processing library " + resourcePath + ": " + err.getMessage();
      } catch (final Throwable err) {
        lastFailure = "Failed to load bundled native processing library " + resourcePath + ": " + err.getMessage();
      }
    }
    return LoadReport.failed(lastFailure.isBlank() ? "Bundled native processing library is not available" : lastFailure);
  }

  private static @Nullable String platformDirectory() {
    final var os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
    final var arch = System.getProperty("os.arch", "").toLowerCase(Locale.ROOT);
    final var is64Bit = arch.contains("64") || arch.equals("amd64") || arch.equals("x86_64");
    if (!is64Bit) {
      return null;
    }
    if (os.contains("linux")) {
      return "linux_64";
    }
    if (os.contains("win")) {
      return "windows_64";
    }
    if (os.contains("mac") || os.contains("darwin")) {
      return "macos_64";
    }
    return null;
  }

  private static @NotNull List<String> preferredLibraryBaseNames() {
    final var requestedVariant = NativeCpuFeatures.firstNonBlank(
      System.getProperty(NATIVE_VARIANT_PROPERTY),
      System.getenv(NATIVE_VARIANT_ENV)
    );
    final var normalizedVariant = requestedVariant == null
      ? "auto"
      : requestedVariant.trim().toLowerCase(Locale.ROOT);
    final var result = new ArrayList<String>(3);
    if ("sse2".equals(normalizedVariant) || "baseline".equals(normalizedVariant)) {
      if (NativeCpuFeatures.supportsSse2Core()) {
        result.add(SSE2_LIBRARY_BASE_NAME);
      }
      return result;
    }
    if ("avx2".equals(normalizedVariant)) {
      if (NativeCpuFeatures.supportsAvx2Core()) {
        result.add(AVX2_LIBRARY_BASE_NAME);
      }
      return result;
    }
    if ("avx512".equals(normalizedVariant)) {
      if (NativeCpuFeatures.supportsAvx512Core()) {
        result.add(AVX512_LIBRARY_BASE_NAME);
      }
      return result;
    }
    if (NativeCpuFeatures.supportsAvx512Core()) {
      result.add(AVX512_LIBRARY_BASE_NAME);
    }
    if (NativeCpuFeatures.supportsAvx2Core()) {
      result.add(AVX2_LIBRARY_BASE_NAME);
    }
    if (NativeCpuFeatures.supportsSse2Core()) {
      result.add(SSE2_LIBRARY_BASE_NAME);
    }
    return result;
  }

  private static @NotNull LoadReport loaded(final @NotNull String source,
                                            final @Nullable String version) {
    return new LoadReport(
      LoadState.LOADED,
      true,
      Objects.requireNonNullElse(version, "unknown"),
      source,
      ""
    );
  }

  private static native @Nullable String nativeVersion();

  private static native long nativeOpenSession();

  private static native long nativeSessionOperationCount(long sessionHandle);

  private static native long nativeSessionFailedOperationCount(long sessionHandle);

  private static native boolean nativeSessionHdf5Available(long sessionHandle);

  private static native boolean nativeComputeBaseSignalDouble(long sessionHandle,
                                                              double[] input,
                                                              double[] rowWeights,
                                                              double[] columnWeights,
                                                              int rows,
                                                              int columns,
                                                              double lnPreLogBase,
                                                              double resolutionScalingCoeff,
                                                              double resolutionLinearScalingCoeff,
                                                              boolean applyResolutionScaling,
                                                              boolean applyResolutionLinearScaling,
                                                              boolean applyCoolerWeights,
                                                              double[] output);

  private static native boolean nativeComputeBaseSignalLong(long sessionHandle,
                                                            long[] input,
                                                            double[] rowWeights,
                                                            double[] columnWeights,
                                                            int rows,
                                                            int columns,
                                                            double lnPreLogBase,
                                                            double resolutionScalingCoeff,
                                                            double resolutionLinearScalingCoeff,
                                                            boolean applyResolutionScaling,
                                                            boolean applyResolutionLinearScaling,
                                                            boolean applyCoolerWeights,
                                                            double[] output);

  private static native boolean nativeMapLinearGradientRgba(long sessionHandle,
                                                            double[] signal,
                                                            int rows,
                                                            int columns,
                                                            float[] startRgba,
                                                            float[] endRgba,
                                                            double minSignal,
                                                            double maxSignal,
                                                            byte[] outputRgba);

  private static native boolean nativeApplyPostLog(long sessionHandle,
                                                   double[] values,
                                                   double lnPostLogBase);

  private static native boolean nativeCountStripeBlocks(long sessionHandle,
                                                        long[] columnBins,
                                                        int stripeCount,
                                                        int submatrixSize,
                                                        int denseThreshold,
                                                        long[] outputSparseDenseCounts);

  private static native boolean nativeAggregatePrecomputedSeries(long sessionHandle,
                                                                 double[] values,
                                                                 long[] support,
                                                                 long queryStartPx,
                                                                 long queryEndPx,
                                                                 int bucketCount,
                                                                 int strategyCode,
                                                                 double[] outputValues,
                                                                 long[] outputSupport);

  private static native boolean nativeAggregateIntervals(long sessionHandle,
                                                         long[] starts,
                                                         long[] ends,
                                                         double[] values,
                                                         long queryStartPx,
                                                         long queryEndPx,
                                                         int bucketCount,
                                                         int modeCode,
                                                         double[] outputValues,
                                                         long[] outputCounts);

  private static native boolean nativeReverseComplementAscii(long sessionHandle,
                                                             byte[] input,
                                                             byte[] output);

  private static native boolean nativeSortSparseBlockDouble(long sessionHandle,
                                                            long[] rows,
                                                            long[] columns,
                                                            double[] values,
                                                            int submatrixSize);

  private static native boolean nativeSortSparseBlockLong(long sessionHandle,
                                                          long[] rows,
                                                          long[] columns,
                                                          long[] values,
                                                          int submatrixSize);

  private static native boolean nativeTransformExpectedSignal(long sessionHandle,
                                                              double[] signal,
                                                              int rows,
                                                              int columns,
                                                              long startRowPx,
                                                              long startColPx,
                                                              int displayModeCode,
                                                              long minDiagonal,
                                                              double[] diagonalMeans,
                                                              double[] output);

  enum LoadState {
    NOT_ATTEMPTED,
    LOADED,
    FAILED
  }

  record LoadReport(@NotNull LoadState state,
                    boolean available,
                    @NotNull String version,
                    @NotNull String source,
                    @NotNull String reason) {
    static @NotNull LoadReport notAttempted() {
      return new LoadReport(LoadState.NOT_ATTEMPTED, false, "unknown", "", "Native processing library was not probed yet");
    }

    static @NotNull LoadReport failed(final @NotNull String reason) {
      return new LoadReport(LoadState.FAILED, false, "unknown", "", reason);
    }
  }

  record SessionReport(boolean active,
                       long operationCount,
                       long failedOperationCount,
                       boolean hdf5BackendAvailable) {
    static @NotNull SessionReport inactive() {
      return new SessionReport(false, 0L, 0L, false);
    }
  }
}
