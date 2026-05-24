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
  private static final @NotNull String BASELINE_LIBRARY_BASE_NAME = "hict_native";
  private static final @NotNull String AVX512_LIBRARY_BASE_NAME = "hict_native_avx512";
  private static final @NotNull String NATIVE_VARIANT_PROPERTY = "hict.native.variant";
  private static final @NotNull String NATIVE_VARIANT_ENV = "HICT_NATIVE_VARIANT";
  private volatile @NotNull LoadReport loadReport = LoadReport.notAttempted();

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
    return nativeComputeBaseSignalDouble(
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
    return nativeComputeBaseSignalLong(
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
    return nativeMapLinearGradientRgba(
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

  boolean countStripeBlocks(final long @NotNull [] columnBins,
                            final int stripeCount,
                            final int submatrixSize,
                            final int denseThreshold,
                            final long @NotNull [] outputSparseDenseCounts) {
    return nativeCountStripeBlocks(
      columnBins,
      stripeCount,
      submatrixSize,
      denseThreshold,
      outputSparseDenseCounts
    );
  }

  private static @NotNull LoadReport tryLoad() {
    final var explicitPath = firstNonBlank(
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

    final var explicitDirectory = firstNonBlank(
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
    final var requestedVariant = firstNonBlank(
      System.getProperty(NATIVE_VARIANT_PROPERTY),
      System.getenv(NATIVE_VARIANT_ENV)
    );
    final var normalizedVariant = requestedVariant == null
      ? "auto"
      : requestedVariant.trim().toLowerCase(Locale.ROOT);
    final var result = new ArrayList<String>(2);
    if ("baseline".equals(normalizedVariant)) {
      result.add(BASELINE_LIBRARY_BASE_NAME);
      return result;
    }
    if ("avx512".equals(normalizedVariant)) {
      if (supportsAvx512Core()) {
        result.add(AVX512_LIBRARY_BASE_NAME);
      }
      return result;
    }
    if (supportsAvx512Core()) {
      result.add(AVX512_LIBRARY_BASE_NAME);
    }
    result.add(BASELINE_LIBRARY_BASE_NAME);
    return result;
  }

  private static boolean supportsAvx512Core() {
    final var disabled = firstNonBlank(
      System.getProperty("hict.native.disableAvx512"),
      System.getenv("HICT_NATIVE_DISABLE_AVX512")
    );
    if (isTruthy(disabled)) {
      return false;
    }
    final var os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
    if (!os.contains("linux")) {
      return false;
    }
    try {
      final var cpuInfo = Files.readString(Path.of("/proc/cpuinfo")).toLowerCase(Locale.ROOT);
      return cpuInfo.contains("avx512f")
        && cpuInfo.contains("avx512dq")
        && cpuInfo.contains("avx512bw")
        && cpuInfo.contains("avx512vl");
    } catch (final IOException err) {
      log.debug("Could not inspect /proc/cpuinfo for AVX-512 support", err);
      return false;
    }
  }

  private static boolean isTruthy(final @Nullable String value) {
    if (value == null || value.isBlank()) {
      return false;
    }
    final var normalized = value.trim().toLowerCase(Locale.ROOT);
    return normalized.equals("1")
      || normalized.equals("true")
      || normalized.equals("yes")
      || normalized.equals("on");
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

  private static @Nullable String firstNonBlank(final @Nullable String... values) {
    for (final var value : values) {
      if (value != null && !value.isBlank()) {
        return value.trim();
      }
    }
    return null;
  }

  private static native @Nullable String nativeVersion();

  private static native boolean nativeComputeBaseSignalDouble(double[] input,
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

  private static native boolean nativeComputeBaseSignalLong(long[] input,
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

  private static native boolean nativeMapLinearGradientRgba(double[] signal,
                                                            int rows,
                                                            int columns,
                                                            float[] startRgba,
                                                            float[] endRgba,
                                                            double minSignal,
                                                            double maxSignal,
                                                            byte[] outputRgba);

  private static native boolean nativeCountStripeBlocks(long[] columnBins,
                                                        int stripeCount,
                                                        int submatrixSize,
                                                        int denseThreshold,
                                                        long[] outputSparseDenseCounts);

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
}
