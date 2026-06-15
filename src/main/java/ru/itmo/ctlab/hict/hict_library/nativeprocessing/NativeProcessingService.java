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
import ru.itmo.ctlab.hict.hict_library.chunkedfile.MatrixQueries;
import ru.itmo.ctlab.hict.hict_library.visualization.DistanceExpectedNormalizer;
import ru.itmo.ctlab.hict.hict_library.visualization.SignalDisplayMode;
import ru.itmo.ctlab.hict.hict_library.visualization.colormap.gradient.SimpleLinearGradient;

import java.awt.*;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Locale;

@Slf4j
public final class NativeProcessingService {
  private static final @NotNull NativeProcessingService INSTANCE = new NativeProcessingService();
  private final @NotNull NativeTileProcessor nativeTileProcessor = new NativeTileProcessor();
  private volatile boolean requestedEnabled;
  private volatile boolean disabledAfterNativeFailure;
  private volatile @NotNull String lastProcessingFailure = "";

  private NativeProcessingService() {
    this.requestedEnabled = parseEnabledFlag(
      System.getProperty("hict.native.processing"),
      System.getenv("HICT_NATIVE_PROCESSING")
    );
  }

  public static @NotNull NativeProcessingService getInstance() {
    return INSTANCE;
  }

  public @NotNull NativeProcessingStatus status() {
    var loadReport = this.nativeTileProcessor.ensureLoaded();
    if (this.requestedEnabled && loadReport.available() && !this.disabledAfterNativeFailure) {
      this.nativeTileProcessor.ensureSessionOpen();
      loadReport = this.nativeTileProcessor.ensureLoaded();
    }
    final var sessionReport = this.nativeTileProcessor.sessionReport();
    final var available = loadReport.available();
    final var enabled = this.requestedEnabled && available && !this.disabledAfterNativeFailure;
    final var reason = enabled
      ? "Native processing is enabled"
      : resolveDisabledReason(loadReport);
    return new NativeProcessingStatus(
      this.requestedEnabled,
      enabled,
      available,
      loadReport.version(),
      loadReport.source(),
      reason,
      this.lastProcessingFailure,
      sessionReport.active(),
      sessionReport.operationCount(),
      sessionReport.failedOperationCount(),
      sessionReport.hdf5BackendAvailable()
    );
  }

  public synchronized @NotNull NativeProcessingStatus setRequestedEnabled(final boolean requestedEnabled) {
    this.requestedEnabled = requestedEnabled;
    if (!requestedEnabled) {
      this.disabledAfterNativeFailure = false;
      this.lastProcessingFailure = "";
    } else {
      this.nativeTileProcessor.ensureLoaded();
      this.nativeTileProcessor.ensureSessionOpen();
    }
    return status();
  }

  public double @Nullable [][] tryPrepareBaseSignalMatrix(final @NotNull MatrixQueries.MatrixWithWeights rawTile,
                                                          final double lnPreLogBase,
                                                          final double resolutionScalingCoeff,
                                                          final double resolutionLinearScalingCoeff,
                                                          final boolean applyResolutionScaling,
                                                          final boolean applyResolutionLinearScaling,
                                                          final boolean applyCoolerWeights) {
    if (!this.requestedEnabled || this.disabledAfterNativeFailure) {
      return null;
    }
    if (!isNativeProcessingActive()) {
      return null;
    }

    final var matrix = rawTile.matrix();
    final var rows = matrix.rows();
    final var columns = matrix.cols();
    final var elementCount = checkedElementCount(rows, columns);
    if (elementCount < 0) {
      return null;
    }
    if (rawTile.rowWeights() != null && rawTile.rowWeights().length < rows) {
      return null;
    }

    final var output = new double[elementCount];
    try {
      final boolean computed;
      if (matrix instanceof MatrixQueries.DoubleMatrix doubleMatrix) {
        computed = this.nativeTileProcessor.computeBaseSignalDouble(
          flattenDoubleMatrix(doubleMatrix.values(), rows, columns),
          safeWeights(rawTile.rowWeights(), rows),
          safeWeights(rawTile.colWeights(), columns),
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
      } else if (matrix instanceof MatrixQueries.LongMatrix longMatrix) {
        computed = this.nativeTileProcessor.computeBaseSignalLong(
          flattenLongMatrix(longMatrix.values(), rows, columns),
          safeWeights(rawTile.rowWeights(), rows),
          safeWeights(rawTile.colWeights(), columns),
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
      } else {
        return null;
      }

      if (!computed) {
        recordProcessingFailure("Native base signal processor rejected the tile input");
        return null;
      }
      return inflateDoubleMatrix(output, rows, columns);
    } catch (final Throwable err) {
      recordProcessingFailure("Native base signal processing failed: " + err.getMessage());
      log.warn("Native base signal processing failed; falling back to Java implementation", err);
      return null;
    }
  }

  public byte @Nullable [] tryMapLinearGradientRgba(final double @NotNull [][] values,
                                                    final @NotNull SimpleLinearGradient gradient) {
    if (!this.requestedEnabled || this.disabledAfterNativeFailure) {
      return null;
    }
    if (!isNativeProcessingActive()) {
      return null;
    }
    final var rows = values.length;
    final var columns = rows > 0 ? values[0].length : 0;
    final var elementCount = checkedElementCount(rows, columns);
    if (elementCount < 0) {
      return null;
    }
    final var output = new byte[elementCount * 4];
    try {
      final var computed = this.nativeTileProcessor.mapLinearGradientRgba(
        flattenDoubleMatrix(values, rows, columns),
        rows,
        columns,
        colorComponents(gradient.getStartColor()),
        colorComponents(gradient.getEndColor()),
        gradient.getMinSignal(),
        gradient.getMaxSignal(),
        output
      );
      if (!computed) {
        recordProcessingFailure("Native linear-gradient renderer rejected the tile input");
        return null;
      }
      return output;
    } catch (final Throwable err) {
      recordProcessingFailure("Native linear-gradient rendering failed: " + err.getMessage());
      log.warn("Native linear-gradient rendering failed; falling back to Java implementation", err);
      return null;
    }
  }

  public boolean tryApplyPostLogInPlace(final double @NotNull [][] values,
                                        final double lnPostLogBase) {
    if (!this.requestedEnabled || this.disabledAfterNativeFailure) {
      return false;
    }
    if (!isNativeProcessingActive()) {
      return false;
    }
    if (!Double.isFinite(lnPostLogBase) || lnPostLogBase <= 0.0d) {
      return false;
    }
    final var rows = values.length;
    final var columns = rows > 0 ? values[0].length : 0;
    final var elementCount = checkedElementCount(rows, columns);
    if (elementCount < 0) {
      return false;
    }
    final var flattened = flattenDoubleMatrix(values, rows, columns);
    try {
      final var computed = this.nativeTileProcessor.applyPostLog(flattened, lnPostLogBase);
      if (!computed) {
        recordProcessingFailure("Native post-log transformer rejected the input");
        return false;
      }
      copyFlatToMatrix(flattened, values, rows, columns);
      return true;
    } catch (final Throwable err) {
      recordProcessingFailure("Native post-log transform failed: " + err.getMessage());
      log.warn("Native post-log transform failed; falling back to Java implementation", err);
      return false;
    }
  }

  public long @Nullable [] tryCountStripeBlocks(final long @NotNull [] columnBins,
                                                final int stripeCount,
                                                final int submatrixSize,
                                                final int denseThreshold) {
    if (!this.requestedEnabled || this.disabledAfterNativeFailure) {
      return null;
    }
    if (!isNativeProcessingActive()) {
      return null;
    }
    if (stripeCount <= 0 || submatrixSize <= 0 || denseThreshold <= 0) {
      return null;
    }
    final var outputSparseDenseCounts = new long[2];
    try {
      final var computed = this.nativeTileProcessor.countStripeBlocks(
        columnBins,
        stripeCount,
        submatrixSize,
        denseThreshold,
        outputSparseDenseCounts
      );
      if (!computed) {
        recordProcessingFailure("Native stripe-block counter rejected the input");
        return null;
      }
      return outputSparseDenseCounts;
    } catch (final Throwable err) {
      recordProcessingFailure("Native stripe-block counting failed: " + err.getMessage());
      log.warn("Native stripe-block counting failed; falling back to Java implementation", err);
      return null;
    }
  }

  public @Nullable NativeAggregationResult tryAggregatePrecomputedSeries(final double @NotNull [] values,
                                                                         final long @NotNull [] support,
                                                                         final long queryStartPx,
                                                                         final long queryEndPx,
                                                                         final int bucketCount,
                                                                         final int strategyCode) {
    if (!this.requestedEnabled || this.disabledAfterNativeFailure) {
      return null;
    }
    if (!isNativeProcessingActive()) {
      return null;
    }
    if (values.length == 0 || support.length < values.length || bucketCount <= 0 || queryEndPx <= queryStartPx) {
      return null;
    }
    final var outputValues = new double[bucketCount];
    final var outputSupport = new long[bucketCount];
    try {
      final var computed = this.nativeTileProcessor.aggregatePrecomputedSeries(
        values,
        support,
        queryStartPx,
        queryEndPx,
        bucketCount,
        strategyCode,
        outputValues,
        outputSupport
      );
      if (!computed) {
        recordProcessingFailure("Native precomputed-series aggregator rejected the input");
        return null;
      }
      return new NativeAggregationResult(outputValues, outputSupport);
    } catch (final Throwable err) {
      recordProcessingFailure("Native precomputed-series aggregation failed: " + err.getMessage());
      log.warn("Native precomputed-series aggregation failed; falling back to Java implementation", err);
      return null;
    }
  }

  public @Nullable NativeAggregationResult tryAggregateIntervals(final long @NotNull [] starts,
                                                                 final long @NotNull [] ends,
                                                                 final double @Nullable [] values,
                                                                 final long queryStartPx,
                                                                 final long queryEndPx,
                                                                 final int bucketCount,
                                                                 final int modeCode) {
    if (!this.requestedEnabled || this.disabledAfterNativeFailure) {
      return null;
    }
    if (!isNativeProcessingActive()) {
      return null;
    }
    if (starts.length == 0 || ends.length < starts.length || bucketCount <= 0 || queryEndPx <= queryStartPx) {
      return null;
    }
    if (values != null && values.length < starts.length) {
      return null;
    }
    final var outputValues = new double[bucketCount];
    final var outputCounts = new long[bucketCount];
    try {
      final var computed = this.nativeTileProcessor.aggregateIntervals(
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
      if (!computed) {
        recordProcessingFailure("Native interval aggregator rejected the input");
        return null;
      }
      return new NativeAggregationResult(outputValues, outputCounts);
    } catch (final Throwable err) {
      recordProcessingFailure("Native interval aggregation failed: " + err.getMessage());
      log.warn("Native interval aggregation failed; falling back to Java implementation", err);
      return null;
    }
  }

  public @Nullable String tryReverseComplementAscii(final @NotNull String sequence) {
    if (!this.requestedEnabled || this.disabledAfterNativeFailure) {
      return null;
    }
    if (!isNativeProcessingActive()) {
      return null;
    }
    if (sequence.isEmpty()) {
      return "";
    }
    final var input = sequence.getBytes(StandardCharsets.US_ASCII);
    final var output = new byte[input.length];
    try {
      final var computed = this.nativeTileProcessor.reverseComplementAscii(input, output);
      if (!computed) {
        recordProcessingFailure("Native FASTA reverse-complement rejected the input");
        return null;
      }
      return new String(output, StandardCharsets.US_ASCII);
    } catch (final Throwable err) {
      recordProcessingFailure("Native FASTA reverse-complement failed: " + err.getMessage());
      log.warn("Native FASTA reverse-complement failed; falling back to Java implementation", err);
      return null;
    }
  }

  public boolean trySortSparseBlockRowMajor(final long @NotNull [] rows,
                                            final long @NotNull [] columns,
                                            final double @NotNull [] values,
                                            final int submatrixSize) {
    if (!this.requestedEnabled || this.disabledAfterNativeFailure) {
      return false;
    }
    if (!isNativeProcessingActive()) {
      return false;
    }
    if (!validSparseBlockInput(rows, columns, values.length, submatrixSize)) {
      return false;
    }
    try {
      final var computed = this.nativeTileProcessor.sortSparseBlockDouble(rows, columns, values, submatrixSize);
      if (!computed) {
        recordProcessingFailure("Native double sparse-block sorter rejected the input");
        return false;
      }
      return true;
    } catch (final Throwable err) {
      recordProcessingFailure("Native double sparse-block sort failed: " + err.getMessage());
      log.warn("Native double sparse-block sort failed; falling back to Java implementation", err);
      return false;
    }
  }

  public boolean trySortSparseBlockRowMajor(final long @NotNull [] rows,
                                            final long @NotNull [] columns,
                                            final long @NotNull [] values,
                                            final int submatrixSize) {
    if (!this.requestedEnabled || this.disabledAfterNativeFailure) {
      return false;
    }
    if (!isNativeProcessingActive()) {
      return false;
    }
    if (!validSparseBlockInput(rows, columns, values.length, submatrixSize)) {
      return false;
    }
    try {
      final var computed = this.nativeTileProcessor.sortSparseBlockLong(rows, columns, values, submatrixSize);
      if (!computed) {
        recordProcessingFailure("Native long sparse-block sorter rejected the input");
        return false;
      }
      return true;
    } catch (final Throwable err) {
      recordProcessingFailure("Native long sparse-block sort failed: " + err.getMessage());
      log.warn("Native long sparse-block sort failed; falling back to Java implementation", err);
      return false;
    }
  }

  public double @Nullable [][] tryTransformExpectedSignal(final double @NotNull [][] signal,
                                                          final long startRowPx,
                                                          final long startColPx,
                                                          final @NotNull SignalDisplayMode displayMode,
                                                          final @NotNull DistanceExpectedNormalizer.DiagonalProfile profile) {
    if (!this.requestedEnabled || this.disabledAfterNativeFailure || displayMode == SignalDisplayMode.OBSERVED) {
      return null;
    }
    if (!isNativeProcessingActive()) {
      return null;
    }
    if (profile.segmentProfiles().length != 0) {
      return null;
    }
    final var rows = signal.length;
    final var columns = rows > 0 ? signal[0].length : 0;
    final var elementCount = checkedElementCount(rows, columns);
    if (elementCount < 0) {
      return null;
    }
    final var output = new double[elementCount];
    try {
      final var computed = this.nativeTileProcessor.transformExpectedSignal(
        flattenDoubleMatrix(signal, rows, columns),
        rows,
        columns,
        startRowPx,
        startColPx,
        displayMode == SignalDisplayMode.EXPECTED ? 1 : 2,
        profile.minDiagonal(),
        profile.means(),
        output
      );
      if (!computed) {
        recordProcessingFailure("Native observed/expected transformer rejected the input");
        return null;
      }
      return inflateDoubleMatrix(output, rows, columns);
    } catch (final Throwable err) {
      recordProcessingFailure("Native observed/expected transform failed: " + err.getMessage());
      log.warn("Native observed/expected transform failed; falling back to Java implementation", err);
      return null;
    }
  }

  private @NotNull String resolveDisabledReason(final @NotNull NativeTileProcessor.LoadReport loadReport) {
    if (!this.requestedEnabled) {
      return "Native processing is available only when explicitly enabled; Java implementation is active";
    }
    if (!loadReport.available()) {
      return loadReport.reason();
    }
    if (this.disabledAfterNativeFailure) {
      return "Native processing was disabled after a native processing failure";
    }
    return "Native processing is disabled";
  }

  private synchronized void recordProcessingFailure(final @NotNull String message) {
    this.disabledAfterNativeFailure = true;
    this.lastProcessingFailure = message;
  }

  private boolean isNativeProcessingActive() {
    final var loadReport = this.nativeTileProcessor.ensureLoaded();
    return loadReport.available() && !this.disabledAfterNativeFailure;
  }

  private static boolean parseEnabledFlag(final @Nullable String... values) {
    for (final var value : values) {
      if (value == null || value.isBlank()) {
        continue;
      }
      final var normalized = value.trim().toLowerCase(Locale.ROOT);
      return normalized.equals("1")
        || normalized.equals("true")
        || normalized.equals("yes")
        || normalized.equals("on");
    }
    return true;
  }

  private static int checkedElementCount(final int rows, final int columns) {
    if (rows < 0 || columns < 0) {
      return -1;
    }
    final long count = (long) rows * (long) columns;
    if (count > Integer.MAX_VALUE) {
      return -1;
    }
    return (int) count;
  }

  private static double @NotNull [] safeWeights(final double @Nullable [] weights,
                                                final int expectedLength) {
    if (weights != null && weights.length >= expectedLength) {
      return weights;
    }
    final var fallback = new double[expectedLength];
    Arrays.fill(fallback, 1.0d);
    return fallback;
  }

  private static double @NotNull [] flattenDoubleMatrix(final double @NotNull [][] values,
                                                        final int rows,
                                                        final int columns) {
    final var result = new double[Math.max(0, rows * columns)];
    var dst = 0;
    for (int row = 0; row < rows; row++) {
      final var rowValues = values[row];
      final var copied = Math.min(columns, rowValues.length);
      System.arraycopy(rowValues, 0, result, dst, copied);
      dst += columns;
    }
    return result;
  }

  private static long @NotNull [] flattenLongMatrix(final long @NotNull [][] values,
                                                    final int rows,
                                                    final int columns) {
    final var result = new long[Math.max(0, rows * columns)];
    var dst = 0;
    for (int row = 0; row < rows; row++) {
      final var rowValues = values[row];
      final var copied = Math.min(columns, rowValues.length);
      System.arraycopy(rowValues, 0, result, dst, copied);
      dst += columns;
    }
    return result;
  }

  private static double @NotNull [][] inflateDoubleMatrix(final double @NotNull [] values,
                                                          final int rows,
                                                          final int columns) {
    final var result = new double[rows][columns];
    var src = 0;
    for (int row = 0; row < rows; row++) {
      System.arraycopy(values, src, result[row], 0, columns);
      src += columns;
    }
    return result;
  }

  private static boolean validSparseBlockInput(final long @NotNull [] rows,
                                               final long @NotNull [] columns,
                                               final int valuesLength,
                                               final int submatrixSize) {
    return rows.length > 1
      && columns.length >= rows.length
      && valuesLength >= rows.length
      && submatrixSize > 0;
  }

  private static void copyFlatToMatrix(final double @NotNull [] values,
                                       final double @NotNull [][] destination,
                                       final int rows,
                                       final int columns) {
    var src = 0;
    for (int row = 0; row < rows; row++) {
      System.arraycopy(values, src, destination[row], 0, columns);
      src += columns;
    }
  }

  private static float @NotNull [] colorComponents(final @NotNull Color color) {
    return color.getRGBComponents(null);
  }

  public record NativeAggregationResult(double @NotNull [] values,
                                        long @NotNull [] counts) {
  }

  public record NativeProcessingStatus(boolean requested,
                                       boolean enabled,
                                       boolean available,
                                       @NotNull String version,
                                       @NotNull String source,
                                       @NotNull String reason,
                                       @NotNull String lastFailure,
                                       boolean nativeSessionActive,
                                       long nativeOperationCount,
                                       long nativeFailedOperationCount,
                                       boolean nativeHdf5BackendAvailable) {
  }
}
