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

import org.jetbrains.annotations.NotNull;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Random;

public final class NativeProcessingBenchmark {
  private NativeProcessingBenchmark() {
  }

  public static void main(final String @NotNull [] args) {
    final int rows = readIntProperty("hict.native.benchmark.rows", 1024);
    final int columns = readIntProperty("hict.native.benchmark.columns", 1024);
    final int warmupIterations = readIntProperty("hict.native.benchmark.warmup", 5);
    final int measuredIterations = readIntProperty("hict.native.benchmark.iterations", 25);
    final int elementCount = Math.multiplyExact(rows, columns);
    final boolean javaOnly = Boolean.getBoolean("hict.native.benchmark.javaOnly");

    final var input = new double[elementCount];
    final var rowWeights = new double[rows];
    final var columnWeights = new double[columns];
    fillSyntheticTile(input, rowWeights, columnWeights, rows, columns);
    if (javaOnly) {
      runJavaOnlyBenchmark(input, rowWeights, columnWeights, rows, columns, warmupIterations, measuredIterations);
      return;
    }

    final var processor = new NativeTileProcessor();
    final var loadReport = processor.ensureLoaded();
    System.out.println("HiCT native benchmark");
    System.out.println("  size: " + rows + " x " + columns + " (" + elementCount + " elements)");
    System.out.println("  iterations: warmup=" + warmupIterations + ", measured=" + measuredIterations);
    System.out.println("  native: " + loadReport.state() + ", version=" + loadReport.version() + ", source=" + loadReport.source());
    if (loadReport.state() != NativeTileProcessor.LoadState.LOADED) {
      System.out.println("  native unavailable: " + loadReport.reason());
      writeUnavailableRows(resolveVariantName(loadReport), loadReport.reason());
      return;
    }

    final var javaOutput = new double[elementCount];
    final var nativeOutput = new double[elementCount];
    final var diagonalMeans = buildDiagonalMeans(input, rows, columns);
    final var javaExpectedOutput = new double[elementCount];
    final var nativeExpectedOutput = new double[elementCount];
    final var javaPostLogWork = new double[elementCount];
    final var nativePostLogWork = new double[elementCount];
    final var seriesSupport = new long[elementCount];
    for (int i = 0; i < seriesSupport.length; i++) {
      seriesSupport[i] = (i % 7) == 0 ? 0L : 1L + (i % 5);
    }
    final int aggregateBucketCount = Math.max(1, columns);
    final var javaAggregateValues = new double[aggregateBucketCount];
    final var nativeAggregateValues = new double[aggregateBucketCount];
    final var javaAggregateSupport = new long[aggregateBucketCount];
    final var nativeAggregateSupport = new long[aggregateBucketCount];

    for (int i = 0; i < warmupIterations; i++) {
      computeJava(input, rowWeights, columnWeights, rows, columns, javaOutput);
      processor.computeBaseSignalDouble(input, rowWeights, columnWeights, rows, columns, Math.log(10.0d), 1.125d, 0.75d, true, true, true, nativeOutput);
      computeObservedOverExpectedJava(input, diagonalMeans, rows, columns, javaExpectedOutput);
      processor.transformExpectedSignal(input, rows, columns, 0L, 0L, 2, 0L, diagonalMeans, nativeExpectedOutput);
      System.arraycopy(input, 0, javaPostLogWork, 0, input.length);
      computePostLogJava(javaPostLogWork, Math.log(10.0d));
      System.arraycopy(input, 0, nativePostLogWork, 0, input.length);
      processor.applyPostLog(nativePostLogWork, Math.log(10.0d));
      aggregatePrecomputedMaxJava(input, seriesSupport, 0L, input.length, aggregateBucketCount, javaAggregateValues, javaAggregateSupport);
      processor.aggregatePrecomputedSeries(input, seriesSupport, 0L, input.length, aggregateBucketCount, 1, nativeAggregateValues, nativeAggregateSupport);
    }

    long javaNanos = 0L;
    for (int i = 0; i < measuredIterations; i++) {
      final long started = System.nanoTime();
      computeJava(input, rowWeights, columnWeights, rows, columns, javaOutput);
      javaNanos += System.nanoTime() - started;
    }

    long nativeNanos = 0L;
    for (int i = 0; i < measuredIterations; i++) {
      final long started = System.nanoTime();
      final boolean ok = processor.computeBaseSignalDouble(input, rowWeights, columnWeights, rows, columns, Math.log(10.0d), 1.125d, 0.75d, true, true, true, nativeOutput);
      nativeNanos += System.nanoTime() - started;
      if (!ok) {
        throw new IllegalStateException("Native benchmark computation was rejected");
      }
    }

    final double maxAbsDiff = maxAbsDiff(javaOutput, nativeOutput);
    if (maxAbsDiff > 1.0e-12d) {
      throw new IllegalStateException("Native benchmark output differs from Java implementation, maxAbsDiff=" + maxAbsDiff);
    }

    long javaExpectedNanos = 0L;
    for (int i = 0; i < measuredIterations; i++) {
      final long started = System.nanoTime();
      computeObservedOverExpectedJava(input, diagonalMeans, rows, columns, javaExpectedOutput);
      javaExpectedNanos += System.nanoTime() - started;
    }

    long nativeExpectedNanos = 0L;
    for (int i = 0; i < measuredIterations; i++) {
      final long started = System.nanoTime();
      final boolean ok = processor.transformExpectedSignal(input, rows, columns, 0L, 0L, 2, 0L, diagonalMeans, nativeExpectedOutput);
      nativeExpectedNanos += System.nanoTime() - started;
      if (!ok) {
        throw new IllegalStateException("Native observed/expected benchmark computation was rejected");
      }
    }

    final double expectedMaxAbsDiff = maxAbsDiff(javaExpectedOutput, nativeExpectedOutput);
    if (expectedMaxAbsDiff > 1.0e-12d) {
      throw new IllegalStateException("Native observed/expected benchmark output differs from Java implementation, maxAbsDiff=" + expectedMaxAbsDiff);
    }

    long javaPostLogNanos = 0L;
    for (int i = 0; i < measuredIterations; i++) {
      System.arraycopy(input, 0, javaPostLogWork, 0, input.length);
      final long started = System.nanoTime();
      computePostLogJava(javaPostLogWork, Math.log(10.0d));
      javaPostLogNanos += System.nanoTime() - started;
    }

    long nativePostLogNanos = 0L;
    for (int i = 0; i < measuredIterations; i++) {
      System.arraycopy(input, 0, nativePostLogWork, 0, input.length);
      final long started = System.nanoTime();
      final boolean ok = processor.applyPostLog(nativePostLogWork, Math.log(10.0d));
      nativePostLogNanos += System.nanoTime() - started;
      if (!ok) {
        throw new IllegalStateException("Native post-log benchmark computation was rejected");
      }
    }

    final double postLogMaxAbsDiff = maxAbsDiff(javaPostLogWork, nativePostLogWork);
    if (postLogMaxAbsDiff > 1.0e-12d) {
      throw new IllegalStateException("Native post-log output differs from Java implementation, maxAbsDiff=" + postLogMaxAbsDiff);
    }

    long javaAggregateNanos = 0L;
    for (int i = 0; i < measuredIterations; i++) {
      final long started = System.nanoTime();
      aggregatePrecomputedMaxJava(input, seriesSupport, 0L, input.length, aggregateBucketCount, javaAggregateValues, javaAggregateSupport);
      javaAggregateNanos += System.nanoTime() - started;
    }

    long nativeAggregateNanos = 0L;
    for (int i = 0; i < measuredIterations; i++) {
      final long started = System.nanoTime();
      final boolean ok = processor.aggregatePrecomputedSeries(input, seriesSupport, 0L, input.length, aggregateBucketCount, 1, nativeAggregateValues, nativeAggregateSupport);
      nativeAggregateNanos += System.nanoTime() - started;
      if (!ok) {
        throw new IllegalStateException("Native precomputed-track aggregation benchmark computation was rejected");
      }
    }

    final double aggregateMaxAbsDiff = maxAbsDiff(javaAggregateValues, nativeAggregateValues);
    if (aggregateMaxAbsDiff > 1.0e-12d) {
      throw new IllegalStateException("Native precomputed-track aggregation differs from Java implementation, maxAbsDiff=" + aggregateMaxAbsDiff);
    }
    if (!java.util.Arrays.equals(javaAggregateSupport, nativeAggregateSupport)) {
      throw new IllegalStateException("Native precomputed-track support aggregation differs from Java implementation");
    }

    final double javaMillis = nanosToMillis(javaNanos) / measuredIterations;
    final double nativeMillis = nanosToMillis(nativeNanos) / measuredIterations;
    final double javaExpectedMillis = nanosToMillis(javaExpectedNanos) / measuredIterations;
    final double nativeExpectedMillis = nanosToMillis(nativeExpectedNanos) / measuredIterations;
    final double javaPostLogMillis = nanosToMillis(javaPostLogNanos) / measuredIterations;
    final double nativePostLogMillis = nanosToMillis(nativePostLogNanos) / measuredIterations;
    final double javaAggregateMillis = nanosToMillis(javaAggregateNanos) / measuredIterations;
    final double nativeAggregateMillis = nanosToMillis(nativeAggregateNanos) / measuredIterations;
    System.out.println("  Base signal preparation:");
    System.out.printf(Locale.ROOT, "    Java mean:   %.3f ms%n", javaMillis);
    System.out.printf(Locale.ROOT, "    Native mean: %.3f ms%n", nativeMillis);
    System.out.printf(Locale.ROOT, "    Speedup:     %.3fx%n", javaMillis / Math.max(nativeMillis, 1.0e-9d));
    System.out.printf(Locale.ROOT, "    Max abs diff: %.3g%n", maxAbsDiff);
    System.out.println("  Observed/expected transform:");
    System.out.printf(Locale.ROOT, "    Java mean:   %.3f ms%n", javaExpectedMillis);
    System.out.printf(Locale.ROOT, "    Native mean: %.3f ms%n", nativeExpectedMillis);
    System.out.printf(Locale.ROOT, "    Speedup:     %.3fx%n", javaExpectedMillis / Math.max(nativeExpectedMillis, 1.0e-9d));
    System.out.printf(Locale.ROOT, "    Max abs diff: %.3g%n", expectedMaxAbsDiff);
    System.out.println("  Post-log transform:");
    System.out.printf(Locale.ROOT, "    Java mean:   %.3f ms%n", javaPostLogMillis);
    System.out.printf(Locale.ROOT, "    Native mean: %.3f ms%n", nativePostLogMillis);
    System.out.printf(Locale.ROOT, "    Speedup:     %.3fx%n", javaPostLogMillis / Math.max(nativePostLogMillis, 1.0e-9d));
    System.out.printf(Locale.ROOT, "    Max abs diff: %.3g%n", postLogMaxAbsDiff);
    System.out.println("  Precomputed 1D max aggregation:");
    System.out.printf(Locale.ROOT, "    Java mean:   %.3f ms%n", javaAggregateMillis);
    System.out.printf(Locale.ROOT, "    Native mean: %.3f ms%n", nativeAggregateMillis);
    System.out.printf(Locale.ROOT, "    Speedup:     %.3fx%n", javaAggregateMillis / Math.max(nativeAggregateMillis, 1.0e-9d));
    System.out.printf(Locale.ROOT, "    Max abs diff: %.3g%n", aggregateMaxAbsDiff);
    writeBenchmarkRows(
      new BenchmarkRow("base-signal", "java", true, javaMillis, 0.0d, ""),
      new BenchmarkRow("base-signal", resolveVariantName(loadReport), true, nativeMillis, maxAbsDiff, loadReport.version()),
      new BenchmarkRow("observed-expected", "java", true, javaExpectedMillis, 0.0d, ""),
      new BenchmarkRow("observed-expected", resolveVariantName(loadReport), true, nativeExpectedMillis, expectedMaxAbsDiff, loadReport.version()),
      new BenchmarkRow("post-log", "java", true, javaPostLogMillis, 0.0d, ""),
      new BenchmarkRow("post-log", resolveVariantName(loadReport), true, nativePostLogMillis, postLogMaxAbsDiff, loadReport.version()),
      new BenchmarkRow("precomputed-1d-max", "java", true, javaAggregateMillis, 0.0d, ""),
      new BenchmarkRow("precomputed-1d-max", resolveVariantName(loadReport), true, nativeAggregateMillis, aggregateMaxAbsDiff, loadReport.version())
    );
  }

  private static void runJavaOnlyBenchmark(final double @NotNull [] input,
                                           final double @NotNull [] rowWeights,
                                           final double @NotNull [] columnWeights,
                                           final int rows,
                                           final int columns,
                                           final int warmupIterations,
                                           final int measuredIterations) {
    final int elementCount = Math.multiplyExact(rows, columns);
    final var javaOutput = new double[elementCount];
    final var diagonalMeans = buildDiagonalMeans(input, rows, columns);
    final var javaExpectedOutput = new double[elementCount];
    final var javaPostLogWork = new double[elementCount];
    final var seriesSupport = new long[elementCount];
    for (int i = 0; i < seriesSupport.length; i++) {
      seriesSupport[i] = (i % 7) == 0 ? 0L : 1L + (i % 5);
    }
    final int aggregateBucketCount = Math.max(1, columns);
    final var javaAggregateValues = new double[aggregateBucketCount];
    final var javaAggregateSupport = new long[aggregateBucketCount];

    System.out.println("HiCT Java baseline benchmark");
    System.out.println("  size: " + rows + " x " + columns + " (" + elementCount + " elements)");
    System.out.println("  iterations: warmup=" + warmupIterations + ", measured=" + measuredIterations);

    for (int i = 0; i < warmupIterations; i++) {
      computeJava(input, rowWeights, columnWeights, rows, columns, javaOutput);
      computeObservedOverExpectedJava(input, diagonalMeans, rows, columns, javaExpectedOutput);
      System.arraycopy(input, 0, javaPostLogWork, 0, input.length);
      computePostLogJava(javaPostLogWork, Math.log(10.0d));
      aggregatePrecomputedMaxJava(input, seriesSupport, 0L, input.length, aggregateBucketCount, javaAggregateValues, javaAggregateSupport);
    }

    long javaNanos = 0L;
    for (int i = 0; i < measuredIterations; i++) {
      final long started = System.nanoTime();
      computeJava(input, rowWeights, columnWeights, rows, columns, javaOutput);
      javaNanos += System.nanoTime() - started;
    }

    long javaExpectedNanos = 0L;
    for (int i = 0; i < measuredIterations; i++) {
      final long started = System.nanoTime();
      computeObservedOverExpectedJava(input, diagonalMeans, rows, columns, javaExpectedOutput);
      javaExpectedNanos += System.nanoTime() - started;
    }

    long javaPostLogNanos = 0L;
    for (int i = 0; i < measuredIterations; i++) {
      System.arraycopy(input, 0, javaPostLogWork, 0, input.length);
      final long started = System.nanoTime();
      computePostLogJava(javaPostLogWork, Math.log(10.0d));
      javaPostLogNanos += System.nanoTime() - started;
    }

    long javaAggregateNanos = 0L;
    for (int i = 0; i < measuredIterations; i++) {
      final long started = System.nanoTime();
      aggregatePrecomputedMaxJava(input, seriesSupport, 0L, input.length, aggregateBucketCount, javaAggregateValues, javaAggregateSupport);
      javaAggregateNanos += System.nanoTime() - started;
    }

    final double javaMillis = nanosToMillis(javaNanos) / measuredIterations;
    final double javaExpectedMillis = nanosToMillis(javaExpectedNanos) / measuredIterations;
    final double javaPostLogMillis = nanosToMillis(javaPostLogNanos) / measuredIterations;
    final double javaAggregateMillis = nanosToMillis(javaAggregateNanos) / measuredIterations;
    System.out.println("  Base signal preparation:");
    System.out.printf(Locale.ROOT, "    Java mean: %.3f ms%n", javaMillis);
    System.out.println("  Observed/expected transform:");
    System.out.printf(Locale.ROOT, "    Java mean: %.3f ms%n", javaExpectedMillis);
    System.out.println("  Post-log transform:");
    System.out.printf(Locale.ROOT, "    Java mean: %.3f ms%n", javaPostLogMillis);
    System.out.println("  Precomputed 1D max aggregation:");
    System.out.printf(Locale.ROOT, "    Java mean: %.3f ms%n", javaAggregateMillis);
    writeBenchmarkRows(
      new BenchmarkRow("base-signal", "java", true, javaMillis, 0.0d, ""),
      new BenchmarkRow("observed-expected", "java", true, javaExpectedMillis, 0.0d, ""),
      new BenchmarkRow("post-log", "java", true, javaPostLogMillis, 0.0d, ""),
      new BenchmarkRow("precomputed-1d-max", "java", true, javaAggregateMillis, 0.0d, "")
    );
  }

  private static @NotNull String resolveVariantName(final NativeTileProcessor.@NotNull LoadReport loadReport) {
    final var requestedVariant = System.getProperty("hict.native.variant", "").trim().toLowerCase(Locale.ROOT);
    if (!requestedVariant.isBlank() && !"auto".equals(requestedVariant)) {
      if ("baseline".equals(requestedVariant) || "sse2".equals(requestedVariant) || "generic".equals(requestedVariant) || "x86_64-v3".equals(requestedVariant)) {
        return "generic";
      }
      return requestedVariant;
    }
    final var version = loadReport.version().toLowerCase(Locale.ROOT);
    if (version.contains("avx512")) {
      return "avx512";
    }
    if (version.contains("avx2")) {
      return "avx2";
    }
    if (version.contains("generic") || version.contains("sse2")) {
      return "generic";
    }
    return "generic";
  }

  private static void writeUnavailableRows(final @NotNull String variant,
                                           final @NotNull String reason) {
    writeBenchmarkRows(
      new BenchmarkRow("base-signal", variant, false, Double.NaN, Double.NaN, reason),
      new BenchmarkRow("observed-expected", variant, false, Double.NaN, Double.NaN, reason),
      new BenchmarkRow("post-log", variant, false, Double.NaN, Double.NaN, reason),
      new BenchmarkRow("precomputed-1d-max", variant, false, Double.NaN, Double.NaN, reason)
    );
  }

  private static void writeBenchmarkRows(final BenchmarkRow @NotNull ... rows) {
    final var outputCsv = System.getProperty("hict.native.benchmark.outputCsv", "").trim();
    if (outputCsv.isBlank()) {
      return;
    }
    final var path = Path.of(outputCsv);
    try {
      final var parent = path.getParent();
      if (parent != null) {
        Files.createDirectories(parent);
      }
      try (BufferedWriter writer = Files.newBufferedWriter(path)) {
        writer.write(BenchmarkRow.csvHeader());
        writer.newLine();
        for (final var row : rows) {
          writer.write(row.toCsv());
          writer.newLine();
        }
      }
    } catch (final IOException e) {
      throw new IllegalStateException("Failed to write benchmark CSV to " + path, e);
    }
  }

  private record BenchmarkRow(@NotNull String operation,
                              @NotNull String variant,
                              boolean available,
                              double meanMillis,
                              double maxAbsDiff,
                              @NotNull String notes) {
    static @NotNull String csvHeader() {
      return "operation,variant,available,meanMillis,requestsPerSecond,maxAbsDiff,notes";
    }

    @NotNull String toCsv() {
      final double requestsPerSecond = available && meanMillis > 0.0d
        ? 1000.0d / meanMillis
        : Double.NaN;
      return String.join(
        ",",
        csv(operation),
        csv(variant),
        Boolean.toString(available),
        number(meanMillis),
        number(requestsPerSecond),
        number(maxAbsDiff),
        csv(notes)
      );
    }

    private static @NotNull String number(final double value) {
      return Double.isFinite(value) ? String.format(Locale.ROOT, "%.9g", value) : "";
    }

    private static @NotNull String csv(final @NotNull String value) {
      return "\"" + value.replace("\"", "\"\"") + "\"";
    }
  }

  private static int readIntProperty(final @NotNull String propertyName,
                                     final int fallback) {
    final var value = System.getProperty(propertyName);
    if (value == null || value.isBlank()) {
      return fallback;
    }
    return Math.max(1, Integer.parseInt(value.trim()));
  }

  private static void fillSyntheticTile(final double @NotNull [] input,
                                        final double @NotNull [] rowWeights,
                                        final double @NotNull [] columnWeights,
                                        final int rows,
                                        final int columns) {
    final var random = new Random(42L);
    for (int row = 0; row < rows; row++) {
      rowWeights[row] = 0.8d + (0.4d * random.nextDouble());
    }
    for (int column = 0; column < columns; column++) {
      columnWeights[column] = 0.8d + (0.4d * random.nextDouble());
    }
    for (int row = 0; row < rows; row++) {
      final int rowOffset = row * columns;
      for (int column = 0; column < columns; column++) {
        final var diagonal = Math.exp(-Math.abs(row - column) / 96.0d) * 64.0d;
        final var texture = random.nextDouble() * 8.0d;
        input[rowOffset + column] = diagonal + texture;
      }
    }
  }

  private static void computeJava(final double @NotNull [] input,
                                  final double @NotNull [] rowWeights,
                                  final double @NotNull [] columnWeights,
                                  final int rows,
                                  final int columns,
                                  final double @NotNull [] output) {
    final double logBase = Math.log(10.0d);
    for (int row = 0; row < rows; row++) {
      final int rowOffset = row * columns;
      final double rowWeight = rowWeights[row];
      for (int column = 0; column < columns; column++) {
        final int offset = rowOffset + column;
        var signal = input[offset];
        if (!Double.isFinite(signal) || signal < 0.0d) {
          signal = 0.0d;
        }
        signal = Math.log1p(signal) / logBase;
        signal *= 1.125d;
        signal *= 0.75d;
        signal *= rowWeight * columnWeights[column];
        output[offset] = Double.isFinite(signal) ? signal : 0.0d;
      }
    }
  }

  private static double @NotNull [] buildDiagonalMeans(final double @NotNull [] input,
                                                       final int rows,
                                                       final int columns) {
    final int diagonals = Math.max(rows, columns);
    final var sums = new double[diagonals];
    final var counts = new long[diagonals];
    for (int row = 0; row < rows; row++) {
      final int rowOffset = row * columns;
      for (int column = 0; column < columns; column++) {
        final int diagonal = Math.abs(column - row);
        final var signal = sanitizePositive(input[rowOffset + column]);
        if (signal > 0.0d) {
          sums[diagonal] += signal;
          counts[diagonal]++;
        }
      }
    }
    final var means = new double[diagonals];
    for (int diagonal = 0; diagonal < diagonals; diagonal++) {
      means[diagonal] = counts[diagonal] == 0L ? 0.0d : sums[diagonal] / counts[diagonal];
    }
    return means;
  }

  private static void computeObservedOverExpectedJava(final double @NotNull [] input,
                                                      final double @NotNull [] diagonalMeans,
                                                      final int rows,
                                                      final int columns,
                                                      final double @NotNull [] output) {
    for (int row = 0; row < rows; row++) {
      final int rowOffset = row * columns;
      for (int column = 0; column < columns; column++) {
        final int offset = rowOffset + column;
        final var expected = diagonalMeans[Math.abs(column - row)];
        output[offset] = expected > 1.0e-12d && Double.isFinite(expected)
          ? sanitizePositive(input[offset]) / expected
          : 0.0d;
      }
    }
  }

  private static void computePostLogJava(final double @NotNull [] values,
                                         final double lnPostLogBase) {
    for (int i = 0; i < values.length; i++) {
      final var value = values[i];
      values[i] = Double.isFinite(value) && value > 0.0d
        ? Math.log1p(value) / lnPostLogBase
        : 0.0d;
    }
  }

  private static void aggregatePrecomputedMaxJava(final double @NotNull [] values,
                                                  final long @NotNull [] support,
                                                  final long queryStartPx,
                                                  final long queryEndPx,
                                                  final int bucketCount,
                                                  final double @NotNull [] outputValues,
                                                  final long @NotNull [] outputSupport) {
    final var span = Math.max(1L, queryEndPx - queryStartPx);
    final var bucketSpan = Math.max(1.0d, span / (double) bucketCount);
    for (int i = 0; i < bucketCount; i++) {
      final var startPx = queryStartPx + (long) Math.floor(i * bucketSpan);
      final var endPx = Math.min(queryEndPx, queryStartPx + (long) Math.ceil((i + 1) * bucketSpan));
      final var safeEndPx = Math.max(startPx + 1L, endPx);
      final int from = (int) Math.max(0L, Math.min(startPx, values.length - 1L));
      final int to = (int) Math.max(from + 1L, Math.min(safeEndPx, values.length));
      double maxValue = 0.0d;
      long supportSum = 0L;
      for (int idx = from; idx < to; idx++) {
        maxValue = Math.max(maxValue, values[idx]);
        supportSum += support[idx];
      }
      outputValues[i] = maxValue;
      outputSupport[i] = supportSum;
    }
  }

  private static double sanitizePositive(final double signal) {
    return !Double.isFinite(signal) || signal <= 0.0d ? 0.0d : signal;
  }

  private static double maxAbsDiff(final double @NotNull [] expected,
                                   final double @NotNull [] actual) {
    double max = 0.0d;
    for (int i = 0; i < expected.length; i++) {
      max = Math.max(max, Math.abs(expected[i] - actual[i]));
    }
    return max;
  }

  private static double nanosToMillis(final long nanos) {
    return nanos / 1_000_000.0d;
  }
}
