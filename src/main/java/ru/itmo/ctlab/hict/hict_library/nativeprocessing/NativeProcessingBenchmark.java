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

    final var input = new double[elementCount];
    final var rowWeights = new double[rows];
    final var columnWeights = new double[columns];
    fillSyntheticTile(input, rowWeights, columnWeights, rows, columns);

    final var processor = new NativeTileProcessor();
    final var loadReport = processor.ensureLoaded();
    System.out.println("HiCT native benchmark");
    System.out.println("  size: " + rows + " x " + columns + " (" + elementCount + " elements)");
    System.out.println("  iterations: warmup=" + warmupIterations + ", measured=" + measuredIterations);
    System.out.println("  native: " + loadReport.state() + ", version=" + loadReport.version() + ", source=" + loadReport.source());
    if (loadReport.state() != NativeTileProcessor.LoadState.LOADED) {
      System.out.println("  native unavailable: " + loadReport.reason());
      return;
    }

    final var javaOutput = new double[elementCount];
    final var nativeOutput = new double[elementCount];

    for (int i = 0; i < warmupIterations; i++) {
      computeJava(input, rowWeights, columnWeights, rows, columns, javaOutput);
      processor.computeBaseSignalDouble(input, rowWeights, columnWeights, rows, columns, Math.log(10.0d), 1.125d, 0.75d, true, true, true, nativeOutput);
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

    final double javaMillis = nanosToMillis(javaNanos) / measuredIterations;
    final double nativeMillis = nanosToMillis(nativeNanos) / measuredIterations;
    System.out.printf(Locale.ROOT, "  Java mean:   %.3f ms%n", javaMillis);
    System.out.printf(Locale.ROOT, "  Native mean: %.3f ms%n", nativeMillis);
    System.out.printf(Locale.ROOT, "  Speedup:     %.3fx%n", javaMillis / Math.max(nativeMillis, 1.0e-9d));
    System.out.printf(Locale.ROOT, "  Max abs diff: %.3g%n", maxAbsDiff);
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
