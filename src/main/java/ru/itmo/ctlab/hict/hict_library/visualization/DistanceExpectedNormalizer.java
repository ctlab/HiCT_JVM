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

package ru.itmo.ctlab.hict.hict_library.visualization;

import org.jetbrains.annotations.NotNull;

public final class DistanceExpectedNormalizer {
  private DistanceExpectedNormalizer() {
  }

  public static double @NotNull [][] transformSignal(final double @NotNull [][] signal,
                                                     final long startRowPx,
                                                     final long startColPx,
                                                     final @NotNull SignalDisplayMode displayMode) {
    if (signal.length == 0 || displayMode == SignalDisplayMode.OBSERVED) {
      return signal;
    }
    final var rowCount = signal.length;
    final var columnCount = signal[0].length;
    if (columnCount == 0) {
      return signal;
    }

    final var diagonalStats = DiagonalStats.fromSignal(signal, startRowPx, startColPx);
    final var result = new double[rowCount][columnCount];
    for (int row = 0; row < rowCount; row++) {
      for (int col = 0; col < columnCount; col++) {
        final var observed = sanitizeSignal(signal[row][col]);
        final var expected = diagonalStats.meanFor(row, col);
        if (displayMode == SignalDisplayMode.EXPECTED) {
          result[row][col] = expected;
        } else {
          result[row][col] =
            expected > 1e-12d && Double.isFinite(expected)
              ? observed / expected
              : 0.0d;
        }
      }
    }
    return result;
  }

  private static double sanitizeSignal(final double rawValue) {
    if (!Double.isFinite(rawValue) || rawValue <= 0.0d) {
      return 0.0d;
    }
    return rawValue;
  }

  private record DiagonalStats(long minDiagonal, long startRowPx, long startColPx, double[] means) {
    private static @NotNull DiagonalStats fromSignal(final double @NotNull [][] signal,
                                                    final long startRowPx,
                                                    final long startColPx) {
      final var rowCount = signal.length;
      final var columnCount = signal[0].length;
      final long[] cornerDiagonals = {
        Math.abs(startColPx - startRowPx),
        Math.abs((startColPx + columnCount - 1L) - startRowPx),
        Math.abs(startColPx - (startRowPx + rowCount - 1L)),
        Math.abs((startColPx + columnCount - 1L) - (startRowPx + rowCount - 1L))
      };
      long minDiagonal = cornerDiagonals[0];
      long maxDiagonal = cornerDiagonals[0];
      for (final var cornerDiagonal : cornerDiagonals) {
        minDiagonal = Math.min(minDiagonal, cornerDiagonal);
        maxDiagonal = Math.max(maxDiagonal, cornerDiagonal);
      }
      final int diagonalCount = (int) (maxDiagonal - minDiagonal + 1L);
      final var sums = new double[diagonalCount];
      final var counts = new int[diagonalCount];
      for (int row = 0; row < rowCount; row++) {
        for (int col = 0; col < columnCount; col++) {
          final var value = sanitizeSignal(signal[row][col]);
          if (value <= 0.0d) {
            continue;
          }
          final long diagonal = Math.abs((startColPx + col) - (startRowPx + row));
          final int diagonalIndex = (int) (diagonal - minDiagonal);
          sums[diagonalIndex] += value;
          counts[diagonalIndex] += 1;
        }
      }
      final var means = new double[diagonalCount];
      for (int index = 0; index < diagonalCount; index++) {
        means[index] = counts[index] > 0 ? (sums[index] / counts[index]) : 0.0d;
      }
      return new DiagonalStats(minDiagonal, startRowPx, startColPx, means);
    }

    private double meanFor(final int row, final int col) {
      final long diagonal = Math.abs((this.startColPx + col) - (this.startRowPx + row));
      final int diagonalIndex = (int) (diagonal - this.minDiagonal);
      if (diagonalIndex < 0 || diagonalIndex >= this.means.length) {
        return 0.0d;
      }
      return this.means[diagonalIndex];
    }
  }
}
