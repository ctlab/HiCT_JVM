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
import org.jetbrains.annotations.Nullable;

public final class DistanceExpectedNormalizer {
  private DistanceExpectedNormalizer() {
  }

  public static @NotNull DiagonalAccumulator newAccumulator(final int resolutionOrder,
                                                            final long startRowPx,
                                                            final long endRowPx,
                                                            final long startColPx,
                                                            final long endColPx) {
    return new DiagonalAccumulator(
      resolutionOrder,
      startRowPx,
      endRowPx,
      startColPx,
      endColPx
    );
  }

  public static @NotNull DiagonalProfile buildProfile(final double @NotNull [][] signal,
                                                      final long startRowPx,
                                                      final long startColPx,
                                                      final int resolutionOrder) {
    if (signal.length == 0 || signal[0].length == 0) {
      return new DiagonalProfile(
        resolutionOrder,
        startRowPx,
        startRowPx,
        startColPx,
        startColPx,
        0L,
        new double[0]
      );
    }
    final var stats = DiagonalStats.fromSignal(signal, startRowPx, startColPx);
    return new DiagonalProfile(
      resolutionOrder,
      startRowPx,
      startRowPx + signal.length,
      startColPx,
      startColPx + signal[0].length,
      stats.minDiagonal(),
      stats.means().clone()
    );
  }

  public static double @NotNull [][] transformSignal(final double @NotNull [][] signal,
                                                     final long startRowPx,
                                                     final long startColPx,
                                                     final @NotNull SignalDisplayMode displayMode) {
    return transformSignal(signal, startRowPx, startColPx, displayMode, null);
  }

  public static double @NotNull [][] transformSignal(final double @NotNull [][] signal,
                                                     final long startRowPx,
                                                     final long startColPx,
                                                     final @NotNull SignalDisplayMode displayMode,
                                                     final @Nullable DiagonalProfile profile) {
    if (signal.length == 0 || displayMode == SignalDisplayMode.OBSERVED) {
      return signal;
    }
    final var rowCount = signal.length;
    final var columnCount = signal[0].length;
    if (columnCount == 0) {
      return signal;
    }

    final var diagonalMeans =
      profile != null && profile.matchesResolutionWindow(startRowPx, startColPx, rowCount, columnCount)
        ? profile
        : DiagonalStats.fromSignal(signal, startRowPx, startColPx).toProfile(-1);
    final var result = new double[rowCount][columnCount];
    for (int row = 0; row < rowCount; row++) {
      for (int col = 0; col < columnCount; col++) {
        final var observed = sanitizeSignal(signal[row][col]);
        final var expected = diagonalMeans.meanForAbsoluteDiagonal(
          Math.abs((startColPx + col) - (startRowPx + row))
        );
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

  public record DiagonalProfile(int resolutionOrder,
                                long startRowPx,
                                long endRowPx,
                                long startColPx,
                                long endColPx,
                                long minDiagonal,
                                double[] means) {
    public boolean matchesResolutionWindow(final long queryStartRowPx,
                                           final long queryStartColPx,
                                           final int rowCount,
                                           final int columnCount) {
      return this.means.length > 0
        && queryStartRowPx >= this.startRowPx
        && queryStartColPx >= this.startColPx
        && (queryStartRowPx + rowCount) <= this.endRowPx
        && (queryStartColPx + columnCount) <= this.endColPx;
    }

    public double meanForAbsoluteDiagonal(final long absoluteDiagonal) {
      final int diagonalIndex = (int) (absoluteDiagonal - this.minDiagonal);
      if (diagonalIndex < 0 || diagonalIndex >= this.means.length) {
        return 0.0d;
      }
      return this.means[diagonalIndex];
    }
  }

  public static final class DiagonalAccumulator {
    private final int resolutionOrder;
    private final long startRowPx;
    private final long endRowPx;
    private final long startColPx;
    private final long endColPx;
    private final long minDiagonal;
    private final double[] sums;
    private final long[] counts;

    private DiagonalAccumulator(final int resolutionOrder,
                                final long startRowPx,
                                final long endRowPx,
                                final long startColPx,
                                final long endColPx) {
      this.resolutionOrder = resolutionOrder;
      this.startRowPx = startRowPx;
      this.endRowPx = Math.max(startRowPx, endRowPx);
      this.startColPx = startColPx;
      this.endColPx = Math.max(startColPx, endColPx);
      if (this.endRowPx <= this.startRowPx || this.endColPx <= this.startColPx) {
        this.minDiagonal = 0L;
        this.sums = new double[0];
        this.counts = new long[0];
        return;
      }
      final var diagonalBounds = diagonalBounds(
        this.startRowPx,
        this.endRowPx - 1L,
        this.startColPx,
        this.endColPx - 1L
      );
      this.minDiagonal = diagonalBounds.minDiagonal();
      final var diagonalCount = (int) Math.max(
        0L,
        diagonalBounds.maxDiagonal() - diagonalBounds.minDiagonal() + 1L
      );
      this.sums = new double[diagonalCount];
      this.counts = new long[diagonalCount];
    }

    public void addSignal(final double @NotNull [][] signal,
                          final long chunkStartRowPx,
                          final long chunkStartColPx) {
      if (signal.length == 0 || this.sums.length == 0) {
        return;
      }
      final var rowCount = signal.length;
      final var columnCount = signal[0].length;
      if (columnCount == 0) {
        return;
      }
      for (int row = 0; row < rowCount; row++) {
        for (int col = 0; col < columnCount; col++) {
          final var value = sanitizeSignal(signal[row][col]);
          if (value <= 0.0d) {
            continue;
          }
          final long diagonal = Math.abs((chunkStartColPx + col) - (chunkStartRowPx + row));
          final int diagonalIndex = (int) (diagonal - this.minDiagonal);
          if (diagonalIndex < 0 || diagonalIndex >= this.sums.length) {
            continue;
          }
          this.sums[diagonalIndex] += value;
          this.counts[diagonalIndex] += 1L;
        }
      }
    }

    public @NotNull DiagonalProfile toProfile() {
      final var means = new double[this.sums.length];
      for (int index = 0; index < this.sums.length; index++) {
        means[index] =
          this.counts[index] > 0L
            ? (this.sums[index] / this.counts[index])
            : 0.0d;
      }
      return new DiagonalProfile(
        this.resolutionOrder,
        this.startRowPx,
        this.endRowPx,
        this.startColPx,
        this.endColPx,
        this.minDiagonal,
        means
      );
    }
  }

  private record DiagonalBounds(long minDiagonal, long maxDiagonal) {
  }

  private static @NotNull DiagonalBounds diagonalBounds(final long topRowPx,
                                                        final long bottomRowPx,
                                                        final long leftColPx,
                                                        final long rightColPx) {
    final long[] cornerDiagonals = {
      Math.abs(leftColPx - topRowPx),
      Math.abs(rightColPx - topRowPx),
      Math.abs(leftColPx - bottomRowPx),
      Math.abs(rightColPx - bottomRowPx)
    };
    long minDiagonal = cornerDiagonals[0];
    long maxDiagonal = cornerDiagonals[0];
    for (final var cornerDiagonal : cornerDiagonals) {
      maxDiagonal = Math.max(maxDiagonal, cornerDiagonal);
    }
    if (rightColPx < topRowPx) {
      minDiagonal = topRowPx - rightColPx;
    } else if (bottomRowPx < leftColPx) {
      minDiagonal = leftColPx - bottomRowPx;
    } else {
      minDiagonal = 0L;
    }
    return new DiagonalBounds(minDiagonal, maxDiagonal);
  }

  private record DiagonalStats(long minDiagonal, long startRowPx, long startColPx, double[] means) {
    private static @NotNull DiagonalStats fromSignal(final double @NotNull [][] signal,
                                                    final long startRowPx,
                                                    final long startColPx) {
      final var rowCount = signal.length;
      final var columnCount = signal[0].length;
      final var diagonalBounds = diagonalBounds(
        startRowPx,
        startRowPx + rowCount - 1L,
        startColPx,
        startColPx + columnCount - 1L
      );
      final var minDiagonal = diagonalBounds.minDiagonal();
      final var maxDiagonal = diagonalBounds.maxDiagonal();
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

    private @NotNull DiagonalProfile toProfile(final int resolutionOrder) {
      return new DiagonalProfile(
        resolutionOrder,
        this.startRowPx,
        Long.MAX_VALUE,
        this.startColPx,
        Long.MAX_VALUE,
        this.minDiagonal,
        this.means.clone()
      );
    }
  }
}
