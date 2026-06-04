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
import ru.itmo.ctlab.hict.hict_library.nativeprocessing.NativeProcessingService;

import java.util.Arrays;

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
      endColPx,
      null,
      startRowPx,
      endRowPx
    );
  }

  public static @NotNull SegmentedDiagonalAccumulator newSegmentedAccumulator(
    final int resolutionOrder,
    final long startRowPx,
    final long endRowPx,
    final long startColPx,
    final long endColPx,
    final PixelDomain @NotNull [] domains
  ) {
    return new SegmentedDiagonalAccumulator(
      resolutionOrder,
      startRowPx,
      endRowPx,
      startColPx,
      endColPx,
      domains
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
        new double[0],
        new SegmentProfile[0]
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
      stats.means().clone(),
      new SegmentProfile[0]
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
    final var nativeResult = NativeProcessingService.getInstance().tryTransformExpectedSignal(
      signal,
      startRowPx,
      startColPx,
      displayMode,
      diagonalMeans
    );
    if (nativeResult != null) {
      return nativeResult;
    }
    final var result = new double[rowCount][columnCount];
    for (int row = 0; row < rowCount; row++) {
      for (int col = 0; col < columnCount; col++) {
        final var observed = sanitizeSignal(signal[row][col]);
        final var absoluteRowPx = startRowPx + row;
        final var absoluteColPx = startColPx + col;
        final var expected = diagonalMeans.expectedValueForCoordinates(
          absoluteRowPx,
          absoluteColPx
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
                                double[] means,
                                SegmentProfile[] segmentProfiles) {
    public DiagonalProfile {
      means = means.clone();
      final var safeSegments = (segmentProfiles != null) ? segmentProfiles.clone() : new SegmentProfile[0];
      Arrays.sort(safeSegments, (left, right) -> Long.compare(left.startPx(), right.startPx()));
      segmentProfiles = safeSegments;
    }

    public boolean matchesResolutionWindow(final long queryStartRowPx,
                                           final long queryStartColPx,
                                           final int rowCount,
                                           final int columnCount) {
      return queryStartRowPx >= this.startRowPx
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

    public double expectedValueForCoordinates(final long rowPx,
                                              final long colPx) {
      if (this.segmentProfiles.length == 0) {
        return meanForAbsoluteDiagonal(Math.abs(colPx - rowPx));
      }
      final var segment = findSegmentContaining(rowPx);
      if (segment == null || !segment.contains(colPx)) {
        return 0.0d;
      }
      return segment.meanForAbsoluteDiagonal(Math.abs(colPx - rowPx));
    }

    private @Nullable SegmentProfile findSegmentContaining(final long px) {
      int left = 0;
      int right = this.segmentProfiles.length - 1;
      while (left <= right) {
        final int mid = (left + right) >>> 1;
        final var segment = this.segmentProfiles[mid];
        if (px < segment.startPx()) {
          right = mid - 1;
        } else if (px >= segment.endPx()) {
          left = mid + 1;
        } else {
          return segment;
        }
      }
      return null;
    }
  }

  public record PixelDomain(long startPx, long endPx) {
    public boolean contains(final long px) {
      return px >= this.startPx && px < this.endPx;
    }

    public boolean intersects(final long startPx,
                              final long endPx) {
      return this.endPx > startPx && this.startPx < endPx;
    }
  }

  public record SegmentProfile(long startPx,
                               long endPx,
                               long minDiagonal,
                               double[] means) {
    public SegmentProfile {
      means = means.clone();
    }

    public boolean contains(final long px) {
      return px >= this.startPx && px < this.endPx;
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
    private final @Nullable PixelDomain domain;
    private final long segmentStartPx;
    private final long segmentEndPx;
    private final long minDiagonal;
    private final double[] sums;
    private final long[] counts;

    private DiagonalAccumulator(final int resolutionOrder,
                                final long startRowPx,
                                final long endRowPx,
                                final long startColPx,
                                final long endColPx,
                                final @Nullable PixelDomain domain,
                                final long segmentStartPx,
                                final long segmentEndPx) {
      this.resolutionOrder = resolutionOrder;
      this.startRowPx = startRowPx;
      this.endRowPx = Math.max(startRowPx, endRowPx);
      this.startColPx = startColPx;
      this.endColPx = Math.max(startColPx, endColPx);
      this.domain = domain;
      this.segmentStartPx = segmentStartPx;
      this.segmentEndPx = Math.max(segmentStartPx, segmentEndPx);
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

    private static @NotNull DiagonalAccumulator forDomain(final int resolutionOrder,
                                                          final long viewStartRowPx,
                                                          final long viewEndRowPx,
                                                          final long viewStartColPx,
                                                          final long viewEndColPx,
                                                          final @NotNull PixelDomain domain) {
      final var domainRowStartPx = Math.max(viewStartRowPx, domain.startPx());
      final var domainRowEndPx = Math.min(viewEndRowPx, domain.endPx());
      final var domainColStartPx = Math.max(viewStartColPx, domain.startPx());
      final var domainColEndPx = Math.min(viewEndColPx, domain.endPx());
      return new DiagonalAccumulator(
        resolutionOrder,
        domainRowStartPx,
        domainRowEndPx,
        domainColStartPx,
        domainColEndPx,
        domain,
        domain.startPx(),
        domain.endPx()
      );
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
        final long absoluteRowPx = chunkStartRowPx + row;
        if (this.domain != null && !this.domain.contains(absoluteRowPx)) {
          continue;
        }
        for (int col = 0; col < columnCount; col++) {
          final var value = sanitizeSignal(signal[row][col]);
          if (value <= 0.0d) {
            continue;
          }
          final long absoluteColPx = chunkStartColPx + col;
          if (this.domain != null && !this.domain.contains(absoluteColPx)) {
            continue;
          }
          final long diagonal = Math.abs(absoluteColPx - absoluteRowPx);
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
        means,
        new SegmentProfile[0]
      );
    }

    private @NotNull SegmentProfile toSegmentProfile() {
      final var means = new double[this.sums.length];
      for (int index = 0; index < this.sums.length; index++) {
        means[index] =
          this.counts[index] > 0L
            ? (this.sums[index] / this.counts[index])
            : 0.0d;
      }
      return new SegmentProfile(
        this.segmentStartPx,
        this.segmentEndPx,
        this.minDiagonal,
        means
      );
    }
  }

  public static final class SegmentedDiagonalAccumulator {
    private final int resolutionOrder;
    private final long startRowPx;
    private final long endRowPx;
    private final long startColPx;
    private final long endColPx;
    private final ScopedAccumulator[] scopedAccumulators;

    private SegmentedDiagonalAccumulator(final int resolutionOrder,
                                         final long startRowPx,
                                         final long endRowPx,
                                         final long startColPx,
                                         final long endColPx,
                                         final PixelDomain @NotNull [] domains) {
      this.resolutionOrder = resolutionOrder;
      this.startRowPx = startRowPx;
      this.endRowPx = endRowPx;
      this.startColPx = startColPx;
      this.endColPx = endColPx;
      this.scopedAccumulators = Arrays.stream(domains)
        .filter(domain -> domain != null && domain.endPx() > domain.startPx())
        .map(domain -> new ScopedAccumulator(
          domain,
          DiagonalAccumulator.forDomain(
            resolutionOrder,
            startRowPx,
            endRowPx,
            startColPx,
            endColPx,
            domain
          )
        ))
        .filter(scoped -> scoped.accumulator().sums.length > 0)
        .toArray(ScopedAccumulator[]::new);
    }

    public void addSignal(final double @NotNull [][] signal,
                          final long chunkStartRowPx,
                          final long chunkStartColPx) {
      if (signal.length == 0 || this.scopedAccumulators.length == 0) {
        return;
      }
      final var chunkRowEndPx = chunkStartRowPx + signal.length;
      final var chunkColEndPx = chunkStartColPx + signal[0].length;
      for (final var scoped : this.scopedAccumulators) {
        if (!scoped.domain().intersects(chunkStartRowPx, chunkRowEndPx)) {
          continue;
        }
        if (!scoped.domain().intersects(chunkStartColPx, chunkColEndPx)) {
          continue;
        }
        scoped.accumulator().addSignal(signal, chunkStartRowPx, chunkStartColPx);
      }
    }

    public @NotNull DiagonalProfile toProfile() {
      if (this.scopedAccumulators.length == 0) {
        return new DiagonalProfile(
          this.resolutionOrder,
          this.startRowPx,
          this.endRowPx,
          this.startColPx,
          this.endColPx,
          0L,
          new double[0],
          new SegmentProfile[0]
        );
      }

      long minDiagonal = Long.MAX_VALUE;
      long maxDiagonal = Long.MIN_VALUE;
      boolean hasAnyDiagonal = false;
      for (final var scoped : this.scopedAccumulators) {
        final var accumulator = scoped.accumulator();
        if (accumulator.sums.length == 0) {
          continue;
        }
        hasAnyDiagonal = true;
        minDiagonal = Math.min(minDiagonal, accumulator.minDiagonal);
        maxDiagonal = Math.max(
          maxDiagonal,
          accumulator.minDiagonal + accumulator.sums.length - 1L
        );
      }

      final double[] means;
      final long overallMinDiagonal;
      if (!hasAnyDiagonal) {
        means = new double[0];
        overallMinDiagonal = 0L;
      } else {
        overallMinDiagonal = minDiagonal;
        final var diagonalCount = (int) (maxDiagonal - minDiagonal + 1L);
        final var sums = new double[diagonalCount];
        final var counts = new long[diagonalCount];
        for (final var scoped : this.scopedAccumulators) {
          final var accumulator = scoped.accumulator();
          for (int index = 0; index < accumulator.sums.length; index++) {
            final int globalIndex = (int) ((accumulator.minDiagonal + index) - minDiagonal);
            sums[globalIndex] += accumulator.sums[index];
            counts[globalIndex] += accumulator.counts[index];
          }
        }
        means = new double[diagonalCount];
        for (int index = 0; index < diagonalCount; index++) {
          means[index] =
            counts[index] > 0L
              ? (sums[index] / counts[index])
              : 0.0d;
        }
      }

      final var segments = Arrays.stream(this.scopedAccumulators)
        .map(scoped -> scoped.accumulator().toSegmentProfile())
        .toArray(SegmentProfile[]::new);
      return new DiagonalProfile(
        this.resolutionOrder,
        this.startRowPx,
        this.endRowPx,
        this.startColPx,
        this.endColPx,
        overallMinDiagonal,
        means,
        segments
      );
    }

    private record ScopedAccumulator(@NotNull PixelDomain domain,
                                     @NotNull DiagonalAccumulator accumulator) {
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
        this.means.clone(),
        new SegmentProfile[0]
      );
    }
  }
}
