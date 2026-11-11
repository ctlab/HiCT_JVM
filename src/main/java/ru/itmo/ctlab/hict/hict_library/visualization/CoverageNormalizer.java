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

import java.util.function.BooleanSupplier;

/**
 * Square-root coverage normalization of a symmetric contact matrix.
 *
 * <p>Reference: Juicebox's {@code VC_SQRT} (aidenlab/Juicebox at 3c4936c0,
 * {@code NormalizationCalculations.computeVC} followed by
 * {@code NormalizationVectorUpdater.buildVCOrVCSQRT}). With {@code r_i} the coverage of bin
 * {@code i} (its row sum, the diagonal counted once), {@code d_i = sqrt(r_i)}, {@code A = sum_ij M_ij /
 * (d_i d_j)} and {@code S = sum_ij M_ij} over valid bins ({@code d_i > 0}), the divisor is
 * {@code v_i = f * d_i} with {@code f = sqrt(A / S)} and the normalized matrix is
 * {@code M_ij / (v_i v_j)}. Entries are given as the upper triangle; off-diagonal entries count twice
 * in {@code A} and {@code S}.
 *
 * <p>The factor {@code f} rescales the matrix so that its total is preserved by the normalization.
 * A bin with no contacts has no divisor, and its weight is {@code NaN}.
 *
 * <p>The domain is whatever matrix the caller streams in. HiCT streams the whole opened map, so
 * contacts between any two contigs count towards coverage. Juicebox computes the vector for each
 * chromosome separately, so values agree with it exactly only when the domain is the same.
 */
public final class CoverageNormalizer {
  /** Changes whenever the numerical recipe changes, so cached vectors are never reused across versions. */
  public static final String ALGORITHM_VERSION = "sqrt-coverage-1";

  private CoverageNormalizer() {
  }

  /** Receives one stored upper-triangle contact. */
  @FunctionalInterface
  public interface ContactVisitor {
    void accept(long row, long col, double value);
  }

  /** A matrix that can be streamed more than once. */
  @FunctionalInterface
  public interface ContactSource {
    void forEach(@NotNull ContactVisitor visitor);
  }

  public static final class CancelledException extends RuntimeException {
    public CancelledException() {
      super("Coverage normalization was cancelled");
    }
  }

  /**
   * @param divisor    {@code v_i}; zero where the bin has no contacts
   * @param sumFactor  {@code f}; {@code NaN} when the matrix has no valid entries
   * @param validBins  number of bins with a divisor
   */
  public record Vector(double @NotNull [] divisor, double sumFactor, long validBins) {
    /** Multiplier for the raw count: {@code 1 / v_i}, or {@code NaN} for a bin without contacts. */
    public double weight(final long bin) {
      final double v = divisor[(int) bin];
      return v > 0.0d ? 1.0d / v : Double.NaN;
    }

    public int length() {
      return divisor.length;
    }
  }

  public static long estimatedBytes(final long binCount) {
    return Math.multiplyExact(binCount, 16L);
  }

  public static @NotNull Vector compute(final long binCount,
                                        final @NotNull ContactSource source,
                                        final @NotNull BooleanSupplier cancelled) {
    if (binCount < 0 || binCount > Integer.MAX_VALUE - 8) {
      throw new IllegalArgumentException("Cannot normalize a matrix with " + binCount + " bins");
    }
    final int n = (int) binCount;
    final double[] coverage = new double[n];
    source.forEach((row, col, value) -> {
      if (cancelled.getAsBoolean()) {
        throw new CancelledException();
      }
      if (!Double.isFinite(value) || row < 0 || col < 0 || row >= n || col >= n) {
        return;
      }
      coverage[(int) row] += value;
      if (row != col) {
        coverage[(int) col] += value;
      }
    });

    final double[] divisor = new double[n];
    long valid = 0;
    for (int i = 0; i < n; i++) {
      final double d = Math.sqrt(coverage[i]);
      if (d > 0.0d && Double.isFinite(d)) {
        divisor[i] = d;
        valid++;
      }
    }

    final double[] sums = new double[2];
    source.forEach((row, col, value) -> {
      if (cancelled.getAsBoolean()) {
        throw new CancelledException();
      }
      if (!Double.isFinite(value) || row < 0 || col < 0 || row >= n || col >= n) {
        return;
      }
      final double dr = divisor[(int) row];
      final double dc = divisor[(int) col];
      if (dr > 0.0d && dc > 0.0d) {
        final double weight = row == col ? 1.0d : 2.0d;
        sums[0] += weight * value / (dr * dc);
        sums[1] += weight * value;
      }
    });
    final double factor = Math.sqrt(sums[0] / sums[1]);
    if (!Double.isFinite(factor) || factor <= 0.0d) {
      return new Vector(new double[n], Double.NaN, 0L);
    }
    for (int i = 0; i < n; i++) {
      divisor[i] *= factor;
    }
    return new Vector(divisor, factor, valid);
  }
}
