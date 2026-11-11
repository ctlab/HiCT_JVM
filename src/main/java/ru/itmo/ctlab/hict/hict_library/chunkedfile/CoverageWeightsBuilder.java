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

package ru.itmo.ctlab.hict.hict_library.chunkedfile;

import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.resolution.ResolutionDescriptor;
import ru.itmo.ctlab.hict.hict_library.visualization.CoverageNormalizer;

import java.util.function.BooleanSupplier;
import java.util.function.DoubleConsumer;

/**
 * Computes square-root coverage weights for every resolution of one map.
 *
 * <p>Each resolution is scanned twice (see {@link CoverageNormalizer}). The result is returned as one
 * {@link NormalizationWeights.Prepared} object that the caller publishes atomically, or the call fails and
 * nothing changes. Memory is checked before any scan starts.
 */
@Slf4j
public final class CoverageWeightsBuilder {
  /** Largest number of bytes the weight vectors of one map may take, across all resolutions. */
  public static final long DEFAULT_BUDGET_BYTES = Long.getLong("HICT_COVERAGE_MAX_BYTES", 1L << 30);

  private CoverageWeightsBuilder() {
  }

  /** Bytes needed for the final vectors plus the working vector of the largest resolution. */
  public static long estimateBytes(final @NotNull ChunkedFile file) {
    long total = 0L;
    long largest = 0L;
    for (int order = 0; order < file.getResolutions().length; order++) {
      if (file.getStripeCount()[order] <= 0) {
        continue;
      }
      final long bins = file.getMatrixQueries().sourceAxisBins(order);
      total = Math.addExact(total, Math.multiplyExact(bins, 8L));
      largest = Math.max(largest, CoverageNormalizer.estimatedBytes(bins));
    }
    return Math.addExact(total, largest);
  }

  public static NormalizationWeights.@NotNull Prepared build(final @NotNull ChunkedFile file,
                                                              final long budgetBytes,
                                                              final @NotNull BooleanSupplier cancelled,
                                                              final @Nullable DoubleConsumer progress) {
    final long needed = estimateBytes(file);
    if (needed > budgetBytes) {
      throw new IllegalStateException(
        "Square-root coverage needs about " + (needed >> 20) + " MiB for this map but the limit is "
          + (budgetBytes >> 20) + " MiB (HICT_COVERAGE_MAX_BYTES)"
      );
    }
    final int levels = file.getResolutions().length;
    final double[][] weights = new double[levels][];
    final long[] valid = new long[levels];
    final int active = (int) java.util.stream.IntStream.range(0, levels).filter(o -> file.getStripeCount()[o] > 0).count();
    int done = 0;
    for (int order = 0; order < levels; order++) {
      if (file.getStripeCount()[order] <= 0) {
        weights[order] = new double[0];
        continue;
      }
      final var descriptor = ResolutionDescriptor.fromResolutionOrder(order);
      final long bins = file.getMatrixQueries().sourceAxisBins(order);
      final int finishedLevels = done;
      // Each resolution is read twice; report the position inside the current read too.
      final int[] pass = {0};
      final int levelIndex = done;
      final var vector = CoverageNormalizer.compute(
        bins,
        visitor -> {
          final int currentPass = pass[0]++;
          file.getMatrixQueries().scanStoredContacts(descriptor, visitor, cancelled, progress == null ? null
            : fraction -> progress.accept((levelIndex + (currentPass + fraction) / 2.0d) / Math.max(1, active)));
        },
        cancelled
      );
      final var level = new double[vector.length()];
      for (int i = 0; i < level.length; i++) {
        level[i] = vector.weight(i);
      }
      weights[order] = level;
      valid[order] = vector.validBins();
      done++;
      if (progress != null) {
        progress.accept(done / (double) Math.max(1, active));
      }
      log.debug("Coverage weights for resolution order {}: {} of {} bins valid ({} levels before)", order, valid[order], bins, finishedLevels);
    }
    return new NormalizationWeights.Prepared(CoverageNormalizer.ALGORITHM_VERSION, weights, valid);
  }
}
