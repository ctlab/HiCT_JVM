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

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.itmo.ctlab.hict.hict_library.visualization.CoverageNormalizer;


/**
 * Which per-bin weights a map multiplies its counts by when weights are applied.
 *
 * <p>{@link Mode#FILE} uses the balancing weights stored in the file (Cooler). {@link Mode#SQRT_COVERAGE}
 * uses vectors computed by {@link CoverageNormalizer} for every resolution. The vectors are published as
 * one immutable {@link Prepared} object, and the mode only moves to coverage once it exists, so a tile
 * is never built from some resolutions normalized and others not. A reader takes {@link #snapshot()}
 * once and uses it for the whole query.
 */
public final class NormalizationWeights {
  public enum Mode {
    FILE,
    SQRT_COVERAGE
  }

  /** Weights for every resolution, indexed by resolution order, then by bin along the source axis. */
  public record Prepared(@NotNull String algorithmVersion,
                         double @NotNull [] @NotNull [] weightsByResolution,
                         long @NotNull [] validBinsByResolution) {
  }

  /** What one query should use. */
  public record Snapshot(@NotNull Mode mode, @Nullable Prepared prepared) {
    public boolean usesCoverage() {
      return mode == Mode.SQRT_COVERAGE && prepared != null;
    }
  }

  private volatile Prepared prepared;
  private volatile Mode mode = Mode.FILE;

  public @NotNull Snapshot snapshot() {
    final var current = prepared;
    return new Snapshot(current == null ? Mode.FILE : mode, current);
  }

  public @Nullable Prepared prepared() {
    return prepared;
  }

  public @NotNull Mode mode() {
    return snapshot().mode();
  }

  /** Publishes complete vectors. The mode is unchanged. */
  public void publish(final @NotNull Prepared next) {
    this.prepared = next;
  }

  /** Switches the mode. Coverage needs published vectors. */
  public void select(final @NotNull Mode next) {
    if (next == Mode.SQRT_COVERAGE && prepared == null) {
      throw new IllegalStateException("Square-root coverage has not been prepared for this map");
    }
    this.mode = next;
  }

  /** Drops the vectors, for example when the map's structure changes under them. */
  public void invalidate() {
    this.mode = Mode.FILE;
    this.prepared = null;
  }
}
