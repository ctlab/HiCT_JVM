package ru.itmo.ctlab.hict.hict_server.handlers.tiles;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

/** Request-local extrema of the values already read/rendered, never another matrix query. */
public final class TileSignalStatistics {
  /**
   * Positive values are counted in quarter-octave buckets taken directly from the IEEE-754 exponent
   * and the two leading mantissa bits (no logarithm per value). Bucket {@code b} covers
   * {@code [2^e * (1 + q/4), 2^e * (1 + (q+1)/4))} with {@code e = b / 4 + HISTOGRAM_MIN_EXPONENT}
   * and {@code q = b % 4}; values outside the exponent range are clamped into the first/last bucket.
   * Clients use these counts for robust (percentile) slider limits; min/max stay exact.
   */
  public static final int HISTOGRAM_MIN_EXPONENT = -64;
  public static final int HISTOGRAM_OCTAVES = 128;
  private static final int HISTOGRAM_BUCKETS = HISTOGRAM_OCTAVES * 4;

  public static final class Accumulator {
    private double min = Double.POSITIVE_INFINITY;
    private double max = Double.NEGATIVE_INFINITY;
    private long count;
    private long zeros;
    private long[] positive;
    public void accept(double value) {
      if (Double.isFinite(value)) {
        min = Math.min(min, value); max = Math.max(max, value); count++;
        if (value > 0.0d) {
          if (positive == null) positive = new long[HISTOGRAM_BUCKETS];
          positive[bucket(value)]++;
        } else if (value == 0.0d) {
          zeros++;
        }
      }
    }
    static int bucket(final double value) {
      final int exponent = Math.getExponent(value);
      if (exponent < HISTOGRAM_MIN_EXPONENT) return 0;
      if (exponent >= HISTOGRAM_MIN_EXPONENT + HISTOGRAM_OCTAVES) return HISTOGRAM_BUCKETS - 1;
      final int quarter = (int) ((Double.doubleToRawLongBits(value) >>> 50) & 0x3L);
      return ((exponent - HISTOGRAM_MIN_EXPONENT) << 2) | quarter;
    }
    public Range snapshot() {
      List<long[]> buckets = null;
      if (positive != null) {
        buckets = new ArrayList<>();
        for (int i = 0; i < positive.length; i++) if (positive[i] != 0L) buckets.add(new long[]{i, positive[i]});
      }
      return new Range(count == 0 ? null : min, count == 0 ? null : max, count,
        buckets == null ? null : new Histogram(HISTOGRAM_MIN_EXPONENT, zeros, buckets));
    }
  }
  /** Sparse quarter-octave histogram: {@code buckets} holds {@code [bucketIndex, count]} pairs. */
  public record Histogram(int minExponent, long zeros, List<long[]> buckets) {}
  public record Range(Double min, Double max, long count, Histogram histogram) {
    public Range(Double min, Double max, long count) { this(min, max, count, null); }
  }
  public record Signal(String expression, Range range) {}
  public record Snapshot(String source, long bpResolution, String revision, Range raw, List<Signal> processed) {}
  public final Accumulator raw = new Accumulator();
  private final LinkedHashMap<String, Accumulator> processed = new LinkedHashMap<>();
  public Accumulator signal(String expression) { return processed.computeIfAbsent(expression, key -> new Accumulator()); }
  public void traditional(double[][] values) {
    final var accumulator = signal("");
    for (final var row : values) for (final var value : row) accumulator.accept(value);
  }
  public Snapshot snapshot(String source, long bpResolution, String revision) {
    return new Snapshot(source, bpResolution, revision, raw.snapshot(), processed.entrySet().stream()
      .map(entry -> new Signal(entry.getKey(), entry.getValue().snapshot())).toList());
  }
}
