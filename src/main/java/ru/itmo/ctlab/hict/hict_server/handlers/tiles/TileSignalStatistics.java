package ru.itmo.ctlab.hict.hict_server.handlers.tiles;

import java.util.LinkedHashMap;
import java.util.List;

/** Request-local extrema of the values already read/rendered, never another matrix query. */
public final class TileSignalStatistics {
  public static final class Accumulator {
    private double min = Double.POSITIVE_INFINITY;
    private double max = Double.NEGATIVE_INFINITY;
    private long count;
    public void accept(double value) {
      if (Double.isFinite(value)) { min = Math.min(min, value); max = Math.max(max, value); count++; }
    }
    public Range snapshot() { return new Range(count == 0 ? null : min, count == 0 ? null : max, count); }
  }
  public record Range(Double min, Double max, long count) {}
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
