package ru.itmo.ctlab.hict.hict_server.handlers.tiles;

import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;
import ru.itmo.ctlab.hict.hict_library.visualization.SimpleVisualizationOptions;
import ru.itmo.ctlab.hict.hict_library.visualization.SignalDisplayMode;
import ru.itmo.ctlab.hict.hict_library.visualization.colormap.gradient.SimpleLinearGradient;
import java.awt.Color;
import static org.junit.jupiter.api.Assertions.*;

class TileSignalStatisticsTest {
  @Test void finiteRangeIncludesZerosAndNegativesButNotMaskedValues() {
    var values = new TileSignalStatistics.Accumulator();
    assertNull(values.snapshot().min());
    for (double value : new double[]{Double.NaN, 0, -7, 1e-12, 5, Double.POSITIVE_INFINITY}) values.accept(value);
    assertRange(values.snapshot(), -7d, 5d, 4);
  }

  @Test void pipelineObservesUnclippedSignalWithoutChangingPixels() {
    var input = new JsonObject().put("type", "binary").put("op", "MUL")
      .put("left", new JsonObject().put("type", "source").put("source", "PRIMARY"))
      .put("right", new JsonObject().put("type", "constant").put("value", 3));
    var color = new JsonObject().put("type", "colormap").put("input", input)
      .put("minSignal", 0).put("maxSignal", 10);
    var pipeline = RenderPipelineConfig.fromJson(new JsonObject().put("enabled", true)
      .put("upperExpression", color).put("lowerExpression", color));
    var options = new SimpleVisualizationOptions(-1, -1, false, false, false, false, .995,
      SignalDisplayMode.OBSERVED, new SimpleLinearGradient(32, Color.WHITE, Color.RED, 0, 10));
    var context = new RenderPipelineConfig.MutablePixelContext();
    var stats = new TileSignalStatistics();
    for (double raw : new double[]{-2, 0, 4, 10}) {
      context.primaryValue = raw;
      context.signalStatistics = null;
      int without = pipeline.evaluateSourceArgb("PRIMARY", context, options);
      context.signalStatistics = stats;
      assertEquals(without, pipeline.evaluateSourceArgb("PRIMARY", context, options));
    }
    var result = stats.snapshot("PRIMARY", 1000, "revision");
    assertEquals(1, result.processed().size());
    assertEquals(input, new JsonObject(result.processed().getFirst().expression()));
    assertRange(result.processed().getFirst().range(), -6d, 30d, 4);
  }

  @Test void requestsAreIsolatedAndSnapshotsDoNotChange() {
    var first = new TileSignalStatistics();
    first.traditional(new double[][]{{0, 2}, {3, Double.NaN}});
    var saved = first.snapshot("PRIMARY", 1000, "a");
    first.traditional(new double[][]{{100}});
    assertEquals(3d, saved.processed().getFirst().range().max());
    assertTrue(new TileSignalStatistics().snapshot("SECONDARY", 5000, "b").processed().isEmpty());
  }

  private static void assertRange(TileSignalStatistics.Range range, double min, double max, long count) {
    assertEquals(min, range.min());
    assertEquals(max, range.max());
    assertEquals(count, range.count());
  }

  private static long histogramTotal(TileSignalStatistics.Range range) {
    return range.histogram().buckets().stream().mapToLong(bucket -> bucket[1]).sum();
  }

  @Test void histogramCountsPositiveValuesInQuarterOctavesAndZerosSeparately() {
    var values = new TileSignalStatistics.Accumulator();
    for (double value : new double[]{0, 0, -3, 1, 1.24, 1.25, 1.99, 2, 0.0087, Double.NaN}) values.accept(value);
    var range = values.snapshot();
    assertEquals(2L, range.histogram().zeros());
    assertEquals(6L, histogramTotal(range), "only finite positive values are bucketed");
    int base = -TileSignalStatistics.HISTOGRAM_MIN_EXPONENT * 4;
    assertEquals(base, TileSignalStatistics.Accumulator.bucket(1));
    assertEquals(base, TileSignalStatistics.Accumulator.bucket(1.24));
    assertEquals(base + 1, TileSignalStatistics.Accumulator.bucket(1.25));
    assertEquals(base + 3, TileSignalStatistics.Accumulator.bucket(1.99));
    assertEquals(base + 4, TileSignalStatistics.Accumulator.bucket(2));
    // Every bucket lower bound is <= value < upper bound.
    for (double value : new double[]{0.0087, 3e-9, 7.5, 1690, 12345.678}) {
      int b = TileSignalStatistics.Accumulator.bucket(value);
      double lower = Math.scalb(1 + (b % 4) / 4.0, b / 4 + TileSignalStatistics.HISTOGRAM_MIN_EXPONENT);
      double upper = Math.scalb(1 + (b % 4 + 1) / 4.0, b / 4 + TileSignalStatistics.HISTOGRAM_MIN_EXPONENT);
      assertTrue(lower <= value && value < upper, value + " in [" + lower + ", " + upper + ")");
    }
    assertEquals(0, TileSignalStatistics.Accumulator.bucket(Double.MIN_VALUE));
    assertEquals(TileSignalStatistics.HISTOGRAM_OCTAVES * 4 - 1, TileSignalStatistics.Accumulator.bucket(Double.MAX_VALUE));
  }

  @Test void emptyOrNonPositiveAccumulatorsOmitHistogramBuckets() {
    assertNull(new TileSignalStatistics.Accumulator().snapshot().histogram());
    var values = new TileSignalStatistics.Accumulator();
    values.accept(-1);
    assertNull(values.snapshot().histogram());
  }
}
