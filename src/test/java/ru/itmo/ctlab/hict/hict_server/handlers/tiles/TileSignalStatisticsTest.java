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
    assertEquals(new TileSignalStatistics.Range(-7d, 5d, 4), values.snapshot());
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
    assertEquals(new TileSignalStatistics.Range(-6d, 30d, 4), result.processed().getFirst().range());
  }

  @Test void requestsAreIsolatedAndSnapshotsDoNotChange() {
    var first = new TileSignalStatistics();
    first.traditional(new double[][]{{0, 2}, {3, Double.NaN}});
    var saved = first.snapshot("PRIMARY", 1000, "a");
    first.traditional(new double[][]{{100}});
    assertEquals(3d, saved.processed().getFirst().range().max());
    assertTrue(new TileSignalStatistics().snapshot("SECONDARY", 5000, "b").processed().isEmpty());
  }
}
