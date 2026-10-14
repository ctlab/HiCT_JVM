package ru.itmo.ctlab.hict.hict_server.handlers.tiles;

import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;
import ru.itmo.ctlab.hict.hict_library.visualization.SimpleVisualizationOptions;
import ru.itmo.ctlab.hict.hict_library.visualization.colormap.gradient.SimpleLinearGradient;
import java.awt.Color;
import static org.junit.jupiter.api.Assertions.*;

class ModDotPlotColormapTest {
  private static RenderPipelineConfig config(double minimum) {
    var expression = new JsonObject().put("type", "colormap").put("mode", "MODDOTPLOT_IDENTITY")
      .put("input", new JsonObject().put("type", "source").put("source", "SECONDARY"))
      .put("minSignal", minimum * 10000).put("maxSignal", 1000000);
    return RenderPipelineConfig.fromJson(new JsonObject().put("enabled", true)
      .put("upperExpression", expression).put("lowerExpression", expression));
  }
  private static int color(RenderPipelineConfig config, double value) {
    var pixel = new RenderPipelineConfig.MutablePixelContext(); pixel.secondaryValue = value;
    var options = new SimpleVisualizationOptions(-1, -1, false, false, false,
      new SimpleLinearGradient(32, Color.WHITE, Color.BLACK, 0, 1));
    return config.evaluateArgb(false, pixel, options);
  }
  @Test void masksMissingAndBelowThresholdScores() {
    var config = config(80);
    for (double value : new double[]{0, -1, 799999, Double.NaN, Double.POSITIVE_INFINITY})
      assertEquals(0, color(config, value));
  }
  @Test void usesScaledIdentityAndMultipleDistinctPaletteColors() {
    var config = config(80);
    assertEquals(0xff313695, color(config, 800000));
    assertEquals(0xffffffbf, color(config, 900000));
    assertEquals(0xffa50026, color(config, 1000000));
    var colors = new java.util.HashSet<Integer>();
    for (int percent = 80; percent <= 100; percent += 2) colors.add(color(config, percent * 10000));
    assertEquals(11, colors.size());
  }
  @Test void exactIdentityThresholdDoesNotDivideByZero() {
    assertEquals(0xffa50026, color(config(100), 1000000));
    assertEquals(0, color(config(100), 999999));
  }
}
