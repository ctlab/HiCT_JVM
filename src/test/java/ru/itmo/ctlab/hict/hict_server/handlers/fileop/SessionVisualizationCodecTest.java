package ru.itmo.ctlab.hict.hict_server.handlers.fileop;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;
import ru.itmo.ctlab.hict.hict_library.visualization.SimpleVisualizationOptions;
import ru.itmo.ctlab.hict.hict_library.visualization.SignalDisplayMode;
import ru.itmo.ctlab.hict.hict_library.visualization.CoolerWeightsNaNPolicy;
import ru.itmo.ctlab.hict.hict_library.visualization.colormap.gradient.SimpleLinearGradient;
import ru.itmo.ctlab.hict.hict_server.handlers.tiles.RenderPipelineConfig;
import java.awt.Color;
import static org.junit.jupiter.api.Assertions.*;

class SessionVisualizationCodecTest {
  static SimpleVisualizationOptions sampleOptions() {
    return new SimpleVisualizationOptions(2, 3, true, true, true, true, .975,
      SignalDisplayMode.OBSERVED_OVER_EXPECTED, CoolerWeightsNaNPolicy.REPLACE_NANS_WITH_ZERO,
      new SimpleLinearGradient(32, new Color(10, 20, 30, 2), new Color(200, 150, 100, 128), -5, 21));
  }
  static JsonObject sample() { return SessionVisualizationCodec.encode(sampleOptions()); }

  @Test void preservesEverySettingAcrossJsonSerialization() {
    var json = sample();
    var restored = SessionVisualizationCodec.decode(new JsonObject(json.encode()));
    assertEquals(json, SessionVisualizationCodec.encode(restored));
    assertEquals(2, restored.getPreLogBase());
    assertEquals(3, restored.getPostLogBase());
    assertTrue(restored.isApplyCoolerWeights());
    assertTrue(restored.isResolutionScaling());
    assertTrue(restored.isResolutionLinearScaling());
    assertTrue(restored.isAutoThresholdEnabled());
    assertEquals(.975, restored.getAutoThresholdQuantile());
    assertEquals(SignalDisplayMode.OBSERVED_OVER_EXPECTED, restored.getSignalDisplayMode());
    assertEquals(CoolerWeightsNaNPolicy.REPLACE_NANS_WITH_ZERO, restored.getCoolerWeightsNaNPolicy());
  }

  @Test void preservesAllByteAlphaValuesAndInterpolatedNumericRootColors() {
    var root = new JsonObject().put("type", "source").put("source", "PRIMARY");
    var pipeline = RenderPipelineConfig.fromJson(new JsonObject().put("enabled", true)
      .put("upperExpression", root).put("lowerExpression", root.copy()));
    var context = new RenderPipelineConfig.MutablePixelContext();
    for (int alpha = 0; alpha < 256; alpha++) {
      var original = new SimpleVisualizationOptions(-1, -1, false, false, false,
        new SimpleLinearGradient(32, new Color(10, 20, 30, alpha), new Color(200, 150, 100, 255-alpha), -5, 21));
      var restored = SessionVisualizationCodec.decode(new JsonObject(SessionVisualizationCodec.encode(original).encode()));
      var gradient = (SimpleLinearGradient)restored.getColormap();
      assertEquals(alpha, gradient.getStartColor().getAlpha());
      assertEquals(255-alpha, gradient.getEndColor().getAlpha());
      for (double value : new double[]{-10, -5, -2.5, 0, 4.125, 12.5, 21, 30}) {
        context.primaryValue = value;
        assertEquals(pipeline.evaluateArgb(true, context, original), pipeline.evaluateArgb(true, context, restored));
      }
    }
  }

  @Test void rejectsValuesThatTheLegacyDtoWouldCoerceOrReplace() {
    for (var json : new JsonObject[]{
      sample().put("postLogBase", 1), sample().put("preLogBase", "2"),
      sample().put("applyCoolerWeights", 1), sample().put("autoThresholdQuantile", 2),
      sample().put("signalDisplayMode", "UNKNOWN"), sample().put("coolerWeightsNaNPolicy", "UNKNOWN")}) {
      assertTrue(assertThrows(IllegalArgumentException.class, () -> SessionVisualizationCodec.decode(json))
        .getMessage().startsWith("visualizationOptions."));
    }
    var json = sample();
    json.getJsonObject("colormap").put("startColor", new JsonArray().add(0).add(0).add(0).add(2));
    assertTrue(assertThrows(IllegalArgumentException.class, () -> SessionVisualizationCodec.decode(json))
      .getMessage().contains("colormap.startColor[3]"));
    final var reversed = sample();
    reversed.getJsonObject("colormap").put("maxSignal", -6);
    assertTrue(assertThrows(IllegalArgumentException.class, () -> SessionVisualizationCodec.decode(reversed))
      .getMessage().contains("colormap.maxSignal"));
  }
}
