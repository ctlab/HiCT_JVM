package ru.itmo.ctlab.hict.hict_server.handlers.tiles;

import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;
import ru.itmo.ctlab.hict.hict_library.visualization.SimpleVisualizationOptions;
import ru.itmo.ctlab.hict.hict_library.visualization.colormap.gradient.SimpleLinearGradient;
import java.awt.Color;
import static org.junit.jupiter.api.Assertions.*;

class RenderPipelineObservedExpectedPaletteTest {
  @Test void customRatioPaletteHasTheSameNeutralEndpointsAlphaAndLogInterpolation() {
    final var color = new JsonObject().put("type", "colormap").put("mode", "OBSERVED_OVER_EXPECTED")
      .put("input", new JsonObject().put("type", "source").put("source", "PRIMARY"))
      .put("startColor", "#0a141e50").put("endColor", "#96a0aac8")
      .put("minSignal", 0.5).put("maxSignal", 2)
      .put("min", new JsonObject().put("type", "constant").put("value", 0.25))
      .put("max", new JsonObject().put("type", "constant").put("value", 8));
    final var config = RenderPipelineConfig.fromJson(new JsonObject().put("enabled", true)
      .put("upperExpression", color).put("lowerExpression", color));
    final var options = new SimpleVisualizationOptions(-1,-1,false,false,false,
      new SimpleLinearGradient(32,Color.WHITE,Color.BLACK,0,1));
    final var context = new RenderPipelineConfig.MutablePixelContext();
    double[] ratios = {0,0.25,0.5,1,Math.sqrt(8),8,Double.NaN,-1};
    int[] expected = {0x500a141e,0x500a141e,0xa8858a8f,0xffffffff,0xe4cbd0d5,0xc896a0aa,0,0};
    for (int i=0;i<ratios.length;i++) {
      context.primaryValue=ratios[i];
      assertEquals(expected[i],config.evaluateArgb(true,context,options),"ratio "+ratios[i]);
      assertEquals(expected[i],config.evaluateSourceArgb("PRIMARY",true,context,options));
    }
    assertEquals(color,config.toJson().getJsonObject("upperExpression"));
  }
}
