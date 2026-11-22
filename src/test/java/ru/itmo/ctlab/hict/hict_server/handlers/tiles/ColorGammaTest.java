package ru.itmo.ctlab.hict.hict_server.handlers.tiles;

import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;
import ru.itmo.ctlab.hict.hict_library.nativeprocessing.NativeProcessingService;
import ru.itmo.ctlab.hict.hict_library.visualization.SimpleVisualizationOptions;
import ru.itmo.ctlab.hict.hict_library.visualization.colormap.gradient.SimpleLinearGradient;
import ru.itmo.ctlab.hict.hict_server.dto.symmetric.visualization.SimpleLinearGradientDTO;
import ru.itmo.ctlab.hict.hict_server.handlers.fileop.SessionVisualizationCodec;
import java.awt.Color;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class ColorGammaTest {
  @Test void gammaChangesOnlyRgbaInterpolationAndPreservesDefaultEndpoints() {
    for (double gamma : new double[]{0.05, 0.5, 1, 2, 10, 100}) {
      var gradient = new SimpleLinearGradient(32, new Color(255,255,255,0), new Color(0,0,0,255), 10, 110, gamma);
      assertEquals(gradient.getStartColor(), gradient.mapSignal(-1));
      assertEquals(gradient.getEndColor(), gradient.mapSignal(500));
      double midpoint = 10 + 100 * Math.pow(0.5, 1 / gamma);
      assertEquals(128, gradient.mapSignal(midpoint).getAlpha(), 1);
      assertEquals(128, gradient.mapSignal(midpoint).getRed(), 1);
    }
    var gammaTwo = new SimpleLinearGradient(32, Color.WHITE, Color.BLACK, 0, 100, 2);
    assertEquals(191, gammaTwo.mapSignal(50).getRed(), 1);
    for (double invalid : new double[]{0, -1, 0.001, 101, Double.NaN, Double.POSITIVE_INFINITY})
      assertThrows(IllegalArgumentException.class, () -> new SimpleLinearGradient(32, Color.WHITE, Color.BLACK, 0, 1, invalid));
  }

  @Test void dtoAndSessionPreserveGammaWhileLegacyPayloadsDefaultToOne() {
    var json = new JsonObject().put("startColorRGBAString", "rgba(255,255,255,0)")
      .put("endColorRGBAString", "rgba(0,0,0,1)").put("minSignal", 0).put("maxSignal", 100);
    assertEquals(1, SimpleLinearGradientDTO.fromJSONObject(json).toEntity().getGamma());
    assertFalse(JsonObject.mapFrom(SimpleLinearGradientDTO.fromJSONObject(json)).containsKey("gamma"));
    assertEquals(2, JsonObject.mapFrom(SimpleLinearGradientDTO.fromJSONObject(json.copy().put("gamma", 2))).getDouble("gamma"));
    var gradient = SimpleLinearGradientDTO.fromJSONObject(json.copy().put("gamma", 2)).toEntity();
    assertEquals(2, gradient.getGamma());
    var options = new SimpleVisualizationOptions(-1, -1, false, false, false, gradient);
    var encoded = SessionVisualizationCodec.encode(options);
    var restored = SessionVisualizationCodec.decode(new JsonObject(encoded.encode()));
    assertEquals(encoded, SessionVisualizationCodec.encode(restored));
    assertEquals(2, ((SimpleLinearGradient)restored.getColormap()).getGamma());
    encoded.getJsonObject("colormap").remove("gamma");
    assertEquals(1, ((SimpleLinearGradient)SessionVisualizationCodec.decode(encoded).getColormap()).getGamma());
  }

  @Test void graphAndTraditionalColorsAgreeWithoutChangingNumericGraphSignal() {
    for (double gamma : new double[]{0.5, 1, 2, 4}) {
      var gradient = new SimpleLinearGradient(32, Color.WHITE, Color.BLACK, 0, 100, gamma);
      var options = new SimpleVisualizationOptions(-1,-1,false,false,false,gradient);
      var expression = new JsonObject().put("type","colormap").put("mode","LINEAR")
        .put("input",new JsonObject().put("type","source").put("source","PRIMARY"))
        .put("startColor","#ffffffff").put("endColor","#000000ff").put("minSignal",0).put("maxSignal",100).put("gamma",gamma);
      var config = RenderPipelineConfig.fromJson(new JsonObject().put("enabled",true)
        .put("upperExpression",expression).put("lowerExpression",expression.copy()));
      for (double value : new double[]{-5,0,25,50,75,100,105}) {
        var context = new RenderPipelineConfig.MutablePixelContext(); context.primaryValue = value;
        assertEquals(Math.max(0, Math.min(100, value)), config.evaluate(true, context), 1e-12);
        var actual = new Color(config.evaluateArgb(true,context,options),true);
        assertEquals(gradient.mapSignal(value).getRed(), actual.getRed(), 1);
      }
    }
  }

  @Test void multipointCurveSurvivesSessionsAndAgreesWithTheGraph() {
    var points = java.util.List.of(0.2,0.7,0.6,0.3);
    var gradient = new SimpleLinearGradient(32,Color.BLACK,Color.WHITE,0,1,1,points);
    var options = new SimpleVisualizationOptions(-1,-1,false,false,false,gradient);
    var restored = (SimpleLinearGradient)SessionVisualizationCodec.decode(SessionVisualizationCodec.encode(options)).getColormap();
    assertEquals(points,restored.getCurve());
    var expr = new JsonObject().put("type","colormap").put("mode","LINEAR")
      .put("input",new JsonObject().put("type","source").put("source","PRIMARY"))
      .put("startColor","#000000ff").put("endColor","#ffffffff").put("minSignal",0).put("maxSignal",1)
      .put("curve",new io.vertx.core.json.JsonArray(points));
    var graph = RenderPipelineConfig.fromJson(new JsonObject().put("enabled",true)
      .put("upperExpression",expr).put("lowerExpression",expr.copy()));
    for (double value : new double[]{0,0.1,0.2,0.4,0.6,0.8,1}) {
      var context = new RenderPipelineConfig.MutablePixelContext();context.primaryValue=value;
      assertEquals(restored.mapSignal(value).getRed(),new Color(graph.evaluateArgb(true,context,options),true).getRed(),1);
    }
    assertEquals(0.5,SimpleLinearGradient.transfer(0.4,1,points),1e-12);
    assertThrows(IllegalArgumentException.class,()->SimpleLinearGradient.validateCurve(java.util.List.of(0.7,1.0,0.5,0.1)));
  }

  @Test void identityCurveChangesColorWhilePreservingThePercentageThreshold() {
    var expr = new JsonObject().put("type", "colormap").put("mode", "MODDOTPLOT_IDENTITY")
      .put("input", new JsonObject().put("type", "source").put("source", "PRIMARY"))
      .put("minSignal", 80).put("maxSignal", 100);
    var graph = RenderPipelineConfig.fromJson(new JsonObject().put("enabled", true)
      .put("upperExpression", expr.copy().put("curve", new io.vertx.core.json.JsonArray(java.util.List.of(0.25, 0.75))))
      .put("lowerExpression", expr));
    var options = new SimpleVisualizationOptions(-1,-1,false,false,false,
      new SimpleLinearGradient(32,Color.WHITE,Color.BLACK,80,100));
    var pixel = new RenderPipelineConfig.MutablePixelContext();
    pixel.primaryValue = 85;
    int curved = graph.evaluateArgb(true, pixel, options);
    pixel.primaryValue = 95;
    assertEquals(graph.evaluateArgb(false, pixel, options), curved);
    pixel.primaryValue = 79;
    assertEquals(0, graph.evaluateArgb(true, pixel, options));
  }

  @Test void nativeAndJavaGammaColorsAgreeAndNeverModifyTheirInput() {
    var service = NativeProcessingService.getInstance();
    try {
      assumeTrue(service.setRequestedEnabled(true).enabled());
      double[][] values = {{0, 1, 25, 50, 75, 100}};
      for (double gamma : new double[]{0.5,1,2,4}) {
        var gradient = new SimpleLinearGradient(32, new Color(255,255,255,0), new Color(0,0,0,255), 0,100,gamma);
        var rgba = service.tryMapLinearGradientRgba(values, gradient);assertNotNull(rgba);
        for (int index=0;index<values[0].length;index++) {
          var expected=gradient.mapSignal(values[0][index]);
          assertEquals(expected.getRed(), Byte.toUnsignedInt(rgba[index*4]),1);
          assertEquals(expected.getAlpha(), Byte.toUnsignedInt(rgba[index*4+3]),1);
        }
      }
      assertArrayEquals(new double[]{0,1,25,50,75,100},values[0]);
    } finally { service.setRequestedEnabled(false); }
  }
}
