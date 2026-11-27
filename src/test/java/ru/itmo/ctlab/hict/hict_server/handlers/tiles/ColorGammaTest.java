package ru.itmo.ctlab.hict.hict_server.handlers.tiles;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;
import ru.itmo.ctlab.hict.hict_library.nativeprocessing.NativeProcessingService;
import ru.itmo.ctlab.hict.hict_library.visualization.SimpleVisualizationOptions;
import ru.itmo.ctlab.hict.hict_library.visualization.colormap.gradient.SimpleLinearGradient;
import ru.itmo.ctlab.hict.hict_server.dto.symmetric.visualization.SimpleLinearGradientDTO;
import ru.itmo.ctlab.hict.hict_server.handlers.fileop.SessionVisualizationCodec;
import java.awt.Color;
import java.nio.charset.StandardCharsets;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class ColorGammaTest {
  private record TransferCase(double input, List<Double> curve, double weight) {}

  @Test void bothSmoothModesMatchGoldenVectorsAndFindTheInflectionAtTheKnot() throws Exception {
    try (var vectors = getClass().getResourceAsStream("/color-curve-vectors.json")) {
      var cases = new JsonArray(new String(vectors.readAllBytes(), StandardCharsets.UTF_8));
      for (int i = 0; i < cases.size(); i++) {
        var item = cases.getJsonObject(i);
        var curve = SimpleLinearGradientDTO.readCurve(new JsonObject().put("curve", item.getJsonArray("points")));
        String mode = item.getString("mode");
        double gamma = item.getDouble("gamma");
        var transfer = SimpleLinearGradient.prepareTransfer(gamma, curve, mode);
        for (int s = 0; s < item.getJsonArray("samples").size(); s++) {
          var sample = item.getJsonArray("samples").getJsonArray(s);
          double input = sample.getDouble(0);
          double output = transfer.applyAsDouble(input);
          assertEquals(sample.getDouble(1), output, 1e-9, mode + " " + item.getString("name") + " at " + input);
          if (mode == null) assertEquals(SimpleLinearGradient.transfer(input, gamma, curve), output);
        }
      }
    }
    for (var mode : List.of("smooth-value", "smooth-log")) {
      java.util.function.DoubleUnaryOperator coordinate = mode.equals("smooth-log")
        ? x -> x <= 0 ? 0 : Math.pow(10, -10 + 10 * x) : x -> x;
      var transfer = SimpleLinearGradient.prepareTransfer(1, List.of(0.66, 0.38), mode);
      java.util.function.DoubleUnaryOperator f = x -> transfer.applyAsDouble(coordinate.applyAsDouble(x));
      double h = 1e-7;
      java.util.function.DoubleUnaryOperator secondDifference = x -> f.applyAsDouble(x + h) - 2 * f.applyAsDouble(x) + f.applyAsDouble(x - h);
      assertTrue(secondDifference.applyAsDouble(0.66 - 1e-6) > 0, mode);
      assertTrue(secondDifference.applyAsDouble(0.66 + 1e-6) < 0, mode);
      var knots = List.of(0.0, 0.17, 0.15, 0.73, 0.53, 0.33, 0.8, 0.37, 1.0, 0.2);
      var spline = SimpleLinearGradient.prepareTransfer(1, knots, mode);
      for (int i = 2; i < knots.size(); i += 2) {
        double start = knots.get(i - 2), end = knots.get(i), a = knots.get(i - 1), b = knots.get(i + 1);
        for (int j = 0; j <= 100; j++) {
          double y = spline.applyAsDouble(coordinate.applyAsDouble(start + (end - start) * j / 100));
          assertTrue(y >= Math.min(a, b) - 1e-12 && y <= Math.max(a, b) + 1e-12, mode);
        }
      }
    }
  }

  @Test void explicitEndpointsSetColorWeightsAtAndBeyondTheBounds() {
    final var right = List.of(0.85, 1.0, 1.0, 0.0);
    final var cases = List.of(
      new TransferCase(0.5, null, 0.5), new TransferCase(0.5, List.of(0.5, 0.8), 0.8),
      new TransferCase(0, List.of(0.0, 0.25), 0.25), new TransferCase(-3, List.of(0.0, 0.25), 0.25),
      new TransferCase(0.5, List.of(0.0, 0.25), 0.625), new TransferCase(1, right, 0),
      new TransferCase(7, right, 0), new TransferCase(0.85, right, 1),
      new TransferCase(0.925, right, 0.5)
    );
    for (var sample : cases) {
      assertEquals(sample.weight(), SimpleLinearGradient.transfer(sample.input(), 1, sample.curve()), 1e-12);
    }
    assertEquals(List.of(0.0, 0.2, 0.5, 0.5, 1.0, 0.0),
      SimpleLinearGradient.validateCurve(List.of(0.0, 0.2, 0.5, 0.5, 1.0, 0.0)));
    for (var invalid : List.of(List.of(0.5, 0.5, 0.0, 0.2), List.of(1.0, 0.0, 0.5, 0.5), List.of(0.0, 1.2))) {
      assertThrows(IllegalArgumentException.class, () -> SimpleLinearGradient.validateCurve(invalid));
    }
    assertEquals(0.5, SimpleLinearGradient.transfer(0.5, 2, List.of()));
  }

  @Test void endpointWeightsReachRenderedPixelsAndRoundTripThroughSession() {
    final var start = new Color(11, 22, 33, 0);
    final var end = Color.RED;
    final var points = List.of(0.0, 0.25, 0.85, 1.0, 1.0, 0.0);
    final var gradient = new SimpleLinearGradient(32, start, end, 10, 110, 1, points);
    final var options = new SimpleVisualizationOptions(-1, -1, false, false, false, gradient);
    final var restored = (SimpleLinearGradient) SessionVisualizationCodec.decode(
      SessionVisualizationCodec.encode(options)).getColormap();
    assertEquals(points, restored.getCurve());
    assertEquals(start.getRGB(), restored.mapSignal(120).getRGB());
    final var expression = new JsonObject().put("type", "colormap").put("mode", "LINEAR")
      .put("input", new JsonObject().put("type", "source").put("source", "PRIMARY"))
      .put("startColor", "#0b162100").put("endColor", "#ff0000ff")
      .put("minSignal", 10).put("maxSignal", 110).put("curve", new io.vertx.core.json.JsonArray(points));
    final var config = RenderPipelineConfig.fromJson(new JsonObject().put("enabled", true)
      .put("upperExpression", expression).put("lowerExpression", expression.copy()));
    final var pixel = new RenderPipelineConfig.MutablePixelContext();
    pixel.primaryValue = 120;
    assertEquals(start.getRGB(), config.evaluateArgb(true, pixel, options));
  }

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

  @Test void smoothModeRoundTripsAndColorsTheSameSignalThroughEveryRenderer() {
    var palette = new JsonObject().put("startColorRGBAString", "rgba(255,255,255,1)")
      .put("endColorRGBAString", "rgba(255,0,0,1)").put("minSignal", 0).put("maxSignal", 100)
      .put("curve", new JsonArray(List.of(0.66, 0.38))).put("curveMode", "smooth-log");
    var dto = SimpleLinearGradientDTO.fromJSONObject(palette);
    var gradient = dto.toEntity();
    assertEquals("smooth-log", JsonObject.mapFrom(dto).getString("curveMode"));
    var options = new SimpleVisualizationOptions(-1,-1,false,false,false,gradient);
    var restored = (SimpleLinearGradient) SessionVisualizationCodec.decode(SessionVisualizationCodec.encode(options)).getColormap();
    assertEquals(gradient.getCurveMode(), restored.getCurveMode());
    var expr = new JsonObject().put("type","colormap").put("mode","LINEAR")
      .put("input", new JsonObject().put("type", "source").put("source", "PRIMARY"))
      .put("startColor", "#ffffffff").put("endColor", "#ff0000ff").put("minSignal", 0).put("maxSignal", 100)
      .put("curveMode", "smooth-log").put("curve", palette.getJsonArray("curve"));
    var graph = RenderPipelineConfig.fromJson(new JsonObject().put("enabled", true)
      .put("upperExpression", expr).put("lowerExpression", expr.copy()));
    var context = new RenderPipelineConfig.MutablePixelContext();
    context.primaryValue = 0.02;
    assertEquals(restored.mapSignal(0.02).getGreen(), new Color(graph.evaluateArgb(true, context, options), true).getGreen());
    assertThrows(IllegalArgumentException.class, () -> SimpleLinearGradientDTO.fromJSONObject(palette.copy().put("curveMode", "linear")));
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
      for (String mode : new String[]{null, "smooth-value", "smooth-log"}) {
        final var endpoints = new SimpleLinearGradient(32, new Color(255,255,255,0), Color.BLACK,
          10, 75, 1, List.of(0.0, 0.25, 0.85, 1.0, 1.0, 0.0), mode);
        final var mapped = service.tryMapLinearGradientRgba(values, endpoints);
        assertNotNull(mapped);
        for (int index = 0; index < values[0].length; index++) {
          final var expected = endpoints.mapSignal(values[0][index]);
          assertEquals(expected.getRed(), Byte.toUnsignedInt(mapped[index * 4]), 1);
          assertEquals(expected.getAlpha(), Byte.toUnsignedInt(mapped[index * 4 + 3]), 1);
        }
      }
      assertArrayEquals(new double[]{0,1,25,50,75,100},values[0]);
    } finally { service.setRequestedEnabled(false); }
  }
}
