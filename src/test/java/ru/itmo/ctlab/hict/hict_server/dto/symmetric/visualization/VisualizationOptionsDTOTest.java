package ru.itmo.ctlab.hict.hict_server.dto.symmetric.visualization;

import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;
import ru.itmo.ctlab.hict.hict_library.visualization.CoolerWeightsNaNPolicy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class VisualizationOptionsDTOTest {

  @Test
  void emptyAutoThresholdQuantileFallsBackInsteadOfThrowing() {
    final var dto = VisualizationOptionsDTO.fromJSONObject(new JsonObject()
      .put("preLogBase", 0)
      .put("postLogBase", -1)
      .put("applyCoolerWeights", false)
      .put("resolutionScaling", false)
      .put("resolutionLinearScaling", false)
      .put("autoThresholdEnabled", false)
      .put("autoThresholdQuantile", "")
      .put("signalDisplayMode", "OBSERVED")
      .put("coolerWeightsNaNPolicy", "DISABLE_WEIGHTS")
      .put("colormap", new JsonObject()
        .put("colormapType", "SimpleLinearGradient")
        .put("startColorRGBAString", "rgba(255,255,255,1)")
        .put("endColorRGBAString", "rgba(232,0,0,1)")
        .put("minSignal", 0)
        .put("maxSignal", 0.003)
      ));

    final var options = dto.toEntity();
    assertFalse(options.isAutoThresholdEnabled());
    assertEquals(0.995d, options.getAutoThresholdQuantile(), 1e-12);
    assertEquals(CoolerWeightsNaNPolicy.DISABLE_WEIGHTS, options.getCoolerWeightsNaNPolicy());
  }

  @Test
  void coolerWeightNanPoliciesSanitizeNonFiniteWeights() {
    assertEquals(1.0d, CoolerWeightsNaNPolicy.DISABLE_WEIGHTS.sanitize(42.0d), 1e-12);
    assertEquals(1.0d, CoolerWeightsNaNPolicy.REPLACE_NANS_WITH_ONE.sanitize(Double.NaN), 1e-12);
    assertEquals(0.0d, CoolerWeightsNaNPolicy.REPLACE_NANS_WITH_ZERO.sanitize(Double.POSITIVE_INFINITY), 1e-12);
    assertEquals(0.25d, CoolerWeightsNaNPolicy.REPLACE_NANS_WITH_ZERO.sanitize(0.25d), 1e-12);
  }
  @Test
  void preservesEveryEightBitAlphaThroughTheApiDecimalFormat() {
    for (int alpha = 0; alpha <= 255; alpha++) {
      var start = String.format(java.util.Locale.US, "rgba(12,34,56,%.6f)", alpha / 255.0d);
      var end = String.format(java.util.Locale.US, "rgba(232,0,0,%.6f)", (255 - alpha) / 255.0d);
      var gradient = new SimpleLinearGradientDTO(start, end, 0, 10).toEntity();
      assertEquals(alpha, gradient.getStartColor().getAlpha(), "start alpha " + alpha);
      assertEquals(255 - alpha, gradient.getEndColor().getAlpha(), "end alpha " + alpha);
    }
    assertEquals(128,
      new SimpleLinearGradientDTO("rgba(12,34,56,0.5)", "rgba(232,0,0,1)", 0, 10).toEntity().getStartColor().getAlpha());
  }

  @Test
  void rejectsAlphaOutsideItsRangeBeforeIntegerConversion() {
    for (String alpha : new String[]{"-0.1", "1.1", "16843009.0039215686"}) {
      assertThrows(IllegalArgumentException.class,
        () -> new SimpleLinearGradientDTO("rgba(12,34,56," + alpha + ")", "rgba(232,0,0,1)", 0, 10).toEntity());
    }
  }

}
