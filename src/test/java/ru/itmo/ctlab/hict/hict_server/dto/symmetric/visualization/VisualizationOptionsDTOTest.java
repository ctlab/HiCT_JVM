package ru.itmo.ctlab.hict.hict_server.dto.symmetric.visualization;

import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;
import ru.itmo.ctlab.hict.hict_library.visualization.CoolerWeightsNaNPolicy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

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
}
