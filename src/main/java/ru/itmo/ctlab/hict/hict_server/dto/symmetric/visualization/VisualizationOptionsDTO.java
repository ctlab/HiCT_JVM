/*
 * MIT License
 *
 * Copyright (c) 2021-2026. Aleksandr Serdiukov, Anton Zamyatin, Aleksandr Sinitsyn, Vitalii Dravgelis and Computer Technologies Laboratory ITMO University team.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ru.itmo.ctlab.hict.hict_server.dto.symmetric.visualization;

import io.vertx.core.json.JsonObject;
import org.jetbrains.annotations.NotNull;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.visualization.CoolerWeightsNaNPolicy;
import ru.itmo.ctlab.hict.hict_library.visualization.SignalDisplayMode;
import ru.itmo.ctlab.hict.hict_library.visualization.SimpleVisualizationOptions;

public record VisualizationOptionsDTO(double preLogBase,
                                      double postLogBase,
                                      boolean applyCoolerWeights,
                                      boolean resolutionScaling,
                                      boolean resolutionLinearScaling,
                                      boolean autoThresholdEnabled,
                                      double autoThresholdQuantile,
                                      String signalDisplayMode,
                                      String coolerWeightsNaNPolicy,
                                      boolean coolerWeightsHaveNaNs,
                                      long coolerWeightsNaNCount,
                                      ColormapDTO colormap
) {
  public static @NotNull VisualizationOptionsDTO fromEntity(final @NotNull SimpleVisualizationOptions options, final @NotNull ChunkedFile chunkedFile) {
    return new VisualizationOptionsDTO(
      options.getPreLogBase(),
      options.getPostLogBase(),
      options.isApplyCoolerWeights(),
      options.isResolutionScaling(),
      options.isResolutionLinearScaling(),
      options.isAutoThresholdEnabled(),
      options.getAutoThresholdQuantile(),
      options.getSignalDisplayMode().name(),
      options.getCoolerWeightsNaNPolicy().name(),
      chunkedFile.getCoolerWeightsNaNCount().get() > 0L,
      chunkedFile.getCoolerWeightsNaNCount().get(),
      ColormapDTO.fromEntity(options.getColormap(), chunkedFile)
    );
  }

  public static @NotNull VisualizationOptionsDTO fromJSONObject(final @NotNull JsonObject json) {
    return new VisualizationOptionsDTO(
      getFiniteDouble(json, "preLogBase", -1.0d),
      getFiniteDouble(json, "postLogBase", -1.0d),
      json.getBoolean("applyCoolerWeights", false),
      json.getBoolean("resolutionScaling", false),
      json.getBoolean("resolutionLinearScaling", false),
      json.getBoolean("autoThresholdEnabled", false),
      clampAutoThresholdQuantile(getFiniteDouble(json, "autoThresholdQuantile", 0.995d)),
      json.getString("signalDisplayMode", SignalDisplayMode.OBSERVED.name()),
      CoolerWeightsNaNPolicy.fromRaw(json.getString("coolerWeightsNaNPolicy")).name(),
      false,
      0L,
      ColormapDTO.fromJSONObject(json.getJsonObject("colormap"))
    );
  }

  public @NotNull SimpleVisualizationOptions toEntity() {
    return new SimpleVisualizationOptions(
      this.preLogBase,
      this.postLogBase,
      this.applyCoolerWeights,
      this.resolutionScaling,
      this.resolutionLinearScaling,
      this.autoThresholdEnabled,
      this.autoThresholdQuantile,
      SignalDisplayMode.fromRaw(this.signalDisplayMode),
      CoolerWeightsNaNPolicy.fromRaw(this.coolerWeightsNaNPolicy),
      this.colormap.toEntity()
    );
  }

  private static double getFiniteDouble(final @NotNull JsonObject json, final @NotNull String key, final double fallback) {
    final var value = json.getValue(key);
    final double parsed;
    if (value instanceof Number number) {
      parsed = number.doubleValue();
    } else if (value instanceof String stringValue) {
      if (stringValue.isBlank()) {
        return fallback;
      }
      try {
        parsed = Double.parseDouble(stringValue.trim());
      } catch (final NumberFormatException ignored) {
        return fallback;
      }
    } else {
      return fallback;
    }
    return Double.isFinite(parsed) ? parsed : fallback;
  }

  private static double clampAutoThresholdQuantile(final double value) {
    if (!Double.isFinite(value)) {
      return 0.995d;
    }
    return Math.max(0.5d, Math.min(0.999999d, value));
  }
}
