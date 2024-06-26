/*
 * MIT License
 *
 * Copyright (c) 2024. Aleksandr Serdiukov, Anton Zamyatin, Aleksandr Sinitsyn, Vitalii Dravgelis and Computer Technologies Laboratory ITMO University team.
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
import ru.itmo.ctlab.hict.hict_library.visualization.SimpleVisualizationOptions;

public record VisualizationOptionsDTO(double preLogBase,
                                      double postLogBase,
                                      boolean applyCoolerWeights,
                                      boolean resolutionScaling,
                                      boolean resolutionLinearScaling,
                                      ColormapDTO colormap
) {
  public static @NotNull VisualizationOptionsDTO fromEntity(final @NotNull SimpleVisualizationOptions options, final @NotNull ChunkedFile chunkedFile) {
    return new VisualizationOptionsDTO(
      options.getPreLogBase(),
      options.getPostLogBase(),
      options.isApplyCoolerWeights(),
      options.isResolutionScaling(),
      options.isResolutionLinearScaling(),
      ColormapDTO.fromEntity(options.getColormap(), chunkedFile)
    );
  }

  public static @NotNull VisualizationOptionsDTO fromJSONObject(final @NotNull JsonObject json) {
    return new VisualizationOptionsDTO(
      json.getDouble("preLogBase"),
      json.getDouble("postLogBase"),
      json.getBoolean("applyCoolerWeights"),
      json.getBoolean("resolutionScaling"),
      json.getBoolean("resolutionLinearScaling"),
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
      this.colormap.toEntity()
    );
  }
}
