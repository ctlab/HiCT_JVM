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
