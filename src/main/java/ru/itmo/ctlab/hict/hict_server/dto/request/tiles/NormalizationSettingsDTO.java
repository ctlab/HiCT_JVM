package ru.itmo.ctlab.hict.hict_server.dto.request.tiles;

import io.vertx.core.json.JsonObject;
import org.jetbrains.annotations.NotNull;

public record NormalizationSettingsDTO(double preLogBase, double postLogBase,
                                       boolean applyCoolerWeights) {

  public static @NotNull NormalizationSettingsDTO fromJSONObject(final @NotNull JsonObject json) {
    return new NormalizationSettingsDTO(
      json.getDouble("preLogBase"),
      json.getDouble("postLogBase"),
      json.getBoolean("applyCoolerWeights")
    );
  }
}
