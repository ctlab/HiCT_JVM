package ru.itmo.ctlab.hict.hict_server.dto.request.tiles;

import io.vertx.core.json.JsonObject;
import org.jetbrains.annotations.NotNull;

public record ContrastRangeSettingsDTO(double lowerSignalBound, double upperSignalBound) {
  public static @NotNull ContrastRangeSettingsDTO fromJSONObject(final @NotNull JsonObject json) {
    return new ContrastRangeSettingsDTO(
      json.getDouble("lowerSignalBound"),
      json.getDouble("upperSignalBound")
    );
  }
}
