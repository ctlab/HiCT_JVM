package ru.itmo.ctlab.hict.hict_server.dto.request.scaffolding;

import io.vertx.core.json.JsonObject;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public record ScaffoldRegionRequestDTO(
  long startBP,
  long endBP,
  @Nullable String scaffoldName,
  @Nullable Long spacerLength
) {

  public static @NotNull ScaffoldRegionRequestDTO fromJSONObject(final @NotNull JsonObject json) {
    return new ScaffoldRegionRequestDTO(
      json.getLong("startBP"),
      json.getLong("endBP"),
      json.getString("scaffoldName", null),
      json.getLong("spacerLength", null)
    );
  }
}
