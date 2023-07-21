package ru.itmo.ctlab.hict.hict_server.dto.request.scaffolding;

import io.vertx.core.json.JsonObject;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public record UnscaffoldRegionRequestDTO(
  long startBP,
  long endBP
) {

  public static @NotNull UnscaffoldRegionRequestDTO fromJSONObject(final @NotNull JsonObject json) {
    return new UnscaffoldRegionRequestDTO(
      json.getLong("startBP"),
      json.getLong("endBP")
    );
  }
}
