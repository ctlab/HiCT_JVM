package ru.itmo.ctlab.hict.hict_server.dto.request.scaffolding;

import io.vertx.core.json.JsonObject;
import org.jetbrains.annotations.NotNull;

public record MoveSelectionToDebrisRequestDTO(
  long startBP,
  long endBP
) {

  public static @NotNull MoveSelectionToDebrisRequestDTO fromJSONObject(final @NotNull JsonObject json) {
    return new MoveSelectionToDebrisRequestDTO(
      json.getLong("startBP"),
      json.getLong("endBP")
    );
  }
}
