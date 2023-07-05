package ru.itmo.ctlab.hict.hict_server.dto.request.scaffolding;

import io.vertx.core.json.JsonObject;
import org.jetbrains.annotations.NotNull;

public record MoveSelectionRangeRequestDTO(
  long startBP,
  long endBP,
  long targetStartBP
) {

  public static @NotNull MoveSelectionRangeRequestDTO fromJSONObject(final @NotNull JsonObject json) {
    return new MoveSelectionRangeRequestDTO(json.getLong("startBP"), json.getLong("endBP"), json.getLong("targetStartBP"));
  }
}
