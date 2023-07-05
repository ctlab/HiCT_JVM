package ru.itmo.ctlab.hict.hict_server.dto.request.scaffolding;

import io.vertx.core.json.JsonObject;
import org.jetbrains.annotations.NotNull;

public record ReverseSelectionRangeRequestDTO(long startBP,
                                              long endBP) {

  public static @NotNull ReverseSelectionRangeRequestDTO fromJSONObject(final @NotNull JsonObject json) {
    return new ReverseSelectionRangeRequestDTO(json.getLong("startBP"), json.getLong("endBP"));
  }
}
