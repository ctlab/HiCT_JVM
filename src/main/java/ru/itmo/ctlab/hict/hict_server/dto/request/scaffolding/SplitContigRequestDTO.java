package ru.itmo.ctlab.hict.hict_server.dto.request.scaffolding;

import io.vertx.core.json.JsonObject;
import org.jetbrains.annotations.NotNull;

public record SplitContigRequestDTO(
  long splitPx,
  long bpResolution
) {

  public static @NotNull SplitContigRequestDTO fromJSONObject(final @NotNull JsonObject json) {
    return new SplitContigRequestDTO(json.getLong("splitPx"), json.getLong("bpResolution"));
  }
}
