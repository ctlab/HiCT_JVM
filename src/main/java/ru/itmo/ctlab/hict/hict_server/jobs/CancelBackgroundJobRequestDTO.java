package ru.itmo.ctlab.hict.hict_server.jobs;

import io.vertx.core.json.JsonObject;

public record CancelBackgroundJobRequestDTO(String service, String localId) {
  public static CancelBackgroundJobRequestDTO fromJson(JsonObject body) {
    if (body == null || !(body.getValue("id") instanceof String id) || id.length() > 256) {
      throw new IllegalArgumentException("A job ID is required");
    }
    final int slash = id.indexOf('/');
    if (slash <= 0 || slash == id.length() - 1) throw new IllegalArgumentException("Invalid job ID");
    return new CancelBackgroundJobRequestDTO(id.substring(0, slash), id.substring(slash + 1));
  }
}
