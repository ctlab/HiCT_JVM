package ru.itmo.ctlab.hict.hict_server.dto.request.sequence;

import io.vertx.core.json.JsonObject;
import ru.itmo.ctlab.hict.hict_library.assembly.RepeatPeriod;

/** Start a gapless assembly interval measurement or read an existing job. */
public record RepeatPeriodRequestDTO(String jobId, long startBp, long endBp) {
  public static RepeatPeriodRequestDTO fromJson(JsonObject body) {
    if (body == null) throw new IllegalArgumentException("An interval or job ID is required");
    if (body.containsKey("jobId")) {
      if (!(body.getValue("jobId") instanceof String id) || !id.matches("[a-zA-Z0-9-]{1,64}"))
        throw new IllegalArgumentException("Invalid repeat measurement job ID");
      return new RepeatPeriodRequestDTO(id, 0, 0);
    }
    final long start = coordinate(body, "startBp"), end = coordinate(body, "endBp");
    if (end <= start || end - start > RepeatPeriod.MAX_BASES)
      throw new IllegalArgumentException("Select 1 to " + RepeatPeriod.MAX_BASES + " bases for repeat measurement");
    return new RepeatPeriodRequestDTO(null, start, end);
  }
  private static long coordinate(JsonObject body, String key) {
    if (body.getValue(key) instanceof Number number) {
      double value = number.doubleValue();
      if (Double.isFinite(value) && value >= 0 && value <= 9_007_199_254_740_991L
          && value == Math.floor(value)) return number.longValue();
    }
    throw new IllegalArgumentException(key + " must be a nonnegative safe integer");
  }
}
