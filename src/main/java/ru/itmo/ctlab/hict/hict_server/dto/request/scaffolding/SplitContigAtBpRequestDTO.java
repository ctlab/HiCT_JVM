package ru.itmo.ctlab.hict.hict_server.dto.request.scaffolding;

import io.vertx.core.json.JsonObject;
import ru.itmo.ctlab.hict.hict_server.dto.RequestJson;

/** Boundary offset in the currently oriented contig, guarded by journal revision. */
public record SplitContigAtBpRequestDTO(int contigId, long offsetBp, long expectedRevision) {
  public static SplitContigAtBpRequestDTO fromJSONObject(JsonObject json) {
    if (json == null) throw new IllegalArgumentException("Expected an exact-cut request object");
    return new SplitContigAtBpRequestDTO((int) integer(json, "contigId", 0, Integer.MAX_VALUE),
      integer(json, "offsetBp", 1, 9_007_199_254_740_991L),
      integer(json, "expectedRevision", 0, 9_007_199_254_740_991L));
  }

  private static long integer(JsonObject json, String key, long min, long max) {
    final long value = RequestJson.requireLong(json, key);
    if (value < min || value > max)
      throw new IllegalArgumentException(key + " must be an integer in [" + min + ", " + max + "]");
    return value;
  }
}
