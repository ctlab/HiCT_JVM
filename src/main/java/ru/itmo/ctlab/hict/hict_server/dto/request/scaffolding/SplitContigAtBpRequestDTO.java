package ru.itmo.ctlab.hict.hict_server.dto.request.scaffolding;

import io.vertx.core.json.JsonObject;

/** Boundary offset in the currently oriented contig, guarded by journal revision. */
public record SplitContigAtBpRequestDTO(int contigId, long offsetBp, long expectedRevision) {
  public static SplitContigAtBpRequestDTO fromJSONObject(JsonObject json) {
    if (json == null) throw new IllegalArgumentException("Expected an exact-cut request object");
    return new SplitContigAtBpRequestDTO((int) integer(json, "contigId", 0, Integer.MAX_VALUE),
      integer(json, "offsetBp", 1, 9_007_199_254_740_991L),
      integer(json, "expectedRevision", 0, 9_007_199_254_740_991L));
  }

  private static long integer(JsonObject json, String key, long min, long max) {
    if (!(json.getValue(key) instanceof Number value) || !Double.isFinite(value.doubleValue())
      || value.doubleValue() != Math.rint(value.doubleValue()) || value.doubleValue() < min || value.doubleValue() > max)
      throw new IllegalArgumentException(key + " must be an integer in [" + min + ", " + max + "]");
    return value.longValue();
  }
}
