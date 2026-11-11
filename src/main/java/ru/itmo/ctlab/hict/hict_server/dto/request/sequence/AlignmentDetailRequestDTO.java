package ru.itmo.ctlab.hict.hict_server.dto.request.sequence;

import io.vertx.core.json.JsonObject;

/** Either start a bounded alignment or read the status of a submitted alignment. */
public record AlignmentDetailRequestDTO(String jobId, long rowStart, long rowEnd,
                                        long columnStart, long columnEnd, boolean includeGaps) {
  private static final int MAX_WINDOW_BP = 4096;

  public static AlignmentDetailRequestDTO fromJson(JsonObject body) {
    if (body == null) throw new IllegalArgumentException("Alignment windows or a job ID are required");
    if (body.containsKey("jobId")) {
      final var value = body.getValue("jobId");
      if (!(value instanceof String id) || id.isBlank() || id.length() > 64) {
        throw new IllegalArgumentException("Invalid alignment job ID");
      }
      return new AlignmentDetailRequestDTO(id, 0, 0, 0, 0, false);
    }
    final long rowStart = coordinate(body, "rowStart");
    final long rowEnd = coordinate(body, "rowEnd");
    final long colStart = coordinate(body, "columnStart");
    final long colEnd = coordinate(body, "columnEnd");
    if (rowEnd <= rowStart || colEnd <= colStart
      || rowEnd - rowStart > MAX_WINDOW_BP || colEnd - colStart > MAX_WINDOW_BP) {
      throw new IllegalArgumentException("Each alignment window must contain 1 to " + MAX_WINDOW_BP + " bases");
    }
    if (!(body.getValue("includeGaps", false) instanceof Boolean gaps)) {
      throw new IllegalArgumentException("includeGaps must be true or false");
    }
    return new AlignmentDetailRequestDTO(null, rowStart, rowEnd, colStart, colEnd, gaps);
  }

  private static long coordinate(JsonObject body, String key) {
    if (body.getValue(key) instanceof Number number) {
      final double value = number.doubleValue();
      if (value >= 0 && value <= 9_007_199_254_740_991L && value == Math.floor(value)) return number.longValue();
    }
    throw new IllegalArgumentException(key + " must be a nonnegative integer");
  }
}
