package ru.itmo.ctlab.hict.hict_server.dto.response.sequence;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.List;

public record SequenceAnalysisEstimateResponseDTO(
    long rowBp, long columnBp, List<Long> requestedLevels, List<Long> overviewLevels,
    long expectedBytes, long worstCaseBytes, long diskBudgetBytes,
    Long warningLevel, Long limitingLevel, String hardLimitReason) {
  public JsonObject toJson() {
    var result = new JsonObject().put("rowBp", rowBp).put("columnBp", columnBp)
      .put("requestedLevels", new JsonArray(requestedLevels)).put("overviewLevels", new JsonArray(overviewLevels))
      .put("estimatedBytes", expectedBytes).put("expectedBytes", expectedBytes)
      .put("worstCaseBytes", worstCaseBytes).put("diskBudgetBytes", diskBudgetBytes);
    if (warningLevel != null) result.put("warningLevel", warningLevel);
    if (limitingLevel != null) result.put("limitingLevel", limitingLevel).put("hardLimitReason", hardLimitReason);
    return result;
  }
}
