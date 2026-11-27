package ru.itmo.ctlab.hict.hict_server.dto.request.sequence;

import io.vertx.core.json.JsonObject;

public record SequenceAnalysisBudgetRequestDTO(boolean acceptLargeOutput) {
  public static SequenceAnalysisBudgetRequestDTO fromJson(JsonObject request) {
    Object value = request.getValue("acceptLargeOutput", false);
    if (value instanceof Boolean accepted) return new SequenceAnalysisBudgetRequestDTO(accepted);
    throw new IllegalArgumentException("acceptLargeOutput must be a boolean");
  }
}
