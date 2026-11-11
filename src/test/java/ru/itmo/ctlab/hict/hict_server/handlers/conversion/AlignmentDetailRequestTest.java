package ru.itmo.ctlab.hict.hict_server.handlers.conversion;

import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;
import ru.itmo.ctlab.hict.hict_server.dto.request.sequence.AlignmentDetailRequestDTO;

import static org.junit.jupiter.api.Assertions.*;

class AlignmentDetailRequestTest {
  @Test
  void acceptsBoundedIntervalsAndJobStatusRequests() {
    final var request = AlignmentDetailRequestDTO.fromJson(window());
    assertEquals(10, request.rowStart());
    assertEquals(60, request.rowEnd());
    assertFalse(request.includeGaps());
    assertEquals("job-1", AlignmentDetailRequestDTO.fromJson(new JsonObject().put("jobId", "job-1")).jobId());
  }

  @Test
  void rejectsCoercedCoordinatesEmptyWindowsAndOversizedWork() {
    for (var malformed : new JsonObject[] {
      window().put("rowStart", "10"), window().put("rowStart", 0.5),
      window().put("rowStart", -1), window().put("rowEnd", 10),
      window().put("columnEnd", 5000), window().put("includeGaps", "true"),
      new JsonObject().put("jobId", ""), new JsonObject().put("jobId", 12)
    }) {
      assertThrows(IllegalArgumentException.class, () -> AlignmentDetailRequestDTO.fromJson(malformed));
    }
  }

  private static JsonObject window() {
    return new JsonObject().put("rowStart", 10).put("rowEnd", 60).put("columnStart", 100).put("columnEnd", 150);
  }
}
