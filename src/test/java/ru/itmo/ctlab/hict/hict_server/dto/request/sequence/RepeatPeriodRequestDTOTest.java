package ru.itmo.ctlab.hict.hict_server.dto.request.sequence;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
class RepeatPeriodRequestDTOTest {
  @Test void acceptsLongAssemblyPositionsAndStatus() {
    assertEquals(2_500_000_003L, RepeatPeriodRequestDTO.fromJson(new JsonObject().put("startBp",2_500_000_000L).put("endBp",2_500_000_003L)).endBp());
    assertEquals("abc-123", RepeatPeriodRequestDTO.fromJson(new JsonObject().put("jobId","abc-123")).jobId());
  }
  @Test void rejectsMalformedUnsafeAndOversizedIntervals() {
    assertThrows(IllegalArgumentException.class, () -> RepeatPeriodRequestDTO.fromJson(null));
    for (Object start : new Object[]{-1, .5, "0", Double.NaN, Double.POSITIVE_INFINITY, 9_007_199_254_740_992L})
      assertThrows(IllegalArgumentException.class, () -> RepeatPeriodRequestDTO.fromJson(new JsonObject().put("startBp",start).put("endBp",100)));
    for (int end : new int[]{0, -1, 4097})
      assertThrows(IllegalArgumentException.class, () -> RepeatPeriodRequestDTO.fromJson(new JsonObject().put("startBp",0).put("endBp",end)));
    assertThrows(IllegalArgumentException.class, () -> RepeatPeriodRequestDTO.fromJson(new JsonObject().put("jobId","../x")));
  }
}
