package ru.itmo.ctlab.hict.hict_server.handlers.conversion;

import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;
import ru.itmo.ctlab.hict.hict_server.jobs.BackgroundJob;
import static org.junit.jupiter.api.Assertions.*;

class SequenceResultDisplayModeTest {
  @Test void defaultsFollowScopeAndExplicitChoicesSurviveJobSerialization() {
    assertEquals("overlay", SequenceAnalysisHandlersHolder.resultDisplayMode(new JsonObject().put("scope", "selection")));
    assertEquals("below", SequenceAnalysisHandlersHolder.resultDisplayMode(new JsonObject().put("scope", "assembly")));
    for (String mode : new String[]{"overlay", "above", "below", "none"}) {
      var request = new JsonObject().put("scope", "assembly").put("displayMode", mode);
      var output = new BackgroundJob.Output("result.hict.hdf5", "attach_identity", null, null,
        "source.hict.hdf5", SequenceAnalysisHandlersHolder.resultDisplayMode(request));
      assertEquals(mode, JsonObject.mapFrom(output).getString("displayMode"));
    }
  }
  @Test void rejectsInvalidPreferencesWithoutCoercingThem() {
    for (Object value : new Object[]{"invalid", 3, true, new JsonObject()}) {
      assertThrows(IllegalArgumentException.class, () -> SequenceAnalysisHandlersHolder.resultDisplayMode(
        new JsonObject().put("scope", "selection").put("displayMode", value)));
    }
  }
}
