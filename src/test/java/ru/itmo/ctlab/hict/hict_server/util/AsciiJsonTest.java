package ru.itmo.ctlab.hict.hict_server.util;

import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class AsciiJsonTest {
  @Test
  void plansWithUnicodePathsAreAsciiAndRoundTrip(@TempDir Path directory) throws Exception {
    var plan = new JsonObject().put("input", "C:\\Lab\\Δοκιμή\\a.fa")
      .put("output", "C:\\Lab\\試験\\🧪.hict.hdf5");
    for (boolean pretty : new boolean[]{false, true}) {
      var file = directory.resolve("analysis-plan.json");
      Files.writeString(file, AsciiJson.encode(plan, pretty));
      for (byte unit : Files.readAllBytes(file)) assertTrue(unit >= 0);
      assertEquals(plan, new JsonObject(Files.readString(file)));
    }
  }
}
