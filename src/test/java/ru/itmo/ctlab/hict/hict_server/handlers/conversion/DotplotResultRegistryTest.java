package ru.itmo.ctlab.hict.hict_server.handlers.conversion;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class DotplotResultRegistryTest {
  @TempDir Path data;

  @Test void keysIgnoreDetailAndExecutionAndSeparateDifferentSourcesAndSettings() throws Exception {
    var map = Files.writeString(data.resolve("map.hict.hdf5"), "a");
    var stamp = SequenceAnalysisHandlersHolder.Stamp.read(map);
    var first = new JsonObject().put("k", 21).put("resolutions", new JsonArray(List.of(1000)))
      .put("threads", 2);
    var second = new JsonObject().put("k", 21).put("resolutions", new JsonArray(List.of(2000, 5000)))
      .put("overviewLevels", new JsonArray(List.of(20_000))).put("threads", 7);
    var key = DotplotResultRegistry.key(map, stamp, "fasta-one", "moddotplot", first, "gaps", "tool", "");
    assertEquals(key, DotplotResultRegistry.key(map, stamp, "fasta-one", "moddotplot", second, "gaps", "tool", ""));
    assertNotEquals(key, DotplotResultRegistry.key(map, stamp, "fasta-two", "moddotplot", first, "gaps", "tool", ""));
    assertNotEquals(key, DotplotResultRegistry.key(map.resolveSibling("different.hict.hdf5"), stamp,
      "fasta-one", "moddotplot", first, "gaps", "tool", ""));
    assertNotEquals(key, DotplotResultRegistry.key(map, stamp, "fasta-one", "moddotplot",
      second.put("k", 31), "gaps", "tool", ""));
  }

  @Test void laterCompatibleJobsFindTheLatestPublishedSetWithoutReplacingOtherSets() throws Exception {
    var root = Files.createDirectory(data.resolve("sequence-analysis"));
    var first = Files.writeString(root.resolve("first.hict.hdf5"), "first version");
    var second = Files.writeString(root.resolve("second.hict.hdf5"), "second version");
    var firstId = UUID.randomUUID().toString();
    var secondId = UUID.randomUUID().toString();
    assertNull(DotplotResultRegistry.latest(data, root, "digest"));
    write(root, first, firstId, 1);
    assertEquals(first, DotplotResultRegistry.latest(data, root, "digest").file());
    write(root, second, secondId, 2);
    var previous = DotplotResultRegistry.latest(data, root, "digest");
    assertEquals(second, previous.file());
    assertEquals(secondId, previous.setId());
    assertEquals(1, previous.regionCount());
    assertTrue(Files.exists(DotplotResultRegistry.manifest(root, "digest", firstId)));
    assertTrue(Files.exists(first));
  }

  private void write(Path root, Path version, String id, long updatedAt) throws Exception {
    var manifest = new JsonObject().put("setId", id).put("regionCount", 1)
      .put("outputFile", data.relativize(version).toString()).put("updatedAtMs", updatedAt);
    Files.writeString(DotplotResultRegistry.manifest(root, "digest", id), manifest.encode());
  }
}
