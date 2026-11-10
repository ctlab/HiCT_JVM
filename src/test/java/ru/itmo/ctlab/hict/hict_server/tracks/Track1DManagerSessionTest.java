package ru.itmo.ctlab.hict.hict_server.tracks;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import io.vertx.core.json.JsonObject;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class Track1DManagerSessionTest {
  @TempDir Path directory;

  @Test void restoresSparseIdsOrderAppearanceAndVirtualSource() throws Exception {
    Files.writeString(directory.resolve("track.bed"), "tiny\t0\t3\tfeature\n");
    try (var original = new Track1DManager(directory); var candidate = new Track1DManager(directory)) {
      var removed = original.openTrack("track.bed", "Removed", "#123456");
      original.removeTrack(removed.getTrackId());
      var bed = original.openTrack("track.bed", "BED", "#123456");
      original.updateTrack(bed.getTrackId(), false, "#654321", "Custom", "READ_DENSITY", "SUM", true, false, -2.0, 8.0);
      original.openCoolerWeightsTrack("Secondary weights", "#112233", "SECONDARY");
      original.reorderTrack(bed.getTrackId(), 1);
      var definitions = original.listTracks();
      candidate.restoreTrackDefinitions(definitions);
      assertEquals(definitions.stream().map(JsonObject::mapFrom).toList(), candidate.listTracks().stream().map(JsonObject::mapFrom).toList());
      var next = candidate.openTrack("track.bed", "Next", "#123456");
      assertEquals("trk_4", next.getTrackId());
      assertEquals(2, original.listTracks().size());
    }
  }

  @Test void failureKeepsCandidateEmptyAndCounterUnchanged() throws Exception {
    var file = directory.resolve("track.bed");
    Files.writeString(file, "tiny\t0\t3\tfeature\n");
    try (var original = new Track1DManager(directory); var candidate = new Track1DManager(directory)) {
      var bed = original.openTrack("track.bed", "BED", "#123456");
      assertThrows(IllegalArgumentException.class, () -> candidate.restoreTrackDefinitions(List.of(bed, bed)));
      assertTrue(candidate.listTracks().isEmpty());
      var next = candidate.openTrack("track.bed", "Next", "#123456");
      assertEquals("trk_1", next.getTrackId());
      assertThrows(IllegalStateException.class, () -> candidate.restoreTrackDefinitions(List.of(bed)));
      assertEquals(1, candidate.listTracks().size());
      candidate.removeTrack(next.getTrackId());
      Files.delete(file);
      assertThrows(IllegalArgumentException.class, () -> candidate.restoreTrackDefinitions(List.of(bed)));
      assertTrue(candidate.listTracks().isEmpty());
    }
  }
}
