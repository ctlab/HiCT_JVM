package ru.itmo.ctlab.hict.hict_server.handlers.fileop;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.itmo.ctlab.hict.fixtures.ReinforcementHdf5Fixture;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_server.dto.response.fileop.SessionSnapshotDTO;
import ru.itmo.ctlab.hict.hict_server.handlers.tiles.RenderPipelineConfig;
import ru.itmo.ctlab.hict.hict_server.tracks.Track1DManager;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class SessionRestoreCandidateTest {
  @TempDir Path directory;

  private JsonObject source(ChunkedFile file) throws Exception {
    return SessionSnapshotDTO.source(file, file.getHdfFilePath().getFileName().toString(), "")
      .put("fileFingerprint", JsonObject.mapFrom(SessionFileFingerprint.compute(file.getHdfFilePath())));
  }

  private SessionSnapshotDTO snapshot(ChunkedFile primary, ChunkedFile secondary, Track1DManager tracks) throws Exception {
    var summaries = new JsonArray();
    for (var definition : tracks.listTracks()) {
      var json = JsonObject.mapFrom(definition);
      if ("COOLER_WEIGHTS".equals(definition.getType())) json.put("sourceReference", "SECONDARY");
      else json.put("fileFingerprint", JsonObject.mapFrom(SessionFileFingerprint.compute(directory.resolve(definition.getSourceFile()))));
      summaries.add(json);
    }
    return new SessionSnapshotDTO(1, true, true, true, source(primary), source(secondary),
      "PRIMARY", RenderPipelineConfig.disabled().toJson(), summaries, new JsonObject(), SessionVisualizationCodecTest.sample());
  }

  @Test void preparesTwoEditedSourcesTracksAndFastaWithoutChangingLiveSession() throws Exception {
    var p = ReinforcementHdf5Fixture.create(directory, "primary", List.of(4L, 10L));
    var s = ReinforcementHdf5Fixture.create(directory, "secondary", List.of(3L, 7L));
    Files.writeString(directory.resolve("track.bed"), "tiny\t0\t3\tfeature\n");
    var fasta = Files.writeString(directory.resolve("tiny.fa"), ">tiny\nACGTACGTACGTACGTA\n");
    try (var primary = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(p, 1, 2));
         var secondary = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(s, 1, 2));
         var tracks = new Track1DManager(directory)) {
      primary.getScaffoldingOperations().reverseSelectionRangeBp(0, 17);
      secondary.getScaffoldingOperations().reverseSelectionRangeBp(0, 17);
      tracks.openTrack("track.bed", "Features", "#123456");
      tracks.openCoolerWeightsTrack("Weights", "#654321", "SECONDARY");
      var saved = snapshot(primary, secondary, tracks);
      saved.primary().put("fastaFilename", "tiny.fa").put("fastaFingerprint", JsonObject.mapFrom(SessionFileFingerprint.compute(fasta)));
      var before = saved.primary().encode();
      var candidate = SessionRestoreCandidate.prepare(directory, directory.resolve("processed"), JsonObject.mapFrom(saved));
      try (candidate) {
        SessionAssemblyRestorer.verifyContactBinMapping(primary, candidate.primary());
        SessionAssemblyRestorer.verifyContactBinMapping(secondary, candidate.secondary());
        assertEquals(saved.primary().getJsonObject("assemblyInfo"), source(candidate.primary()).getJsonObject("assemblyInfo"));
        assertEquals(tracks.listTracks().stream().map(JsonObject::mapFrom).toList(),
          candidate.tracks().listTracks().stream().map(JsonObject::mapFrom).toList());
        assertEquals(fasta.toRealPath(), candidate.primaryFasta());
        assertEquals(saved.renderPipeline(), candidate.pipeline().toJson());
        assertEquals(saved.visualizationOptions(), SessionVisualizationCodec.encode(candidate.visualization()));
        assertEquals(before, saved.primary().encode());
      }
      candidate.close();
      assertNull(candidate.primary());
      assertNull(candidate.secondary());
      assertNull(candidate.tracks());
      assertEquals(saved.primary().getJsonObject("assemblyInfo"), source(primary).getJsonObject("assemblyInfo"));
      assertEquals(2, tracks.listTracks().size());
    }
  }

  @Test void rejectsReplacedTrackBytesEvenWithSameSizeAndTimestampAndRecovers() throws Exception {
    var p = ReinforcementHdf5Fixture.create(directory, "primary", List.of(4L, 10L));
    var s = ReinforcementHdf5Fixture.create(directory, "secondary", List.of(4L, 10L));
    var bed = Files.writeString(directory.resolve("track.bed"), "tiny\t0\t3\tfeature\n");
    try (var primary = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(p, 1, 2));
         var secondary = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(s, 1, 2));
         var tracks = new Track1DManager(directory)) {
      tracks.openTrack("track.bed", "Features", "#123456");
      var saved = snapshot(primary, secondary, tracks);
      var stamp = Files.getLastModifiedTime(bed);
      Files.writeString(bed, "tiny\t0\t4\tfeature\n");
      Files.setLastModifiedTime(bed, stamp);
      var failure = assertThrows(java.io.IOException.class,
        () -> SessionRestoreCandidate.prepare(directory, directory.resolve("processed"), saved, tracks.listTracks()));
      assertTrue(failure.getMessage().contains("tracks[0].fileFingerprint"), failure.getMessage());
      Files.writeString(bed, "tiny\t0\t3\tfeature\n");
      try (var candidate = SessionRestoreCandidate.prepare(directory, directory.resolve("processed"), saved, tracks.listTracks())) {
        assertNotNull(candidate.primary());
      }
    }
  }

  @Test void secondaryPreparationFailureKeepsLiveAssemblyAndAllowsRetry() throws Exception {
    var p = ReinforcementHdf5Fixture.create(directory, "primary", List.of(4L, 10L));
    var s = ReinforcementHdf5Fixture.create(directory, "secondary", List.of(4L, 10L));
    try (var primary = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(p, 1, 2));
         var secondary = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(s, 1, 2));
         var tracks = new Track1DManager(directory)) {
      primary.getScaffoldingOperations().reverseSelectionRangeBp(0, 17);
      var saved = snapshot(primary, secondary, tracks);
      var original = saved.secondary().getString("agpSha256");
      saved.secondary().put("agpSha256", "0".repeat(64));
      assertThrows(IllegalArgumentException.class,
        () -> SessionRestoreCandidate.prepare(directory, directory.resolve("processed"), saved, tracks.listTracks()));
      assertEquals(saved.primary().getJsonObject("assemblyInfo"), source(primary).getJsonObject("assemblyInfo"));
      saved.secondary().put("agpSha256", original);
      try (var candidate = SessionRestoreCandidate.prepare(directory, directory.resolve("processed"), saved, tracks.listTracks())) {
        SessionAssemblyRestorer.verifyContactBinMapping(primary, candidate.primary());
      }
    }
  }

  @Test void preparesPrimaryOnlyAndRejectsAbsentActiveSecondary() throws Exception {
    var path = ReinforcementHdf5Fixture.create(directory, "primary", List.of(4L, 10L));
    try (var live = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(path, 1, 2))) {
      var saved = new SessionSnapshotDTO(1, true, true, true, source(live), null,
        "PRIMARY", RenderPipelineConfig.disabled().toJson(), new JsonArray(), new JsonObject(), SessionVisualizationCodecTest.sample());
      try (var candidate = SessionRestoreCandidate.prepare(directory, directory.resolve("processed"), saved, List.of())) {
        assertNotNull(candidate.primary());
        assertNull(candidate.secondary());
        assertNull(candidate.primaryFasta());
        assertTrue(candidate.tracks().listTracks().isEmpty());
      }
      var missing = new SessionSnapshotDTO(1, true, true, true, saved.primary(), null,
        "SECONDARY", saved.renderPipeline(), saved.tracks(), saved.workspace(), saved.visualizationOptions());
      assertThrows(IllegalArgumentException.class,
        () -> SessionRestoreCandidate.prepare(directory, directory.resolve("processed"), missing, List.of()));
    }
  }

  @Test void rejectsMissingFingerprintsAndEscapingNamesButFollowsLinksInsideDataDir() throws Exception {
    var p = ReinforcementHdf5Fixture.create(directory, "primary", List.of(4L, 10L));
    var s = ReinforcementHdf5Fixture.create(directory, "secondary", List.of(4L, 10L));
    try (var primary = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(p, 1, 2));
         var secondary = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(s, 1, 2));
         var tracks = new Track1DManager(directory)) {
      var saved = snapshot(primary, secondary, tracks);
      saved.primary().remove("fileFingerprint");
      assertThrows(IllegalArgumentException.class,
        () -> SessionRestoreCandidate.prepare(directory, directory.resolve("processed"), saved, tracks.listTracks()));
      saved.primary().put("fileFingerprint", JsonObject.mapFrom(SessionFileFingerprint.compute(p)));
      saved.primary().put("filename", "../outside.hict.hdf5");
      assertThrows(IllegalArgumentException.class,
        () -> SessionRestoreCandidate.prepare(directory, directory.resolve("processed"), saved, tracks.listTracks()));
      // A symlink placed inside DATA_DIR is followed, as /open does. Names that leave DATA_DIR are still refused.
      var nested = Files.createDirectory(directory.resolve("nested"));
      try {
        Files.createSymbolicLink(nested.resolve("linked.hict.hdf5"), p);
        Files.createSymbolicLink(nested.resolve("secondary.hict.hdf5"), s);
      }
      catch (UnsupportedOperationException | java.nio.file.FileSystemException unavailable) { return; }
      saved.primary().put("filename", "linked.hict.hdf5");
      try (var candidate = SessionRestoreCandidate.prepare(nested, nested.resolve("processed"), saved, tracks.listTracks())) {
        org.junit.jupiter.api.Assertions.assertNotNull(candidate.primary());
      }
      saved.primary().put("filename", "../primary.hict.hdf5");
      assertThrows(IllegalArgumentException.class,
        () -> SessionRestoreCandidate.prepare(nested, nested.resolve("processed"), saved, tracks.listTracks()));
    }
  }
}
