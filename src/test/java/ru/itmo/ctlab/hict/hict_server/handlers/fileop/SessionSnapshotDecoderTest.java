package ru.itmo.ctlab.hict.hict_server.handlers.fileop;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;
import ru.itmo.ctlab.hict.hict_server.handlers.tiles.RenderPipelineConfig;

import java.nio.file.Path;
import java.util.function.Consumer;
import static org.junit.jupiter.api.Assertions.*;

class SessionSnapshotDecoderTest {
  private static JsonObject valid() {
    var fingerprint = new JsonObject().put("algorithm", "SHA-256").put("sizeBytes", 10).put("sha256", "0".repeat(64));
    var contig = new JsonObject().put("contigId", 0).put("contigName", "tiny").put("contigOriginalName", "tiny")
      .put("contigSourceName", "tiny").put("contigOffsetInSource", 0).put("contigDirection", 1)
      .put("contigLengthBp", 17).put("contigLengthBins", new JsonObject().put("4", 5))
      .put("contigPresenceAtResolution", new JsonObject().put("4", 1));
    var source = new JsonObject().put("filename", "tiny.hict.hdf5").put("fastaFilename", "").put("agp", "")
      .put("agpSha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
      .put("fileFingerprint", fingerprint).put("capabilities", new JsonObject())
      .put("contigNameOverrides", new JsonObject()).put("scaffoldNameOverrides", new JsonObject())
      .put("assemblyInfo", new JsonObject().put("contigDescriptors", new JsonArray().add(contig))
        .put("scaffoldDescriptors", new JsonArray()));
    var track = new JsonObject().put("trackId", "trk_1").put("name", "Features").put("type", "BED")
      .put("sourceFile", "track.bed").put("color", "#123456").put("visible", true).put("featureCount", 1)
      .put("renderStyle", "SIGNAL").put("renderMode", "COVERAGE").put("aggregationMode", "MAX")
      .put("logScale", false).put("rangeAuto", true).put("rangeMin", 0).put("rangeMax", 1)
      .put("fileFingerprint", fingerprint.copy());
    return new JsonObject().put("snapshotVersion", 1).put("assemblyVerified", false)
      .put("filesFingerprinted", false).put("tracksVerified", false).put("primary", source)
      .put("secondary", null).put("assemblySource", "PRIMARY").put("tracks", new JsonArray().add(track))
      .put("renderPipeline", RenderPipelineConfig.disabled().toJson()).put("workspace", new JsonObject())
      .put("visualizationOptions", SessionVisualizationCodecTest.sample());
  }

  private static JsonObject contig(JsonObject json) {
    return json.getJsonObject("primary").getJsonObject("assemblyInfo").getJsonArray("contigDescriptors").getJsonObject(0);
  }

  private static void rejects(String field, Consumer<JsonObject> edit) {
    var json = valid();
    edit.accept(json);
    var failure = assertThrows(IllegalArgumentException.class, () -> SessionSnapshotDecoder.decode(json));
    assertTrue(failure.getMessage().contains(field), failure.getMessage());
  }

  @Test void roundTripsValuesAndOwnsInputWithoutTrustingVerificationFlags() {
    var json = valid();
    var decoded = SessionSnapshotDecoder.decode(json);
    assertEquals(json, JsonObject.mapFrom(decoded.snapshot()));
    assertEquals("trk_1", decoded.tracks().getFirst().getTrackId());
    assertFalse(decoded.snapshot().assemblyVerified());
    json.getJsonObject("primary").put("filename", "changed");
    assertEquals("tiny.hict.hdf5", decoded.snapshot().primary().getString("filename"));
    assertThrows(UnsupportedOperationException.class, () -> decoded.tracks().clear());
  }

  @Test void rejectsCoercionsBadVersionsAndInvalidReferences() {
    rejects("snapshotVersion", x -> x.put("snapshotVersion", 2));
    rejects("snapshotVersion", x -> x.put("snapshotVersion", 1.1));
    rejects("assemblyVerified", x -> x.put("assemblyVerified", "true"));
    rejects("contigDirection", x -> contig(x).put("contigDirection", 0.5));
    rejects("contigLengthBp", x -> contig(x).put("contigLengthBp", 9007199254740992L));
    rejects("contigId", x -> x.getJsonObject("primary").getJsonObject("assemblyInfo")
      .getJsonArray("contigDescriptors").add(contig(x).copy()));
    rejects("contigNameOverrides.4", x -> x.getJsonObject("primary").getJsonObject("contigNameOverrides").put("4", "missing"));
    rejects("resolution", x -> contig(x).getJsonObject("contigPresenceAtResolution").put("10", 1));
    rejects("tracks[1].trackId", x -> x.getJsonArray("tracks").add(x.getJsonArray("tracks").getJsonObject(0).copy()));
    rejects("tracks[0].visible", x -> x.getJsonArray("tracks").getJsonObject(0).put("visible", 1));
    rejects("tracks[0].rangeMax", x -> x.getJsonArray("tracks").getJsonObject(0).put("rangeMax", 0));
    rejects("assemblySource", x -> x.put("assemblySource", "SECONDARY"));
    rejects("renderPipeline.upperExpression.trackId", x -> x.getJsonObject("renderPipeline")
      .put("upperExpression", new JsonObject().put("type", "track1d").put("trackId", "missing")));
  }

  @Test void rejectsInvalidFingerprintsHashesAndScaffoldIntervals() {
    rejects("primary.agpSha256", x -> x.getJsonObject("primary").put("agp", "changed"));
    rejects("fileFingerprint.sizeBytes", x -> x.getJsonObject("primary").getJsonObject("fileFingerprint").put("sizeBytes", 1.5));
    rejects("fileFingerprint.sha256", x -> x.getJsonObject("primary").getJsonObject("fileFingerprint").put("sha256", "no"));
    rejects("fastaFingerprint", x -> x.getJsonObject("primary").put("fastaFilename", "tiny.fa"));
    rejects("scaffoldBordersBP.lengthBp", x -> x.getJsonObject("primary").getJsonObject("assemblyInfo")
      .getJsonArray("scaffoldDescriptors").add(new JsonObject().put("scaffoldId", 1)
        .put("scaffoldName", "s").put("scaffoldOriginalName", "s").put("spacerLength", 10)
        .put("scaffoldBordersBP", new JsonObject().put("startBP", 0).put("endBP", 17).put("lengthBp", 16))));
  }

  @Test void validatesViewAndIndependentScopeReferencesAgainstActiveAssembly() {
    var scope = new JsonObject().put("kind", "contig").put("id", 0).put("label", "tiny").put("startBp", 0).put("endBp", 17);
    var state = new JsonObject().put("version", 1).put("view", new JsonObject()
      .put("center", new JsonArray().add(2).add(-2)).put("resolution", 1).put("rotation", 0))
      .put("axisScopes", new JsonObject().put("row", scope).put("column", new JsonObject().put("kind", "all").put("label", "All Columns")));
    var json = valid();
    json.getJsonObject("workspace").put("workspaceState", state);
    assertEquals(json, JsonObject.mapFrom(SessionSnapshotDecoder.decode(json).snapshot()));
    state.getJsonObject("view").put("resolution", 0);
    assertTrue(assertThrows(IllegalArgumentException.class, () -> SessionSnapshotDecoder.decode(json)).getMessage().contains("view.resolution"));
    state.getJsonObject("view").put("resolution", 1);
    scope.put("id", 8);
    assertTrue(assertThrows(IllegalArgumentException.class, () -> SessionSnapshotDecoder.decode(json)).getMessage().contains("axisScopes.row.id"));
    scope.put("id", 0).put("endBp", 16);
    assertTrue(assertThrows(IllegalArgumentException.class, () -> SessionSnapshotDecoder.decode(json)).getMessage().contains("axisScopes.row.endBp"));
  }

  @Test void boundsAllFieldsIncludingUnknownExtensionsBeforeCopyOrCompilation() {
    rejects("complexity", x -> {
      var nested = new JsonObject();
      var root = nested;
      for (int i = 0; i < 65; i++) { var next = new JsonObject(); nested.put("child", next); nested = next; }
      x.put("extension", root);
    });
    rejects("complexity", x -> {
      var array = new JsonArray();
      for (int i = 0; i < 100001; i++) array.add(null);
      x.put("extension", array);
    });
    rejects("8 MiB", x -> x.put("extension", "x".repeat(SessionSnapshotDecoder.MAX_BYTES + 1)));
    rejects("reserved key", x -> x.put("constructor", new JsonObject()));
    rejects("finite", x -> x.put("extension", Double.NaN));
  }

  @Test void invalidDocumentIsRejectedBeforeResolvingAnyResource() {
    var json = valid().put("tracks", "invalid");
    var failure = assertThrows(IllegalArgumentException.class,
      () -> SessionRestoreCandidate.prepare(Path.of("/nonexistent/session-decoder-test"), Path.of("/nonexistent"), json));
    assertTrue(failure.getMessage().contains("snapshot.tracks"), failure.getMessage());
  }
}
