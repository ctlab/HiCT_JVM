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
    rejects("snapshotVersion", x -> x.put("snapshotVersion", 3));
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

  @Test void validatesSplitReplayFieldsAndOrderBeforeOpeningResources() {
    var split = new JsonObject().put("parentId", 0).put("sourceName", "tiny")
      .put("sourceOffsetBp", 0).put("parentLengthBp", 17).put("reversed", false)
      .put("splitOffsetBp", 8).put("leftId", 1).put("rightId", 2);
    var json = valid();
    json.getJsonObject("primary").put("splitEdits", new JsonArray().add(split));
    assertEquals(json, JsonObject.mapFrom(SessionSnapshotDecoder.decode(json).snapshot()));
    var exact = json.copy().put("snapshotVersion", 2);
    exact.getJsonObject("primary").getJsonArray("splitEdits").getJsonObject(0).put("exact", true);
    contig(exact).put("contigBinOffsets", new JsonObject().put("4", -1));
    assertEquals(exact, JsonObject.mapFrom(SessionSnapshotDecoder.decode(exact).snapshot()));
    rejects("splitEdits[0].exact", x -> x.getJsonObject("primary")
      .put("splitEdits", new JsonArray().add(split.copy().put("exact", true))));
    rejects("splitEdits[0].exact", x -> x.getJsonObject("primary")
      .put("splitEdits", new JsonArray().add(split.copy().put("exact", "true"))));
    rejects("contigBinOffsets", x -> contig(x).put("contigBinOffsets", new JsonObject().put("4", 0.5)));
    rejects("contigBinOffsets", x -> contig(x).put("contigBinOffsets", new JsonObject().put("99", 0)));
    rejects("splitEdits[0].splitOffsetBp", x -> x.getJsonObject("primary")
      .put("splitEdits", new JsonArray().add(split.copy().put("splitOffsetBp", 17))));
    rejects("splitEdits[0].rightId", x -> x.getJsonObject("primary")
      .put("splitEdits", new JsonArray().add(split.copy().put("rightId", 1))));
    rejects("splitEdits[1].leftId", x -> x.getJsonObject("primary")
      .put("splitEdits", new JsonArray().add(split).add(split.copy())));
  }

  @Test void acceptsLongSourceOffsetsButRejectsImpreciseAndOverflowingIntervals() {
    var json = valid().put("snapshotVersion", 2);
    contig(json).put("contigOffsetInSource", 2_500_000_001L);
    var split = new JsonObject().put("parentId", 0).put("sourceName", "tiny")
      .put("sourceOffsetBp", 2_500_000_001L).put("parentLengthBp", 17).put("reversed", false)
      .put("splitOffsetBp", 8).put("leftId", 1).put("rightId", 2).put("exact", true);
    json.getJsonObject("primary").put("splitEdits", new JsonArray().add(split));
    var decoded = SessionSnapshotDecoder.decode(json).snapshot().primary();
    assertEquals(2_500_000_001L, decoded.getJsonObject("assemblyInfo").getJsonArray("contigDescriptors")
      .getJsonObject(0).getLong("contigOffsetInSource"));
    assertEquals(2_500_000_001L, decoded.getJsonArray("splitEdits").getJsonObject(0).getLong("sourceOffsetBp"));
    rejects("contigOffsetInSource", x -> contig(x).put("contigOffsetInSource", 9_007_199_254_740_992L));
    rejects("contigOffsetInSource", x -> contig(x).put("contigOffsetInSource", -1));
    rejects("contigLengthBp", x -> contig(x).put("contigOffsetInSource", 9_007_199_254_740_990L));
    rejects("splitEdits[0].parentLengthBp", x -> x.getJsonObject("primary").put("splitEdits",
      new JsonArray().add(split.copy().put("sourceOffsetBp", 9_007_199_254_740_990L).put("exact", false))));
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

  @Test void roundTripsRegionIsolationAndRejectsInvalidBounds() {
    var row = new JsonObject().put("kind", "selection").put("label", "Selection").put("startBp", 0).put("endBp", 17);
    var column = row.copy().put("startBp", 3).put("endBp", 12);
    var json = valid();
    json.getJsonObject("workspace").put("workspaceState", new JsonObject().put("version", 1)
      .put("axisScopes", new JsonObject().put("row", row).put("column", column)));
    assertEquals(json, JsonObject.mapFrom(SessionSnapshotDecoder.decode(json).snapshot()));

    for (var end : new Number[]{0, 18, 1.25}) {
      row.put("endBp", end);
      assertTrue(assertThrows(IllegalArgumentException.class, () -> SessionSnapshotDecoder.decode(json))
        .getMessage().contains("axisScopes.row.endBp"));
    }
    row.remove("endBp");
    assertTrue(assertThrows(IllegalArgumentException.class, () -> SessionSnapshotDecoder.decode(json))
      .getMessage().contains("axisScopes.row.endBp"));
  }

  @Test void validatesContigSelectionsAndTheirSavedBounds() {
    var json = valid();
    var contigs = json.getJsonObject("primary").getJsonObject("assemblyInfo").getJsonArray("contigDescriptors");
    contigs.add(contig(json).copy().put("contigId", 1)).add(contig(json).copy().put("contigId", 2));
    var column = new JsonObject().put("kind", "selection").put("label", "Selection")
      .put("contigIds", new JsonArray().add(2).add(1)).put("startBp", 17).put("endBp", 51);
    json.getJsonObject("workspace").put("workspaceState", new JsonObject().put("version", 1)
      .put("axisScopes", new JsonObject().put("row", new JsonObject().put("kind", "all").put("label", "All rows"))
        .put("column", column)));
    assertEquals(json, JsonObject.mapFrom(SessionSnapshotDecoder.decode(json).snapshot()));
    column.put("startBp", 18);
    assertTrue(assertThrows(IllegalArgumentException.class, () -> SessionSnapshotDecoder.decode(json))
      .getMessage().contains("axisScopes.column.startBp"));
    column.remove("startBp");
    column.remove("endBp");
    assertEquals(json, JsonObject.mapFrom(SessionSnapshotDecoder.decode(json).snapshot()));

    for (var invalidIds : new JsonArray[]{new JsonArray().add(0).add(2), new JsonArray().add(1).add(1),
        new JsonArray().add(99), new JsonArray().add(1.5)}) {
      column.put("contigIds", invalidIds);
      assertTrue(assertThrows(IllegalArgumentException.class, () -> SessionSnapshotDecoder.decode(json))
        .getMessage().contains("axisScopes.column.contigIds"));
    }
  }

  @Test void boundsAllFieldsIncludingUnknownExtensionsBeforeCopyOrCompilation() {
    rejects("nesting", x -> {
      var nested = new JsonObject();
      var root = nested;
      for (int i = 0; i < 161; i++) { var next = new JsonObject(); nested.put("child", next); nested = next; }
      x.put("extension", root);
    });
    rejects("complexity", x -> {
      var array = new JsonArray();
      for (int i = 0; i <= SessionSnapshotDecoder.MAX_VALUES; i++) array.add(null);
      x.put("extension", array);
    });
    rejects("8 MiB", x -> x.put("extension", "x".repeat(SessionSnapshotDecoder.MAX_BYTES + 1)));
    rejects("reserved key", x -> x.put("constructor", new JsonObject()));
    rejects("finite", x -> x.put("extension", Double.NaN));
  }

  @Test void acceptsMultiResolutionPrimaryAndSecondaryDescriptorsWithinByteBudget() {
    var json = valid();
    var template = contig(json).copy();
    var assembly = json.getJsonObject("primary").getJsonObject("assemblyInfo");
    var contigs = new JsonArray();
    var scaffolds = new JsonArray();
    for (int i = 0; i < 1765; i++) {
      var bins = new JsonObject();
      var presence = new JsonObject();
      for (int resolution = 1; resolution <= 13; resolution++) {
        bins.put(Integer.toString(resolution), (17 + resolution - 1) / resolution);
        presence.put(Integer.toString(resolution), 1);
      }
      contigs.add(template.copy().put("contigId", i).put("contigName", "c" + i)
        .put("contigLengthBins", bins).put("contigPresenceAtResolution", presence));
      scaffolds.add(new JsonObject().put("scaffoldId", i).put("scaffoldName", "s" + i)
        .put("scaffoldOriginalName", "s" + i).put("spacerLength", 1000)
        .put("scaffoldBordersBP", new JsonObject().put("startBP", i * 17L)
          .put("endBP", (i + 1) * 17L).put("lengthBp", 17)));
    }
    assembly.put("contigDescriptors", contigs).put("scaffoldDescriptors", scaffolds);
    json.put("secondary", json.getJsonObject("primary").copy().put("filename", "identity.hict.hdf5"));
    assertTrue(json.encode().length() < SessionSnapshotDecoder.MAX_BYTES);
    var decoded = SessionSnapshotDecoder.decode(json);
    assertEquals(json, JsonObject.mapFrom(decoded.snapshot()));
    assertEquals(1765, decoded.snapshot().secondary().getJsonObject("assemblyInfo").getJsonArray("contigDescriptors").size());
  }

  @Test void invalidDocumentIsRejectedBeforeResolvingAnyResource() {
    var json = valid().put("tracks", "invalid");
    var failure = assertThrows(IllegalArgumentException.class,
      () -> SessionRestoreCandidate.prepare(Path.of("/nonexistent/session-decoder-test"), Path.of("/nonexistent"), json));
    assertTrue(failure.getMessage().contains("snapshot.tracks"), failure.getMessage());
  }
}
