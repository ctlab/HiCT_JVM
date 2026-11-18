package ru.itmo.ctlab.hict.hict_server.examples;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import ru.itmo.ctlab.hict.hict_server.handlers.conversion.ConversionDirection;
import ru.itmo.ctlab.hict.hict_server.util.cache.FileFingerprintService;
import ru.itmo.ctlab.hict.hict_server.util.cache.MatrixConversionCacheManager;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;

class ExampleSourceProvenanceTest {
  @TempDir Path root;

  private JsonObject example(String filename, String format) throws Exception {
    var folder = Files.createDirectories(root.resolve("examples/provenance-fixture"));
    Files.writeString(folder.resolve(filename), "Synthetic input bytes");
    var hash = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(folder.resolve(filename))));
    var example = new JsonObject().put("id", "provenance-fixture").put("sectionId", "fixtures")
      .put("title", "Recorded fixture").put("description", "An independently identified fixture")
      .put("publications", new JsonArray().add(new JsonObject().put("title", "Fixture paper").put("citation", "Original citation").put("url", "https://example.org/paper")))
      .put("attribution", new JsonObject().put("provider", "Original provider").put("url", "https://example.org/")
        .put("termsUrl", "https://example.org/terms").put("acknowledgement", "Original acknowledgement"))
      .put("files", new JsonArray().add(new JsonObject().put("id", "matrix").put("role", "matrix").put("format", format)
        .put("url", "https://example.org/" + filename).put("filename", filename).put("required", true).put("sha256", hash)))
      .put("openingRecipe", new JsonObject().put("matrix", "matrix").put("quickStart", true));
    var manifest = new JsonObject().put("exampleId", example.getString("id")).put("catalogFingerprint", ExampleCache.fingerprint(example))
      .put("catalogExample", example.copy()).put("catalogSource", "original-catalog.json")
      .put("files", new JsonArray().add(new JsonObject().put("filename", filename).put("sha256", hash)
        .put("bytes", Files.size(folder.resolve(filename))).put("modifiedAt", Files.getLastModifiedTime(folder.resolve(filename)).toString())));
    Files.writeString(folder.resolve("manifest.json"), manifest.encode());
    var catalog = new JsonObject().put("schemaVersion", 1).put("catalogId", "provenance-fixture")
      .put("sections", new JsonArray().add(new JsonObject().put("id", "fixtures").put("title", "Fixtures")))
      .put("examples", new JsonArray().add(example));
    Files.createDirectories(root.resolve("examples/catalogs"));
    Files.writeString(root.resolve("examples/catalogs/fixture.json"), catalog.encode());
    return example;
  }

  private JsonObject capture(String filename) {
    return ExampleSourceProvenance.capture(root, root.resolve("processed"), root.resolve("examples/provenance-fixture/" + filename), new FileFingerprintService());
  }

  @Test void freezesCitationRecipeAndHashesWithoutDependingOnTheCurrentCatalogDuringRestore() throws Exception {
    var example = example("fixture.hict.hdf5", "hict");
    var provenance = capture("fixture.hict.hdf5");
    assertNotNull(provenance);
    assertEquals(example, provenance.getJsonObject("catalogExample"));
    assertEquals("DECLARED_HICT", provenance.getString("matrixAssociation"));
    assertEquals("original-catalog.json", provenance.getString("catalogSource"));
    assertEquals(1, provenance.getJsonArray("files").size());
    Files.delete(root.resolve("examples/catalogs/fixture.json"));
    Files.delete(root.resolve("examples/provenance-fixture/manifest.json"));
    assertDoesNotThrow(() -> ExampleSourceProvenance.validate(provenance, "examples/provenance-fixture/fixture.hict.hdf5"));
    assertEquals("Original citation", provenance.getJsonObject("catalogExample").getJsonArray("publications").getJsonObject(0).getString("citation"));
  }

  @Test void convertedAssociationRequiresCurrentMatchingCacheAndReadySourceFiles() throws Exception {
    example("fixture.mcool", "mcool");
    var source = root.resolve("examples/provenance-fixture/fixture.mcool");
    var output = root.resolve("examples/provenance-fixture/fixture.hict.hdf5");
    Files.writeString(output, "Converted output bytes");
    assertNull(capture("fixture.hict.hdf5"));
    var cache = new MatrixConversionCacheManager(root, root.resolve("processed"), new FileFingerprintService());
    cache.recordSuccessfulConversion(source, output, ConversionDirection.MCOOL_TO_HICT);
    var provenance = capture("fixture.hict.hdf5");
    assertNotNull(provenance);
    assertEquals("CURRENT_CONVERSION_CACHE", provenance.getString("matrixAssociation"));
    Files.writeString(output, "Unrelated replacement with a different size");
    assertNull(capture("fixture.hict.hdf5"));
    assertDoesNotThrow(() -> ExampleSourceProvenance.validate(provenance, "examples/provenance-fixture/fixture.hict.hdf5"));
  }

  @Test void malformedOrMismatchedRecordedMetadataIsRejected() throws Exception {
    example("fixture.hict.hdf5", "hict");
    var valid = capture("fixture.hict.hdf5");
    assertNotNull(valid);
    var mutations = java.util.List.<Consumer<JsonObject>>of(
      p -> p.put("schemaVersion", 2),
      p -> p.put("schemaVersion", 1.5),
      p -> p.put("schemaVersion", "1"),
      p -> p.put("capturedAt", "yesterday"),
      p -> p.put("matrixOpenedFilename", "unrelated.hict.hdf5"),
      p -> p.put("matrixInputFilename", "wrong.hict.hdf5"),
      p -> p.put("matrixAssociation", "UNVERIFIED"),
      p -> p.put("catalogFingerprint", "0".repeat(64)),
      p -> p.put("files", new JsonArray()),
      p -> p.getJsonArray("files").getJsonObject(0).put("sha256", "0".repeat(64)),
      p -> p.getJsonArray("files").getJsonObject(0).put("bytes", 0.5),
      p -> p.getJsonArray("files").getJsonObject(0).put("filename", "../secret"),
      p -> p.getJsonObject("catalogExample").getJsonObject("attribution").put("termsUrl", "javascript:alert(1)"));
    for (var mutate : mutations) {
      var invalid = valid.copy();
      mutate.accept(invalid);
      assertThrows(IllegalArgumentException.class, () -> ExampleSourceProvenance.validate(invalid, "examples/provenance-fixture/fixture.hict.hdf5"));
    }
  }

  @Test
  @EnabledOnOs({OS.LINUX, OS.MAC})
  void convertedProvenanceFindsTheExistingCacheThroughAnAliasedDataRoot() throws Exception {
    example("fixture.mcool", "mcool");
    var alias = root.resolve("data-root-alias");
    Files.createSymbolicLink(alias, root);
    var output = alias.resolve("examples/provenance-fixture/fixture.hict.hdf5");
    Files.writeString(output, "Converted output bytes");
    // TempDir may itself use /var rather than /private/var on macOS.
    // Exercise the supplied alias and the actual canonical root independently.
    for (var conversionRoot : java.util.List.of(alias, root.toRealPath())) {
      var cache = new MatrixConversionCacheManager(conversionRoot, alias.resolve("processed"), new FileFingerprintService());
      cache.dropAllMetadata();
      cache.recordSuccessfulConversion(conversionRoot.resolve("examples/provenance-fixture/fixture.mcool"),
        conversionRoot.resolve("examples/provenance-fixture/fixture.hict.hdf5"), ConversionDirection.MCOOL_TO_HICT);
      var provenance = ExampleSourceProvenance.capture(alias, alias.resolve("processed"), output, new FileFingerprintService());
      assertNotNull(provenance, () -> "Missing provenance for conversion root " + conversionRoot);
      assertEquals("CURRENT_CONVERSION_CACHE", provenance.getString("matrixAssociation"));
      assertEquals("examples/provenance-fixture/fixture.hict.hdf5", provenance.getString("matrixOpenedFilename"));
      assertDoesNotThrow(() -> ExampleSourceProvenance.validate(provenance, "examples/provenance-fixture/fixture.hict.hdf5"));
    }
  }

  @Test void unrelatedMapsAndUnavailableCacheHaveNoInferredAttribution() throws Exception {
    var example = example("fixture.hict.hdf5", "hict");
    Files.writeString(root.resolve("local.hict.hdf5"), "local bytes");
    assertNull(ExampleSourceProvenance.capture(root, root.resolve("processed"), root.resolve("local.hict.hdf5"), new FileFingerprintService()));
    assertEquals(new JsonArray().add("fixture.hict.hdf5"), ExampleCache.readyFiles(root, example));
    Files.delete(root.resolve("examples/provenance-fixture/fixture.hict.hdf5"));
    assertNull(capture("fixture.hict.hdf5"));
    assertNull(ExampleCache.readyFiles(root, example));
  }
}
