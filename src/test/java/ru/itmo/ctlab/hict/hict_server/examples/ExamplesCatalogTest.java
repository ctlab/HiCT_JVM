package ru.itmo.ctlab.hict.hict_server.examples;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;

class ExamplesCatalogTest {
  @TempDir Path data;

  private static JsonObject validExample(String id, String section) {
    return new JsonObject().put("id", id).put("sectionId", section).put("title", "Title " + id).put("description", "What it shows")
      .put("publications", new JsonArray().add(new JsonObject().put("title", "P").put("citation", "C").put("url", "https://doi.org/10.1/x").put("doi", "10.1234/xyz").put("kind", "method")))
      .put("files", new JsonArray()
        .add(new JsonObject().put("id", "m").put("role", "matrix").put("format", "hic").put("url", "https://h.example/m.hic").put("filename", "m.hic").put("required", true).put("bytes", 100).put("sha256", "a".repeat(64)))
        .add(new JsonObject().put("id", "f").put("role", "fasta").put("format", "fasta.gz").put("url", "https://h.example/f.fa.gz").put("filename", "f.fa.gz").put("required", true))
        .add(new JsonObject().put("id", "a").put("role", "assembly").put("format", "assembly").put("url", "https://h.example/a.assembly").put("filename", "a.assembly").put("required", false)))
      .put("attribution", new JsonObject().put("provider", "Prov").put("url", "https://p.example/").put("termsUrl", "https://p.example/terms").put("acknowledgement", "Thanks"))
      .put("openingRecipe", new JsonObject().put("matrix", "m").put("fasta", "f").put("assembly", "a"));
  }

  private static JsonObject catalog(String id, JsonObject... examples) {
    var array = new JsonArray();
    for (var e : examples) array.add(e);
    return new JsonObject().put("schemaVersion", 1).put("catalogId", id)
      .put("sections", new JsonArray().add(new JsonObject().put("id", "s1").put("title", "Section one").put("order", 2))
        .add(new JsonObject().put("id", "s0").put("title", "Section zero").put("order", 1)))
      .put("examples", array);
  }

  private static List<ExamplesCatalog.Problem> problems(JsonObject catalog) {
    var found = new ArrayList<ExamplesCatalog.Problem>();
    ExamplesCatalog.validate("t", catalog.encode(), found);
    return found;
  }

  private static String firstPointer(Consumer<JsonObject> mutate) {
    var example = validExample("e1", "s1");
    mutate.accept(example);
    var found = problems(catalog("c", example));
    return found.isEmpty() ? "" : found.get(0).pointer();
  }

  @Test void theBundledCatalogIsValidAndSeedsTwoCardsFromData() {
    var merged = ExamplesCatalog.load(null);
    assertTrue(merged.problems().isEmpty(), merged.problems().toString());
    assertEquals(List.of("dnazoo-cheetah", "dnazoo-aedes"), merged.examples().stream().map(ExamplesCatalog.Example::id).toList());
    var cheetah = merged.find("dnazoo-cheetah").body();
    assertEquals("https://www.dnazoo.org/usage", cheetah.getJsonObject("attribution").getString("termsUrl"));
    assertFalse(cheetah.getJsonObject("attribution").containsKey("licenseName"), "no licence is invented for the provider");
    assertEquals("draft", cheetah.getJsonObject("provenance").getString("reviewStatus"), "not claimed verified before it was opened");
    assertTrue(cheetah.getJsonArray("files").stream().map(o -> (JsonObject) o).anyMatch(f -> f.getString("filename").equals("aciJub1_HiC.hic") && f.getLong("bytes") == 1407754884L));
  }

  @Test void validExamplesPassAndSectionsFollowTheirOrderFromData() {
    assertTrue(problems(catalog("c", validExample("e1", "s1"))).isEmpty());
    var dir = data.resolve("examples/catalogs");
    try {
      Files.createDirectories(dir);
      Files.writeString(dir.resolve("a.json"), catalog("local", validExample("local1", "s1")).encode());
    } catch (java.io.IOException e) {
      fail(e);
    }
    var merged = ExamplesCatalog.load(data);
    assertTrue(merged.problems().isEmpty(), merged.problems().toString());
    assertEquals(List.of("s0", "s1", "dnazoo"), merged.sections().stream().map(x -> x.getString("id")).toList(), "order comes from the data");
    var local = merged.find("local1");
    assertFalse(local.bundled());
    assertEquals("local:a.json", local.source());
  }

  @Test void problemsNameTheJsonPointer() {
    assertEquals("/examples/0/files/0/url", firstPointer(e -> e.getJsonArray("files").getJsonObject(0).put("url", "http://insecure.example/x")));
    assertEquals("/examples/0/files/0/filename", firstPointer(e -> e.getJsonArray("files").getJsonObject(0).put("filename", "../x")));
    assertEquals("/examples/0/files/0/sha256", firstPointer(e -> e.getJsonArray("files").getJsonObject(0).put("sha256", "XYZ")));
    assertEquals("/examples/0/files/0/bytes", firstPointer(e -> e.getJsonArray("files").getJsonObject(0).put("bytes", -5)));
    assertEquals("/examples/0/files/0/format", firstPointer(e -> e.getJsonArray("files").getJsonObject(0).put("format", "exe")));
    assertEquals("/examples/0/sectionId", firstPointer(e -> e.put("sectionId", "nope")));
    assertEquals("/examples/0/title", firstPointer(e -> e.put("title", "")));
    assertEquals("/examples/0/title", firstPointer(e -> e.put("title", "x".repeat(301))));
    assertEquals("/examples/0/publications/0/doi", firstPointer(e -> e.getJsonArray("publications").getJsonObject(0).put("doi", "not a doi")));
    assertEquals("/examples/0/attribution/termsUrl", firstPointer(e -> e.getJsonObject("attribution").put("termsUrl", "javascript:alert(1)")));
    assertEquals("/examples/0/surprise", firstPointer(e -> e.put("surprise", 1)));
  }

  @Test void recipesMustPointAtFilesOfTheRightRole() {
    assertEquals("/examples/0/openingRecipe/agp", firstPointer(e -> e.getJsonObject("openingRecipe").put("agp", "a")), "an assembly file is not an AGP");
    assertEquals("/examples/0/openingRecipe/matrix", firstPointer(e -> e.getJsonObject("openingRecipe").put("matrix", "missing")));
    assertEquals("/examples/0/openingRecipe/fasta", firstPointer(e -> e.getJsonObject("openingRecipe").put("fasta", "m")));
  }

  @Test void duplicateIdsAreFoundWithinAndAcrossCatalogs() throws Exception {
    var found = problems(catalog("c", validExample("e1", "s1"), validExample("e1", "s1")));
    assertTrue(found.stream().anyMatch(p -> p.message().contains("Duplicate example id")), found.toString());
    var dupFiles = validExample("e2", "s1");
    dupFiles.getJsonArray("files").getJsonObject(1).put("id", "m");
    assertFalse(problems(catalog("c", dupFiles)).isEmpty());
    var dir = data.resolve("examples/catalogs");
    Files.createDirectories(dir);
    Files.writeString(dir.resolve("one.json"), catalog("one", validExample("shared", "s1")).encode());
    Files.writeString(dir.resolve("two.json"), catalog("two", validExample("shared", "s1"), validExample("extra", "s1")).encode());
    var merged = ExamplesCatalog.load(data);
    assertEquals("local:one.json", merged.find("shared").source());
    assertNull(merged.find("extra"), "an ambiguous file is not used at all");
    assertTrue(merged.problems().stream().anyMatch(p -> p.source().equals("local:two.json") && p.pointer().equals("/examples/0/id")), merged.problems().toString());
  }

  @Test void aBadLocalCatalogKeepsTheLastValidOne() throws Exception {
    var dir = data.resolve("examples/catalogs");
    Files.createDirectories(dir);
    Files.writeString(dir.resolve("broken.json"), "{ not json");
    Files.writeString(dir.resolve("old.json"), catalog("old", validExample("keep", "s1")).put("schemaVersion", 2).encode());
    var merged = ExamplesCatalog.load(data);
    assertEquals(2, merged.examples().size(), "only the bundled cards remain");
    assertEquals(2, merged.problems().size());
    assertTrue(merged.problems().stream().anyMatch(p -> p.message().contains("Not valid JSON")));
    assertTrue(merged.problems().stream().anyMatch(p -> p.pointer().equals("/schemaVersion")));
  }

  @Test void oversizedCatalogsAreRefused() {
    var found = new ArrayList<ExamplesCatalog.Problem>();
    assertNull(ExamplesCatalog.validate("t", "{\"x\":\"" + "a".repeat(ExamplesCatalog.MAX_CATALOG_BYTES) + "\"}", found));
    assertFalse(found.isEmpty());
  }

  @Test void theSchemaFileIsValidJsonAndDescribesTheSameEnumerations() throws Exception {
    try (var in = getClass().getResourceAsStream("/examples/hict-examples.schema.json")) {
      var schema = new JsonObject(new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8));
      assertEquals("https://json-schema.org/draft/2020-12/schema", schema.getString("$schema"));
      var roles = schema.getJsonObject("$defs").getJsonObject("file").getJsonObject("properties").getJsonObject("role").getJsonArray("enum");
      assertEquals(List.of("matrix", "fasta", "agp", "assembly", "track"), roles.getList());
    }
  }
}
