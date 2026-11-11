/*
 * MIT License
 *
 * Copyright (c) 2021-2026. Aleksandr Serdiukov, Anton Zamyatin, Aleksandr Sinitsyn, Vitalii Dravgelis and Computer Technologies Laboratory ITMO University team.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ru.itmo.ctlab.hict.hict_server.examples;

import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * The catalog of example datasets, read from data and nothing else: sections, cards, files and
 * attribution all come from JSON that follows {@code examples/hict-examples.schema.json}.
 *
 * <p>The bundled catalog is merged with {@code <data>/examples/catalogs/*.json}. A file that does not
 * validate is reported with its name and the JSON pointer of the first problem and is left out, and the
 * rest of the catalog stays as it was. An example id that two catalogs both define is ambiguous, so the
 * later file is rejected. Catalogs added locally are marked and are not trusted more than the bundled one.
 * The JSON Schema file documents the shape for tools that generate catalogs; the rules are enforced here
 * (lengths, enumerations, URL scheme, file names, hashes, id references, size limit).
 */
public final class ExamplesCatalog {
  public static final int SCHEMA_VERSION = 1;
  public static final int MAX_CATALOG_BYTES = 1 << 20;
  private static final Pattern ID = Pattern.compile("^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$");
  private static final Pattern FILENAME = Pattern.compile("^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$");
  private static final Pattern SHA256 = Pattern.compile("^[0-9a-f]{64}$");
  private static final Pattern DOI = Pattern.compile("^10\\.\\d{4,9}/\\S+$");
  private static final Pattern DATE = Pattern.compile("^\\d{4}-\\d{2}-\\d{2}$");
  private static final Set<String> ROLES = Set.of("matrix", "fasta", "agp", "assembly", "track");
  private static final Set<String> FORMATS = Set.of("hic", "mcool", "hict", "fasta", "fasta.gz", "agp", "assembly", "bed", "bigwig", "gff", "vcf");

  /** A problem in one catalog file. {@code pointer} is a JSON pointer such as {@code /examples/0/files/1/url}. */
  public record Problem(@NotNull String source, @NotNull String pointer, @NotNull String message) {
  }

  /** One validated example, with the catalog it came from. {@code body} is the example as written. */
  public record Example(@NotNull String id, @NotNull String sectionId, @NotNull String source, boolean bundled,
                        @NotNull JsonObject body) {
  }

  public record Merged(@NotNull List<JsonObject> sections, @NotNull List<Example> examples,
                       @NotNull List<Problem> problems) {
    public JsonObject toJson() {
      final var sectionArray = new JsonArray();
      sections.forEach(sectionArray::add);
      final var exampleArray = new JsonArray();
      for (final var example : examples) {
        exampleArray.add(example.body().copy().put("source", example.source()).put("bundled", example.bundled()));
      }
      final var problemArray = new JsonArray();
      for (final var problem : problems) {
        problemArray.add(new JsonObject().put("source", problem.source()).put("pointer", problem.pointer())
          .put("message", problem.message()));
      }
      return new JsonObject().put("schemaVersion", SCHEMA_VERSION).put("sections", sectionArray)
        .put("examples", exampleArray).put("problems", problemArray);
    }

    public Example find(final String id) {
      return examples.stream().filter(e -> e.id().equals(id)).findFirst().orElse(null);
    }
  }

  private ExamplesCatalog() {
  }

  /** Reads the bundled catalog and merges any local ones. Never throws for bad local files. */
  public static @NotNull Merged load(final Path dataDirectory) {
    final var problems = new ArrayList<Problem>();
    final var sections = new LinkedHashMap<String, JsonObject>();
    final var examples = new LinkedHashMap<String, Example>();
    try (InputStream in = ExamplesCatalog.class.getResourceAsStream("/examples/hict-examples.json")) {
      if (in == null) {
        problems.add(new Problem("bundled", "", "The bundled catalog is missing"));
      } else {
        merge("bundled", true, new String(in.readAllBytes(), StandardCharsets.UTF_8), sections, examples, problems);
      }
    } catch (final IOException e) {
      problems.add(new Problem("bundled", "", "The bundled catalog could not be read: " + e.getMessage()));
    }
    if (dataDirectory != null) {
      final var folder = dataDirectory.resolve("examples").resolve("catalogs");
      if (Files.isDirectory(folder)) {
        try (Stream<Path> list = Files.list(folder)) {
          for (final var file : list.filter(p -> p.getFileName().toString().endsWith(".json"))
            .sorted(Comparator.comparing(p -> p.getFileName().toString())).toList()) {
            final var name = "local:" + file.getFileName();
            try {
              if (Files.size(file) > MAX_CATALOG_BYTES) {
                problems.add(new Problem(name, "", "The file is larger than " + MAX_CATALOG_BYTES + " bytes"));
                continue;
              }
              merge(name, false, Files.readString(file, StandardCharsets.UTF_8), sections, examples, problems);
            } catch (final IOException | RuntimeException e) {
              problems.add(new Problem(name, "", "The file could not be read: " + e.getMessage()));
            }
          }
        } catch (final IOException e) {
          problems.add(new Problem("local", "", "The catalogs folder could not be listed: " + e.getMessage()));
        }
      }
    }
    final var orderedSections = new ArrayList<>(sections.values());
    orderedSections.sort(Comparator.comparingInt((JsonObject s) -> s.getInteger("order", Integer.MAX_VALUE))
      .thenComparing(s -> s.getString("title")));
    return new Merged(List.copyOf(orderedSections), List.copyOf(examples.values()), List.copyOf(problems));
  }

  private static void merge(final String source, final boolean bundled, final String text,
                            final Map<String, JsonObject> sections, final Map<String, Example> examples,
                            final List<Problem> problems) {
    final var found = new ArrayList<Problem>();
    final var parsed = validate(source, text, found);
    if (!found.isEmpty() || parsed == null) {
      problems.addAll(found);
      return;
    }
    // Duplicate ids across catalogs are ambiguous: refuse the whole later file and say which id.
    final var incoming = parsed.getJsonArray("examples");
    for (int i = 0; i < incoming.size(); i++) {
      final var id = incoming.getJsonObject(i).getString("id");
      if (examples.containsKey(id)) {
        problems.add(new Problem(source, "/examples/" + i + "/id",
          "Example id " + id + " is already defined by " + examples.get(id).source() + ". The file was not used."));
        return;
      }
    }
    final var incomingSections = parsed.getJsonArray("sections");
    for (int i = 0; i < incomingSections.size(); i++) {
      final var section = incomingSections.getJsonObject(i);
      sections.putIfAbsent(section.getString("id"), section);
    }
    for (int i = 0; i < incoming.size(); i++) {
      final var body = incoming.getJsonObject(i);
      examples.put(body.getString("id"), new Example(body.getString("id"), body.getString("sectionId"), source, bundled, body));
    }
  }

  /** Validates one catalog text. Returns it when valid; otherwise adds problems and returns null. */
  public static JsonObject validate(final String source, final String text, final List<Problem> problems) {
    final int before = problems.size();
    if (text.getBytes(StandardCharsets.UTF_8).length > MAX_CATALOG_BYTES) {
      problems.add(new Problem(source, "", "The catalog is larger than " + MAX_CATALOG_BYTES + " bytes"));
      return null;
    }
    final JsonObject root;
    try {
      root = new JsonObject(text);
    } catch (final DecodeException | ClassCastException e) {
      problems.add(new Problem(source, "", "Not valid JSON: " + firstLine(e.getMessage())));
      return null;
    }
    final var v = new Checker(source, problems);
    v.noExtra(root, "", Set.of("schemaVersion", "catalogId", "generatedAt", "generator", "sections", "examples"));
    if (!(root.getValue("schemaVersion") instanceof Integer version) || version != SCHEMA_VERSION) {
      v.fail("/schemaVersion", "Unsupported schema version. This build reads version " + SCHEMA_VERSION);
    }
    v.id(root, "/catalogId", "catalogId");
    if (root.containsKey("generator")) v.text(root, "/generator", "generator", 200);
    final var sectionIds = new HashSet<String>();
    final var sections = v.array(root, "/sections", "sections", 50);
    for (int i = 0; sections != null && i < sections.size(); i++) {
      final var p = "/sections/" + i;
      final var s = v.object(sections, i, p);
      if (s == null) continue;
      v.noExtra(s, p, Set.of("id", "title", "description", "order"));
      if (v.id(s, p + "/id", "id") && !sectionIds.add(s.getString("id"))) v.fail(p + "/id", "Duplicate section id " + s.getString("id"));
      v.text(s, p + "/title", "title", 300);
      if (s.containsKey("description")) v.text(s, p + "/description", "description", 2000);
      if (s.containsKey("order") && !(s.getValue("order") instanceof Integer o && o >= 0 && o <= 100000)) v.fail(p + "/order", "order must be an integer from 0 to 100000");
    }
    final var exampleIds = new HashSet<String>();
    final var examples = v.array(root, "/examples", "examples", 500);
    for (int i = 0; examples != null && i < examples.size(); i++) {
      example(v, examples, i, sectionIds, exampleIds);
    }
    return problems.size() == before ? root : null;
  }

  private static void example(final Checker v, final JsonArray examples, final int index, final Set<String> sectionIds,
                              final Set<String> exampleIds) {
    final var p = "/examples/" + index;
    final var e = v.object(examples, index, p);
    if (e == null) return;
    v.noExtra(e, p, Set.of("id", "sectionId", "title", "description", "organism", "assemblyIds", "tags", "publications",
      "files", "attribution", "openingRecipe", "provenance"));
    if (v.id(e, p + "/id", "id") && !exampleIds.add(e.getString("id"))) v.fail(p + "/id", "Duplicate example id " + e.getString("id"));
    if (v.id(e, p + "/sectionId", "sectionId") && !sectionIds.contains(e.getString("sectionId"))) {
      v.fail(p + "/sectionId", "Unknown section " + e.getString("sectionId"));
    }
    v.text(e, p + "/title", "title", 300);
    v.text(e, p + "/description", "description", 2000);
    if (e.containsKey("organism")) v.text(e, p + "/organism", "organism", 300);
    for (final var key : List.of("assemblyIds", "tags")) {
      final var list = e.containsKey(key) ? v.array(e, p + "/" + key, key, 30) : null;
      for (int i = 0; list != null && i < list.size(); i++) {
        if (!(list.getValue(i) instanceof String s) || s.isBlank() || s.length() > 300) v.fail(p + "/" + key + "/" + i, "Expected a text of 1 to 300 characters");
      }
    }
    final var publications = v.array(e, p + "/publications", "publications", 20);
    for (int i = 0; publications != null && i < publications.size(); i++) {
      final var q = p + "/publications/" + i;
      final var pub = v.object(publications, i, q);
      if (pub == null) continue;
      v.noExtra(pub, q, Set.of("title", "citation", "url", "doi", "kind"));
      v.text(pub, q + "/title", "title", 300);
      v.text(pub, q + "/citation", "citation", 2000);
      v.https(pub, q + "/url", "url");
      if (pub.containsKey("doi") && !(pub.getValue("doi") instanceof String d && DOI.matcher(d).matches())) v.fail(q + "/doi", "Not a DOI");
      if (pub.containsKey("kind") && !(Set.of("method", "dataset").contains(pub.getValue("kind")))) v.fail(q + "/kind", "kind must be method or dataset");
    }
    final var fileIds = new HashMap<String, String>();
    final var files = v.array(e, p + "/files", "files", 30);
    if (files != null && files.isEmpty()) v.fail(p + "/files", "An example needs at least one file");
    final var filenames = new HashSet<String>();
    for (int i = 0; files != null && i < files.size(); i++) {
      final var q = p + "/files/" + i;
      final var f = v.object(files, i, q);
      if (f == null) continue;
      v.noExtra(f, q, Set.of("id", "role", "format", "url", "filename", "required", "bytes", "sha256", "description"));
      final boolean idOk = v.id(f, q + "/id", "id");
      if (idOk && fileIds.put(f.getString("id"), String.valueOf(f.getValue("role"))) != null) v.fail(q + "/id", "Duplicate file id " + f.getString("id"));
      if (!(ROLES.contains(f.getValue("role")))) v.fail(q + "/role", "role must be one of " + new java.util.TreeSet<>(ROLES));
      if (!(FORMATS.contains(f.getValue("format")))) v.fail(q + "/format", "Unsupported format");
      v.https(f, q + "/url", "url");
      if (!(f.getValue("filename") instanceof String n && FILENAME.matcher(n).matches())) v.fail(q + "/filename", "filename must be a plain file name of letters, digits, dot, dash and underscore");
      else if (!filenames.add(n)) v.fail(q + "/filename", "Two files of one example use the file name " + n);
      if (!(f.getValue("required") instanceof Boolean)) v.fail(q + "/required", "required must be true or false");
      if (f.containsKey("bytes") && !(f.getValue("bytes") instanceof Number n && n.longValue() >= 1 && n.longValue() <= (1L << 40) && n.doubleValue() == n.longValue())) v.fail(q + "/bytes", "bytes must be a whole number from 1 to 2^40");
      if (f.containsKey("sha256") && !(f.getValue("sha256") instanceof String h && SHA256.matcher(h).matches())) v.fail(q + "/sha256", "sha256 must be 64 lower-case hex digits");
      if (f.containsKey("description")) v.text(f, q + "/description", "description", 2000);
    }
    final var a = v.objectField(e, p + "/attribution", "attribution");
    if (a != null) {
      v.noExtra(a, p + "/attribution", Set.of("provider", "url", "termsUrl", "acknowledgement", "licenseName", "licenseUrl", "restrictions"));
      v.text(a, p + "/attribution/provider", "provider", 300);
      v.https(a, p + "/attribution/url", "url");
      v.https(a, p + "/attribution/termsUrl", "termsUrl");
      v.text(a, p + "/attribution/acknowledgement", "acknowledgement", 2000);
      if (a.containsKey("licenseName")) v.text(a, p + "/attribution/licenseName", "licenseName", 300);
      if (a.containsKey("licenseUrl")) v.https(a, p + "/attribution/licenseUrl", "licenseUrl");
      if (a.containsKey("restrictions")) v.text(a, p + "/attribution/restrictions", "restrictions", 2000);
    }
    final var r = v.objectField(e, p + "/openingRecipe", "openingRecipe");
    if (r != null) {
      v.noExtra(r, p + "/openingRecipe", Set.of("matrix", "fasta", "agp", "assembly", "tracks", "referenceId", "quickStart", "fastaPreparation"));
      if (r.containsKey("quickStart") && !(r.getValue("quickStart") instanceof Boolean))
        v.fail(p + "/openingRecipe/quickStart", "quickStart must be true or false");
      for (final var role : List.of("matrix", "fasta", "agp", "assembly")) {
        if (!r.containsKey(role)) continue;
        final var id = r.getValue(role);
        final var q = p + "/openingRecipe/" + role;
        if (!(id instanceof String s) || !fileIds.containsKey(s)) v.fail(q, "Refers to a file id that this example does not have");
        else if (!fileIds.get(s).equals(role)) v.fail(q, "File " + s + " has role " + fileIds.get(s) + ", not " + role + " (an assembly file is not an AGP)");
      }
      final var tracks = r.containsKey("tracks") ? v.array(r, p + "/openingRecipe/tracks", "tracks", 20) : null;
      for (int i = 0; tracks != null && i < tracks.size(); i++) {
        if (!(tracks.getValue(i) instanceof String s) || !"track".equals(fileIds.get(s))) v.fail(p + "/openingRecipe/tracks/" + i, "Must name a file with role track");
      }
      if (r.containsKey("referenceId")) v.text(r, p + "/openingRecipe/referenceId", "referenceId", 300);
      if (r.containsKey("fastaPreparation")) validatePreparation(v, e, r, p + "/openingRecipe/fastaPreparation");
    }
    if (e.containsKey("provenance")) {
      final var pr = v.objectField(e, p + "/provenance", "provenance");
      if (pr != null) {
        v.noExtra(pr, p + "/provenance", Set.of("sourceUrl", "retrieved", "origin", "generatorVersion", "reviewStatus"));
        if (pr.containsKey("sourceUrl")) v.https(pr, p + "/provenance/sourceUrl", "sourceUrl");
        if (pr.containsKey("retrieved") && !(pr.getValue("retrieved") instanceof String d && DATE.matcher(d).matches())) v.fail(p + "/provenance/retrieved", "Expected a date as YYYY-MM-DD");
        if (pr.containsKey("origin") && !(Set.of("curated", "pipeline").contains(pr.getValue("origin")))) v.fail(p + "/provenance/origin", "origin must be curated or pipeline");
        if (pr.containsKey("reviewStatus") && !(Set.of("draft", "reviewed", "verified").contains(pr.getValue("reviewStatus")))) v.fail(p + "/provenance/reviewStatus", "reviewStatus must be draft, reviewed or verified");
        if (pr.containsKey("generatorVersion")) v.text(pr, p + "/provenance/generatorVersion", "generatorVersion", 300);
      }
    }
  }

  private static void validatePreparation(Checker v, JsonObject example, JsonObject recipe, String path) {
    var preparation = v.objectField(recipe, path, "fastaPreparation");
    if (preparation == null) return;
    v.noExtra(preparation, path, Set.of("filename", "sha256", "records"));
    var filename = preparation.getValue("filename");
    if (!(filename instanceof String name) || !FILENAME.matcher(name).matches() || !name.endsWith(".fa"))
      v.fail(path + "/filename", "Expected a plain .fa filename");
    var files = example.getValue("files") instanceof JsonArray entries ? entries : new JsonArray();
    for (var entry : files) {
      if (entry instanceof JsonObject file && java.util.Objects.equals(filename, file.getValue("filename")))
        v.fail(path + "/filename", "Prepared reference must have a distinct filename");
    }
    if (!recipe.containsKey("fasta")) v.fail(path, "Preparation requires a FASTA source");
    if (!(preparation.getValue("sha256") instanceof String hash) || !SHA256.matcher(hash).matches())
      v.fail(path + "/sha256", "Expected SHA-256 of the prepared reference");
    var records = v.array(preparation, path + "/records", "records", 1000);
    if (records == null || records.isEmpty()) { v.fail(path + "/records", "Expected selected reference records"); return; }
    var sources = new HashSet<String>();
    var targets = new HashSet<String>();
    long total = 0;
    for (int i = 0; i < records.size(); i++) {
      var pointer = path + "/records/" + i;
      if (!(records.getValue(i) instanceof JsonObject record)) { v.fail(pointer, "Expected a reference record"); continue; }
      v.noExtra(record, pointer, Set.of("source", "target", "length"));
      for (var key : List.of("source", "target")) {
        var value = record.getValue(key);
        var unique = key.equals("source") ? sources : targets;
        if (!(value instanceof String id) || !FILENAME.matcher(id).matches() || !unique.add(id))
          v.fail(pointer + "/" + key, "Expected a unique plain sequence identifier");
      }
      if (!(record.getValue("length") instanceof Number length) || length.longValue() < 1
          || length.longValue() > (32L << 20) || length.doubleValue() != length.longValue())
        v.fail(pointer + "/length", "Expected a positive bounded reference length");
      else total += length.longValue();
    }
    if (total > (32L << 20)) v.fail(path + "/records", "Prepared reference exceeds 32 MiB");
  }

  private static String firstLine(final String text) {
    if (text == null) return "";
    final int newline = text.indexOf('\n');
    return newline < 0 ? text : text.substring(0, newline);
  }

  private static final class Checker {
    private final String source;
    private final List<Problem> problems;

    Checker(final String source, final List<Problem> problems) {
      this.source = source;
      this.problems = problems;
    }

    void fail(final String pointer, final String message) {
      problems.add(new Problem(source, pointer, message));
    }

    void noExtra(final JsonObject o, final String pointer, final Set<String> allowed) {
      for (final var key : o.fieldNames()) {
        if (!allowed.contains(key)) fail(pointer + "/" + key, "Unknown property " + key);
      }
    }

    boolean id(final JsonObject o, final String pointer, final String key) {
      if (o.getValue(key) instanceof String s && ID.matcher(s).matches()) return true;
      fail(pointer, key + " must be 1 to 64 letters, digits, dot, dash or underscore, starting with a letter or digit");
      return false;
    }

    void text(final JsonObject o, final String pointer, final String key, final int max) {
      if (!(o.getValue(key) instanceof String s) || s.isBlank() || s.length() > max) fail(pointer, key + " must be a text of 1 to " + max + " characters");
    }

    void https(final JsonObject o, final String pointer, final String key) {
      if (!(o.getValue(key) instanceof String s) || !s.startsWith("https://") || s.length() > 1000 || s.chars().anyMatch(Character::isWhitespace)) {
        fail(pointer, key + " must be an https:// address");
        return;
      }
      try {
        final var uri = java.net.URI.create(s);
        if (uri.getHost() == null || uri.getUserInfo() != null) fail(pointer, key + " must name a host and carry no credentials");
      } catch (final IllegalArgumentException e) {
        fail(pointer, key + " is not a valid address");
      }
    }

    JsonArray array(final JsonObject o, final String pointer, final String key, final int max) {
      if (!(o.getValue(key) instanceof JsonArray a)) {
        fail(pointer, key + " must be an array");
        return null;
      }
      if (a.size() > max) fail(pointer, key + " has more than " + max + " entries");
      return a;
    }

    JsonObject object(final JsonArray a, final int index, final String pointer) {
      if (a.getValue(index) instanceof JsonObject o) return o;
      fail(pointer, "Expected an object");
      return null;
    }

    JsonObject objectField(final JsonObject o, final String pointer, final String key) {
      if (o.getValue(key) instanceof JsonObject inner) return inner;
      fail(pointer, key + " must be an object");
      return null;
    }
  }
}
