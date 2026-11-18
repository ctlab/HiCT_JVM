package ru.itmo.ctlab.hict.hict_server.examples;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import ru.itmo.ctlab.hict.hict_server.handlers.conversion.ConversionDirection;
import ru.itmo.ctlab.hict.hict_server.util.cache.FileFingerprintService;
import ru.itmo.ctlab.hict.hict_server.util.cache.MatrixConversionCacheManager;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.HashMap;

/** Frozen recorded dataset metadata. Session restore validates it without fetching or executing its recipe. */
public final class ExampleSourceProvenance {
  private ExampleSourceProvenance() { }

  public static JsonObject capture(Path root, Path processed, Path opened, FileFingerprintService fingerprints) {
    try {
      var suppliedRoot = root.normalize().toAbsolutePath();
      root = root.toRealPath();
      opened = opened.toRealPath();
      if (!opened.startsWith(root)) return null;
      var relative = root.relativize(opened);
      if (relative.getNameCount() != 3 || !relative.getName(0).toString().equals("examples")) return null;
      var entry = ExamplesCatalog.load(root).find(relative.getName(1).toString());
      if (entry == null || !ExampleCache.available(root, entry.body())) return null;
      var folder = opened.getParent();
      var manifest = ExampleCache.matchingManifest(folder, entry.body());
      if (manifest == null) return null;
      var example = manifest.getJsonObject("catalogExample");
      var matrix = matrix(example);
      var input = folder.resolve(matrix.getString("filename"));
      String association;
      if (input.equals(opened)) {
        association = "DECLARED_HICT";
      } else {
        // Cache keys retain the spelling used at conversion time. macOS /var
        // aliases and Windows short paths can differ from the real data root.
        boolean current = false;
        for (var cacheRoot : java.util.List.of(suppliedRoot, root)) {
          var resolved = new MatrixConversionCacheManager(cacheRoot, processed, fingerprints)
            .resolveOpenPath(root.relativize(input).toString());
          if (resolved.action() == MatrixConversionCacheManager.MatrixOpenAction.REUSE_CONVERTED
            && cacheRoot.resolve(resolved.resolvedFilename()).toRealPath().equals(opened)) {
            current = true;
            break;
          }
        }
        if (!current) return null;
        association = "CURRENT_CONVERSION_CACHE";
      }
      var result = new JsonObject().put("schemaVersion", 1).put("capturedAt", Instant.now().toString())
        .put("catalogSource", manifest.getString("catalogSource", entry.source()))
        .put("catalogFingerprint", manifest.getString("catalogFingerprint"))
        .put("catalogExample", example.copy()).put("files", manifest.getJsonArray("files").copy())
        .put("matrixInputFilename", root.relativize(input).toString().replace('\\', '/'))
        .put("matrixOpenedFilename", relative.toString().replace('\\', '/'))
        .put("matrixAssociation", association);
      validate(result, relative.toString().replace('\\', '/'));
      return result;
    } catch (Exception unavailable) {
      // An ordinary local map remains openable without an example cache association.
      return null;
    }
  }

  private static JsonObject matrix(JsonObject example) {
    var id = example.getJsonObject("openingRecipe").getString("matrix");
    for (var value : example.getJsonArray("files")) {
      var file = (JsonObject) value;
      if (file.getString("id").equals(id)) return file;
    }
    throw new IllegalArgumentException("Example matrix is missing");
  }

  public static void validate(JsonObject value, String sourceFilename) {
    try {
      sourceFilename = Path.of(sourceFilename).normalize().toString().replace('\\', '/');
      if (!Integer.valueOf(1).equals(value.getValue("schemaVersion"))) throw new IllegalArgumentException("unsupported version");
      Instant.parse(value.getString("capturedAt"));
      var catalogSource = value.getString("catalogSource");
      if (catalogSource == null || catalogSource.length() > 8192) throw new IllegalArgumentException("invalid catalog source");
      var example = value.getJsonObject("catalogExample");
      var problems = new ArrayList<ExamplesCatalog.Problem>();
      var catalog = new JsonObject().put("schemaVersion", 1).put("catalogId", "session-provenance")
        .put("sections", new JsonArray().add(new JsonObject().put("id", example.getString("sectionId")).put("title", "Recorded example")))
        .put("examples", new JsonArray().add(example.copy()));
      if (ExamplesCatalog.validate("session provenance", catalog.encode(), problems) == null || !problems.isEmpty())
        throw new IllegalArgumentException("invalid recorded example: " + problems);
      if (!ExampleCache.fingerprint(example).equals(value.getString("catalogFingerprint")))
        throw new IllegalArgumentException("catalog fingerprint differs from recorded declaration");
      var matrix = matrix(example);
      var input = "examples/" + example.getString("id") + "/" + matrix.getString("filename");
      if (!input.equals(value.getString("matrixInputFilename")) || !sourceFilename.equals(value.getString("matrixOpenedFilename")))
        throw new IllegalArgumentException("source filenames do not match");
      var association = value.getString("matrixAssociation");
      if ("DECLARED_HICT".equals(association)) {
        if (!input.equals(sourceFilename) || !ConversionDirection.isHictFilename(sourceFilename))
          throw new IllegalArgumentException("declared matrix does not match");
      } else if ("CURRENT_CONVERSION_CACHE".equals(association)) {
        var expected = ConversionDirection.defaultForSource(Path.of(input)).deriveOutputPath(Path.of(input));
        if (!expected.toString().replace('\\', '/').equals(sourceFilename))
          throw new IllegalArgumentException("converted filename does not match");
      } else throw new IllegalArgumentException("unsupported matrix association");
      var declarations = new HashMap<String, JsonObject>();
      for (var file : example.getJsonArray("files")) {
        var spec = (JsonObject) file;
        declarations.put(spec.getString("filename"), spec);
      }
      var prep = example.getJsonObject("openingRecipe").getJsonObject("fastaPreparation");
      if (prep != null) declarations.put(prep.getString("filename"), prep);
      Set<String> seen = new HashSet<>();
      for (var file : value.getJsonArray("files")) {
        var record = (JsonObject) file;
        var name = record.getString("filename");
        var spec = declarations.get(name);
        var bytes = record.getValue("bytes");
        if (spec == null || !seen.add(name) || !(bytes instanceof Number size)
          || !Double.isFinite(size.doubleValue()) || size.doubleValue() < 0 || size.doubleValue() > 9007199254740991d
          || size.doubleValue() != size.longValue()
          || !record.getString("sha256", "").matches("[0-9a-f]{64}"))
          throw new IllegalArgumentException("invalid recorded file");
        if (spec.containsKey("sha256") && !spec.getString("sha256").equals(record.getString("sha256")))
          throw new IllegalArgumentException("recorded hash differs from declared hash");
        if (spec.getLong("bytes", 0L) > 0 && !spec.getLong("bytes").equals(record.getLong("bytes")))
          throw new IllegalArgumentException("recorded size differs from declared size");
      }
      for (var spec : declarations.values())
        if (spec.getBoolean("required", true) && !seen.contains(spec.getString("filename")))
          throw new IllegalArgumentException("required file hash is missing");
    } catch (RuntimeException invalid) {
      throw new IllegalArgumentException("Invalid session exampleProvenance: " + invalid.getMessage(), invalid);
    }
  }
}
