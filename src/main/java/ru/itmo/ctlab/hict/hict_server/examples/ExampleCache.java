package ru.itmo.ctlab.hict.hict_server.examples;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.TreeMap;

/** Cheap cache discovery. Opening still rehashes files in the cancellable download job. */
public final class ExampleCache {
  private ExampleCache() { }

  public static String fingerprint(JsonObject example) {
    var declaration = example.copy();
    declaration.remove("downloaded");
    declaration.remove("source");
    declaration.remove("bundled");
    declaration.remove("downloadedFiles");
    try {
      return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
        .digest(Json.encode(canonical(declaration)).getBytes(java.nio.charset.StandardCharsets.UTF_8)));
    } catch (NoSuchAlgorithmException impossible) {
      throw new IllegalStateException(impossible);
    }
  }

  private static Object canonical(Object value) {
    if (value instanceof JsonObject object) {
      var sorted = new TreeMap<String, Object>();
      object.fieldNames().forEach(key -> sorted.put(key, canonical(object.getValue(key))));
      return sorted;
    }
    if (value instanceof JsonArray array) {
      var ordered = new ArrayList<Object>();
      for (int i = 0; i < array.size(); i++) ordered.add(canonical(array.getValue(i)));
      return ordered;
    }
    return value;
  }

  private static JsonObject readManifest(Path folder) {
    try {
      var path = folder.resolve("manifest.json");
      if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS) || Files.size(path) > ExamplesCatalog.MAX_CATALOG_BYTES)
        return null;
      return new JsonObject(Files.readString(path));
    } catch (IOException | RuntimeException invalid) {
      return null;
    }
  }

  static JsonObject matchingManifest(Path folder, JsonObject example) {
    var manifest = readManifest(folder);
    try {
      return manifest != null && fingerprint(example).equals(manifest.getString("catalogFingerprint"))
        && example.getString("id").equals(manifest.getString("exampleId")) ? manifest : null;
    } catch (RuntimeException invalid) {
      return null;
    }
  }

  /** Recipe/attribution changes need not refetch unchanged source files. */
  static JsonObject reusableFile(Path folder, JsonObject example, JsonObject spec) {
    var manifest = readManifest(folder);
    try {
      if (manifest == null || !example.getString("id").equals(manifest.getString("exampleId"))) return null;
      var previous = manifest.getJsonObject("catalogExample");
      if (previous == null || !fingerprint(previous).equals(manifest.getString("catalogFingerprint"))) return null;
      for (var item : previous.getJsonArray("files")) {
        var old = (JsonObject) item;
        if (Json.encode(canonical(old)).equals(Json.encode(canonical(spec))))
          return recordedFile(manifest, spec.getString("filename"));
      }
    } catch (RuntimeException invalid) {
      return null;
    }
    return null;
  }

  static JsonObject recordedFile(JsonObject manifest, String name) {
    if (manifest == null) return null;
    try {
      for (var item : manifest.getJsonArray("files", new JsonArray())) {
        var file = (JsonObject) item;
        if (name.equals(file.getString("filename")))
          return file.getString("sha256", "").matches("[0-9a-f]{64}") ? file : null;
      }
    } catch (RuntimeException invalid) {
      return null;
    }
    return null;
  }

  public static boolean available(Path root, JsonObject example) {
    try {
      var folder = root.resolve("examples").resolve(example.getString("id"));
      if (!Files.isDirectory(folder, LinkOption.NOFOLLOW_LINKS)) return false;
      var manifest = matchingManifest(folder, example);
      if (manifest == null) return false;
      for (var item : example.getJsonArray("files")) {
        var spec = (JsonObject) item;
        var record = recordedFile(manifest, spec.getString("filename"));
        if (record == null && !spec.getBoolean("required", true)) continue;
        if (!matches(folder, spec, record)) return false;
      }
      var preparation = example.getJsonObject("openingRecipe").getJsonObject("fastaPreparation");
      return preparation == null || matches(folder, preparation,
        recordedFile(manifest, preparation.getString("filename")));
    } catch (IOException | RuntimeException invalid) {
      return false;
    }
  }

  /** Names verified by cache metadata, for omitting unavailable optional recipe files. */
  public static JsonArray readyFiles(Path root, JsonObject example) {
    try {
      if (root == null || !available(root, example)) return null;
      var manifest = matchingManifest(root.resolve("examples").resolve(example.getString("id")), example);
      if (manifest == null) return null;
      var names = new JsonArray();
      for (var file : manifest.getJsonArray("files")) names.add(((JsonObject) file).getString("filename"));
      return names;
    } catch (RuntimeException changed) {
      return null;
    }
  }

  private static boolean matches(Path folder, JsonObject spec, JsonObject record) throws IOException {
    if (record == null) return false;
    var file = folder.resolve(spec.getString("filename"));
    if (!file.normalize().getParent().equals(folder.normalize())
      || !Files.isRegularFile(file, LinkOption.NOFOLLOW_LINKS)) return false;
    long bytes = Files.size(file);
    var hash = record.getString("sha256", "");
    return bytes == record.getLong("bytes", -1L)
      && (spec.getLong("bytes", 0L) <= 0 || bytes == spec.getLong("bytes"))
      && Files.getLastModifiedTime(file).toString().equals(record.getString("modifiedAt"))
      && hash.matches("[0-9a-f]{64}")
      && (spec.getString("sha256") == null || hash.equals(spec.getString("sha256")));
  }
}
