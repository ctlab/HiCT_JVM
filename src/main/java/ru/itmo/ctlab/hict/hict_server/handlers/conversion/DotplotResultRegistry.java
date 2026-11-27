package ru.itmo.ctlab.hict.hict_server.handlers.conversion;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.TreeMap;
import java.util.UUID;

/** Latest compatible result plus the manifest for each independently generated set. */
final class DotplotResultRegistry {
  // Claim the previous version before handing it to the adapter. A result cannot
  // be deleted between that lookup and publication of the replacement.
  static final HashSet<Path> WRITING = new HashSet<>();
  private DotplotResultRegistry() {}

  static String key(Path template, SequenceAnalysisHandlersHolder.Stamp templateStamp, String fastaIdentity,
                    String kind, JsonObject settings, String gapPolicy, String toolHash, String alignerHash)
      throws NoSuchAlgorithmException {
    var scientificSettings = new TreeMap<>(settings.getMap());
    scientificSettings.keySet().removeAll(List.of("threads", "resolutions", "analysisLevels", "overviewLevels"));
    var payload = Json.encode(List.of(template.toString(), templateStamp.toString(), fastaIdentity,
      kind, scientificSettings, gapPolicy, toolHash, alignerHash));
    return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
      .digest(payload.getBytes(StandardCharsets.UTF_8)));
  }

  record Previous(Path file, String setId, int regionCount) {}

  record Entry(Path manifest, Path file, JsonObject metadata) { }

  static List<Entry> entries(Path data, Path root, String mapFilename) throws IOException {
    if (!Files.isDirectory(root)) return List.of();
    var entries = new ArrayList<Entry>();
    try (var paths = Files.newDirectoryStream(root, "overlay-*.json")) {
      for (var path : paths) {
        if (!Files.isRegularFile(path, java.nio.file.LinkOption.NOFOLLOW_LINKS)) continue;
        var metadata = new JsonObject(Files.readString(path, StandardCharsets.UTF_8));
        if (metadata.getString("setId") == null || !mapFilename.equals(metadata.getString("mapFilename"))) continue;
        var file = data.resolve(metadata.getString("outputFile")).normalize();
        if (!Files.isRegularFile(file, java.nio.file.LinkOption.NOFOLLOW_LINKS)) continue;
        entries.add(new Entry(path, SequenceOverlayCleanup.managed(data, root, metadata.getString("outputFile")), metadata));
      }
    }
    entries.sort(java.util.Comparator.comparingLong((Entry entry) -> entry.metadata().getLong("updatedAtMs", 0L)).reversed());
    return List.copyOf(entries);
  }

  static Previous latest(Path data, Path root, String key) throws IOException {
    JsonObject index = null;
    try (var manifests = Files.newDirectoryStream(root, "overlay-" + key + "-*.json")) {
      for (var manifest : manifests) {
        if (!Files.isRegularFile(manifest, java.nio.file.LinkOption.NOFOLLOW_LINKS)) continue;
        var candidate = new JsonObject(Files.readString(manifest, StandardCharsets.UTF_8));
        var output = data.resolve(candidate.getString("outputFile")).normalize();
        if (output.startsWith(root) && Files.notExists(output, java.nio.file.LinkOption.NOFOLLOW_LINKS)) continue;
        if (index == null || candidate.getLong("updatedAtMs") > index.getLong("updatedAtMs")) index = candidate;
      }
    }
    if (index == null) return null;
    var previous = data.resolve(index.getString("outputFile")).normalize();
    if (!previous.startsWith(root) || !Files.isRegularFile(previous) ||
        !previous.toRealPath().startsWith(root.toRealPath())) {
      throw new IOException("Previous overlay is unavailable; its index was retained. Generate a separate overlay or restore the file.");
    }
    return new Previous(previous, index.getString("setId"), index.getInteger("regionCount"));
  }

  static Path manifest(Path root, String key, String setId) {
    return root.resolve("overlay-" + key + "-" + UUID.fromString(setId) + ".json");
  }

  static String name(JsonObject settings, Path aligner) {
    if (aligner == null) {
      var identity = java.math.BigDecimal.valueOf(settings.getDouble("identity")).stripTrailingZeros().toPlainString();
      return "ModDotPlot · k " + settings.getInteger("k") + " · identity " + identity + "%";
    }
    var program = aligner.getFileName().toString().toLowerCase(Locale.ROOT).replaceFirst("\\.exe$", "");
    var name = switch (program) {
      case "mm2plus-avx2" -> "mm2-plus AVX2";
      case "mm2plus-avx512" -> "mm2-plus AVX-512";
      default -> program;
    };
    var standard = settings.getInteger("k") == 15 && settings.getInteger("window") == 10
      && settings.getInteger("chainScore") == 40;
    return "Dotplot · " + name + (standard ? " default" : " custom");
  }
}
