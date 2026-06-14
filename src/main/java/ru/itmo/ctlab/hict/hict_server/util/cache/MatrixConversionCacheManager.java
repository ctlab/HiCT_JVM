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

package ru.itmo.ctlab.hict.hict_server.util.cache;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.jetbrains.annotations.NotNull;
import ru.itmo.ctlab.hict.hict_server.handlers.conversion.ConversionDirection;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;

public final class MatrixConversionCacheManager {
  private static final String CACHE_VERSION = "2";

  private final @NotNull Path dataDirectory;
  private final @NotNull Path processedDirectory;
  private final @NotNull FileFingerprintService fingerprintService;

  public MatrixConversionCacheManager(final @NotNull Path dataDirectory,
                                      final @NotNull Path processedDirectory,
                                      final @NotNull FileFingerprintService fingerprintService) {
    this.dataDirectory = dataDirectory.normalize().toAbsolutePath();
    this.processedDirectory = processedDirectory.normalize().toAbsolutePath();
    this.fingerprintService = fingerprintService;
  }

  public @NotNull MatrixOpenResolution resolveOpenPath(final @NotNull String relativeFilename) {
    final var sourcePath = this.dataDirectory.resolve(relativeFilename).normalize().toAbsolutePath();
    if (!sourcePath.startsWith(this.dataDirectory)) {
      throw new IllegalArgumentException("Invalid filename");
    }
    if (!Files.exists(sourcePath) || !Files.isRegularFile(sourcePath)) {
      throw new IllegalArgumentException("Source file not found: " + relativeFilename);
    }

    final var warnings = new ArrayList<String>();
    final var lowered = relativeFilename.toLowerCase(Locale.ROOT);
    if (ConversionDirection.isHictFilename(lowered)) {
      return new MatrixOpenResolution(
        relativeFilename,
        kindForFilename(relativeFilename),
        MatrixOpenAction.OPEN_DIRECT,
        relativeFilename,
        null,
        null,
        false,
        true,
        warnings,
        null,
        null
      );
    }
    if (!(ConversionDirection.isCoolerFilename(lowered) || ConversionDirection.isHicFilename(lowered) || ConversionDirection.isHictkLoadFilename(lowered))) {
      warnings.add("Unsupported matrix source type for cache resolution.");
      return new MatrixOpenResolution(
        relativeFilename,
        kindForFilename(relativeFilename),
        MatrixOpenAction.UNSUPPORTED,
        relativeFilename,
        null,
        null,
        false,
        false,
        warnings,
        null,
        null
      );
    }

    final var direction = ConversionDirection.defaultForSource(sourcePath);
    final var outputPath = direction.deriveOutputPath(sourcePath);
    final var outputRelative = relativizeToDataDirectory(outputPath);
    final var metadataPath = metadataPath(sourcePath, direction);
    final var outputExists = Files.exists(outputPath) && Files.isRegularFile(outputPath);
    if (!outputExists) {
      warnings.add("Converted HiCT output is missing and conversion is required.");
      return new MatrixOpenResolution(
        relativeFilename,
        kindForFilename(relativeFilename),
        MatrixOpenAction.CONVERSION_REQUIRED,
        outputRelative,
        outputRelative,
        direction.wireName(),
        false,
        false,
        warnings,
        null,
        null
      );
    }

    if (!Files.exists(metadataPath) || !Files.isRegularFile(metadataPath)) {
      warnings.add("Converted output exists, but cache metadata is missing. Conversion should be repeated to validate freshness.");
      return new MatrixOpenResolution(
        relativeFilename,
        kindForFilename(relativeFilename),
        MatrixOpenAction.CONVERSION_REQUIRED,
        outputRelative,
        outputRelative,
        direction.wireName(),
        true,
        false,
        warnings,
        null,
        null
      );
    }

    final var metadata = readMetadata(metadataPath);
    final var currentSourceFingerprint = this.fingerprintService.fingerprint(sourcePath);
    final var sourceCurrent = metadata.sourceFingerprint().matches(currentSourceFingerprint);
    final var currentDependencyFingerprints = fingerprintDependencies(sourcePath, direction);
    final var dependenciesCurrent = metadata.dependenciesMatch(currentDependencyFingerprints);
    final boolean outputCurrent;
    final FileFingerprint currentOutputFingerprint;
    if (outputExists) {
      currentOutputFingerprint = this.fingerprintService.fingerprint(outputPath);
      outputCurrent = metadata.outputFingerprint() == null || metadata.outputFingerprint().matches(currentOutputFingerprint);
    } else {
      currentOutputFingerprint = null;
      outputCurrent = false;
    }

    if (sourceCurrent && dependenciesCurrent && outputCurrent) {
      return new MatrixOpenResolution(
        relativeFilename,
        kindForFilename(relativeFilename),
        MatrixOpenAction.REUSE_CONVERTED,
        outputRelative,
        outputRelative,
        direction.wireName(),
        true,
        true,
        warnings,
        currentSourceFingerprint,
        currentOutputFingerprint
      );
    }

    if (!sourceCurrent) {
      warnings.add("Source file changed since the last conversion. A fresh conversion is required.");
    }
    if (!dependenciesCurrent) {
      warnings.add("One or more conversion sidecar files changed or were added/removed. A fresh conversion is required.");
    }
    if (!outputCurrent) {
      warnings.add("Converted output changed since the cache metadata was written. Re-convert to ensure consistency.");
    }
    return new MatrixOpenResolution(
      relativeFilename,
      kindForFilename(relativeFilename),
      MatrixOpenAction.CONVERSION_REQUIRED,
      outputRelative,
      outputRelative,
      direction.wireName(),
      true,
      false,
      warnings,
      currentSourceFingerprint,
      currentOutputFingerprint
    );
  }

  public void recordSuccessfulConversion(final @NotNull Path sourcePath,
                                         final @NotNull Path outputPath,
                                         final @NotNull ConversionDirection direction) {
    recordSuccessfulConversion(sourcePath, outputPath, direction, List.of());
  }

  public void recordSuccessfulConversion(final @NotNull Path sourcePath,
                                         final @NotNull Path outputPath,
                                         final @NotNull ConversionDirection direction,
                                         final @NotNull List<Path> dependencyPaths) {
    final var absoluteSource = sourcePath.normalize().toAbsolutePath();
    final var absoluteOutput = outputPath.normalize().toAbsolutePath();
    if (!absoluteSource.startsWith(this.dataDirectory) || !absoluteOutput.startsWith(this.dataDirectory)) {
      return;
    }
    final var dependencies = fingerprintDependencies(absoluteSource, direction, dependencyPaths);
    final var metadata = new MatrixConversionMetadata(
      CACHE_VERSION,
      relativizeToDataDirectory(absoluteSource),
      relativizeToDataDirectory(absoluteOutput),
      direction.wireName(),
      this.fingerprintService.fingerprint(absoluteSource),
      this.fingerprintService.fingerprint(absoluteOutput),
      dependencies,
      System.currentTimeMillis()
    );
    final var metadataPath = metadataPath(absoluteSource, direction);
    try {
      Files.createDirectories(metadataPath.getParent());
      Files.writeString(metadataPath, metadata.toJson().encodePrettily(), StandardCharsets.UTF_8);
    } catch (final IOException e) {
      throw new RuntimeException("Failed to persist matrix conversion cache metadata", e);
    }
  }

  public int dropAllMetadata() {
    return deleteRecursively(this.processedDirectory.resolve("matrix_conversion_cache"));
  }

  public @NotNull Path metadataDirectory() {
    return this.processedDirectory.resolve("matrix_conversion_cache");
  }

  private @NotNull Path metadataPath(final @NotNull Path sourcePath,
                                     final @NotNull ConversionDirection direction) {
    return metadataDirectory().resolve(sha256Hex(
      CACHE_VERSION + "|" + sourcePath.normalize().toAbsolutePath() + "|" + direction.wireName()
    ) + ".json");
  }

  private @NotNull MatrixConversionMetadata readMetadata(final @NotNull Path metadataPath) {
    try {
      return MatrixConversionMetadata.fromJson(new JsonObject(Files.readString(metadataPath, StandardCharsets.UTF_8)));
    } catch (final IOException e) {
      throw new RuntimeException("Failed to read conversion cache metadata " + metadataPath, e);
    }
  }

  private @NotNull String relativizeToDataDirectory(final @NotNull Path path) {
    if (path.startsWith(this.dataDirectory)) {
      return this.dataDirectory.relativize(path).toString();
    }
    return path.toString();
  }

  private static @NotNull String kindForFilename(final @NotNull String filename) {
    if (ConversionDirection.isHictFilename(filename)) {
      return "HICT";
    }
    if (ConversionDirection.isCoolerFilename(filename)) {
      return filename.toLowerCase(Locale.ROOT).endsWith(".mcool") ? "MCOOL" : "COOL";
    }
    if (ConversionDirection.isHicFilename(filename)) {
      return "HIC";
    }
    if (ConversionDirection.isHicProMatrixFilename(filename)) {
      return "HIC_PRO_MATRIX";
    }
    if (ConversionDirection.isPairsFilename(filename)) {
      return "PAIRS";
    }
    if (ConversionDirection.isValidPairsFilename(filename)) {
      return "VALID_PAIRS";
    }
    if (ConversionDirection.isBg2Filename(filename)) {
      return "BEDPE_BG2";
    }
    if (ConversionDirection.isCooFilename(filename)) {
      return "COO";
    }
    return "UNKNOWN";
  }

  private @NotNull List<DependencyFingerprint> fingerprintDependencies(final @NotNull Path sourcePath,
                                                                       final @NotNull ConversionDirection direction) {
    return fingerprintDependencies(sourcePath, direction, List.of());
  }

  private @NotNull List<DependencyFingerprint> fingerprintDependencies(final @NotNull Path sourcePath,
                                                                       final @NotNull ConversionDirection direction,
                                                                       final @NotNull List<Path> explicitDependencies) {
    final var dependencies = new ArrayList<Path>();
    explicitDependencies.stream()
      .filter(path -> path != null && Files.isRegularFile(path))
      .map(path -> path.normalize().toAbsolutePath())
      .forEach(dependencies::add);
    discoverDefaultDependencies(sourcePath, direction).forEach(path -> {
      final var absolute = path.normalize().toAbsolutePath();
      if (!dependencies.contains(absolute)) {
        dependencies.add(absolute);
      }
    });
    return dependencies.stream()
      .filter(path -> path.startsWith(this.dataDirectory) && Files.isRegularFile(path))
      .map(path -> new DependencyFingerprint(relativizeToDataDirectory(path), this.fingerprintService.fingerprint(path)))
      .sorted(java.util.Comparator.comparing(DependencyFingerprint::filename))
      .toList();
  }

  private static @NotNull List<Path> discoverDefaultDependencies(final @NotNull Path sourcePath,
                                                                 final @NotNull ConversionDirection direction) {
    if (direction == ConversionDirection.HICPRO_MATRIX_TO_HICT || direction == ConversionDirection.COO_TO_HICT) {
      final var candidate = discoverSibling(sourcePath, List.of(".bed", ".bins.bed", ".bin_table.bed"));
      return candidate == null ? List.of() : List.of(candidate);
    }
    if (direction == ConversionDirection.BG2_TO_HICT || direction == ConversionDirection.VALIDPAIRS_TO_HICT) {
      final var candidate = discoverSibling(sourcePath, List.of(".chrom.sizes", ".chromsizes", ".chrom_sizes.txt"));
      return candidate == null ? List.of() : List.of(candidate);
    }
    return List.of();
  }

  private static Path discoverSibling(final @NotNull Path inputPath,
                                      final @NotNull List<String> suffixes) {
    final var parent = inputPath.getParent();
    if (parent == null) {
      return null;
    }
    final var filename = inputPath.getFileName().toString();
    final var decompressed = ConversionDirection.stripCompressionSuffix(filename);
    final var dot = decompressed.lastIndexOf('.');
    final var stem = dot > 0 ? decompressed.substring(0, dot) : decompressed;
    for (final var suffix : suffixes) {
      final var candidate = parent.resolve(stem + suffix);
      if (Files.isRegularFile(candidate)) {
        return candidate;
      }
    }
    for (final var suffix : suffixes) {
      final var candidate = parent.resolve(suffix.startsWith(".") ? suffix.substring(1) : suffix);
      if (Files.isRegularFile(candidate)) {
        return candidate;
      }
    }
    return null;
  }

  private static int deleteRecursively(final @NotNull Path root) {
    if (!Files.exists(root)) {
      return 0;
    }
    final var deletedCount = new int[]{0};
    try (final var stream = Files.walk(root)) {
      stream.sorted(java.util.Comparator.reverseOrder()).forEach(path -> {
        try {
          Files.deleteIfExists(path);
          deletedCount[0]++;
        } catch (final IOException e) {
          throw new RuntimeException("Failed to delete cache entry " + path, e);
        }
      });
      return deletedCount[0];
    } catch (final IOException e) {
      throw new RuntimeException("Failed to drop cache directory " + root, e);
    }
  }

  private static @NotNull String sha256Hex(final @NotNull String input) {
    try {
      final var digest = MessageDigest.getInstance("SHA-256")
        .digest(input.getBytes(StandardCharsets.UTF_8));
      return HexFormat.of().formatHex(digest);
    } catch (final NoSuchAlgorithmException e) {
      throw new RuntimeException("SHA-256 is not available", e);
    }
  }

  public enum MatrixOpenAction {
    OPEN_DIRECT,
    REUSE_CONVERTED,
    CONVERSION_REQUIRED,
    UNSUPPORTED
  }

  public record MatrixOpenResolution(@NotNull String inputFilename,
                                     @NotNull String inputKind,
                                     @NotNull MatrixOpenAction action,
                                     @NotNull String resolvedFilename,
                                     String expectedOutputFilename,
                                     String conversionDirection,
                                     boolean cachedOutputExists,
                                     boolean cacheCurrent,
                                     @NotNull List<String> warnings,
                                     FileFingerprint sourceFingerprint,
                                     FileFingerprint outputFingerprint) {
    public @NotNull JsonObject toJson() {
      return new JsonObject()
        .put("inputFilename", this.inputFilename)
        .put("inputKind", this.inputKind)
        .put("action", this.action.name())
        .put("resolvedFilename", this.resolvedFilename)
        .put("expectedOutputFilename", this.expectedOutputFilename)
        .put("conversionDirection", this.conversionDirection)
        .put("cachedOutputExists", this.cachedOutputExists)
        .put("cacheCurrent", this.cacheCurrent)
        .put("warnings", new JsonArray(this.warnings))
        .put("sourceFingerprint", this.sourceFingerprint != null ? this.sourceFingerprint.toJson() : null)
        .put("outputFingerprint", this.outputFingerprint != null ? this.outputFingerprint.toJson() : null);
    }
  }

  private record MatrixConversionMetadata(@NotNull String version,
                                          @NotNull String sourceFilename,
                                          @NotNull String outputFilename,
                                          @NotNull String conversionDirection,
                                          @NotNull FileFingerprint sourceFingerprint,
                                          FileFingerprint outputFingerprint,
                                          @NotNull List<DependencyFingerprint> dependencies,
                                          long createdAtMs) {
    private @NotNull JsonObject toJson() {
      return new JsonObject()
        .put("version", this.version)
        .put("sourceFilename", this.sourceFilename)
        .put("outputFilename", this.outputFilename)
        .put("conversionDirection", this.conversionDirection)
        .put("sourceFingerprint", this.sourceFingerprint.toJson())
        .put("outputFingerprint", this.outputFingerprint != null ? this.outputFingerprint.toJson() : null)
        .put("dependencies", new JsonArray(this.dependencies.stream().map(DependencyFingerprint::toJson).toList()))
        .put("createdAtMs", this.createdAtMs);
    }

    private boolean dependenciesMatch(final @NotNull List<DependencyFingerprint> currentDependencies) {
      if (this.dependencies.size() != currentDependencies.size()) {
        return false;
      }
      for (int i = 0; i < this.dependencies.size(); i++) {
        final var expected = this.dependencies.get(i);
        final var current = currentDependencies.get(i);
        if (!expected.filename().equals(current.filename()) || !expected.fingerprint().matches(current.fingerprint())) {
          return false;
        }
      }
      return true;
    }

    private static @NotNull MatrixConversionMetadata fromJson(final @NotNull JsonObject json) {
      final var dependenciesJson = json.getJsonArray("dependencies", new JsonArray());
      final var dependencies = new ArrayList<DependencyFingerprint>(dependenciesJson.size());
      for (int i = 0; i < dependenciesJson.size(); i++) {
        dependencies.add(DependencyFingerprint.fromJson(dependenciesJson.getJsonObject(i)));
      }
      return new MatrixConversionMetadata(
        json.getString("version", ""),
        json.getString("sourceFilename", ""),
        json.getString("outputFilename", ""),
        json.getString("conversionDirection", ""),
        FileFingerprint.fromJson(json.getJsonObject("sourceFingerprint", new JsonObject())),
        json.getJsonObject("outputFingerprint") != null
          ? FileFingerprint.fromJson(json.getJsonObject("outputFingerprint"))
          : null,
        dependencies,
        json.getLong("createdAtMs", 0L)
      );
    }
  }

  private record DependencyFingerprint(@NotNull String filename,
                                       @NotNull FileFingerprint fingerprint) {
    private @NotNull JsonObject toJson() {
      return new JsonObject()
        .put("filename", this.filename)
        .put("fingerprint", this.fingerprint.toJson());
    }

    private static @NotNull DependencyFingerprint fromJson(final @NotNull JsonObject json) {
      return new DependencyFingerprint(
        json.getString("filename", ""),
        FileFingerprint.fromJson(json.getJsonObject("fingerprint", new JsonObject()))
      );
    }
  }
}
