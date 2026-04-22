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
  private static final String CACHE_VERSION = "1";

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
    if (!(ConversionDirection.isCoolerFilename(lowered) || ConversionDirection.isHicFilename(lowered))) {
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
    final boolean outputCurrent;
    final FileFingerprint currentOutputFingerprint;
    if (outputExists) {
      currentOutputFingerprint = this.fingerprintService.fingerprint(outputPath);
      outputCurrent = metadata.outputFingerprint() == null || metadata.outputFingerprint().matches(currentOutputFingerprint);
    } else {
      currentOutputFingerprint = null;
      outputCurrent = false;
    }

    if (sourceCurrent && outputCurrent) {
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
    final var absoluteSource = sourcePath.normalize().toAbsolutePath();
    final var absoluteOutput = outputPath.normalize().toAbsolutePath();
    if (!absoluteSource.startsWith(this.dataDirectory) || !absoluteOutput.startsWith(this.dataDirectory)) {
      return;
    }
    final var metadata = new MatrixConversionMetadata(
      CACHE_VERSION,
      relativizeToDataDirectory(absoluteSource),
      relativizeToDataDirectory(absoluteOutput),
      direction.wireName(),
      this.fingerprintService.fingerprint(absoluteSource),
      this.fingerprintService.fingerprint(absoluteOutput),
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
    return "UNKNOWN";
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
                                          long createdAtMs) {
    private @NotNull JsonObject toJson() {
      return new JsonObject()
        .put("version", this.version)
        .put("sourceFilename", this.sourceFilename)
        .put("outputFilename", this.outputFilename)
        .put("conversionDirection", this.conversionDirection)
        .put("sourceFingerprint", this.sourceFingerprint.toJson())
        .put("outputFingerprint", this.outputFingerprint != null ? this.outputFingerprint.toJson() : null)
        .put("createdAtMs", this.createdAtMs);
    }

    private static @NotNull MatrixConversionMetadata fromJson(final @NotNull JsonObject json) {
      return new MatrixConversionMetadata(
        json.getString("version", ""),
        json.getString("sourceFilename", ""),
        json.getString("outputFilename", ""),
        json.getString("conversionDirection", ""),
        FileFingerprint.fromJson(json.getJsonObject("sourceFingerprint", new JsonObject())),
        json.getJsonObject("outputFingerprint") != null
          ? FileFingerprint.fromJson(json.getJsonObject("outputFingerprint"))
          : null,
        json.getLong("createdAtMs", 0L)
      );
    }
  }
}
