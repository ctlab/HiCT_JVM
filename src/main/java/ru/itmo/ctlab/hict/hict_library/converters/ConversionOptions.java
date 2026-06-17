package ru.itmo.ctlab.hict.hict_library.converters;

import org.jetbrains.annotations.NotNull;

import java.nio.file.Path;
import java.util.List;

public record ConversionOptions(
  @NotNull Path inputPath,
  @NotNull Path outputPath,
  @NotNull List<@NotNull Long> resolutions,
  int chunkSize,
  int compressionLevel,
  @NotNull CompressionAlgorithm compressionAlgorithm,
  @NotNull String agpPath,
  boolean applyAgpBeforeExport,
  int parallelism,
  boolean exportAllResolutions,
  boolean buildResolutionPyramid,
  @NotNull ExportMode exportMode
) {
  public static final String NO_AGP = "";

  public ConversionOptions(
    final @NotNull Path inputPath,
    final @NotNull Path outputPath,
    final @NotNull List<@NotNull Long> resolutions,
    final int chunkSize,
    final int compressionLevel,
    final @NotNull CompressionAlgorithm compressionAlgorithm,
    final @NotNull String agpPath,
    final boolean applyAgpBeforeExport,
    final int parallelism,
    final boolean exportAllResolutions,
    final @NotNull ExportMode exportMode
  ) {
    this(
      inputPath,
      outputPath,
      resolutions,
      chunkSize,
      compressionLevel,
      compressionAlgorithm,
      agpPath,
      applyAgpBeforeExport,
      parallelism,
      exportAllResolutions,
      defaultBuildResolutionPyramid(),
      exportMode
    );
  }

  public ConversionOptions {
    if (chunkSize <= 0) {
      chunkSize = 8_192;
    }
    if (compressionLevel < 0 || compressionLevel > 9) {
      compressionLevel = 0;
    }
    if (compressionAlgorithm == null) {
      compressionAlgorithm = CompressionAlgorithm.DEFLATE;
    }
    if (agpPath == null) {
      agpPath = NO_AGP;
    }
    if (parallelism <= 0) {
      parallelism = Math.max(1, Runtime.getRuntime().availableProcessors());
    }
    if (exportMode == null) {
      exportMode = ExportMode.AUTO;
    }
  }

  public static boolean defaultBuildResolutionPyramid() {
    final var value = firstNonBlank(
      System.getProperty("hict.buildResolutionPyramid"),
      System.getenv("HICT_BUILD_RESOLUTION_PYRAMID"),
      System.getenv("HICT_IMPORT_BUILD_RESOLUTION_PYRAMID")
    );
    return value == null || isTruthy(value);
  }

  private static boolean isTruthy(final @NotNull String value) {
    final var normalized = value.trim().toLowerCase();
    return normalized.equals("1")
      || normalized.equals("true")
      || normalized.equals("yes")
      || normalized.equals("y")
      || normalized.equals("on");
  }

  private static String firstNonBlank(final String... values) {
    for (final var value : values) {
      if (value != null && !value.isBlank()) {
        return value;
      }
    }
    return null;
  }

  public enum CompressionAlgorithm {
    DEFLATE,
    ZSTD,
    LZF;

    public static @NotNull CompressionAlgorithm parse(final @NotNull String value) {
      final var normalized = value.trim().toUpperCase();
      return switch (normalized) {
        case "DEFLATE" -> DEFLATE;
        case "ZSTD" -> ZSTD;
        case "LZF" -> LZF;
        default -> throw new IllegalArgumentException("Unknown compression algorithm: " + value + " (expected: deflate|zstd|lzf)");
      };
    }
  }

  public enum ExportMode {
    AUTO,
    INTERNAL,
    HICTK;

    public static @NotNull ExportMode parse(final @NotNull String value) {
      final var normalized = value.trim().toUpperCase();
      return switch (normalized) {
        case "AUTO" -> AUTO;
        case "INTERNAL", "DIRECT" -> INTERNAL;
        case "HICTK", "TOOLCHAIN" -> HICTK;
        default -> throw new IllegalArgumentException("Unknown export mode: " + value + " (expected: auto|internal|hictk)");
      };
    }
  }
}
