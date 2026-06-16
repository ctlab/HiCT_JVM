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
  @NotNull ExportMode exportMode
) {
  public static final String NO_AGP = "";

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
