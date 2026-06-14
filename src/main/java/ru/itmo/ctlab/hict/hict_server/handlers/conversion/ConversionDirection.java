package ru.itmo.ctlab.hict.hict_server.handlers.conversion;

import org.jetbrains.annotations.NotNull;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

public enum ConversionDirection {
  HICT_TO_MCOOL("hict-to-mcool", ".mcool"),
  MCOOL_TO_HICT("mcool-to-hict", ".hict.hdf5"),
  HIC_TO_MCOOL("hic-to-mcool", ".mcool"),
  HIC_TO_HICT("hic-to-hict", ".hict.hdf5"),
  HICPRO_MATRIX_TO_HICT("hicpro-matrix-to-hict", ".hict.hdf5"),
  COO_TO_HICT("coo-to-hict", ".hict.hdf5"),
  BG2_TO_HICT("bg2-to-hict", ".hict.hdf5"),
  PAIRS_TO_HICT("pairs-to-hict", ".hict.hdf5"),
  VALIDPAIRS_TO_HICT("validpairs-to-hict", ".hict.hdf5");

  private final @NotNull String wireName;
  private final @NotNull String outputExtension;

  ConversionDirection(final @NotNull String wireName,
                      final @NotNull String outputExtension) {
    this.wireName = wireName;
    this.outputExtension = outputExtension;
  }

  public @NotNull String wireName() {
    return this.wireName;
  }

  public @NotNull String outputExtension() {
    return this.outputExtension;
  }

  public boolean requiresExternalHicToolchain() {
    return this == HIC_TO_MCOOL || this == HIC_TO_HICT || requiresHictkLoadToolchain();
  }

  public boolean requiresHictkLoadToolchain() {
    return switch (this) {
      case HICPRO_MATRIX_TO_HICT, COO_TO_HICT, BG2_TO_HICT, PAIRS_TO_HICT, VALIDPAIRS_TO_HICT -> true;
      default -> false;
    };
  }

  public boolean acceptsSource(final @NotNull Path sourcePath) {
    return switch (this) {
      case HICT_TO_MCOOL -> isHictFilename(sourcePath);
      case MCOOL_TO_HICT -> isCoolerFilename(sourcePath);
      case HIC_TO_MCOOL, HIC_TO_HICT -> isHicFilename(sourcePath);
      case HICPRO_MATRIX_TO_HICT -> isHicProMatrixFilename(sourcePath);
      case COO_TO_HICT -> isCooFilename(sourcePath);
      case BG2_TO_HICT -> isBg2Filename(sourcePath);
      case PAIRS_TO_HICT -> isPairsFilename(sourcePath);
      case VALIDPAIRS_TO_HICT -> isValidPairsFilename(sourcePath);
    };
  }

  public @NotNull Path deriveOutputPath(final @NotNull Path sourcePath) {
    return sourcePath.getParent().resolve(stripKnownSuffix(sourcePath.getFileName().toString()) + this.outputExtension);
  }

  public static @NotNull ConversionDirection fromRequestOrSource(final String requestedDirection,
                                                                  final @NotNull Path sourcePath) {
    final var direction = requestedDirection == null || requestedDirection.isBlank()
      ? defaultForSource(sourcePath)
      : Arrays.stream(values())
      .filter(value -> value.wireName.equalsIgnoreCase(requestedDirection.trim()))
      .findFirst()
      .orElseThrow(() -> new IllegalArgumentException("Unknown conversion direction: " + requestedDirection));
    if (!direction.acceptsSource(sourcePath)) {
      throw new IllegalArgumentException(
        "Conversion direction " + direction.wireName + " is not valid for input " + sourcePath.getFileName()
      );
    }
    return direction;
  }

  public static @NotNull ConversionDirection defaultForSource(final @NotNull Path sourcePath) {
    if (isHicFilename(sourcePath)) {
      return HIC_TO_HICT;
    }
    if (isCoolerFilename(sourcePath)) {
      return MCOOL_TO_HICT;
    }
    if (isHictFilename(sourcePath)) {
      return HICT_TO_MCOOL;
    }
    if (isHicProMatrixFilename(sourcePath)) {
      return HICPRO_MATRIX_TO_HICT;
    }
    if (isPairsFilename(sourcePath)) {
      return PAIRS_TO_HICT;
    }
    if (isValidPairsFilename(sourcePath)) {
      return VALIDPAIRS_TO_HICT;
    }
    if (isBg2Filename(sourcePath)) {
      return BG2_TO_HICT;
    }
    if (isCooFilename(sourcePath)) {
      return COO_TO_HICT;
    }
    throw new IllegalArgumentException("Unsupported input format: " + sourcePath.getFileName());
  }

  public static boolean isHicFilename(final @NotNull Path path) {
    return isHicFilename(path.getFileName().toString());
  }

  public static boolean isHictFilename(final @NotNull Path path) {
    return isHictFilename(path.getFileName().toString());
  }

  public static boolean isCoolerFilename(final @NotNull Path path) {
    return isCoolerFilename(path.getFileName().toString());
  }

  public static boolean isHictkLoadFilename(final @NotNull String filename) {
    return isHicProMatrixFilename(filename)
      || isCooFilename(filename)
      || isBg2Filename(filename)
      || isPairsFilename(filename)
      || isValidPairsFilename(filename);
  }

  public static boolean isHictkLoadFilename(final @NotNull Path path) {
    return isHictkLoadFilename(path.getFileName().toString());
  }

  public static boolean isHicFilename(final @NotNull String filename) {
    return filename.toLowerCase(Locale.ROOT).endsWith(".hic");
  }

  public static boolean isHictFilename(final @NotNull String filename) {
    final var lowered = filename.toLowerCase(Locale.ROOT);
    return lowered.endsWith(".hict.hdf5") || lowered.endsWith(".hict");
  }

  public static boolean isCoolerFilename(final @NotNull String filename) {
    final var lowered = filename.toLowerCase(Locale.ROOT);
    return lowered.endsWith(".cool") || lowered.endsWith(".mcool");
  }

  public static boolean isHicProMatrixFilename(final @NotNull Path path) {
    return isHicProMatrixFilename(path.getFileName().toString());
  }

  public static boolean isHicProMatrixFilename(final @NotNull String filename) {
    final var lowered = stripCompressionSuffix(filename.toLowerCase(Locale.ROOT));
    return lowered.endsWith(".matrix");
  }

  public static boolean isCooFilename(final @NotNull Path path) {
    return isCooFilename(path.getFileName().toString());
  }

  public static boolean isCooFilename(final @NotNull String filename) {
    final var lowered = stripCompressionSuffix(filename.toLowerCase(Locale.ROOT));
    return lowered.endsWith(".coo") || lowered.endsWith(".coo.tsv") || lowered.endsWith(".coo.csv")
      || lowered.endsWith(".tsv") || lowered.endsWith(".csv");
  }

  public static boolean isBg2Filename(final @NotNull Path path) {
    return isBg2Filename(path.getFileName().toString());
  }

  public static boolean isBg2Filename(final @NotNull String filename) {
    final var lowered = stripCompressionSuffix(filename.toLowerCase(Locale.ROOT));
    return lowered.endsWith(".bg2") || lowered.endsWith(".bedgraph2") || lowered.endsWith(".bedpe");
  }

  public static boolean isPairsFilename(final @NotNull Path path) {
    return isPairsFilename(path.getFileName().toString());
  }

  public static boolean isPairsFilename(final @NotNull String filename) {
    final var lowered = stripCompressionSuffix(filename.toLowerCase(Locale.ROOT));
    return lowered.endsWith(".pairs");
  }

  public static boolean isValidPairsFilename(final @NotNull Path path) {
    return isValidPairsFilename(path.getFileName().toString());
  }

  public static boolean isValidPairsFilename(final @NotNull String filename) {
    final var lowered = stripCompressionSuffix(filename.toLowerCase(Locale.ROOT));
    return lowered.endsWith(".validpairs");
  }

  public static @NotNull String stripCompressionSuffix(final @NotNull String filename) {
    var out = filename;
    for (final var suffix : List.of(".gz", ".bgz", ".xz", ".zst", ".zstd", ".bz2", ".lz4", ".lzo")) {
      if (out.endsWith(suffix)) {
        out = out.substring(0, out.length() - suffix.length());
        break;
      }
    }
    return out;
  }

  private static @NotNull String stripKnownSuffix(final @NotNull String filename) {
    final var lowered = filename.toLowerCase(Locale.ROOT);
    final var decompressedLowered = stripCompressionSuffix(lowered);
    final var compressionLength = lowered.length() - decompressedLowered.length();
    final var uncompressedFilename = compressionLength > 0
      ? filename.substring(0, filename.length() - compressionLength)
      : filename;
    if (lowered.endsWith(".hict.hdf5")) {
      return filename.substring(0, filename.length() - ".hict.hdf5".length());
    }
    if (lowered.endsWith(".mcool")) {
      return filename.substring(0, filename.length() - ".mcool".length());
    }
    if (lowered.endsWith(".cool")) {
      return filename.substring(0, filename.length() - ".cool".length());
    }
    if (lowered.endsWith(".hic")) {
      return filename.substring(0, filename.length() - ".hic".length());
    }
    if (lowered.endsWith(".hict")) {
      return filename.substring(0, filename.length() - ".hict".length());
    }
    for (final var suffix : List.of(".coo.tsv", ".coo.csv", ".matrix", ".bedgraph2", ".validpairs", ".pairs", ".bedpe", ".coo", ".bg2", ".tsv", ".csv")) {
      if (decompressedLowered.endsWith(suffix)) {
        return uncompressedFilename.substring(0, uncompressedFilename.length() - suffix.length());
      }
    }
    return filename;
  }
}
