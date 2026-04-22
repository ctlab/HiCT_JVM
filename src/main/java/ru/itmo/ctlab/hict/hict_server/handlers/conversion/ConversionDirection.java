package ru.itmo.ctlab.hict.hict_server.handlers.conversion;

import org.jetbrains.annotations.NotNull;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Locale;

public enum ConversionDirection {
  HICT_TO_MCOOL("hict-to-mcool", ".mcool"),
  MCOOL_TO_HICT("mcool-to-hict", ".hict.hdf5"),
  HIC_TO_MCOOL("hic-to-mcool", ".mcool"),
  HIC_TO_HICT("hic-to-hict", ".hict.hdf5");

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
    return this == HIC_TO_MCOOL || this == HIC_TO_HICT;
  }

  public boolean acceptsSource(final @NotNull Path sourcePath) {
    return switch (this) {
      case HICT_TO_MCOOL -> isHictFilename(sourcePath);
      case MCOOL_TO_HICT -> isCoolerFilename(sourcePath);
      case HIC_TO_MCOOL, HIC_TO_HICT -> isHicFilename(sourcePath);
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

  private static @NotNull String stripKnownSuffix(final @NotNull String filename) {
    final var lowered = filename.toLowerCase(Locale.ROOT);
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
    return filename;
  }
}
