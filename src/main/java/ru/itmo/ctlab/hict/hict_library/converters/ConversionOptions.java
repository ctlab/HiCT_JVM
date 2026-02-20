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
  @NotNull String agpPath,
  boolean applyAgpBeforeExport,
  int parallelism
) {
  public static final String NO_AGP = "";

  public ConversionOptions {
    if (chunkSize <= 0) {
      chunkSize = 8_192;
    }
    if (compressionLevel < 0 || compressionLevel > 9) {
      compressionLevel = 0;
    }
    if (agpPath == null) {
      agpPath = NO_AGP;
    }
    if (parallelism <= 0) {
      parallelism = Math.max(1, Runtime.getRuntime().availableProcessors());
    }
  }
}
