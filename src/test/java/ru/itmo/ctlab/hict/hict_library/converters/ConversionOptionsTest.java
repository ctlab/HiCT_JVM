package ru.itmo.ctlab.hict.hict_library.converters;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConversionOptionsTest {
  @Test
  void legacyConstructorKeepsResolutionPyramidEnabledByDefault() {
    final var options = new ConversionOptions(
      Path.of("input.mcool"),
      Path.of("output.hict.hdf5"),
      List.of(),
      8192,
      6,
      ConversionOptions.CompressionAlgorithm.DEFLATE,
      ConversionOptions.NO_AGP,
      false,
      1,
      false,
      ConversionOptions.ExportMode.AUTO
    );

    assertTrue(options.buildResolutionPyramid());
    assertTrue(options.balanceInputCoolers());
    assertTrue(options.balanceExportedCoolers());
  }

  @Test
  void canonicalConstructorCanDisableResolutionPyramid() {
    final var options = new ConversionOptions(
      Path.of("input.mcool"),
      Path.of("output.hict.hdf5"),
      List.of(),
      8192,
      6,
      ConversionOptions.CompressionAlgorithm.DEFLATE,
      ConversionOptions.NO_AGP,
      false,
      1,
      false,
      false,
      false,
      false,
      ConversionOptions.ExportMode.AUTO
    );

    assertFalse(options.buildResolutionPyramid());
    assertFalse(options.balanceInputCoolers());
    assertFalse(options.balanceExportedCoolers());
  }

  @Test
  void invalidCompressionLevelFailsFastInsteadOfDisablingCompression() {
    assertThrows(
      IllegalArgumentException.class,
      () -> new ConversionOptions(
        Path.of("input.mcool"),
        Path.of("output.hict.hdf5"),
        List.of(),
        8192,
        99,
        ConversionOptions.CompressionAlgorithm.DEFLATE,
        ConversionOptions.NO_AGP,
        false,
        1,
        false,
        false,
        false,
        false,
        ConversionOptions.ExportMode.AUTO
      )
    );
  }
}
