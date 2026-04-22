package ru.itmo.ctlab.hict.hict_server.handlers.conversion;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ConversionDirectionTest {
  @Test
  void derivesDefaultDirectionAndOutputPathFromInputExtension() {
    assertEquals(
      ConversionDirection.HIC_TO_HICT,
      ConversionDirection.defaultForSource(Path.of("/tmp/sample.hic"))
    );
    assertEquals(
      Path.of("/tmp/sample.hict.hdf5"),
      ConversionDirection.HIC_TO_HICT.deriveOutputPath(Path.of("/tmp/sample.hic"))
    );

    assertEquals(
      ConversionDirection.MCOOL_TO_HICT,
      ConversionDirection.defaultForSource(Path.of("/tmp/sample.mcool"))
    );
    assertEquals(
      Path.of("/tmp/sample.hict.hdf5"),
      ConversionDirection.MCOOL_TO_HICT.deriveOutputPath(Path.of("/tmp/sample.mcool"))
    );

    assertEquals(
      ConversionDirection.HICT_TO_MCOOL,
      ConversionDirection.defaultForSource(Path.of("/tmp/sample.hict.hdf5"))
    );
    assertEquals(
      Path.of("/tmp/sample.mcool"),
      ConversionDirection.HICT_TO_MCOOL.deriveOutputPath(Path.of("/tmp/sample.hict.hdf5"))
    );
  }

  @Test
  void resolvesExplicitDirectionsWithValidation() {
    assertEquals(
      ConversionDirection.HIC_TO_MCOOL,
      ConversionDirection.fromRequestOrSource("hic-to-mcool", Path.of("/tmp/input.hic"))
    );
    assertEquals(
      ConversionDirection.MCOOL_TO_HICT,
      ConversionDirection.fromRequestOrSource("mcool-to-hict", Path.of("/tmp/input.cool"))
    );
  }
}
