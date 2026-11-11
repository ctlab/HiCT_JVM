package ru.itmo.ctlab.hict.hict_server.handlers.conversion;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConversionHandlersHolderTest {

  @Test
  void cleanupExpiresOnlyJobsThatFinishedMoreThanAnHourAgo() {
    long now = 10_000_000L;
    assertFalse(ConversionHandlersHolder.isExpired(0, now));
    assertFalse(ConversionHandlersHolder.isExpired(now - 3_600_000, now));
    assertTrue(ConversionHandlersHolder.isExpired(now - 3_600_001, now));
  }

  @Test
  void clientFilenamePreservesDataDirectoryRelativeParent() {
    final var dataDirectory = Path.of("/work/HiCT");
    final var convertedPath = dataDirectory.resolve("data/arabiensis.0.hict.hdf5");

    assertEquals(
      Path.of("data", "arabiensis.0.hict.hdf5").toString(),
      ConversionHandlersHolder.toClientFilename(convertedPath, dataDirectory)
    );
  }

  @Test
  void clientFilenameFallsBackToBasenameOutsideDataDirectory() {
    assertEquals(
      "hict-converter-out-1.hict.hdf5",
      ConversionHandlersHolder.toClientFilename(
        Path.of("/tmp/hict-converter-out-1.hict.hdf5"),
        Path.of("/work/HiCT")
      )
    );
  }
}
