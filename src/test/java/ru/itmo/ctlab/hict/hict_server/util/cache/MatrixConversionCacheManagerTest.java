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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.itmo.ctlab.hict.hict_server.handlers.conversion.ConversionDirection;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MatrixConversionCacheManagerTest {
  @TempDir
  Path tempDir;

  @Test
  void reusesConvertedOutputWhenFingerprintsMatch() throws Exception {
    final var dataDir = tempDir.resolve("data");
    final var processedDir = tempDir.resolve("processed");
    Files.createDirectories(dataDir);
    Files.createDirectories(processedDir);

    final var input = dataDir.resolve("example.mcool");
    final var output = dataDir.resolve("example.hict.hdf5");
    Files.writeString(input, "mcool-data");
    Files.writeString(output, "hict-data");

    final var manager = new MatrixConversionCacheManager(dataDir, processedDir, new FileFingerprintService());
    manager.recordSuccessfulConversion(input, output, ConversionDirection.MCOOL_TO_HICT);

    final var resolution = manager.resolveOpenPath("example.mcool");

    assertEquals(MatrixConversionCacheManager.MatrixOpenAction.REUSE_CONVERTED, resolution.action());
    assertEquals("example.hict.hdf5", resolution.resolvedFilename());
    assertTrue(resolution.cacheCurrent());
  }

  @Test
  void requiresFreshConversionWhenCachedCoolerImportWasNotPreparedWithRequestedOptions() throws Exception {
    final var dataDir = tempDir.resolve("data");
    final var processedDir = tempDir.resolve("processed");
    Files.createDirectories(dataDir);
    Files.createDirectories(processedDir);

    final var input = dataDir.resolve("single-resolution.mcool");
    final var output = dataDir.resolve("single-resolution.hict.hdf5");
    Files.writeString(input, "mcool-data");
    Files.writeString(output, "hict-data");

    final var manager = new MatrixConversionCacheManager(dataDir, processedDir, new FileFingerprintService());
    manager.recordSuccessfulConversion(input, output, ConversionDirection.MCOOL_TO_HICT);

    final var resolution = manager.resolveOpenPath("single-resolution.mcool", true, true);

    assertEquals(MatrixConversionCacheManager.MatrixOpenAction.CONVERSION_REQUIRED, resolution.action());
    assertTrue(
      resolution.warnings().stream().anyMatch(warning -> warning.toLowerCase().contains("cooler preparation"))
    );
  }

  @Test
  void reusesPreparedCoolerImportWhenRequestedOptionsAreSatisfied() throws Exception {
    final var dataDir = tempDir.resolve("data");
    final var processedDir = tempDir.resolve("processed");
    Files.createDirectories(dataDir);
    Files.createDirectories(processedDir);

    final var input = dataDir.resolve("prepared.mcool");
    final var output = dataDir.resolve("prepared.hict.hdf5");
    Files.writeString(input, "mcool-data");
    Files.writeString(output, "hict-data");

    final var manager = new MatrixConversionCacheManager(dataDir, processedDir, new FileFingerprintService());
    manager.recordSuccessfulConversion(input, output, ConversionDirection.MCOOL_TO_HICT, java.util.List.of(), true, true);

    final var resolution = manager.resolveOpenPath("prepared.mcool", true, true);

    assertEquals(MatrixConversionCacheManager.MatrixOpenAction.REUSE_CONVERTED, resolution.action());
    assertEquals("prepared.hict.hdf5", resolution.resolvedFilename());
    assertTrue(resolution.cacheCurrent());
  }

  @Test
  void requiresFreshConversionWhenSourceChanges() throws Exception {
    final var dataDir = tempDir.resolve("data");
    final var processedDir = tempDir.resolve("processed");
    Files.createDirectories(dataDir);
    Files.createDirectories(processedDir);

    final var input = dataDir.resolve("example.hic");
    final var output = dataDir.resolve("example.hict.hdf5");
    Files.writeString(input, "old-hic-data");
    Files.writeString(output, "hict-data");

    final var manager = new MatrixConversionCacheManager(dataDir, processedDir, new FileFingerprintService());
    manager.recordSuccessfulConversion(input, output, ConversionDirection.HIC_TO_HICT);

    Files.writeString(input, "new-hic-data");

    final var resolution = manager.resolveOpenPath("example.hic");

    assertEquals(MatrixConversionCacheManager.MatrixOpenAction.CONVERSION_REQUIRED, resolution.action());
    assertTrue(
      resolution.warnings().stream().anyMatch(warning -> warning.toLowerCase().contains("changed"))
    );
  }
}
