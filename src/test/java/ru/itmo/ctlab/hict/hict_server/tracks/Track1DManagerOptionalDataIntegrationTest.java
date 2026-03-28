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

package ru.itmo.ctlab.hict.hict_server.tracks;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.domain.QueryLengthUnit;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class Track1DManagerOptionalDataIntegrationTest {
  @TempDir
  Path tempDir;

  @Test
  void coolerWeightsTrackProducesBinsAcrossUnitsWhenOptionalDataPresent() {
    final var dataRoot = Path.of(
      System.getenv().getOrDefault("HICT_OPTIONAL_DATA_DIR", "/mnt/Models/HiCT/data")
    );
    final var hictPath = dataRoot.resolve("build/quad/combined_ind2_4DN.hict.hdf5");
    Assumptions.assumeTrue(
      Files.isRegularFile(hictPath),
      () -> "Optional integration data is not present: " + hictPath
    );

    final var manager = new Track1DManager(dataRoot, tempDir.resolve("processed"));
    final var chunkedFile = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(hictPath, 1, 4));
    try {
      final var opened = manager.openCoolerWeightsTrack("weights", "#4e79a7");
      assertEquals("COOLER_WEIGHTS", opened.getType());

      final var bpResolution = Arrays.stream(chunkedFile.getResolutions())
        .filter(value -> value > 0L)
        .findFirst()
        .orElseThrow();

      final var byPixels = manager.queryVisibleTracks(
        chunkedFile,
        0L,
        5000L,
        1024,
        bpResolution,
        QueryLengthUnit.PIXELS
      );
      assertEquals(1, byPixels.getTracks().size());
      assertEquals("COOLER_WEIGHTS", byPixels.getTracks().get(0).getType());
      assertFalse(byPixels.getTracks().get(0).getBins().isEmpty());

      final var byBins = manager.queryVisibleTracks(
        chunkedFile,
        0L,
        5000L,
        1024,
        bpResolution,
        QueryLengthUnit.BINS
      );
      assertEquals(1, byBins.getTracks().size());
      assertFalse(byBins.getTracks().get(0).getBins().isEmpty());

      final var byBp = manager.queryVisibleTracks(
        chunkedFile,
        0L,
        Math.max(bpResolution, 250_000_000L),
        1024,
        bpResolution,
        QueryLengthUnit.BASE_PAIRS
      );
      assertEquals(1, byBp.getTracks().size());
      assertFalse(byBp.getTracks().get(0).getBins().isEmpty());
      assertTrue(byBp.getTracks().get(0).getMaxValue() >= 0.0d);

      final var updated = manager.updateTrack(
        opened.getTrackId(),
        null,
        null,
        null,
        null,
        null,
        true
      );
      assertNotNull(updated);
      assertTrue(updated.isLogScale());
    } finally {
      chunkedFile.close();
      manager.close();
    }
  }
}

