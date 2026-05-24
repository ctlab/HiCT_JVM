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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class Track1DManagerCoolerWeightsTest {
  @TempDir
  Path tempDir;

  @Test
  void canOpenUpdateAndRemoveCoolerWeightsTrack() {
    final var manager = new Track1DManager(tempDir, tempDir.resolve("processed"));
    try {
      final var opened = manager.openCoolerWeightsTrack(null, null);
      assertEquals("COOLER_WEIGHTS", opened.getType());
      assertEquals("Cooler weights - Primary", opened.getName());
      assertEquals("__internal__/cooler_weights/PRIMARY", opened.getSourceFile());
      assertFalse(opened.isLogScale());

      final var secondary = manager.openCoolerWeightsTrack(null, null, "SECONDARY");
      assertEquals("COOLER_WEIGHTS", secondary.getType());
      assertEquals("Cooler weights - Secondary", secondary.getName());
      assertEquals("__internal__/cooler_weights/SECONDARY", secondary.getSourceFile());

      final var updated = manager.updateTrack(
        opened.getTrackId(),
        null,
        null,
        "Weights",
        null,
        null,
        true
      );
      assertEquals("Weights", updated.getName());
      assertTrue(updated.isLogScale());

      final var listed = manager.listTracks();
      assertEquals(2, listed.size());
      assertEquals("COOLER_WEIGHTS", listed.get(0).getType());
      assertTrue(listed.get(0).isLogScale());

      manager.removeTrack(opened.getTrackId());
      manager.removeTrack(secondary.getTrackId());
      assertTrue(manager.listTracks().isEmpty());
    } finally {
      manager.close();
    }
  }
}
