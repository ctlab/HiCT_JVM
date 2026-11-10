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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Track files report what they skip or correct and refuse input they cannot use at all. */
class Track1DManagerValidationTest {
  @TempDir
  Path tempDir;

  private Track1DManager manager() {
    return new Track1DManager(tempDir, tempDir.resolve("processed"));
  }

  private String write(final String name, final String content) throws IOException {
    Files.writeString(tempDir.resolve(name), content);
    return name;
  }

  @Test
  void cleanFileOpensWithoutWarnings() throws IOException {
    try (var m = manager()) {
      final var summary = m.openTrack(write("clean.bed", "ctg1\t0\t10\tgeneA\nctg1\t20\t30\tgeneB\n"), null, null);
      assertEquals(2, summary.getFeatureCount());
      assertTrue(summary.getImportWarnings().isEmpty());
    }
  }

  @Test
  void unplaceableRecordsAreSkippedAndListedWithLineNumbers() throws IOException {
    final var content = String.join("\n",
      "# header",
      "ctg1\t0\t10\tok",
      "ctg1\t-5\t10\tnegative",
      "ctg1\t30\t30\tempty",
      "ctg1\t50\t40\treversed",
      "ctg1\t7",
      "ctg1\t60\t70\tlast",
      "");
    try (var m = manager()) {
      final var summary = m.openTrack(write("mixed.bed", content), null, null);
      assertEquals(2, summary.getFeatureCount());
      final var text = String.join("\n", summary.getImportWarnings());
      assertTrue(text.contains("4 of 6 records skipped"), text);
      assertTrue(text.contains("line 3 skipped: negative start"), text);
      assertTrue(text.contains("line 6 skipped: fewer than 3"), text);
    }
  }

  @Test
  void nonFiniteAndNegativeValuesAreCorrectedAndCounted() throws IOException {
    final var content = "ctg1\t0\t10\t1.5\nctg1\t10\t20\tNaN\nctg1\t20\t30\tInfinity\nctg1\t30\t40\t-2\n";
    try (var m = manager()) {
      final var summary = m.openTrack(write("values.bed", content), null, null);
      assertEquals(4, summary.getFeatureCount());
      final var text = String.join("\n", summary.getImportWarnings());
      assertTrue(text.contains("0 of 4 records skipped, 3 corrected"), text);
      assertTrue(text.contains("negative value shown as 0"), text);
    }
  }

  @Test
  void syntaxErrorsRejectTheFileWithItsLineAndKeepTheTrackList() throws IOException {
    try (var m = manager()) {
      m.openTrack(write("first.bed", "ctg1\t0\t10\tx\n"), null, null);
      final var overflow = write("overflow.bed", "ctg1\t0\t10\tx\nctg1\t99999999999999999999\t5\ty\n");
      final var error = assertThrows(IllegalArgumentException.class, () -> m.openTrack(overflow, null, null));
      assertTrue(error.getMessage().contains("line 2"), error.getMessage());
      assertEquals(1, m.listTracks().size());
    }
  }

  @Test
  void aFileWithNoUsableRecordsIsRejectedAndNothingIsAdded() throws IOException {
    try (var m = manager()) {
      final var empty = write("empty.bed", "# only a comment\n");
      final var e1 = assertThrows(IllegalArgumentException.class, () -> m.openTrack(empty, null, null));
      assertTrue(e1.getMessage().contains("no usable records"), e1.getMessage());
      final var bad = write("bad.bed", "ctg1\t-1\t5\ta\nctg1\t9\t3\tb\n");
      final var e2 = assertThrows(IllegalArgumentException.class, () -> m.openTrack(bad, null, null));
      assertTrue(e2.getMessage().contains("2 of 2 records skipped"), e2.getMessage());
      assertTrue(m.listTracks().isEmpty());
    }
  }

  @Test
  void rangeThatCannotHoldAValueIsRefusedInsteadOfBeingResetSilently() throws IOException {
    try (var m = manager()) {
      final var id = m.openTrack(write("signal.bed", "ctg1\t0\t10\t3\n"), null, null).getTrackId();
      final var ok = m.updateTrack(id, null, null, null, null, null, null, false, 2.0, 8.0);
      assertEquals(2.0, ok.getRangeMin());
      assertEquals(8.0, ok.getRangeMax());
      final var error = assertThrows(IllegalArgumentException.class,
        () -> m.updateTrack(id, null, null, null, null, null, null, false, 9.0, null));
      assertTrue(error.getMessage().contains("must be less than"), error.getMessage());
      final var kept = m.listTracks().get(0);
      assertEquals(2.0, kept.getRangeMin());
      assertEquals(8.0, kept.getRangeMax());
      assertFalse(kept.isRangeAuto());
    }
  }
}
