package ru.itmo.ctlab.hict.hict_library.converters;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

class FractionalContactSorterTest {
  @TempDir Path directory;

  @Test void multipassMergePreservesFractionsAndEmitsUniqueUpperTriangleInRowMajorOrder() throws Exception {
    final var random = new Random(1701);
    final var expected = new HashMap<String, Double>();
    final var actual = new HashMap<String, Double>();
    try (var sorter = new FractionalContactSorter(directory, 7, 2, () -> false)) {
      for (int i = 0; i < 1000; i++) {
        long row = random.nextInt(20), col = row + random.nextInt(20);
        final double value = (random.nextInt(31) - 15) / 16.0;
        sorter.add(row, col, value); expected.merge(row + ":" + col, value, Double::sum);
      }
      final long[] last = {-1, -1};
      sorter.finish((row, col, value) -> {
        assertTrue(row > last[0] || row == last[0] && col > last[1]);
        last[0] = row; last[1] = col;
        assertNull(actual.put(row + ":" + col, value));
      });
      assertThrows(IllegalStateException.class, () -> sorter.add(0, 0, 1));
      assertThrows(IllegalStateException.class, () -> sorter.finish((r, c, v) -> fail()));
    }
    expected.values().removeIf(value -> value == 0);
    assertEquals(expected, actual);
    assertEmptyDirectory();
  }

  @Test void cancellationAndConsumerFailureLeaveNoOwnedTemporaryFiles() throws Exception {
    final var cancel = new AtomicBoolean();
    try (var sorter = new FractionalContactSorter(directory, 2, 2, cancel::get)) {
      sorter.add(0, 0, 0.25); sorter.add(0, 1, 0.5);
      cancel.set(true);
      assertThrows(CancellationException.class, () -> sorter.finish((r, c, v) -> fail()));
    }
    assertEmptyDirectory();
    try (var sorter = new FractionalContactSorter(directory, 1, 2, () -> false)) {
      sorter.add(0, 1, 0.25);
      assertThrows(IOException.class, () -> sorter.finish((r, c, v) -> { throw new IOException("Output is full"); }));
    }
    assertEmptyDirectory();
  }

  @Test void rejectsNonfiniteAndInvalidCoordinatesAndHandlesEmptyInput() throws Exception {
    try (var sorter = new FractionalContactSorter(directory, 3, 2, () -> false)) {
      assertThrows(IllegalArgumentException.class, () -> sorter.add(1, 0, 1));
      assertThrows(IllegalArgumentException.class, () -> sorter.add(-1, 0, 1));
      assertThrows(IllegalArgumentException.class, () -> sorter.add(0, 0, Double.POSITIVE_INFINITY));
      sorter.add(0, 0, 0);
      sorter.finish((r, c, v) -> fail("Empty input has no output"));
    }
    assertEmptyDirectory();
  }

  private void assertEmptyDirectory() throws IOException {
    try (var files = Files.list(directory)) { assertEquals(0, files.count()); }
  }
}
