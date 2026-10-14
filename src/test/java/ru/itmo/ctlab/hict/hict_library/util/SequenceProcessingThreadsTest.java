package ru.itmo.ctlab.hict.hict_library.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SequenceProcessingThreadsTest {
  @Test
  void distinguishesAllAutoAndExplicitModes() {
    final int available = ProcessingThreads.available();
    assertEquals(new SequenceProcessingThreads.Selection(-1, available, "all-allocated"),
      SequenceProcessingThreads.resolve(-1, "HICT_TEST_SEQUENCE_THREADS", 3));
    assertEquals(new SequenceProcessingThreads.Selection(0, Math.min(available, 3), "auto"),
      SequenceProcessingThreads.resolve(0, "HICT_TEST_SEQUENCE_THREADS", 3));
    assertEquals(new SequenceProcessingThreads.Selection(2, Math.min(available, 2), "explicit"),
      SequenceProcessingThreads.resolve(2, "HICT_TEST_SEQUENCE_THREADS", 100));
  }

  @Test
  void rejectsUnsupportedNegativeValues() {
    assertThrows(IllegalArgumentException.class,
      () -> SequenceProcessingThreads.resolve(-2, "HICT_TEST_SEQUENCE_THREADS", 1));
  }

  @Test
  void reportsOnlyConcurrencyThatIndependentTasksCanUse() {
    final var requested = new SequenceProcessingThreads.Selection(-1, 24, "all-allocated");
    assertEquals(new SequenceProcessingThreads.Selection(-1, 3, "all-allocated"),
      SequenceProcessingThreads.boundToTasks(requested, 3));
    assertEquals(new SequenceProcessingThreads.Selection(-1, 1, "all-allocated"),
      SequenceProcessingThreads.boundToTasks(requested, 0));
  }
}
