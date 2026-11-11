package ru.itmo.ctlab.hict.hict_library.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

class ProcessingThreadsTest {
  @Test void respectsPerTaskAllocationWithoutCpuBinding() {
    assertEquals(24, ProcessingThreads.available(128, Map.of("SLURM_CPUS_PER_TASK", "24")));
  }
  @Test void respectsAffinityEvenWithLargerSlurmAllocation() {
    assertEquals(4, ProcessingThreads.available(4, Map.of("SLURM_CPUS_PER_TASK", "24")));
  }
  @Test void handlesSingleTaskNodeAllocationAndMacWithoutSlurm() {
    assertEquals(12, ProcessingThreads.available(12, Map.of()));
    assertEquals(8, ProcessingThreads.available(64, Map.of("SLURM_JOB_ID", "1", "SLURM_CPUS_ON_NODE", "8")));
  }
  @Test void doesNotAssignWholeNodeToEachTask() {
    assertEquals(1, ProcessingThreads.available(64, Map.of("SLURM_JOB_ID", "1", "SLURM_NTASKS", "4", "SLURM_CPUS_ON_NODE", "32")));
  }
  @ParameterizedTest @ValueSource(strings = {"", "-1", "0", "1", "128"})
  void acceptsAutoAndPositiveSettings(String value) {
    assertTrue(ProcessingThreads.parse(value, "test") >= -1);
  }
  @ParameterizedTest @ValueSource(strings = {"-2", "1.5", "two", "2147483648"})
  void rejectsInvalidSettings(String value) {
    assertThrows(IllegalArgumentException.class, () -> ProcessingThreads.parse(value, "test"));
  }
}
