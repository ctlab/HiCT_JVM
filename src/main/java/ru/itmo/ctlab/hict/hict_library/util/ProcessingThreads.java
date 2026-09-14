package ru.itmo.ctlab.hict.hict_library.util;

import java.util.Map;

/** CPU budget for one process, bounded by JVM affinity/container limits and Slurm allocation. */
public final class ProcessingThreads {
  private ProcessingThreads() {}

  public static int available() {
    return available(Runtime.getRuntime().availableProcessors(), System.getenv());
  }

  static int available(final int visible, final Map<String, String> environment) {
    int budget = Math.max(1, visible);
    final var perTask = environment.get("SLURM_CPUS_PER_TASK");
    if (perTask != null && !perTask.isBlank()) {
      return Math.min(budget, positive(perTask, "SLURM_CPUS_PER_TASK"));
    }
    if (environment.containsKey("SLURM_JOB_ID")) {
      // A node's total allocation is not a per-task allocation in multi-task jobs.
      final int tasks = positive(environment.getOrDefault("SLURM_NTASKS", "1"), "SLURM_NTASKS");
      final String nodeCpus = environment.get("SLURM_CPUS_ON_NODE");
      budget = tasks == 1 && nodeCpus != null ? Math.min(budget, positive(nodeCpus, "SLURM_CPUS_ON_NODE")) : 1;
    }
    return budget;
  }

  public static int configured(final String key) {
    String value = System.getProperty(key);
    if ((value == null || value.isBlank()) && key.equals("HICT_CONVERSION_THREADS")) {
      value = System.getProperty("hict.conversion.threads");
    }
    if (value == null || value.isBlank()) value = System.getenv(key);
    if ((value == null || value.isBlank()) && key.equals("HICT_CONVERSION_THREADS")) value = System.getenv("HICT_CONVERSION_PARALLELISM");
    return parse(value, key);
  }

  public static int parse(final String value, final String key) {
    if (value == null || value.isBlank()) return -1;
    try {
      final int count = Integer.parseInt(value.trim());
      if (count >= -1) return count; // 0 is retained as a legacy spelling of auto.
    } catch (NumberFormatException ignored) {
      // Use the same actionable message for malformed and out-of-range values.
    }
    throw new IllegalArgumentException(key + " must be -1 (auto) or a positive integer (0 also means auto)");
  }

  public static int resolve(final int requested, final String key) {
    if (requested < -1) throw new IllegalArgumentException(key + " must be -1 (auto) or a positive integer (0 also means auto)");
    final int configured = requested > 0 ? requested : configured(key);
    final int available = available();
    return Math.min(available, configured > 0 ? configured : available);
  }

  private static int positive(final String value, final String key) {
    final int count = parse(value, key);
    if (count <= 0) throw new IllegalArgumentException(key + " must be a positive integer");
    return count;
  }
}
