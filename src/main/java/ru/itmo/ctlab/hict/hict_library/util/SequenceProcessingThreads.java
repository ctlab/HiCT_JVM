package ru.itmo.ctlab.hict.hict_library.util;

/** Thread policy for sequence analysis. Unlike legacy conversion options, zero is adaptive auto. */
public final class SequenceProcessingThreads {
  private SequenceProcessingThreads() { }

  public record Selection(int requested, int effective, String mode) { }

  public static Selection resolve(final int requested, final String key, final long parallelTasks) {
    if (requested < -1) {
      throw new IllegalArgumentException("threads must be -1 (all allocated CPUs), 0 (auto), or a positive integer");
    }
    final int configured = requested == -1 ? ProcessingThreads.configured(key) : requested;
    final int available = ProcessingThreads.available();
    if (configured > 0) {
      return new Selection(requested, Math.min(available, configured), "explicit");
    }
    if (configured == -1) {
      return new Selection(requested, available, "all-allocated");
    }
    final int taskBound = (int) Math.max(1L, Math.min(Integer.MAX_VALUE, parallelTasks));
    return new Selection(requested, Math.min(available, taskBound), "auto");
  }

  /** Restrict a resolved ceiling to independent work units so diagnostics match usable concurrency. */
  public static Selection boundToTasks(final Selection selection, final long parallelTasks) {
    final int taskBound = (int) Math.max(1L, Math.min(Integer.MAX_VALUE, parallelTasks));
    return new Selection(selection.requested(), Math.min(selection.effective(), taskBound), selection.mode());
  }
}
