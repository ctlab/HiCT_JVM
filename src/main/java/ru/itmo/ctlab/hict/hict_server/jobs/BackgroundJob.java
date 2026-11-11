package ru.itmo.ctlab.hict.hict_server.jobs;

import java.util.List;
import java.util.Locale;

/** Common read-only representation of an existing job service. */
public record BackgroundJob(String id, String kind, String state, Double progress,
                            Long createdAtMs, Long startedAtMs, Long finishedAtMs,
                            String message, String error, List<Output> outputs) {
  public record Output(String filename, String action, String fastaFilename, String agpFilename, String mapFilename, String displayMode) {
    public Output(String filename, String action, String fastaFilename, String agpFilename, String mapFilename) {
      this(filename, action, fastaFilename, agpFilename, mapFilename, null);
    }
    public Output(String filename, String action, String fastaFilename) {
      this(filename, action, fastaFilename, null, null);
    }
    public Output(String filename, String action, String fastaFilename, String agpFilename) {
      this(filename, action, fastaFilename, agpFilename, null);
    }
  }

  public BackgroundJob {
    state = switch (state.toLowerCase(Locale.ROOT)) {
      case "ready", "complete", "completed" -> "finished";
      case "pending", "queued" -> "submitted";
      case "preparing", "running", "cancelling" -> "running";
      default -> state.toLowerCase(Locale.ROOT);
    };
    progress = progress == null || !Double.isFinite(progress) ? null : Math.max(0, Math.min(1, progress));
    createdAtMs = timestamp(createdAtMs);
    startedAtMs = timestamp(startedAtMs);
    finishedAtMs = timestamp(finishedAtMs);
    outputs = List.copyOf(outputs);
  }

  public boolean canCancel() {
    return state.equals("submitted") || state.equals("running");
  }

  public BackgroundJob withId(String globalId) {
    return new BackgroundJob(globalId, kind, state, progress, createdAtMs, startedAtMs, finishedAtMs, message, error, outputs);
  }

  private static Long timestamp(Long value) {
    return value == null || value <= 0 ? null : value;
  }
}
