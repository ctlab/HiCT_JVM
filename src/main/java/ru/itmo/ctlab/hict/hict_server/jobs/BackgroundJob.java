package ru.itmo.ctlab.hict.hict_server.jobs;

import java.util.List;
import java.util.Locale;

/** Common read-only representation of an existing job service. */
public record BackgroundJob(String id, String kind, String state, Double progress,
                            Long createdAtMs, Long startedAtMs, Long finishedAtMs,
                            String message, String error, List<Output> outputs,
                            @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL) Details details) {
  public record Details(String title, Long completedBytes, Long totalBytes, String currentFile,
                        @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_EMPTY) List<String> warnings,
                        @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL) String exampleId) {
    public Details { warnings = warnings == null ? List.of() : List.copyOf(warnings); }
    public Details(String title, Long completedBytes, Long totalBytes, String currentFile) {
      this(title, completedBytes, totalBytes, currentFile, List.of(), null);
    }
  }

  public BackgroundJob(String id, String kind, String state, Double progress,
                       Long createdAtMs, Long startedAtMs, Long finishedAtMs,
                       String message, String error, List<Output> outputs) {
    this(id, kind, state, progress, createdAtMs, startedAtMs, finishedAtMs, message, error, outputs, null);
  }
  public record Output(String filename, String action, String fastaFilename, String agpFilename, String mapFilename, String displayMode,
                       @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL) String displayName) {
    public Output(String filename, String action, String fastaFilename, String agpFilename, String mapFilename, String displayMode) {
      this(filename, action, fastaFilename, agpFilename, mapFilename, displayMode, null);
    }
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
    return new BackgroundJob(globalId, kind, state, progress, createdAtMs, startedAtMs, finishedAtMs, message, error, outputs, details);
  }

  private static Long timestamp(Long value) {
    return value == null || value <= 0 ? null : value;
  }
}
