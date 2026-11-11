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

package ru.itmo.ctlab.hict.hict_server.normalization;

import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.CoverageWeightsBuilder;
import ru.itmo.ctlab.hict.hict_library.visualization.CoverageNormalizer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Runs square-root coverage preparation as one cancellable background job.
 *
 * <p>Only one job runs at a time. Starting again while one is running for the same files joins it.
 * Vectors are published to the files only when every resolution of every file is done, so a cancelled
 * or failed job leaves the maps exactly as they were. Progress counts finished files, with the
 * fraction of the current file's resolutions inside it.
 */
@Slf4j
public final class CoverageNormalizationService {
  public enum State {
    IDLE,
    RUNNING,
    READY,
    FAILED,
    CANCELLED
  }

  /** What a client sees. {@code fileKey} identifies the files the job was started for. */
  public record Status(@NotNull State state, double progress, @NotNull String message,
                       @NotNull String algorithmVersion, @Nullable String error, long jobId) {
  }

  private final ExecutorService executor = Executors.newSingleThreadExecutor(runnable -> {
    final var thread = new Thread(runnable, "hict-coverage-normalization");
    thread.setDaemon(true);
    return thread;
  });
  private final Object lock = new Object();
  private Status status = new Status(State.IDLE, 0.0d, "Not started", CoverageNormalizer.ALGORITHM_VERSION, null, 0L);
  private AtomicBoolean cancelFlag = new AtomicBoolean(false);
  private long nextJobId = 1L;

  public @NotNull Status status() {
    synchronized (lock) {
      return status;
    }
  }

  public static final class AlreadyRunningException extends IllegalStateException {
    private AlreadyRunningException() {
      super("Square-root coverage is already being prepared. Wait for it or cancel it first.");
    }
  }

  /** Starts a job for the files. Clients can follow an existing job through status. */
  public @NotNull Status start(final @NotNull List<ChunkedFile> files, final long budgetBytes) {
    synchronized (lock) {
      if (status.state() == State.RUNNING) {
        throw new AlreadyRunningException();
      }
      if (files.stream().allMatch(f -> f.getNormalizationWeights().prepared() != null)) {
        status = new Status(State.READY, 1.0d, "Already prepared", CoverageNormalizer.ALGORITHM_VERSION, null, nextJobId++);
        return status;
      }
      final long jobId = nextJobId++;
      final var cancel = new AtomicBoolean(false);
      this.cancelFlag = cancel;
      status = new Status(State.RUNNING, 0.0d, "Reading contacts", CoverageNormalizer.ALGORITHM_VERSION, null, jobId);
      executor.execute(() -> run(jobId, List.copyOf(files), budgetBytes, cancel));
      return status;
    }
  }

  public @NotNull Status cancel() {
    synchronized (lock) {
      if (status.state() == State.RUNNING) {
        cancelFlag.set(true);
      }
      return status;
    }
  }

  public void invalidateForMapChange() {
    synchronized (lock) {
      cancelFlag.set(true);
      final boolean running = status.state() == State.RUNNING;
      status = new Status(running ? State.CANCELLED : State.IDLE, 0.0d,
        running ? "Cancelled because the map changed" : "Not started",
        CoverageNormalizer.ALGORITHM_VERSION, null, 0L);
    }
  }

  private void run(final long jobId, final @NotNull List<ChunkedFile> files, final long budgetBytes,
                   final @NotNull AtomicBoolean cancel) {
    try {
      final var prepared = new ArrayList<ru.itmo.ctlab.hict.hict_library.chunkedfile.NormalizationWeights.Prepared>();
      for (int i = 0; i < files.size(); i++) {
        final int index = i;
        final var file = files.get(i);
        if (file.getNormalizationWeights().prepared() != null) {
          prepared.add(file.getNormalizationWeights().prepared());
          continue;
        }
        prepared.add(CoverageWeightsBuilder.build(file, budgetBytes, cancel::get, fraction -> update(jobId,
          (index + fraction) / files.size(),
          files.size() > 1 ? "Reading contacts of source " + (index + 1) + " of " + files.size() : "Reading contacts")));
      }
      if (cancel.get()) {
        throw new CoverageNormalizer.CancelledException();
      }
      // Publish only now, so nothing changes unless every file finished.
      synchronized (lock) {
        if (status.jobId() != jobId) return;
        if (cancel.get()) throw new CoverageNormalizer.CancelledException();
        for (int i = 0; i < files.size(); i++) {
          files.get(i).getNormalizationWeights().publish(prepared.get(i));
        }
        status = new Status(State.READY, 1.0d, "Ready", CoverageNormalizer.ALGORITHM_VERSION, null, jobId);
      }
    } catch (final CoverageNormalizer.CancelledException cancelled) {
      synchronized (lock) {
        if (status.jobId() != jobId) return;
        status = new Status(State.CANCELLED, status.progress(), "Cancelled", CoverageNormalizer.ALGORITHM_VERSION, null, jobId);
      }
    } catch (final Throwable failure) {
      synchronized (lock) {
        if (status.jobId() != jobId) return;
        if (cancel.get()) {
          status = new Status(State.CANCELLED, status.progress(), "Cancelled", CoverageNormalizer.ALGORITHM_VERSION, null, jobId);
          return;
        }
        log.warn("Coverage normalization failed", failure);
        final var message = failure.getMessage() == null ? failure.getClass().getSimpleName() : failure.getMessage();
        status = new Status(State.FAILED, status.progress(), "Failed", CoverageNormalizer.ALGORITHM_VERSION, message, jobId);
      }
    }
  }

  private void update(final long jobId, final double progress, final @NotNull String message) {
    synchronized (lock) {
      if (status.state() == State.RUNNING && status.jobId() == jobId) {
        status = new Status(State.RUNNING, Math.min(0.99d, Math.max(0.0d, progress)), message,
          CoverageNormalizer.ALGORITHM_VERSION, null, jobId);
      }
    }
  }

  public void close() {
    invalidateForMapChange();
    executor.shutdownNow();
  }
}
