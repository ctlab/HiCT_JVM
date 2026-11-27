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

import ru.itmo.ctlab.hict.hict_server.jobs.BackgroundJob;

import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.CoverageWeightsBuilder;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.NormalizationWeights;
import ru.itmo.ctlab.hict.hict_library.visualization.CoverageNormalizer;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.DoubleConsumer;

/** Prepares all coverage levels for one source, publishing only a complete result. */
@Slf4j
public final class CoverageNormalizationService {
  public enum State {
    IDLE,
    RUNNING,
    READY,
    FAILED,
    CANCELLED
  }

  /** The state and identity of the latest preparation for this source. */
  public record Status(@NotNull State state, double progress, @NotNull String message,
                       @NotNull String algorithmVersion, @Nullable String error, long jobId) {
  }

  private static final AtomicLong JOB_IDS = new AtomicLong();
  private final ExecutorService executor;
  private final String sourceLabel;
  private final Preparation preparation;

  @FunctionalInterface
  interface Preparation {
    NormalizationWeights.Prepared scan(ChunkedFile source, long budget, BooleanSupplier cancelled, DoubleConsumer progress);
  }

  public CoverageNormalizationService(ExecutorService executor, String sourceLabel) {
    this(executor, sourceLabel, CoverageWeightsBuilder::build);
  }

  CoverageNormalizationService(ExecutorService executor, String sourceLabel, Preparation preparation) {
    this.executor = executor;
    this.sourceLabel = sourceLabel;
    this.preparation = preparation;
  }

  private final Object lock = new Object();
  private Status status = new Status(State.IDLE, 0.0d, "Not started", CoverageNormalizer.ALGORITHM_VERSION, null, 0L);
  private AtomicBoolean cancelFlag = new AtomicBoolean(false);
  private long startedAtMs;
  private long finishedAtMs;

  public java.util.List<BackgroundJob> backgroundJobs() {
    synchronized (lock) {
      if (status.state() == State.IDLE) return java.util.List.of();
      return java.util.List.of(new BackgroundJob(
        Long.toString(status.jobId()), "coverage", status.state().name(), status.progress(),
        startedAtMs, startedAtMs, finishedAtMs, sourceLabel + ": " + status.message(), status.error(), java.util.List.of()));
    }
  }

  public boolean cancelBackgroundJob(String id) {
    synchronized (lock) {
      if (status.state() != State.RUNNING || !Long.toString(status.jobId()).equals(id)) return false;
      cancel();
      return true;
    }
  }


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

  /** Starts a job for the selected source. Clients can follow it through status. */
  public @NotNull Status start(final @NotNull ChunkedFile file, final long budgetBytes,
                               final @NotNull Path processedDirectory) {
    synchronized (lock) {
      if (status.state() == State.RUNNING) {
        throw new AlreadyRunningException();
      }
      startedAtMs = System.currentTimeMillis();
      finishedAtMs = 0;
      if (file.getNormalizationWeights().prepared() != null) {
        finishedAtMs = System.currentTimeMillis();
        var message = status.state() == State.READY ? status.message() : "Already prepared";
        status = new Status(State.READY, 1.0d, message, CoverageNormalizer.ALGORITHM_VERSION, null, JOB_IDS.incrementAndGet());
        return status;
      }
      final long jobId = JOB_IDS.incrementAndGet();
      final var cancel = new AtomicBoolean(false);
      this.cancelFlag = cancel;
      status = new Status(State.RUNNING, 0.0d, "Preparing coverage", CoverageNormalizer.ALGORITHM_VERSION, null, jobId);
      executor.execute(() -> run(jobId, file, budgetBytes, processedDirectory, cancel));
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
        CoverageNormalizer.ALGORITHM_VERSION, null, running ? status.jobId() : 0L);
      if (running) finishedAtMs = System.currentTimeMillis();
    }
  }

  private void run(final long jobId, final @NotNull ChunkedFile file, final long budgetBytes,
                   final @NotNull Path processedDirectory, final @NotNull AtomicBoolean cancel) {
    try {
      if (cancel.get()) throw new CoverageNormalizer.CancelledException();
      CoverageWeightsBuilder.requireWithinBudget(file, budgetBytes);
      String cacheKey = null;
      NormalizationWeights.Prepared prepared = null;
      try {
        cacheKey = CoverageCache.key(file);
        prepared = CoverageCache.read(processedDirectory, cacheKey, CoverageCache.expectedBins(file));
      } catch (IOException invalid) {
        log.warn("Ignoring saved coverage for {}: {}", file.getHdfFilePath().getFileName(), invalid.getMessage());
      }
      final boolean loaded = prepared != null;
      boolean saved = loaded;
      if (!loaded) {
        prepared = preparation.scan(file, budgetBytes, cancel::get, fraction -> update(jobId, fraction, "Reading contacts"));
        if (cancel.get()) throw new CoverageNormalizer.CancelledException();
        try {
          if (cacheKey != null && cacheKey.equals(CoverageCache.key(file))) {
            CoverageCache.write(processedDirectory, cacheKey, prepared);
            saved = true;
          }
        } catch (IOException unavailable) {
          log.warn("Could not save coverage for {}: {}", file.getHdfFilePath().getFileName(), unavailable.getMessage());
        }
      }
      synchronized (lock) {
        if (status.jobId() != jobId) return;
        if (cancel.get()) throw new CoverageNormalizer.CancelledException();
        file.getNormalizationWeights().publish(prepared);
        finishedAtMs = System.currentTimeMillis();
        var message = loaded ? "Loaded saved coverage" : saved ? "Saved coverage" : "Ready";
        if (loaded) log.info("Loaded saved coverage for {}", file.getHdfFilePath().getFileName());
        status = new Status(State.READY, 1.0d, message, CoverageNormalizer.ALGORITHM_VERSION, null, jobId);
      }
    } catch (final CoverageNormalizer.CancelledException cancelled) {
      synchronized (lock) {
        if (status.jobId() != jobId) return;
        finishedAtMs = System.currentTimeMillis();
        status = new Status(State.CANCELLED, status.progress(), "Cancelled", CoverageNormalizer.ALGORITHM_VERSION, null, jobId);
      }
    } catch (final Throwable failure) {
      synchronized (lock) {
        if (status.jobId() != jobId) return;
        if (cancel.get()) {
          finishedAtMs = System.currentTimeMillis();
          status = new Status(State.CANCELLED, status.progress(), "Cancelled", CoverageNormalizer.ALGORITHM_VERSION, null, jobId);
          return;
        }
        log.warn("Coverage normalization failed", failure);
        final var message = failure.getMessage() == null ? failure.getClass().getSimpleName() : failure.getMessage();
        finishedAtMs = System.currentTimeMillis();
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

}
