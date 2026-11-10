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

package ru.itmo.ctlab.hict.hict_server.fastaonly;

import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.itmo.ctlab.hict.hict_library.converters.FastaOnlyAssembly;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

/**
 * Builds an assembly from a FASTA file as one cancellable background job.
 *
 * <p>Output goes to {@code <data>/fasta-only/}, next to nothing the user owns, as a {@code .hict.hdf5} and,
 * when splitting at runs of N, the AGP that describes the pieces. A file is written to a temporary name
 * and moved into place only when complete, so cancelling or failing never leaves something that looks
 * finished. Starting again with the same file and settings while the output is newer than the FASTA
 * reuses it.
 */
@Slf4j
public final class FastaOnlyJobService {
  public enum State {
    IDLE,
    RUNNING,
    READY,
    FAILED,
    CANCELLED
  }

  public record Request(@NotNull String fastaFilename, boolean splitAtN, int minNRun) {
  }

  /** What a client sees. Paths are relative to the data directory. */
  public record Status(@NotNull State state, double progress, @NotNull String message, @Nullable String error,
                       @Nullable String outputFilename, @Nullable String agpFilename, @Nullable String fastaFilename,
                       int contigCount, long totalBases, @NotNull List<String> warnings, boolean reused) {
  }

  private static final Pattern UNSAFE = Pattern.compile("[^A-Za-z0-9._-]");
  private static final Status IDLE = new Status(State.IDLE, 0.0d, "Not started", null, null, null, null, 0, 0L, List.of(), false);

  private final ExecutorService executor = Executors.newSingleThreadExecutor(runnable -> {
    final var thread = new Thread(runnable, "hict-fasta-only");
    thread.setDaemon(true);
    return thread;
  });
  private final Object lock = new Object();
  private Status status = IDLE;
  private AtomicBoolean cancel = new AtomicBoolean(false);

  public @NotNull Status status() {
    synchronized (lock) {
      return status;
    }
  }

  public @NotNull Status cancel() {
    synchronized (lock) {
      if (status.state() == State.RUNNING) {
        cancel.set(true);
      }
      return status;
    }
  }

  public @NotNull Status start(final @NotNull Path dataDirectory, final @NotNull Request request) {
    synchronized (lock) {
      if (status.state() == State.RUNNING) {
        throw new IllegalStateException("A FASTA is already being read. Wait for it or cancel it first.");
      }
      final var flag = new AtomicBoolean(false);
      this.cancel = flag;
      status = new Status(State.RUNNING, 0.0d, "Reading the FASTA", null, null, null, null, 0, 0L, List.of(), false);
      executor.execute(() -> run(dataDirectory, request, flag));
      return status;
    }
  }

  private void run(final @NotNull Path dataDirectory, final @NotNull Request request, final @NotNull AtomicBoolean flag) {
    Path output = null;
    Path agpPath = null;
    try {
      final var root = dataDirectory.toRealPath();
      final var fasta = resolveInside(root, request.fastaFilename());
      if (!Files.isRegularFile(fasta)) {
        throw new IllegalArgumentException("FASTA file not found: " + request.fastaFilename());
      }
      final int minRun = request.splitAtN() ? request.minNRun() : FastaOnlyAssembly.DEFAULT_MIN_N_RUN;
      final var outputDirectory = root.resolve("fasta-only");
      Files.createDirectories(outputDirectory);
      if (!outputDirectory.toRealPath().startsWith(root)) {
        throw new IllegalArgumentException("The fasta-only folder must be inside the data directory");
      }
      final var stem = UNSAFE.matcher(fasta.getFileName().toString()).replaceAll("_")
        + (request.splitAtN() ? ".split" + minRun : ".whole");
      output = outputDirectory.resolve(stem + ".hict.hdf5");
      agpPath = outputDirectory.resolve(stem + ".agp");
      final var relativeOutput = root.relativize(output).toString();
      final var relativeAgp = root.relativize(agpPath).toString();
      final var relativeFasta = root.relativize(fasta.toRealPath().startsWith(root) ? fasta.toRealPath() : fasta).toString();

      final var scan = FastaOnlyAssembly.scan(fasta, minRun, flag::get,
        fraction -> update(0.6d * fraction, "Reading the FASTA"));
      final var layout = request.splitAtN() ? FastaOnlyAssembly.agpLayout(scan) : null;
      final boolean reuse = Files.exists(output)
        && Files.getLastModifiedTime(output).compareTo(Files.getLastModifiedTime(fasta)) > 0
        && (layout == null || Files.exists(agpPath));
      if (!reuse) {
        update(0.6d, "Building the assembly");
        FastaOnlyAssembly.write(scan, output, flag::get, fraction -> update(0.6d + 0.4d * fraction, "Building the assembly"));
        if (layout != null) {
          final var temporary = agpPath.resolveSibling(agpPath.getFileName() + ".partial");
          Files.writeString(temporary, layout, StandardCharsets.UTF_8);
          Files.move(temporary, agpPath, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        } else {
          Files.deleteIfExists(agpPath);
        }
      }
      if (flag.get()) {
        throw new CancellationException("Cancelled");
      }
      final var finalAgp = layout != null ? relativeAgp : null;
      final int contigs = scan.records().size();
      final var warnings = new java.util.ArrayList<>(scan.warnings());
      if (request.splitAtN() && layout == null) {
        warnings.add("No run of at least " + minRun + " N was found, so nothing was split.");
      }
      synchronized (lock) {
        status = new Status(State.READY, 1.0d, "Ready", null, relativeOutput, finalAgp, relativeFasta, contigs,
          scan.totalBases(), List.copyOf(warnings), reuse);
      }
    } catch (final CancellationException cancelled) {
      synchronized (lock) {
        status = new Status(State.CANCELLED, status.progress(), "Cancelled", null, null, null, null, 0, 0L, List.of(), false);
      }
    } catch (final Throwable failure) {
      log.info("FASTA-only build failed: {}", failure.getMessage());
      synchronized (lock) {
        final var message = failure.getMessage() == null ? failure.getClass().getSimpleName() : failure.getMessage();
        status = new Status(State.FAILED, status.progress(), "Failed", message, null, null, null, 0, 0L, List.of(), false);
      }
    }
  }

  private void update(final double progress, final @NotNull String message) {
    synchronized (lock) {
      if (status.state() == State.RUNNING) {
        status = new Status(State.RUNNING, Math.min(0.99d, Math.max(0.0d, progress)), message, null, null, null, null, 0, 0L,
          List.of(), false);
      }
    }
  }

  static @NotNull Path resolveInside(final @NotNull Path root, final @NotNull String relative) throws IOException {
    if (relative.isBlank() || relative.indexOf('\0') >= 0) {
      throw new IllegalArgumentException("A FASTA file name is required");
    }
    final var resolved = root.resolve(relative).normalize();
    if (!resolved.startsWith(root)) {
      throw new IllegalArgumentException("The FASTA file must be inside the data directory");
    }
    return resolved;
  }

  public void close() {
    cancel.set(true);
    executor.shutdownNow();
  }
}
