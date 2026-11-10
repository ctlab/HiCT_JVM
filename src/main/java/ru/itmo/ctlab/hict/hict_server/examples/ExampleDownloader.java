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

package ru.itmo.ctlab.hict.hict_server.examples;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Downloads the files of one catalog example, as one cancellable background job.
 *
 * <p>The client names only an example id. The addresses, names, sizes and hashes come from the catalog the
 * server loaded. Every address, including each redirect, must be https and resolve only to public
 * addresses; redirects are followed by hand, a few at most. Files go to
 * {@code <data>/examples/<exampleId>/<filename>} (names are validated and the final path is checked to be
 * inside), are written to a {@code .part} file next to a small record of what was requested, and are moved
 * into place only after the length and the hash (when the catalog has one) check out. A partial file is
 * resumed with {@code Range} and {@code If-Range}; a changed remote object, a refused range or a
 * {@code 200} answer restarts from zero. Nothing downloaded is ever executed.
 */
@Slf4j
public final class ExampleDownloader {
  public enum State {
    IDLE,
    RUNNING,
    READY,
    FAILED,
    CANCELLED
  }

  /** What a client sees. */
  public record Status(@NotNull State state, @Nullable String exampleId, double progress, @NotNull String message,
                       @Nullable String error, long downloadedBytes, long totalBytes, @Nullable String currentFile,
                       @NotNull List<String> files, @NotNull List<String> skippedOptional, boolean allRequiredPresent) {
  }

  /** Decides which addresses may be contacted. The test constructor can allow plain http and loopback. */
  public interface UrlPolicy {
    void check(@NotNull URI uri) throws IOException;

    /** https only, and only public addresses. */
    static @NotNull UrlPolicy production() {
      return uri -> {
        if (!"https".equalsIgnoreCase(uri.getScheme())) {
          throw new IOException("Only https addresses are allowed: " + uri.getHost());
        }
        if (uri.getHost() == null || uri.getUserInfo() != null) {
          throw new IOException("The address has no host or carries credentials");
        }
        if (uri.getPort() != -1 && uri.getPort() != 443) {
          throw new IOException("Only the standard https port is allowed");
        }
        final InetAddress[] addresses;
        try {
          addresses = InetAddress.getAllByName(uri.getHost());
        } catch (final UnknownHostException e) {
          throw new IOException("The host " + uri.getHost() + " could not be found");
        }
        for (final var address : addresses) {
          if (!isPublic(address)) {
            throw new IOException(uri.getHost() + " resolves to a non-public address and was refused");
          }
        }
      };
    }

    static boolean isPublic(final InetAddress a) {
      if (a.isAnyLocalAddress() || a.isLoopbackAddress() || a.isLinkLocalAddress() || a.isSiteLocalAddress() || a.isMulticastAddress()) {
        return false;
      }
      final var bytes = a.getAddress();
      if (bytes.length == 4) {
        final int b0 = bytes[0] & 0xff;
        final int b1 = bytes[1] & 0xff;
        return !(b0 == 100 && b1 >= 64 && b1 <= 127) && b0 != 0 && !(b0 == 198 && (b1 == 18 || b1 == 19));
      }
      return (bytes[0] & 0xfe) != 0xfc;
    }
  }

  private static final int MAX_REDIRECTS = 5;
  private static final long DEFAULT_MAX_BYTES = 64L << 30;
  private static final long SAFETY_MARGIN = 512L << 20;

  private final Path dataDirectory;
  private final UrlPolicy policy;
  private final HttpClient client;
  private final long freeSpaceMargin;
  private final ExecutorService executor = Executors.newSingleThreadExecutor(r -> {
    final var t = new Thread(r, "hict-example-download");
    t.setDaemon(true);
    return t;
  });
  private final Object lock = new Object();
  private Status status = idle();
  private AtomicBoolean cancel = new AtomicBoolean(false);

  public ExampleDownloader(final @NotNull Path dataDirectory) {
    this(dataDirectory, UrlPolicy.production(), SAFETY_MARGIN);
  }

  /** For tests only: the production constructor above is the only one the server uses. */
  ExampleDownloader(final @NotNull Path dataDirectory, final @NotNull UrlPolicy policy, final long freeSpaceMargin) {
    this.dataDirectory = dataDirectory;
    this.policy = policy;
    this.freeSpaceMargin = freeSpaceMargin;
    this.client = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NEVER).connectTimeout(Duration.ofSeconds(15)).build();
  }

  private static Status idle() {
    return new Status(State.IDLE, null, 0.0d, "Not started", null, 0L, 0L, null, List.of(), List.of(), false);
  }

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

  /** Starts downloading the example, or reports the running job when it is the same example. */
  public @NotNull Status start(final @NotNull ExamplesCatalog.Example example) {
    synchronized (lock) {
      if (status.state() == State.RUNNING) {
        if (example.id().equals(status.exampleId())) {
          return status;
        }
        throw new IllegalStateException("Another example is being downloaded. Wait for it or cancel it first.");
      }
      final var flag = new AtomicBoolean(false);
      this.cancel = flag;
      status = new Status(State.RUNNING, example.id(), 0.0d, "Starting", null, 0L, 0L, null, List.of(), List.of(), false);
      executor.execute(() -> run(example, flag));
      return status;
    }
  }

  private void run(final ExamplesCatalog.Example example, final AtomicBoolean flag) {
    final var done = new ArrayList<String>();
    final var skipped = new ArrayList<String>();
    try {
      final var files = example.body().getJsonArray("files");
      final var folder = destinationFolder(example.id());
      long total = 0L;
      for (int i = 0; i < files.size(); i++) {
        total += files.getJsonObject(i).getLong("bytes", 0L);
      }
      final long expectedTotal = total;
      final long[] finishedBytes = {0L};
      for (int i = 0; i < files.size(); i++) {
        final var file = files.getJsonObject(i);
        final boolean required = file.getBoolean("required", true);
        final var name = file.getString("filename");
        try {
          download(example, file, folder, flag, (soFar, size) -> publish(example.id(), finishedBytes[0] + soFar,
            expectedTotal > 0 ? expectedTotal : Math.max(1L, finishedBytes[0] + size), name, done, skipped));
          done.add(name);
          finishedBytes[0] += Files.size(folder.resolve(name));
        } catch (final CancellationException cancelled) {
          throw cancelled;
        } catch (final IOException | RuntimeException failure) {
          if (required) {
            throw failure;
          }
          log.info("Optional example file {} was not downloaded: {}", name, failure.getMessage());
          skipped.add(name + " (" + failure.getMessage() + ")");
        }
      }
      writeManifest(example, folder, done);
      synchronized (lock) {
        status = new Status(State.READY, example.id(), 1.0d, "Ready to open", null, finishedBytes[0], finishedBytes[0], null,
          List.copyOf(done), List.copyOf(skipped), true);
      }
    } catch (final CancellationException cancelled) {
      synchronized (lock) {
        status = new Status(State.CANCELLED, example.id(), status.progress(), "Cancelled. Partial files are kept so the download can resume.",
          null, status.downloadedBytes(), status.totalBytes(), null, List.copyOf(done), List.copyOf(skipped), false);
      }
    } catch (final Throwable failure) {
      final var message = failure.getMessage() == null ? failure.getClass().getSimpleName() : failure.getMessage();
      log.info("Example download failed: {}", message);
      synchronized (lock) {
        status = new Status(State.FAILED, example.id(), status.progress(), "Failed", message, status.downloadedBytes(),
          status.totalBytes(), null, List.copyOf(done), List.copyOf(skipped), false);
      }
    }
  }

  private void publish(final String exampleId, final long soFar, final long total, final String file,
                       final List<String> done, final List<String> skipped) {
    synchronized (lock) {
      if (status.state() == State.RUNNING) {
        status = new Status(State.RUNNING, exampleId, Math.min(0.99d, soFar / (double) Math.max(1L, total)), "Downloading " + file,
          null, soFar, total, file, List.copyOf(done), List.copyOf(skipped), false);
      }
    }
  }

  private @NotNull Path destinationFolder(final String exampleId) throws IOException {
    final var root = dataDirectory.toRealPath();
    final var folder = root.resolve("examples").resolve(exampleId);
    Files.createDirectories(folder);
    if (!folder.toRealPath().startsWith(root)) {
      throw new IOException("The examples folder must be inside the data directory");
    }
    return folder.toRealPath();
  }

  @FunctionalInterface
  private interface Progress {
    void update(long downloaded, long size);
  }

  private void download(final ExamplesCatalog.Example example, final JsonObject file, final Path folder,
                        final AtomicBoolean flag, final Progress progress) throws IOException {
    final var name = file.getString("filename");
    final var target = folder.resolve(name).normalize();
    if (!target.startsWith(folder) || !target.getParent().equals(folder)) {
      throw new IOException("The file name " + name + " is not a plain name");
    }
    final long expected = file.getLong("bytes", -1L);
    final var sha = file.getString("sha256");
    if (Files.isRegularFile(target) && verified(target, expected, sha, false)) {
      progress.update(Files.size(target), Files.size(target));
      return;
    }
    Files.deleteIfExists(target);
    final var part = folder.resolve(name + ".part");
    final var meta = folder.resolve(name + ".part.json");
    var uri = URI.create(file.getString("url"));
    final long limit = expected > 0 ? expected : DEFAULT_MAX_BYTES;

    long have = Files.isRegularFile(part) ? Files.size(part) : 0L;
    String etag = null;
    String lastModified = null;
    if (have > 0 && Files.isRegularFile(meta)) {
      final var saved = new JsonObject(Files.readString(meta, StandardCharsets.UTF_8));
      if (uri.toString().equals(saved.getString("url"))) {
        etag = saved.getString("etag");
        lastModified = saved.getString("lastModified");
      } else {
        have = 0;
      }
    } else if (have > 0) {
      have = 0;
    }
    if (have == 0) {
      Files.deleteIfExists(part);
      Files.deleteIfExists(meta);
    }
    if (have > limit) {
      Files.deleteIfExists(part);
      Files.deleteIfExists(meta);
      have = 0;
    }

    for (int attempt = 0; attempt < 2; attempt++) {
      final var response = open(uri, have, etag != null ? etag : lastModified, flag);
      final int code = response.statusCode();
      if (code == 416) {
        response.body().close();
        if (expected > 0 && have == expected) {
          break;
        }
        // The server will not honor the range: start again from nothing.
        Files.deleteIfExists(part);
        Files.deleteIfExists(meta);
        have = 0;
        etag = null;
        lastModified = null;
        continue;
      }
      if (code != 200 && code != 206) {
        response.body().close();
        throw new IOException("The server answered " + code + " for " + name);
      }
      final var contentRange = response.headers().firstValue("Content-Range").orElse(null);
      final long length = response.headers().firstValueAsLong("Content-Length").orElse(-1L);
      if (code == 206) {
        if (contentRange == null || !contentRange.startsWith("bytes " + have + "-")) {
          response.body().close();
          throw new IOException("The server resumed " + name + " at a different position than asked");
        }
      } else if (have > 0) {
        // A 200 to a range request means the object changed or ranges are ignored. Restart cleanly.
        have = 0;
        Files.deleteIfExists(part);
        Files.deleteIfExists(meta);
      }
      final long size = code == 206 ? have + length : length;
      if (expected > 0 && size > 0 && size != expected) {
        response.body().close();
        throw new IOException(name + " is " + size + " bytes but the catalog says " + expected + ". Nothing was kept.");
      }
      if (size > limit) {
        response.body().close();
        throw new IOException(name + " is larger than the allowed " + limit + " bytes");
      }
      final long needed = size > 0 ? size - have : 0L;
      final long usable = Files.getFileStore(folder).getUsableSpace();
      if (needed > 0 && usable < needed + freeSpaceMargin) {
        response.body().close();
        throw new IOException("Not enough free disk space for " + name + ": needs about " + (needed >> 20) + " MiB plus a safety margin, "
          + (usable >> 20) + " MiB free.");
      }
      etag = response.headers().firstValue("ETag").orElse(etag);
      lastModified = response.headers().firstValue("Last-Modified").orElse(lastModified);
      Files.writeString(meta, new JsonObject().put("url", uri.toString()).put("etag", etag).put("lastModified", lastModified)
        .put("startedAt", java.time.Instant.now().toString()).encode(), StandardCharsets.UTF_8);
      try (InputStream in = response.body(); RandomAccessFile out = new RandomAccessFile(part.toFile(), "rw")) {
        out.setLength(have);
        out.seek(have);
        final var buffer = new byte[1 << 16];
        long position = have;
        int read;
        long lastReport = 0L;
        while ((read = in.read(buffer)) >= 0) {
          if (flag.get()) {
            throw new CancellationException("Cancelled");
          }
          position += read;
          if (position > limit) {
            throw new IOException(name + " exceeded the allowed " + limit + " bytes");
          }
          out.write(buffer, 0, read);
          final long now = System.nanoTime();
          if (now - lastReport > 200_000_000L) {
            progress.update(position, size > 0 ? size : position);
            lastReport = now;
          }
        }
        if (size > 0 && position != size) {
          throw new IOException("The connection ended after " + position + " of " + size + " bytes. Run it again to resume.");
        }
        progress.update(position, size > 0 ? size : position);
      }
      break;
    }
    if (!verified(part, expected, sha, true)) {
      Files.deleteIfExists(part);
      Files.deleteIfExists(meta);
      throw new IOException(name + " failed verification (length or sha256 does not match the catalog) and was discarded");
    }
    Files.move(part, target, StandardCopyOption.ATOMIC_MOVE);
    Files.deleteIfExists(meta);
  }

  private HttpResponse<InputStream> open(final URI first, final long have, final @Nullable String validator,
                                         final AtomicBoolean flag) throws IOException {
    var uri = first;
    for (int hop = 0; hop <= MAX_REDIRECTS; hop++) {
      policy.check(uri);
      final var builder = HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(60)).GET();
      if (have > 0) {
        builder.header("Range", "bytes=" + have + "-");
        if (validator != null) {
          builder.header("If-Range", validator);
        }
      }
      final HttpResponse<InputStream> response;
      try {
        response = client.send(builder.build(), HttpResponse.BodyHandlers.ofInputStream());
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new CancellationException("Interrupted");
      }
      final int code = response.statusCode();
      if (code >= 300 && code < 400 && code != 304) {
        final var location = response.headers().firstValue("Location").orElseThrow(() -> new IOException("A redirect had no Location"));
        response.body().close();
        uri = uri.resolve(location);
        if (flag.get()) {
          throw new CancellationException("Cancelled");
        }
        continue;
      }
      return response;
    }
    throw new IOException("Too many redirects");
  }

  private static boolean verified(final Path file, final long expectedBytes, final @Nullable String sha, final boolean complete) throws IOException {
    if (expectedBytes > 0 && Files.size(file) != expectedBytes) {
      return false;
    }
    if (sha != null) {
      try (InputStream in = Files.newInputStream(file)) {
        final var digest = MessageDigest.getInstance("SHA-256");
        final var buffer = new byte[1 << 16];
        int read;
        while ((read = in.read(buffer)) >= 0) {
          digest.update(buffer, 0, read);
        }
        return HexFormat.of().formatHex(digest.digest()).equals(sha);
      } catch (final java.security.NoSuchAlgorithmException e) {
        throw new IOException(e);
      }
    }
    return complete || expectedBytes > 0;
  }

  /** The record that stays with the files: where they came from, their hashes and the attribution to show. */
  private void writeManifest(final ExamplesCatalog.Example example, final Path folder, final List<String> downloaded) throws IOException {
    final var files = new JsonArray();
    for (final var name : downloaded) {
      final var path = folder.resolve(name);
      final var entry = new JsonObject().put("filename", name).put("bytes", Files.size(path));
      try (InputStream in = Files.newInputStream(path)) {
        final var digest = MessageDigest.getInstance("SHA-256");
        final var buffer = new byte[1 << 16];
        int read;
        while ((read = in.read(buffer)) >= 0) {
          digest.update(buffer, 0, read);
        }
        entry.put("sha256", HexFormat.of().formatHex(digest.digest()));
      } catch (final java.security.NoSuchAlgorithmException e) {
        throw new IOException(e);
      }
      files.add(entry);
    }
    final var body = example.body();
    final var manifest = new JsonObject().put("exampleId", example.id()).put("title", body.getString("title"))
      .put("catalogSource", example.source()).put("completedAt", java.time.Instant.now().toString())
      .put("files", files).put("openingRecipe", body.getJsonObject("openingRecipe"))
      .put("attribution", body.getJsonObject("attribution")).put("publications", body.getJsonArray("publications"));
    final var temporary = folder.resolve("manifest.json.partial");
    Files.writeString(temporary, manifest.encodePrettily(), StandardCharsets.UTF_8);
    Files.move(temporary, folder.resolve("manifest.json"), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
  }

  public void close() {
    cancel.set(true);
    executor.shutdownNow();
  }
}
