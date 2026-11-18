package ru.itmo.ctlab.hict.hict_server.handlers.conversion;

import io.vertx.core.json.JsonObject;
import ru.itmo.ctlab.hict.hict_library.assembly.SequenceSearchQueries;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

/** Download an existing RID with GET only. This class cannot submit sequence searches. */
final class NcbiBlastResultRetriever {
  private static final URI NCBI = URI.create("https://blast.ncbi.nlm.nih.gov/Blast.cgi");
  private static final HttpClient CLIENT = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10))
    .followRedirects(HttpClient.Redirect.NEVER).build();
  private static final LinkedHashMap<String, Long> LAST_REQUESTS = new LinkedHashMap<>();
  private final HttpClient client;
  private final URI endpoint;
  private final Duration ridSpacing;
  private final long maximumBytes;

  NcbiBlastResultRetriever() { this(CLIENT, NCBI, Duration.ofMinutes(1), RemoteBlastRunner.MAX_OUTPUT_BYTES); }
  // An explicit local-provider seam for deterministic tests, never an HTTP request parameter.
  NcbiBlastResultRetriever(HttpClient client, URI endpoint, Duration ridSpacing, long maximumBytes) {
    if (!endpoint.isAbsolute() || endpoint.getRawQuery() != null || endpoint.getFragment() != null
        || !List.of("http", "https").contains(endpoint.getScheme()) || ridSpacing.isNegative()
        || maximumBytes < 1 || maximumBytes > RemoteBlastRunner.MAX_OUTPUT_BYTES) throw new IllegalArgumentException("Invalid retrieval configuration");
    this.client = client; this.endpoint = endpoint; this.ridSpacing = ridSpacing; this.maximumBytes = maximumBytes;
  }

  RemoteBlastRunner.Result retrieve(Path directory, List<SequenceSearchQueries.Query> queries,
                                   RemoteBlastOptions options, String querySha256, Consumer<String> log,
                                   BooleanSupplier cancelled, Duration timeout, Runnable requestStarting) throws Exception {
    String rid = options.requestId();
    if (rid == null || !rid.matches("[A-Z0-9]{8,64}")) throw new IllegalArgumentException("A valid NCBI request ID is required");
    long deadline = System.nanoTime() + timeout.toNanos();
    String key = endpoint + "/" + rid;
    log.accept("Retrieving existing NCBI request " + rid + ". Pasted queries are verified locally; no query is uploaded.");
    waitForRid(key, deadline, cancelled, log);
    var request = HttpRequest.newBuilder(URI.create(endpoint + "?CMD=Get&RID=" + rid + "&FORMAT_TYPE=JSON2_S"))
      .timeout(timeout).header("User-Agent", "HiCT-NCBI-result-retrieval/1.0").GET().build();
    var subscriber = new LimitedBody(maximumBytes);
    checkCancelled(cancelled);
    requestStarting.run();
    synchronized (LAST_REQUESTS) {
      LAST_REQUESTS.remove(key); LAST_REQUESTS.put(key, System.nanoTime());
      while (LAST_REQUESTS.size() > 128) LAST_REQUESTS.remove(LAST_REQUESTS.keySet().iterator().next());
    }
    Instant started = Instant.now();
    var future = client.sendAsync(request, ignored -> subscriber);
    Path raw = directory.resolve("remote-blast.tsv"), report = directory.resolve("remote-blast.json"),
      provider = directory.resolve("remote-blast-provider.json");
    boolean succeeded = false;
    try {
      HttpResponse<byte[]> response;
      while (true) {
        checkCancelled(cancelled);
        if (System.nanoTime() >= deadline) throw new TimeoutException("NCBI result retrieval timed out. Keep RID " + rid + " and retrieve it later; no search was resubmitted.");
        try { response = future.get(200, TimeUnit.MILLISECONDS); break; }
        catch (java.util.concurrent.TimeoutException waiting) { /* Observe cancellation and the total deadline. */ }
        catch (ExecutionException failed) {
          if (failed.getCause() instanceof java.net.http.HttpTimeoutException) throw new TimeoutException(
            "NCBI result retrieval timed out. Keep RID " + rid + " and retrieve it later; no search was resubmitted.");
          if (failed.getCause() instanceof IOException io) throw io;
          throw new IOException("NCBI result download failed. Check the connection and retrieve the same RID later.");
        }
      }
      checkCancelled(cancelled);
      if (response.statusCode() != 200) throw new IOException("NCBI result retrieval returned HTTP " + response.statusCode()
        + ". Keep the RID and retry later; no search was submitted.");
      byte[] bytes = response.body();
      String beginning = new String(bytes, 0, Math.min(bytes.length, 128), StandardCharsets.UTF_8).stripLeading();
      if (!beginning.startsWith("{")) throw new IOException("NCBI request " + rid
        + " has no JSON result available. It may still be pending or may have expired. Check the RID and wait at least a minute before retrieving again.");
      var decoded = NcbiBlastJson.decode(bytes, queries, options, raw);
      checkCancelled(cancelled);
      var document = RemoteBlastRunner.reportDocument(queries, options, decoded.parsed(), querySha256, started)
        .put("provider", "NCBI Common URL API").put("retrievedRequestId", rid).put("submissionPerformed", false)
        .put("task", "Retrieved NCBI blastn request").put("lowComplexityMasking", null)
        .mergeIn(decoded.provenance());
      Files.write(provider, bytes, StandardOpenOption.CREATE_NEW);
      Files.writeString(report, document.encode(), StandardOpenOption.CREATE_NEW);
      if (Files.size(report) > RemoteBlastRunner.MAX_OUTPUT_BYTES * 2) throw new IOException("Retrieved BLAST report exceeds the size limit");
      checkCancelled(cancelled);
      succeeded = true;
      log.accept("Downloaded NCBI request " + rid + "; verified returned query labels, lengths and available aligned bases.");
      return new RemoteBlastRunner.Result(report, raw, decoded.parsed().count(), querySha256);
    } finally {
      future.cancel(true);
      subscriber.cancel();
      if (!succeeded) { Files.deleteIfExists(raw); Files.deleteIfExists(report); Files.deleteIfExists(provider); }
    }
  }

  private void waitForRid(String key, long deadline, BooleanSupplier cancelled, Consumer<String> log) throws Exception {
    boolean announced = false;
    while (true) {
      checkCancelled(cancelled);
      if (System.nanoTime() >= deadline) throw new TimeoutException("NCBI retrieval timed out while waiting for the per-request polling interval");
      final Long previous;
      synchronized (LAST_REQUESTS) { previous = LAST_REQUESTS.get(key); }
      long remaining = previous == null ? 0 : ridSpacing.toNanos() - (System.nanoTime() - previous);
      if (remaining <= 0) return;
      if (!announced) { log.accept("Waiting for NCBI's one-minute interval before retrieving this RID again."); announced = true; }
      TimeUnit.NANOSECONDS.sleep(Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(200)));
    }
  }
  private static void checkCancelled(BooleanSupplier cancelled) {
    if (cancelled.getAsBoolean() || Thread.currentThread().isInterrupted()) throw new CancellationException();
  }

  /** Bound allocation while the HTTP body arrives, rather than after ofByteArray has buffered it. */
  private static final class LimitedBody implements HttpResponse.BodySubscriber<byte[]> {
    private final long limit;
    private final ByteArrayOutputStream output = new ByteArrayOutputStream();
    private final CompletableFuture<byte[]> result = new CompletableFuture<>();
    private volatile Flow.Subscription subscription;
    LimitedBody(long limit) { this.limit = limit; }
    @Override public CompletionStage<byte[]> getBody() { return result; }
    @Override public void onSubscribe(Flow.Subscription value) { subscription = value; if (result.isDone()) value.cancel(); else value.request(1); }
    @Override public void onNext(List<ByteBuffer> buffers) {
      if (result.isDone()) { subscription.cancel(); return; }
      for (var buffer : buffers) {
        if (buffer.remaining() > limit - output.size()) {
          subscription.cancel(); result.completeExceptionally(new IOException("NCBI result exceeds the bounded download size. Reduce the original search's targets or queries.")); return;
        }
        var chunk = new byte[Math.min(8192, buffer.remaining())];
        while (buffer.hasRemaining()) {
          int count = Math.min(chunk.length, buffer.remaining()); buffer.get(chunk, 0, count); output.write(chunk, 0, count);
        }
      }
      subscription.request(1);
    }
    @Override public void onError(Throwable failure) { result.completeExceptionally(failure); }
    @Override public void onComplete() { if (!result.isDone()) result.complete(output.toByteArray()); }
    void cancel() { var value = subscription; if (value != null) value.cancel(); result.cancel(false); }
  }
}
