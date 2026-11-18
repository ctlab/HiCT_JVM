package ru.itmo.ctlab.hict.hict_server.handlers.conversion;

import com.sun.net.httpserver.HttpServer;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.itmo.ctlab.hict.hict_library.assembly.SequenceSearchQueries;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.junit.jupiter.api.Assertions.*;

class NcbiBlastResultRetrieverTest {
  @TempDir Path directory;
  private byte[] resource(String name) throws Exception {
    try (var stream = getClass().getResourceAsStream("/remote-blast/" + name)) { return stream.readAllBytes(); }
  }
  private List<SequenceSearchQueries.Query> queries() throws Exception {
    return SequenceSearchQueries.parse(new String(resource("ncbi-public-160.fasta"), StandardCharsets.UTF_8));
  }
  private RemoteBlastOptions options() {
    return new RemoteBlastOptions("16S_ribosomal_RNA", 60, new BlastSearchRunner.Options(true, 1e-5, 70, 50, 50), "FR9P7TGF016");
  }
  private final String sha = "1".repeat(64);
  private static final class Provider implements AutoCloseable {
    final HttpServer server;
    final ExecutorService executor = Executors.newCachedThreadPool();
    final List<String> methods = new CopyOnWriteArrayList<>(), requests = new CopyOnWriteArrayList<>();
    final List<byte[]> bodies = new CopyOnWriteArrayList<>();
    final CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
    volatile byte[] response; volatile int status = 200; volatile boolean hold;
    Provider(byte[] response) throws Exception {
      this.response = response;
      server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0); server.setExecutor(executor);
      server.createContext("/Blast.cgi", exchange -> {
        methods.add(exchange.getRequestMethod()); requests.add(exchange.getRequestURI().toString());
        bodies.add(exchange.getRequestBody().readAllBytes());
        entered.countDown();
        try {
          if (hold) release.await(10, TimeUnit.SECONDS);
          exchange.sendResponseHeaders(status, this.response.length);
          exchange.getResponseBody().write(this.response);
        } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
        catch (IOException cancelled) { /* The owned HTTP request was cancelled by the test. */ }
        finally { exchange.close(); }
      }); server.start();
    }
    URI endpoint() { return URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/Blast.cgi"); }
    NcbiBlastResultRetriever client(Duration spacing, long maximum) {
      return new NcbiBlastResultRetriever(HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).followRedirects(HttpClient.Redirect.NEVER).build(), endpoint(), spacing, maximum);
    }
    @Override public void close() { release.countDown(); server.stop(0); executor.shutdownNow(); }
  }

  @Test void downloadsOnlyAnExistingRidAndPublishesVerifiedExternalResults() throws Exception {
    try (var provider = new Provider(resource("ncbi-public-160-json2.json"))) {
      var result = provider.client(Duration.ZERO, RemoteBlastRunner.MAX_OUTPUT_BYTES)
        .retrieve(directory, queries(), options(), sha, ignored -> {}, () -> false, Duration.ofSeconds(3), () -> {});
      assertEquals(List.of("GET"), provider.methods);
      assertEquals("/Blast.cgi?CMD=Get&RID=FR9P7TGF016&FORMAT_TYPE=JSON2_S", provider.requests.getFirst());
      assertEquals(0, provider.bodies.getFirst().length);
      assertEquals(10, result.hits());
      var report = new JsonObject(Files.readString(result.report()));
      assertEquals(false, report.getBoolean("submissionPerformed"));
      assertEquals("FR9P7TGF016", report.getString("retrievedRequestId"));
      assertEquals("FULL_QUERY", report.getJsonArray("queryVerification").getJsonObject(0).getString("scope"));
      assertEquals(sha, report.getString("querySha256"));
      assertTrue(Files.exists(directory.resolve("remote-blast-provider.json")));
    }
  }

  @Test void theSharedRunnerRetrievalPathNeverNeedsOrStartsABlastExecutable() throws Exception {
    try (var provider = new Provider(resource("ncbi-public-160-json2.json"))) {
      var startedProcesses = new AtomicInteger();
      var runner = new RemoteBlastRunner(() -> provider.client(Duration.ZERO, RemoteBlastRunner.MAX_OUTPUT_BYTES));
      var result = runner.run(null, directory, queries(), options(), ignored -> {}, process -> { if (process != null) startedProcesses.incrementAndGet(); },
        () -> false, Duration.ofSeconds(3), Duration.ZERO);
      assertEquals(0, startedProcesses.get()); assertEquals(10, result.hits());
      assertFalse(Files.exists(directory.resolve("remote-blast.log")));
      assertTrue(Files.readString(directory.resolve("queries.fasta")).startsWith(">query_1\n"));
      assertEquals(List.of("GET"), provider.methods);
    }
  }

  @Test void rejectsOversizedResponsesDuringDownloadAndPublishesNoPartialReport() throws Exception {
    try (var provider = new Provider(new byte[8192])) {
      var error = assertThrows(IOException.class, () -> provider.client(Duration.ZERO, 1024)
        .retrieve(directory, queries(), options(), sha, ignored -> {}, () -> false, Duration.ofSeconds(3), () -> {}));
      assertTrue(error.getMessage().contains("bounded download size"), error.getMessage());
      assertFalse(Files.exists(directory.resolve("remote-blast.json")));
      assertFalse(Files.exists(directory.resolve("remote-blast.tsv")));
    }
  }

  @Test void cancellationStopsAWaitingRequestWithoutSubmittingAnything() throws Exception {
    try (var provider = new Provider(resource("ncbi-public-160-json2.json"))) {
      provider.hold = true;
      var cancelled = new AtomicBoolean();
      var executor = Executors.newSingleThreadExecutor();
      try {
        var queries = queries();
        var future = executor.submit(() -> provider.client(Duration.ZERO, RemoteBlastRunner.MAX_OUTPUT_BYTES)
          .retrieve(directory, queries, options(), sha, ignored -> {}, cancelled::get, Duration.ofSeconds(5), () -> {}));
        assertTrue(provider.entered.await(3, TimeUnit.SECONDS));
        cancelled.set(true);
        var failure = assertThrows(ExecutionException.class, () -> future.get(3, TimeUnit.SECONDS));
        assertInstanceOf(CancellationException.class, failure.getCause());
        assertEquals(List.of("GET"), provider.methods); assertEquals(0, provider.bodies.getFirst().length);
        assertFalse(Files.exists(directory.resolve("remote-blast.json")));
      } finally { provider.release.countDown(); executor.shutdownNow(); }
    }
  }

  @Test void timeoutAndPendingOrHttpErrorsRetainNoIncompleteOutput() throws Exception {
    try (var provider = new Provider(resource("ncbi-public-160-json2.json"))) {
      provider.hold = true;
      assertThrows(TimeoutException.class, () -> provider.client(Duration.ZERO, RemoteBlastRunner.MAX_OUTPUT_BYTES)
        .retrieve(directory, queries(), options(), sha, ignored -> {}, () -> false, Duration.ofMillis(100), () -> {}));
      provider.hold = false; provider.release.countDown();
      provider.response = "<html>Request pending</html>".getBytes(StandardCharsets.UTF_8);
      var pending = assertThrows(IOException.class, () -> provider.client(Duration.ZERO, RemoteBlastRunner.MAX_OUTPUT_BYTES)
        .retrieve(directory, queries(), options(), sha, ignored -> {}, () -> false, Duration.ofSeconds(3), () -> {}));
      assertTrue(pending.getMessage().contains("no JSON result"));
      provider.status = 429;
      var busy = assertThrows(IOException.class, () -> provider.client(Duration.ZERO, RemoteBlastRunner.MAX_OUTPUT_BYTES)
        .retrieve(directory, queries(), options(), sha, ignored -> {}, () -> false, Duration.ofSeconds(3), () -> {}));
      assertTrue(busy.getMessage().contains("429"));
      assertFalse(Files.exists(directory.resolve("remote-blast.json")));
      assertFalse(Files.exists(directory.resolve("remote-blast.tsv")));
      assertTrue(provider.methods.stream().allMatch("GET"::equals));
    }
  }

  @Test void waitsForTheRidIntervalAndCanCancelBeforeMakingAnotherRequest() throws Exception {
    try (var provider = new Provider(resource("ncbi-public-160-json2.json"))) {
      var client = provider.client(Duration.ofMinutes(1), RemoteBlastRunner.MAX_OUTPUT_BYTES);
      client.retrieve(directory, queries(), options(), sha, ignored -> {}, () -> false, Duration.ofSeconds(3), () -> {});
      Files.delete(directory.resolve("remote-blast.json")); Files.delete(directory.resolve("remote-blast.tsv")); Files.delete(directory.resolve("remote-blast-provider.json"));
      var cancelled = new AtomicBoolean();
      var waiting = new CountDownLatch(1);
      var executor = Executors.newSingleThreadExecutor();
      try {
        var queries = queries();
        var future = executor.submit(() -> client.retrieve(directory, queries, options(), sha, line -> { if (line.startsWith("Waiting for NCBI")) waiting.countDown(); }, cancelled::get, Duration.ofSeconds(3), () -> {}));
        assertTrue(waiting.await(2, TimeUnit.SECONDS)); cancelled.set(true);
        var failure = assertThrows(ExecutionException.class, () -> future.get(3, TimeUnit.SECONDS));
        assertInstanceOf(CancellationException.class, failure.getCause());
        assertEquals(1, provider.methods.size());
      } finally { executor.shutdownNow(); }
    }
  }

  @Test void refusesMalformedRequestIdsBeforeAnyNetworkAccess() throws Exception {
    try (var provider = new Provider(resource("ncbi-public-160-json2.json"))) {
      var bad = new RemoteBlastOptions("nt", 60, options().alignment(), "RID&CMD=Put&QUERY=ACGT");
      assertThrows(IllegalArgumentException.class, () -> provider.client(Duration.ZERO, 1024)
        .retrieve(directory, queries(), bad, sha, ignored -> {}, () -> false, Duration.ofSeconds(3), () -> {}));
      assertTrue(provider.methods.isEmpty());
    }
  }

  @Test void aMalformedLateHitRemovesEveryPartialOutput() throws Exception {
    var document = new JsonObject(new String(resource("ncbi-public-160-json2.json"), StandardCharsets.UTF_8));
    var hits = document.getJsonArray("BlastOutput2").getJsonObject(0).getJsonObject("report")
      .getJsonObject("results").getJsonObject("search").getJsonArray("hits");
    hits.getJsonObject(hits.size() - 1).getJsonArray("hsps").getJsonObject(0).put("query_to", 100000);
    try (var provider = new Provider(document.toBuffer().getBytes())) {
      assertThrows(IOException.class, () -> provider.client(Duration.ZERO, RemoteBlastRunner.MAX_OUTPUT_BYTES)
        .retrieve(directory, queries(), options(), sha, ignored -> {}, () -> false, Duration.ofSeconds(3), () -> {}));
      for (String name : List.of("remote-blast.json", "remote-blast.tsv", "remote-blast-provider.json"))
        assertFalse(Files.exists(directory.resolve(name)), name);
      assertEquals(List.of("GET"), provider.methods);
    }
  }
}
