package ru.itmo.ctlab.hict.hict_server.examples;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;

/** Controlled-server tests of the example downloader. Test-only code may allow http and loopback. */
class ExampleDownloaderTest {
  @TempDir Path data;
  HttpServer server;
  String base;
  final List<String> requests = new CopyOnWriteArrayList<>();
  byte[] payload;
  final List<ExampleDownloader> downloaders = new CopyOnWriteArrayList<>();

  @BeforeEach void start() throws IOException {
    payload = new byte[300_000];
    new Random(3).nextBytes(payload);
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    base = "http://127.0.0.1:" + server.getAddress().getPort();
  }

  @AfterEach void stop() {
    downloaders.forEach(ExampleDownloader::close);
    server.stop(0);
  }

  private static String sha(byte[] bytes) throws Exception {
    return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
  }

  private ExamplesCatalog.Example example(JsonObject... files) {
    var array = new JsonArray();
    for (var f : files) array.add(f);
    var body = new JsonObject().put("id", "ex1").put("title", "T").put("files", array)
      .put("openingRecipe", new JsonObject()).put("attribution", new JsonObject().put("provider", "P"))
      .put("publications", new JsonArray());
    return new ExamplesCatalog.Example("ex1", "s", "test", true, body);
  }

  private JsonObject file(String name, String path, long bytes, String sha, boolean required) {
    var f = new JsonObject().put("id", name).put("filename", name).put("url", base + path).put("required", required);
    if (bytes > 0) f.put("bytes", bytes);
    if (sha != null) f.put("sha256", sha);
    return f;
  }

  private ExampleDownloader downloader() {
    var downloader = new ExampleDownloader(data, uri -> { }, 0L);
    downloaders.add(downloader);
    return downloader;
  }

  private ExampleDownloader.Status await(ExampleDownloader d) throws Exception {
    for (int i = 0; i < 400; i++) {
      var s = d.status();
      if (s.state() != ExampleDownloader.State.RUNNING) return s;
      Thread.sleep(25);
    }
    fail("The job did not finish");
    return null;
  }

  private void serve(String path, Consumer<HttpExchange> handler) {
    server.createContext(path, exchange -> {
      requests.add(exchange.getRequestMethod() + " " + exchange.getRequestURI() + " range=" + exchange.getRequestHeaders().getFirst("Range")
        + " ifrange=" + exchange.getRequestHeaders().getFirst("If-Range"));
      try {
        handler.accept(exchange);
      } finally {
        exchange.close();
      }
    });
    server.start();
  }

  private static void send(HttpExchange e, int code, byte[] body, long declared) {
    try {
      e.sendResponseHeaders(code, declared);
      if (body != null && body.length > 0) e.getResponseBody().write(body);
    } catch (IOException ignored) {
    }
  }

  @Test void publishesPreparedReferenceAndRecordsBothPinnedAndDerivedHashes() throws Exception {
    var bytes = new java.io.ByteArrayOutputStream();
    try (var gzip = new java.util.zip.GZIPOutputStream(bytes)) {
      gzip.write(">accession.1 description\nAcgT\n>unused\nNNNN\n".getBytes(StandardCharsets.US_ASCII));
    }
    byte[] original = bytes.toByteArray();
    byte[] prepared = ">chr1\nAcgT\n".getBytes(StandardCharsets.US_ASCII);
    serve("/reference", exchange -> send(exchange, 200, original, original.length));
    var example = example(file("original.fa.gz", "/reference", original.length, sha(original), true));
    example.body().getJsonObject("openingRecipe").put("fasta", "original.fa.gz")
      .put("fastaPreparation", new JsonObject().put("filename", "matching.fa").put("sha256", sha(prepared))
        .put("records", new JsonArray().add(new JsonObject().put("source", "accession.1").put("target", "chr1").put("length", 4))));
    var downloader = downloader();
    downloader.start(example);
    assertEquals(ExampleDownloader.State.READY, await(downloader).state());
    var folder = data.resolve("examples/ex1");
    assertArrayEquals(original, Files.readAllBytes(folder.resolve("original.fa.gz")));
    assertArrayEquals(prepared, Files.readAllBytes(folder.resolve("matching.fa")));
    var manifest = new JsonObject(Files.readString(folder.resolve("manifest.json")));
    assertEquals(2, manifest.getJsonArray("files").size());
    assertEquals(sha(original), manifest.getJsonArray("files").getJsonObject(0).getString("sha256"));
    assertEquals(sha(prepared), manifest.getJsonArray("files").getJsonObject(1).getString("sha256"));
    assertEquals(example.body().getJsonObject("openingRecipe"), manifest.getJsonObject("openingRecipe"));
  }

  private void plainServer() {
    serve("/f", e -> {
      e.getResponseHeaders().add("ETag", "\"v1\"");
      var range = e.getRequestHeaders().getFirst("Range");
      if (range != null && range.startsWith("bytes=")) {
        int from = Integer.parseInt(range.substring(6, range.indexOf('-')));
        if (from >= payload.length) { send(e, 416, null, -1); return; }
        e.getResponseHeaders().add("Content-Range", "bytes " + from + "-" + (payload.length - 1) + "/" + payload.length);
        send(e, 206, java.util.Arrays.copyOfRange(payload, from, payload.length), payload.length - from);
      } else {
        send(e, 200, payload, payload.length);
      }
    });
  }

  @Test void closingDownloaderKeepsItsManagedVertxContextAlive() throws Exception {
    final var vertx = io.vertx.core.Vertx.vertx();
    try {
      final var completed = new java.util.concurrent.CompletableFuture<Void>();
      vertx.runOnContext(ignored -> {
        try {
          final var downloader = downloader();
          downloader.close();
          vertx.runOnContext(next -> completed.complete(null));
        } catch (Throwable failure) { completed.completeExceptionally(failure); }
      });
      completed.get(5, java.util.concurrent.TimeUnit.SECONDS);
    } finally { vertx.close().toCompletionStage().toCompletableFuture().get(5, java.util.concurrent.TimeUnit.SECONDS); }
  }

  @Test void downloadsVerifiesAndWritesAManifestWithAttribution() throws Exception {
    plainServer();
    var d = downloader();
    d.start(example(file("a.bin", "/f", payload.length, sha(payload), true)));
    var s = await(d);
    assertEquals(ExampleDownloader.State.READY, s.state(), s.error());
    assertArrayEquals(payload, Files.readAllBytes(data.resolve("examples/ex1/a.bin")));
    assertFalse(Files.exists(data.resolve("examples/ex1/a.bin.part")));
    var manifest = new JsonObject(Files.readString(data.resolve("examples/ex1/manifest.json")));
    assertEquals("P", manifest.getJsonObject("attribution").getString("provider"));
    assertEquals(sha(payload), manifest.getJsonArray("files").getJsonObject(0).getString("sha256"));
    assertTrue(s.allRequiredPresent());
  }

  @Test void aWrongChecksumDiscardsTheFile() throws Exception {
    plainServer();
    var d = downloader();
    d.start(example(file("a.bin", "/f", payload.length, "0".repeat(64), true)));
    var s = await(d);
    assertEquals(ExampleDownloader.State.FAILED, s.state());
    assertTrue(s.error().contains("failed verification"), s.error());
    assertFalse(Files.exists(data.resolve("examples/ex1/a.bin")));
    assertFalse(Files.exists(data.resolve("examples/ex1/a.bin.part")));
  }

  @Test void aWrongLengthIsRefusedBeforeAnythingIsKept() throws Exception {
    plainServer();
    var d = downloader();
    d.start(example(file("a.bin", "/f", 10, null, true)));
    var s = await(d);
    assertEquals(ExampleDownloader.State.FAILED, s.state());
    assertTrue(s.error().contains("catalog says 10"), s.error());
    assertFalse(Files.exists(data.resolve("examples/ex1/a.bin")));
  }

  @Test void resumesWithRangeAndIfRangeAfterAnInterruptedDownload() throws Exception {
    plainServer();
    Files.createDirectories(data.resolve("examples/ex1"));
    Files.write(data.resolve("examples/ex1/a.bin.part"), java.util.Arrays.copyOf(payload, 100_000));
    Files.writeString(data.resolve("examples/ex1/a.bin.part.json"), new JsonObject().put("url", base + "/f").put("etag", "\"v1\"").encode());
    var d = downloader();
    d.start(example(file("a.bin", "/f", payload.length, sha(payload), true)));
    var s = await(d);
    assertEquals(ExampleDownloader.State.READY, s.state(), s.error());
    assertArrayEquals(payload, Files.readAllBytes(data.resolve("examples/ex1/a.bin")));
    assertTrue(requests.stream().anyMatch(r -> r.contains("range=bytes=100000-") && r.contains("ifrange=\"v1\"")), requests.toString());
  }

  @Test void resumesChunkedPartialResponseWithoutContentLength() throws Exception {
    serve("/f", exchange -> {
      exchange.getResponseHeaders().add("Content-Range", "bytes 100000-" + (payload.length - 1) + "/" + payload.length);
      send(exchange, 206, java.util.Arrays.copyOfRange(payload, 100000, payload.length), 0);
    });
    Files.createDirectories(data.resolve("examples/ex1"));
    Files.write(data.resolve("examples/ex1/a.bin.part"), java.util.Arrays.copyOf(payload, 100000));
    Files.writeString(data.resolve("examples/ex1/a.bin.part.json"), new JsonObject().put("url", base + "/f").encode());
    var downloader = downloader();
    downloader.start(example(file("a.bin", "/f", payload.length, sha(payload), true)));
    assertEquals(ExampleDownloader.State.READY, await(downloader).state());
    assertArrayEquals(payload, Files.readAllBytes(data.resolve("examples/ex1/a.bin")));
  }

  @Test void connectsToPinnedAddressWithOriginHost() throws Exception {
    serve("/f", exchange -> {
      assertEquals("origin.invalid:" + server.getAddress().getPort(), exchange.getRequestHeaders().getFirst("Host"));
      send(exchange, 200, payload, payload.length);
    });
    var policy = new ExampleDownloader.UrlPolicy() {
      @Override public void check(URI uri) { }
      @Override public java.net.InetAddress resolve(URI uri) throws IOException {
        return java.net.InetAddress.getByName("127.0.0.1");
      }
    };
    var downloader = new ExampleDownloader(data, policy, 0);
    downloaders.add(downloader);
    var spec = file("a.bin", "/f", payload.length, sha(payload), true);
    spec.put("url", "http://origin.invalid:" + server.getAddress().getPort() + "/f");
    downloader.start(example(spec));
    var result = await(downloader);
    assertEquals(ExampleDownloader.State.READY, result.state(), result.error());
  }

  @Test void aServerThatIgnoresRangeRestartsFromZero() throws Exception {
    serve("/f", e -> send(e, 200, payload, payload.length));
    Files.createDirectories(data.resolve("examples/ex1"));
    Files.write(data.resolve("examples/ex1/a.bin.part"), new byte[1000]);
    Files.writeString(data.resolve("examples/ex1/a.bin.part.json"), new JsonObject().put("url", base + "/f").put("etag", "\"old\"").encode());
    var d = downloader();
    d.start(example(file("a.bin", "/f", payload.length, sha(payload), true)));
    var s = await(d);
    assertEquals(ExampleDownloader.State.READY, s.state(), s.error());
    assertArrayEquals(payload, Files.readAllBytes(data.resolve("examples/ex1/a.bin")));
  }

  @Test void aRefusedRangeRestartsAndAWrongResumePositionFails() throws Exception {
    // 416 once, then a full answer.
    var calls = new java.util.concurrent.atomic.AtomicInteger();
    serve("/f", e -> {
      if (e.getRequestHeaders().getFirst("Range") != null && calls.getAndIncrement() == 0) send(e, 416, null, -1);
      else send(e, 200, payload, payload.length);
    });
    Files.createDirectories(data.resolve("examples/ex1"));
    Files.write(data.resolve("examples/ex1/a.bin.part"), new byte[500]);
    Files.writeString(data.resolve("examples/ex1/a.bin.part.json"), new JsonObject().put("url", base + "/f").encode());
    var d = downloader();
    d.start(example(file("a.bin", "/f", payload.length, sha(payload), true)));
    assertEquals(ExampleDownloader.State.READY, await(d).state());
  }

  @Test void aResumeAtTheWrongPositionIsRejected() throws Exception {
    serve("/f", e -> {
      e.getResponseHeaders().add("Content-Range", "bytes 0-" + (payload.length - 1) + "/" + payload.length);
      send(e, 206, payload, payload.length);
    });
    Files.createDirectories(data.resolve("examples/ex1"));
    Files.write(data.resolve("examples/ex1/a.bin.part"), new byte[500]);
    Files.writeString(data.resolve("examples/ex1/a.bin.part.json"), new JsonObject().put("url", base + "/f").encode());
    var d = downloader();
    d.start(example(file("a.bin", "/f", payload.length, null, true)));
    var s = await(d);
    assertEquals(ExampleDownloader.State.FAILED, s.state());
    assertTrue(s.error().contains("different position"), s.error());
  }

  @Test void anUnknownLengthStillCompletesWhenTheCatalogHasNoSize() throws Exception {
    serve("/f", e -> send(e, 200, payload, 0)); // 0 = chunked, no Content-Length
    var d = downloader();
    d.start(example(file("a.bin", "/f", 0, null, true)));
    var s = await(d);
    assertEquals(ExampleDownloader.State.READY, s.state(), s.error());
    assertArrayEquals(payload, Files.readAllBytes(data.resolve("examples/ex1/a.bin")));
  }

  @org.junit.jupiter.api.RepeatedTest(20) void redirectsAreFollowedAndTheirLimitIsEnforced() throws Exception {
    serve("/", e -> {
      var path = e.getRequestURI().getPath();
      if (path.equals("/start")) { e.getResponseHeaders().add("Location", "/final"); send(e, 302, null, -1); }
      else if (path.equals("/final")) send(e, 200, payload, payload.length);
      else { e.getResponseHeaders().add("Location", path + "x"); send(e, 302, null, -1); }
    });
    var d = downloader();
    d.start(example(file("a.bin", "/start", payload.length, null, true)));
    var followed = await(d);
    assertEquals(ExampleDownloader.State.READY, followed.state(), followed.error());
    assertArrayEquals(payload, Files.readAllBytes(data.resolve("examples/ex1/a.bin")));
    var loop = downloader();
    loop.start(example(file("b.bin", "/loop", 0, null, true)));
    var s = await(loop);
    assertEquals(ExampleDownloader.State.FAILED, s.state());
    assertTrue(s.error().contains("Too many redirects"), s.error());
    assertEquals(6, requests.stream().filter(r -> r.contains(" /loop")).count(), requests.toString());
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(ints = {0, 32, 131072, -1})
  void redirectBodiesAreDiscardedWithoutContaminatingTheDownloadedFile(int bodySize) throws Exception {
    final var redirectBody = new byte[bodySize < 0 ? 10000 : bodySize];
    serve("/", exchange -> {
      if (exchange.getRequestURI().getPath().equals("/start")) {
        exchange.getResponseHeaders().add("Location", "/final");
        send(exchange, 302, redirectBody, bodySize == 0 ? -1 : bodySize < 0 ? 0 : bodySize);
      } else send(exchange, 200, payload, payload.length);
    });
    var downloader = downloader();
    downloader.start(example(file("a.bin", "/start", payload.length, sha(payload), true)));
    var result = await(downloader);
    assertEquals(ExampleDownloader.State.READY, result.state(), result.error());
    assertArrayEquals(payload, Files.readAllBytes(data.resolve("examples/ex1/a.bin")));
  }

  @Test void aMissingRedirectLocationFailsWithoutBreakingTheNextDownload() throws Exception {
    serve("/", exchange -> {
      if (exchange.getRequestURI().getPath().equals("/bad")) send(exchange, 302, null, -1);
      else send(exchange, 200, payload, payload.length);
    });
    var downloader = downloader();
    downloader.start(example(file("bad.bin", "/bad", 0, null, true)));
    var failed = await(downloader);
    assertEquals(ExampleDownloader.State.FAILED, failed.state());
    assertTrue(failed.error().contains("no Location"), failed.error());
    downloader.start(example(file("good.bin", "/good", payload.length, sha(payload), true)));
    var retried = await(downloader);
    assertEquals(ExampleDownloader.State.READY, retried.state(), retried.error());
  }

  @Test void theUrlPolicyChecksEveryAddress() {
    var policy = ExampleDownloader.UrlPolicy.production();
    for (var bad : List.of("http://example.org/x", "https://127.0.0.1/x", "https://10.1.2.3/x", "https://192.168.0.5/x",
      "https://169.254.169.254/latest", "https://[::1]/x", "https://user:pw@example.org/x", "https://example.org:8443/x",
      "https://100.64.0.1/x", "ftp://example.org/x")) {
      assertThrows(IOException.class, () -> policy.check(URI.create(bad)), bad);
    }
  }

  @Test void aRedirectToANonPublicAddressIsRefusedByTheProductionPolicy() throws Exception {
    serve("/", e -> { e.getResponseHeaders().add("Location", "https://169.254.169.254/latest/meta-data"); send(e, 302, null, -1); });
    // Allow only this server's own http address, as a test policy, but keep the real rule for redirect targets.
    var production = ExampleDownloader.UrlPolicy.production();
    var d = new ExampleDownloader(data, uri -> { if (!uri.getHost().equals("127.0.0.1")) production.check(uri); }, 0L);
    d.start(example(file("a.bin", "/x", 0, null, true)));
    var s = await(d);
    assertEquals(ExampleDownloader.State.FAILED, s.state());
    assertTrue(s.error().contains("non-public") || s.error().contains("Only https"), s.error());
  }

  @Test void notEnoughDiskSpaceIsReportedBeforeWriting() throws Exception {
    plainServer();
    var d = new ExampleDownloader(data, uri -> { }, Long.MAX_VALUE / 4);
    d.start(example(file("a.bin", "/f", payload.length, null, true)));
    var s = await(d);
    assertEquals(ExampleDownloader.State.FAILED, s.state());
    assertTrue(s.error().contains("Not enough free disk space"), s.error());
    assertFalse(Files.exists(data.resolve("examples/ex1/a.bin")));
  }

  @Test void anOptionalFileMayFailButARequiredOneMayNot() throws Exception {
    serve("/", e -> {
      if (e.getRequestURI().getPath().equals("/ok")) send(e, 200, payload, payload.length); else send(e, 404, null, -1);
    });
    var d = downloader();
    d.start(example(file("a.bin", "/ok", payload.length, null, true), file("b.opt", "/missing", 0, null, false)));
    var s = await(d);
    assertEquals(ExampleDownloader.State.READY, s.state(), s.error());
    assertEquals(1, s.skippedOptional().size());
    var d2 = downloader();
    d2.start(example(file("c.bin", "/missing", 0, null, true)));
    assertEquals(ExampleDownloader.State.FAILED, await(d2).state());
  }

  @Test void cancelKeepsThePartialFileForAResume() throws Exception {
    var started = new java.util.concurrent.CountDownLatch(1);
    serve("/f", e -> {
      try {
        e.sendResponseHeaders(200, payload.length);
        var out = e.getResponseBody();
        for (int i = 0; i < payload.length; i += 10_000) {
          out.write(payload, i, Math.min(10_000, payload.length - i));
          out.flush();
          started.countDown();
          Thread.sleep(40);
        }
      } catch (Exception ignored) {
      }
    });
    var d = downloader();
    d.start(example(file("a.bin", "/f", payload.length, null, true)));
    assertTrue(started.await(5, java.util.concurrent.TimeUnit.SECONDS));
    for (int attempt = 0; attempt < 100 && !Files.exists(data.resolve("examples/ex1/a.bin.part")); attempt++) Thread.sleep(10);
    d.cancel();
    var s = await(d);
    assertEquals(ExampleDownloader.State.CANCELLED, s.state());
    assertFalse(Files.exists(data.resolve("examples/ex1/a.bin")));
    assertTrue(Files.exists(data.resolve("examples/ex1/a.bin.part")));
  }

  @Test void oneJobAtATimeAndTheSameExampleJoins() throws Exception {
    serve("/f", e -> { try { Thread.sleep(300); } catch (InterruptedException ignored) { } send(e, 200, payload, payload.length); });
    var d = downloader();
    var first = d.start(example(file("a.bin", "/f", payload.length, null, true)));
    assertEquals(ExampleDownloader.State.RUNNING, first.state());
    assertEquals(ExampleDownloader.State.RUNNING, d.start(example(file("a.bin", "/f", payload.length, null, true))).state());
    var other = new ExamplesCatalog.Example("ex2", "s", "t", true, example(file("a.bin", "/f", 1, null, true)).body());
    assertThrows(IllegalStateException.class, () -> d.start(other));
    await(d);
  }

  @Test void fileNamesCannotEscapeTheExampleFolder() throws Exception {
    plainServer();
    var d = downloader();
    d.start(example(file("../evil.bin", "/f", payload.length, null, true)));
    var s = await(d);
    assertEquals(ExampleDownloader.State.FAILED, s.state());
    assertFalse(Files.exists(data.resolve("examples/evil.bin")));
  }
}
