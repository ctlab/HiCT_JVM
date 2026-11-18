package ru.itmo.ctlab.hict.hict_server.handlers.fileop;

import io.vertx.core.Vertx;
import io.vertx.ext.web.Router;
import org.junit.jupiter.api.Test;
import java.io.IOException;
import java.net.URI;
import java.net.http.*;
import java.nio.file.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import static org.junit.jupiter.api.Assertions.*;

class TemporaryFastaDownloadTest {
  @Test void disconnectDuringTransferRemovesThePreparedTemporaryFile() throws Exception {
    var vertx = Vertx.vertx();
    var temporary = new AtomicReference<Path>();
    try {
      var router = Router.router(vertx);
      router.get("/").handler(context -> {
        var download = new TemporaryFastaDownload(context);
        vertx.<Path>executeBlocking(() -> download.write(output -> {
          try { temporary.set(path(download)); }
          catch (Exception error) { throw new IOException(error); }
          var block = new byte[65536];
          java.util.Arrays.fill(block, (byte) 'A');
          for (int i = 0; i < 128; i++) output.write(block);
        })).onSuccess(download::send).onFailure(context::fail);
      });
      var server = vertx.createHttpServer().requestHandler(router).listen(0)
        .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
      try (var socket = new java.net.Socket()) {
        socket.setReceiveBufferSize(1024);
        socket.connect(new java.net.InetSocketAddress("127.0.0.1", server.actualPort()));
        socket.setSoTimeout(5000);
        socket.getOutputStream().write("GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"
          .getBytes(java.nio.charset.StandardCharsets.US_ASCII));
        socket.getOutputStream().flush();
        var input = socket.getInputStream();
        var header = new StringBuilder();
        while (!header.toString().endsWith("\r\n\r\n")) {
          int next = input.read();
          assertTrue(next >= 0, "Download ended before headers");
          assertTrue(header.length() < 8192, "Unexpected download headers");
          header.append((char) next);
        }
        assertTrue(header.toString().startsWith("HTTP/1.1 200"));
        assertEquals('A', input.read());
        assertNotNull(temporary.get());
        assertTrue(Files.exists(temporary.get()));
      }
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (Files.exists(temporary.get()) && System.nanoTime() < deadline) Thread.sleep(5);
      assertFalse(Files.exists(temporary.get()), "Interrupted transfer must remove its prepared FASTA");
    } finally { vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS); }
  }

  @Test void disconnectDuringPreparationStopsTheWriterAndRemovesItsTemporaryFile() throws Exception {
    var vertx = Vertx.vertx();
    var scheduler = new ru.itmo.ctlab.hict.hict_server.concurrent.RequestTaskScheduler(vertx,
      new ru.itmo.ctlab.hict.hict_server.concurrent.RequestTaskScheduler.SchedulerConfig(10, 8, 30, java.util.Map.of()));
    var entered = new CountDownLatch(1);
    var finished = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    var temporary = new AtomicReference<Path>();
    try {
      var router = Router.router(vertx);
      router.get("/").handler(context -> {
        var download = new TemporaryFastaDownload(context);
        scheduler.submit(context,
          ru.itmo.ctlab.hict.hict_server.concurrent.RequestTaskScheduler.RequestPriority.EXPORT,
          ru.itmo.ctlab.hict.hict_server.concurrent.RequestTaskScheduler.CancellationDomain.EXPORT,
          () -> {
            try {
              return download.write(output -> {
                try { temporary.set(path(download)); }
                catch (Exception error) { throw new IOException(error); }
                output.write(new byte[65536]);
                entered.countDown();
                try { release.await(); }
                catch (InterruptedException cancelled) {
                  Thread.currentThread().interrupt();
                  throw new java.io.InterruptedIOException("Export client disconnected");
                }
              });
            } finally { finished.countDown(); }
          }, download::send, download::close);
      });
      var server = vertx.createHttpServer().requestHandler(router).listen(0)
        .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
      try (var socket = new java.net.Socket("127.0.0.1", server.actualPort())) {
        socket.getOutputStream().write("GET / HTTP/1.1\r\nHost: localhost\r\n\r\n".getBytes(java.nio.charset.StandardCharsets.US_ASCII));
        socket.getOutputStream().flush();
        assertTrue(entered.await(5, TimeUnit.SECONDS));
      }
      assertTrue(finished.await(3, TimeUnit.SECONDS), "A disconnected export must stop preparing its FASTA");
      assertNotNull(temporary.get());
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (Files.exists(temporary.get()) && System.nanoTime() < deadline) Thread.sleep(5);
      assertFalse(Files.exists(temporary.get()), "Disconnected preparation must remove its temporary FASTA");
    } finally {
      release.countDown();
      scheduler.close();
      vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
    }
  }

  private static Path path(TemporaryFastaDownload download) throws Exception {
    var field = TemporaryFastaDownload.class.getDeclaredField("path");
    field.setAccessible(true);
    return (Path) field.get(download);
  }

  @Test void sendsCompleteBytesAndCleansSuccessFailureAndCancellation() throws Exception {
    var vertx = Vertx.vertx();
    try (var client = HttpClient.newHttpClient()) {
      var temporary = new AtomicReference<Path>();
      var router = Router.router(vertx);
      byte[] expected = (">masked\n" + "aCGTryMKSWBDHVN\n".repeat(100000)).getBytes();
      router.get("/:kind").handler(context -> {
        var download = new TemporaryFastaDownload(context);
        vertx.<Path>executeBlocking(promise -> {
          try { promise.complete(download.write(output -> {
          try { temporary.set(path(download)); }
          catch (Exception error) { throw new IOException(error); }
          output.write(expected);
          if (context.pathParam("kind").equals("failure")) throw new IOException("Expected export failure");
          if (context.pathParam("kind").equals("cancel")) download.close();
        })); } catch (Exception error) { promise.fail(error); }
        }).onSuccess(file -> {
          if (context.pathParam("kind").equals("cancel")) context.response().setStatusCode(409).end();
          else download.send(file);
        }).onFailure(context::fail);
      });
      var server = vertx.createHttpServer().requestHandler(router).listen(0)
        .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
      for (String kind : new String[]{"success", "failure", "cancel"}) {
        var response = client.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + server.actualPort() + "/" + kind))
          .timeout(java.time.Duration.ofSeconds(10)).build(), HttpResponse.BodyHandlers.ofByteArray());
        assertEquals(kind.equals("success") ? 200 : kind.equals("failure") ? 500 : 409, response.statusCode());
        if (kind.equals("success")) assertArrayEquals(expected, response.body());
        assertNotNull(temporary.get());
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (Files.exists(temporary.get()) && System.nanoTime() < deadline) Thread.sleep(5);
        assertFalse(Files.exists(temporary.get()), "Temporary FASTA must not survive a completed/failed/cancelled download");
      }
    } finally { vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS); }
  }
}
