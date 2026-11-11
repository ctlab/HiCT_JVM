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
