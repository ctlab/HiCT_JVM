package ru.itmo.ctlab.hict.hict_server;

import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.itmo.ctlab.hict.fixtures.ReinforcementHdf5Fixture;
import ru.itmo.ctlab.hict.hict_library.assembly.FASTAProcessor;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_server.handlers.fileop.FileOpHandlersHolder;
import ru.itmo.ctlab.hict.hict_server.util.shareable.ShareableWrappers;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.*;

class MainVerticleShutdownTest {
  @TempDir Path directory;

  @Test
  void stoppingReleasesBothPublishedMapsAndTheirGzipIndexes() throws Exception {
    final var primaryPath = ReinforcementHdf5Fixture.create(directory, "primary", List.of(4L, 10L));
    final var secondaryPath = ReinforcementHdf5Fixture.create(directory, "secondary", List.of(4L, 10L));
    final var fasta = directory.resolve("reference.fa.gz");
    try (var compressed = new GZIPOutputStream(Files.newOutputStream(fasta))) {
      compressed.write(">tiny\nACGTRYMKSWBDHVNAC\n".getBytes(StandardCharsets.US_ASCII));
    }
    final var vertx = Vertx.vertx();
    try (var primary = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(primaryPath, 1, 2));
         var secondary = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(secondaryPath, 1, 2))) {
      primary.getFastaProcessor().readRegion(fasta, 0, 17);
      secondary.getFastaProcessor().readRegion(fasta, 0, 17);
      final var indexes = List.of(indexPath(primary), indexPath(secondary));
      assertTrue(indexes.stream().allMatch(Files::exists));
      final var map = vertx.sharedData().getLocalMap("hict_server");
      map.put("chunkedFile", new ShareableWrappers.ChunkedFileWrapper(primary));
      map.put("chunkedFileSecondary", new ShareableWrappers.ChunkedFileWrapper(secondary));
      map.put("openedFilename", "primary.hict.hdf5");
      final var application = application(new FileOpHandlersHolder(vertx));
      final var deployment = vertx.deployVerticle(application).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
      vertx.undeploy(deployment).toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
      assertTrue(indexes.stream().noneMatch(Files::exists), "Shutdown must remove private gzip indexes");
      assertNull(map.get("chunkedFile"));
      assertNull(map.get("chunkedFileSecondary"));
      assertNull(map.get("openedFilename"));
      assertTrue(Files.exists(fasta));
    } finally {
      vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
    }
  }

  @Test
  void cleanupFailureCompletesTheStopPromiseExceptionally() throws Exception {
    final var vertx = Vertx.vertx();
    try {
      final var operations = new FileOpHandlersHolder(vertx) {
        @Override public JsonObject closeSession() {
          throw new IllegalStateException("Simulated session cleanup failure");
        }
      };
      final var deployment = vertx.deployVerticle(application(operations))
        .toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
      final var failure = assertThrows(ExecutionException.class, () ->
        vertx.undeploy(deployment).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS));
      assertEquals("Simulated session cleanup failure", failure.getCause().getMessage());
    } finally {
      vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
    }
  }

  private static MainVerticle application(FileOpHandlersHolder operations) throws Exception {
    final var application = new MainVerticle() {
      @Override public void start(Promise<Void> promise) { promise.complete(); }
    };
    final var field = MainVerticle.class.getDeclaredField("fileOperations");
    field.setAccessible(true);
    field.set(application, operations);
    return application;
  }

  private static Path indexPath(ChunkedFile file) throws Exception {
    final var field = FASTAProcessor.class.getDeclaredField("sourceIndex");
    field.setAccessible(true);
    final var source = field.get(file.getFastaProcessor());
    final var path = source.getClass().getDeclaredField("indexedPath");
    path.setAccessible(true);
    return (Path) path.get(source);
  }
}
