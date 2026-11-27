package ru.itmo.ctlab.hict.hict_server.handlers.conversion;

import ch.systemsx.cisd.hdf5.HDF5Factory;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.HttpException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.itmo.ctlab.hict.fixtures.ReinforcementHdf5Fixture;
import ru.itmo.ctlab.hict.hict_library.assembly.SequenceSourceDigest;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_server.concurrent.RequestTaskScheduler;
import ru.itmo.ctlab.hict.hict_server.handlers.fileop.FileOpHandlersHolder;
import ru.itmo.ctlab.hict.hict_server.util.shareable.ShareableWrappers;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.EnumMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DotplotResultsHandlersTest {
  @TempDir Path data;

  @Test void resultLifecycleIncludesCurrentCoordinatesAndProtectsSavedCopies() throws Exception {
    var primaryPath = ReinforcementHdf5Fixture.create(data, "original", List.of(4L, 10L));
    var mismatchPath = ReinforcementHdf5Fixture.create(data, "mismatch", List.of(4L));
    var saved = Files.copy(primaryPath, data.resolve("saved.hict.hdf5"));
    try (var source = HDF5Factory.openForReading(primaryPath.toFile()); var overlay = HDF5Factory.open(saved.toFile())) {
      var meta = "/sequence_analysis";
      var update = meta + "/updates/region-one";
      overlay.object().createGroup(update + "/4");
      overlay.int64().writeMatrix(update + "/4/row_bins", new long[][]{{0, 1}});
      overlay.int64().writeMatrix(update + "/4/column_bins", new long[][]{{0, 1}});
      overlay.int64().setAttr(update, "ordinal", 1);
      overlay.int64().setAttr(update, "created_at_ms", 1_700_000_000_000L);
      overlay.bool().setAttr(meta, "complete", true);
      overlay.bool().setAttr(meta, "explicit_coverage", true);
      overlay.string().setAttr("/", "template_name", primaryPath.getFileName().toString());
      overlay.string().setAttr(meta, "compatibility", new JsonObject()
        .put("sourceMetadataSha256", SequenceSourceDigest.digest(source, List.of(4L, 10L)))
        .put("unit", "identity_percent").put("scale", 10000)
        .put("settings", new JsonObject().put("kind", "moddotplot")
          .put("parameters", new JsonObject().put("k", 21).put("identity", 86))).encode());
    }

    var vertx = Vertx.vertx();
    var sizes = new EnumMap<RequestTaskScheduler.RequestPriority, RequestTaskScheduler.PoolSizing>(RequestTaskScheduler.RequestPriority.class);
    for (var priority : RequestTaskScheduler.RequestPriority.values()) sizes.put(priority, new RequestTaskScheduler.PoolSizing(1, 1));
    var scheduler = new RequestTaskScheduler(vertx, new RequestTaskScheduler.SchedulerConfig(5, 16, 10, sizes));
    try (var primary = openMap(primaryPath); var mismatch = openMap(mismatchPath)) {
      var shared = vertx.sharedData().<String, Object>getLocalMap("hict_server");
      shared.put("dataDirectory", new ShareableWrappers.PathWrapper(data));
      shared.put(RequestTaskScheduler.LOCAL_MAP_KEY, new ShareableWrappers.RequestTaskSchedulerWrapper(scheduler));
      shared.put("chunkedFile", new ShareableWrappers.ChunkedFileWrapper(primary));
      var router = Router.router(vertx);
      router.route().handler(BodyHandler.create());
      var files = new FileOpHandlersHolder(vertx);
      new DotplotResultsHandlers(vertx, files).addHandlersToRouter(router);
      router.route().failureHandler(ctx -> ctx.response().setStatusCode(ctx.failure() instanceof HttpException http ? http.getStatusCode() : 500)
        .end(new JsonObject().put("error", ctx.failure() instanceof HttpException http
          ? http.getPayload() : ctx.failure().getMessage()).encode()));
      var server = vertx.createHttpServer().requestHandler(router).listen(0, "127.0.0.1").toCompletionStage().toCompletableFuture().get();
      try (var client = HttpClient.newHttpClient()) {
        String root = "http://127.0.0.1:" + server.actualPort() + "/sequence/overlays/";
        assertEquals("[]", post(client, root + "list", new JsonObject()).body());
        var open = new JsonObject().put("filename", saved.getFileName().toString());
        var registered = post(client, root + "open", open);
        assertEquals(200, registered.statusCode(), registered.body());
        var id = new JsonObject(registered.body()).getString("id");
        var entry = listed(client, root);
        assertTrue(entry.getBoolean("shown"));
        Path managed = data.resolve(entry.getString("file"));
        assertNotEquals(saved, managed);
        var region = entry.getJsonArray("regions").getJsonObject(0);
        assertEquals(new JsonObject().put("start", 4L).put("end", 8L), region.getJsonObject("rows"));
        assertFalse(region.getBoolean("split"));
        primary.getScaffoldingOperations().reverseSelectionRangeBp(0, 17);
        assertEquals(new JsonObject().put("start", 9L).put("end", 13L),
          listed(client, root).getJsonArray("regions").getJsonObject(0).getJsonObject("columns"));

        var download = HttpRequest.newBuilder(URI.create(root + "download?id=" + id)).GET().build();
        var bytes = client.send(download, HttpResponse.BodyHandlers.ofByteArray());
        assertEquals(200, bytes.statusCode());
        assertTrue(bytes.headers().firstValue("Content-Disposition").orElseThrow().endsWith("original.moddotplot.hict.hdf5"));
        assertArrayEquals(Files.readAllBytes(saved), bytes.body());

        var select = new JsonObject().put("id", id);
        var hide = post(client, root + "show", select.copy().put("shown", false));
        assertEquals(200, hide.statusCode(), hide.body());
        assertFalse(listed(client, root).getBoolean("shown"));
        var show = post(client, root + "show", select.copy().put("shown", true));
        assertEquals(200, show.statusCode(), show.body());
        assertTrue(listed(client, root).getBoolean("shown"));
        var lock = data.toRealPath().resolve("processed/sequence-analysis").resolve(id);
        synchronized (DotplotResultRegistry.WRITING) { DotplotResultRegistry.WRITING.add(lock); }
        try {
          assertEquals(409, post(client, root + "delete", select).statusCode());
        } finally {
          synchronized (DotplotResultRegistry.WRITING) { DotplotResultRegistry.WRITING.remove(lock); }
        }
        var deleted = post(client, root + "delete", select);
        assertEquals(200, deleted.statusCode(), deleted.body());
        assertFalse(Files.exists(managed));
        assertTrue(Files.exists(saved));
        assertEquals(404, client.send(download, HttpResponse.BodyHandlers.discarding()).statusCode());

        shared.put("chunkedFile", new ShareableWrappers.ChunkedFileWrapper(mismatch));
        var rejected = post(client, root + "open", open);
        assertEquals(409, rejected.statusCode(), rejected.body());
        assertTrue(rejected.body().contains("It was made for “original.hict.hdf5”"), rejected.body());
      } finally {
        files.closeSecondarySource();
        server.close().toCompletionStage().toCompletableFuture().get();
      }
    } finally {
      scheduler.close();
      vertx.close().toCompletionStage().toCompletableFuture().get();
    }
  }

  private static ChunkedFile openMap(Path path) throws Exception {
    return new ChunkedFile(new ChunkedFile.ChunkedFileOptions(path, 1, 2));
  }

  private static HttpResponse<String> post(HttpClient client, String uri, JsonObject body) throws Exception {
    return client.send(HttpRequest.newBuilder(URI.create(uri)).header("Content-Type", "application/json")
      .POST(HttpRequest.BodyPublishers.ofString(body.encode())).build(), HttpResponse.BodyHandlers.ofString());
  }

  private static JsonObject listed(HttpClient client, String root) throws Exception {
    var response = post(client, root + "list", new JsonObject());
    assertEquals(200, response.statusCode(), response.body());
    return new JsonArray(response.body()).getJsonObject(0);
  }
}
