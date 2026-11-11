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

package ru.itmo.ctlab.hict.hict_server;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.LocalMap;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.itmo.ctlab.hict.hict_server.concurrent.RequestTaskScheduler;
import ru.itmo.ctlab.hict.hict_server.handlers.conversion.ConversionHandlersHolder;
import ru.itmo.ctlab.hict.hict_server.handlers.files.FSHandlersHolder;
import ru.itmo.ctlab.hict.hict_server.handlers.fileop.FileOpHandlersHolder;
import ru.itmo.ctlab.hict.hict_server.handlers.tiles.TileHandlersHolder;
import ru.itmo.ctlab.hict.hict_library.visualization.SimpleVisualizationOptions;
import ru.itmo.ctlab.hict.hict_library.visualization.colormap.gradient.SimpleLinearGradient;
import ru.itmo.ctlab.hict.hict_server.handlers.info.ApiDocsHandlersHolder;
import ru.itmo.ctlab.hict.hict_server.handlers.info.InfoHandlersHolder;
import ru.itmo.ctlab.hict.hict_server.util.shareable.ShareableWrappers;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ApiHttpIntegrationTest {
  @TempDir
  Path tempDataDir;

  private Vertx vertx;
  private RequestTaskScheduler scheduler;
  private io.vertx.core.http.HttpServer server;
  private int port;
  private final HttpClient httpClient = HttpClient.newHttpClient();

  @AfterEach
  void tearDown() throws Exception {
    if (server != null) {
      final var closeFuture = new CompletableFuture<Void>();
      server.close(ar -> {
        if (ar.succeeded()) {
          closeFuture.complete(null);
        } else {
          closeFuture.completeExceptionally(ar.cause());
        }
      });
      closeFuture.get(10, TimeUnit.SECONDS);
      server = null;
    }
    if (vertx != null) {
      final var map = vertx.sharedData().getLocalMap("hict_server");
      if (map.get("Track1DManager") instanceof ShareableWrappers.Track1DManagerWrapper wrapper) {
        wrapper.getTrack1DManager().close();
      }
      if (map.get("chunkedFile") instanceof ShareableWrappers.ChunkedFileWrapper wrapper) {
        wrapper.getChunkedFile().close();
      }
      if (map.get("chunkedFileSecondary") instanceof ShareableWrappers.ChunkedFileWrapper wrapper) {
        wrapper.getChunkedFile().close();
      }
    }
    if (scheduler != null) {
      scheduler.close();
      scheduler = null;
    }
    if (vertx != null) {
      final var closeFuture = new CompletableFuture<Void>();
      vertx.close(ar -> {
        if (ar.succeeded()) {
          closeFuture.complete(null);
        } else {
          closeFuture.completeExceptionally(ar.cause());
        }
      });
      closeFuture.get(10, TimeUnit.SECONDS);
      vertx = null;
    }
  }

  @Test
  void cancelledTileAndMatrixResponsesAreExplicitAndPngIsTransparent() throws Exception {
    final var started = new CountDownLatch(2);
    final var release = new CountDownLatch(1);
    startServerWithInfoAndFileHandlers(router -> router.get("/hold-tile-worker").handler(ctx ->
      scheduler.submit(ctx, RequestTaskScheduler.RequestPriority.TILE, null, () -> {
        started.countDown();
        if (!release.await(10, TimeUnit.SECONDS)) throw new IllegalStateException("Test worker was not released");
        return "released";
      }, value -> ctx.response().end(value))));
    final var holdRequest = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/hold-tile-worker")).GET().build();
    final var holds = java.util.List.of(
      httpClient.sendAsync(holdRequest, HttpResponse.BodyHandlers.ofString()),
      httpClient.sendAsync(holdRequest, HttpResponse.BodyHandlers.ofString()));
    try {
      assertTrue(started.await(5, TimeUnit.SECONDS), "Both tile workers must be occupied");
      final var requests = java.util.List.of(
        HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/get_tile?row=0&col=0&format=JSON_PNG_WITH_RANGES")).GET().build(),
        HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/get_tile?row=0&col=0&format=PNG"))
          .header("Accept-Encoding", "gzip").GET().build(),
        HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/matrix/query"))
          .header("content-type", "application/json").POST(HttpRequest.BodyPublishers.ofString("{\"format\":\"JSON\"}")).build(),
        HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/matrix/query"))
          .header("content-type", "application/json").POST(HttpRequest.BodyPublishers.ofString("{\"format\":\"BINARY_FLOAT32\"}")).build());
      final var responses = requests.stream().map(request -> httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray())).toList();
      final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (scheduler.diagnosticsSnapshot().pools().get(RequestTaskScheduler.RequestPriority.TILE).queueSize() < 4
        && System.nanoTime() < deadline) Thread.sleep(10);
      assertEquals(4, scheduler.diagnosticsSnapshot().pools().get(RequestTaskScheduler.RequestPriority.TILE).queueSize());
      scheduler.bumpGeneration(RequestTaskScheduler.CancellationDomain.TILE);
      for (final var pending : responses) {
        final var response = pending.get(5, TimeUnit.SECONDS);
        assertEquals(200, response.statusCode());
        assertEquals("true", response.headers().firstValue("x-hict-cancelled").orElse(""));
        assertEquals("no-store", response.headers().firstValue("cache-control").orElse(""));
      }
      final var tile = new JsonObject(new String(responses.get(0).get().body(), StandardCharsets.UTF_8));
      assertTrue(tile.getBoolean("cancelled"));
      final var png = java.util.Base64.getDecoder().decode(tile.getString("image").substring("data:image/png;base64,".length()));
      final var pngResponse = responses.get(1).get();
      assertEquals("identity", pngResponse.headers().firstValue("content-encoding").orElse(""));
      org.junit.jupiter.api.Assertions.assertArrayEquals(png, pngResponse.body());
      assertTransparentPng(png);
      final var matrix = new JsonObject(new String(responses.get(2).get().body(), StandardCharsets.UTF_8));
      assertTrue(matrix.getBoolean("cancelled"));
      assertEquals(0, matrix.getInteger("rows"));
      assertEquals(0, matrix.getInteger("cols"));
      assertTrue(matrix.getJsonArray("values").isEmpty());
      assertEquals(0, responses.get(3).get().body().length);
    } finally {
      release.countDown();
      for (final var hold : holds) assertEquals("released", hold.get(5, TimeUnit.SECONDS).body());
    }
  }

  private static void assertTransparentPng(byte[] png) throws IOException {
    final var bytes = java.nio.ByteBuffer.wrap(png);
    assertEquals(0x89504e470d0a1a0aL, bytes.getLong());
    while (bytes.hasRemaining()) {
      final int length = bytes.getInt();
      assertTrue(length >= 0 && length <= bytes.remaining() - 8, "Truncated PNG chunk");
      final var crc = new java.util.zip.CRC32();
      crc.update(png, bytes.position(), length + 4);
      bytes.position(bytes.position() + length + 4);
      assertEquals((int) crc.getValue(), bytes.getInt(), "Invalid PNG chunk checksum");
    }
    final var image = javax.imageio.ImageIO.read(new java.io.ByteArrayInputStream(png));
    assertEquals(1, image.getWidth());
    assertEquals(1, image.getHeight());
    assertEquals(0, image.getRGB(0, 0) >>> 24, "Cancellation placeholder must be transparent");
  }

  @Test
  void optionalLargeMapsOpenAndQueryOverHttpWithoutWritingSourceData() throws Exception {
    final var configuredRoot = System.getenv("HICT_OPENING_DATA_DIR");
    assumeTrue(configuredRoot != null, "Set HICT_OPENING_DATA_DIR for the large-map HTTP checks");
    final var dataRoot = Path.of(configuredRoot).toAbsolutePath().normalize();
    final var inputs = java.util.List.of(
      "4DNFIM351CAA_HomoSapiens_HiC_1kb.hict.hdf5",
      "RHINO_1k.mcool.hict.hdf5",
      "DNAZoo/AedesAegypti/AaegL5.0.hict.hdf5",
      "micro-c/GSE286495/GSE286495_mESC_merged_15.6B_mm39.mcool.local.hict.hdf5"
    );
    for (final var input : inputs) assertTrue(Files.isRegularFile(dataRoot.resolve(input)), input);
    startServerWithInfoAndFileHandlers();
    final var map = vertx.sharedData().getLocalMap("hict_server");
    map.put("dataDirectory", new ShareableWrappers.PathWrapper(dataRoot));
    map.put("processedDirectory", new ShareableWrappers.PathWrapper(tempDataDir.resolve("processed")));
    map.put("visualizationOptions", new ShareableWrappers.SimpleVisualizationOptionsWrapper(
      new SimpleVisualizationOptions(0, 0, false, false, false,
        new SimpleLinearGradient(32, java.awt.Color.WHITE, java.awt.Color.RED, 0, 10))));
    for (final var input : inputs) {
      long start = System.nanoTime();
      final var response = post("/open", new JsonObject().put("filename", input).encode());
      assertEquals(200, response.statusCode(), response.body());
      final var opened = new JsonObject(response.body());
      assertEquals("Opened", opened.getString("status"));
      assertTrue(!opened.getJsonArray("resolutions").isEmpty());
      final var progress = post("/open_progress", "{}");
      assertEquals("done", new JsonObject(progress.body()).getString("stage"));
      System.out.printf("HTTP_OPEN %s %.3f seconds%n", input, (System.nanoTime() - start) / 1e9);
      for (final var resolution : opened.getJsonArray("resolutions")) {
        final var matrix = post("/matrix/query", new JsonObject().put("bpResolution", resolution)
          .put("rows", 4).put("cols", 4).put("unit", "PIXELS")
          .put("signalMode", "RAW_COUNTS").put("format", "JSON").encode());
        assertEquals(200, matrix.statusCode(), matrix.body());
        assertEquals(16, new JsonObject(matrix.body()).getJsonArray("values").size());
      }
      final var tile = get("/get_tile?bpResolution=" + opened.getJsonArray("resolutions").getValue(0)
        + "&row=0&col=0&format=JSON_PNG_WITH_RANGES");
      assertEquals(200, tile.statusCode(), tile.body());
      assertTrue(tile.body().contains("data:image/png;base64,"), "A rendered tile must be returned");
    }
  }

  @Test
  void apiDocsEndpointsServeSpecAndSupportCaching() throws Exception {
    startServerWithInfoAndFileHandlers();

    final var docsResponse = get("/api/v1/");
    assertEquals(200, docsResponse.statusCode());
    assertTrue(docsResponse.body().contains("SwaggerUIBundle"));
    assertTrue(docsResponse.body().contains("/api/v1/openapi.yaml"));
    assertTrue(docsResponse.body().contains("validatorUrl: null"));
    assertTrue(!docsResponse.body().contains("unpkg.com"));
    for (final var asset : Map.of(
      "swagger-ui.css", "text/css",
      "swagger-ui-bundle.js", "text/javascript"
    ).entrySet()) {
      final var path = "/api/v1/assets/" + asset.getKey();
      final var response = get(path);
      assertEquals(200, response.statusCode(), response.body());
      assertTrue(response.headers().firstValue("content-type").orElse("").startsWith(asset.getValue()));
      assertTrue(response.body().length() > 1000);
      final var etag = response.headers().firstValue("etag").orElseThrow();
      assertEquals(304, get(path, Map.of("If-None-Match", etag)).statusCode());
    }
    final var docsEtag = docsResponse.headers().firstValue("etag").orElse("");
    assertTrue(!docsEtag.isBlank());

    final var docsCached = get("/api/v1/", Map.of("If-None-Match", docsEtag));
    assertEquals(304, docsCached.statusCode());

    final var redirect = get("/api/v1");
    assertEquals(307, redirect.statusCode());
    assertEquals("/api/v1/", redirect.headers().firstValue("location").orElse(""));

    final var specResponse = get("/api/v1/openapi.yaml");
    assertEquals(200, specResponse.statusCode());
    assertTrue(specResponse.body().contains("openapi: 3.0.3"));
    assertTrue(specResponse.body().contains("/tracks/query_1d:"));
    assertTrue(specResponse.body().contains("/convert/jobs/{jobId}:"));
    final var specEtag = specResponse.headers().firstValue("etag").orElse("");
    assertTrue(!specEtag.isBlank());

    final var specCached = get("/api/v1/openapi.yaml", Map.of("If-None-Match", specEtag));
    assertEquals(304, specCached.statusCode());
  }

  @Test
  void tileRequestsRejectOversizedDimensionsAndOverflowingOffsets() throws Exception {
    startServerWithInfoAndFileHandlers();
    for (final var query : List.of(
      "tile_size=1025", "tile_size=0",
      "format=PNG_BY_PIXELS&rows=1&cols=1025",
      "format=PNG_BY_PIXELS&rows=1025&cols=1",
      "row=9223372036854775807&tile_size=256",
      "format=PNG_BY_PIXELS&row=9223372036854775807&rows=2&cols=1"
    )) {
      final var response = get("/get_tile?" + query);
      assertEquals(400, response.statusCode(), query + ": " + response.body());
    }
  }

  @Test
  void fastaOnlySourcesReturnBlankTilesAndRejectContactNormalization() throws Exception {
    final var fasta = tempDataDir.resolve("reference.fa");
    Files.writeString(fasta, ">sequence\nACGTACGTACGT\n");
    final var scan = ru.itmo.ctlab.hict.hict_library.converters.FastaOnlyAssembly.scan(fasta, 100, () -> false, null);
    ru.itmo.ctlab.hict.hict_library.converters.FastaOnlyAssembly.write(
      scan, tempDataDir.resolve("sequence.hict.hdf5"), () -> false, null);
    startServerWithInfoAndFileHandlers(router ->
      new ru.itmo.ctlab.hict.hict_server.handlers.normalization.NormalizationHandlersHolder(vertx).addHandlersToRouter(router));
    vertx.sharedData().getLocalMap("hict_server").put("visualizationOptions",
      new ShareableWrappers.SimpleVisualizationOptionsWrapper(
        new SimpleVisualizationOptions(0, 0, false, false, false,
          new SimpleLinearGradient(32, java.awt.Color.WHITE, java.awt.Color.RED, 0, 10))));

    final var opened = post("/open", "{\"filename\":\"sequence.hict.hdf5\"}");
    assertEquals(200, opened.statusCode(), opened.body());
    final var resolution = new JsonObject(opened.body()).getJsonArray("resolutions").getValue(0);
    final var tile = get("/get_tile?bpResolution=" + resolution + "&tile_size=4");
    assertEquals(200, tile.statusCode(), tile.body());
    final var image = new JsonObject(tile.body()).getString("image");
    final var png = java.util.Base64.getDecoder().decode(image.substring("data:image/png;base64,".length()));
    final var decoded = javax.imageio.ImageIO.read(new java.io.ByteArrayInputStream(png));
    assertEquals(4, decoded.getWidth());
    assertEquals(4, decoded.getHeight());
    for (int row = 0; row < 4; row++) for (int col = 0; col < 4; col++) {
      assertEquals(0, decoded.getRGB(col, row) >>> 24);
    }
    final var stats = post("/matrix/statistics", new JsonObject().put("bpResolution", resolution).encode());
    assertEquals(200, stats.statusCode(), stats.body());
    assertEquals(0, new JsonObject(stats.body()).getInteger("count"));
    final var coverage = post("/normalization/coverage/prepare", "{}");
    assertEquals(409, coverage.statusCode(), coverage.body());
    assertTrue(coverage.body().contains("needs contact data"));
  }

  @Test
  void infoAndFileEndpointsRespondOverHttpWithScheduler() throws Exception {
    Files.createDirectories(tempDataDir.resolve("build/quad"));
    final var uploads = Files.createDirectories(tempDataDir.resolve(ConversionHandlersHolder.UPLOAD_DIRECTORY_NAME));
    Files.writeString(uploads.resolve("output-private.hict.hdf5"), "temporary");
    Files.writeString(tempDataDir.resolve("build/quad/a.hict.hdf5"), "x", StandardCharsets.UTF_8);
    Files.writeString(tempDataDir.resolve("build/quad/source.hic"), "x", StandardCharsets.UTF_8);
    Files.writeString(tempDataDir.resolve("build/quad/b.cool"), "x", StandardCharsets.UTF_8);
    Files.writeString(tempDataDir.resolve("build/quad/c.mcool"), "x", StandardCharsets.UTF_8);
    Files.writeString(tempDataDir.resolve("build/quad/genome.fasta"), ">chr1\nACGT\n", StandardCharsets.UTF_8);
    Files.writeString(tempDataDir.resolve("build/quad/example.agp"), "##agp\n", StandardCharsets.UTF_8);

    startServerWithInfoAndFileHandlers();

    final var version = get("/version");
    assertEquals(200, version.statusCode());
    assertTrue(version.body().contains("\"version\""));
    assertTrue(version.body().contains("\"webuiVersion\""));

    final var diagnostics = post("/diagnostics/workers", "{}");
    assertEquals(200, diagnostics.statusCode());
    assertTrue(diagnostics.body().contains("\"pools\""));
    assertTrue(diagnostics.body().contains("\"cancellationDomains\""));

    final var files = post("/list_files", "{}");
    assertEquals(200, files.statusCode());
    assertTrue(files.body().contains("a.hict.hdf5"));
    assertTrue(!files.body().contains("output-private.hict.hdf5"));
    final var root = post("/list_directory", "{}");
    assertEquals(200, root.statusCode(), root.body());
    assertTrue(!root.body().contains(ConversionHandlersHolder.UPLOAD_DIRECTORY_NAME));

    final var detailed = post("/list_files_detailed", "{}");
    assertEquals(200, detailed.statusCode());
    assertTrue(detailed.body().contains("\"sizeBytes\""));
    assertTrue(detailed.body().contains("\"modifiedAtMs\""));
    assertTrue(detailed.body().contains("\"extension\""));

    final var coolers = post("/list_coolers", "{}");
    assertEquals(200, coolers.statusCode());
    assertTrue(coolers.body().contains(".hic"));
    assertTrue(coolers.body().contains("b.cool"));
    assertTrue(coolers.body().contains("c.mcool"));

    final var convertibleMatrices = post("/list_convertible_matrices", "{}");
    assertEquals(200, convertibleMatrices.statusCode());
    assertTrue(convertibleMatrices.body().contains(".hic"));
    assertTrue(convertibleMatrices.body().contains("b.cool"));
    assertTrue(convertibleMatrices.body().contains("c.mcool"));

    final var toolchain = post("/convert/toolchain", "{}");
    assertEquals(200, toolchain.statusCode());
    assertTrue(toolchain.body().contains("\"hicConversionAvailable\""));
    assertTrue(toolchain.body().contains("\"summary\""));

    final var dotplotAligner = post("/convert/toolchain/dotplot-aligner", "{\"alignerPreference\":\"minimap2\"}");
    assertEquals(200, dotplotAligner.statusCode());
    assertTrue(dotplotAligner.body().contains("\"dotplotAlignerPreference\":\"minimap2\""));

    final var fasta = post("/list_fasta_files", "{}");
    assertEquals(200, fasta.statusCode());
    assertTrue(fasta.body().contains("genome.fasta"));

    final var agp = post("/list_agp_files", "{}");
    assertEquals(200, agp.statusCode());
    assertTrue(agp.body().contains("example.agp"));
  }

  @Test
  void fileListingSkipsUnreadableDirectories() throws Exception {
    assumeTrue(
      FileSystems.getDefault().supportedFileAttributeViews().contains("posix"),
      "POSIX permissions are required to model an inaccessible directory locally"
    );
    Files.createDirectories(tempDataDir.resolve("visible"));
    Files.writeString(tempDataDir.resolve("visible/ok.hict.hdf5"), "x", StandardCharsets.UTF_8);
    final var blocked = tempDataDir.resolve("Application Data");
    Files.createDirectories(blocked);
    Files.setPosixFilePermissions(blocked, Set.of());
    try {
      startServerWithInfoAndFileHandlers();

      final var detailed = post("/list_files_detailed", "{}");
      assertEquals(200, detailed.statusCode());
      assertTrue(detailed.body().contains("ok.hict.hdf5"));

      final var files = post("/list_files", "{}");
      assertEquals(200, files.statusCode());
      assertTrue(files.body().contains("ok.hict.hdf5"));
    } finally {
      Files.setPosixFilePermissions(blocked, Set.of(
        PosixFilePermission.OWNER_READ,
        PosixFilePermission.OWNER_WRITE,
        PosixFilePermission.OWNER_EXECUTE
      ));
    }
  }

  @Test
  void attachesIndependentPyramidsAndRendersOnThePrimaryGrid() throws Exception {
    final var fixture = tempDataDir;
    ru.itmo.ctlab.hict.fixtures.ReinforcementHdf5Fixture.create(fixture, "primary", java.util.List.of(4L, 10L));
    ru.itmo.ctlab.hict.fixtures.ReinforcementHdf5Fixture.create(fixture, "overlay", java.util.List.of(3L, 7L));
    startServerWithInfoAndFileHandlers();
    vertx.sharedData().getLocalMap("hict_server").put("visualizationOptions",
      new ShareableWrappers.SimpleVisualizationOptionsWrapper(new SimpleVisualizationOptions(
        0, 0, false, false, false, new SimpleLinearGradient(32, java.awt.Color.WHITE, java.awt.Color.RED, 0, 10))));
    assertEquals(200, post("/open", "{\"filename\":\"primary.hict.hdf5\"}").statusCode());
    final var attached = post("/secondary/open", "{\"filename\":\"overlay.hict.hdf5\"}");
    assertEquals(200, attached.statusCode(), attached.body());
    final var status = new JsonObject(attached.body());
    assertTrue(status.getBoolean("attached"));
    assertTrue(status.getJsonObject("compatibility").getBoolean("sharedReference"));
    final var composed = get("/get_tile?bpResolution=4&tile_size=8");
    assertEquals(200, composed.statusCode(), composed.body());
    final var projected = get("/get_tile?source=SECONDARY&bpResolution=3&gridBpResolution=4&gridSampleFactor=2&tile_size=16");
    assertEquals(200, projected.statusCode(), projected.body());
    final var image = new JsonObject(projected.body()).getString("image");
    final var png = javax.imageio.ImageIO.read(new java.io.ByteArrayInputStream(
      java.util.Base64.getDecoder().decode(image.substring("data:image/png;base64,".length()))));
    assertEquals(16, png.getWidth());
    assertEquals(16, png.getHeight());
    assertEquals(400, get("/get_tile?source=SECONDARY&bpResolution=3&gridBpResolution=4&gridSampleFactor=65").statusCode());
  }

  @Test
  void rejectedFastaRelinksRetainPublishedPrimaryAndSecondaryPaths() throws Exception {
    for (String name : java.util.List.of("primary", "secondary"))
      ru.itmo.ctlab.hict.fixtures.ReinforcementHdf5Fixture.create(tempDataDir, name, java.util.List.of(4L, 10L));
    Files.writeString(tempDataDir.resolve("valid.fa"), ">tiny\nACGTRYMKSWBDHVNAC\n");
    Files.writeString(tempDataDir.resolve("mismatch.fa"), ">unrelated\nACGT\n");
    startServerWithInfoAndFileHandlers();
    assertEquals(200, post("/open", "{\"filename\":\"primary.hict.hdf5\"}").statusCode());
    assertEquals(200, post("/secondary/open", "{\"filename\":\"secondary.hict.hdf5\"}").statusCode());
    var map = vertx.sharedData().getLocalMap("hict_server");
    for (String source : java.util.List.of("PRIMARY", "SECONDARY")) {
      assertEquals(200, post("/link_fasta", new JsonObject().put("source", source).put("fastaFilename", "valid.fa").encode()).statusCode());
      String key = "linkedFasta" + (source.equals("PRIMARY") ? "Primary" : "Secondary");
      var path = map.get(key + "Path");
      var missing = post("/link_fasta", new JsonObject().put("source", source).put("fastaFilename", "missing.fa").encode());
      assertTrue(missing.statusCode() >= 400, missing.body());
      var mismatch = post("/link_fasta", new JsonObject().put("source", source).put("fastaFilename", "mismatch.fa").encode());
      assertEquals(200, mismatch.statusCode(), mismatch.body());
      assertTrue(new JsonObject(mismatch.body()).getBoolean("requiresConfirmation"), mismatch.body());
      org.junit.jupiter.api.Assertions.assertSame(path, map.get(key + "Path"));
      assertEquals("valid.fa", map.get(key + "Filename"));
    }
    assertEquals("valid.fa", map.get("linkedFastaFilename"));
  }

  @Test
  void sessionSnapshotCapturesEditedAssemblyNamesAndVerbatimGraph() throws Exception {
    for (String name : java.util.List.of("primary", "secondary"))
      ru.itmo.ctlab.hict.fixtures.ReinforcementHdf5Fixture.create(tempDataDir, name, java.util.List.of(4L, 10L));
    startServerWithInfoAndFileHandlers();
    assertTrue(post("/session/snapshot", "{}").statusCode() >= 400);
    assertEquals(200, post("/open", "{\"filename\":\"primary.hict.hdf5\"}").statusCode());
    assertEquals(200, post("/secondary/open", "{\"filename\":\"secondary.hict.hdf5\"}").statusCode());
    var map = vertx.sharedData().getLocalMap("hict_server");
    var primary = ((ShareableWrappers.ChunkedFileWrapper) map.get("chunkedFile")).getChunkedFile();
    primary.getScaffoldingOperations().reverseSelectionRangeBp(0, 17);
    primary.setContigNameOverride(0, "renamed_tiny");
    var graph = new JsonObject(Files.readString(Path.of("src/test/resources/reinforcement/custom-pipeline.json")));
    var config = ru.itmo.ctlab.hict.hict_server.handlers.tiles.RenderPipelineConfig.fromJson(graph);
    map.put(ru.itmo.ctlab.hict.hict_server.handlers.tiles.RenderPipelineConfig.LOCAL_MAP_KEY,
      new ShareableWrappers.RenderPipelineConfigWrapper(config));
    assertTrue(post("/session/snapshot", "{}").statusCode() >= 400);
    var visualization = new SimpleVisualizationOptions(2, 3, true, false, true,
      new SimpleLinearGradient(32, new java.awt.Color(10, 20, 30, 2),
        new java.awt.Color(200, 150, 100, 128), -5, 21));
    map.put("visualizationOptions", new ShareableWrappers.SimpleVisualizationOptionsWrapper(visualization));
    var response = post("/session/snapshot", "{\"verifyAssemblyRoundTrip\":true}");
    assertEquals(200, response.statusCode(), response.body());
    var snapshot = new JsonObject(response.body());
    // The encoder holds Float values while the HTTP body parses back as Double, so compare serialized forms.
    assertEquals(new JsonObject(ru.itmo.ctlab.hict.hict_server.handlers.fileop.SessionVisualizationCodec
        .encode(visualization).encode()),
      snapshot.getJsonObject("visualizationOptions"));
    assertEquals(1, snapshot.getInteger("snapshotVersion"));
    assertTrue(snapshot.getBoolean("assemblyVerified"));
    assertEquals(graph, snapshot.getJsonObject("renderPipeline"));
    var source = snapshot.getJsonObject("primary");
    assertEquals("renamed_tiny", source.getJsonObject("contigNameOverrides").getString("0"));
    assertEquals(0, source.getJsonObject("assemblyInfo").getJsonArray("contigDescriptors").getJsonObject(0).getInteger("contigDirection"));
    assertEquals(java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256")
      .digest(source.getString("agp").getBytes(StandardCharsets.UTF_8))), source.getString("agpSha256"));
    assertEquals("secondary.hict.hdf5", snapshot.getJsonObject("secondary").getString("filename"));
    assertEquals(0, snapshot.getJsonArray("tracks").size());
    assertEquals(snapshot, new JsonObject(post("/session/snapshot", "{\"verifyAssemblyRoundTrip\":true}").body()));
    primary.getScaffoldingOperations().reverseSelectionRangeBp(0, 17);
    var later = new JsonObject(post("/session/snapshot", "{}").body());
    org.junit.jupiter.api.Assertions.assertNotEquals(source.getString("agpSha256"), later.getJsonObject("primary").getString("agpSha256"));
    assertEquals(0, source.getJsonObject("assemblyInfo").getJsonArray("contigDescriptors").getJsonObject(0).getInteger("contigDirection"));
  }

  @Test
  void sessionRestorePublishesSavedStateAtomically() throws Exception {
    for (String name : java.util.List.of("primary", "secondary"))
      ru.itmo.ctlab.hict.fixtures.ReinforcementHdf5Fixture.create(tempDataDir, name, java.util.List.of(4L, 10L));
    Files.writeString(tempDataDir.resolve("tiny.fa"), ">tiny\nACGTRYMKSWBDHVNAC\n");
    Files.writeString(tempDataDir.resolve("marks.bed"), "tiny\t0\t5\tfeature\n");
    startServerWithInfoAndFileHandlers();
    var map = vertx.sharedData().getLocalMap("hict_server");
    map.put("visualizationOptions", new ShareableWrappers.SimpleVisualizationOptionsWrapper(
      new SimpleVisualizationOptions(0, 0, false, false, false,
        new SimpleLinearGradient(32, java.awt.Color.WHITE, java.awt.Color.RED, 0, 10))));
    assertEquals(200, post("/open", "{\"filename\":\"primary.hict.hdf5\"}").statusCode());
    assertEquals(200, post("/secondary/open", "{\"filename\":\"secondary.hict.hdf5\"}").statusCode());
    assertEquals(200, post("/link_fasta", "{\"fastaFilename\":\"tiny.fa\"}").statusCode());
    var primary = ((ShareableWrappers.ChunkedFileWrapper) map.get("chunkedFile")).getChunkedFile();
    primary.getScaffoldingOperations().reverseSelectionRangeBp(0, 17);
    primary.setContigNameOverride(0, "renamed_tiny");
    var tracks = ((ShareableWrappers.Track1DManagerWrapper) map.get("Track1DManager")).getTrack1DManager();
    tracks.openTrack("marks.bed", "Marks", "#aa3300");
    tracks.openCoolerWeightsTrack("Weights", "#0033aa", "SECONDARY");
    var graph = new JsonObject(Files.readString(Path.of("src/test/resources/reinforcement/custom-pipeline.json")));
    map.put(ru.itmo.ctlab.hict.hict_server.handlers.tiles.RenderPipelineConfig.LOCAL_MAP_KEY,
      new ShareableWrappers.RenderPipelineConfigWrapper(
        ru.itmo.ctlab.hict.hict_server.handlers.tiles.RenderPipelineConfig.fromJson(graph)));
    var visualization = new SimpleVisualizationOptions(2, 3, true, false, true,
      new SimpleLinearGradient(32, new java.awt.Color(10, 20, 30, 2),
        new java.awt.Color(200, 150, 100, 128), -5, 21));
    map.put("visualizationOptions", new ShareableWrappers.SimpleVisualizationOptionsWrapper(visualization));
    var attach = new JsonObject(post("/attach", "{}").body());
    var workspaceVersion = attach.getJsonObject("workspaceVersion");
    var view = new JsonObject().put("version", 1).put("center", new io.vertx.core.json.JsonArray().add(3).add(4));
    assertEquals(200, post("/workspace/state", new JsonObject().put("filename", "primary.hict.hdf5")
      .put("expectedVersion", workspaceVersion).put("state", view).encode()).statusCode());

    var saveResponse = post("/session/snapshot",
      "{\"verifyAssemblyRoundTrip\":true,\"fingerprintFiles\":true,\"verifyTrackDefinitions\":true}");
    assertEquals(200, saveResponse.statusCode(), saveResponse.body());
    var saved = new JsonObject(saveResponse.body());
    assertEquals(2, saved.getJsonArray("tracks").size());
    final String savedAgpHash = saved.getJsonObject("primary").getString("agpSha256");

    // Diverge: undo the edit, rename back, drop the tracks and the graph.
    primary.getScaffoldingOperations().reverseSelectionRangeBp(0, 17);
    primary.setContigNameOverride(0, "other_name");
    for (var track : tracks.listTracks()) tracks.removeTrack(track.getTrackId());
    map.put(ru.itmo.ctlab.hict.hict_server.handlers.tiles.RenderPipelineConfig.LOCAL_MAP_KEY,
      new ShareableWrappers.RenderPipelineConfigWrapper(
        ru.itmo.ctlab.hict.hict_server.handlers.tiles.RenderPipelineConfig.disabled()));
    assertEquals(200, post("/secondary/close", "{}").statusCode());
    var diverged = new JsonObject(post("/session/snapshot", "{}").body());
    org.junit.jupiter.api.Assertions.assertNotEquals(savedAgpHash, diverged.getJsonObject("primary").getString("agpSha256"));

    var restoredResponse = post("/session/restore", saved.encode());
    assertEquals(200, restoredResponse.statusCode(), restoredResponse.body());
    var restoredBody = new JsonObject(restoredResponse.body());
    assertEquals("primary.hict.hdf5", restoredBody.getString("filename"));
    assertEquals("tiny.fa", restoredBody.getString("fastaFilename"));
    assertEquals(view, restoredBody.getJsonObject("workspaceState"));
    assertTrue(restoredBody.getJsonObject("secondarySource").getBoolean("attached", true));

    var again = new JsonObject(post("/session/snapshot",
      "{\"verifyAssemblyRoundTrip\":true,\"fingerprintFiles\":true,\"verifyTrackDefinitions\":true}").body());
    assertEquals(savedAgpHash, again.getJsonObject("primary").getString("agpSha256"));
    assertEquals(saved.getJsonObject("primary").getJsonObject("contigNameOverrides"), again.getJsonObject("primary").getJsonObject("contigNameOverrides"));
    assertEquals(saved.getJsonArray("tracks"), again.getJsonArray("tracks"));
    assertEquals(saved.getJsonObject("renderPipeline"), again.getJsonObject("renderPipeline"));
    assertEquals(saved.getJsonObject("visualizationOptions"), again.getJsonObject("visualizationOptions"));
    assertEquals(saved.getJsonObject("secondary").getString("agpSha256"), again.getJsonObject("secondary").getString("agpSha256"));
    assertEquals("tiny.fa", again.getJsonObject("primary").getString("fastaFilename"));
    assertEquals(graph, again.getJsonObject("renderPipeline"));
  }

  @Test
  void rejectedSessionRestoreLeavesTheLiveSessionUntouched() throws Exception {
    ru.itmo.ctlab.hict.fixtures.ReinforcementHdf5Fixture.create(tempDataDir, "primary", java.util.List.of(4L, 10L));
    startServerWithInfoAndFileHandlers();
    var map = vertx.sharedData().getLocalMap("hict_server");
    map.put("visualizationOptions", new ShareableWrappers.SimpleVisualizationOptionsWrapper(
      new SimpleVisualizationOptions(0, 0, false, false, false,
        new SimpleLinearGradient(32, java.awt.Color.WHITE, java.awt.Color.RED, 0, 10))));
    assertTrue(post("/session/restore", "{}").statusCode() >= 400);
    Files.writeString(tempDataDir.resolve("tiny.fa"), ">tiny\nACGTRYMKSWBDHVNAC\n");
    assertEquals(200, post("/open", "{\"filename\":\"primary.hict.hdf5\"}").statusCode());
    assertEquals(200, post("/link_fasta", "{\"fastaFilename\":\"tiny.fa\"}").statusCode());
    var live = ((ShareableWrappers.ChunkedFileWrapper) map.get("chunkedFile")).getChunkedFile();
    live.getScaffoldingOperations().reverseSelectionRangeBp(0, 17);
    var saved = new JsonObject(post("/session/snapshot",
      "{\"verifyAssemblyRoundTrip\":true,\"fingerprintFiles\":true,\"verifyTrackDefinitions\":true}").body());
    var before = new JsonObject(post("/session/snapshot", "{}").body()).getJsonObject("primary").getString("agpSha256");

    assertEquals("HICT-QUICK-1", saved.getJsonObject("primary").getJsonObject("fileFingerprint").getString("algorithm"));
    assertEquals("HICT-QUICK-1", saved.getJsonObject("primary").getJsonObject("fastaFingerprint").getString("algorithm"));
    var fullSnapshot = post("/session/snapshot",
      "{\"fingerprintFiles\":true,\"fullFingerprints\":true}");
    assertEquals("SHA-256", new JsonObject(fullSnapshot.body()).getJsonObject("primary").getJsonObject("fileFingerprint").getString("algorithm"));

    Files.writeString(tempDataDir.resolve("tiny.fa"), ">tiny\nACGTRYMKSWBDHVNACGGGG\n");
    var refused = post("/session/restore", saved.encode());
    assertEquals(409, refused.statusCode());
    assertTrue(refused.body().contains("tiny.fa") && refused.body().contains("size changed"), refused.body());
    org.junit.jupiter.api.Assertions.assertSame(live, ((ShareableWrappers.ChunkedFileWrapper) map.get("chunkedFile")).getChunkedFile());
    Files.writeString(tempDataDir.resolve("tiny.fa"), ">tiny\nACGTRYMKSWBDHVNAC\n");
    assertEquals(200, post("/session/restore", saved.encode()).statusCode());
    live = ((ShareableWrappers.ChunkedFileWrapper) map.get("chunkedFile")).getChunkedFile();

    var tampered = saved.copy();
    tampered.getJsonObject("primary").getJsonObject("fileFingerprint").put("sha256", "0".repeat(64));
    assertEquals(409, post("/session/restore", tampered.encode()).statusCode());
    var missing = saved.copy();
    missing.getJsonObject("primary").put("filename", "absent.hict.hdf5");
    var missingResponse = post("/session/restore", missing.encode());
    assertEquals(409, missingResponse.statusCode());
    assertTrue(missingResponse.body().contains("absent.hict.hdf5"), missingResponse.body());
    var malformed = saved.copy();
    malformed.put("assemblySource", "NEITHER");
    assertEquals(400, post("/session/restore", malformed.encode()).statusCode());

    org.junit.jupiter.api.Assertions.assertSame(live, ((ShareableWrappers.ChunkedFileWrapper) map.get("chunkedFile")).getChunkedFile());
    assertEquals(before, new JsonObject(post("/session/snapshot", "{}").body()).getJsonObject("primary").getString("agpSha256"));
    assertEquals(200, post("/session/snapshot", "{}").statusCode());
  }

  @Test
  void failedSecondarySynchronizationRollsBackPrimaryAndRetainsOldSecondary() throws Exception {
    for (String name : java.util.List.of("primary", "secondary", "candidate"))
      ru.itmo.ctlab.hict.fixtures.ReinforcementHdf5Fixture.create(tempDataDir, name, java.util.List.of(4L, 10L));
    Files.writeString(tempDataDir.resolve("tiny.fa"), ">tiny\nACGTRYMKSWBDHVNAC\n");
    startServerWithInfoAndFileHandlers();
    assertEquals(200, post("/open", "{\"filename\":\"primary.hict.hdf5\"}").statusCode());
    var map = vertx.sharedData().getLocalMap("hict_server");
    var failAfterImport = new java.util.concurrent.atomic.AtomicBoolean(false);
    var primary = new ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile(
      new ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile.ChunkedFileOptions(
        tempDataDir.resolve("primary.hict.hdf5"), 1, 2)) {
      @Override public void importAGP(java.io.Reader reader) throws java.io.IOException, NoSuchFieldException {
        super.importAGP(reader);
        if (failAfterImport.get()) throw new java.io.IOException("Injected failure after actual assembly import");
      }
    };
    ((ShareableWrappers.ChunkedFileWrapper) map.get("chunkedFile")).getChunkedFile().close();
    map.put("chunkedFile", new ShareableWrappers.ChunkedFileWrapper(primary));
    primary.getScaffoldingOperations().reverseSelectionRangeBp(0, 17);
    assertEquals(200, post("/secondary/open", "{\"filename\":\"secondary.hict.hdf5\"}").statusCode());
    assertEquals(200, post("/link_fasta", "{\"fastaFilename\":\"tiny.fa\",\"source\":\"SECONDARY\"}").statusCode());
    assertEquals(200, post("/secondary/set_assembly_source", "{\"assemblySource\":\"SECONDARY\"}").statusCode());
    var oldSecondary = (ShareableWrappers.ChunkedFileWrapper) map.get("chunkedFileSecondary");
    var priorStatus = post("/secondary/status", "{}").body();
    var primaryAgp = primary.getAgpProcessor().getAssemblyAGPRecords().toString();
    var secondaryAgp = oldSecondary.getChunkedFile().getAgpProcessor().getAssemblyAGPRecords().toString();
    failAfterImport.set(true);
    for (int attempt = 0; attempt < 2; attempt++) {
      var failed = post("/secondary/open", "{\"filename\":\"candidate.hict.hdf5\"}");
      assertTrue(failed.statusCode() >= 400, failed.body());
      org.junit.jupiter.api.Assertions.assertSame(oldSecondary, map.get("chunkedFileSecondary"));
      assertEquals(priorStatus, post("/secondary/status", "{}").body());
      assertEquals("tiny.fa", map.get("linkedFastaSecondaryFilename"));
      assertEquals(primaryAgp, primary.getAgpProcessor().getAssemblyAGPRecords().toString());
      assertEquals(secondaryAgp, oldSecondary.getChunkedFile().getAgpProcessor().getAssemblyAGPRecords().toString());
      for (var file : java.util.List.of(primary, oldSecondary.getChunkedFile())) {
        var resolution = ru.itmo.ctlab.hict.hict_library.chunkedfile.resolution.ResolutionDescriptor.fromBpResolution(4, file);
        assertEquals(11, file.getMatrixQueries().getSubmatrix(resolution, 0, 0, 1, 1, false).matrix().getAsDouble(0, 0));
      }
    }
    // A failed source switch must not publish the requested source flag either.
    map.put("assemblyInfoSource", "PRIMARY");
    assertTrue(post("/secondary/set_assembly_source", "{\"assemblySource\":\"SECONDARY\"}").statusCode() >= 400);
    assertEquals("PRIMARY", map.get("assemblyInfoSource"));
    map.put("assemblyInfoSource", "SECONDARY");
    failAfterImport.set(false);
    var accepted = post("/secondary/open", "{\"filename\":\"candidate.hict.hdf5\"}");
    assertEquals(200, accepted.statusCode(), accepted.body());
    assertEquals(io.vertx.core.json.JsonObject.mapFrom(
      ru.itmo.ctlab.hict.hict_server.dto.response.assembly.AssemblyInfoDTO.generateFromChunkedFile(primary)),
      new JsonObject(accepted.body()).getJsonObject("assemblyInfo"));
    org.junit.jupiter.api.Assertions.assertNotSame(oldSecondary, map.get("chunkedFileSecondary"));
    assertEquals(null, map.get("linkedFastaSecondaryFilename"));
    var resolution = ru.itmo.ctlab.hict.hict_library.chunkedfile.resolution.ResolutionDescriptor.fromBpResolution(4, primary);
    assertEquals(1, primary.getMatrixQueries().getSubmatrix(resolution, 0, 0, 1, 1, false).matrix().getAsDouble(0, 0));
    ru.itmo.ctlab.hict.fixtures.ReinforcementHdf5Fixture.create(tempDataDir, "disjoint", java.util.List.of(3L, 7L));
    var mismatched = post("/secondary/open", "{\"filename\":\"disjoint.hict.hdf5\",\"allowMismatch\":true}");
    assertEquals(200, mismatched.statusCode(), mismatched.body());
    var primaryAssembly = io.vertx.core.json.JsonObject.mapFrom(
      ru.itmo.ctlab.hict.hict_server.dto.response.assembly.AssemblyInfoDTO.generateFromChunkedFile(primary));
    assertEquals(primaryAssembly, new JsonObject(mismatched.body()).getJsonObject("assemblyInfo"));
    for (var source : java.util.List.of("PRIMARY", "SECONDARY")) {
      var switched = post("/secondary/set_assembly_source", new JsonObject().put("assemblySource", source).encode());
      assertEquals(200, switched.statusCode(), switched.body());
      assertEquals(io.vertx.core.json.JsonObject.mapFrom(
        ru.itmo.ctlab.hict.hict_server.dto.response.assembly.AssemblyInfoDTO.generateFromChunkedFile(primary)),
        new JsonObject(switched.body()).getJsonObject("assemblyInfo"));
    }
  }

  @Test
  void failedPrimaryReplacementKeepsEditedSourcesFastaTracksAndWorkspace() throws Exception {
    ru.itmo.ctlab.hict.fixtures.ReinforcementHdf5Fixture.create(tempDataDir, "primary", java.util.List.of(4L, 10L));
    ru.itmo.ctlab.hict.fixtures.ReinforcementHdf5Fixture.create(tempDataDir, "secondary", java.util.List.of(4L, 10L));
    Files.writeString(tempDataDir.resolve("tiny.fa"), ">tiny\nACGTRYMKSWBDHVNAC\n");
    Files.writeString(tempDataDir.resolve("broken.hict.hdf5"), "Not an HDF5 file");
    startServerWithInfoAndFileHandlers();
    assertEquals(200, post("/open", "{\"filename\":\"primary.hict.hdf5\"}").statusCode());
    assertEquals(200, post("/secondary/open", "{\"filename\":\"secondary.hict.hdf5\"}").statusCode());
    var linked = post("/link_fasta", "{\"fastaFilename\":\"tiny.fa\"}");
    assertEquals(200, linked.statusCode(), linked.body());
    var map = vertx.sharedData().getLocalMap("hict_server");
    var primary = (ShareableWrappers.ChunkedFileWrapper) map.get("chunkedFile");
    var secondary = map.get("chunkedFileSecondary");
    var tracks = map.get("Track1DManager");
    var workspace = map.get("workspaceStateStore");
    var statistics = map.get("TileStatisticHolder");
    var file = primary.getChunkedFile();
    file.getScaffoldingOperations().reverseSelectionRangeBp(0, 17);
    var agp = file.getAgpProcessor().getAssemblyAGPRecords().toString();
    var resolution = ru.itmo.ctlab.hict.hict_library.chunkedfile.resolution.ResolutionDescriptor.fromBpResolution(4, file);
    var pixels = file.getMatrixQueries().getSubmatrix(resolution, 0, 0, 5, 5, false).matrix();
    var attachedBefore = post("/attach", "{}").body();
    for (String name : java.util.List.of("missing.hict.hdf5", "broken.hict.hdf5", "broken.hict.hdf5")) {
      var failed = post("/open", new JsonObject().put("filename", name).encode());
      assertTrue(failed.statusCode() >= 400, failed.body());
      org.junit.jupiter.api.Assertions.assertSame(primary, map.get("chunkedFile"));
      org.junit.jupiter.api.Assertions.assertSame(secondary, map.get("chunkedFileSecondary"));
      org.junit.jupiter.api.Assertions.assertSame(tracks, map.get("Track1DManager"));
      org.junit.jupiter.api.Assertions.assertSame(workspace, map.get("workspaceStateStore"));
      org.junit.jupiter.api.Assertions.assertSame(statistics, map.get("TileStatisticHolder"));
      assertEquals(agp, file.getAgpProcessor().getAssemblyAGPRecords().toString());
      var retainedPixels = file.getMatrixQueries().getSubmatrix(resolution, 0, 0, 5, 5, false).matrix();
      for (int row = 0; row < 5; row++) for (int col = 0; col < 5; col++)
        assertEquals(pixels.getAsDouble(row, col), retainedPixels.getAsDouble(row, col));
      assertEquals(attachedBefore, post("/attach", "{}").body());
    }
    assertEquals("failed", new JsonObject(post("/open_progress", "{}").body()).getString("stage"));
    // A failure after opening/validating the candidate must also retain live state.
    var unusableDirectory = tempDataDir.resolve("not-a-directory");
    Files.writeString(unusableDirectory, "occupied");
    map.put("processedDirectory", new ShareableWrappers.PathWrapper(unusableDirectory));
    var preparationFailure = post("/open", "{\"filename\":\"primary.hict.hdf5\"}");
    assertTrue(preparationFailure.statusCode() >= 400, preparationFailure.body());
    org.junit.jupiter.api.Assertions.assertSame(primary, map.get("chunkedFile"));
    org.junit.jupiter.api.Assertions.assertSame(tracks, map.get("Track1DManager"));
    assertEquals(attachedBefore, post("/attach", "{}").body());
    assertEquals(agp, file.getAgpProcessor().getAssemblyAGPRecords().toString());
    var stillReadable = file.getMatrixQueries().getSubmatrix(resolution, 0, 0, 5, 5, false).matrix();
    for (int row = 0; row < 5; row++) for (int col = 0; col < 5; col++)
      assertEquals(pixels.getAsDouble(row, col), stillReadable.getAsDouble(row, col));
    map.remove("processedDirectory");
    // Opening the same physical HDF5 while the old read handles are live must work.
    var reopened = post("/open", "{\"filename\":\"primary.hict.hdf5\"}");
    assertEquals(200, reopened.statusCode(), reopened.body());
    org.junit.jupiter.api.Assertions.assertNotSame(primary, map.get("chunkedFile"));
    assertEquals(null, map.get("chunkedFileSecondary"));
    assertEquals(null, map.get("linkedFastaFilename"));
    var replacement = ((ShareableWrappers.ChunkedFileWrapper) map.get("chunkedFile")).getChunkedFile();
    assertEquals(1, replacement.getMatrixQueries().getSubmatrix(
      ru.itmo.ctlab.hict.hict_library.chunkedfile.resolution.ResolutionDescriptor.fromBpResolution(4, replacement), 0, 0, 1, 1, false).matrix().getAsDouble(0, 0));
  }

  @Test
  void sourceCapabilitiesSurviveOpenAttachAndDisjointSecondaryStatus() throws Exception {
    ru.itmo.ctlab.hict.fixtures.ReinforcementHdf5Fixture.create(tempDataDir, "primary", java.util.List.of(4L, 10L));
    ru.itmo.ctlab.hict.fixtures.ReinforcementHdf5Fixture.create(tempDataDir, "secondary", java.util.List.of(3L, 7L));
    startServerWithInfoAndFileHandlers();
    var opened = post("/open", "{\"filename\":\"primary.hict.hdf5\"}");
    assertEquals(200, opened.statusCode(), opened.body());
    var capabilities = new JsonObject(opened.body()).getJsonObject("capabilities");
    assertEquals(1, capabilities.getInteger("schemaVersion"));
    assertEquals("UNKNOWN", capabilities.getString("units"));
    assertEquals("UNAVAILABLE", capabilities.getJsonObject("importedWeights").getString("state"));
    var attached = post("/attach", "{}");
    assertEquals(200, attached.statusCode(), attached.body());
    assertEquals(capabilities, new JsonObject(attached.body()).getJsonObject("openFileResponse").getJsonObject("capabilities"));
    var secondary = post("/secondary/open", "{\"filename\":\"secondary.hict.hdf5\",\"allowMismatch\":true}");
    assertEquals(200, secondary.statusCode(), secondary.body());
    var secondaryCapabilities = new JsonObject(secondary.body()).getJsonObject("capabilities");
    assertEquals(new io.vertx.core.json.JsonArray("[3,7]"), secondaryCapabilities.getJsonArray("resolutions"));
    var status = post("/secondary/status", "{}");
    assertEquals(secondaryCapabilities, new JsonObject(status.body()).getJsonObject("capabilities"));
    var detached = post("/secondary/close", "{}");
    assertEquals(200, detached.statusCode(), detached.body());
    assertTrue(!new JsonObject(detached.body()).containsKey("capabilities"));
  }

  @Test
  void fullPresetRejectsUnavailableInputsBeforeChangingOptionsOrGraph() throws Exception {
    ru.itmo.ctlab.hict.fixtures.ReinforcementHdf5Fixture.create(tempDataDir, "primary", List.of(4L, 10L));
    startServerWithInfoAndFileHandlers();
    vertx.sharedData().getLocalMap("hict_server").put("visualizationOptions",
      new ShareableWrappers.SimpleVisualizationOptionsWrapper(new SimpleVisualizationOptions(
        0, 0, false, false, false,
        new SimpleLinearGradient(32, java.awt.Color.WHITE, java.awt.Color.RED, 0, 10))));
    assertEquals(200, post("/open", "{\"filename\":\"primary.hict.hdf5\"}").statusCode());
    var before = new JsonObject(post("/session/snapshot", "{}").body());
    var options = new JsonObject(post("/get_visualization_options", "{}").body());
    var primary = new JsonObject().put("type", "source").put("source", "PRIMARY");
    for (var input : List.of(
      new JsonObject().put("type", "source").put("source", "SECONDARY"),
      new JsonObject().put("type", "track1d").put("trackId", "missing-track").put("axis", "ROW"))) {
      var graph = new JsonObject().put("enabled", true).put("swapUpperLower", true)
        .put("upperExpression", primary.copy()).put("lowerExpression", input);
      var rejected = post("/set_visualization_options", options.copy().put("postLogBase", 2).put("renderPipeline", graph).encode());
      assertEquals(409, rejected.statusCode(), rejected.body());
      assertTrue(rejected.body().contains(input.getString("type").equals("source") ? "secondary source" : "required track"), rejected.body());
      var after = new JsonObject(post("/session/snapshot", "{}").body());
      assertEquals(before.getJsonObject("visualizationOptions"), after.getJsonObject("visualizationOptions"));
      assertEquals(before.getJsonObject("renderPipeline"), after.getJsonObject("renderPipeline"));
      assertEquals(before.getJsonObject("primary"), after.getJsonObject("primary"));
    }
    var graph = new JsonObject().put("enabled", true).put("swapUpperLower", true)
      .put("upperExpression", primary.copy())
      .put("lowerExpression", new JsonObject().put("type", "constant").put("value", 7));
    var accepted = post("/set_visualization_options", options.copy().put("postLogBase", 2).put("renderPipeline", graph).encode());
    assertEquals(200, accepted.statusCode(), accepted.body());
    var after = new JsonObject(post("/session/snapshot", "{}").body());
    assertEquals(graph, after.getJsonObject("renderPipeline"));
    assertEquals(2.0, after.getJsonObject("visualizationOptions").getDouble("postLogBase"));
  }

  private void startServerWithInfoAndFileHandlers() throws Exception {
    startServerWithInfoAndFileHandlers(router -> {});
  }

  private void startServerWithInfoAndFileHandlers(java.util.function.Consumer<Router> configure) throws Exception {
    vertx = Vertx.vertx();
    scheduler = new RequestTaskScheduler(vertx, schedulerConfig());

    final @NotNull LocalMap<String, Object> map = vertx.sharedData().getLocalMap("hict_server");
    map.put("dataDirectory", new ShareableWrappers.PathWrapper(tempDataDir.toAbsolutePath().normalize()));
    map.put(
      RequestTaskScheduler.LOCAL_MAP_KEY,
      new ShareableWrappers.RequestTaskSchedulerWrapper(scheduler)
    );

    final var router = Router.router(vertx);
    router.route().handler(BodyHandler.create());
    configure.accept(router);
    router.route().failureHandler(ctx -> {
      final var message = ctx.failure() != null && ctx.failure().getMessage() != null
        ? ctx.failure().getMessage()
        : "Request failed";
      final int status = ctx.failure() instanceof io.vertx.ext.web.handler.HttpException http ? http.getStatusCode() : 500;
      final var payload = ctx.failure() instanceof io.vertx.ext.web.handler.HttpException http ? http.getPayload() : null;
      ctx.response()
        .putHeader("content-type", "application/json")
        .setStatusCode(status)
        .end("{\"error\":\"" + (payload != null ? payload : message).replace("\"", "\\\"") + "\"}");
    });

    new InfoHandlersHolder(vertx).addHandlersToRouter(router);
    new FSHandlersHolder(vertx).addHandlersToRouter(router);
    new ConversionHandlersHolder(vertx).addHandlersToRouter(router);
    new FileOpHandlersHolder(vertx).addHandlersToRouter(router);
    new TileHandlersHolder(vertx).addHandlersToRouter(router);
    new ApiDocsHandlersHolder().addHandlersToRouter(router);

    server = vertx.createHttpServer();
    final var listenFuture = new CompletableFuture<Integer>();
    server.requestHandler(router).listen(0, "127.0.0.1", ar -> {
      if (ar.succeeded()) {
        listenFuture.complete(ar.result().actualPort());
      } else {
        listenFuture.completeExceptionally(ar.cause());
      }
    });
    port = listenFuture.get(10, TimeUnit.SECONDS);
  }

  private RequestTaskScheduler.SchedulerConfig schedulerConfig() {
    final var sizing = new EnumMap<RequestTaskScheduler.RequestPriority, RequestTaskScheduler.PoolSizing>(
      RequestTaskScheduler.RequestPriority.class
    );
    for (final var priority : RequestTaskScheduler.RequestPriority.values()) {
      sizing.put(priority, new RequestTaskScheduler.PoolSizing(2, 2));
    }
    return new RequestTaskScheduler.SchedulerConfig(10, 16, 10, sizing);
  }

  private HttpResponse<String> get(final @NotNull String path) throws IOException, InterruptedException {
    return get(path, Map.of());
  }

  private HttpResponse<String> get(final @NotNull String path,
                                   final @NotNull Map<String, String> headers) throws IOException, InterruptedException {
    var builder = HttpRequest.newBuilder()
      .uri(URI.create("http://127.0.0.1:" + port + path))
      .timeout(java.time.Duration.ofMinutes(2))
      .GET();
    for (final var entry : headers.entrySet()) {
      builder = builder.header(entry.getKey(), entry.getValue());
    }
    return httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
  }

  private HttpResponse<String> post(final @NotNull String path,
                                    final @NotNull String body) throws IOException, InterruptedException {
    final var request = HttpRequest.newBuilder()
      .uri(URI.create("http://127.0.0.1:" + port + path))
      .timeout(java.time.Duration.ofMinutes(2))
      .header("content-type", "application/json")
      .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
      .build();
    return httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
  }
}
