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
import io.vertx.core.shareddata.LocalMap;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.itmo.ctlab.hict.hict_server.concurrent.RequestTaskScheduler;
import ru.itmo.ctlab.hict.hict_server.handlers.files.FSHandlersHolder;
import ru.itmo.ctlab.hict.hict_server.handlers.info.ApiDocsHandlersHolder;
import ru.itmo.ctlab.hict.hict_server.handlers.info.InfoHandlersHolder;
import ru.itmo.ctlab.hict.hict_server.util.shareable.ShareableWrappers;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

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
  void apiDocsEndpointsServeSpecAndSupportCaching() throws Exception {
    startServerWithInfoAndFileHandlers();

    final var docsResponse = get("/api/v1/");
    assertEquals(200, docsResponse.statusCode());
    assertTrue(docsResponse.body().contains("SwaggerUIBundle"));
    assertTrue(docsResponse.body().contains("/api/v1/openapi.yaml"));
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
  void infoAndFileEndpointsRespondOverHttpWithScheduler() throws Exception {
    Files.createDirectories(tempDataDir.resolve("build/quad"));
    Files.writeString(tempDataDir.resolve("build/quad/a.hict.hdf5"), "x", StandardCharsets.UTF_8);
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

    final var detailed = post("/list_files_detailed", "{}");
    assertEquals(200, detailed.statusCode());
    assertTrue(detailed.body().contains("\"sizeBytes\""));
    assertTrue(detailed.body().contains("\"modifiedAtMs\""));
    assertTrue(detailed.body().contains("\"extension\""));

    final var coolers = post("/list_coolers", "{}");
    assertEquals(200, coolers.statusCode());
    assertTrue(coolers.body().contains("b.cool"));
    assertTrue(coolers.body().contains("c.mcool"));

    final var fasta = post("/list_fasta_files", "{}");
    assertEquals(200, fasta.statusCode());
    assertTrue(fasta.body().contains("genome.fasta"));

    final var agp = post("/list_agp_files", "{}");
    assertEquals(200, agp.statusCode());
    assertTrue(agp.body().contains("example.agp"));
  }

  private void startServerWithInfoAndFileHandlers() throws Exception {
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
    router.route().failureHandler(ctx -> {
      final var message = ctx.failure() != null && ctx.failure().getMessage() != null
        ? ctx.failure().getMessage()
        : "Request failed";
      ctx.response()
        .putHeader("content-type", "application/json")
        .setStatusCode(500)
        .end("{\"error\":\"" + message.replace("\"", "\\\"") + "\"}");
    });

    new InfoHandlersHolder(vertx).addHandlersToRouter(router);
    new FSHandlersHolder(vertx).addHandlersToRouter(router);
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
      .header("content-type", "application/json")
      .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
      .build();
    return httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
  }
}
