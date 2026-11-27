package ru.itmo.ctlab.hict.hict_server;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ResourceLock(Resources.SYSTEM_PROPERTIES)
class SinglePortServingTest {
  private static final String INDEX = "<html><title>HiCT test UI</title></html>";
  private static final String ASSET = "/assets/app.0123456789.js";

  @TempDir
  Path temporaryDirectory;

  private final Map<String, String> previousProperties = new HashMap<>();
  private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
  private Vertx vertx;

  @AfterEach
  void stop() throws Exception {
    try {
      if (vertx != null) {
        vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
      }
    } finally {
      previousProperties.forEach((key, value) -> {
        if (value == null) {
          System.clearProperty(key);
        } else {
          System.setProperty(key, value);
        }
      });
    }
  }

  @Test
  void servesApiAndStaticFilesOnOnePort() throws Exception {
    start("0", null, null, true);
    final var state = vertx.sharedData().getLocalMap("hict_server");
    final int port = (int) state.get("HICT_PORT");
    assertEquals(port, state.get("VXPORT"));
    assertApiAndIndex(port);

    final var asset = get(port, ASSET);
    assertEquals(200, asset.statusCode());
    assertEquals("console.log('HiCT');", asset.body());
    assertTrue(asset.headers().firstValue("Cache-Control").orElseThrow().contains("immutable"));
    final var head = send(port, ASSET, "HEAD", HttpRequest.BodyPublishers.noBody());
    assertEquals(200, head.statusCode());
    assertTrue(head.body().isEmpty());

    final var badOpen = send(port, "/open", "POST", HttpRequest.BodyPublishers.ofString("{}"));
    assertTrue(badOpen.statusCode() >= 400 && badOpen.statusCode() < 500, badOpen.body());
    assertTrue(badOpen.headers().firstValue("Content-Type").orElseThrow().startsWith("application/json"));
    assertEquals(200, get(port, "/api/v1/openapi.yaml").statusCode());
    assertEquals(404, get(port, "/no-such").statusCode());
  }

  @Test
  void servesOnlyApiWhenWebUiIsDisabled() throws Exception {
    start(null, "0", null, false);
    final int port = (int) vertx.sharedData().getLocalMap("hict_server").get("HICT_PORT");
    assertVersion(port);
    assertEquals(404, get(port, "/").statusCode());
  }

  @Test
  void servesBothSurfacesOnEachLegacyPort() throws Exception {
    final var host = InetAddress.getByName("127.0.0.1");
    final int vxPort;
    final int webUiPort;
    try (var primary = new ServerSocket(0, 1, host); var secondary = new ServerSocket(0, 1, host)) {
      vxPort = primary.getLocalPort();
      webUiPort = secondary.getLocalPort();
    }
    start(null, Integer.toString(vxPort), Integer.toString(webUiPort), true);
    assertApiAndIndex(vxPort);
    assertApiAndIndex(webUiPort);
  }

  private void assertApiAndIndex(final int port) throws Exception {
    final var index = get(port, "/");
    assertEquals(200, index.statusCode());
    assertEquals(INDEX, index.body());
    assertEquals("no-cache", index.headers().firstValue("Cache-Control").orElseThrow());
    assertVersion(port);
  }

  private void assertVersion(final int port) throws Exception {
    final var version = get(port, "/version");
    assertEquals(200, version.statusCode());
    assertTrue(version.headers().firstValue("Content-Type").orElseThrow().startsWith("application/json"));
    assertTrue(new JsonObject(version.body()).containsKey("version"));
  }

  private void start(final String hictPort, final String vxPort, final String webUiPort,
                     final boolean serveWebUi) throws Exception {
    final var root = Files.createDirectory(temporaryDirectory.resolve("webui"));
    Files.writeString(root.resolve("index.html"), INDEX);
    Files.createDirectory(root.resolve("assets"));
    Files.writeString(root.resolve(ASSET.substring(1)), "console.log('HiCT');");
    setProperty("WEBUI_ROOT", root.toString());
    setProperty("DATA_DIR", Files.createDirectory(temporaryDirectory.resolve("data")).toString());
    setProperty("PROCESSED_DIR", Files.createDirectory(temporaryDirectory.resolve("processed")).toString());
    setProperty("HICT_BIND_HOST", "127.0.0.1");
    setProperty("HICT_PORT", hictPort);
    setProperty("VXPORT", vxPort);
    setProperty("WEBUI_PORT", webUiPort);
    setProperty("SERVE_WEBUI", Boolean.toString(serveWebUi));
    setProperty("AUTO_OPEN_BROWSER", "false");
    vertx = Vertx.vertx();
    vertx.deployVerticle(new MainVerticle()).toCompletionStage().toCompletableFuture().get(15, TimeUnit.SECONDS);
  }

  private void setProperty(final String key, final String value) {
    previousProperties.put(key, System.getProperty(key));
    if (value == null) {
      System.clearProperty(key);
    } else {
      System.setProperty(key, value);
    }
  }

  private HttpResponse<String> get(final int port, final String path) throws Exception {
    return send(port, path, "GET", HttpRequest.BodyPublishers.noBody());
  }

  private HttpResponse<String> send(final int port, final String path, final String method,
                                    final HttpRequest.BodyPublisher body) throws Exception {
    final var request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
      .timeout(Duration.ofSeconds(10)).header("Content-Type", "application/json").method(method, body);
    return http.send(request.build(), HttpResponse.BodyHandlers.ofString());
  }
}
