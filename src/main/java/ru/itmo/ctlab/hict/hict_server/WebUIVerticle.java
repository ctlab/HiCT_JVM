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

import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.SLF4JLogDelegateFactory;
import io.vertx.core.shareddata.LocalMap;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.CorsHandler;
import io.vertx.ext.web.handler.FileSystemAccess;
import io.vertx.ext.web.handler.StaticHandler;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;

import java.awt.Desktop;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;

@Slf4j
public class WebUIVerticle extends AbstractVerticle {


  @Override
  public void start(final Promise<Void> startPromise) throws Exception {
    try {
    // set vertx logger delegate factory to slf4j
    String logFactory = System.getProperty("org.vertx.logger-delegate-factory-class-name");
    if (logFactory == null) {
      System.setProperty("org.vertx.logger-delegate-factory-class-name", SLF4JLogDelegateFactory.class.getName());
    }

    log.info("Logging for WebUI initialized");

    final ConfigStoreOptions jsonEnvConfig = new ConfigStoreOptions().setType("env")
      .setConfig(new JsonObject().put("keys", new JsonArray()
        .add("SERVE_WEBUI")
        .add("WEBUI_PORT")
        .add("AUTO_OPEN_BROWSER")
        .add("HICT_BIND_HOST")));
    final ConfigRetrieverOptions myOptions = new ConfigRetrieverOptions().addStore(jsonEnvConfig);
    final ConfigRetriever configRetriever = ConfigRetriever.create(vertx, myOptions);
    configRetriever.getConfig(event -> {
      if (event.failed()) {
        log.error("Failed to load WebUI configuration", event.cause());
        startPromise.fail(event.cause());
        return;
      }
      try {
        final var serveWebUI = resolveServeWebUI(event.result());
        final var webuiPort = resolveWebuiPort(event.result());
        final var autoOpenBrowser = resolveAutoOpenBrowser(event.result());
        final var bindHost = resolveBindHost(event.result());

        log.info("Writing WebUI configuration to local shared state");
        final @NotNull @NonNull LocalMap<String, Object> map = vertx.sharedData().getLocalMap("webui_server");
        map.put("WEBUI_PORT", webuiPort);
        map.put("SERVE_WEBUI", serveWebUI);
        map.put("AUTO_OPEN_BROWSER", autoOpenBrowser);
        map.put("HICT_BIND_HOST", bindHost);

        if (!serveWebUI) {
          log.info("Not serving WebUI because SERVE_WEBUI=false");
          startPromise.complete();
          return;
        }

        final HttpServerOptions webuiServerOptions = new HttpServerOptions();
        webuiServerOptions.setCompressionSupported(true);
        final var webuiServer = vertx.createHttpServer(webuiServerOptions);
        final var webuiRouter = Router.router(vertx);

        webuiRouter.route().handler(CorsHandler.create()
          .allowedMethod(io.vertx.core.http.HttpMethod.GET)
          .allowedMethod(io.vertx.core.http.HttpMethod.POST)
          .allowedMethod(io.vertx.core.http.HttpMethod.OPTIONS)
          .allowedHeader("Access-Control-Request-Method")
          .allowedHeader("Access-Control-Allow-Credentials")
          .allowedHeader("Access-Control-Allow-Origin")
          .allowedHeader("Access-Control-Allow-Headers")
          .allowedHeader("Content-Type"));

        final var webuiStaticHandler = createWebuiStaticHandler();
        webuiRouter.route("/").handler(ctx -> ctx.reroute("/index.html"));
        webuiRouter.route("/*").handler(webuiStaticHandler);

        log.info("Starting WebUI server on {}:{}", bindHost, webuiPort);
        webuiServer.requestHandler(webuiRouter).listen(webuiPort, bindHost, ar -> {
          if (ar.succeeded()) {
            log.info("WebUI Server started on {}:{}", bindHost, webuiServer.actualPort());
            if (autoOpenBrowser) {
              tryOpenBrowser(webuiServer.actualPort());
            }
            startPromise.complete();
          } else {
            log.error("Failed to start WebUI server on {}:{}", bindHost, webuiPort, ar.cause());
            startPromise.fail(ar.cause());
          }
        });
      } catch (final Throwable t) {
        log.error("WebUI verticle start failed", t);
        startPromise.fail(t);
      }
    });
    } catch (Throwable t) {
      log.error("WebUI verticle start failed", t);
      startPromise.fail(t);
    }
  }

  private boolean resolveServeWebUI(final @NotNull JsonObject config) {
    final var systemOverride = System.getProperty("SERVE_WEBUI");
    if (systemOverride != null && !systemOverride.isBlank()) {
      return Boolean.parseBoolean(systemOverride.trim());
    }
    return config.getBoolean("SERVE_WEBUI", true);
  }

  private int resolveWebuiPort(final @NotNull JsonObject config) {
    final var systemOverride = System.getProperty("WEBUI_PORT");
    if (systemOverride != null && !systemOverride.isBlank()) {
      try {
        return Integer.parseInt(systemOverride.trim());
      } catch (NumberFormatException ignored) {
        log.warn("Invalid WEBUI_PORT system property: {}", systemOverride);
      }
    }
    return config.getInteger("WEBUI_PORT", 8080);
  }

  private boolean resolveAutoOpenBrowser(final @NotNull JsonObject config) {
    final var systemOverride = System.getProperty("AUTO_OPEN_BROWSER");
    if (systemOverride != null && !systemOverride.isBlank()) {
      return Boolean.parseBoolean(systemOverride.trim());
    }
    return config.getBoolean("AUTO_OPEN_BROWSER", false);
  }

  private @NotNull String resolveBindHost(final @NotNull JsonObject config) {
    final var systemOverride = System.getProperty("HICT_BIND_HOST");
    if (systemOverride != null && !systemOverride.isBlank()) {
      return systemOverride.trim();
    }
    return config.getString("HICT_BIND_HOST", "0.0.0.0");
  }

  private void tryOpenBrowser(final int port) {
    try {
      if (!Desktop.isDesktopSupported() || !Desktop.getDesktop().isSupported(Desktop.Action.BROWSE)) {
        log.info("Desktop browsing is not supported on this system, skipping automatic WebUI launch");
        return;
      }
      Desktop.getDesktop().browse(URI.create("http://localhost:" + port + "/"));
    } catch (final Throwable t) {
      log.warn("Failed to open WebUI in the default browser", t);
    }
  }

  private @NotNull StaticHandler createWebuiStaticHandler() {
    final var explicitRoot = System.getenv("WEBUI_ROOT");
    if (explicitRoot != null && !explicitRoot.isBlank()) {
      final var explicitRootPath = Path.of(explicitRoot).toAbsolutePath().normalize();
      if (Files.isDirectory(explicitRootPath)) {
        log.info("Serving WebUI from WEBUI_ROOT={}", explicitRootPath);
        return StaticHandler.create(FileSystemAccess.ROOT, explicitRootPath.toString());
      }
      log.warn("WEBUI_ROOT is set but does not exist: {}", explicitRootPath);
    }

    final var localDist = Path.of("../HiCT_WebUI/dist").toAbsolutePath().normalize();
    if (Files.isDirectory(localDist)) {
      log.info("Serving WebUI from local checkout: {}", localDist);
      return StaticHandler.create(FileSystemAccess.ROOT, localDist.toString());
    }

    final var builtCloneDist = Path.of("build/webui/HiCT_WebUI/dist").toAbsolutePath().normalize();
    if (Files.isDirectory(builtCloneDist)) {
      log.info("Serving WebUI from gradle clone output: {}", builtCloneDist);
      return StaticHandler.create(FileSystemAccess.ROOT, builtCloneDist.toString());
    }

    log.info("Serving WebUI from classpath resources: webui");
    return StaticHandler.create("webui");
  }
}
