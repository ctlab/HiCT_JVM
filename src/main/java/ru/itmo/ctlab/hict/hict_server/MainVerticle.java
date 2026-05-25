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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Launcher;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.SLF4JLogDelegateFactory;
import io.vertx.core.shareddata.LocalMap;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.slf4j.LoggerFactory;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.hdf5.HDF5LibraryInitializer;
import ru.itmo.ctlab.hict.hict_library.visualization.SimpleVisualizationOptions;
import ru.itmo.ctlab.hict.hict_library.visualization.colormap.gradient.SimpleLinearGradient;
import ru.itmo.ctlab.hict.hict_server.concurrent.RequestTaskScheduler;
import ru.itmo.ctlab.hict.hict_server.handlers.fileop.FileOpHandlersHolder;
import ru.itmo.ctlab.hict.hict_server.handlers.files.FSHandlersHolder;
import ru.itmo.ctlab.hict.hict_server.handlers.names.NameMappingHandlersHolder;
import ru.itmo.ctlab.hict.hict_server.handlers.operations.ScaffoldingOpHandlersHolder;
import ru.itmo.ctlab.hict.hict_server.handlers.conversion.ConversionHandlersHolder;
import ru.itmo.ctlab.hict.hict_server.handlers.conversion.DotplotHandlersHolder;
import ru.itmo.ctlab.hict.hict_server.handlers.info.ApiDocsHandlersHolder;
import ru.itmo.ctlab.hict.hict_server.handlers.info.InfoHandlersHolder;
import ru.itmo.ctlab.hict.hict_server.handlers.tiles.RenderPipelineConfig;
import ru.itmo.ctlab.hict.hict_server.handlers.tiles.TileHandlersHolder;
import ru.itmo.ctlab.hict.hict_server.handlers.tracks.TrackHandlersHolder;
import ru.itmo.ctlab.hict.hict_server.info.AttributionInfo;
import ru.itmo.ctlab.hict.hict_server.util.shareable.ShareableWrappers;

import java.awt.*;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

@Slf4j(topic = "MainVerticle")
public class MainVerticle extends AbstractVerticle {
  private RequestTaskScheduler requestTaskScheduler;
  private String bindHost = "0.0.0.0";

  static {
    HDF5LibraryInitializer.initializeHDF5Library();
  }

  public static void main(final String[] args) {
    Launcher.executeCommand("run", MainVerticle.class.getName());
  }

  @Override
  public void start(final Promise<Void> startPromise) throws Exception {
    // set vertx logger delegate factory to slf4j
    String logFactory = System.getProperty("org.vertx.logger-delegate-factory-class-name");
    if (logFactory == null) {
      System.setProperty("org.vertx.logger-delegate-factory-class-name", SLF4JLogDelegateFactory.class.getName());
    }

    final Logger root = (Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
    final var rootLogLevel = resolveRootLogLevel();
    root.setLevel(rootLogLevel);

    log.info("Logging initialized at {} level", rootLogLevel);

    final ConfigStoreOptions jsonEnvConfig = new ConfigStoreOptions().setType("env")
        .setConfig(new JsonObject().put("keys",
            new JsonArray()
              .add("DATA_DIR")
              .add("PROCESSED_DIR")
              .add("TILE_SIZE")
              .add("VXPORT")
              .add("HICT_BIND_HOST")
              .add("MIN_DS_POOL")
              .add("MAX_DS_POOL")
              .add("HICT_WORKERS_TOTAL_MAX")
              .add("HICT_WORKERS_QUEUE_CAPACITY")
              .add("HICT_WORKERS_KEEPALIVE_SECONDS")
              .add("HICT_WORKERS_UI_MIN")
              .add("HICT_WORKERS_UI_MAX")
              .add("HICT_WORKERS_ASSEMBLY_MIN")
              .add("HICT_WORKERS_ASSEMBLY_MAX")
              .add("HICT_WORKERS_TILE_MIN")
              .add("HICT_WORKERS_TILE_MAX")
              .add("HICT_WORKERS_TRACK_MIN")
              .add("HICT_WORKERS_TRACK_MAX")
              .add("HICT_WORKERS_EXPORT_MIN")
              .add("HICT_WORKERS_EXPORT_MAX")));
    final ConfigRetrieverOptions myOptions = new ConfigRetrieverOptions().addStore(jsonEnvConfig);
    final ConfigRetriever configRetriever = ConfigRetriever.create(vertx, myOptions);
    configRetriever.getConfig(event -> {
      if (event.failed()) {
        log.error("Failed to load server configuration", event.cause());
        startPromise.fail(event.cause());
        return;
      }

      final int port;
      try {
        port = configureServerState(event.result());
      } catch (final Exception ex) {
        log.error("Failed to initialize server state", ex);
        startPromise.fail(ex);
        return;
      }

      final HttpServerOptions serverOptions = new HttpServerOptions();
      serverOptions.setCompressionSupported(true);
      final var server = vertx.createHttpServer(serverOptions);
      final var router = createRouter();

      log.info("Starting server on {}:{}", this.bindHost, port);
      server.requestHandler(router).listen(port, this.bindHost, ar -> {
        if (ar.succeeded()) {
          log.info("Server started on {}:{}", this.bindHost, ar.result().actualPort());
          AttributionInfo.startupBannerLines().forEach(log::info);
          deployWebUiVerticle();
          startPromise.complete();
        } else {
          log.error("Failed to start server on {}:{}", this.bindHost, port, ar.cause());
          startPromise.fail(ar.cause());
        }
      });
    });
  }

  @Override
  public void stop(final Promise<Void> stopPromise) {
    if (this.requestTaskScheduler != null) {
      this.requestTaskScheduler.close();
      this.requestTaskScheduler = null;
    }
    stopPromise.complete();
  }

  private static @NotNull Level resolveRootLogLevel() {
    final var value = firstNonBlank(
      System.getProperty("HICT_LOG_LEVEL"),
      System.getenv("HICT_LOG_LEVEL")
    );
    if (value == null) {
      return Level.INFO;
    }
    return Level.toLevel(value.trim().toUpperCase(Locale.ROOT), Level.INFO);
  }

  private static String firstNonBlank(final String... values) {
    for (final var value : values) {
      if (value != null && !value.isBlank()) {
        return value;
      }
    }
    return null;
  }

  private static int getIntegerSetting(final @NotNull JsonObject config,
                                       final @NotNull String key,
                                       final int defaultValue) {
    final Object raw = config.getValue(key);
    if (raw instanceof Number number) {
      return number.intValue();
    }
    if (raw instanceof String value && !value.isBlank()) {
      try {
        return Integer.parseInt(value.trim());
      } catch (final NumberFormatException ignored) {
        // Fall through to system property/default.
      }
    }
    final String systemPropertyValue = System.getProperty(key);
    if (systemPropertyValue != null && !systemPropertyValue.isBlank()) {
      try {
        return Integer.parseInt(systemPropertyValue.trim());
      } catch (final NumberFormatException ignored) {
        // Fall through to default.
      }
    }
    return defaultValue;
  }

  private static @NotNull String getStringSetting(final @NotNull JsonObject config,
                                                  final @NotNull String key,
                                                  final @NotNull String defaultValue) {
    final Object raw = config.getValue(key);
    if (raw instanceof String value && !value.isBlank()) {
      return value.trim();
    }
    final String systemPropertyValue = System.getProperty(key);
    if (systemPropertyValue != null && !systemPropertyValue.isBlank()) {
      return systemPropertyValue.trim();
    }
    return defaultValue;
  }

  private int configureServerState(final @NotNull JsonObject config) {
    final var dataDirectoryString = config.getString("DATA_DIR", ".");
    final var dataDirectory = Path.of(dataDirectoryString).normalize().toAbsolutePath().normalize();
    final var processedDirectoryString = config.getString(
      "PROCESSED_DIR",
      dataDirectory.resolve("processed").toString()
    );
    final var processedDirectory = Path.of(processedDirectoryString).normalize().toAbsolutePath().normalize();
    final var tileSize = getIntegerSetting(config, "TILE_SIZE", 256);
    final var minDSPool = getIntegerSetting(config, "MIN_DS_POOL", 4);
    final var maxDSPool = getIntegerSetting(config, "MAX_DS_POOL", 16);
    final var port = getIntegerSetting(config, "VXPORT", 5000);
    final var bindHost = getStringSetting(config, "HICT_BIND_HOST", "0.0.0.0");
    this.bindHost = bindHost;
    final int cores = Math.max(2, Runtime.getRuntime().availableProcessors());
    final int totalWorkersDefault = Math.max(10, cores * 2);
    final int totalWorkers = getIntegerSetting(config, "HICT_WORKERS_TOTAL_MAX", totalWorkersDefault);
    final int queueCapacity = getIntegerSetting(config, "HICT_WORKERS_QUEUE_CAPACITY", 32);
    final int keepAliveSeconds = getIntegerSetting(config, "HICT_WORKERS_KEEPALIVE_SECONDS", 30);
    final int defaultPoolMax = Math.max(2, Math.min(totalWorkers, cores));
    final var perPrioritySizing = new EnumMap<RequestTaskScheduler.RequestPriority, RequestTaskScheduler.PoolSizing>(
      RequestTaskScheduler.RequestPriority.class
    );
    perPrioritySizing.put(
      RequestTaskScheduler.RequestPriority.UI_UX,
      new RequestTaskScheduler.PoolSizing(
        getIntegerSetting(config, "HICT_WORKERS_UI_MIN", 4),
        getIntegerSetting(config, "HICT_WORKERS_UI_MAX", defaultPoolMax)
      )
    );
    perPrioritySizing.put(
      RequestTaskScheduler.RequestPriority.ASSEMBLY,
      new RequestTaskScheduler.PoolSizing(
        getIntegerSetting(config, "HICT_WORKERS_ASSEMBLY_MIN", 4),
        getIntegerSetting(config, "HICT_WORKERS_ASSEMBLY_MAX", defaultPoolMax)
      )
    );
    perPrioritySizing.put(
      RequestTaskScheduler.RequestPriority.TILE,
      new RequestTaskScheduler.PoolSizing(
        getIntegerSetting(config, "HICT_WORKERS_TILE_MIN", 8),
        getIntegerSetting(config, "HICT_WORKERS_TILE_MAX", defaultPoolMax)
      )
    );
    perPrioritySizing.put(
      RequestTaskScheduler.RequestPriority.TRACK,
      new RequestTaskScheduler.PoolSizing(
        getIntegerSetting(config, "HICT_WORKERS_TRACK_MIN", 4),
        getIntegerSetting(config, "HICT_WORKERS_TRACK_MAX", defaultPoolMax)
      )
    );
    perPrioritySizing.put(
      RequestTaskScheduler.RequestPriority.EXPORT,
      new RequestTaskScheduler.PoolSizing(
        getIntegerSetting(config, "HICT_WORKERS_EXPORT_MIN", 2),
        getIntegerSetting(config, "HICT_WORKERS_EXPORT_MAX", defaultPoolMax)
      )
    );

    log.info("Writing server configuration to local shared state");
    final @NotNull @NonNull LocalMap<String, Object> map = vertx.sharedData().getLocalMap("hict_server");
    map.put("dataDirectory", new ShareableWrappers.PathWrapper(dataDirectory));
    map.put("processedDirectory", new ShareableWrappers.PathWrapper(processedDirectory));
    map.put("tileSize", tileSize);
    map.put("VXPORT", port);
    map.put("HICT_BIND_HOST", bindHost);
    map.put("MIN_DS_POOL", minDSPool);
    map.put("MAX_DS_POOL", maxDSPool);
    this.requestTaskScheduler = new RequestTaskScheduler(
      vertx,
      new RequestTaskScheduler.SchedulerConfig(
        totalWorkers,
        queueCapacity,
        keepAliveSeconds,
        perPrioritySizing
      )
    );
    map.put(
      RequestTaskScheduler.LOCAL_MAP_KEY,
      new ShareableWrappers.RequestTaskSchedulerWrapper(this.requestTaskScheduler)
    );

    final var defaultVisualizationOptions = new SimpleVisualizationOptions(
      10.0,
      0.0,
      false,
      false,
      false,
      false,
      0.995d,
      new SimpleLinearGradient(
        32,
        new Color(255, 255, 255, 0),
        new Color(0, 96, 0, 255),
        0.0d,
        1.0d
      )
    );

    map.put(
      "visualizationOptions",
      new ShareableWrappers.SimpleVisualizationOptionsWrapper(defaultVisualizationOptions)
    );
    map.put(
      RenderPipelineConfig.LOCAL_MAP_KEY,
      new ShareableWrappers.RenderPipelineConfigWrapper(RenderPipelineConfig.disabled())
    );

    log.info("Using {} as data directory", dataDirectory);
    log.info("Using {} as processed directory", processedDirectory);
    log.info("Using tile size {}", tileSize);
    return port;
  }

  private @NotNull Router createRouter() {
    final var router = Router.router(vertx);
    router.route().handler(CorsHandler.create()
      .allowedMethod(io.vertx.core.http.HttpMethod.GET)
      .allowedMethod(io.vertx.core.http.HttpMethod.POST)
      .allowedMethod(io.vertx.core.http.HttpMethod.OPTIONS)
      .allowedHeader("Access-Control-Request-Method")
      .allowedHeader("Access-Control-Allow-Credentials")
      .allowedHeader("Access-Control-Allow-Origin")
      .allowedHeader("Access-Control-Allow-Headers")
      .allowedHeader("Content-Type"));
    router.route().handler(BodyHandler.create().setUploadsDirectory("/tmp").setBodyLimit(2L * 1024 * 1024 * 1024));

    vertx.exceptionHandler(event -> {
      log.error("An exception was caught at the top level", event);
      log.debug(event.getMessage());
    });

    getVertx().exceptionHandler(err -> log.error("An exception was caught at VertX top-level", err));

    final List<HandlersHolder> handlersHolders = new ArrayList<>();
    handlersHolders.add(new FSHandlersHolder(vertx));
    handlersHolders.add(new TileHandlersHolder(vertx));
    handlersHolders.add(new FileOpHandlersHolder(vertx));
    handlersHolders.add(new ScaffoldingOpHandlersHolder(vertx));
    handlersHolders.add(new NameMappingHandlersHolder(vertx));
    handlersHolders.add(new ConversionHandlersHolder(vertx));
    handlersHolders.add(new DotplotHandlersHolder(vertx));
    handlersHolders.add(new InfoHandlersHolder(vertx));
    handlersHolders.add(new ApiDocsHandlersHolder());
    handlersHolders.add(new TrackHandlersHolder(vertx));

    router.route().failureHandler(ctx -> {
      log.error("An exception was caught at router top-level", ctx.failure());
      final var message = ctx.failure() != null && ctx.failure().getMessage() != null
        ? ctx.failure().getMessage()
        : "Request failed";
      final int statusCode = ctx.statusCode() > 0 ? ctx.statusCode() : 500;
      ctx.response()
        .putHeader("content-type", "application/json")
        .setStatusCode(statusCode)
        .end(Json.encode(Map.of("error", message)));
    });

    log.info("Configuring router");
    handlersHolders.forEach(handlersHolder -> handlersHolder.addHandlersToRouter(router));
    return router;
  }

  private void deployWebUiVerticle() {
    log.info("Deploying WebUI Verticle");
    vertx.deployVerticle(new WebUIVerticle(), ar -> {
      if (ar.succeeded()) {
        log.info("WebUI verticle deployed");
      } else {
        log.error("WebUI verticle deployment failed", ar.cause());
      }
    });
  }
}
