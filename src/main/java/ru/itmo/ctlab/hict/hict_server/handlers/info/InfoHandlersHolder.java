package ru.itmo.ctlab.hict.hict_server.handlers.info;

import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.shareddata.LocalMap;
import io.vertx.ext.web.Router;
import org.jetbrains.annotations.NotNull;
import ru.itmo.ctlab.hict.hict_server.HandlersHolder;
import ru.itmo.ctlab.hict.hict_server.concurrent.RequestTaskScheduler;
import ru.itmo.ctlab.hict.hict_server.util.shareable.ShareableWrappers;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

public class InfoHandlersHolder extends HandlersHolder {
  private final @NotNull Vertx vertx;

  public InfoHandlersHolder(final @NotNull Vertx vertx) {
    this.vertx = vertx;
  }

  @Override
  public void addHandlersToRouter(final @NotNull Router router) {
    router.get("/version").handler(ctx -> {
      final var version = readVersion();
      final var webuiVersion = readWebUiVersion();
      ctx.response()
        .putHeader("content-type", "application/json")
        .setStatusCode(200)
        .end(Json.encode(Map.of(
          "version", version,
          "webuiVersion", webuiVersion
        )));
    });

    router.post("/diagnostics/workers").handler(ctx -> {
      final @NotNull LocalMap<String, Object> map = this.vertx.sharedData().getLocalMap("hict_server");
      final var schedulerWrapper =
        (ShareableWrappers.RequestTaskSchedulerWrapper) map.get(RequestTaskScheduler.LOCAL_MAP_KEY);
      if (schedulerWrapper == null) {
        ctx.fail(new IllegalStateException("Request scheduler is not initialized"));
        return;
      }
      final var snapshot = schedulerWrapper.getRequestTaskScheduler().diagnosticsSnapshot();
      ctx.response()
        .putHeader("content-type", "application/json")
        .setStatusCode(200)
        .end(Json.encode(snapshot));
    });
  }

  private @NotNull String readVersion() {
    final var systemProp = System.getProperty("hict.version");
    if (systemProp != null && !systemProp.isBlank()) {
      return systemProp.trim();
    }
    try {
      final var versionPath = Path.of("version.txt");
      if (Files.exists(versionPath)) {
        return Files.readString(versionPath).trim();
      }
    } catch (final Exception ignored) {
      // ignore
    }
    try (final InputStream stream = getClass().getResourceAsStream("/version.txt")) {
      if (stream != null) {
        try (final BufferedReader reader =
               new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
          final var line = reader.readLine();
          if (line != null && !line.isBlank()) {
            return line.trim();
          }
        }
      }
    } catch (final Exception ignored) {
      // ignore
    }
    return "unknown";
  }

  private @NotNull String readWebUiVersion() {
    try (final InputStream stream = getClass().getResourceAsStream("/webui-package.json")) {
      if (stream != null) {
        final var json = new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        final var marker = "\"version\"";
        final var idx = json.indexOf(marker);
        if (idx >= 0) {
          final var colon = json.indexOf(':', idx);
          final var quoteStart = json.indexOf('"', colon + 1);
          final var quoteEnd = json.indexOf('"', quoteStart + 1);
          if (quoteStart >= 0 && quoteEnd > quoteStart) {
            return json.substring(quoteStart + 1, quoteEnd);
          }
        }
      }
    } catch (final Exception ignored) {
      // ignore
    }
    return "unknown";
  }
}
