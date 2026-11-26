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

import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.FileSystemAccess;
import io.vertx.ext.web.handler.StaticHandler;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Pattern;

@Slf4j
final class WebUiRoutes {
  private static final Pattern HASHED_ASSET = Pattern.compile("^/assets/[^/]+\\.[0-9a-f]{8,}\\.[^/]+$");

  private WebUiRoutes() {}

  static void mount(final Router router) {
    router.route("/").method(HttpMethod.GET).method(HttpMethod.HEAD)
      .handler(ctx -> ctx.reroute("/index.html"));
    router.route("/*").method(HttpMethod.GET).method(HttpMethod.HEAD).handler(ctx -> {
      final var cacheControl = HASHED_ASSET.matcher(ctx.normalizedPath()).matches()
        ? "public, max-age=31536000, immutable"
        : "no-cache";
      ctx.response().putHeader("Cache-Control", cacheControl);
      ctx.next();
    }).handler(createWebuiStaticHandler());
  }

  private static @NotNull StaticHandler createWebuiStaticHandler() {
    final var explicitRoot = System.getProperty("WEBUI_ROOT", System.getenv("WEBUI_ROOT"));
    if (explicitRoot != null && !explicitRoot.isBlank()) {
      final var explicitRootPath = Path.of(explicitRoot).toAbsolutePath().normalize();
      if (Files.isDirectory(explicitRootPath)) {
        log.info("Serving WebUI from WEBUI_ROOT={}", explicitRootPath);
        return disableStaticCaching(StaticHandler.create(FileSystemAccess.ROOT, explicitRootPath.toString()));
      }
      log.warn("WEBUI_ROOT is set but does not exist: {}", explicitRootPath);
    }

    final var localDist = Path.of("../HiCT_WebUI/dist").toAbsolutePath().normalize();
    if (Files.isDirectory(localDist)) {
      log.info("Serving WebUI from local checkout: {}", localDist);
      return disableStaticCaching(StaticHandler.create(FileSystemAccess.ROOT, localDist.toString()));
    }

    final var builtCloneDist = Path.of("build/webui/HiCT_WebUI/dist").toAbsolutePath().normalize();
    if (Files.isDirectory(builtCloneDist)) {
      log.info("Serving WebUI from gradle clone output: {}", builtCloneDist);
      return disableStaticCaching(StaticHandler.create(FileSystemAccess.ROOT, builtCloneDist.toString()));
    }

    log.info("Serving WebUI from classpath resources: webui");
    return disableStaticCaching(StaticHandler.create("webui"));
  }

  private static @NotNull StaticHandler disableStaticCaching(final @NotNull StaticHandler handler) {
    return handler.setCachingEnabled(false).setMaxAgeSeconds(0);
  }
}
