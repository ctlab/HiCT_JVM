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

package ru.itmo.ctlab.hict.hict_server.handlers.examples;

import ru.itmo.ctlab.hict.hict_server.jobs.BackgroundJob;

import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.shareddata.LocalMap;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.HttpException;
import org.jetbrains.annotations.NotNull;
import ru.itmo.ctlab.hict.hict_server.HandlersHolder;
import ru.itmo.ctlab.hict.hict_server.examples.ExampleDownloader;
import ru.itmo.ctlab.hict.hict_server.examples.ExamplesCatalog;
import ru.itmo.ctlab.hict.hict_server.util.shareable.ShareableWrappers;

import java.nio.file.Path;

/**
 * {@code /examples/catalog} lists the merged catalog, and {@code /examples/download/start|status|cancel} fetch one
 * example. The client sends an example id only. The server looks the example up in the catalog it loads itself,
 * so addresses, file names and hashes are never taken from a request.
 */
public class ExamplesHandlersHolder extends HandlersHolder {
  private final Vertx vertx;
  private volatile ExampleDownloader downloader;
  private volatile Path downloaderRoot;

  public ExamplesHandlersHolder(final Vertx vertx) {
    this.vertx = vertx;
  }

  public java.util.List<BackgroundJob> backgroundJobs() {
    final var current = downloader;
    return current == null ? java.util.List.of() : current.backgroundJobs();
  }

  public boolean cancelBackgroundJob(String id) {
    final var current = downloader;
    return current != null && current.cancelBackgroundJob(id);
  }

  public void close() {
    final var current = downloader;
    if (current != null) current.close();
  }

  private Path dataDirectory() {
    final @NotNull LocalMap<String, Object> map = vertx.sharedData().getLocalMap("hict_server");
    final var wrapper = (ShareableWrappers.PathWrapper) map.get("dataDirectory");
    return wrapper == null ? null : wrapper.getPath();
  }

  private static boolean isDownloaded(final Path root, final io.vertx.core.json.JsonObject example) {
    try {
      final var folder = root.resolve("examples").resolve(example.getString("id"));
      if (!java.nio.file.Files.isRegularFile(folder.resolve("manifest.json"))) {
        return false;
      }
      for (final var entry : example.getJsonArray("files")) {
        final var file = (io.vertx.core.json.JsonObject) entry;
        if (file.getBoolean("required", true) && !java.nio.file.Files.isRegularFile(folder.resolve(file.getString("filename")))) {
          return false;
        }
      }
      final var preparation = example.getJsonObject("openingRecipe").getJsonObject("fastaPreparation");
      return preparation == null || java.nio.file.Files.isRegularFile(folder.resolve(preparation.getString("filename")));
    } catch (final RuntimeException e) {
      return false;
    }
  }

  private synchronized ExampleDownloader downloader(final Path root) {
    if (downloader == null || !root.equals(downloaderRoot)) {
      if (downloader != null) {
        downloader.close();
      }
      downloader = new ExampleDownloader(root);
      downloaderRoot = root;
    }
    return downloader;
  }

  @Override
  public void addHandlersToRouter(final @NotNull Router router) {
    router.post("/examples/catalog").handler(ctx -> {
      final var root = dataDirectory();
      final var json = ExamplesCatalog.load(root).toJson();
      // An example counts as downloaded when its manifest exists and every required file is on disk.
      for (final var entry : json.getJsonArray("examples")) {
        final var example = (io.vertx.core.json.JsonObject) entry;
        example.put("downloaded", root != null && isDownloaded(root, example));
      }
      ctx.response().putHeader("content-type", "application/json").end(json.encode());
    });
    router.post("/examples/download/start").handler(ctx -> {
      final var body = ctx.body().asJsonObject();
      final var id = body == null ? null : body.getString("exampleId");
      final var root = dataDirectory();
      if (id == null || id.isBlank() || root == null) {
        ctx.fail(new HttpException(400, "exampleId is required"));
        return;
      }
      final var example = ExamplesCatalog.load(root).find(id);
      if (example == null) {
        ctx.fail(new HttpException(404, "Unknown example " + id));
        return;
      }
      try {
        ctx.response().putHeader("content-type", "application/json").end(Json.encode(downloader(root).start(example)));
      } catch (final IllegalStateException busy) {
        ctx.fail(new HttpException(409, busy.getMessage()));
      }
    });
    router.post("/examples/download/status").handler(ctx -> {
      final var root = dataDirectory();
      ctx.response().putHeader("content-type", "application/json")
        .end(Json.encode(root == null ? null : downloader(root).status()));
    });
    router.post("/examples/download/cancel").handler(ctx -> {
      final var root = dataDirectory();
      ctx.response().putHeader("content-type", "application/json")
        .end(Json.encode(root == null ? null : downloader(root).cancel()));
    });
  }
}
