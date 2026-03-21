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

package ru.itmo.ctlab.hict.hict_server.handlers.files;

import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.shareddata.LocalMap;
import io.vertx.ext.web.Router;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import ru.itmo.ctlab.hict.hict_server.HandlersHolder;
import ru.itmo.ctlab.hict.hict_server.concurrent.RequestTaskScheduler;
import ru.itmo.ctlab.hict.hict_server.util.shareable.ShareableWrappers;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.stream.Collectors;

@RequiredArgsConstructor
@Slf4j
public class FSHandlersHolder extends HandlersHolder {
  private final Vertx vertx;
  private static final List<String> FASTA_SUFFIXES = List.of(
    ".fasta", ".fa", ".fna", ".fas",
    ".fasta.gz", ".fa.gz", ".fna.gz", ".fas.gz"
  );

  @Override
  public void addHandlersToRouter(final @NotNull Router router) {
    router.post("/list_files").handler(ctx -> {
      final var scheduler = getScheduler(ctx);
      if (scheduler == null) {
        return;
      }
      scheduler.submit(
        ctx,
        RequestTaskScheduler.RequestPriority.UI_UX,
        null,
        () -> {
          final var dataDirectoryWrapper = (ShareableWrappers.PathWrapper) vertx.sharedData().getLocalMap("hict_server").get("dataDirectory");
          if (dataDirectoryWrapper == null) {
            throw new RuntimeException("Data directory is not present in local map");
          }
          final var dataDirectory = dataDirectoryWrapper.getPath();

          try (final var fileStream = Files.walk(dataDirectory)) {
            return fileStream.filter(Files::isRegularFile).map(dataDirectory::relativize).map(Object::toString).collect(Collectors.toList());
          } catch (final IOException e) {
            throw new RuntimeException(e);
          }
        },
        files -> ctx.response().putHeader("content-type", "application/json").end(Json.encode(files))
      );
    });

    router.post("/list_agp_files").handler(ctx -> {
      final var scheduler = getScheduler(ctx);
      if (scheduler == null) {
        return;
      }
      scheduler.submit(
        ctx,
        RequestTaskScheduler.RequestPriority.UI_UX,
        null,
        () -> {
          final var dataDirectoryWrapper = (ShareableWrappers.PathWrapper) vertx.sharedData().getLocalMap("hict_server").get("dataDirectory");
          if (dataDirectoryWrapper == null) {
            throw new RuntimeException("Data directory is not present in local map");
          }
          final var dataDirectory = dataDirectoryWrapper.getPath();

          try (final var fileStream = Files.walk(dataDirectory)) {
            return fileStream.filter(Files::isRegularFile).map(dataDirectory::relativize).map(Object::toString).filter(p -> p.toLowerCase().endsWith(".agp")).collect(Collectors.toList());
          } catch (final IOException e) {
            throw new RuntimeException(e);
          }
        },
        files -> ctx.response().putHeader("content-type", "application/json").end(Json.encode(files))
      );
    });

    router.post("/list_fasta_files").handler(ctx -> {
      final var scheduler = getScheduler(ctx);
      if (scheduler == null) {
        return;
      }
      scheduler.submit(
        ctx,
        RequestTaskScheduler.RequestPriority.UI_UX,
        null,
        () -> {
          final var dataDirectoryWrapper = (ShareableWrappers.PathWrapper) vertx.sharedData().getLocalMap("hict_server").get("dataDirectory");
          if (dataDirectoryWrapper == null) {
            throw new RuntimeException("Data directory is not present in local map");
          }
          final var dataDirectory = dataDirectoryWrapper.getPath();

          try (final var fileStream = Files.walk(dataDirectory)) {
            return fileStream
              .filter(Files::isRegularFile)
              .map(dataDirectory::relativize)
              .map(Object::toString)
              .filter(FSHandlersHolder::isFastaFilename)
              .collect(Collectors.toList());
          } catch (final IOException e) {
            throw new RuntimeException(e);
          }
        },
        files -> ctx.response().putHeader("content-type", "application/json").end(Json.encode(files))
      );
    });

    router.post("/list_coolers").handler(ctx -> {
      final var scheduler = getScheduler(ctx);
      if (scheduler == null) {
        return;
      }
      scheduler.submit(
        ctx,
        RequestTaskScheduler.RequestPriority.UI_UX,
        null,
        () -> {
          final var dataDirectoryWrapper = (ShareableWrappers.PathWrapper) vertx.sharedData().getLocalMap("hict_server").get("dataDirectory");
          if (dataDirectoryWrapper == null) {
            throw new RuntimeException("Data directory is not present in local map");
          }
          final var dataDirectory = dataDirectoryWrapper.getPath();

          try (final var fileStream = Files.walk(dataDirectory)) {
            return fileStream.filter(Files::isRegularFile).map(dataDirectory::relativize).map(Object::toString).filter(p -> p.toLowerCase().endsWith(".cool") || p.toLowerCase().endsWith(".mcool")).collect(Collectors.toList());
          } catch (final IOException e) {
            throw new RuntimeException(e);
          }
        },
        files -> ctx.response().putHeader("content-type", "application/json").end(Json.encode(files))
      );
    });
  }

  private RequestTaskScheduler getScheduler(final @NotNull io.vertx.ext.web.RoutingContext ctx) {
    final @NotNull LocalMap<String, Object> map = vertx.sharedData().getLocalMap("hict_server");
    final var wrapper = (ShareableWrappers.RequestTaskSchedulerWrapper) map.get(RequestTaskScheduler.LOCAL_MAP_KEY);
    if (wrapper == null) {
      ctx.fail(new IllegalStateException("Request scheduler is not initialized"));
      return null;
    }
    return wrapper.getRequestTaskScheduler();
  }

  private static boolean isFastaFilename(final @NotNull String path) {
    final var lowered = path.toLowerCase();
    return FASTA_SUFFIXES.stream().anyMatch(lowered::endsWith);
  }
}
