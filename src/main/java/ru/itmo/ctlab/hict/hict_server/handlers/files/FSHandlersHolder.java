/*
 * MIT License
 *
 * Copyright (c) 2024. Aleksandr Serdiukov, Anton Zamyatin, Aleksandr Sinitsyn, Vitalii Dravgelis and Computer Technologies Laboratory ITMO University team.
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
import io.vertx.ext.web.Router;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import ru.itmo.ctlab.hict.hict_server.HandlersHolder;
import ru.itmo.ctlab.hict.hict_server.util.shareable.ShareableWrappers;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.stream.Collectors;

@RequiredArgsConstructor
@Slf4j
public class FSHandlersHolder extends HandlersHolder {
  private final Vertx vertx;

  @Override
  public void addHandlersToRouter(final @NotNull Router router) {
    router.post("/list_files").blockingHandler(ctx -> {
      final var dataDirectoryWrapper = (ShareableWrappers.PathWrapper) vertx.sharedData().getLocalMap("hict_server").get("dataDirectory");
      if (dataDirectoryWrapper == null) {
        ctx.fail(new RuntimeException("Data directory is not present in local map"));
        return;
      }
      final var dataDirectory = dataDirectoryWrapper.getPath();

      final List<?> files;
      try (final var fileStream = Files.walk(dataDirectory)) {
        files = fileStream.filter(Files::isRegularFile).map(dataDirectory::relativize).map(Object::toString).collect(Collectors.toList());
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
      ctx.response().putHeader("content-type", "application/json").end(Json.encode(files));
    });

    router.post("/list_agp_files").blockingHandler(ctx -> {
      final var dataDirectoryWrapper = (ShareableWrappers.PathWrapper) vertx.sharedData().getLocalMap("hict_server").get("dataDirectory");
      if (dataDirectoryWrapper == null) {
        ctx.fail(new RuntimeException("Data directory is not present in local map"));
        return;
      }
      final var dataDirectory = dataDirectoryWrapper.getPath();

      final List<?> files;
      try (final var fileStream = Files.walk(dataDirectory)) {
        files = fileStream.filter(Files::isRegularFile).map(dataDirectory::relativize).map(Object::toString).filter(p -> p.toLowerCase().endsWith(".agp")).collect(Collectors.toList());
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
      ctx.response().putHeader("content-type", "application/json").end(Json.encode(files));
    });

    router.post("/list_fasta_files").blockingHandler(ctx -> {
      final var dataDirectoryWrapper = (ShareableWrappers.PathWrapper) vertx.sharedData().getLocalMap("hict_server").get("dataDirectory");
      if (dataDirectoryWrapper == null) {
        ctx.fail(new RuntimeException("Data directory is not present in local map"));
        return;
      }
      final var dataDirectory = dataDirectoryWrapper.getPath();

      final List<?> files;
      try (final var fileStream = Files.walk(dataDirectory)) {
        files = fileStream.filter(Files::isRegularFile).map(dataDirectory::relativize).map(Object::toString).filter(p -> p.toLowerCase().endsWith(".fasta")).collect(Collectors.toList());
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
      ctx.response().putHeader("content-type", "application/json").end(Json.encode(files));
    });

    router.post("/list_coolers").blockingHandler(ctx -> {
      final var dataDirectoryWrapper = (ShareableWrappers.PathWrapper) vertx.sharedData().getLocalMap("hict_server").get("dataDirectory");
      if (dataDirectoryWrapper == null) {
        ctx.fail(new RuntimeException("Data directory is not present in local map"));
        return;
      }
      final var dataDirectory = dataDirectoryWrapper.getPath();

      final List<?> files;
      try (final var fileStream = Files.walk(dataDirectory)) {
        files = fileStream.filter(Files::isRegularFile).map(dataDirectory::relativize).map(Object::toString).filter(p -> p.toLowerCase().endsWith(".cool") || p.toLowerCase().endsWith(".mcool")).collect(Collectors.toList());
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
      ctx.response().putHeader("content-type", "application/json").end(Json.encode(files));
    });
  }
}
