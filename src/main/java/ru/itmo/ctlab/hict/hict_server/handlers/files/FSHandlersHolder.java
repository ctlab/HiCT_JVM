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
import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.LocalMap;
import io.vertx.ext.web.Router;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import ru.itmo.ctlab.hict.hict_server.HandlersHolder;
import ru.itmo.ctlab.hict.hict_server.concurrent.RequestTaskScheduler;
import ru.itmo.ctlab.hict.hict_server.tracks.Track1DManager;
import ru.itmo.ctlab.hict.hict_server.util.cache.FileFingerprintService;
import ru.itmo.ctlab.hict.hict_server.util.cache.MatrixConversionCacheManager;
import ru.itmo.ctlab.hict.hict_server.util.shareable.ShareableWrappers;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

@RequiredArgsConstructor
@Slf4j
public class FSHandlersHolder extends HandlersHolder {
  private final Vertx vertx;
  private final FileFingerprintService fingerprintService = new FileFingerprintService();
  private static final List<String> FASTA_SUFFIXES = List.of(
    ".fasta", ".fa", ".fna", ".fas",
    ".fasta.gz", ".fa.gz", ".fna.gz", ".fas.gz"
  );
  private static final List<String> CONVERTIBLE_MATRIX_SUFFIXES = List.of(
    ".hic", ".cool", ".mcool"
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

    router.post("/list_files_detailed").handler(ctx -> {
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
              .map(path -> toDetailedFileEntry(dataDirectory, path))
              .sorted(java.util.Comparator.comparing(FileEntry::path))
              .toList();
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

    router.post("/list_coolers").handler(this::handleConvertibleMatrixList);
    router.post("/list_convertible_matrices").handler(this::handleConvertibleMatrixList);

    router.post("/resolve_matrix_source").handler(ctx -> {
      final var scheduler = getScheduler(ctx);
      if (scheduler == null) {
        return;
      }
      scheduler.submit(
        ctx,
        RequestTaskScheduler.RequestPriority.UI_UX,
        null,
        () -> {
          final var request = ctx.body() != null && ctx.body().asJsonObject() != null
            ? ctx.body().asJsonObject()
            : new JsonObject();
          final var filename = request.getString("filename");
          if (filename == null || filename.isBlank()) {
            throw new IllegalArgumentException("filename is required");
          }
          return cacheManager().resolveOpenPath(filename).toJson();
        },
        response -> ctx.response().putHeader("content-type", "application/json").end(response.encode())
      );
    });

    router.post("/cache/drop_all").handler(ctx -> {
      final var scheduler = getScheduler(ctx);
      if (scheduler == null) {
        return;
      }
      scheduler.submit(
        ctx,
        RequestTaskScheduler.RequestPriority.ASSEMBLY,
        null,
        () -> {
          final var cacheManager = cacheManager();
          final var matrixDeleted = cacheManager.dropAllMetadata();
          final var processedDirectory = processedDirectory();
          final var trackPrecomputeDir = processedDirectory.resolve("track_precompute");
          final var trackDeleted = deleteRecursively(trackPrecomputeDir);
          final @NotNull LocalMap<String, Object> map = vertx.sharedData().getLocalMap("hict_server");
          final var trackManagerWrapper = (ShareableWrappers.Track1DManagerWrapper) map.get("Track1DManager");
          if (trackManagerWrapper != null) {
            final Track1DManager manager = trackManagerWrapper.getTrack1DManager();
            manager.invalidateInMemoryCache();
            manager.clearPrecomputeStatus();
          }
          return new JsonObject()
            .put("status", "dropped")
            .put("matrixMetadataDeleted", matrixDeleted)
            .put("trackCacheEntriesDeleted", trackDeleted);
        },
        response -> ctx.response().putHeader("content-type", "application/json").end(response.encode())
      );
    });
  }

  private void handleConvertibleMatrixList(final @NotNull io.vertx.ext.web.RoutingContext ctx) {
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
            .filter(FSHandlersHolder::isConvertibleMatrixFilename)
            .collect(Collectors.toList());
        } catch (final IOException e) {
          throw new RuntimeException(e);
        }
      },
      files -> ctx.response().putHeader("content-type", "application/json").end(Json.encode(files))
    );
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

  private static boolean isConvertibleMatrixFilename(final @NotNull String path) {
    final var lowered = path.toLowerCase();
    return CONVERTIBLE_MATRIX_SUFFIXES.stream().anyMatch(lowered::endsWith);
  }

  private static @NotNull FileEntry toDetailedFileEntry(final @NotNull java.nio.file.Path dataDirectory,
                                                        final @NotNull java.nio.file.Path path) {
    final var relative = dataDirectory.relativize(path).toString();
    final var fileName = path.getFileName() == null ? relative : path.getFileName().toString();
    final var lowered = fileName.toLowerCase();
    final int dotIndex = lowered.lastIndexOf('.');
    final var extension = dotIndex >= 0 ? lowered.substring(dotIndex) : "";
    try {
      final var attrs = Files.readAttributes(path, java.nio.file.attribute.BasicFileAttributes.class);
      return new FileEntry(relative, fileName, attrs.size(), attrs.lastModifiedTime().toMillis(), extension);
    } catch (final IOException e) {
      return new FileEntry(relative, fileName, -1L, 0L, extension);
    }
  }

  private record FileEntry(@NotNull String path,
                           @NotNull String name,
                           long sizeBytes,
                           long modifiedAtMs,
                           @NotNull String extension) {
  }

  private @NotNull MatrixConversionCacheManager cacheManager() {
    return new MatrixConversionCacheManager(dataDirectory(), processedDirectory(), this.fingerprintService);
  }

  private @NotNull Path dataDirectory() {
    final var dataDirectoryWrapper = (ShareableWrappers.PathWrapper) vertx.sharedData().getLocalMap("hict_server").get("dataDirectory");
    if (dataDirectoryWrapper == null) {
      throw new RuntimeException("Data directory is not present in local map");
    }
    return dataDirectoryWrapper.getPath();
  }

  private @NotNull Path processedDirectory() {
    final var processedDirectoryWrapper =
      (ShareableWrappers.PathWrapper) vertx.sharedData().getLocalMap("hict_server").get("processedDirectory");
    return processedDirectoryWrapper != null
      ? processedDirectoryWrapper.getPath()
      : dataDirectory().resolve("processed").normalize().toAbsolutePath();
  }

  private static int deleteRecursively(final @NotNull Path root) {
    if (!Files.exists(root)) {
      return 0;
    }
    final var deletedCount = new int[]{0};
    try (final var stream = Files.walk(root)) {
      stream.sorted(java.util.Comparator.reverseOrder()).forEach(path -> {
        try {
          Files.deleteIfExists(path);
          deletedCount[0]++;
        } catch (final IOException e) {
          throw new RuntimeException("Failed to delete cache path " + path, e);
        }
      });
      return deletedCount[0];
    } catch (final IOException e) {
      throw new RuntimeException("Failed to delete cache tree " + root, e);
    }
  }
}
