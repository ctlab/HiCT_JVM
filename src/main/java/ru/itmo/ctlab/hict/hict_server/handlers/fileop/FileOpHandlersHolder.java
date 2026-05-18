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

package ru.itmo.ctlab.hict.hict_server.handlers.fileop;

import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;
import io.vertx.core.shareddata.LocalMap;
import io.vertx.ext.web.Router;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.Initializers;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_server.HandlersHolder;
import ru.itmo.ctlab.hict.hict_server.concurrent.RequestTaskScheduler;
import ru.itmo.ctlab.hict.hict_server.dto.response.assembly.AssemblyInfoDTO;
import ru.itmo.ctlab.hict.hict_server.dto.response.fasta.FastaLinkResponseDTO;
import ru.itmo.ctlab.hict.hict_server.dto.response.fileop.OpenFileResponseDTO;
import ru.itmo.ctlab.hict.hict_server.handlers.tiles.TileHandlersHolder;
import ru.itmo.ctlab.hict.hict_server.handlers.util.TileStatisticHolder;
import ru.itmo.ctlab.hict.hict_server.tracks.Track1DManager;
import ru.itmo.ctlab.hict.hict_server.util.shareable.ShareableWrappers;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;

@RequiredArgsConstructor
@Slf4j
public class FileOpHandlersHolder extends HandlersHolder {
  private final Vertx vertx;
  private static final String PRIMARY_CHUNKED_FILE_KEY = "chunkedFile";
  private static final String SECONDARY_CHUNKED_FILE_KEY = "chunkedFileSecondary";
  private static final String OPENED_SECONDARY_FILENAME_KEY = "openedSecondaryFilename";
  private static final String SECONDARY_COMPATIBILITY_KEY = "secondarySourceCompatibility";
  private static final String ASSEMBLY_SOURCE_KEY = "assemblyInfoSource";
  private static final String ASSEMBLY_SOURCE_PRIMARY = "PRIMARY";
  private static final String ASSEMBLY_SOURCE_SECONDARY = "SECONDARY";
  private static final String PRIMARY_FASTA_PATH_KEY = "linkedFastaPrimaryPath";
  private static final String PRIMARY_FASTA_FILENAME_KEY = "linkedFastaPrimaryFilename";
  private static final String SECONDARY_FASTA_PATH_KEY = "linkedFastaSecondaryPath";
  private static final String SECONDARY_FASTA_FILENAME_KEY = "linkedFastaSecondaryFilename";
  private record JsonRouteResult(int statusCode, @NotNull io.vertx.core.json.JsonObject payload) {}

  @Override
  public void addHandlersToRouter(final @NotNull Router router) {
    router.post("/open").handler(ctx -> {
      final var scheduler = getScheduler(ctx);
      if (scheduler == null) {
        return;
      }
      scheduler.submit(
        ctx,
        RequestTaskScheduler.RequestPriority.ASSEMBLY,
        null,
        () -> {
          final var dataDirectoryWrapper = (ShareableWrappers.PathWrapper) vertx.sharedData().getLocalMap("hict_server").get("dataDirectory");
          if (dataDirectoryWrapper == null) {
            throw new RuntimeException("Data directory is not present in local map");
          }
          final var dataDirectory = dataDirectoryWrapper.getPath();

          final @NotNull var requestBody = ctx.body();
          final @NotNull var requestJSON = requestBody.asJsonObject();

          final @Nullable var filename = requestJSON.getString("filename");
          final @Nullable var fastaFilename = requestJSON.getString("fastaFilename");

          log.debug("Got filename: {} and FASTA filename: {}", filename, fastaFilename);
          if (filename == null) {
            throw new RuntimeException("Filename must be specified to open the file");
          }
          final boolean verbose = Boolean.parseBoolean(System.getProperty("HICT_VERBOSE", "false"));
          if (verbose) {
            log.info("Opening file {}", filename);
          }

          final @NotNull @NonNull LocalMap<String, Object> map = vertx.sharedData().getLocalMap("hict_server");
          closeChunkedFileWrapper((ShareableWrappers.ChunkedFileWrapper) map.get(PRIMARY_CHUNKED_FILE_KEY));
          map.remove(PRIMARY_CHUNKED_FILE_KEY);
          closeChunkedFileWrapper((ShareableWrappers.ChunkedFileWrapper) map.get(SECONDARY_CHUNKED_FILE_KEY));
          map.remove(SECONDARY_CHUNKED_FILE_KEY);
          map.remove(OPENED_SECONDARY_FILENAME_KEY);
          map.remove(SECONDARY_COMPATIBILITY_KEY);
          TileHandlersHolder.clearExpectedProfileCache(map);
          map.put(ASSEMBLY_SOURCE_KEY, ASSEMBLY_SOURCE_PRIMARY);

          final var oldTrackManagerWrapper = (ShareableWrappers.Track1DManagerWrapper) map.get("Track1DManager");
          if (oldTrackManagerWrapper != null) {
            oldTrackManagerWrapper.getTrack1DManager().setLinkedFastaAliasesBySource(java.util.Map.of());
            oldTrackManagerWrapper.getTrack1DManager().close();
          }
          map.remove("linkedFastaPath");
          map.remove("linkedFastaFilename");
          map.remove(PRIMARY_FASTA_PATH_KEY);
          map.remove(PRIMARY_FASTA_FILENAME_KEY);
          map.remove(SECONDARY_FASTA_PATH_KEY);
          map.remove(SECONDARY_FASTA_FILENAME_KEY);

          map.put("openProgress", new io.vertx.core.json.JsonObject()
            .put("stage", "starting")
            .put("progress", 0.0));

          final var chunkedFile = Initializers.withProgressReporter((stage, progressValue) -> {
            map.put("openProgress", new io.vertx.core.json.JsonObject()
              .put("stage", stage)
              .put("progress", progressValue));
            if (verbose) {
              log.info(String.format("Open progress: %s (%.1f%%)", stage, progressValue * 100.0));
            }
          }, () -> new ChunkedFile(
            new ChunkedFile.ChunkedFileOptions(
              Path.of(dataDirectory.toString(), filename),
              (int) map.getOrDefault("MIN_DS_POOL", 4),
              (int) map.getOrDefault("MAX_DS_POOL", 16)
            )
          ));
          final var chunkedFileWrapper = new ShareableWrappers.ChunkedFileWrapper(chunkedFile);
          log.info("Putting chunkedFile into the local map");
          map.put(PRIMARY_CHUNKED_FILE_KEY, chunkedFileWrapper);
          map.put("openedFilename", filename);
          map.put("TileStatisticHolder", TileStatisticHolder.newDefaultStatisticHolder(chunkedFile.getResolutions().length));

          final var processedDirectoryWrapper = (ShareableWrappers.PathWrapper) map.get("processedDirectory");
          final var processedDirectory = processedDirectoryWrapper != null
            ? processedDirectoryWrapper.getPath()
            : dataDirectory.resolve("processed").normalize().toAbsolutePath();
          map.put("Track1DManager", new ShareableWrappers.Track1DManagerWrapper(new Track1DManager(dataDirectory, processedDirectory)));

          final var schedulerWrapper = (ShareableWrappers.RequestTaskSchedulerWrapper) map.get(RequestTaskScheduler.LOCAL_MAP_KEY);
          if (schedulerWrapper != null) {
            schedulerWrapper.getRequestTaskScheduler().bumpAssemblyGeneration();
          }

          map.put("openProgress", new io.vertx.core.json.JsonObject()
            .put("stage", "done")
            .put("progress", 1.0));
          return generateOpenFileResponse(chunkedFile);
        },
        response -> ctx.response()
          .putHeader("content-type", "application/json")
          .end(Json.encode(response))
      );
    });

    router.post("/open_progress").handler(ctx -> {
      final @NotNull @NonNull LocalMap<String, Object> map = vertx.sharedData().getLocalMap("hict_server");
      final var progressObj = map.get("openProgress");
      if (!(progressObj instanceof io.vertx.core.json.JsonObject)) {
        ctx.response()
          .putHeader("content-type", "application/json")
          .end(Json.encode(new io.vertx.core.json.JsonObject()
            .put("stage", "idle")
            .put("progress", 0.0)));
        return;
      }
      ctx.response()
        .putHeader("content-type", "application/json")
        .end(((io.vertx.core.json.JsonObject) progressObj).encode());
    });

    router.post("/secondary/status").handler(ctx -> {
      final var scheduler = getScheduler(ctx);
      if (scheduler == null) {
        return;
      }
      scheduler.submit(
        ctx,
        RequestTaskScheduler.RequestPriority.UI_UX,
        null,
        () -> secondaryStatusJson(vertx.sharedData().getLocalMap("hict_server")),
        response -> ctx.response()
          .putHeader("content-type", "application/json")
          .end(response.encode())
      );
    });

    router.post("/secondary/open").handler(ctx -> {
      final var scheduler = getScheduler(ctx);
      if (scheduler == null) {
        return;
      }
      scheduler.submit(
        ctx,
        RequestTaskScheduler.RequestPriority.ASSEMBLY,
        null,
        () -> {
          final @NotNull @NonNull LocalMap<String, Object> map = vertx.sharedData().getLocalMap("hict_server");
          final var primaryWrapper = (ShareableWrappers.ChunkedFileWrapper) map.get(PRIMARY_CHUNKED_FILE_KEY);
          if (primaryWrapper == null) {
            throw new IllegalStateException("Open primary Hi-C source before attaching secondary source");
          }
          final var dataDirectoryWrapper = (ShareableWrappers.PathWrapper) map.get("dataDirectory");
          if (dataDirectoryWrapper == null) {
            throw new RuntimeException("Data directory is not present in local map");
          }
          final var requestJson = ctx.body().asJsonObject();
          final var filename = requestJson.getString("filename");
          final var allowMismatch = requestJson.getBoolean("allowMismatch", false);
          if (filename == null || filename.isBlank()) {
            throw new IllegalArgumentException("Secondary source filename is required");
          }
          final var dataDirectory = dataDirectoryWrapper.getPath();
          final var filePath = dataDirectory.resolve(filename).normalize().toAbsolutePath();
          if (!filePath.startsWith(dataDirectory)) {
            throw new IllegalArgumentException("Secondary source path " + filename + " is outside DATA_DIR");
          }
          if (!Files.exists(filePath) || !Files.isRegularFile(filePath)) {
            throw new IllegalArgumentException("Secondary source file " + filename + " does not exist");
          }
          final var secondaryChunkedFile = new ChunkedFile(
            new ChunkedFile.ChunkedFileOptions(
              filePath,
              (int) map.getOrDefault("MIN_DS_POOL", 4),
              (int) map.getOrDefault("MAX_DS_POOL", 16)
            )
          );
          final SecondaryCompatibility compatibility;
          try {
            compatibility = analyzeSecondaryCompatibility(primaryWrapper.getChunkedFile(), secondaryChunkedFile);
          } catch (final RuntimeException ex) {
            try {
              secondaryChunkedFile.close();
            } catch (final Exception ignored) {
              // no-op
            }
            throw ex;
          }
          if (!compatibility.exactMatch() && !allowMismatch) {
            try {
              secondaryChunkedFile.close();
            } catch (final Exception ignored) {
              // no-op
            }
            final var currentStatus = secondaryStatusJson(map);
            return currentStatus
              .put("requiresConfirmation", true)
              .put("requestedFilename", filename)
              .put("compatibility", compatibility.toJson())
              .put("warnings", compatibility.warningsAsJsonArray());
          }
          closeChunkedFileWrapper((ShareableWrappers.ChunkedFileWrapper) map.get(SECONDARY_CHUNKED_FILE_KEY));
          map.put(SECONDARY_CHUNKED_FILE_KEY, new ShareableWrappers.ChunkedFileWrapper(secondaryChunkedFile));
          map.put(OPENED_SECONDARY_FILENAME_KEY, filename);
          map.put(SECONDARY_COMPATIBILITY_KEY, compatibility.toJson());
          TileHandlersHolder.clearExpectedProfileCache(map);
          map.putIfAbsent(ASSEMBLY_SOURCE_KEY, ASSEMBLY_SOURCE_PRIMARY);
          final var schedulerWrapper = (ShareableWrappers.RequestTaskSchedulerWrapper) map.get(RequestTaskScheduler.LOCAL_MAP_KEY);
          if (schedulerWrapper != null) {
            schedulerWrapper.getRequestTaskScheduler().bumpGeneration(RequestTaskScheduler.CancellationDomain.TILE);
          }
          return secondaryStatusJson(map)
            .put("requiresConfirmation", false)
            .put("compatibility", compatibility.toJson())
            .put("warnings", compatibility.warningsAsJsonArray());
        },
        response -> ctx.response()
          .putHeader("content-type", "application/json")
          .end(response.encode())
      );
    });

    router.post("/secondary/close").handler(ctx -> {
      final var scheduler = getScheduler(ctx);
      if (scheduler == null) {
        return;
      }
      scheduler.submit(
        ctx,
        RequestTaskScheduler.RequestPriority.ASSEMBLY,
        null,
        () -> {
          final @NotNull @NonNull LocalMap<String, Object> map = vertx.sharedData().getLocalMap("hict_server");
          closeChunkedFileWrapper((ShareableWrappers.ChunkedFileWrapper) map.get(SECONDARY_CHUNKED_FILE_KEY));
          map.remove(SECONDARY_CHUNKED_FILE_KEY);
          map.remove(OPENED_SECONDARY_FILENAME_KEY);
          map.remove(SECONDARY_COMPATIBILITY_KEY);
          TileHandlersHolder.clearExpectedProfileCache(map);
          if (ASSEMBLY_SOURCE_SECONDARY.equalsIgnoreCase(String.valueOf(map.getOrDefault(ASSEMBLY_SOURCE_KEY, ASSEMBLY_SOURCE_PRIMARY)))) {
            map.put(ASSEMBLY_SOURCE_KEY, ASSEMBLY_SOURCE_PRIMARY);
          }
          final var schedulerWrapper = (ShareableWrappers.RequestTaskSchedulerWrapper) map.get(RequestTaskScheduler.LOCAL_MAP_KEY);
          if (schedulerWrapper != null) {
            schedulerWrapper.getRequestTaskScheduler().bumpGeneration(RequestTaskScheduler.CancellationDomain.TILE);
          }
          return secondaryStatusJson(map);
        },
        response -> ctx.response()
          .putHeader("content-type", "application/json")
          .end(response.encode())
      );
    });

    router.post("/secondary/set_assembly_source").handler(ctx -> {
      final var scheduler = getScheduler(ctx);
      if (scheduler == null) {
        return;
      }
      scheduler.submit(
        ctx,
        RequestTaskScheduler.RequestPriority.ASSEMBLY,
        null,
        () -> {
          final @NotNull @NonNull LocalMap<String, Object> map = vertx.sharedData().getLocalMap("hict_server");
          final var requestJson = ctx.body().asJsonObject();
          final var requestedSource = String.valueOf(requestJson.getString("assemblySource", ASSEMBLY_SOURCE_PRIMARY))
            .trim()
            .toUpperCase();
          final var primaryWrapper = (ShareableWrappers.ChunkedFileWrapper) map.get(PRIMARY_CHUNKED_FILE_KEY);
          if (primaryWrapper == null) {
            throw new IllegalStateException("Open primary Hi-C source first");
          }
          final var secondaryWrapper = (ShareableWrappers.ChunkedFileWrapper) map.get(SECONDARY_CHUNKED_FILE_KEY);
          final ChunkedFile sourceChunkedFile;
          final String normalizedSource;
          if (ASSEMBLY_SOURCE_SECONDARY.equals(requestedSource)) {
            if (secondaryWrapper == null) {
              throw new IllegalStateException("Secondary source is not attached");
            }
            normalizedSource = ASSEMBLY_SOURCE_SECONDARY;
            sourceChunkedFile = secondaryWrapper.getChunkedFile();
          } else {
            normalizedSource = ASSEMBLY_SOURCE_PRIMARY;
            sourceChunkedFile = primaryWrapper.getChunkedFile();
          }
          map.put(ASSEMBLY_SOURCE_KEY, normalizedSource);
          return new io.vertx.core.json.JsonObject()
            .put("assemblySource", normalizedSource)
            .put("assemblyInfo", io.vertx.core.json.JsonObject.mapFrom(AssemblyInfoDTO.generateFromChunkedFile(sourceChunkedFile)));
        },
        response -> ctx.response()
          .putHeader("content-type", "application/json")
          .end(response.encode())
      );
    });

    router.post("/attach").handler(ctx -> {
      final var scheduler = getScheduler(ctx);
      if (scheduler == null) {
        return;
      }
      scheduler.submit(
        ctx,
        RequestTaskScheduler.RequestPriority.ASSEMBLY,
        null,
        () -> {
          final @NotNull @NonNull LocalMap<String, Object> map = vertx.sharedData().getLocalMap("hict_server");
          final var chunkedFileWrapper = ((ShareableWrappers.ChunkedFileWrapper) (map.get(PRIMARY_CHUNKED_FILE_KEY)));
          if (chunkedFileWrapper == null) {
            return new JsonRouteResult(
              404,
              new io.vertx.core.json.JsonObject().put("error", "No session to attach")
            );
          }
          final var chunkedFile = chunkedFileWrapper.getChunkedFile();
          final var filename = (String) map.getOrDefault("openedFilename", "");
          final var secondaryStatus = secondaryStatusJson(map);
          return new JsonRouteResult(
            200,
            new io.vertx.core.json.JsonObject()
              .put("filename", filename)
              .put("fastaFilename", map.getOrDefault("linkedFastaFilename", map.getOrDefault(PRIMARY_FASTA_FILENAME_KEY, "")))
              .put("secondarySource", secondaryStatus)
              .put("openFileResponse", generateOpenFileResponse(chunkedFile))
          );
        },
        response -> ctx.response()
          .setStatusCode(response.statusCode())
          .putHeader("content-type", "application/json")
          .end(response.payload().encode())
      );
    });

    router.post("/close").handler(ctx -> {
      final var scheduler = getScheduler(ctx);
      if (scheduler == null) {
        return;
      }
      scheduler.submit(
        ctx,
        RequestTaskScheduler.RequestPriority.ASSEMBLY,
        null,
        () -> {
          final @NotNull @NonNull LocalMap<String, Object> map = vertx.sharedData().getLocalMap("hict_server");
          final var chunkedFileWrapper = ((ShareableWrappers.ChunkedFileWrapper) (map.get(PRIMARY_CHUNKED_FILE_KEY)));
          if (chunkedFileWrapper != null) {
            try {
              chunkedFileWrapper.getChunkedFile().close();
            } catch (Exception e) {
              log.warn("Failed to close chunked file", e);
            }
          }
          map.remove(PRIMARY_CHUNKED_FILE_KEY);
          closeChunkedFileWrapper((ShareableWrappers.ChunkedFileWrapper) map.get(SECONDARY_CHUNKED_FILE_KEY));
          map.remove(SECONDARY_CHUNKED_FILE_KEY);
          map.remove(OPENED_SECONDARY_FILENAME_KEY);
          map.remove(SECONDARY_COMPATIBILITY_KEY);
          map.put(ASSEMBLY_SOURCE_KEY, ASSEMBLY_SOURCE_PRIMARY);
          map.remove("TileStatisticHolder");
          map.remove("openedFilename");
          map.remove("linkedFastaPath");
          map.remove("linkedFastaFilename");
          map.remove(PRIMARY_FASTA_PATH_KEY);
          map.remove(PRIMARY_FASTA_FILENAME_KEY);
          map.remove(SECONDARY_FASTA_PATH_KEY);
          map.remove(SECONDARY_FASTA_FILENAME_KEY);
          final var trackManagerWrapper = (ShareableWrappers.Track1DManagerWrapper) map.remove("Track1DManager");
          if (trackManagerWrapper != null) {
            trackManagerWrapper.getTrack1DManager().setLinkedFastaAliasesBySource(java.util.Map.of());
            trackManagerWrapper.getTrack1DManager().close();
          }
          final var schedulerWrapper = (ShareableWrappers.RequestTaskSchedulerWrapper) map.get(RequestTaskScheduler.LOCAL_MAP_KEY);
          if (schedulerWrapper != null) {
            schedulerWrapper.getRequestTaskScheduler().bumpAssemblyGeneration();
          }
          return new io.vertx.core.json.JsonObject().put("status", "closed");
        },
        response -> ctx.response()
          .putHeader("content-type", "application/json")
          .end(Json.encode(response))
      );
    });

    router.post("/link_fasta").handler(ctx -> {
      final var scheduler = getScheduler(ctx);
      if (scheduler == null) {
        return;
      }
      scheduler.submit(
        ctx,
        RequestTaskScheduler.RequestPriority.ASSEMBLY,
        null,
        () -> {
          final @NotNull @NonNull LocalMap<String, Object> map = vertx.sharedData().getLocalMap("hict_server");
          final var chunkedFileWrapper = ((ShareableWrappers.ChunkedFileWrapper) (map.get("chunkedFile")));
          if (chunkedFileWrapper == null) {
            throw new IllegalStateException("Open a Hi-C file before linking FASTA");
          }
          final var dataDirectoryWrapper = (ShareableWrappers.PathWrapper) map.get("dataDirectory");
          if (dataDirectoryWrapper == null) {
            throw new RuntimeException("Data directory is not present in local map");
          }
          final var requestJSON = ctx.body().asJsonObject();
          final var fastaFilename = requestJSON.getString("fastaFilename");
          final boolean allowMismatch = requestJSON.getBoolean("allowMismatch", false);
          final var requestedSource = String.valueOf(requestJSON.getString("source", ASSEMBLY_SOURCE_PRIMARY))
            .trim()
            .toUpperCase();
          if (fastaFilename == null || fastaFilename.isBlank()) {
            throw new IllegalArgumentException("FASTA filename is required");
          }
          final ChunkedFile targetChunkedFile;
          final String normalizedSource;
          if (ASSEMBLY_SOURCE_SECONDARY.equals(requestedSource)) {
            final var secondaryWrapper = (ShareableWrappers.ChunkedFileWrapper) map.get(SECONDARY_CHUNKED_FILE_KEY);
            if (secondaryWrapper == null) {
              throw new IllegalStateException("Attach a secondary source before linking a secondary FASTA");
            }
            targetChunkedFile = secondaryWrapper.getChunkedFile();
            normalizedSource = ASSEMBLY_SOURCE_SECONDARY;
          } else {
            targetChunkedFile = chunkedFileWrapper.getChunkedFile();
            normalizedSource = ASSEMBLY_SOURCE_PRIMARY;
          }

          final Path fastaPath = dataDirectoryWrapper.getPath().resolve(fastaFilename).normalize().toAbsolutePath();
          if (!fastaPath.startsWith(dataDirectoryWrapper.getPath())) {
            throw new IllegalArgumentException("FASTA path " + fastaFilename + " is outside DATA_DIR");
          }
          if (!Files.exists(fastaPath) || !Files.isRegularFile(fastaPath)) {
            throw new IllegalArgumentException("FASTA file " + fastaFilename + " does not exist");
          }

          final var report = targetChunkedFile.getFastaProcessor().analyzeLinkCandidate(fastaPath);
          final boolean requiresConfirmation = report.hasWarnings() && !allowMismatch;
          if (!requiresConfirmation) {
            if (ASSEMBLY_SOURCE_SECONDARY.equals(normalizedSource)) {
              map.put(SECONDARY_FASTA_PATH_KEY, new ShareableWrappers.PathWrapper(fastaPath));
              map.put(SECONDARY_FASTA_FILENAME_KEY, fastaFilename);
            } else {
              map.put("linkedFastaPath", new ShareableWrappers.PathWrapper(fastaPath));
              map.put("linkedFastaFilename", fastaFilename);
              map.put(PRIMARY_FASTA_PATH_KEY, new ShareableWrappers.PathWrapper(fastaPath));
              map.put(PRIMARY_FASTA_FILENAME_KEY, fastaFilename);
              final var trackManagerWrapper = (ShareableWrappers.Track1DManagerWrapper) map.get("Track1DManager");
              if (trackManagerWrapper != null) {
                trackManagerWrapper.getTrack1DManager().setLinkedFastaAliasesBySource(
                  chunkedFileWrapper.getChunkedFile().getFastaProcessor().buildSourceNameAliases(fastaPath)
                );
              }
            }
          }
          return FastaLinkResponseDTO.fromReport(report, !requiresConfirmation, requiresConfirmation);
        },
        response -> ctx.response()
          .putHeader("content-type", "application/json")
          .end(Json.encode(response))
      );
    });

    router.post("/get_fasta_for_assembly").handler(ctx -> {
      final var scheduler = getScheduler(ctx);
      if (scheduler == null) {
        return;
      }
      scheduler.submit(
        ctx,
        RequestTaskScheduler.RequestPriority.EXPORT,
        RequestTaskScheduler.CancellationDomain.EXPORT,
        () -> {
          final @NotNull @NonNull LocalMap<String, Object> map = vertx.sharedData().getLocalMap("hict_server");
          final var source = String.valueOf(
            ctx.body() != null && ctx.body().asJsonObject() != null
              ? ctx.body().asJsonObject().getString("source", String.valueOf(map.getOrDefault(ASSEMBLY_SOURCE_KEY, ASSEMBLY_SOURCE_PRIMARY)))
              : map.getOrDefault(ASSEMBLY_SOURCE_KEY, ASSEMBLY_SOURCE_PRIMARY)
          ).trim().toUpperCase();
          final var chunkedFileWrapper = resolveChunkedFileWrapperBySource(map, source);
          final var fastaPathWrapper = resolveFastaPathWrapperBySource(map, source);
          if (fastaPathWrapper == null) {
            throw new IllegalStateException("Link a FASTA file before exporting FASTA");
          }
          return chunkedFileWrapper.getChunkedFile().getFastaProcessor().exportAssembly(fastaPathWrapper.getPath());
        },
        fasta -> ctx.response()
          .setChunked(true)
          .putHeader("Content-Type", "text/plain")
          .end(Buffer.buffer(fasta, StandardCharsets.UTF_8.name())),
        () -> ctx.response()
          .setChunked(true)
          .putHeader("Content-Type", "text/plain")
          .end(Buffer.buffer("", StandardCharsets.UTF_8.name()))
      );
    });

    router.post("/get_fasta_for_selection").handler(ctx -> {
      final var scheduler = getScheduler(ctx);
      if (scheduler == null) {
        return;
      }
      final var requestJSON = ctx.body().asJsonObject();
      final var fromBpX = requestJSON.getLong("fromBpX");
      final var fromBpY = requestJSON.getLong("fromBpY");
      final var toBpX = requestJSON.getLong("toBpX");
      final var toBpY = requestJSON.getLong("toBpY");
      if (fromBpX == null || fromBpY == null || toBpX == null || toBpY == null) {
        ctx.fail(new IllegalArgumentException("Selection coordinates must be provided"));
        return;
      }
      scheduler.submit(
        ctx,
        RequestTaskScheduler.RequestPriority.EXPORT,
        RequestTaskScheduler.CancellationDomain.EXPORT,
        () -> {
          final @NotNull @NonNull LocalMap<String, Object> map = vertx.sharedData().getLocalMap("hict_server");
          final var horizontalSource = String.valueOf(requestJSON.getString("horizontalSource", ASSEMBLY_SOURCE_PRIMARY))
            .trim()
            .toUpperCase();
          final var verticalSource = String.valueOf(
            requestJSON.getString("verticalSource", horizontalSource)
          ).trim().toUpperCase();
          final var explicitAxisSources =
            requestJSON.containsKey("horizontalSource") || requestJSON.containsKey("verticalSource");

          final var horizontalChunkedFileWrapper = resolveChunkedFileWrapperBySource(map, horizontalSource);
          final var horizontalFastaPathWrapper = resolveFastaPathWrapperBySource(map, horizontalSource);
          if (horizontalFastaPathWrapper == null) {
            throw new IllegalStateException("Link a FASTA file before exporting FASTA");
          }
          if (!explicitAxisSources) {
            return horizontalChunkedFileWrapper.getChunkedFile().getFastaProcessor().exportSelection(
              horizontalFastaPathWrapper.getPath(),
              fromBpX,
              fromBpY,
              toBpX,
              toBpY
            );
          }

          final long horizontalStart = Math.min(fromBpX, toBpX);
          final long horizontalEnd = Math.max(fromBpX, toBpX);
          final long verticalStart = Math.min(fromBpY, toBpY);
          final long verticalEnd = Math.max(fromBpY, toBpY);
          if (
            Objects.equals(horizontalSource, verticalSource) &&
              horizontalStart == verticalStart &&
              horizontalEnd == verticalEnd
          ) {
            return horizontalChunkedFileWrapper.getChunkedFile().getFastaProcessor().exportInterval(
              horizontalFastaPathWrapper.getPath(),
              horizontalStart,
              horizontalEnd,
              String.format("selection_%d_%d", horizontalStart, horizontalEnd)
            );
          }

          final var verticalChunkedFileWrapper = resolveChunkedFileWrapperBySource(map, verticalSource);
          final var verticalFastaPathWrapper = resolveFastaPathWrapperBySource(map, verticalSource);
          if (verticalFastaPathWrapper == null) {
            throw new IllegalStateException("Link a FASTA file for the vertical source before exporting FASTA");
          }
          final var horizontalFasta = horizontalChunkedFileWrapper.getChunkedFile().getFastaProcessor().exportInterval(
            horizontalFastaPathWrapper.getPath(),
            horizontalStart,
            horizontalEnd,
            String.format("selection_horizontal_%d_%d_%s", horizontalStart, horizontalEnd, horizontalSource.toLowerCase())
          );
          final var verticalFasta = verticalChunkedFileWrapper.getChunkedFile().getFastaProcessor().exportInterval(
            verticalFastaPathWrapper.getPath(),
            verticalStart,
            verticalEnd,
            String.format("selection_vertical_%d_%d_%s", verticalStart, verticalEnd, verticalSource.toLowerCase())
          );
          return horizontalFasta + verticalFasta;
        },
        fasta -> ctx.response()
          .setChunked(true)
          .putHeader("Content-Type", "text/plain")
          .end(Buffer.buffer(fasta, StandardCharsets.UTF_8.name())),
        () -> ctx.response()
          .setChunked(true)
          .putHeader("Content-Type", "text/plain")
          .end(Buffer.buffer("", StandardCharsets.UTF_8.name()))
      );
    });

    router.post("/get_agp_for_assembly").handler(ctx -> {
      final var scheduler = getScheduler(ctx);
      if (scheduler == null) {
        return;
      }
      final @NotNull var requestBody = ctx.body();
      final @NotNull var requestJSON = requestBody.asJsonObject();
      final long defaultSpacerLength = requestJSON.getLong("defaultSpacerLength", 1000L);
      scheduler.submit(
        ctx,
        RequestTaskScheduler.RequestPriority.EXPORT,
        RequestTaskScheduler.CancellationDomain.EXPORT,
        () -> {
          final @NotNull @NonNull LocalMap<String, Object> map = vertx.sharedData().getLocalMap("hict_server");
          final var chunkedFileWrapper = ((ShareableWrappers.ChunkedFileWrapper) (map.get("chunkedFile")));
          if (chunkedFileWrapper == null) {
            throw new RuntimeException("Chunked file is not present in the local map, maybe the file is not yet opened?");
          }
          final var chunkedFile = chunkedFileWrapper.getChunkedFile();

          final var buffer = Buffer.buffer();
          chunkedFile.getAgpProcessor().getAGPStream(defaultSpacerLength).sequential().forEach(s -> buffer.appendBytes(s.getBytes(StandardCharsets.UTF_8)));
          return buffer;
        },
        buffer -> ctx.response().setChunked(true).putHeader("Content-Type", "text/plain").end(buffer),
        () -> ctx.response().setChunked(true).putHeader("Content-Type", "text/plain").end(Buffer.buffer())
      );
    });

    router.post("/load_agp").handler(ctx -> {
      final var scheduler = getScheduler(ctx);
      if (scheduler == null) {
        return;
      }
      scheduler.submit(
        ctx,
        RequestTaskScheduler.RequestPriority.ASSEMBLY,
        null,
        () -> {
          final @NotNull @NonNull LocalMap<String, Object> map = vertx.sharedData().getLocalMap("hict_server");
          log.debug("Got map");
          final var chunkedFileWrapper = ((ShareableWrappers.ChunkedFileWrapper) (map.get("chunkedFile")));
          if (chunkedFileWrapper == null) {
            throw new RuntimeException("Chunked file is not present in the local map, maybe the file is not yet opened?");
          }
          final var chunkedFile = chunkedFileWrapper.getChunkedFile();
          log.debug("Got ChunkedFile from map");

          final @NotNull var requestBody = ctx.body();
          final @NotNull var requestJSON = requestBody.asJsonObject();
          final var agpFilename = Objects.requireNonNull(requestJSON.getString("agpFilename"), "AGP filename must be provided to load it.");
          final var requestedSource = String.valueOf(requestJSON.getString("source", ASSEMBLY_SOURCE_PRIMARY))
            .trim()
            .toUpperCase();
          final ShareableWrappers.ChunkedFileWrapper targetWrapper =
            resolveChunkedFileWrapperBySource(map, requestedSource);
          final var targetChunkedFile = targetWrapper.getChunkedFile();

          final var dataDirectoryWrapper = (ShareableWrappers.PathWrapper) vertx.sharedData().getLocalMap("hict_server").get("dataDirectory");
          if (dataDirectoryWrapper == null) {
            throw new RuntimeException("Data directory is not present in local map");
          }
          final var dataDirectory = dataDirectoryWrapper.getPath();
          final var agpFile = Path.of(dataDirectory.toString(), agpFilename);
          try (final var reader = Files.newBufferedReader(agpFile, StandardCharsets.UTF_8)) {
            targetChunkedFile.importAGP(reader);
          } catch (IOException | NoSuchFieldException e) {
            throw new RuntimeException(e);
          }
          final var schedulerWrapper = (ShareableWrappers.RequestTaskSchedulerWrapper) map.get(RequestTaskScheduler.LOCAL_MAP_KEY);
          if (schedulerWrapper != null) {
            schedulerWrapper.getRequestTaskScheduler().bumpAssemblyGeneration();
          }
          TileHandlersHolder.clearExpectedProfileCache(map);
          final var trackManagerWrapper = (ShareableWrappers.Track1DManagerWrapper) map.get("Track1DManager");
          if (trackManagerWrapper != null) {
            trackManagerWrapper.getTrack1DManager().invalidateInMemoryCache();
          }
          return AssemblyInfoDTO.generateFromChunkedFile(targetChunkedFile);
        },
        response -> ctx.response().end(Json.encode(response))
      );
    });
  }

  private static void closeChunkedFileWrapper(final ShareableWrappers.ChunkedFileWrapper wrapper) {
    if (wrapper == null) {
      return;
    }
    try {
      wrapper.getChunkedFile().close();
    } catch (final Exception ignored) {
      // no-op
    }
  }

  private static @NotNull ShareableWrappers.ChunkedFileWrapper resolveChunkedFileWrapperBySource(final @NotNull LocalMap<String, Object> map,
                                                                                                  final @NotNull String source) {
    final ShareableWrappers.ChunkedFileWrapper wrapper;
    if (ASSEMBLY_SOURCE_SECONDARY.equalsIgnoreCase(source)) {
      wrapper = (ShareableWrappers.ChunkedFileWrapper) map.get(SECONDARY_CHUNKED_FILE_KEY);
      if (wrapper == null) {
        throw new IllegalStateException("Secondary source is not attached");
      }
    } else {
      wrapper = (ShareableWrappers.ChunkedFileWrapper) map.get(PRIMARY_CHUNKED_FILE_KEY);
      if (wrapper == null) {
        throw new IllegalStateException("Open a Hi-C file before using FASTA/AGP operations");
      }
    }
    return wrapper;
  }

  private static ShareableWrappers.PathWrapper resolveFastaPathWrapperBySource(final @NotNull LocalMap<String, Object> map,
                                                                               final @NotNull String source) {
    if (ASSEMBLY_SOURCE_SECONDARY.equalsIgnoreCase(source)) {
      return (ShareableWrappers.PathWrapper) map.get(SECONDARY_FASTA_PATH_KEY);
    }
    final var legacy = (ShareableWrappers.PathWrapper) map.get("linkedFastaPath");
    if (legacy != null) {
      return legacy;
    }
    return (ShareableWrappers.PathWrapper) map.get(PRIMARY_FASTA_PATH_KEY);
  }

  private static @NotNull SecondaryCompatibility analyzeSecondaryCompatibility(final @NotNull ChunkedFile primary,
                                                                               final @NotNull ChunkedFile secondary) {
    final var primaryResolutions = primary.getResolutions().clone();
    final var secondaryResolutions = secondary.getResolutions().clone();
    final var primaryMatrixSizeBins = primary.getMatrixSizeBins().clone();
    final var secondaryMatrixSizeBins = secondary.getMatrixSizeBins().clone();
    return new SecondaryCompatibility(
      Arrays.equals(primaryResolutions, secondaryResolutions),
      Arrays.equals(primaryMatrixSizeBins, secondaryMatrixSizeBins),
      primaryMatrixSizeBins,
      secondaryMatrixSizeBins
    );
  }

  private io.vertx.core.json.JsonObject secondaryStatusJson(final @NotNull LocalMap<String, Object> map) {
    final var attached = map.get(SECONDARY_CHUNKED_FILE_KEY) instanceof ShareableWrappers.ChunkedFileWrapper;
    final var assemblySource = String.valueOf(map.getOrDefault(ASSEMBLY_SOURCE_KEY, ASSEMBLY_SOURCE_PRIMARY));
    final var filename = String.valueOf(map.getOrDefault(OPENED_SECONDARY_FILENAME_KEY, ""));
    final var status = new io.vertx.core.json.JsonObject()
      .put("attached", attached)
      .put("filename", attached ? filename : "")
      .put("assemblySource", assemblySource);
    final var compatibility = map.get(SECONDARY_COMPATIBILITY_KEY);
    if (attached && compatibility instanceof io.vertx.core.json.JsonObject compatibilityJson) {
      status.put("compatibility", compatibilityJson.copy());
    }
    return status;
  }

  private record SecondaryCompatibility(boolean sameResolutions,
                                        boolean sameMatrixSizes,
                                        long[] primaryMatrixSizeBins,
                                        long[] secondaryMatrixSizeBins) {
    private boolean exactMatch() {
      return sameResolutions && sameMatrixSizes;
    }

    private io.vertx.core.json.JsonArray warningsAsJsonArray() {
      final var warnings = new io.vertx.core.json.JsonArray();
      if (!sameResolutions) {
        warnings.add("Primary and secondary sources expose different resolution sets.");
      }
      if (!sameMatrixSizes) {
        warnings.add("Primary and secondary sources have different matrix sizes. Smaller source will be padded with background.");
      }
      return warnings;
    }

    private io.vertx.core.json.JsonObject toJson() {
      final var maxLength = Math.max(primaryMatrixSizeBins.length, secondaryMatrixSizeBins.length);
      final var mismatchedOrders = new io.vertx.core.json.JsonArray();
      for (int idx = 0; idx < maxLength; idx++) {
        final var primaryValue = idx < primaryMatrixSizeBins.length ? primaryMatrixSizeBins[idx] : -1L;
        final var secondaryValue = idx < secondaryMatrixSizeBins.length ? secondaryMatrixSizeBins[idx] : -1L;
        if (primaryValue != secondaryValue) {
          mismatchedOrders.add(idx);
        }
      }
      final var primaryMaxBins = Arrays.stream(primaryMatrixSizeBins).max().orElse(0L);
      final var secondaryMaxBins = Arrays.stream(secondaryMatrixSizeBins).max().orElse(0L);
      return new io.vertx.core.json.JsonObject()
        .put("sameResolutions", sameResolutions)
        .put("sameMatrixSizes", sameMatrixSizes)
        .put("exactMatch", exactMatch())
        .put("primaryMaxBins", primaryMaxBins)
        .put("secondaryMaxBins", secondaryMaxBins)
        .put("primaryBinsByResolution", Arrays.stream(primaryMatrixSizeBins).boxed().toList())
        .put("secondaryBinsByResolution", Arrays.stream(secondaryMatrixSizeBins).boxed().toList())
        .put("mismatchedResolutionOrders", mismatchedOrders);
    }
  }

  private RequestTaskScheduler getScheduler(final @NotNull io.vertx.ext.web.RoutingContext ctx) {
    final @NotNull @NonNull LocalMap<String, Object> map = vertx.sharedData().getLocalMap("hict_server");
    final var wrapper = (ShareableWrappers.RequestTaskSchedulerWrapper) map.get(RequestTaskScheduler.LOCAL_MAP_KEY);
    if (wrapper == null) {
      ctx.fail(new IllegalStateException("Request scheduler is not initialized"));
      return null;
    }
    return wrapper.getRequestTaskScheduler();
  }

  private @NotNull OpenFileResponseDTO generateOpenFileResponse(final @NotNull ChunkedFile chunkedFile) {
    final var resolutionsWithoutZero = Arrays.stream(chunkedFile.getResolutions()).skip(1L).toArray();
    ArrayUtils.reverse(resolutionsWithoutZero);
    final var matrixSizeBins = chunkedFile.getMatrixSizeBins().clone();
    ArrayUtils.reverse(matrixSizeBins);
    final long minResolution = Arrays.stream(resolutionsWithoutZero).min().orElse(1L);
//    Arrays.stream(chunkedFile.getMatrixSizeBins()).forEachOrdered(i -> log.debug("New resolutrion matrix size bins: " + i));
    return new OpenFileResponseDTO(
      "Opened",
      (String) vertx.sharedData().getLocalMap("hict_server").getOrDefault("transport_dtype", "uint8"),
      Arrays.stream(resolutionsWithoutZero).boxed().toList(),
      Arrays.stream(resolutionsWithoutZero).mapToDouble(r -> (double) r / minResolution).boxed().toList(),
      chunkedFile.getDenseBlockSize(),
      AssemblyInfoDTO.generateFromChunkedFile(chunkedFile),
      Arrays.stream(matrixSizeBins).limit(matrixSizeBins.length - 1).mapToInt(l -> (int) l).boxed().toList()
    );
  }
}
