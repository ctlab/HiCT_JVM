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
import ru.itmo.ctlab.hict.hict_library.assembly.AGPProcessor;
import ru.itmo.ctlab.hict.hict_library.assembly.AssemblyLayoutConverter;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.Initializers;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.resolution.ResolutionDescriptor;
import ru.itmo.ctlab.hict.hict_library.domain.ContigDescriptor;
import ru.itmo.ctlab.hict.hict_library.domain.ContigHideType;
import ru.itmo.ctlab.hict.hict_library.domain.QueryLengthUnit;
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
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
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
          if (filename == null || filename.isBlank()) {
            throw new IllegalArgumentException("Filename must be specified to open the file");
          }
          final var requestedFilename = filename.trim();
          final var dataRoot = dataDirectory.normalize().toAbsolutePath();
          final var requestedFilePath = dataRoot.resolve(requestedFilename).normalize().toAbsolutePath();
          if (!requestedFilePath.startsWith(dataRoot)) {
            throw new IllegalArgumentException("File path must stay inside the configured data directory: " + requestedFilename);
          }
          if (!Files.isRegularFile(requestedFilePath)) {
            throw new IllegalArgumentException("Path is not a regular HiCT file: " + requestedFilename);
          }
          final boolean verbose = Boolean.parseBoolean(System.getProperty("HICT_VERBOSE", "false"));
          if (verbose) {
            log.info("Opening file {}", requestedFilename);
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
              requestedFilePath,
              (int) map.getOrDefault("MIN_DS_POOL", 4),
              (int) map.getOrDefault("MAX_DS_POOL", 16)
            )
          ));
          final var chunkedFileWrapper = new ShareableWrappers.ChunkedFileWrapper(chunkedFile);
          log.info("Putting chunkedFile into the local map");
          map.put(PRIMARY_CHUNKED_FILE_KEY, chunkedFileWrapper);
          map.put("openedFilename", requestedFilename);
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
          map.putIfAbsent(ASSEMBLY_SOURCE_KEY, ASSEMBLY_SOURCE_PRIMARY);
          synchronizeOverlayAssembly(map, String.valueOf(map.getOrDefault(ASSEMBLY_SOURCE_KEY, ASSEMBLY_SOURCE_PRIMARY)));
          final var synchronizedCompatibility = refreshSecondaryCompatibility(map);
          TileHandlersHolder.clearExpectedProfileCache(map);
          final var schedulerWrapper = (ShareableWrappers.RequestTaskSchedulerWrapper) map.get(RequestTaskScheduler.LOCAL_MAP_KEY);
          if (schedulerWrapper != null) {
            schedulerWrapper.getRequestTaskScheduler().bumpGeneration(RequestTaskScheduler.CancellationDomain.TILE);
          }
          return secondaryStatusJson(map)
            .put("requiresConfirmation", false)
            .put("compatibility", synchronizedCompatibility != null ? synchronizedCompatibility.toJson() : compatibility.toJson())
            .put("warnings", synchronizedCompatibility != null ? synchronizedCompatibility.warningsAsJsonArray() : compatibility.warningsAsJsonArray());
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
          synchronizeOverlayAssembly(map, normalizedSource);
          final var synchronizedCompatibility = refreshSecondaryCompatibility(map);
          TileHandlersHolder.clearExpectedProfileCache(map);
          final var schedulerWrapper = (ShareableWrappers.RequestTaskSchedulerWrapper) map.get(RequestTaskScheduler.LOCAL_MAP_KEY);
          if (schedulerWrapper != null) {
            schedulerWrapper.getRequestTaskScheduler().bumpAssemblyGeneration();
          }
          return new io.vertx.core.json.JsonObject()
            .put("assemblySource", normalizedSource)
            .put("assemblyInfo", io.vertx.core.json.JsonObject.mapFrom(AssemblyInfoDTO.generateFromChunkedFile(sourceChunkedFile)))
            .put("compatibility", synchronizedCompatibility != null ? synchronizedCompatibility.toJson() : null);
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

          log.info("Linking {} FASTA file {}", normalizedSource, fastaPath);
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
            log.info("Linked {} FASTA file {} warnings={}", normalizedSource, fastaPath, report.warnings().size());
          } else {
            log.info("FASTA link for {} needs confirmation: {} warnings={}", normalizedSource, fastaPath, report.warnings().size());
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
          log.info("Exporting assembly FASTA for {} from {}", source, fastaPathWrapper.getPath());
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
      if (ctx.body() == null || ctx.body().asJsonObject() == null) {
        ctx.fail(new IllegalArgumentException("Selection FASTA export requires a JSON request body"));
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
          log.info(
            "Exporting selection FASTA: horizontal={} vertical={} x={}..{} y={}..{}",
            horizontalSource,
            verticalSource,
            fromBpX,
            toBpX,
            fromBpY,
            toBpY
          );
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
          final var agpFilename = Objects.requireNonNull(
            requestJSON.getString("agpFilename", requestJSON.getString("assemblyFilename")),
            "AGP or assembly filename must be provided to load it."
          );
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
          final var sourceLayoutFile = resolveAssemblyLayoutFile(dataDirectory, agpFilename);
          final var agpFile = materializeAgpLayoutFile(sourceLayoutFile);
          try (final var reader = Files.newBufferedReader(agpFile, StandardCharsets.UTF_8)) {
            targetChunkedFile.importAGP(reader);
          } catch (IOException | NoSuchFieldException e) {
            throw new RuntimeException(e);
          }
          final var activeAssemblySource = String.valueOf(map.getOrDefault(ASSEMBLY_SOURCE_KEY, ASSEMBLY_SOURCE_PRIMARY));
          if (requestedSource.equalsIgnoreCase(activeAssemblySource)) {
            synchronizeOverlayAssembly(map, requestedSource);
            refreshSecondaryCompatibility(map);
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

    router.post("/apply_juicebox_assembly").handler(ctx -> {
      final var scheduler = getScheduler(ctx);
      if (scheduler == null) {
        return;
      }
      scheduler.submit(
        ctx,
        RequestTaskScheduler.RequestPriority.ASSEMBLY,
        null,
        () -> applyAssemblyLayoutFromRequest(ctx),
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

  public static @NotNull ChunkedFile resolveActiveAssemblyChunkedFile(final @NotNull LocalMap<String, Object> map) {
    final var activeAssemblySource = String.valueOf(map.getOrDefault(ASSEMBLY_SOURCE_KEY, ASSEMBLY_SOURCE_PRIMARY));
    return resolveChunkedFileWrapperBySource(map, activeAssemblySource).getChunkedFile();
  }

  public static void synchronizeOverlayAssemblyForSharedState(final @NotNull LocalMap<String, Object> map) {
    final var activeAssemblySource = String.valueOf(map.getOrDefault(ASSEMBLY_SOURCE_KEY, ASSEMBLY_SOURCE_PRIMARY));
    synchronizeOverlayAssembly(map, activeAssemblySource);
    refreshSecondaryCompatibility(map);
  }

  private @NotNull AssemblyInfoDTO applyAssemblyLayoutFromRequest(final @NotNull io.vertx.ext.web.RoutingContext ctx) {
    final @NotNull @NonNull LocalMap<String, Object> map = vertx.sharedData().getLocalMap("hict_server");
    final var chunkedFileWrapper = ((ShareableWrappers.ChunkedFileWrapper) (map.get("chunkedFile")));
    if (chunkedFileWrapper == null) {
      throw new RuntimeException("Chunked file is not present in the local map, maybe the file is not yet opened?");
    }
    final var chunkedFile = chunkedFileWrapper.getChunkedFile();

    final var requestJSON = ctx.body().asJsonObject();
    final var assemblyFilename = Objects.requireNonNull(
      requestJSON.getString("assemblyFilename", requestJSON.getString("agpFilename")),
      "Assembly filename must be provided"
    );
    final var requestedSource = String.valueOf(requestJSON.getString("source", ASSEMBLY_SOURCE_PRIMARY))
      .trim()
      .toUpperCase();
    final var fastaFilename = requestJSON.getString("fastaFilename");
    final var dataDirectoryWrapper = (ShareableWrappers.PathWrapper) map.get("dataDirectory");
    if (dataDirectoryWrapper == null) {
      throw new RuntimeException("Data directory is not present in local map");
    }
    final var dataDirectory = dataDirectoryWrapper.getPath();

    final var layoutFile = resolveAssemblyLayoutFile(dataDirectory, assemblyFilename);
    final var agpFile = materializeAgpLayoutFile(layoutFile);
    final var targetWrapper = resolveChunkedFileWrapperBySource(map, requestedSource);
    final var targetChunkedFile = targetWrapper.getChunkedFile();
    if (fastaFilename == null || fastaFilename.isBlank()) {
      log.warn("Applying Juicebox assembly {} without original FASTA; contig matching will be best-effort", assemblyFilename);
    } else {
      final Path fastaPath = dataDirectory.resolve(fastaFilename).normalize().toAbsolutePath();
      if (!fastaPath.startsWith(dataDirectory)) {
        throw new IllegalArgumentException("FASTA path " + fastaFilename + " is outside DATA_DIR");
      }
      if (!Files.isRegularFile(fastaPath)) {
        throw new IllegalArgumentException("FASTA file " + fastaFilename + " does not exist");
      }

      log.info("Linking FASTA {} before applying Juicebox assembly {}", fastaPath, assemblyFilename);
      final var report = targetChunkedFile.getFastaProcessor().analyzeLinkCandidate(fastaPath);
      final var normalizedSource = requestedSource.equalsIgnoreCase(ASSEMBLY_SOURCE_SECONDARY)
        ? ASSEMBLY_SOURCE_SECONDARY
        : ASSEMBLY_SOURCE_PRIMARY;
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
            targetChunkedFile.getFastaProcessor().buildSourceNameAliases(fastaPath)
          );
        }
      }
      log.info("Linked FASTA {} for Juicebox assembly apply warnings={} mismatches={}", fastaPath, report.warnings().size(), report.mismatches().size());
    }
    try (final var reader = Files.newBufferedReader(agpFile, StandardCharsets.UTF_8)) {
      targetChunkedFile.importAGP(reader);
    } catch (IOException | NoSuchFieldException e) {
      throw new RuntimeException(e);
    }
    if (requestedSource.equalsIgnoreCase(String.valueOf(map.getOrDefault(ASSEMBLY_SOURCE_KEY, ASSEMBLY_SOURCE_PRIMARY)))) {
      synchronizeOverlayAssembly(map, requestedSource);
      refreshSecondaryCompatibility(map);
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
  }

  private static @NotNull Path resolveAssemblyLayoutFile(final @NotNull Path dataDirectory,
                                                         final @NotNull String filename) {
    final var lowerFilename = filename.toLowerCase(java.util.Locale.ROOT);
    if (!lowerFilename.endsWith(".assembly") && !lowerFilename.endsWith(".agp")) {
      throw new IllegalArgumentException("Assembly layout file must be .assembly or .agp: " + filename);
    }
    final var assemblyPath = dataDirectory.resolve(filename).normalize();
    if (!assemblyPath.startsWith(dataDirectory)) {
      throw new IllegalArgumentException("Invalid assembly layout filename");
    }
    if (!Files.isRegularFile(assemblyPath)) {
      throw new IllegalArgumentException("Assembly layout file not found: " + filename);
    }
    return assemblyPath;
  }

  private static @NotNull Path materializeAgpLayoutFile(final @NotNull Path sourceLayoutFile) {
    final var lowerFilename = sourceLayoutFile.getFileName().toString().toLowerCase(java.util.Locale.ROOT);
    if (!lowerFilename.endsWith(".assembly")) {
      return sourceLayoutFile;
    }
    final var outputPath = deriveAgpOutputPath(sourceLayoutFile);
    try {
      AssemblyLayoutConverter.convertToAgp(sourceLayoutFile, outputPath);
    } catch (IOException | NoSuchFieldException e) {
      throw new RuntimeException("Failed to convert Juicebox assembly to AGP: " + sourceLayoutFile.getFileName(), e);
    }
    return outputPath;
  }

  private static @NotNull Path deriveAgpOutputPath(final @NotNull Path sourcePath) {
    final var filename = sourcePath.getFileName().toString();
    final var lowerFilename = filename.toLowerCase(java.util.Locale.ROOT);
    if (lowerFilename.endsWith(".assembly")) {
      return sourcePath.resolveSibling(filename.substring(0, filename.length() - ".assembly".length()) + ".agp");
    }
    if (lowerFilename.endsWith(".agp")) {
      return sourcePath;
    }
    return sourcePath.resolveSibling(filename + ".agp");
  }

  private static void synchronizeOverlayAssembly(final @NotNull LocalMap<String, Object> map,
                                                 final @NotNull String assemblySource) {
    final var primaryWrapper = (ShareableWrappers.ChunkedFileWrapper) map.get(PRIMARY_CHUNKED_FILE_KEY);
    final var secondaryWrapper = (ShareableWrappers.ChunkedFileWrapper) map.get(SECONDARY_CHUNKED_FILE_KEY);
    if (primaryWrapper == null || secondaryWrapper == null) {
      return;
    }

    final var primary = primaryWrapper.getChunkedFile();
    final var secondary = secondaryWrapper.getChunkedFile();
    final var source = ASSEMBLY_SOURCE_SECONDARY.equalsIgnoreCase(assemblySource) ? secondary : primary;
    final var target = ASSEMBLY_SOURCE_SECONDARY.equalsIgnoreCase(assemblySource) ? primary : secondary;

    try {
      final var targetAgp = buildTargetAgpForOverlay(source, target);
      if (targetAgp.isBlank()) {
        log.warn("Skipping overlay assembly synchronization because no visible active-source contigs can be resolved in the other source");
        return;
      }
      try (final var reader = new StringReader(targetAgp)) {
        target.importAGP(reader);
      }
      synchronizeTargetVisibilityFromSource(source, target);
      log.info("Synchronized {} assembly layout into {} source for overlay rendering",
        ASSEMBLY_SOURCE_SECONDARY.equalsIgnoreCase(assemblySource) ? ASSEMBLY_SOURCE_SECONDARY : ASSEMBLY_SOURCE_PRIMARY,
        ASSEMBLY_SOURCE_SECONDARY.equalsIgnoreCase(assemblySource) ? ASSEMBLY_SOURCE_PRIMARY : ASSEMBLY_SOURCE_SECONDARY
      );
    } catch (final Exception ex) {
      log.warn("Failed to synchronize {} assembly layout for overlay rendering; sources will keep their own layout",
        assemblySource,
        ex
      );
    }
  }

  private static @NotNull String buildTargetAgpForOverlay(final @NotNull ChunkedFile source,
                                                          final @NotNull ChunkedFile target) {
    final var recordsByScaffold = new LinkedHashMap<String, List<AGPProcessor.ContigAGPRecord>>();
    int skippedVisibleRecords = 0;

    for (final var record : source.getAgpProcessor().getAGPRecords(1000L)) {
      if (!(record instanceof AGPProcessor.ContigAGPRecord sourceRecord)) {
        continue;
      }
      final var sourceDescriptor = resolveContigDescriptor(source, sourceRecord.getContigName());
      if (sourceDescriptor == null) {
        ++skippedVisibleRecords;
        log.warn("Skipping overlay AGP record for unresolved active-source contig {}", sourceRecord.getContigName());
        continue;
      }
      final var targetDescriptor = resolveEquivalentContigDescriptor(source, target, sourceDescriptor);
      if (targetDescriptor == null) {
        if (isContigShownAtAnyMatrixResolution(sourceDescriptor)) {
          ++skippedVisibleRecords;
          log.warn("Skipping visible active-source contig {} while synchronizing overlay assembly; it is absent in the other source",
            source.getContigDisplayName(sourceDescriptor.getContigId()));
        }
        continue;
      }

      final var componentLength = sourceRecord.getIntraContigEndBpIncl() - sourceRecord.getIntraContigStartBpIncl() + 1L;
      if (componentLength != targetDescriptor.getLengthBp()) {
        ++skippedVisibleRecords;
        log.warn(
          "Skipping active-source contig {} while synchronizing overlay assembly; source component length {} bp differs from target contig length {} bp",
          source.getContigDisplayName(sourceDescriptor.getContigId()),
          componentLength,
          targetDescriptor.getLengthBp()
        );
        continue;
      }

      recordsByScaffold.computeIfAbsent(sourceRecord.getScaffoldName(), ignored -> new ArrayList<>()).add(
        new AGPProcessor.ContigAGPRecord(
          sourceRecord.getScaffoldName(),
          0L,
          0L,
          0,
          target.getContigDisplayName(targetDescriptor.getContigId()),
          1L,
          targetDescriptor.getLengthBp(),
          sourceRecord.getContigOrientation()
        )
      );
    }

    final var synchronizedAgp = new StringBuilder();
    recordsByScaffold.forEach((scaffoldName, contigRecords) -> {
      long positionBp = 1L;
      int partNumber = 1;
      for (final var record : contigRecords) {
        final var componentLength = record.getIntraContigEndBpIncl() - record.getIntraContigStartBpIncl() + 1L;
        synchronizedAgp.append(new AGPProcessor.ContigAGPRecord(
          scaffoldName,
          positionBp,
          positionBp + componentLength - 1L,
          partNumber,
          record.getContigName(),
          record.getIntraContigStartBpIncl(),
          record.getIntraContigEndBpIncl(),
          record.getContigOrientation()
        )).append(System.lineSeparator());
        positionBp += componentLength;
        ++partNumber;
      }
    });

    if (skippedVisibleRecords > 0) {
      log.warn("Overlay assembly synchronization skipped {} visible active-source contig record(s); common contigs remain synchronized", skippedVisibleRecords);
    }
    return synchronizedAgp.toString();
  }

  private static @Nullable ContigDescriptor resolveContigDescriptor(final @NotNull ChunkedFile source,
                                                                    final @NotNull String contigName) {
    try {
      return source.resolveContigDescriptorByName(contigName);
    } catch (final IllegalArgumentException ignored) {
      return null;
    }
  }

  private static @Nullable ContigDescriptor resolveEquivalentContigDescriptor(final @NotNull ChunkedFile source,
                                                                             final @NotNull ChunkedFile target,
                                                                             final @NotNull ContigDescriptor sourceDescriptor) {
    final var candidateNames = List.of(
      source.getContigDisplayName(sourceDescriptor.getContigId()),
      source.getContigOriginalName(sourceDescriptor.getContigId()),
      sourceDescriptor.getContigName(),
      sourceDescriptor.getContigNameInSourceFASTA()
    );
    for (final var candidateName : candidateNames) {
      try {
        return target.resolveContigDescriptorByName(candidateName);
      } catch (final IllegalArgumentException ignored) {
        // Try the next stable name alias.
      }
    }
    return null;
  }

  private static boolean isContigShownAtAnyMatrixResolution(final @NotNull ContigDescriptor descriptor) {
    final var presenceAtResolution = descriptor.getPresenceAtResolution();
    for (int order = 1; order < presenceAtResolution.size(); order++) {
      if (presenceAtResolution.get(order) == ContigHideType.SHOWN) {
        return true;
      }
    }
    return false;
  }

  private static void synchronizeTargetVisibilityFromSource(final @NotNull ChunkedFile source,
                                                            final @NotNull ChunkedFile target) {
    final var sourceResolutions = source.getResolutions();
    final var targetResolutions = target.getResolutions();
    if (sourceResolutions.length <= 1 || targetResolutions.length <= 1) {
      return;
    }

    final var sourceDescriptorsByName = new HashMap<String, ContigDescriptor>();
    source.getContigTree().getContigDescriptors().forEach((contigId, descriptor) -> {
      sourceDescriptorsByName.put(source.getContigDisplayName(contigId), descriptor);
      sourceDescriptorsByName.put(source.getContigOriginalName(contigId), descriptor);
      sourceDescriptorsByName.put(descriptor.getContigName(), descriptor);
      sourceDescriptorsByName.put(descriptor.getContigNameInSourceFASTA(), descriptor);
    });

    target.getContigTree().getContigDescriptors().forEach((contigId, targetDescriptor) -> {
      var sourceDescriptor = sourceDescriptorsByName.get(target.getContigDisplayName(contigId));
      if (sourceDescriptor == null) {
        sourceDescriptor = sourceDescriptorsByName.get(target.getContigOriginalName(contigId));
      }
      if (sourceDescriptor == null) {
        sourceDescriptor = sourceDescriptorsByName.get(targetDescriptor.getContigName());
      }
      if (sourceDescriptor == null) {
        sourceDescriptor = sourceDescriptorsByName.get(targetDescriptor.getContigNameInSourceFASTA());
      }
      if (sourceDescriptor == null) {
        for (int targetOrder = 1; targetOrder < targetResolutions.length && targetOrder < targetDescriptor.getPresenceAtResolution().size(); targetOrder++) {
          targetDescriptor.getPresenceAtResolution().set(targetOrder, ContigHideType.HIDDEN);
        }
        return;
      }

      for (int targetOrder = 1; targetOrder < targetResolutions.length; targetOrder++) {
        if (targetOrder >= targetDescriptor.getPresenceAtResolution().size()) {
          continue;
        }
        final var sourceOrder = exactResolutionOrder(sourceResolutions, targetResolutions[targetOrder]);
        final var presence = sourceOrder > 0 && sourceOrder < sourceDescriptor.getPresenceAtResolution().size()
          ? sourceDescriptor.getPresenceAtResolution().get(sourceOrder)
          : inferVisibilityForResolution(targetDescriptor, targetResolutions[targetOrder], targetOrder);
        targetDescriptor.getPresenceAtResolution().set(targetOrder, presence);
      }
    });
  }

  private static int exactResolutionOrder(final long @NotNull [] resolutions, final long targetResolution) {
    for (int order = 1; order < resolutions.length; order++) {
      if (resolutions[order] == targetResolution) {
        return order;
      }
    }
    return -1;
  }

  private static @NotNull ContigHideType inferVisibilityForResolution(final @NotNull ContigDescriptor descriptor,
                                                                      final long resolution,
                                                                      final int resolutionOrder) {
    final long[] lengthBinsAtResolution = descriptor.getLengthBinsAtResolution();
    final long lengthBins;
    if (resolutionOrder >= 0 && resolutionOrder < lengthBinsAtResolution.length) {
      lengthBins = lengthBinsAtResolution[resolutionOrder];
    } else if (descriptor.getLengthBp() <= 0L || resolution <= 0L) {
      lengthBins = 0L;
    } else {
      lengthBins = Math.max(1L, (descriptor.getLengthBp() + resolution - 1L) / resolution);
    }
    return lengthBins > 0L && descriptor.getLengthBp() >= resolution
      ? ContigHideType.SHOWN
      : ContigHideType.HIDDEN;
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
    final var primaryResolutions = responseResolutions(primary);
    final var secondaryResolutions = responseResolutions(secondary);
    final var primaryMatrixSizeBins = responseMatrixSizeBins(primary);
    final var secondaryMatrixSizeBins = responseMatrixSizeBins(secondary);
    return new SecondaryCompatibility(
      Arrays.equals(primaryResolutions, secondaryResolutions),
      Arrays.equals(primaryMatrixSizeBins, secondaryMatrixSizeBins),
      primaryResolutions,
      secondaryResolutions,
      primaryMatrixSizeBins,
      secondaryMatrixSizeBins
    );
  }

  private static @Nullable SecondaryCompatibility refreshSecondaryCompatibility(final @NotNull LocalMap<String, Object> map) {
    final var primaryWrapper = (ShareableWrappers.ChunkedFileWrapper) map.get(PRIMARY_CHUNKED_FILE_KEY);
    final var secondaryWrapper = (ShareableWrappers.ChunkedFileWrapper) map.get(SECONDARY_CHUNKED_FILE_KEY);
    if (primaryWrapper == null || secondaryWrapper == null) {
      map.remove(SECONDARY_COMPATIBILITY_KEY);
      return null;
    }
    final var compatibility = analyzeSecondaryCompatibility(primaryWrapper.getChunkedFile(), secondaryWrapper.getChunkedFile());
    map.put(SECONDARY_COMPATIBILITY_KEY, compatibility.toJson());
    return compatibility;
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
                                        long[] primaryResolutions,
                                        long[] secondaryResolutions,
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
        .put("primaryResolutions", Arrays.stream(primaryResolutions).boxed().toList())
        .put("secondaryResolutions", Arrays.stream(secondaryResolutions).boxed().toList())
        .put("primaryPixelResolutions", Arrays.stream(primaryResolutions).mapToDouble(value -> (double) value).boxed().toList())
        .put("secondaryPixelResolutions", Arrays.stream(secondaryResolutions).mapToDouble(value -> (double) value).boxed().toList())
        .put("primaryBinsByResolution", Arrays.stream(primaryMatrixSizeBins).boxed().toList())
        .put("secondaryBinsByResolution", Arrays.stream(secondaryMatrixSizeBins).boxed().toList())
        .put("mismatchedResolutionOrders", mismatchedOrders);
    }
  }

  private static long @NotNull [] responseResolutions(final @NotNull ChunkedFile chunkedFile) {
    final var resolutionsWithoutZero = Arrays.stream(chunkedFile.getResolutions()).skip(1L).toArray();
    ArrayUtils.reverse(resolutionsWithoutZero);
    return resolutionsWithoutZero;
  }

  private static long @NotNull [] responseMatrixSizeBins(final @NotNull ChunkedFile chunkedFile) {
    final var resolutions = chunkedFile.getResolutions();
    final var visibleMatrixSizeBins = new long[Math.max(0, resolutions.length - 1)];
    for (int order = 1; order < resolutions.length; order++) {
      visibleMatrixSizeBins[order - 1] = chunkedFile.getContigTree().getLengthInUnits(
        QueryLengthUnit.PIXELS,
        ResolutionDescriptor.fromResolutionOrder(order)
      );
    }
    ArrayUtils.reverse(visibleMatrixSizeBins);
    return visibleMatrixSizeBins;
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
    final var resolutionsWithoutZero = responseResolutions(chunkedFile);
    final var matrixSizeBins = responseMatrixSizeBins(chunkedFile);
    final long minResolution = Arrays.stream(resolutionsWithoutZero).min().orElse(1L);
//    Arrays.stream(chunkedFile.getMatrixSizeBins()).forEachOrdered(i -> log.debug("New resolutrion matrix size bins: " + i));
    return new OpenFileResponseDTO(
      "Opened",
      (String) vertx.sharedData().getLocalMap("hict_server").getOrDefault("transport_dtype", "uint8"),
      Arrays.stream(resolutionsWithoutZero).boxed().toList(),
      Arrays.stream(resolutionsWithoutZero).mapToDouble(r -> (double) r / minResolution).boxed().toList(),
      chunkedFile.getDenseBlockSize(),
      AssemblyInfoDTO.generateFromChunkedFile(chunkedFile),
      Arrays.stream(matrixSizeBins).mapToInt(l -> (int) l).boxed().toList()
    );
  }
}
