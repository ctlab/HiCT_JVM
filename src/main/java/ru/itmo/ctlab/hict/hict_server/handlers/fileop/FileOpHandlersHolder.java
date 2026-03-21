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
          final var oldTrackManagerWrapper = (ShareableWrappers.Track1DManagerWrapper) map.get("Track1DManager");
          if (oldTrackManagerWrapper != null) {
            oldTrackManagerWrapper.getTrack1DManager().setLinkedFastaAliasesBySource(java.util.Map.of());
            oldTrackManagerWrapper.getTrack1DManager().close();
          }
          map.remove("linkedFastaPath");
          map.remove("linkedFastaFilename");

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
          map.put("chunkedFile", chunkedFileWrapper);
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
          final var chunkedFileWrapper = ((ShareableWrappers.ChunkedFileWrapper) (map.get("chunkedFile")));
          if (chunkedFileWrapper == null) {
            return new JsonRouteResult(
              404,
              new io.vertx.core.json.JsonObject().put("error", "No session to attach")
            );
          }
          final var chunkedFile = chunkedFileWrapper.getChunkedFile();
          final var filename = (String) map.getOrDefault("openedFilename", "");
          return new JsonRouteResult(
            200,
            new io.vertx.core.json.JsonObject()
              .put("filename", filename)
              .put("fastaFilename", map.getOrDefault("linkedFastaFilename", ""))
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
          final var chunkedFileWrapper = ((ShareableWrappers.ChunkedFileWrapper) (map.get("chunkedFile")));
          if (chunkedFileWrapper != null) {
            try {
              chunkedFileWrapper.getChunkedFile().close();
            } catch (Exception e) {
              log.warn("Failed to close chunked file", e);
            }
          }
          map.remove("chunkedFile");
          map.remove("TileStatisticHolder");
          map.remove("openedFilename");
          map.remove("linkedFastaPath");
          map.remove("linkedFastaFilename");
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
          if (fastaFilename == null || fastaFilename.isBlank()) {
            throw new IllegalArgumentException("FASTA filename is required");
          }

          final Path fastaPath = dataDirectoryWrapper.getPath().resolve(fastaFilename).normalize().toAbsolutePath();
          if (!fastaPath.startsWith(dataDirectoryWrapper.getPath())) {
            throw new IllegalArgumentException("FASTA path " + fastaFilename + " is outside DATA_DIR");
          }
          if (!Files.exists(fastaPath) || !Files.isRegularFile(fastaPath)) {
            throw new IllegalArgumentException("FASTA file " + fastaFilename + " does not exist");
          }

          final var report = chunkedFileWrapper.getChunkedFile().getFastaProcessor().analyzeLinkCandidate(fastaPath);
          final boolean requiresConfirmation = report.hasWarnings() && !allowMismatch;
          if (!requiresConfirmation) {
            map.put("linkedFastaPath", new ShareableWrappers.PathWrapper(fastaPath));
            map.put("linkedFastaFilename", fastaFilename);
            final var trackManagerWrapper = (ShareableWrappers.Track1DManagerWrapper) map.get("Track1DManager");
            if (trackManagerWrapper != null) {
              trackManagerWrapper.getTrack1DManager().setLinkedFastaAliasesBySource(
                chunkedFileWrapper.getChunkedFile().getFastaProcessor().buildSourceNameAliases(fastaPath)
              );
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
          final var chunkedFileWrapper = ((ShareableWrappers.ChunkedFileWrapper) (map.get("chunkedFile")));
          if (chunkedFileWrapper == null) {
            throw new IllegalStateException("Open a Hi-C file before exporting FASTA");
          }
          final var fastaPathWrapper = (ShareableWrappers.PathWrapper) map.get("linkedFastaPath");
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
          final var chunkedFileWrapper = ((ShareableWrappers.ChunkedFileWrapper) (map.get("chunkedFile")));
          if (chunkedFileWrapper == null) {
            throw new IllegalStateException("Open a Hi-C file before exporting FASTA");
          }
          final var fastaPathWrapper = (ShareableWrappers.PathWrapper) map.get("linkedFastaPath");
          if (fastaPathWrapper == null) {
            throw new IllegalStateException("Link a FASTA file before exporting FASTA");
          }
          return chunkedFileWrapper.getChunkedFile().getFastaProcessor().exportSelection(
            fastaPathWrapper.getPath(),
            fromBpX,
            fromBpY,
            toBpX,
            toBpY
          );
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

          final var dataDirectoryWrapper = (ShareableWrappers.PathWrapper) vertx.sharedData().getLocalMap("hict_server").get("dataDirectory");
          if (dataDirectoryWrapper == null) {
            throw new RuntimeException("Data directory is not present in local map");
          }
          final var dataDirectory = dataDirectoryWrapper.getPath();
          final var agpFile = Path.of(dataDirectory.toString(), agpFilename);
          try (final var reader = Files.newBufferedReader(agpFile, StandardCharsets.UTF_8)) {
            chunkedFile.importAGP(reader);
          } catch (IOException | NoSuchFieldException e) {
            throw new RuntimeException(e);
          }
          final var schedulerWrapper = (ShareableWrappers.RequestTaskSchedulerWrapper) map.get(RequestTaskScheduler.LOCAL_MAP_KEY);
          if (schedulerWrapper != null) {
            schedulerWrapper.getRequestTaskScheduler().bumpAssemblyGeneration();
          }
          final var trackManagerWrapper = (ShareableWrappers.Track1DManagerWrapper) map.get("Track1DManager");
          if (trackManagerWrapper != null) {
            trackManagerWrapper.getTrack1DManager().invalidateInMemoryCache();
          }
          return AssemblyInfoDTO.generateFromChunkedFile(chunkedFile);
        },
        response -> ctx.response().end(Json.encode(response))
      );
    });
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
