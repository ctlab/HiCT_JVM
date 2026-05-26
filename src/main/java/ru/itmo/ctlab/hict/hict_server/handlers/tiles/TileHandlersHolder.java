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

package ru.itmo.ctlab.hict.hict_server.handlers.tiles;

import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.LocalMap;
import io.vertx.ext.web.Router;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.domain.QueryLengthUnit;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.resolution.ResolutionDescriptor;
import ru.itmo.ctlab.hict.hict_library.visualization.DistanceExpectedNormalizer;
import ru.itmo.ctlab.hict.hict_library.visualization.SignalDisplayMode;
import ru.itmo.ctlab.hict.hict_server.HandlersHolder;
import ru.itmo.ctlab.hict.hict_server.concurrent.RequestTaskScheduler;
import ru.itmo.ctlab.hict.hict_server.dto.symmetric.visualization.VisualizationOptionsDTO;
import ru.itmo.ctlab.hict.hict_server.handlers.util.TileStatisticHolder;
import ru.itmo.ctlab.hict.hict_server.tracks.Track1DManager;
import ru.itmo.ctlab.hict.hict_server.util.shareable.ShareableWrappers;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@RequiredArgsConstructor
@Slf4j
public class TileHandlersHolder extends HandlersHolder {
  private final Vertx vertx;
  public static final String EXPECTED_PROFILE_LOCAL_MAP_KEY = "ViewportExpectedProfile";
  private static final String TRANSPARENT_PNG_BASE64 =
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO7Z3ioAAAAASUVORK5CYII=";
  private static final byte[] TRANSPARENT_PNG_BYTES = Base64.getDecoder().decode(TRANSPARENT_PNG_BASE64);
  private static final int MAX_MATRIX_QUERY_ELEMENTS = Integer.getInteger("HICT_MATRIX_QUERY_MAX_ELEMENTS", 16_777_216);

  public static void clearExpectedProfileCache(final @NotNull LocalMap<String, Object> map) {
    map.remove(EXPECTED_PROFILE_LOCAL_MAP_KEY);
  }

  @Override
  public void addHandlersToRouter(final @NotNull Router router) {
    router.post("/set_visualization_options").handler(ctx -> {
      final var scheduler = getScheduler(ctx);
      if (scheduler == null) {
        return;
      }
      final @NotNull var requestBody = ctx.body();
      final @NotNull var requestJSON = requestBody.asJsonObject();

      final @NotNull @NonNull var request = VisualizationOptionsDTO.fromJSONObject(requestJSON);
      final var preserveRenderPipeline = requestJSON.getBoolean("preserveRenderPipeline", false);
      scheduler.submit(
        ctx,
        RequestTaskScheduler.RequestPriority.ASSEMBLY,
        null,
        () -> {
          final @NotNull @NonNull LocalMap<String, Object> map = vertx.sharedData().getLocalMap("hict_server");
          log.debug("Got map");
          final var optionsEntity = request.toEntity();
          map.put("visualizationOptions", new ShareableWrappers.SimpleVisualizationOptionsWrapper(optionsEntity));
          if (optionsEntity.getSignalDisplayMode() == SignalDisplayMode.OBSERVED) {
            clearExpectedProfileCache(map);
          }
          if (!preserveRenderPipeline) {
            final var previousPipelineWrapper =
              (ShareableWrappers.RenderPipelineConfigWrapper) map.get(RenderPipelineConfig.LOCAL_MAP_KEY);
            final var previousPipeline =
              previousPipelineWrapper != null ? previousPipelineWrapper.getRenderPipelineConfig() : RenderPipelineConfig.disabled();
            final var syncedPipeline = RenderPipelineConfig.fromVisualizationOptions(
              optionsEntity,
              previousPipeline.enabled(),
              previousPipeline.swapUpperLower()
            );
            map.put(
              RenderPipelineConfig.LOCAL_MAP_KEY,
              new ShareableWrappers.RenderPipelineConfigWrapper(syncedPipeline)
            );
          }
          final var chunkedFileWrapper = ((ShareableWrappers.ChunkedFileWrapper) (map.get("chunkedFile")));
          if (chunkedFileWrapper == null) {
            throw new RuntimeException("Chunked file is not present in the local map, maybe the file is not yet opened?");
          }
          final var chunkedFile = chunkedFileWrapper.getChunkedFile();
          log.debug("Got ChunkedFile from map");

          final var stats = (TileStatisticHolder) map.get("TileStatisticHolder");
          if (stats == null) {
            throw new RuntimeException("Tile statistics is not present in the local map, maybe the file is not yet opened?");
          }
          map.put("TileStatisticHolder", TileStatisticHolder.resetRangesKeepingVersion(stats, chunkedFile.getResolutions().length));
          scheduler.bumpGeneration(RequestTaskScheduler.CancellationDomain.TILE);
          final var visualizationOptionsWrapper = ((ShareableWrappers.SimpleVisualizationOptionsWrapper) (map.get("visualizationOptions")));
          if (visualizationOptionsWrapper == null) {
            throw new RuntimeException("Visualization options are not present in the local map, maybe the file is not yet opened?");
          }
          final var options = visualizationOptionsWrapper.getSimpleVisualizationOptions();
          return VisualizationOptionsDTO.fromEntity(options, chunkedFile);
        },
        responseDto -> ctx.response()
          .putHeader("content-type", "application/json")
          .setStatusCode(200)
          .end(Json.encode(responseDto))
      );
    });

    router.post("/get_visualization_options").handler(ctx -> {
      final var scheduler = getScheduler(ctx);
      if (scheduler == null) {
        return;
      }
      scheduler.submit(
        ctx,
        RequestTaskScheduler.RequestPriority.UI_UX,
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
          final var visualizationOptionsWrapper = ((ShareableWrappers.SimpleVisualizationOptionsWrapper) (map.get("visualizationOptions")));
          if (visualizationOptionsWrapper == null) {
            throw new RuntimeException("Visualization options are not present in the local map, maybe the file is not yet opened?");
          }
          final var options = visualizationOptionsWrapper.getSimpleVisualizationOptions();
          return VisualizationOptionsDTO.fromEntity(options, chunkedFile);
        },
        responseDto -> ctx.response()
          .putHeader("content-type", "application/json")
          .setStatusCode(200)
          .end(Json.encode(responseDto))
      );
    });

    router.post("/visualization/expected_profile").handler(ctx -> {
      final var scheduler = getScheduler(ctx);
      if (scheduler == null) {
        return;
      }
      final @NotNull var requestJSON = ctx.body().asJsonObject();
      scheduler.submit(
        ctx,
        RequestTaskScheduler.RequestPriority.UI_UX,
        null,
        () -> {
          final @NotNull @NonNull LocalMap<String, Object> map = vertx.sharedData().getLocalMap("hict_server");
          final var chunkedFileWrapper = (ShareableWrappers.ChunkedFileWrapper) map.get("chunkedFile");
          if (chunkedFileWrapper == null) {
            throw new RuntimeException("Chunked file is not present in the local map, maybe the file is not yet opened?");
          }
          final var visualizationOptionsWrapper =
            (ShareableWrappers.SimpleVisualizationOptionsWrapper) map.get("visualizationOptions");
          if (visualizationOptionsWrapper == null) {
            throw new RuntimeException("Visualization options are not present in the local map, maybe the file is not yet opened?");
          }
          final var options = visualizationOptionsWrapper.getSimpleVisualizationOptions();
          if (options.getSignalDisplayMode() == SignalDisplayMode.OBSERVED) {
            clearExpectedProfileCache(map);
            return new JsonObject().put("status", "cleared");
          }

          final var bpResolution = requestJSON.getLong("bpResolution");
          if (bpResolution == null || bpResolution <= 0L) {
            throw new IllegalArgumentException("Field 'bpResolution' must be a positive integer");
          }
          final var startRowPx = requestJSON.getLong("startRowPx");
          final var endRowPx = requestJSON.getLong("endRowPx");
          final var startColPx = requestJSON.getLong("startColPx");
          final var endColPx = requestJSON.getLong("endColPx");
          if (startRowPx == null || endRowPx == null || startColPx == null || endColPx == null) {
            throw new IllegalArgumentException("Viewport expected profile request must include start/end pixel coordinates");
          }
          if (endRowPx <= startRowPx || endColPx <= startColPx) {
            throw new IllegalArgumentException("Viewport expected profile bounds must define a positive area");
          }

          final var chunkedFile = chunkedFileWrapper.getChunkedFile();
          final var resolutionOrder = chunkedFile.getResolutionToIndex().get(bpResolution);
          if (resolutionOrder == null) {
            throw new IllegalArgumentException("Requested bpResolution is not present in opened file: " + bpResolution);
          }
          final var resolutionDescriptor = ResolutionDescriptor.fromResolutionOrder(resolutionOrder);
          final var totalAssemblyLength = chunkedFile.getContigTree().getLengthInUnits(
            QueryLengthUnit.PIXELS,
            resolutionDescriptor
          );
          final long clampedStartRowPx = Math.max(0L, Math.min(startRowPx, totalAssemblyLength));
          final long clampedEndRowPx = Math.max(
            clampedStartRowPx,
            Math.min(endRowPx, totalAssemblyLength)
          );
          final long clampedStartColPx = Math.max(0L, Math.min(startColPx, totalAssemblyLength));
          final long clampedEndColPx = Math.max(
            clampedStartColPx,
            Math.min(endColPx, totalAssemblyLength)
          );
          if (clampedEndRowPx <= clampedStartRowPx || clampedEndColPx <= clampedStartColPx) {
            clearExpectedProfileCache(map);
            return new JsonObject()
              .put("status", "empty")
              .put("resolutionOrder", resolutionOrder)
              .put("startRowPx", clampedStartRowPx)
              .put("endRowPx", clampedEndRowPx)
              .put("startColPx", clampedStartColPx)
              .put("endColPx", clampedEndColPx);
          }

          final var expectedDomains = buildExpectedDomains(
            chunkedFile,
            resolutionDescriptor,
            clampedStartRowPx,
            clampedEndRowPx,
            clampedStartColPx,
            clampedEndColPx
          );
          final var chunkSize = Math.max(
            32,
            ((Number) map.getOrDefault("tileSize", 256)).intValue()
          );
          final var accumulator = DistanceExpectedNormalizer.newSegmentedAccumulator(
            resolutionOrder,
            clampedStartRowPx,
            clampedEndRowPx,
            clampedStartColPx,
            clampedEndColPx,
            expectedDomains
          );
          for (long chunkStartRowPx = clampedStartRowPx; chunkStartRowPx < clampedEndRowPx; chunkStartRowPx += chunkSize) {
            final var chunkEndRowPx = Math.min(clampedEndRowPx, chunkStartRowPx + chunkSize);
            for (long chunkStartColPx = clampedStartColPx; chunkStartColPx < clampedEndColPx; chunkStartColPx += chunkSize) {
              final var chunkEndColPx = Math.min(clampedEndColPx, chunkStartColPx + chunkSize);
              final var matrixWithWeights = chunkedFile.matrixQueries().getSubmatrix(
                resolutionDescriptor,
                chunkStartRowPx,
                chunkStartColPx,
                chunkEndRowPx,
                chunkEndColPx,
                true
              );
              final var baseSignal = chunkedFile.tileVisualizationProcessor().prepareSignalMatrix(
                matrixWithWeights,
                options
              );
              accumulator.addSignal(
                baseSignal,
                matrixWithWeights.startRowIncl(),
                matrixWithWeights.startColIncl()
              );
            }
          }
          final var profile = accumulator.toProfile();
          map.put(
            EXPECTED_PROFILE_LOCAL_MAP_KEY,
            new ShareableWrappers.DiagonalExpectedProfileWrapper(profile)
          );
          return new JsonObject()
            .put("status", "ok")
            .put("resolutionOrder", resolutionOrder)
            .put("startRowPx", clampedStartRowPx)
            .put("endRowPx", clampedEndRowPx)
            .put("startColPx", clampedStartColPx)
            .put("endColPx", clampedEndColPx)
            .put("domainCount", expectedDomains.length)
            .put("minDiagonal", profile.minDiagonal())
            .put("diagonalCount", profile.means().length);
        },
        response -> ctx.response()
          .putHeader("content-type", "application/json")
          .setStatusCode(200)
          .end(response.encode())
      );
    });

    router.post("/render_pipeline/get").handler(ctx -> {
      final var scheduler = getScheduler(ctx);
      if (scheduler == null) {
        return;
      }
      scheduler.submit(
        ctx,
        RequestTaskScheduler.RequestPriority.UI_UX,
        null,
        () -> {
          final @NotNull @NonNull LocalMap<String, Object> map = vertx.sharedData().getLocalMap("hict_server");
          final var wrapper = (ShareableWrappers.RenderPipelineConfigWrapper) map.get(RenderPipelineConfig.LOCAL_MAP_KEY);
          final var config = (wrapper != null) ? wrapper.getRenderPipelineConfig() : RenderPipelineConfig.disabled();
          return config.toJson();
        },
        response -> ctx.response()
          .putHeader("content-type", "application/json")
          .setStatusCode(200)
          .end(response.encode())
      );
    });

    router.post("/render_pipeline/set").handler(ctx -> {
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
          final var requestBody = ctx.body().asJsonObject();
          final var config = RenderPipelineConfig.fromJson(requestBody);
          map.put(
            RenderPipelineConfig.LOCAL_MAP_KEY,
            new ShareableWrappers.RenderPipelineConfigWrapper(config)
          );
          final var chunkedFileWrapper = ((ShareableWrappers.ChunkedFileWrapper) (map.get("chunkedFile")));
          final var stats = (TileStatisticHolder) map.get("TileStatisticHolder");
          if (chunkedFileWrapper != null && stats != null) {
            map.put(
              "TileStatisticHolder",
              TileStatisticHolder.resetRangesWithIncrementedVersion(
                stats,
                chunkedFileWrapper.getChunkedFile().getResolutions().length
              )
            );
          }
          scheduler.bumpGeneration(RequestTaskScheduler.CancellationDomain.TILE);
          return config.toJson();
        },
        response -> ctx.response()
          .putHeader("content-type", "application/json")
          .setStatusCode(200)
          .end(response.encode())
      );
    });

    router.post("/render_pipeline/reset").handler(ctx -> {
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
          final var config = RenderPipelineConfig.disabled();
          map.put(
            RenderPipelineConfig.LOCAL_MAP_KEY,
            new ShareableWrappers.RenderPipelineConfigWrapper(config)
          );
          final var chunkedFileWrapper = ((ShareableWrappers.ChunkedFileWrapper) (map.get("chunkedFile")));
          final var stats = (TileStatisticHolder) map.get("TileStatisticHolder");
          if (chunkedFileWrapper != null && stats != null) {
            map.put(
              "TileStatisticHolder",
              TileStatisticHolder.resetRangesWithIncrementedVersion(
                stats,
                chunkedFileWrapper.getChunkedFile().getResolutions().length
              )
            );
          }
          scheduler.bumpGeneration(RequestTaskScheduler.CancellationDomain.TILE);
          return config.toJson();
        },
        response -> ctx.response()
          .putHeader("content-type", "application/json")
          .setStatusCode(200)
          .end(response.encode())
      );
    });

    router.post("/tiles/reload").handler(ctx -> {
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
            throw new RuntimeException("Chunked file is not present in the local map, maybe the file is not yet opened?");
          }
          final var chunkedFile = chunkedFileWrapper.getChunkedFile();
          final var stats = (TileStatisticHolder) map.get("TileStatisticHolder");
          if (stats == null) {
            throw new RuntimeException("Tile statistics is not present in the local map, maybe the file is not yet opened?");
          }
          final var newStats = TileStatisticHolder.resetRangesWithIncrementedVersion(stats, chunkedFile.getResolutions().length);
          map.put("TileStatisticHolder", newStats);
          scheduler.bumpGeneration(RequestTaskScheduler.CancellationDomain.TILE);
          scheduler.bumpGeneration(RequestTaskScheduler.CancellationDomain.TRACK);
          final var trackManagerWrapper = (ShareableWrappers.Track1DManagerWrapper) map.get("Track1DManager");
          if (trackManagerWrapper != null) {
            trackManagerWrapper.getTrack1DManager().invalidateInMemoryCache();
          }
          return Map.of("version", newStats.versionCounter().get());
        },
        result -> ctx.response().setStatusCode(200).end(Json.encode(result))
      );
    });

    router.get("/get_tile").handler(ctx -> {
      final var scheduler = getScheduler(ctx);
      if (scheduler == null) {
        return;
      }
      final var format = TileFormat.valueOf(ctx.request().getParam("format", "JSON_PNG_WITH_RANGES"));
      scheduler.submit(
        ctx,
        RequestTaskScheduler.RequestPriority.TILE,
        RequestTaskScheduler.CancellationDomain.TILE,
        () -> computeTileResponse(ctx),
        response -> {
          ctx.response().putHeader("content-type", response.contentType());
          if (response.jsonBody() != null) {
            ctx.response().end(response.jsonBody());
          } else {
            ctx.response().end(response.binaryBody());
          }
        },
        () -> respondCancelledTile(ctx, format)
      );
    });

    router.post("/matrix/query").handler(ctx -> {
      final var scheduler = getScheduler(ctx);
      if (scheduler == null) {
        return;
      }
      final var requestJson = ctx.body() != null && ctx.body().asJsonObject() != null
        ? ctx.body().asJsonObject()
        : new JsonObject();
      final var format = MatrixResponseFormat.fromRaw(requestJson.getString("format", MatrixResponseFormat.BINARY_FLOAT32.name()));
      scheduler.submit(
        ctx,
        RequestTaskScheduler.RequestPriority.TILE,
        RequestTaskScheduler.CancellationDomain.TILE,
        () -> computeMatrixQueryResponse(requestJson),
        response -> {
          final var httpResponse = ctx.response().putHeader("content-type", response.contentType());
          response.headers().forEach(httpResponse::putHeader);
          if (response.jsonBody() != null) {
            httpResponse.end(response.jsonBody());
          } else {
            httpResponse.end(response.binaryBody());
          }
        },
        () -> respondCancelledMatrixQuery(ctx, format)
      );
    });
  }

  private TileResponsePayload computeTileResponse(final @NotNull io.vertx.ext.web.RoutingContext ctx) {
    final var row = Long.parseLong(ctx.request().getParam("row", "0"));
    final var col = Long.parseLong(ctx.request().getParam("col", "0"));
    final var requestedVersion = Long.parseLong(ctx.request().getParam("version", "0"));
    final int tileHeight;
    final int tileWidth;
    final var format = TileFormat.valueOf(ctx.request().getParam("format", "JSON_PNG_WITH_RANGES"));

    final @NotNull @NonNull LocalMap<String, Object> map = vertx.sharedData().getLocalMap("hict_server");
    final var primaryChunkedFileWrapper = ((ShareableWrappers.ChunkedFileWrapper) (map.get("chunkedFile")));
    if (primaryChunkedFileWrapper == null) {
      throw new RuntimeException("Chunked file is not present in the local map, maybe the file is not yet opened?");
    }
    final var chunkedFile = primaryChunkedFileWrapper.getChunkedFile();
    final var secondaryChunkedFileWrapper = ((ShareableWrappers.ChunkedFileWrapper) (map.get("chunkedFileSecondary")));
    final var secondaryChunkedFile = secondaryChunkedFileWrapper != null ? secondaryChunkedFileWrapper.getChunkedFile() : null;
    final var visualizationOptionsWrapper = ((ShareableWrappers.SimpleVisualizationOptionsWrapper) (map.get("visualizationOptions")));
    if (visualizationOptionsWrapper == null) {
      throw new RuntimeException("Visualization options are not present in the local map, maybe the file is not yet opened?");
    }
    final var options = visualizationOptionsWrapper.getSimpleVisualizationOptions();
    final var renderPipelineWrapper = (ShareableWrappers.RenderPipelineConfigWrapper) map.get(RenderPipelineConfig.LOCAL_MAP_KEY);
    final var renderPipelineConfig = renderPipelineWrapper != null
      ? renderPipelineWrapper.getRenderPipelineConfig()
      : RenderPipelineConfig.disabled();

    final var requestedBpResolutionParam = ctx.request().getParam("bpResolution");
    final int level;
    if (requestedBpResolutionParam != null) {
      final var requestedBpResolution = Long.parseLong(requestedBpResolutionParam);
      final var resolutionOrder = chunkedFile.getResolutionToIndex().get(requestedBpResolution);
      if (resolutionOrder == null) {
        throw new RuntimeException("Requested bpResolution is not present in opened file: " + requestedBpResolution);
      }
      level = resolutionOrder;
    } else {
      level = chunkedFile.getResolutions().length - Integer.parseInt(ctx.request().getParam("level", "0"));
    }

    final var stats = (TileStatisticHolder) map.get("TileStatisticHolder");
    if (stats == null) {
      throw new RuntimeException("Tile statistics is not present in the local map, maybe the file is not yet opened?");
    }

    var currentVersion = stats.versionCounter().get();
    long version = requestedVersion;
    if (version < currentVersion) {
      version = currentVersion;
    }
    do {
      currentVersion = stats.versionCounter().get();
    } while ((currentVersion < version) && !stats.versionCounter().compareAndSet(currentVersion, version));

    final long startRowPx, startColPx, endRowPx, endColPx;
    if (format == TileFormat.PNG_BY_PIXELS) {
      startRowPx = row;
      startColPx = col;
      endRowPx = startRowPx + Long.parseLong(ctx.request().getParam("rows", "0"));
      endColPx = startColPx + Long.parseLong(ctx.request().getParam("cols", "0"));
      tileHeight = (int) (endRowPx - startRowPx);
      tileWidth = (int) (endColPx - startColPx);
    } else {
      tileHeight = Integer.parseInt(ctx.request().getParam("tile_size", "256"));
      tileWidth = Integer.parseInt(ctx.request().getParam("tile_size", "256"));
      startRowPx = row * tileHeight;
      endRowPx = (row + 1) * tileHeight;
      startColPx = col * tileWidth;
      endColPx = (col + 1) * tileWidth;
    }

    final var matrixWithWeights = chunkedFile.matrixQueries().getSubmatrix(
      ResolutionDescriptor.fromResolutionOrder(level),
      startRowPx,
      startColPx,
      endRowPx,
      endColPx,
      true
    );
    final var secondaryMatrixWithWeights = querySecondarySubmatrix(
      secondaryChunkedFile,
      ResolutionDescriptor.fromResolutionOrder(level),
      startRowPx,
      startColPx,
      endRowPx,
      endColPx
    );
    final var trackManagerWrapper = (ShareableWrappers.Track1DManagerWrapper) map.get("Track1DManager");
    final Track1DManager track1DManager = trackManagerWrapper != null ? trackManagerWrapper.getTrack1DManager() : null;
    final var expectedProfile = resolveExpectedProfile(map, ResolutionDescriptor.fromResolutionOrder(level));

    final BufferedImage image;
    if (renderPipelineConfig.enabled()) {
      image = renderPipelineTile(
        chunkedFile,
        secondaryChunkedFile,
        matrixWithWeights,
        secondaryMatrixWithWeights,
        options,
        renderPipelineConfig,
        track1DManager
      );
    } else if (secondaryChunkedFile != null && secondaryMatrixWithWeights != null) {
      image = renderTraditionalDualSourceTile(
        chunkedFile,
        secondaryChunkedFile,
        matrixWithWeights,
        secondaryMatrixWithWeights,
        options,
        renderPipelineConfig.swapUpperLower(),
        expectedProfile
      );
    } else {
      image = chunkedFile.tileVisualizationProcessor().visualizeTile(matrixWithWeights, options, expectedProfile);
    }
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();

    try {
      if (format == TileFormat.JSON_PNG_WITH_RANGES) {
        ImageIO.write(image, "png", baos);
        final byte[] base64 = Base64.getEncoder().encode(baos.toByteArray());
        final String base64image = new String(base64);
        final var result = new TileWithRanges(
          String.format("data:image/png;base64,%s", base64image),
          buildSignalRanges(stats, chunkedFile)
        );
        return new TileResponsePayload("application/json", Json.encode(result), null);
      }

      ImageIO.write(image, "png", baos);
      return new TileResponsePayload("image/png", null, Buffer.buffer(baos.toByteArray()));
    } catch (final IOException e) {
      throw new RuntimeException("Cannot write tile image", e);
    }
  }

  private void respondCancelledTile(final @NotNull io.vertx.ext.web.RoutingContext ctx,
                                    final @NotNull TileFormat format) {
    if (format == TileFormat.JSON_PNG_WITH_RANGES) {
      final var map = vertx.sharedData().getLocalMap("hict_server");
      final var chunkedFileWrapper = ((ShareableWrappers.ChunkedFileWrapper) map.get("chunkedFile"));
      final var stats = (TileStatisticHolder) map.get("TileStatisticHolder");
      final var ranges = (chunkedFileWrapper != null && stats != null)
        ? buildSignalRanges(stats, chunkedFileWrapper.getChunkedFile())
        : new TileSignalRanges(Collections.emptyMap(), Collections.emptyMap());
      final var result = new TileWithRanges("data:image/png;base64," + TRANSPARENT_PNG_BASE64, ranges);
      ctx.response()
        .putHeader("content-type", "application/json")
        .end(Json.encode(result));
      return;
    }
    ctx.response()
      .putHeader("content-type", "image/png")
      .end(Buffer.buffer(TRANSPARENT_PNG_BYTES));
  }

  private void respondCancelledMatrixQuery(final @NotNull io.vertx.ext.web.RoutingContext ctx,
                                           final @NotNull MatrixResponseFormat format) {
    final var headers = Map.of(
      "x-hict-rows", "0",
      "x-hict-cols", "0",
      "x-hict-dtype", format.defaultDtype(),
      "x-hict-signal-mode", MatrixSignalMode.TRADITIONAL_NORMALIZED.name()
    );
    if (format == MatrixResponseFormat.JSON) {
      final var payload = new JsonObject()
        .put("rows", 0)
        .put("cols", 0)
        .put("dtype", format.defaultDtype())
        .put("signalMode", MatrixSignalMode.TRADITIONAL_NORMALIZED.name())
        .put("values", new ArrayList<>());
      final var response = ctx.response().putHeader("content-type", "application/json");
      headers.forEach(response::putHeader);
      response.end(payload.encode());
    } else {
      final var response = ctx.response().putHeader("content-type", "application/octet-stream");
      headers.forEach(response::putHeader);
      response.end(Buffer.buffer());
    }
  }

  private MatrixResponsePayload computeMatrixQueryResponse(final @NotNull JsonObject request) {
    final @NotNull @NonNull LocalMap<String, Object> map = vertx.sharedData().getLocalMap("hict_server");
    final var primaryChunkedFileWrapper = ((ShareableWrappers.ChunkedFileWrapper) (map.get("chunkedFile")));
    if (primaryChunkedFileWrapper == null) {
      throw new RuntimeException("Chunked file is not present in the local map, maybe the file is not yet opened?");
    }
    final var primaryChunkedFile = primaryChunkedFileWrapper.getChunkedFile();
    final var secondaryChunkedFileWrapper = ((ShareableWrappers.ChunkedFileWrapper) (map.get("chunkedFileSecondary")));
    final var secondaryChunkedFile = secondaryChunkedFileWrapper != null ? secondaryChunkedFileWrapper.getChunkedFile() : null;
    final var visualizationOptionsWrapper = ((ShareableWrappers.SimpleVisualizationOptionsWrapper) (map.get("visualizationOptions")));
    if (visualizationOptionsWrapper == null) {
      throw new RuntimeException("Visualization options are not present in the local map, maybe the file is not yet opened?");
    }
    final var options = visualizationOptionsWrapper.getSimpleVisualizationOptions();
    final var renderPipelineWrapper = (ShareableWrappers.RenderPipelineConfigWrapper) map.get(RenderPipelineConfig.LOCAL_MAP_KEY);
    final var renderPipelineConfig = renderPipelineWrapper != null
      ? renderPipelineWrapper.getRenderPipelineConfig()
      : RenderPipelineConfig.disabled();
    final var trackManagerWrapper = (ShareableWrappers.Track1DManagerWrapper) map.get("Track1DManager");
    final Track1DManager track1DManager = trackManagerWrapper != null ? trackManagerWrapper.getTrack1DManager() : null;

    final var bpResolution = request.getLong("bpResolution");
    if (bpResolution == null || bpResolution <= 0L) {
      throw new IllegalArgumentException("Field 'bpResolution' must be a positive integer");
    }
    final var resolutionOrder = primaryChunkedFile.getResolutionToIndex().get(bpResolution);
    if (resolutionOrder == null) {
      throw new IllegalArgumentException("Requested bpResolution is not present in opened file: " + bpResolution);
    }
    final var resolutionDescriptor = ResolutionDescriptor.fromResolutionOrder(resolutionOrder);
    final var expectedProfile = resolveExpectedProfile(map, resolutionDescriptor);

    final var units = parseUnits(request.getString("unit", request.getString("units", "PIXELS")));
    final var startRowInUnits = resolveRangeStart(request, Axis.ROW, units);
    final var startColInUnits = resolveRangeStart(request, Axis.COL, units);
    final var endRowInUnits = resolveRangeEnd(request, Axis.ROW, units, startRowInUnits);
    final var endColInUnits = resolveRangeEnd(request, Axis.COL, units, startColInUnits);
    if (endRowInUnits < startRowInUnits || endColInUnits < startColInUnits) {
      throw new IllegalArgumentException("End coordinates must be greater than or equal to start coordinates");
    }

    final var startRowPx = convertToPixels(primaryChunkedFile, resolutionDescriptor, units, startRowInUnits);
    final var startColPx = convertToPixels(primaryChunkedFile, resolutionDescriptor, units, startColInUnits);
    final var endRowPx = convertToPixels(primaryChunkedFile, resolutionDescriptor, units, endRowInUnits);
    final var endColPx = convertToPixels(primaryChunkedFile, resolutionDescriptor, units, endColInUnits);

    final var requestedSource = "SECONDARY".equalsIgnoreCase(request.getString("source", "PRIMARY"))
      ? "SECONDARY"
      : "PRIMARY";
    final var requestedChunkedFile =
      "SECONDARY".equals(requestedSource) ? secondaryChunkedFile : primaryChunkedFile;
    if (requestedChunkedFile == null) {
      throw new IllegalStateException("Secondary source is not attached");
    }
    final var matrixWithWeights =
      "SECONDARY".equals(requestedSource)
        ? querySecondarySubmatrix(
          secondaryChunkedFile,
          resolutionDescriptor,
          startRowPx,
          startColPx,
          endRowPx,
          endColPx
        )
        : primaryChunkedFile.matrixQueries().getSubmatrix(
          resolutionDescriptor,
          startRowPx,
          startColPx,
          endRowPx,
          endColPx,
          true
        );
    if (matrixWithWeights == null) {
      throw new IllegalArgumentException("Requested matrix window is outside the available extent for the selected source");
    }
    final var rawMatrix = matrixWithWeights.matrix();
    final var rowCount = rawMatrix.rows();
    final var columnCount = rawMatrix.cols();
    final long elementCount = (long) rowCount * (long) columnCount;
    if (elementCount > MAX_MATRIX_QUERY_ELEMENTS) {
      throw new IllegalArgumentException(
        "Requested matrix window is too large (" + elementCount + " elements); limit is " + MAX_MATRIX_QUERY_ELEMENTS
      );
    }

    final var signalMode = MatrixSignalMode.fromRaw(request.getString("signalMode", MatrixSignalMode.TRADITIONAL_NORMALIZED.name()));
    final var format = MatrixResponseFormat.fromRaw(request.getString("format", MatrixResponseFormat.BINARY_FLOAT32.name()));
    final var includeWeights = request.getBoolean("includeWeights", false);

    final double[][] signalMatrix;
    switch (signalMode) {
      case RAW_COUNTS -> signalMatrix = null;
      case COOLER_WEIGHTED -> signalMatrix = computeCoolerWeightedSignal(rawMatrix, matrixWithWeights.rowWeights(), matrixWithWeights.colWeights());
      case TRADITIONAL_NORMALIZED ->
        signalMatrix = requestedChunkedFile.tileVisualizationProcessor().processTile(matrixWithWeights, options, expectedProfile).values();
      case PIPELINE_SIGNAL -> {
        if ("SECONDARY".equals(requestedSource)) {
          throw new IllegalArgumentException("PIPELINE_SIGNAL queries are supported only for the primary source");
        }
        final var secondaryMatrixWithWeights = querySecondarySubmatrix(
          secondaryChunkedFile,
          resolutionDescriptor,
          matrixWithWeights.startRowIncl(),
          matrixWithWeights.startColIncl(),
          matrixWithWeights.startRowIncl() + rowCount,
          matrixWithWeights.startColIncl() + columnCount
        );
        signalMatrix = computePipelineSignalMatrix(
          primaryChunkedFile,
          secondaryChunkedFile,
          matrixWithWeights,
          secondaryMatrixWithWeights,
          options,
          renderPipelineConfig,
          track1DManager
        );
      }
      default -> throw new IllegalStateException("Unsupported matrix signal mode: " + signalMode);
    }

    final var headers = new HashMap<String, String>();
    headers.put("x-hict-rows", Integer.toString(rowCount));
    headers.put("x-hict-cols", Integer.toString(columnCount));
    headers.put("x-hict-signal-mode", signalMode.name());
    headers.put("x-hict-source", requestedSource);
    headers.put("x-hict-unit", "PIXELS");
    headers.put("x-hict-start-row-px", Long.toString(matrixWithWeights.startRowIncl()));
    headers.put("x-hict-end-row-px", Long.toString(matrixWithWeights.startRowIncl() + rowCount));
    headers.put("x-hict-start-col-px", Long.toString(matrixWithWeights.startColIncl()));
    headers.put("x-hict-end-col-px", Long.toString(matrixWithWeights.startColIncl() + columnCount));

    if (format == MatrixResponseFormat.JSON) {
      final var payload = new JsonObject()
        .put("rows", rowCount)
        .put("cols", columnCount)
        .put("signalMode", signalMode.name())
        .put("unit", "PIXELS")
        .put("startRowPx", matrixWithWeights.startRowIncl())
        .put("endRowPx", matrixWithWeights.startRowIncl() + rowCount)
        .put("startColPx", matrixWithWeights.startColIncl())
        .put("endColPx", matrixWithWeights.startColIncl() + columnCount);
      if (signalMode == MatrixSignalMode.RAW_COUNTS) {
        if (rawMatrix instanceof ru.itmo.ctlab.hict.hict_library.chunkedfile.MatrixQueries.DoubleMatrix doubleMatrix) {
          payload.put("dtype", "float64");
          payload.put("values", flattenDoubleMatrix(doubleMatrix.values(), rowCount, columnCount));
        } else if (rawMatrix instanceof ru.itmo.ctlab.hict.hict_library.chunkedfile.MatrixQueries.LongMatrix longMatrix) {
          payload.put("dtype", "int64");
          payload.put("values", flattenLongMatrix(longMatrix.values(), rowCount, columnCount));
        } else {
          throw new IllegalStateException("Unsupported raw matrix type: " + rawMatrix.getClass().getName());
        }
      } else {
        payload.put("dtype", "float64");
        payload.put("values", flattenDoubleMatrix(signalMatrix, rowCount, columnCount));
      }
      if (includeWeights) {
        payload.put("rowWeights", toJsonArray(matrixWithWeights.rowWeights(), rowCount));
        payload.put("colWeights", toJsonArray(matrixWithWeights.colWeights(), columnCount));
      }
      headers.put("x-hict-dtype", payload.getString("dtype", "float64"));
      return new MatrixResponsePayload("application/json", payload.encode(), null, headers);
    }

    switch (format) {
      case BINARY_INT64 -> {
        final byte[] binary;
        if (signalMode == MatrixSignalMode.RAW_COUNTS) {
          if (rawMatrix instanceof ru.itmo.ctlab.hict.hict_library.chunkedfile.MatrixQueries.LongMatrix longMatrix) {
            headers.put("x-hict-dtype", "int64");
            binary = encodeLongMatrixLittleEndian(longMatrix.values(), rowCount, columnCount);
          } else {
            throw new IllegalArgumentException("BINARY_INT64 is not supported for float-backed RAW_COUNTS matrices");
          }
        } else {
          headers.put("x-hict-dtype", "int64");
          binary = encodeLongMatrixLittleEndian(signalMatrix, rowCount, columnCount);
        }
        return new MatrixResponsePayload("application/octet-stream", null, Buffer.buffer(binary), headers);
      }
      case BINARY_FLOAT64 -> {
        headers.put("x-hict-dtype", "float64");
        final var source = signalMode == MatrixSignalMode.RAW_COUNTS ? toDoubleMatrix(rawMatrix) : signalMatrix;
        final var binary = encodeDoubleMatrixLittleEndian(source, rowCount, columnCount);
        return new MatrixResponsePayload("application/octet-stream", null, Buffer.buffer(binary), headers);
      }
      case BINARY_FLOAT32 -> {
        headers.put("x-hict-dtype", "float32");
        final var source = signalMode == MatrixSignalMode.RAW_COUNTS ? toDoubleMatrix(rawMatrix) : signalMatrix;
        final var binary = encodeFloatMatrixLittleEndian(source, rowCount, columnCount);
        return new MatrixResponsePayload("application/octet-stream", null, Buffer.buffer(binary), headers);
      }
      default -> throw new IllegalStateException("Unsupported matrix response format: " + format);
    }
  }

  private static double[][] computeCoolerWeightedSignal(final ru.itmo.ctlab.hict.hict_library.chunkedfile.MatrixQueries.RawMatrix rawMatrix,
                                                        final double[] rowWeights,
                                                        final double[] colWeights) {
    final var rowCount = rawMatrix.rows();
    final var columnCount = rawMatrix.cols();
    final var result = new double[rowCount][columnCount];
    for (int row = 0; row < rowCount; row++) {
      final var rowWeight = rowWeights != null && row < rowWeights.length ? rowWeights[row] : 1.0d;
      for (int col = 0; col < columnCount; col++) {
        final var colWeight = colWeights != null && col < colWeights.length ? colWeights[col] : 1.0d;
        result[row][col] = rawMatrix.getAsDouble(row, col) * rowWeight * colWeight;
      }
    }
    return result;
  }

  private @Nullable DistanceExpectedNormalizer.DiagonalProfile resolveExpectedProfile(
    final @NotNull LocalMap<String, Object> map,
    final @NotNull ResolutionDescriptor resolutionDescriptor
  ) {
    final var wrapper =
      (ShareableWrappers.DiagonalExpectedProfileWrapper) map.get(EXPECTED_PROFILE_LOCAL_MAP_KEY);
    if (wrapper == null) {
      return null;
    }
    final var profile = wrapper.getDiagonalProfile();
    return profile.resolutionOrder() == resolutionDescriptor.getResolutionOrderInArray()
      ? profile
      : null;
  }

  private DistanceExpectedNormalizer.PixelDomain @NotNull [] buildExpectedDomains(
    final @NotNull ChunkedFile chunkedFile,
    final @NotNull ResolutionDescriptor resolutionDescriptor,
    final long startRowPx,
    final long endRowPx,
    final long startColPx,
    final long endColPx
  ) {
    final var scaffoldDomains = new ArrayList<DistanceExpectedNormalizer.PixelDomain>();
    final var basePairsResolution = ResolutionDescriptor.fromResolutionOrder(0);
    for (final var scaffold : chunkedFile.getScaffoldTree().getScaffoldList()) {
      final var borders = scaffold.scaffoldBordersBP();
      final var startPx = chunkedFile.convertUnits(
        borders.startBP(),
        basePairsResolution,
        QueryLengthUnit.BASE_PAIRS,
        resolutionDescriptor,
        QueryLengthUnit.PIXELS
      );
      final var endPx = chunkedFile.convertUnits(
        borders.endBP(),
        basePairsResolution,
        QueryLengthUnit.BASE_PAIRS,
        resolutionDescriptor,
        QueryLengthUnit.PIXELS
      );
      addExpectedDomain(
        scaffoldDomains,
        startPx,
        endPx
      );
    }

    final var domains = new ArrayList<>(scaffoldDomains);
    long contigStartPx = 0L;
    for (final var contig : chunkedFile.getContigTree().getOrderedContigList()) {
      final var contigLengthPx = contig.descriptor().getLengthInUnits(
        QueryLengthUnit.PIXELS,
        resolutionDescriptor
      );
      final var currentContigStartPx = contigStartPx;
      final var contigEndPx = currentContigStartPx + contigLengthPx;
      if (contigLengthPx > 0L && scaffoldDomains.stream().noneMatch(domain ->
        intervalsOverlap(currentContigStartPx, contigEndPx, domain.startPx(), domain.endPx())
      )) {
        addExpectedDomain(
          domains,
          currentContigStartPx,
          contigEndPx
        );
      }
      contigStartPx = contigEndPx;
    }

    return domains.stream()
      .filter(domain -> domain.intersects(startRowPx, endRowPx))
      .filter(domain -> domain.intersects(startColPx, endColPx))
      .sorted((left, right) -> Long.compare(left.startPx(), right.startPx()))
      .toArray(DistanceExpectedNormalizer.PixelDomain[]::new);
  }

  private static void addExpectedDomain(final @NotNull ArrayList<DistanceExpectedNormalizer.PixelDomain> domains,
                                        final long startPx,
                                        final long endPx) {
    if (endPx <= startPx) {
      return;
    }
    if (domains.stream().anyMatch(domain -> domain.startPx() == startPx && domain.endPx() == endPx)) {
      return;
    }
    domains.add(new DistanceExpectedNormalizer.PixelDomain(startPx, endPx));
  }

  private static boolean intervalsOverlap(final long leftStart,
                                          final long leftEnd,
                                          final long rightStart,
                                          final long rightEnd) {
    return leftEnd > rightStart && leftStart < rightEnd;
  }

  private double[][] computePipelineSignalMatrix(final @NotNull ChunkedFile primaryChunkedFile,
                                                 final ChunkedFile secondaryChunkedFile,
                                                 final @NotNull ru.itmo.ctlab.hict.hict_library.chunkedfile.MatrixQueries.MatrixWithWeights primaryMatrixWithWeights,
                                                 final ru.itmo.ctlab.hict.hict_library.chunkedfile.MatrixQueries.MatrixWithWeights secondaryMatrixWithWeights,
                                                 final @NotNull ru.itmo.ctlab.hict.hict_library.visualization.SimpleVisualizationOptions options,
                                                 final @NotNull RenderPipelineConfig pipelineConfig,
                                                 final Track1DManager track1DManager) {
    final var primaryValues = primaryMatrixWithWeights.matrix();
    final var rowCount = primaryValues.rows();
    final var columnCount = primaryValues.cols();
    final var result = new double[rowCount][columnCount];
    if (rowCount == 0 || columnCount == 0) {
      return result;
    }

    final var secondaryValues = new double[rowCount][columnCount];
    if (secondaryMatrixWithWeights != null && secondaryChunkedFile != null) {
      final var candidate = secondaryMatrixWithWeights.matrix();
      final var candidateRowCount = candidate.rows();
      final var candidateColCount = candidate.cols();
      final var rowOffset =
        (int) (secondaryMatrixWithWeights.startRowIncl() - primaryMatrixWithWeights.startRowIncl());
      final var colOffset =
        (int) (secondaryMatrixWithWeights.startColIncl() - primaryMatrixWithWeights.startColIncl());
      for (int row = 0; row < candidateRowCount; row++) {
        final var dstRow = row + rowOffset;
        if (dstRow < 0 || dstRow >= rowCount) {
          continue;
        }
        for (int col = 0; col < candidateColCount; col++) {
          final var dstCol = col + colOffset;
          if (dstCol < 0 || dstCol >= columnCount) {
            continue;
          }
          secondaryValues[dstRow][dstCol] = candidate.getAsDouble(row, col);
        }
      }
    }

    final var resolutionDescriptor = primaryMatrixWithWeights.resolutionDescriptor();
    final var bpResolution = primaryChunkedFile.getResolutions()[resolutionDescriptor.getResolutionOrderInArray()];
    final var bpResolutionDescriptor = ResolutionDescriptor.fromResolutionOrder(0);
    final var totalVisiblePixels = primaryChunkedFile.getContigTree().getLengthInUnits(
      QueryLengthUnit.PIXELS,
      resolutionDescriptor
    );
    final var resolutionOrder = resolutionDescriptor.getResolutionOrderInArray();
    final var matrixSizeBins = primaryChunkedFile.getMatrixSizeBins();
    final var totalBinsAtResolution =
      resolutionOrder >= 0 && resolutionOrder < matrixSizeBins.length
        ? matrixSizeBins[resolutionOrder]
        : Long.MAX_VALUE;

    final var rowPxValues = new long[rowCount];
    final var rowBinValues = new long[rowCount];
    final var rowBpValues = new long[rowCount];
    for (int row = 0; row < rowCount; ++row) {
      final var rowPx = primaryMatrixWithWeights.startRowIncl() + row;
      rowPxValues[row] = rowPx;
      rowBinValues[row] = primaryChunkedFile.convertUnits(
        rowPx,
        resolutionDescriptor,
        QueryLengthUnit.PIXELS,
        resolutionDescriptor,
        QueryLengthUnit.BINS
      );
      rowBpValues[row] = primaryChunkedFile.convertUnits(
        rowPx,
        resolutionDescriptor,
        QueryLengthUnit.PIXELS,
        bpResolutionDescriptor,
        QueryLengthUnit.BASE_PAIRS
      );
    }

    final var colPxValues = new long[columnCount];
    final var colBinValues = new long[columnCount];
    final var colBpValues = new long[columnCount];
    for (int col = 0; col < columnCount; ++col) {
      final var colPx = primaryMatrixWithWeights.startColIncl() + col;
      colPxValues[col] = colPx;
      colBinValues[col] = primaryChunkedFile.convertUnits(
        colPx,
        resolutionDescriptor,
        QueryLengthUnit.PIXELS,
        resolutionDescriptor,
        QueryLengthUnit.BINS
      );
      colBpValues[col] = primaryChunkedFile.convertUnits(
        colPx,
        resolutionDescriptor,
        QueryLengthUnit.PIXELS,
        bpResolutionDescriptor,
        QueryLengthUnit.BASE_PAIRS
      );
    }

    final var context = new RenderPipelineConfig.MutablePixelContext();
    final var rowWeights = primaryMatrixWithWeights.rowWeights();
    final var colWeights = primaryMatrixWithWeights.colWeights();
    final var resolutionScalingCoeffs = primaryChunkedFile.getResolutionScalingCoefficient();
    final var resolutionLinearScalingCoeffs = primaryChunkedFile.getResolutionLinearScalingCoefficient();
    final var resolutionScalingCoeff =
      resolutionOrder >= 0 && resolutionOrder < resolutionScalingCoeffs.length
        ? resolutionScalingCoeffs[resolutionOrder]
        : 1.0d;
    final var resolutionLinearScalingCoeff =
      resolutionOrder >= 0 && resolutionOrder < resolutionLinearScalingCoeffs.length
        ? resolutionLinearScalingCoeffs[resolutionOrder]
        : 1.0d;

    final var rowTrackValuesByTrackId = new HashMap<String, double[]>();
    final var colTrackValuesByTrackId = new HashMap<String, double[]>();
    if (track1DManager != null) {
      for (final var binding : pipelineConfig.requiredTrackBindings()) {
        if (binding.axis() == RenderPipelineConfig.TrackAxis.ROW) {
          if (!rowTrackValuesByTrackId.containsKey(binding.trackId())) {
            final var sampled = track1DManager.sampleTrackValues(
              primaryChunkedFile,
              binding.trackId(),
              primaryMatrixWithWeights.startRowIncl(),
              primaryMatrixWithWeights.startRowIncl() + rowCount,
              bpResolution,
              QueryLengthUnit.PIXELS
            );
            rowTrackValuesByTrackId.put(binding.trackId(), normalizeTrackValues(sampled, rowCount));
          }
        } else {
          if (!colTrackValuesByTrackId.containsKey(binding.trackId())) {
            final var sampled = track1DManager.sampleTrackValues(
              primaryChunkedFile,
              binding.trackId(),
              primaryMatrixWithWeights.startColIncl(),
              primaryMatrixWithWeights.startColIncl() + columnCount,
              bpResolution,
              QueryLengthUnit.PIXELS
            );
            colTrackValuesByTrackId.put(binding.trackId(), normalizeTrackValues(sampled, columnCount));
          }
        }
      }
    }
    context.rowTrackValuesByTrackId = rowTrackValuesByTrackId;
    context.colTrackValuesByTrackId = colTrackValuesByTrackId;

    for (int row = 0; row < rowCount; ++row) {
      final var rowWeight = rowWeights != null && row < rowWeights.length ? rowWeights[row] : 1.0d;
      final var rowPx = rowPxValues[row];
      final var rowBin = rowBinValues[row];
      final var rowBp = rowBpValues[row];
      for (int col = 0; col < columnCount; ++col) {
        final var primaryValue = primaryValues.getAsDouble(row, col);
        final var secondaryValue = secondaryValues[row][col];
        final var colPx = colPxValues[col];
        final var colBin = colBinValues[col];
        final var rowOutside =
          rowPx < 0L || rowPx >= totalVisiblePixels || rowBin < 0L || rowBin >= totalBinsAtResolution;
        final var colOutside =
          colPx < 0L || colPx >= totalVisiblePixels || colBin < 0L || colBin >= totalBinsAtResolution;
        if (rowOutside || colOutside) {
          result[row][col] = 0.0d;
          continue;
        }
        final var colWeight = colWeights != null && col < colWeights.length ? colWeights[col] : 1.0d;
        context.primaryValue = Double.isFinite(primaryValue) ? primaryValue : 0.0d;
        context.secondaryValue = Double.isFinite(secondaryValue) ? secondaryValue : 0.0d;
        context.rowWeight = rowWeight;
        context.colWeight = colWeight;
        context.resolutionScalingCoeff = resolutionScalingCoeff;
        context.resolutionLinearScalingCoeff = resolutionLinearScalingCoeff;
        context.rowPx = rowPx;
        context.colPx = colPx;
        context.rowBin = rowBin;
        context.colBin = colBin;
        context.rowBp = rowBp;
        context.colBp = colBpValues[col];
        context.bpResolution = bpResolution;
        context.rowLocalIndex = row;
        context.colLocalIndex = col;
        result[row][col] = pipelineConfig.evaluate(context.rowPx <= context.colPx, context);
      }
    }
    return result;
  }

  private static byte[] encodeFloatMatrixLittleEndian(final double[][] matrix, final int rows, final int cols) {
    final var bb = ByteBuffer.allocate(rows * cols * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    for (int row = 0; row < rows; row++) {
      final var sourceRow = matrix[row];
      for (int col = 0; col < cols; col++) {
        bb.putFloat((float) sourceRow[col]);
      }
    }
    return bb.array();
  }

  private static byte[] encodeDoubleMatrixLittleEndian(final double[][] matrix, final int rows, final int cols) {
    final var bb = ByteBuffer.allocate(rows * cols * Double.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    for (int row = 0; row < rows; row++) {
      final var sourceRow = matrix[row];
      for (int col = 0; col < cols; col++) {
        bb.putDouble(sourceRow[col]);
      }
    }
    return bb.array();
  }

  private static byte[] encodeLongMatrixLittleEndian(final long[][] matrix, final int rows, final int cols) {
    final var bb = ByteBuffer.allocate(rows * cols * Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    for (int row = 0; row < rows; row++) {
      final var sourceRow = matrix[row];
      for (int col = 0; col < cols; col++) {
        bb.putLong(sourceRow[col]);
      }
    }
    return bb.array();
  }

  private static byte[] encodeLongMatrixLittleEndian(final double[][] matrix, final int rows, final int cols) {
    final var bb = ByteBuffer.allocate(rows * cols * Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    for (int row = 0; row < rows; row++) {
      final var sourceRow = matrix[row];
      for (int col = 0; col < cols; col++) {
        bb.putLong(Math.round(sourceRow[col]));
      }
    }
    return bb.array();
  }

  private static ArrayList<Double> toJsonArray(final double[] values, final int expectedLength) {
    final var result = new ArrayList<Double>(Math.max(0, expectedLength));
    for (int i = 0; i < expectedLength; i++) {
      final var value = values != null && i < values.length ? values[i] : 1.0d;
      result.add(value);
    }
    return result;
  }

  private static ArrayList<Double> flattenDoubleMatrix(final double[][] matrix, final int rows, final int cols) {
    final var result = new ArrayList<Double>(rows * cols);
    for (int row = 0; row < rows; row++) {
      final var sourceRow = matrix[row];
      for (int col = 0; col < cols; col++) {
        result.add(sourceRow[col]);
      }
    }
    return result;
  }

  private static ArrayList<Long> flattenLongMatrix(final long[][] matrix, final int rows, final int cols) {
    final var result = new ArrayList<Long>(rows * cols);
    for (int row = 0; row < rows; row++) {
      final var sourceRow = matrix[row];
      for (int col = 0; col < cols; col++) {
        result.add(sourceRow[col]);
      }
    }
    return result;
  }

  private static double[][] toDoubleMatrix(final ru.itmo.ctlab.hict.hict_library.chunkedfile.MatrixQueries.RawMatrix matrix) {
    if (matrix instanceof ru.itmo.ctlab.hict.hict_library.chunkedfile.MatrixQueries.DoubleMatrix doubleMatrix) {
      return doubleMatrix.values();
    }
    if (matrix instanceof ru.itmo.ctlab.hict.hict_library.chunkedfile.MatrixQueries.LongMatrix longMatrix) {
      final var rows = longMatrix.rows();
      final var cols = longMatrix.cols();
      final var result = new double[rows][cols];
      final var source = longMatrix.values();
      for (int row = 0; row < rows; row++) {
        final var sourceRow = source[row];
        final var targetRow = result[row];
        for (int col = 0; col < cols; col++) {
          targetRow[col] = sourceRow[col];
        }
      }
      return result;
    }
    throw new IllegalStateException("Unsupported raw matrix type: " + matrix.getClass().getName());
  }

  private long resolveRangeStart(final @NotNull JsonObject request,
                                 final @NotNull Axis axis,
                                 final @NotNull QueryLengthUnit units) {
    final var keySuffix = axis == Axis.ROW ? "Row" : "Col";
    final String[] keys = switch (units) {
      case PIXELS -> new String[]{"start" + keySuffix + "Px", "start" + keySuffix, "startPx", "start"};
      case BINS -> new String[]{"start" + keySuffix + "Bin", "start" + keySuffix, "startBin", "start"};
      case BASE_PAIRS -> new String[]{"start" + keySuffix + "BP", "start" + keySuffix, "startBP", "start"};
    };
    return getLong(request, 0L, keys);
  }

  private long resolveRangeEnd(final @NotNull JsonObject request,
                               final @NotNull Axis axis,
                               final @NotNull QueryLengthUnit units,
                               final long startValue) {
    final var keySuffix = axis == Axis.ROW ? "Row" : "Col";
    final String[] endKeys = switch (units) {
      case PIXELS -> new String[]{"end" + keySuffix + "Px", "end" + keySuffix, "endPx", "end"};
      case BINS -> new String[]{"end" + keySuffix + "Bin", "end" + keySuffix, "endBin", "end"};
      case BASE_PAIRS -> new String[]{"end" + keySuffix + "BP", "end" + keySuffix, "endBP", "end"};
    };
    final var explicitEnd = getOptionalLong(request, endKeys);
    if (explicitEnd != null) {
      return explicitEnd;
    }
    final String[] lengthKeys = axis == Axis.ROW
      ? new String[]{"rows", "height", "rowCount"}
      : new String[]{"cols", "width", "colCount"};
    final var length = getLong(request, 0L, lengthKeys);
    return startValue + Math.max(0L, length);
  }

  private static long convertToPixels(final @NotNull ChunkedFile chunkedFile,
                                      final @NotNull ResolutionDescriptor resolutionDescriptor,
                                      final @NotNull QueryLengthUnit units,
                                      final long value) {
    return switch (units) {
      case PIXELS -> value;
      case BINS -> chunkedFile.convertUnits(
        value,
        resolutionDescriptor,
        QueryLengthUnit.BINS,
        resolutionDescriptor,
        QueryLengthUnit.PIXELS
      );
      case BASE_PAIRS -> chunkedFile.convertUnits(
        value,
        ResolutionDescriptor.fromResolutionOrder(0),
        QueryLengthUnit.BASE_PAIRS,
        resolutionDescriptor,
        QueryLengthUnit.PIXELS
      );
    };
  }

  private static QueryLengthUnit parseUnits(final @NotNull String rawValue) {
    final var normalized = rawValue.trim().toUpperCase();
    return switch (normalized) {
      case "PIXEL", "PIXELS", "PX" -> QueryLengthUnit.PIXELS;
      case "BIN", "BINS" -> QueryLengthUnit.BINS;
      case "BP", "BASE_PAIRS", "BASEPAIR", "BASEPAIRS" -> QueryLengthUnit.BASE_PAIRS;
      default -> throw new IllegalArgumentException(
        "Unsupported unit '" + rawValue + "'. Use one of: PIXELS, BINS, BP."
      );
    };
  }

  private static long getLong(final @NotNull JsonObject request,
                              final long fallback,
                              final @NotNull String... keys) {
    final var value = getOptionalLong(request, keys);
    return value != null ? value : fallback;
  }

  private static Long getOptionalLong(final @NotNull JsonObject request,
                                      final @NotNull String... keys) {
    for (final var key : keys) {
      final var value = request.getValue(key);
      if (value instanceof Number number) {
        return number.longValue();
      }
    }
    return null;
  }

  private TileSignalRanges buildSignalRanges(final @NotNull TileStatisticHolder stats,
                                             final @NotNull ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile chunkedFile) {
    return new TileSignalRanges(
      IntStream.range(0, chunkedFile.getResolutions().length).boxed().map(
        lvl -> Map.entry(chunkedFile.getResolutions().length - lvl, Double.longBitsToDouble(stats.minimumsAtResolutionDoubleBits().get(lvl)))
      ).collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue)),
      IntStream.range(0, chunkedFile.getResolutions().length).boxed().map(
        lvl -> Map.entry(chunkedFile.getResolutions().length - lvl, Double.longBitsToDouble(stats.maximumsAtResolutionDoubleBits().get(lvl)))
      ).collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue))
    );
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


  public enum TileFormat {
    JSON_PNG_WITH_RANGES,
    PNG,
    PNG_BY_PIXELS
  }

  public record TileSignalRanges(@NotNull Map<@NotNull Integer, @NotNull Double> lowerBounds,
                                 @NotNull Map<@NotNull Integer, @NotNull Double> upperBounds) {
  }

  public record TileWithRanges(@NotNull String image, @NotNull TileSignalRanges ranges) {
  }

  private record TileResponsePayload(@NotNull String contentType,
                                     String jsonBody,
                                     Buffer binaryBody) {
  }

  private record MatrixResponsePayload(@NotNull String contentType,
                                       String jsonBody,
                                       Buffer binaryBody,
                                       @NotNull Map<String, String> headers) {
  }

  private enum Axis {
    ROW,
    COL
  }

  public enum MatrixSignalMode {
    RAW_COUNTS,
    COOLER_WEIGHTED,
    TRADITIONAL_NORMALIZED,
    PIPELINE_SIGNAL;

    static @NotNull MatrixSignalMode fromRaw(final @NotNull String rawValue) {
      final var normalized = rawValue.trim().toUpperCase();
      return switch (normalized) {
        case "RAW", "RAW_COUNTS", "COUNTS" -> RAW_COUNTS;
        case "COOLER_WEIGHTED", "WEIGHTED", "BALANCED" -> COOLER_WEIGHTED;
        case "TRADITIONAL_NORMALIZED", "NORMALIZED", "VISUALIZATION_NORMALIZED" -> TRADITIONAL_NORMALIZED;
        case "PIPELINE_SIGNAL", "PIPELINE", "RENDER_PIPELINE_SIGNAL" -> PIPELINE_SIGNAL;
        default -> throw new IllegalArgumentException(
          "Unsupported matrix signal mode '" + rawValue + "'. Use RAW_COUNTS, COOLER_WEIGHTED, TRADITIONAL_NORMALIZED or PIPELINE_SIGNAL."
        );
      };
    }
  }

  public enum MatrixResponseFormat {
    JSON,
    BINARY_FLOAT32,
    BINARY_FLOAT64,
    BINARY_INT64;

    static @NotNull MatrixResponseFormat fromRaw(final @NotNull String rawValue) {
      final var normalized = rawValue.trim().toUpperCase();
      return switch (normalized) {
        case "JSON", "JSON_FLAT" -> JSON;
        case "BINARY_FLOAT32", "FLOAT32", "F32" -> BINARY_FLOAT32;
        case "BINARY_FLOAT64", "FLOAT64", "F64" -> BINARY_FLOAT64;
        case "BINARY_INT64", "INT64", "I64" -> BINARY_INT64;
        default -> throw new IllegalArgumentException(
          "Unsupported matrix response format '" + rawValue + "'. Use JSON, BINARY_FLOAT32, BINARY_FLOAT64 or BINARY_INT64."
        );
      };
    }

    @NotNull String defaultDtype() {
      return switch (this) {
        case JSON -> "float64";
        case BINARY_FLOAT32 -> "float32";
        case BINARY_FLOAT64 -> "float64";
        case BINARY_INT64 -> "int64";
      };
    }
  }

  private @NotNull BufferedImage renderTraditionalDualSourceTile(final @NotNull ChunkedFile primaryChunkedFile,
                                                                 final @NotNull ChunkedFile secondaryChunkedFile,
                                                                 final @NotNull ru.itmo.ctlab.hict.hict_library.chunkedfile.MatrixQueries.MatrixWithWeights primaryMatrixWithWeights,
                                                                 final @NotNull ru.itmo.ctlab.hict.hict_library.chunkedfile.MatrixQueries.MatrixWithWeights secondaryMatrixWithWeights,
                                                                 final @NotNull ru.itmo.ctlab.hict.hict_library.visualization.SimpleVisualizationOptions options,
                                                                 final boolean swapUpperLower,
                                                                 final @Nullable DistanceExpectedNormalizer.DiagonalProfile expectedProfile) {
    final var primaryImage = primaryChunkedFile.tileVisualizationProcessor().visualizeTile(primaryMatrixWithWeights, options, expectedProfile);
    final var secondaryImage = secondaryChunkedFile.tileVisualizationProcessor().visualizeTile(secondaryMatrixWithWeights, options, expectedProfile);
    final var rowCount = primaryImage.getHeight();
    final var columnCount = primaryImage.getWidth();
    final var result = new BufferedImage(columnCount, rowCount, BufferedImage.TYPE_INT_ARGB);
    if (rowCount <= 0 || columnCount <= 0) {
      return result;
    }
    final var primaryRgba = primaryImage.getRGB(0, 0, columnCount, rowCount, null, 0, columnCount);
    final var secondaryRgba = new int[rowCount * columnCount];
    final var secondaryRows = secondaryImage.getHeight();
    final var secondaryCols = secondaryImage.getWidth();
    final var secondaryOffsetRow =
      Math.max(0, (int) (secondaryMatrixWithWeights.startRowIncl() - primaryMatrixWithWeights.startRowIncl()));
    final var secondaryOffsetCol =
      Math.max(0, (int) (secondaryMatrixWithWeights.startColIncl() - primaryMatrixWithWeights.startColIncl()));
    if (secondaryRows > 0 && secondaryCols > 0) {
      final var rawSecondaryRgba = secondaryImage.getRGB(0, 0, secondaryCols, secondaryRows, null, 0, secondaryCols);
      for (int srcRow = 0; srcRow < secondaryRows; srcRow++) {
        final var dstRow = secondaryOffsetRow + srcRow;
        if (dstRow < 0 || dstRow >= rowCount) {
          continue;
        }
        for (int srcCol = 0; srcCol < secondaryCols; srcCol++) {
          final var dstCol = secondaryOffsetCol + srcCol;
          if (dstCol < 0 || dstCol >= columnCount) {
            continue;
          }
          secondaryRgba[dstRow * columnCount + dstCol] = rawSecondaryRgba[srcRow * secondaryCols + srcCol];
        }
      }
    }
    final var merged = new int[rowCount * columnCount];
    final var rowStartPx = primaryMatrixWithWeights.startRowIncl();
    final var colStartPx = primaryMatrixWithWeights.startColIncl();
    int index = 0;
    for (int row = 0; row < rowCount; row++) {
      final long rowPx = rowStartPx + row;
      for (int col = 0; col < columnCount; col++) {
        final long colPx = colStartPx + col;
        final boolean upperTriangle = rowPx <= colPx;
        final boolean usePrimary = swapUpperLower ? !upperTriangle : upperTriangle;
        merged[index] = usePrimary ? primaryRgba[index] : secondaryRgba[index];
        index++;
      }
    }
    result.setRGB(0, 0, columnCount, rowCount, merged, 0, columnCount);
    return result;
  }

  private @NotNull BufferedImage renderPipelineTile(final @NotNull ChunkedFile primaryChunkedFile,
                                                    final ChunkedFile secondaryChunkedFile,
                                                    final @NotNull ru.itmo.ctlab.hict.hict_library.chunkedfile.MatrixQueries.MatrixWithWeights primaryMatrixWithWeights,
                                                    final ru.itmo.ctlab.hict.hict_library.chunkedfile.MatrixQueries.MatrixWithWeights secondaryMatrixWithWeights,
                                                    final @NotNull ru.itmo.ctlab.hict.hict_library.visualization.SimpleVisualizationOptions options,
                                                    final @NotNull RenderPipelineConfig pipelineConfig,
                                                    final Track1DManager track1DManager) {
    final var primaryValues = primaryMatrixWithWeights.matrix();
    final var rowCount = primaryValues.rows();
    final var columnCount = primaryValues.cols();
    final var image = new BufferedImage(columnCount, rowCount, BufferedImage.TYPE_INT_ARGB);
    final var rgba = new int[Math.max(0, rowCount * columnCount)];
    if (rowCount == 0 || columnCount == 0) {
      return image;
    }

    final var secondaryValues = new double[rowCount][columnCount];
    if (secondaryMatrixWithWeights != null && secondaryChunkedFile != null) {
      final var candidate = secondaryMatrixWithWeights.matrix();
      final var candidateRowCount = candidate.rows();
      final var candidateColCount = candidate.cols();
      final var rowOffset =
        (int) (secondaryMatrixWithWeights.startRowIncl() - primaryMatrixWithWeights.startRowIncl());
      final var colOffset =
        (int) (secondaryMatrixWithWeights.startColIncl() - primaryMatrixWithWeights.startColIncl());
      for (int row = 0; row < candidateRowCount; row++) {
        final var dstRow = row + rowOffset;
        if (dstRow < 0 || dstRow >= rowCount) {
          continue;
        }
        for (int col = 0; col < candidateColCount; col++) {
          final var dstCol = col + colOffset;
          if (dstCol < 0 || dstCol >= columnCount) {
            continue;
          }
          secondaryValues[dstRow][dstCol] = candidate.getAsDouble(row, col);
        }
      }
    }

    final var resolutionDescriptor = primaryMatrixWithWeights.resolutionDescriptor();
    final var bpResolution = primaryChunkedFile.getResolutions()[resolutionDescriptor.getResolutionOrderInArray()];
    final var bpResolutionDescriptor = ResolutionDescriptor.fromResolutionOrder(0);
    final var totalVisiblePixels = primaryChunkedFile.getContigTree().getLengthInUnits(
      QueryLengthUnit.PIXELS,
      resolutionDescriptor
    );
    final var resolutionOrder = resolutionDescriptor.getResolutionOrderInArray();
    final var matrixSizeBins = primaryChunkedFile.getMatrixSizeBins();
    final var totalBinsAtResolution =
      resolutionOrder >= 0 && resolutionOrder < matrixSizeBins.length
        ? matrixSizeBins[resolutionOrder]
        : Long.MAX_VALUE;

    final var rowPxValues = new long[rowCount];
    final var rowBinValues = new long[rowCount];
    final var rowBpValues = new long[rowCount];
    for (int row = 0; row < rowCount; ++row) {
      final var rowPx = primaryMatrixWithWeights.startRowIncl() + row;
      rowPxValues[row] = rowPx;
      rowBinValues[row] = primaryChunkedFile.convertUnits(
        rowPx,
        resolutionDescriptor,
        QueryLengthUnit.PIXELS,
        resolutionDescriptor,
        QueryLengthUnit.BINS
      );
      rowBpValues[row] = primaryChunkedFile.convertUnits(
        rowPx,
        resolutionDescriptor,
        QueryLengthUnit.PIXELS,
        bpResolutionDescriptor,
        QueryLengthUnit.BASE_PAIRS
      );
    }

    final var colPxValues = new long[columnCount];
    final var colBinValues = new long[columnCount];
    final var colBpValues = new long[columnCount];
    for (int col = 0; col < columnCount; ++col) {
      final var colPx = primaryMatrixWithWeights.startColIncl() + col;
      colPxValues[col] = colPx;
      colBinValues[col] = primaryChunkedFile.convertUnits(
        colPx,
        resolutionDescriptor,
        QueryLengthUnit.PIXELS,
        resolutionDescriptor,
        QueryLengthUnit.BINS
      );
      colBpValues[col] = primaryChunkedFile.convertUnits(
        colPx,
        resolutionDescriptor,
        QueryLengthUnit.PIXELS,
        bpResolutionDescriptor,
        QueryLengthUnit.BASE_PAIRS
      );
    }

    final var context = new RenderPipelineConfig.MutablePixelContext();
    final var rowWeights = primaryMatrixWithWeights.rowWeights();
    final var colWeights = primaryMatrixWithWeights.colWeights();
    final var resolutionScalingCoeffs = primaryChunkedFile.getResolutionScalingCoefficient();
    final var resolutionLinearScalingCoeffs = primaryChunkedFile.getResolutionLinearScalingCoefficient();
    final var resolutionScalingCoeff =
      resolutionOrder >= 0 && resolutionOrder < resolutionScalingCoeffs.length
        ? resolutionScalingCoeffs[resolutionOrder]
        : 1.0d;
    final var resolutionLinearScalingCoeff =
      resolutionOrder >= 0 && resolutionOrder < resolutionLinearScalingCoeffs.length
        ? resolutionLinearScalingCoeffs[resolutionOrder]
        : 1.0d;

    final var rowTrackValuesByTrackId = new HashMap<String, double[]>();
    final var colTrackValuesByTrackId = new HashMap<String, double[]>();
    if (track1DManager != null) {
      for (final var binding : pipelineConfig.requiredTrackBindings()) {
        if (binding.axis() == RenderPipelineConfig.TrackAxis.ROW) {
          if (!rowTrackValuesByTrackId.containsKey(binding.trackId())) {
            final var sampled = track1DManager.sampleTrackValues(
              primaryChunkedFile,
              binding.trackId(),
              primaryMatrixWithWeights.startRowIncl(),
              primaryMatrixWithWeights.startRowIncl() + rowCount,
              bpResolution,
              QueryLengthUnit.PIXELS
            );
            rowTrackValuesByTrackId.put(binding.trackId(), normalizeTrackValues(sampled, rowCount));
          }
        } else {
          if (!colTrackValuesByTrackId.containsKey(binding.trackId())) {
            final var sampled = track1DManager.sampleTrackValues(
              primaryChunkedFile,
              binding.trackId(),
              primaryMatrixWithWeights.startColIncl(),
              primaryMatrixWithWeights.startColIncl() + columnCount,
              bpResolution,
              QueryLengthUnit.PIXELS
            );
            colTrackValuesByTrackId.put(binding.trackId(), normalizeTrackValues(sampled, columnCount));
          }
        }
      }
    }
    context.rowTrackValuesByTrackId = rowTrackValuesByTrackId;
    context.colTrackValuesByTrackId = colTrackValuesByTrackId;

    int pixelIndex = 0;
    for (int row = 0; row < rowCount; ++row) {
      final var rowWeight = rowWeights != null && row < rowWeights.length ? rowWeights[row] : 1.0d;
      final var rowPx = rowPxValues[row];
      final var rowBin = rowBinValues[row];
      final var rowBp = rowBpValues[row];
      for (int col = 0; col < columnCount; ++col) {
        final var primaryValue = primaryValues.getAsDouble(row, col);
        final var secondaryValue = secondaryValues[row][col];
        final var colPx = colPxValues[col];
        final var colBin = colBinValues[col];
        final var rowOutside =
          rowPx < 0L || rowPx >= totalVisiblePixels || rowBin < 0L || rowBin >= totalBinsAtResolution;
        final var colOutside =
          colPx < 0L || colPx >= totalVisiblePixels || colBin < 0L || colBin >= totalBinsAtResolution;
        if (rowOutside || colOutside) {
          rgba[pixelIndex++] = 0x00000000;
          continue;
        }
        final var colWeight = colWeights != null && col < colWeights.length ? colWeights[col] : 1.0d;
        context.primaryValue = Double.isFinite(primaryValue) ? primaryValue : 0.0d;
        context.secondaryValue = Double.isFinite(secondaryValue) ? secondaryValue : 0.0d;
        context.rowWeight = rowWeight;
        context.colWeight = colWeight;
        context.resolutionScalingCoeff = resolutionScalingCoeff;
        context.resolutionLinearScalingCoeff = resolutionLinearScalingCoeff;
        context.rowPx = rowPx;
        context.colPx = colPx;
        context.rowBin = rowBin;
        context.colBin = colBin;
        context.rowBp = rowBp;
        context.colBp = colBpValues[col];
        context.bpResolution = bpResolution;
        context.rowLocalIndex = row;
        context.colLocalIndex = col;
        rgba[pixelIndex++] = pipelineConfig.evaluateArgb(context.rowPx <= context.colPx, context, options);
      }
    }
    image.setRGB(0, 0, columnCount, rowCount, rgba, 0, columnCount);
    return image;
  }

  private double @NotNull [] normalizeTrackValues(final double[] sampled,
                                                   final int expectedLength) {
    final var safeLength = Math.max(0, expectedLength);
    if (safeLength == 0) {
      return new double[0];
    }
    if (sampled == null || sampled.length == 0) {
      return new double[safeLength];
    }
    if (sampled.length == safeLength) {
      return sampled;
    }
    final var result = new double[safeLength];
    System.arraycopy(sampled, 0, result, 0, Math.min(sampled.length, safeLength));
    return result;
  }

  private @Nullable ru.itmo.ctlab.hict.hict_library.chunkedfile.MatrixQueries.MatrixWithWeights querySecondarySubmatrix(
    final @Nullable ChunkedFile secondaryChunkedFile,
    final @NotNull ResolutionDescriptor resolutionDescriptor,
    final long startRowPx,
    final long startColPx,
    final long endRowPx,
    final long endColPx
  ) {
    if (secondaryChunkedFile == null) {
      return null;
    }
    final var maxVisiblePixels = secondaryChunkedFile.getContigTree().getLengthInUnits(
      QueryLengthUnit.PIXELS,
      resolutionDescriptor
    );
    if (maxVisiblePixels <= 0L) {
      return null;
    }
    final var clampedStartRow = Math.max(0L, Math.min(startRowPx, maxVisiblePixels));
    final var clampedEndRow = Math.max(clampedStartRow, Math.min(endRowPx, maxVisiblePixels));
    final var clampedStartCol = Math.max(0L, Math.min(startColPx, maxVisiblePixels));
    final var clampedEndCol = Math.max(clampedStartCol, Math.min(endColPx, maxVisiblePixels));
    if (clampedEndRow <= clampedStartRow || clampedEndCol <= clampedStartCol) {
      return null;
    }
    try {
      return secondaryChunkedFile.matrixQueries().getSubmatrix(
        resolutionDescriptor,
        clampedStartRow,
        clampedStartCol,
        clampedEndRow,
        clampedEndCol,
        true
      );
    } catch (final RuntimeException ex) {
      log.debug("Failed to query secondary submatrix for requested window", ex);
      return null;
    }
  }
}
