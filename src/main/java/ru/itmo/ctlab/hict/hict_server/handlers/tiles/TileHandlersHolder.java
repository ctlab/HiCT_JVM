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
  private static final String TRANSPARENT_PNG_BASE64 =
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO7Z3ioAAAAASUVORK5CYII=";
  private static final byte[] TRANSPARENT_PNG_BYTES = Base64.getDecoder().decode(TRANSPARENT_PNG_BASE64);

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
      scheduler.submit(
        ctx,
        RequestTaskScheduler.RequestPriority.ASSEMBLY,
        null,
        () -> {
          final @NotNull @NonNull LocalMap<String, Object> map = vertx.sharedData().getLocalMap("hict_server");
          log.debug("Got map");
          final var optionsEntity = request.toEntity();
          map.put("visualizationOptions", new ShareableWrappers.SimpleVisualizationOptionsWrapper(optionsEntity));
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
        renderPipelineConfig.swapUpperLower()
      );
    } else {
      image = chunkedFile.tileVisualizationProcessor().visualizeTile(matrixWithWeights, options);
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

  private @NotNull BufferedImage renderTraditionalDualSourceTile(final @NotNull ChunkedFile primaryChunkedFile,
                                                                 final @NotNull ChunkedFile secondaryChunkedFile,
                                                                 final @NotNull ru.itmo.ctlab.hict.hict_library.chunkedfile.MatrixQueries.MatrixWithWeights primaryMatrixWithWeights,
                                                                 final @NotNull ru.itmo.ctlab.hict.hict_library.chunkedfile.MatrixQueries.MatrixWithWeights secondaryMatrixWithWeights,
                                                                 final @NotNull ru.itmo.ctlab.hict.hict_library.visualization.SimpleVisualizationOptions options,
                                                                 final boolean swapUpperLower) {
    final var primaryImage = primaryChunkedFile.tileVisualizationProcessor().visualizeTile(primaryMatrixWithWeights, options);
    final var secondaryImage = secondaryChunkedFile.tileVisualizationProcessor().visualizeTile(secondaryMatrixWithWeights, options);
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
    final var rowCount = primaryValues.length;
    final var columnCount = rowCount > 0 ? primaryValues[0].length : 0;
    final var image = new BufferedImage(columnCount, rowCount, BufferedImage.TYPE_INT_ARGB);
    final var rgba = new int[Math.max(0, rowCount * columnCount)];
    if (rowCount == 0 || columnCount == 0) {
      return image;
    }

    final var secondaryValues = new double[rowCount][columnCount];
    if (secondaryMatrixWithWeights != null && secondaryChunkedFile != null) {
      final var candidate = secondaryMatrixWithWeights.matrix();
      final var candidateRowCount = candidate.length;
      final var candidateColCount =
        candidateRowCount > 0 && candidate[0] != null ? candidate[0].length : 0;
      final var rowOffset =
        (int) (secondaryMatrixWithWeights.startRowIncl() - primaryMatrixWithWeights.startRowIncl());
      final var colOffset =
        (int) (secondaryMatrixWithWeights.startColIncl() - primaryMatrixWithWeights.startColIncl());
      for (int row = 0; row < candidateRowCount; row++) {
        final var dstRow = row + rowOffset;
        if (dstRow < 0 || dstRow >= rowCount) {
          continue;
        }
        final var sourceRow = candidate[row];
        if (sourceRow == null) {
          continue;
        }
        final var sourceColCount = Math.min(sourceRow.length, candidateColCount);
        for (int col = 0; col < sourceColCount; col++) {
          final var dstCol = col + colOffset;
          if (dstCol < 0 || dstCol >= columnCount) {
            continue;
          }
          secondaryValues[dstRow][dstCol] = sourceRow[col];
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
        final var primaryValue = (double) primaryValues[row][col];
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
