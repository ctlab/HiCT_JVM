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
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.domain.QueryLengthUnit;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.resolution.ResolutionDescriptor;
import ru.itmo.ctlab.hict.hict_library.visualization.TileWithWeights;
import ru.itmo.ctlab.hict.hict_server.HandlersHolder;
import ru.itmo.ctlab.hict.hict_server.concurrent.RequestTaskScheduler;
import ru.itmo.ctlab.hict.hict_server.dto.symmetric.visualization.VisualizationOptionsDTO;
import ru.itmo.ctlab.hict.hict_server.handlers.util.TileStatisticHolder;
import ru.itmo.ctlab.hict.hict_server.util.shareable.ShareableWrappers;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Base64;
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
          map.put("visualizationOptions", new ShareableWrappers.SimpleVisualizationOptionsWrapper(request.toEntity()));
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
    final var chunkedFileWrapper = ((ShareableWrappers.ChunkedFileWrapper) (map.get("chunkedFile")));
    if (chunkedFileWrapper == null) {
      throw new RuntimeException("Chunked file is not present in the local map, maybe the file is not yet opened?");
    }
    final var chunkedFile = chunkedFileWrapper.getChunkedFile();
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

    final var matrixWithWeights = chunkedFile.matrixQueries().getSubmatrix(ResolutionDescriptor.fromResolutionOrder(level), startRowPx, startColPx, endRowPx, endColPx, true);
    final BufferedImage image = renderPipelineConfig.enabled()
      ? renderPipelineTile(chunkedFile, matrixWithWeights, options, renderPipelineConfig)
      : chunkedFile.tileVisualizationProcessor().visualizeTile(matrixWithWeights, options);
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

  private @NotNull BufferedImage renderPipelineTile(final @NotNull ChunkedFile chunkedFile,
                                                    final @NotNull ru.itmo.ctlab.hict.hict_library.chunkedfile.MatrixQueries.MatrixWithWeights matrixWithWeights,
                                                    final @NotNull ru.itmo.ctlab.hict.hict_library.visualization.SimpleVisualizationOptions options,
                                                    final @NotNull RenderPipelineConfig pipelineConfig) {
    final TileWithWeights primaryTile = chunkedFile.tileVisualizationProcessor().processTile(matrixWithWeights, options);
    final var primaryValues = primaryTile.values();
    final var rowCount = primaryValues.length;
    final var columnCount = rowCount > 0 ? primaryValues[0].length : 0;
    final var image = new BufferedImage(columnCount, rowCount, BufferedImage.TYPE_INT_ARGB);
    final var rgba = new int[Math.max(0, rowCount * columnCount)];
    if (rowCount == 0 || columnCount == 0) {
      return image;
    }

    final var resolutionDescriptor = matrixWithWeights.resolutionDescriptor();
    final var bpResolution = chunkedFile.getResolutions()[resolutionDescriptor.getResolutionOrderInArray()];
    final var bpResolutionDescriptor = ResolutionDescriptor.fromResolutionOrder(0);

    final var rowPxValues = new long[rowCount];
    final var rowBinValues = new long[rowCount];
    final var rowBpValues = new long[rowCount];
    for (int row = 0; row < rowCount; ++row) {
      final var rowPx = matrixWithWeights.startRowIncl() + row;
      rowPxValues[row] = rowPx;
      rowBinValues[row] = chunkedFile.convertUnits(
        rowPx,
        resolutionDescriptor,
        QueryLengthUnit.PIXELS,
        resolutionDescriptor,
        QueryLengthUnit.BINS
      );
      rowBpValues[row] = chunkedFile.convertUnits(
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
      final var colPx = matrixWithWeights.startColIncl() + col;
      colPxValues[col] = colPx;
      colBinValues[col] = chunkedFile.convertUnits(
        colPx,
        resolutionDescriptor,
        QueryLengthUnit.PIXELS,
        resolutionDescriptor,
        QueryLengthUnit.BINS
      );
      colBpValues[col] = chunkedFile.convertUnits(
        colPx,
        resolutionDescriptor,
        QueryLengthUnit.PIXELS,
        bpResolutionDescriptor,
        QueryLengthUnit.BASE_PAIRS
      );
    }

    final var context = new RenderPipelineConfig.MutablePixelContext();
    final var rowWeights = primaryTile.rowWeights();
    final var colWeights = primaryTile.columnWeights();
    int pixelIndex = 0;
    for (int row = 0; row < rowCount; ++row) {
      final var rowWeight = rowWeights != null && row < rowWeights.length ? rowWeights[row] : 1.0d;
      final var rowPx = rowPxValues[row];
      final var rowBin = rowBinValues[row];
      final var rowBp = rowBpValues[row];
      for (int col = 0; col < columnCount; ++col) {
        final var colWeight = colWeights != null && col < colWeights.length ? colWeights[col] : 1.0d;
        context.primaryValue = primaryValues[row][col];
        context.secondaryValue = primaryValues[row][col];
        context.rowWeight = rowWeight;
        context.colWeight = colWeight;
        context.rowPx = rowPx;
        context.colPx = colPxValues[col];
        context.rowBin = rowBin;
        context.colBin = colBinValues[col];
        context.rowBp = rowBp;
        context.colBp = colBpValues[col];
        context.bpResolution = bpResolution;

        final var value = pipelineConfig.evaluate(context.rowPx <= context.colPx, context);
        final Color color = options.getColormap().mapSignal(value);
        rgba[pixelIndex++] =
          ((color.getAlpha() & 0xFF) << 24)
            | ((color.getRed() & 0xFF) << 16)
            | ((color.getGreen() & 0xFF) << 8)
            | (color.getBlue() & 0xFF);
      }
    }
    image.setRGB(0, 0, columnCount, rowCount, rgba, 0, columnCount);
    return image;
  }
}
