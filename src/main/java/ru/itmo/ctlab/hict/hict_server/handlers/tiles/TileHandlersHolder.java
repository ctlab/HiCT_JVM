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

package ru.itmo.ctlab.hict.hict_server.handlers.tiles;

import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;
import io.vertx.ext.web.Router;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.resolution.ResolutionDescriptor;
import ru.itmo.ctlab.hict.hict_server.HandlersHolder;
import ru.itmo.ctlab.hict.hict_server.dto.symmetric.visualization.VisualizationOptionsDTO;
import ru.itmo.ctlab.hict.hict_server.handlers.util.TileStatisticHolder;
import ru.itmo.ctlab.hict.hict_server.util.shareable.ShareableWrappers;

import javax.imageio.ImageIO;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@RequiredArgsConstructor
@Slf4j
public class TileHandlersHolder extends HandlersHolder {
  private final Vertx vertx;

  @Override
  public void addHandlersToRouter(final @NotNull Router router) {
    router.post("/set_visualization_options").blockingHandler(ctx -> {
      final @NotNull var requestBody = ctx.body();
      final @NotNull var requestJSON = requestBody.asJsonObject();

      final @NotNull @NonNull var request = VisualizationOptionsDTO.fromJSONObject(requestJSON);

      final var map = vertx.sharedData().getLocalMap("hict_server");
      log.debug("Got map");
      map.put("visualizationOptions", new ShareableWrappers.SimpleVisualizationOptionsWrapper(request.toEntity()));
      final var chunkedFileWrapper = ((ShareableWrappers.ChunkedFileWrapper) (map.get("chunkedFile")));
      if (chunkedFileWrapper == null) {
        ctx.fail(new RuntimeException("Chunked file is not present in the local map, maybe the file is not yet opened?"));
        return;
      }
      final var chunkedFile = chunkedFileWrapper.getChunkedFile();
      log.debug("Got ChunkedFile from map");

      final var stats = (TileStatisticHolder) map.get("TileStatisticHolder");
      if (stats == null) {
        ctx.fail(new RuntimeException("Tile statistics is not present in the local map, maybe the file is not yet opened?"));
        return;
      }
      final var newStats = TileStatisticHolder.newDefaultStatisticHolder(chunkedFile.getResolutions().length);
      newStats.versionCounter().set(stats.versionCounter().get());
      map.put("TileStatisticHolder", newStats);
      final var visualizationOptionsWrapper = ((ShareableWrappers.SimpleVisualizationOptionsWrapper) (map.get("visualizationOptions")));
      if (visualizationOptionsWrapper == null) {
        ctx.fail(new RuntimeException("Visualization options are not present in the local map, maybe the file is not yet opened?"));
        return;
      }
      final var options = visualizationOptionsWrapper.getSimpleVisualizationOptions();
      ctx.response().setStatusCode(200).end(Json.encode(VisualizationOptionsDTO.fromEntity(options, chunkedFile)));
    });

    router.post("/get_visualization_options").blockingHandler(ctx -> {
      final var map = vertx.sharedData().getLocalMap("hict_server");
      log.debug("Got map");
      final var chunkedFileWrapper = ((ShareableWrappers.ChunkedFileWrapper) (map.get("chunkedFile")));
      if (chunkedFileWrapper == null) {
        ctx.fail(new RuntimeException("Chunked file is not present in the local map, maybe the file is not yet opened?"));
        return;
      }
      final var chunkedFile = chunkedFileWrapper.getChunkedFile();
      log.debug("Got ChunkedFile from map");
      final var visualizationOptionsWrapper = ((ShareableWrappers.SimpleVisualizationOptionsWrapper) (map.get("visualizationOptions")));
      if (visualizationOptionsWrapper == null) {
        ctx.fail(new RuntimeException("Visualization options are not present in the local map, maybe the file is not yet opened?"));
        return;
      }
      final var options = visualizationOptionsWrapper.getSimpleVisualizationOptions();
      ctx.response().setStatusCode(200).end(Json.encode(VisualizationOptionsDTO.fromEntity(options, chunkedFile)));
    });

    router.get("/get_tile").handler(ctx -> {
      log.debug("Entered non-blocking handler");
      ctx.next();
    }).blockingHandler(ctx -> {
      log.debug("Entered blockingHandler");

      final var row = Long.parseLong(ctx.request().getParam("row", "0"));
      final var col = Long.parseLong(ctx.request().getParam("col", "0"));
      final var version = Long.parseLong(ctx.request().getParam("version", "0"));
      final int tileHeight;
      final int tileWidth;
      final var format = TileFormat.valueOf(ctx.request().getParam("format", "JSON_PNG_WITH_RANGES"));

      log.debug("Got parameters");

      final var map = vertx.sharedData().getLocalMap("hict_server");

      log.debug("Got map");
      final var chunkedFileWrapper = ((ShareableWrappers.ChunkedFileWrapper) (map.get("chunkedFile")));
      if (chunkedFileWrapper == null) {
        ctx.fail(new RuntimeException("Chunked file is not present in the local map, maybe the file is not yet opened?"));
        return;
      }
      final var chunkedFile = chunkedFileWrapper.getChunkedFile();
      log.debug("Got ChunkedFile from map");
      final var visualizationOptionsWrapper = ((ShareableWrappers.SimpleVisualizationOptionsWrapper) (map.get("visualizationOptions")));
      if (visualizationOptionsWrapper == null) {
        ctx.fail(new RuntimeException("Visualization options are not present in the local map, maybe the file is not yet opened?"));
        return;
      }
      final var options = visualizationOptionsWrapper.getSimpleVisualizationOptions();

      final var level = chunkedFile.getResolutions().length - Integer.parseInt(ctx.request().getParam("level", "0"));

      final var stats = (TileStatisticHolder) map.get("TileStatisticHolder");
      if (stats == null) {
        ctx.fail(new RuntimeException("Tile statistics is not present in the local map, maybe the file is not yet opened?"));
        return;
      }

      var currentVersion = stats.versionCounter().get();
      if (version < currentVersion) {
        log.debug(String.format("Current version is %d and request version is %d", currentVersion, version));
        ctx.response().setStatusCode(204).putHeader("Content-Type", "text/plain").end(String.format("Current version is %d and request version is %d", currentVersion, version));
        return;
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
      final var image = chunkedFile.tileVisualizationProcessor().visualizeTile(matrixWithWeights, options);
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();

      log.debug("Created byte stream");

      /*
      try (final var pool = Executors.newSingleThreadExecutor()) {
        pool.submit(() -> {
          // Update signal ranges
          final var tileSummary = Arrays.stream(normalized).flatMapToLong(Arrays::stream).summaryStatistics();
          final var tileMinimum = tileSummary.getMin();
          final var tileMaximum = tileSummary.getMax();

          long oldMinimumDoubleBits;
          do {
            oldMinimumDoubleBits = stats.minimumsAtResolutionDoubleBits().get(level);
          } while (Double.longBitsToDouble(oldMinimumDoubleBits) > tileMinimum && !stats.minimumsAtResolutionDoubleBits().compareAndSet(level, oldMinimumDoubleBits, Double.doubleToLongBits(tileMinimum)));

          long oldMaximumDoubleBits;
          do {
            oldMaximumDoubleBits = stats.maximumsAtResolutionDoubleBits().get(level);
          } while (Double.longBitsToDouble(oldMaximumDoubleBits) < tileMaximum && !stats.maximumsAtResolutionDoubleBits().compareAndSet(level, oldMaximumDoubleBits, Double.doubleToLongBits(tileMaximum)));
        });
      }
      */


      try {
        if (format == TileFormat.JSON_PNG_WITH_RANGES) {
          ImageIO.write(image, "png", baos); // convert BufferedImage to byte array

          final byte[] base64 = Base64.getEncoder().encode(baos.toByteArray());
          final String base64image = new String(base64);
          final var result = new TileWithRanges(
            String.format("data:image/png;base64,%s", base64image),
            new TileSignalRanges(
              IntStream.range(0, chunkedFile.getResolutions().length).boxed().map(
                lvl -> Map.entry(chunkedFile.getResolutions().length - lvl, Double.longBitsToDouble(stats.minimumsAtResolutionDoubleBits().get(lvl)))
              ).collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue)),
              IntStream.range(0, chunkedFile.getResolutions().length).boxed().map(
                lvl -> Map.entry(chunkedFile.getResolutions().length - lvl, Double.longBitsToDouble(stats.maximumsAtResolutionDoubleBits().get(lvl)))
              ).collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue))
            )
          );
          log.debug("Wrote stream to buffer");
          ctx.response()
            .putHeader("content-type", "application/json")
            .end(Json.encode(result));
        } else {
          ImageIO.write(image, "png", baos); // convert BufferedImage to byte array
          log.debug("Wrote stream to buffer");
          ctx.response()
            .putHeader("content-type", "image/png")
            .end(Buffer.buffer(baos.toByteArray()));
        }
        log.debug("Response");
      } catch (final IOException e) {
        log.error("Cannot write tile image: " + e.getMessage());
      }
    });
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
}
