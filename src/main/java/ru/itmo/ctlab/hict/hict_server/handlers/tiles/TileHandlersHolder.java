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
import ru.itmo.ctlab.hict.hict_library.visualization.SimpleVisualizationOptions;
import ru.itmo.ctlab.hict.hict_server.HandlersHolder;
import ru.itmo.ctlab.hict.hict_server.dto.request.tiles.ContrastRangeSettingsDTO;
import ru.itmo.ctlab.hict.hict_server.dto.request.tiles.NormalizationSettingsDTO;
import ru.itmo.ctlab.hict.hict_server.handlers.util.TileStatisticHolder;
import ru.itmo.ctlab.hict.hict_server.util.shareable.ShareableWrappers;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.*;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@RequiredArgsConstructor
@Slf4j
public class TileHandlersHolder extends HandlersHolder {
  private final Vertx vertx;

  @Override
  public void addHandlersToRouter(final @NotNull Router router) {
    router.post("/set_contrast_range").blockingHandler(ctx -> {
      final @NotNull var requestBody = ctx.body();
      final @NotNull var requestJSON = requestBody.asJsonObject();

      final @NotNull @NonNull var request = ContrastRangeSettingsDTO.fromJSONObject(requestJSON);

      final var map = vertx.sharedData().getLocalMap("hict_server");
      log.debug("Got map");
      final var chunkedFileWrapper = ((ShareableWrappers.ChunkedFileWrapper) (map.get("chunkedFile")));
      if (chunkedFileWrapper == null) {
        ctx.fail(new RuntimeException("Chunked file is not present in the local map, maybe the file is not yet opened?"));
        return;
      }
      final var chunkedFile = chunkedFileWrapper.getChunkedFile();
      log.debug("Got ChunkedFile from map");

      final var tileVisualizationProcessor = chunkedFile.tileVisualizationProcessor();
      final var lock = tileVisualizationProcessor.getVisualizationOptionsLock();

      try {
        lock.writeLock().lock();
        final var oldSettings = tileVisualizationProcessor.getVisualizationOptions();
        final var newOptions = new SimpleVisualizationOptions(
          oldSettings.getPreLogBase(),
          oldSettings.getPostLogBase(),
          oldSettings.isApplyCoolerWeights(),
          request.lowerSignalBound(),
          request.upperSignalBound()
        );
        tileVisualizationProcessor.setVisualizationOptions(newOptions);
      } finally {
        lock.writeLock().unlock();
      }

      final var stats = (TileStatisticHolder) map.get("TileStatisticHolder");
      if (stats == null) {
        ctx.fail(new RuntimeException("Tile statistics is not present in the local map, maybe the file is not yet opened?"));
        return;
      }
      final var newStats = TileStatisticHolder.newDefaultStatisticHolder(chunkedFile.getResolutions().length);
      newStats.versionCounter().set(stats.versionCounter().get());
      map.put("TileStatisticHolder", newStats);


      ctx.response().setStatusCode(200).end();
    });

    router.post("/set_normalization").blockingHandler(ctx -> {
      final @NotNull var requestBody = ctx.body();
      final @NotNull var requestJSON = requestBody.asJsonObject();

      final @NotNull @NonNull var request = NormalizationSettingsDTO.fromJSONObject(requestJSON);

      final var map = vertx.sharedData().getLocalMap("hict_server");
      log.debug("Got map");
      final var chunkedFileWrapper = ((ShareableWrappers.ChunkedFileWrapper) (map.get("chunkedFile")));
      if (chunkedFileWrapper == null) {
        ctx.fail(new RuntimeException("Chunked file is not present in the local map, maybe the file is not yet opened?"));
        return;
      }
      final var chunkedFile = chunkedFileWrapper.getChunkedFile();
      log.debug("Got ChunkedFile from map");

      final var tileVisualizationProcessor = chunkedFile.tileVisualizationProcessor();
      final var lock = tileVisualizationProcessor.getVisualizationOptionsLock();

      try {
        lock.writeLock().lock();
        final var oldSettings = tileVisualizationProcessor.getVisualizationOptions();
        final var newOptions = new SimpleVisualizationOptions(
          request.preLogBase(),
          request.postLogBase(),
          request.applyCoolerWeights(),
          oldSettings.getLowerThreshold(),
          oldSettings.getUpperThreshold()
        );
        tileVisualizationProcessor.setVisualizationOptions(newOptions);
      } finally {
        lock.writeLock().unlock();
      }
      ctx.response().setStatusCode(200).end();
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

      final var level = chunkedFile.getResolutions().length - Integer.parseInt(ctx.request().getParam("level", "0"));

      final var stats = (TileStatisticHolder) map.get("TileStatisticHolder");
      if (stats == null) {
        ctx.fail(new RuntimeException("Tile statistics is not present in the local map, maybe the file is not yet opened?"));
        return;
      }

      final var currentVersion = stats.versionCounter().get();
      if ((version < currentVersion) || ((version > currentVersion) && !stats.versionCounter().compareAndSet(currentVersion, version))) {
        ctx.response().setStatusCode(204).end(String.format("Current version is %d and request version is %d", currentVersion, version));
        return;
      }

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

      final SimpleVisualizationOptions visualizationOptions;
      try {
        chunkedFile.tileVisualizationProcessor().getVisualizationOptionsLock().readLock().lock();
        visualizationOptions = chunkedFile.tileVisualizationProcessor().getVisualizationOptions();
      } finally {
        chunkedFile.tileVisualizationProcessor().getVisualizationOptionsLock().readLock().unlock();
      }

      final var matrixWithWeights = chunkedFile.matrixQueries().getSubmatrix(ResolutionDescriptor.fromResolutionOrder(level), startRowPx, startColPx, endRowPx, endColPx, true);
      final var dense = matrixWithWeights.matrix();
      log.debug("Got dense matrix");
//        final var normalized = Arrays.stream(dense).map(arrayRow -> Arrays.stream(arrayRow).mapToDouble(Math::log).mapToLong(Math::round).mapToInt(l -> (int) l).toArray()).toArray(int[][]::new);
      final var normalized = chunkedFile.tileVisualizationProcessor().processTile(dense, matrixWithWeights.rowWeights(), matrixWithWeights.colWeights());
      log.debug("Normalized dense matrix");

      final var boxedRGBValues = Arrays.stream(normalized)
        .flatMap(arrayRow ->
          Arrays.stream(arrayRow).boxed().
            flatMap(nVal -> Stream.of(
                (byte) ((nVal > visualizationOptions.getLowerThreshold()) ? 0x00 : 0xFF), // Red
                (byte) ((nVal > visualizationOptions.getLowerThreshold()) ? (
                  (nVal < visualizationOptions.getUpperThreshold()) ? (0xFF - 16 * (nVal.byteValue())) : (0xFF)
                ) : (0xFF)
                ), // Green
                (byte) ((nVal > visualizationOptions.getLowerThreshold()) ? 0x00 : 0xFF) // Blue
              )
            )
        )
        .toArray(Byte[]::new);

      final byte[] rgbValues = new byte[boxedRGBValues.length];
      for (var i = 0; i < boxedRGBValues.length; ++i) {
        rgbValues[i] = boxedRGBValues[i];
      }

      final DataBuffer buffer = new DataBufferByte(rgbValues, rgbValues.length);

      //3 bytes per pixel: red, green, blue
      final WritableRaster raster = Raster.createInterleavedRaster(buffer, tileWidth, tileHeight, 3 * tileWidth, 3, new int[]{0, 1, 2}, null);
      final ColorModel cm = new ComponentColorModel(ColorModel.getRGBdefault().getColorSpace(), false, true, Transparency.OPAQUE, DataBuffer.TYPE_BYTE);
      final BufferedImage image = new BufferedImage(cm, raster, true, null);
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();

      log.debug("Created byte stream");

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
