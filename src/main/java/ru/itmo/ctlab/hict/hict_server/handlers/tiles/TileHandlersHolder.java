package ru.itmo.ctlab.hict.hict_server.handlers.tiles;

import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;
import io.vertx.ext.web.Router;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.resolution.ResolutionDescriptor;
import ru.itmo.ctlab.hict.hict_server.HandlersHolder;
import ru.itmo.ctlab.hict.hict_server.util.shareable.ShareableWrappers;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.*;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Base64;
import java.util.stream.Stream;

@RequiredArgsConstructor
@Slf4j
public class TileHandlersHolder extends HandlersHolder {
  private final Vertx vertx;

  @Override
  public void addHandlersToRouter(final @NotNull Router router) {
    router.get("/get_tile").handler(ctx -> {
      log.debug("Entered non-blocking handler");
      ctx.next();
    }).blockingHandler(ctx -> {
      log.debug("Entered blockingHandler");

      final var row = Long.parseLong(ctx.request().getParam("row", "0"));
      final var col = Long.parseLong(ctx.request().getParam("col", "0"));
      final var version = Long.parseLong(ctx.request().getParam("version", "0"));
      final var tileHeight = Integer.parseInt(ctx.request().getParam("tile_size", "256"));
      final var tileWidth = Integer.parseInt(ctx.request().getParam("tile_size", "256"));
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

      final var matrixWithWeights = chunkedFile.getSubmatrix(ResolutionDescriptor.fromResolutionOrder(level), row * tileHeight, col * tileWidth, (row + 1) * tileHeight, (col + 1) * tileWidth, true);
      final var dense = matrixWithWeights.matrix();
      log.debug("Got dense matrix");
      final var normalized = Arrays.stream(dense).map(arrayRow -> Arrays.stream(arrayRow).mapToDouble(Math::log).mapToLong(Math::round).mapToInt(l -> (int) l).toArray()).toArray(int[][]::new);
      log.debug("Normalized dense matrix");
      final var boxedRGBValues = Arrays.stream(normalized)
        .flatMap(arrayRow ->
          Arrays.stream(arrayRow).boxed().
            flatMap(nVal -> Stream.of((byte) ((nVal > 0) ? 0x00 : 0xFF), (byte) ((nVal > 0) ? (0xFF - 16 * (nVal.byteValue())) : (0xFF)), (byte) ((nVal > 0) ? 0x00 : 0xFF)))
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


      try {

        switch (format) {
          case JSON_PNG_WITH_RANGES -> {
            ImageIO.write(image, "png", baos); // convert BufferedImage to byte array

            final byte[] base64 = Base64.getEncoder().encode(baos.toByteArray());
            final String base64image = new String(base64);
            final var result = new TileWithRanges(
              String.format("data:image/png;base64,%s", base64image),
              new TileSignalRanges(0.0, 1.0)
            );
            log.debug("Wrote stream to buffer");
            ctx.response()
              .putHeader("content-type", "application/json")
              .end(Json.encode(result));
          }
          case PNG -> {
            ImageIO.write(image, "png", baos); // convert BufferedImage to byte array
            log.debug("Wrote stream to buffer");
            ctx.response()
              .putHeader("content-type", "image/png")
              .end(Buffer.buffer(baos.toByteArray()));
          }
        }
        log.debug("Response");
      } catch (final IOException e) {
        log.error("Cannot write tile image: " + e.getMessage());
      }
    });
  }


  public enum TileFormat {
    JSON_PNG_WITH_RANGES,
    PNG
  }

  public record TileSignalRanges(double lowerBounds, double upperBounds) {
  }

  public record TileWithRanges(@NotNull String image, @NotNull TileSignalRanges ranges) {
  }
}
