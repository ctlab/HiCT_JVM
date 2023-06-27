package ru.itmo.ctlab.hict.hict_server;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.Json;
import io.vertx.core.logging.SLF4JLogDelegateFactory;
import io.vertx.core.shareddata.Shareable;
import io.vertx.ext.web.Router;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.*;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class MainVerticle extends AbstractVerticle {

  @Getter
  @RequiredArgsConstructor
  private static class ChunkedFileWrapper implements Shareable {
    private final ChunkedFile chunkedFile;
  }

  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    // set vertx logger delegate factory to slf4j
    String logFactory = System.getProperty("org.vertx.logger-delegate-factory-class-name");
    if (logFactory == null) {
      System.setProperty("org.vertx.logger-delegate-factory-class-name", SLF4JLogDelegateFactory.class.getName());
    }

    final var server = vertx.createHttpServer();
    final var router = Router.router(vertx);
    final Path dataDirectory;

    try {
      dataDirectory = Paths.get("/home/tux/HiCT/HiCT_Server/data").toAbsolutePath();
      final var chunkedFile = new ChunkedFile(Path.of("/home/tux/HiCT/HiCT_Server/data/arab_dong_4DN.mcool.hict.hdf5"));
      final var chunkedFileWrapper = new ChunkedFileWrapper(chunkedFile);


      log.info("Trying local map");
      final var map = vertx.sharedData().getLocalMap("hict_server");
      map.put("chunkedFile", chunkedFileWrapper);
      log.info("Added to local map");
    } finally {
      log.info("Finished maps");
    }


    log.info("Configuring router");

    router.get("/get_tile").handler(ctx -> {
      log.info("Entered non-blocking handler");
      ctx.next();
    }).blockingHandler(ctx -> {
      log.info("Entered blockingHandler");

      // ... Do some blocking operation
      final var row = Long.parseLong(ctx.request().getParam("row", "0"));
      final var col = Long.parseLong(ctx.request().getParam("col", "0"));
      final var resolution = Long.parseLong(ctx.request().getParam("resolution", "0"));

      log.info("Got parameters");

//      final var chunkedFile = new ChunkedFile(Path.of("/home/tux/IdeaProjects/hict-server/build/resources/main/zanu_male_4DN.mcool.hict.hdf5"));

      final var map = vertx.sharedData().getLocalMap("hict_server");
      log.info("Got map");
      final var chunkedFile = ((ChunkedFileWrapper) (map.get("chunkedFile"))).getChunkedFile();
      log.info("Got ChunkedFile from map");
      final var dense = chunkedFile.getStripeIntersectionAsDenseMatrix(row, col, resolution);
      log.info("Got dense matrix");
      final var normalized = Arrays.stream(dense).map(arrayRow -> Arrays.stream(arrayRow).mapToDouble(Math::log).mapToLong(Math::round).mapToInt(l -> (int) l).toArray()).toArray(int[][]::new);
      log.info("Normalized dense matrix");
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


      int width = 256;
      int height = 256;

      final DataBuffer buffer = new DataBufferByte(rgbValues, rgbValues.length);

      //3 bytes per pixel: red, green, blue
      final WritableRaster raster = Raster.createInterleavedRaster(buffer, width, height, 3 * width, 3, new int[]{0, 1, 2}, (Point) null);
      final ColorModel cm = new ComponentColorModel(ColorModel.getRGBdefault().getColorSpace(), false, true, Transparency.OPAQUE, DataBuffer.TYPE_BYTE);
      final BufferedImage image = new BufferedImage(cm, raster, true, null);
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();

      log.info("Created byte stream");

      try {
//        ImageIO.write(image, "png", new File("image.png"));
        ImageIO.write(image, "png", baos); // convert BufferedImage to byte array
        log.info("Wrote stream to buffer");
        //ctx.response()
        ctx.response()
          .putHeader("content-type", "image/png")
          .end(Buffer.buffer(baos.toByteArray()));
        log.info("Response");
      } catch (final IOException e) {
        System.out.println("Cannot write image: " + e.getMessage());
      }
    });

    router.get("/list_files").handler(ctx -> {
      log.info("Entered non-blocking handler");
      ctx.next();
    }).blockingHandler(ctx -> {
      log.info("Listing HiCT HDF5 files");
      final List<Path> files;
      try (final var stream = Files.walk(dataDirectory)) {
        files = stream.filter((name) -> name.getFileName().toString().endsWith(".hict.hdf5")).map(Path::normalize).map(Path::toAbsolutePath).map(dataDirectory::relativize).collect(Collectors.toList());
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
      final var json = Json.encode(files);
      ctx.response().putHeader("content-type", "application/json").end(json);
    });

    log.info("Starting server");
    server.requestHandler(router).

      listen(5000);
    log.info("Server started");
  }


  public void helloWorld() {
    System.out.println("Hello world!");

    final var chunkedFile = new ChunkedFile(Path.of("/home/tux/IdeaProjects/hict-server/build/resources/main/zanu_male_4DN.mcool.hict.hdf5"));

    final var dense = chunkedFile.getStripeIntersectionAsDenseMatrix(0L, 0L, 250000L);

    final var normalized = Arrays.stream(dense).map(row -> Arrays.stream(row).mapToDouble(Math::log).mapToLong(Math::round).mapToInt(l -> (int) l).toArray()).toArray(int[][]::new);


    final var boxedRGBValues = Arrays.stream(normalized)
      .flatMap(row ->
        Arrays.stream(row).boxed().
          flatMap(nVal -> Stream.of((byte) ((nVal > 0) ? 0x00 : 0xFF), (byte) ((nVal > 0) ? (0xFF - 16 * (nVal.byteValue())) : (0xFF)), (byte) ((nVal > 0) ? 0x00 : 0xFF)))
      )
      .toArray(Byte[]::new);

    final byte[] rgbValues = new byte[boxedRGBValues.length];
    for (var i = 0; i < boxedRGBValues.length; ++i) {
      rgbValues[i] = boxedRGBValues[i];
    }


    int width = 256;
    int height = 256;

    final DataBuffer buffer = new DataBufferByte(rgbValues, rgbValues.length);

    //3 bytes per pixel: red, green, blue
    final WritableRaster raster = Raster.createInterleavedRaster(buffer, width, height, 3 * width, 3, new int[]{0, 1, 2}, (Point) null);
    ColorModel cm = new ComponentColorModel(ColorModel.getRGBdefault().getColorSpace(), false, true, Transparency.OPAQUE, DataBuffer.TYPE_BYTE);
    BufferedImage image = new BufferedImage(cm, raster, true, null);

    try {
      ImageIO.write(image, "png", new File("image.png"));
    } catch (final IOException e) {
      System.out.println("Cannot write image: " + e.getMessage());
    }
  }
}
