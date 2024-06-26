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

package ru.itmo.ctlab.hict.hict_server.handlers.fileop;

import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;
import io.vertx.ext.web.Router;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_server.HandlersHolder;
import ru.itmo.ctlab.hict.hict_server.dto.response.assembly.AssemblyInfoDTO;
import ru.itmo.ctlab.hict.hict_server.dto.response.fileop.OpenFileResponseDTO;
import ru.itmo.ctlab.hict.hict_server.handlers.util.TileStatisticHolder;
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

  @Override
  public void addHandlersToRouter(final @NotNull Router router) {
    router.post("/open").blockingHandler(ctx -> {
      final var dataDirectoryWrapper = (ShareableWrappers.PathWrapper) vertx.sharedData().getLocalMap("hict_server").get("dataDirectory");
      if (dataDirectoryWrapper == null) {
        ctx.fail(new RuntimeException("Data directory is not present in local map"));
        return;
      }
      final var dataDirectory = dataDirectoryWrapper.getPath();

      final @NotNull var requestBody = ctx.body();
      final @NotNull var requestJSON = requestBody.asJsonObject();

      final @Nullable var filename = requestJSON.getString("filename");
      final @Nullable var fastaFilename = requestJSON.getString("fastaFilename");

      log.debug("Got filename: " + filename + " and FASTA filename: " + fastaFilename);

      if (filename == null) {
        ctx.fail(new RuntimeException("Filename must be specified to open the file"));
        return;
      }

      final var map = vertx.sharedData().getLocalMap("hict_server");

      final var chunkedFile = new ChunkedFile(
        new ChunkedFile.ChunkedFileOptions(
          Path.of(dataDirectory.toString(), filename),
          (int) map.getOrDefault("MIN_DS_POOL", 4),
          (int) map.getOrDefault("MAX_DS_POOL", 16)
        )
      );
      final var chunkedFileWrapper = new ShareableWrappers.ChunkedFileWrapper(chunkedFile);

      log.info("Putting chunkedFile into the local map");
      map.put("chunkedFile", chunkedFileWrapper);

      map.put("TileStatisticHolder", TileStatisticHolder.newDefaultStatisticHolder(chunkedFile.getResolutions().length));

      ctx.response().end(Json.encode(generateOpenFileResponse(chunkedFile)));
    });

    router.post("/get_agp_for_assembly").blockingHandler(ctx -> {
      final var map = vertx.sharedData().getLocalMap("hict_server");
      log.debug("Got map");
      final var chunkedFileWrapper = ((ShareableWrappers.ChunkedFileWrapper) (map.get("chunkedFile")));
      if (chunkedFileWrapper == null) {
        ctx.fail(new RuntimeException("Chunked file is not present in the local map, maybe the file is not yet opened?"));
        return;
      }
      final var chunkedFile = chunkedFileWrapper.getChunkedFile();
      log.debug("Got ChunkedFile from map");

      final @NotNull var requestBody = ctx.body();
      final @NotNull var requestJSON = requestBody.asJsonObject();

      final long defaultSpacerLength = requestJSON.getLong("defaultSpacerLength", 1000L);

      final var buffer = Buffer.buffer();

      chunkedFile.getAgpProcessor().getAGPStream(defaultSpacerLength).sequential().forEach(s -> buffer.appendBytes(s.getBytes(StandardCharsets.UTF_8)));

      ctx.response().setChunked(true).putHeader("Content-Type", "text/plain").end(buffer);
    });

    router.post("/load_agp").blockingHandler(ctx -> {
      final var map = vertx.sharedData().getLocalMap("hict_server");
      log.debug("Got map");
      final var chunkedFileWrapper = ((ShareableWrappers.ChunkedFileWrapper) (map.get("chunkedFile")));
      if (chunkedFileWrapper == null) {
        ctx.fail(new RuntimeException("Chunked file is not present in the local map, maybe the file is not yet opened?"));
        return;
      }
      final var chunkedFile = chunkedFileWrapper.getChunkedFile();
      log.debug("Got ChunkedFile from map");

      final @NotNull var requestBody = ctx.body();
      final @NotNull var requestJSON = requestBody.asJsonObject();

      final var agpFilename = Objects.requireNonNull(requestJSON.getString("agpFilename"), "AGP filename must be provided to load it.");

      final var dataDirectoryWrapper = (ShareableWrappers.PathWrapper) vertx.sharedData().getLocalMap("hict_server").get("dataDirectory");
      if (dataDirectoryWrapper == null) {
        ctx.fail(new RuntimeException("Data directory is not present in local map"));
        return;
      }
      final var dataDirectory = dataDirectoryWrapper.getPath();

      final var agpFile = Path.of(dataDirectory.toString(), agpFilename);
      try (final var reader = Files.newBufferedReader(agpFile, StandardCharsets.UTF_8)) {
        chunkedFile.importAGP(reader);
      } catch (IOException | NoSuchFieldException e) {
        throw new RuntimeException(e);
      }

      ctx.response().end(Json.encode(AssemblyInfoDTO.generateFromChunkedFile(chunkedFile)));
    });
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
