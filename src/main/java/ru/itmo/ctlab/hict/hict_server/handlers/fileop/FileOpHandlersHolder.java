package ru.itmo.ctlab.hict.hict_server.handlers.fileop;

import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.ext.web.Router;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_server.HandlersHolder;
import ru.itmo.ctlab.hict.hict_server.dto.OpenFileResponseDTO;
import ru.itmo.ctlab.hict.hict_server.util.shareable.ShareableWrappers;

import java.nio.file.Path;

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

      final var filename = ctx.request().getParam("filename", null);
      final var fastaFilename = ctx.request().getParam("fastaFilename", null);

      if (filename == null) {
        ctx.fail(new RuntimeException("Filename must be specified to open the file"));
        return;
      }

      final var chunkedFile = new ChunkedFile(
        Path.of(dataDirectory.toString(), filename),
        (int) vertx.sharedData().getLocalMap("hict_server").getOrDefault("tileSize", 256)
      );
      final var chunkedFileWrapper = new ShareableWrappers.ChunkedFileWrapper(chunkedFile);

      log.info("Putting chunkedFile into the local map");
      final var map = vertx.sharedData().getLocalMap("hict_server");
      map.put("chunkedFile", chunkedFileWrapper);

      ctx.response().end(Json.encode(generateOpenFileResponse(chunkedFile)));
    });
  }

  private @NotNull @NonNull OpenFileResponseDTO generateOpenFileResponse(final @NotNull @NonNull ChunkedFile chunkedFile) {
    final long minResolution = chunkedFile.getResolutionsList().stream().min(Long::compare).orElse(1L);
    return new OpenFileResponseDTO(
      "Opened",
      (String) vertx.sharedData().getLocalMap("hict_server").getOrDefault("transport_dtype", "uint8"),
      chunkedFile.getResolutionsList(),
      chunkedFile.getResolutionsList().parallelStream().mapToDouble(r -> (double) r / minResolution).boxed().toList(),
      chunkedFile.getDenseBlockSize(),
      null,
      null
    );
  }
}
