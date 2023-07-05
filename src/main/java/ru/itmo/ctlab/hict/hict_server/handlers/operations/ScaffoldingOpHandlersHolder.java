package ru.itmo.ctlab.hict.hict_server.handlers.operations;

import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.ext.web.Router;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import ru.itmo.ctlab.hict.hict_server.HandlersHolder;
import ru.itmo.ctlab.hict.hict_server.dto.request.scaffolding.MoveSelectionRangeRequestDTO;
import ru.itmo.ctlab.hict.hict_server.dto.request.scaffolding.ReverseSelectionRangeRequestDTO;
import ru.itmo.ctlab.hict.hict_server.dto.response.assembly.AssemblyInfoDTO;
import ru.itmo.ctlab.hict.hict_server.util.shareable.ShareableWrappers;

@RequiredArgsConstructor
@Slf4j
public class ScaffoldingOpHandlersHolder extends HandlersHolder {
  final Vertx vertx;

  @Override
  public void addHandlersToRouter(final @NotNull Router router) {
    router.post("/reverse_selection_range").blockingHandler(ctx -> {
      final @NotNull var requestBody = ctx.body();
      final @NotNull var requestJSON = requestBody.asJsonObject();

      final @NotNull @NonNull var request = ReverseSelectionRangeRequestDTO.fromJSONObject(requestJSON);

      final var map = vertx.sharedData().getLocalMap("hict_server");
      log.debug("Got map");
      final var chunkedFileWrapper = ((ShareableWrappers.ChunkedFileWrapper) (map.get("chunkedFile")));
      if (chunkedFileWrapper == null) {
        ctx.fail(new RuntimeException("Chunked file is not present in the local map, maybe the file is not yet opened?"));
        return;
      }
      final var chunkedFile = chunkedFileWrapper.getChunkedFile();
      log.debug("Got ChunkedFile from map");

      chunkedFile.scaffoldingOperations().reverseSelectionRangeBp(request.startBP(), request.endBP());

      ctx.response().end(Json.encode(AssemblyInfoDTO.generateFromChunkedFile(chunkedFile)));
    });
    router.post("/move_selection_range").blockingHandler(ctx -> {
      final @NotNull var requestBody = ctx.body();
      final @NotNull var requestJSON = requestBody.asJsonObject();

      final @NotNull @NonNull var request = MoveSelectionRangeRequestDTO.fromJSONObject(requestJSON);

      final var map = vertx.sharedData().getLocalMap("hict_server");
      log.debug("Got map");
      final var chunkedFileWrapper = ((ShareableWrappers.ChunkedFileWrapper) (map.get("chunkedFile")));
      if (chunkedFileWrapper == null) {
        ctx.fail(new RuntimeException("Chunked file is not present in the local map, maybe the file is not yet opened?"));
        return;
      }
      final var chunkedFile = chunkedFileWrapper.getChunkedFile();
      log.debug("Got ChunkedFile from map");

      chunkedFile.scaffoldingOperations().moveSelectionRangeBp(request.startBP(), request.endBP(), request.targetStartBP());

      ctx.response().end(Json.encode(AssemblyInfoDTO.generateFromChunkedFile(chunkedFile)));
    });
  }
}
