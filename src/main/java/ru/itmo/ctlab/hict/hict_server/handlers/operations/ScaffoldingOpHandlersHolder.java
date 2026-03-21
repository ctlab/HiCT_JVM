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

package ru.itmo.ctlab.hict.hict_server.handlers.operations;

import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.shareddata.LocalMap;
import io.vertx.ext.web.Router;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.resolution.ResolutionDescriptor;
import ru.itmo.ctlab.hict.hict_library.domain.QueryLengthUnit;
import ru.itmo.ctlab.hict.hict_server.HandlersHolder;
import ru.itmo.ctlab.hict.hict_server.concurrent.RequestTaskScheduler;
import ru.itmo.ctlab.hict.hict_server.dto.request.scaffolding.*;
import ru.itmo.ctlab.hict.hict_server.dto.response.assembly.AssemblyInfoDTO;
import ru.itmo.ctlab.hict.hict_server.dto.response.assembly.AssemblyInfoWithVersionDTO;
import ru.itmo.ctlab.hict.hict_server.handlers.util.TileStatisticHolder;
import ru.itmo.ctlab.hict.hict_server.util.shareable.ShareableWrappers;

@RequiredArgsConstructor
@Slf4j
public class ScaffoldingOpHandlersHolder extends HandlersHolder {
  final Vertx vertx;

  @Override
  public void addHandlersToRouter(final @NotNull Router router) {
    router.post("/reverse_selection_range").handler(ctx -> {
      final var scheduler = getScheduler(ctx);
      if (scheduler == null) {
        return;
      }
      final @NotNull var requestBody = ctx.body();
      final @NotNull var requestJSON = requestBody.asJsonObject();

      final @NotNull @NonNull var request = ReverseSelectionRangeRequestDTO.fromJSONObject(requestJSON);

      scheduler.submit(
        ctx,
        RequestTaskScheduler.RequestPriority.ASSEMBLY,
        null,
        () -> {
          final @NotNull @NonNull LocalMap<String, Object> map = vertx.sharedData().getLocalMap("hict_server");
          log.debug("Got map");
          final var chunkedFile = extractChunkedFile(map);
          log.debug("Got ChunkedFile from map");

          chunkedFile.scaffoldingOperations().reverseSelectionRangeBp(request.startBP(), request.endBP());
          final var newVersion = incrementVersionAndResetTileStats(map, chunkedFile, scheduler);
          return new AssemblyInfoWithVersionDTO(AssemblyInfoDTO.generateFromChunkedFile(chunkedFile), newVersion);
        },
        dto -> ctx.response().end(Json.encode(dto))
      );
    });
    router.post("/move_selection_range").handler(ctx -> {
      final var scheduler = getScheduler(ctx);
      if (scheduler == null) {
        return;
      }
      final @NotNull var requestBody = ctx.body();
      final @NotNull var requestJSON = requestBody.asJsonObject();

      final @NotNull @NonNull var request = MoveSelectionRangeRequestDTO.fromJSONObject(requestJSON);

      scheduler.submit(
        ctx,
        RequestTaskScheduler.RequestPriority.ASSEMBLY,
        null,
        () -> {
          final @NotNull @NonNull LocalMap<String, Object> map = vertx.sharedData().getLocalMap("hict_server");
          log.debug("Got map");
          final var chunkedFile = extractChunkedFile(map);
          log.debug("Got ChunkedFile from map");

          chunkedFile.scaffoldingOperations().moveSelectionRangeBp(request.startBP(), request.endBP(), request.targetStartBP());
          final var newVersion = incrementVersionAndResetTileStats(map, chunkedFile, scheduler);
          return new AssemblyInfoWithVersionDTO(AssemblyInfoDTO.generateFromChunkedFile(chunkedFile), newVersion);
        },
        dto -> ctx.response().end(Json.encode(dto))
      );
    });
    router.post("/split_contig_at_bin").handler(ctx -> {
      final var scheduler = getScheduler(ctx);
      if (scheduler == null) {
        return;
      }
      final @NotNull var requestBody = ctx.body();
      final @NotNull var requestJSON = requestBody.asJsonObject();

      final @NotNull @NonNull var request = SplitContigRequestDTO.fromJSONObject(requestJSON);

      scheduler.submit(
        ctx,
        RequestTaskScheduler.RequestPriority.ASSEMBLY,
        null,
        () -> {
          final @NotNull @NonNull LocalMap<String, Object> map = vertx.sharedData().getLocalMap("hict_server");
          log.debug("Got map");
          final var chunkedFile = extractChunkedFile(map);
          log.debug("Got ChunkedFile from map");

          chunkedFile.scaffoldingOperations().splitContigAtBin(request.splitPx(), ResolutionDescriptor.fromBpResolution(request.bpResolution(), chunkedFile), QueryLengthUnit.PIXELS);
          final var newVersion = incrementVersionAndResetTileStats(map, chunkedFile, scheduler);
          return new AssemblyInfoWithVersionDTO(AssemblyInfoDTO.generateFromChunkedFile(chunkedFile), newVersion);
        },
        dto -> ctx.response().end(Json.encode(dto))
      );
    });
    router.post("/group_contigs_into_scaffold").handler(ctx -> {
      final var scheduler = getScheduler(ctx);
      if (scheduler == null) {
        return;
      }
      final @NotNull var requestBody = ctx.body();
      final @NotNull var requestJSON = requestBody.asJsonObject();

      final @NotNull @NonNull var request = ScaffoldRegionRequestDTO.fromJSONObject(requestJSON);

      scheduler.submit(
        ctx,
        RequestTaskScheduler.RequestPriority.ASSEMBLY,
        null,
        () -> {
          final @NotNull @NonNull LocalMap<String, Object> map = vertx.sharedData().getLocalMap("hict_server");
          log.debug("Got map");
          final var chunkedFile = extractChunkedFile(map);
          log.debug("Got ChunkedFile from map");

          chunkedFile.scaffoldingOperations().scaffoldRegion(request.startBP(), request.endBP(), ResolutionDescriptor.fromResolutionOrder(0), QueryLengthUnit.BASE_PAIRS, null);
          final var newVersion = incrementVersionAndResetTileStats(map, chunkedFile, scheduler);
          return new AssemblyInfoWithVersionDTO(AssemblyInfoDTO.generateFromChunkedFile(chunkedFile), newVersion);
        },
        dto -> ctx.response().end(Json.encode(dto))
      );
    });
    router.post("/ungroup_contigs_from_scaffold").handler(ctx -> {
      final var scheduler = getScheduler(ctx);
      if (scheduler == null) {
        return;
      }
      final @NotNull var requestBody = ctx.body();
      final @NotNull var requestJSON = requestBody.asJsonObject();

      final @NotNull @NonNull var request = UnscaffoldRegionRequestDTO.fromJSONObject(requestJSON);

      scheduler.submit(
        ctx,
        RequestTaskScheduler.RequestPriority.ASSEMBLY,
        null,
        () -> {
          final @NotNull @NonNull LocalMap<String, Object> map = vertx.sharedData().getLocalMap("hict_server");
          log.debug("Got map");
          final var chunkedFile = extractChunkedFile(map);
          log.debug("Got ChunkedFile from map");

          chunkedFile.scaffoldingOperations().unscaffoldRegion(request.startBP(), request.endBP(), ResolutionDescriptor.fromResolutionOrder(0), QueryLengthUnit.BASE_PAIRS);
          final var newVersion = incrementVersionAndResetTileStats(map, chunkedFile, scheduler);
          return new AssemblyInfoWithVersionDTO(AssemblyInfoDTO.generateFromChunkedFile(chunkedFile), newVersion);
        },
        dto -> ctx.response().end(Json.encode(dto))
      );
    });
    router.post("/move_selection_to_debris").handler(ctx -> {
      final var scheduler = getScheduler(ctx);
      if (scheduler == null) {
        return;
      }
      final @NotNull var requestBody = ctx.body();
      final @NotNull var requestJSON = requestBody.asJsonObject();

      final @NotNull @NonNull var request = MoveSelectionToDebrisRequestDTO.fromJSONObject(requestJSON);

      scheduler.submit(
        ctx,
        RequestTaskScheduler.RequestPriority.ASSEMBLY,
        null,
        () -> {
          final @NotNull @NonNull LocalMap<String, Object> map = vertx.sharedData().getLocalMap("hict_server");
          log.debug("Got map");
          final var chunkedFile = extractChunkedFile(map);
          log.debug("Got ChunkedFile from map");

          chunkedFile.scaffoldingOperations().moveRegionToDebris(request.startBP(), request.endBP(), ResolutionDescriptor.fromResolutionOrder(0), QueryLengthUnit.BASE_PAIRS);
          final var newVersion = incrementVersionAndResetTileStats(map, chunkedFile, scheduler);
          return new AssemblyInfoWithVersionDTO(AssemblyInfoDTO.generateFromChunkedFile(chunkedFile), newVersion);
        },
        dto -> ctx.response().end(Json.encode(dto))
      );
    });
  }

  private ChunkedFile extractChunkedFile(final @NotNull LocalMap<String, Object> map) {
    final var chunkedFileWrapper = ((ShareableWrappers.ChunkedFileWrapper) (map.get("chunkedFile")));
    if (chunkedFileWrapper == null) {
      throw new RuntimeException("Chunked file is not present in the local map, maybe the file is not yet opened?");
    }
    return chunkedFileWrapper.getChunkedFile();
  }

  private long incrementVersionAndResetTileStats(final @NotNull LocalMap<String, Object> map,
                                                 final @NotNull ChunkedFile chunkedFile,
                                                 final @NotNull RequestTaskScheduler scheduler) {
    final var stats = (TileStatisticHolder) map.get("TileStatisticHolder");
    if (stats == null) {
      throw new RuntimeException("Tile statistics is not present in the local map, maybe the file is not yet opened?");
    }
    final var newStats = TileStatisticHolder.resetRangesWithIncrementedVersion(stats, chunkedFile.getResolutions().length);
    map.put("TileStatisticHolder", newStats);
    scheduler.bumpAssemblyGeneration();
    return newStats.versionCounter().get();
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
}
