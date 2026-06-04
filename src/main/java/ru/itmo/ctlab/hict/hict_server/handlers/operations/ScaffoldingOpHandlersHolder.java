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
import ru.itmo.ctlab.hict.hict_server.handlers.fileop.FileOpHandlersHolder;
import ru.itmo.ctlab.hict.hict_server.handlers.tiles.TileHandlersHolder;
import ru.itmo.ctlab.hict.hict_server.handlers.util.TileStatisticHolder;
import ru.itmo.ctlab.hict.hict_server.util.shareable.ShareableWrappers;

import java.util.ArrayList;
import java.util.List;

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
          final var chunkedFiles = extractAssemblyChunkedFiles(map);
          final var chunkedFile = chunkedFiles.activeChunkedFile();
          log.debug("Got ChunkedFile from map");

          for (final var file : chunkedFiles.operationOrder()) {
            file.scaffoldingOperations().reverseSelectionRangeBp(request.startBP(), request.endBP());
          }
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
          final var chunkedFiles = extractAssemblyChunkedFiles(map);
          final var chunkedFile = chunkedFiles.activeChunkedFile();
          log.debug("Got ChunkedFile from map");

          for (final var file : chunkedFiles.operationOrder()) {
            file.scaffoldingOperations().moveSelectionRangeBp(request.startBP(), request.endBP(), request.targetStartBP());
          }
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
          final var chunkedFiles = extractAssemblyChunkedFiles(map);
          final var chunkedFile = chunkedFiles.activeChunkedFile();
          log.debug("Got ChunkedFile from map");

          final var splitPositionBp = resolveSplitPositionBp(chunkedFiles, request.splitPx(), request.bpResolution());
          for (final var file : chunkedFiles.operationOrder()) {
            file.scaffoldingOperations().splitContigAtBin(splitPositionBp, ResolutionDescriptor.fromResolutionOrder(0), QueryLengthUnit.BASE_PAIRS);
          }
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
          final var chunkedFiles = extractAssemblyChunkedFiles(map);
          final var chunkedFile = chunkedFiles.activeChunkedFile();
          log.debug("Got ChunkedFile from map");

          for (final var file : chunkedFiles.operationOrder()) {
            file.scaffoldingOperations().scaffoldRegion(request.startBP(), request.endBP(), ResolutionDescriptor.fromResolutionOrder(0), QueryLengthUnit.BASE_PAIRS, null);
          }
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
          final var chunkedFiles = extractAssemblyChunkedFiles(map);
          final var chunkedFile = chunkedFiles.activeChunkedFile();
          log.debug("Got ChunkedFile from map");

          for (final var file : chunkedFiles.operationOrder()) {
            file.scaffoldingOperations().unscaffoldRegion(request.startBP(), request.endBP(), ResolutionDescriptor.fromResolutionOrder(0), QueryLengthUnit.BASE_PAIRS);
          }
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
          final var chunkedFiles = extractAssemblyChunkedFiles(map);
          final var chunkedFile = chunkedFiles.activeChunkedFile();
          log.debug("Got ChunkedFile from map");

          for (final var file : chunkedFiles.operationOrder()) {
            file.scaffoldingOperations().moveRegionToDebris(request.startBP(), request.endBP(), ResolutionDescriptor.fromResolutionOrder(0), QueryLengthUnit.BASE_PAIRS);
          }
          final var newVersion = incrementVersionAndResetTileStats(map, chunkedFile, scheduler);
          return new AssemblyInfoWithVersionDTO(AssemblyInfoDTO.generateFromChunkedFile(chunkedFile), newVersion);
        },
        dto -> ctx.response().end(Json.encode(dto))
      );
    });
  }

  private AssemblyChunkedFiles extractAssemblyChunkedFiles(final @NotNull LocalMap<String, Object> map) {
    final var activeSource = String.valueOf(map.getOrDefault("assemblyInfoSource", "PRIMARY"));
    final var primaryWrapper = (ShareableWrappers.ChunkedFileWrapper) map.get("chunkedFile");
    final var secondaryWrapper = (ShareableWrappers.ChunkedFileWrapper) map.get("chunkedFileSecondary");

    final ChunkedFile activeChunkedFile;
    final var operationOrder = new ArrayList<ChunkedFile>();
    if ("SECONDARY".equalsIgnoreCase(activeSource)) {
      if (secondaryWrapper == null) {
        throw new IllegalStateException("Secondary source is not attached");
      }
      activeChunkedFile = secondaryWrapper.getChunkedFile();
      if (primaryWrapper != null) {
        operationOrder.add(primaryWrapper.getChunkedFile());
      }
      operationOrder.add(activeChunkedFile);
    } else {
      if (primaryWrapper == null) {
        throw new IllegalStateException("Open a Hi-C file before using scaffolding operations");
      }
      activeChunkedFile = primaryWrapper.getChunkedFile();
      if (secondaryWrapper != null) {
        operationOrder.add(secondaryWrapper.getChunkedFile());
      }
      operationOrder.add(activeChunkedFile);
    }
    return new AssemblyChunkedFiles(activeChunkedFile, List.copyOf(operationOrder));
  }

  private long resolveSplitPositionBp(final @NotNull AssemblyChunkedFiles chunkedFiles,
                                      final long splitPx,
                                      final long bpResolution) {
    for (final var file : chunkedFiles.operationOrder()) {
      for (final var resolution : file.getResolutions()) {
        if (resolution == bpResolution) {
          return file.convertUnits(
            splitPx,
            ResolutionDescriptor.fromBpResolution(bpResolution, file),
            QueryLengthUnit.PIXELS,
            ResolutionDescriptor.fromResolutionOrder(0),
            QueryLengthUnit.BASE_PAIRS
          );
        }
      }
    }
    return Math.max(0L, splitPx * bpResolution);
  }

  private long incrementVersionAndResetTileStats(final @NotNull LocalMap<String, Object> map,
                                                 final @NotNull ChunkedFile chunkedFile,
                                                 final @NotNull RequestTaskScheduler scheduler) {
    final var stats = (TileStatisticHolder) map.get("TileStatisticHolder");
    if (stats == null) {
      throw new RuntimeException("Tile statistics is not present in the local map, maybe the file is not yet opened?");
    }
    FileOpHandlersHolder.synchronizeOverlayAssemblyForSharedState(map);
    final var newStats = TileStatisticHolder.resetRangesWithIncrementedVersion(stats, maxOpenSourceResolutionCount(map, chunkedFile));
    map.put("TileStatisticHolder", newStats);
    TileHandlersHolder.clearExpectedProfileCache(map);
    scheduler.bumpAssemblyGeneration();
    final var trackManagerWrapper = (ShareableWrappers.Track1DManagerWrapper) map.get("Track1DManager");
    if (trackManagerWrapper != null) {
      trackManagerWrapper.getTrack1DManager().invalidateInMemoryCache();
    }
    return newStats.versionCounter().get();
  }

  private int maxOpenSourceResolutionCount(final @NotNull LocalMap<String, Object> map,
                                           final @NotNull ChunkedFile fallbackChunkedFile) {
    var resolutionCount = fallbackChunkedFile.getResolutions().length;
    final var primaryWrapper = (ShareableWrappers.ChunkedFileWrapper) map.get("chunkedFile");
    if (primaryWrapper != null) {
      resolutionCount = Math.max(resolutionCount, primaryWrapper.getChunkedFile().getResolutions().length);
    }
    final var secondaryWrapper = (ShareableWrappers.ChunkedFileWrapper) map.get("chunkedFileSecondary");
    if (secondaryWrapper != null) {
      resolutionCount = Math.max(resolutionCount, secondaryWrapper.getChunkedFile().getResolutions().length);
    }
    return resolutionCount;
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

  private record AssemblyChunkedFiles(@NotNull ChunkedFile activeChunkedFile,
                                      @NotNull List<ChunkedFile> operationOrder) {
  }
}
