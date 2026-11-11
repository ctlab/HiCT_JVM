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

package ru.itmo.ctlab.hict.hict_server.handlers.normalization;

import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.LocalMap;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.HttpException;
import org.jetbrains.annotations.NotNull;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.CoverageWeightsBuilder;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.NormalizationWeights;
import ru.itmo.ctlab.hict.hict_server.HandlersHolder;
import ru.itmo.ctlab.hict.hict_server.concurrent.RequestTaskScheduler;
import ru.itmo.ctlab.hict.hict_server.handlers.tiles.TileHandlersHolder;
import ru.itmo.ctlab.hict.hict_server.handlers.util.TileStatisticHolder;
import ru.itmo.ctlab.hict.hict_server.normalization.CoverageNormalizationService;
import ru.itmo.ctlab.hict.hict_server.util.shareable.ShareableWrappers;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Normalization vectors computed from the matrix itself (square-root coverage).
 *
 * <p>{@code /normalization/coverage/prepare|status|cancel} manage the background job, and
 * {@code /normalization/weights} switches which weights tiles use once the vectors exist.
 */
public class NormalizationHandlersHolder extends HandlersHolder {
  private final Vertx vertx;
  private final CoverageNormalizationService service = new CoverageNormalizationService();

  public NormalizationHandlersHolder(final Vertx vertx) {
    this.vertx = vertx;
  }

  public void cancelForMapChange() {
    service.invalidateForMapChange();
  }

  public void close() {
    service.close();
  }

  @Override
  public void addHandlersToRouter(final @NotNull Router router) {
    router.post("/normalization/coverage/prepare").handler(ctx -> {
      final var map = vertx.sharedData().getLocalMap("hict_server");
      synchronized (map) {
        final var files = openFiles();
        if (files.isEmpty()) {
          ctx.fail(new HttpException(409, "Open a map first"));
          return;
        }
        if (files.stream().anyMatch(NormalizationHandlersHolder::lacksContactData)) {
          ctx.fail(new HttpException(409, "Square-root coverage needs contact data in every open source"));
          return;
        }
        try {
          final var status = service.start(files, CoverageWeightsBuilder.DEFAULT_BUDGET_BYTES);
          ctx.response().putHeader("content-type", "application/json").end(Json.encode(describe(status, files)));
        } catch (CoverageNormalizationService.AlreadyRunningException conflict) {
          ctx.fail(new HttpException(409, conflict.getMessage()));
        }
      }
    });
    router.post("/normalization/coverage/status").handler(ctx -> {
      final var files = openFiles();
      ctx.response().putHeader("content-type", "application/json").end(Json.encode(describe(service.status(), files)));
    });
    router.post("/normalization/coverage/cancel").handler(ctx -> {
      final var files = openFiles();
      ctx.response().putHeader("content-type", "application/json").end(Json.encode(describe(service.cancel(), files)));
    });
    router.post("/normalization/weights").handler(ctx -> {
      final var scheduler = scheduler(ctx);
      if (scheduler == null) {
        return;
      }
      final var body = ctx.body().asJsonObject();
      final var requested = body == null ? null : body.getString("mode");
      final NormalizationWeights.Mode mode;
      try {
        mode = NormalizationWeights.Mode.valueOf(String.valueOf(requested).trim().toUpperCase());
      } catch (final IllegalArgumentException e) {
        ctx.fail(new HttpException(400, "mode must be FILE or SQRT_COVERAGE"));
        return;
      }
      scheduler.submit(
        ctx,
        RequestTaskScheduler.RequestPriority.ASSEMBLY,
        null,
        () -> {
          final var files = openFiles();
          if (files.isEmpty()) {
            throw new HttpException(409, "Open a map first");
          }
          if (mode == NormalizationWeights.Mode.SQRT_COVERAGE
            && files.stream().anyMatch(f -> f.getNormalizationWeights().prepared() == null)) {
            throw new HttpException(409, "Square-root coverage has not been prepared for every open source. Prepare it first.");
          }
          // Switch every source together, so the two triangles never use different weights.
          files.forEach(f -> f.getNormalizationWeights().select(mode));
          return Map.of("mode", mode.name(), "version", bumpTileVersion(files.get(0), scheduler));
        },
        result -> ctx.response().putHeader("content-type", "application/json").end(Json.encode(result))
      );
    });
  }

  private @NotNull Map<String, Object> describe(final @NotNull CoverageNormalizationService.Status status,
                                                final @NotNull List<ChunkedFile> files) {
    final boolean prepared = !files.isEmpty() && files.stream().allMatch(f -> f.getNormalizationWeights().prepared() != null);
    final var mode = files.isEmpty() ? NormalizationWeights.Mode.FILE : files.get(0).getNormalizationWeights().mode();
    final var effectiveState = prepared && status.state() != CoverageNormalizationService.State.RUNNING
      ? CoverageNormalizationService.State.READY : status.state();
    return new JsonObject()
      .put("state", effectiveState.name())
      .put("progress", prepared ? 1.0d : status.progress())
      .put("message", status.message())
      .put("algorithm", status.algorithmVersion())
      .put("error", status.error())
      .put("prepared", prepared)
      .put("mode", mode.name())
      .put("consistent", files.stream().allMatch(f -> f.getNormalizationWeights().mode() == mode))
      .put("estimatedBytes", files.stream().mapToLong(CoverageWeightsBuilder::estimateBytes).sum())
      .getMap();
  }

  private @NotNull List<ChunkedFile> openFiles() {
    final @NotNull LocalMap<String, Object> map = vertx.sharedData().getLocalMap("hict_server");
    final var files = new ArrayList<ChunkedFile>(2);
    final var primary = (ShareableWrappers.ChunkedFileWrapper) map.get("chunkedFile");
    if (primary != null) {
      files.add(primary.getChunkedFile());
    }
    final var secondary = (ShareableWrappers.ChunkedFileWrapper) map.get("chunkedFileSecondary");
    if (primary != null && secondary != null) {
      files.add(secondary.getChunkedFile());
    }
    return files;
  }

  private static boolean lacksContactData(ChunkedFile file) {
    return file.getSourceMetadata().contactDataState()
      == ru.itmo.ctlab.hict.hict_library.chunkedfile.SourceMetadata.ContactDataState.ABSENT;
  }

  private long bumpTileVersion(final @NotNull ChunkedFile chunkedFile, final @NotNull RequestTaskScheduler scheduler) {
    final @NotNull LocalMap<String, Object> map = vertx.sharedData().getLocalMap("hict_server");
    final var stats = (TileStatisticHolder) map.get("TileStatisticHolder");
    if (stats == null) {
      throw new IllegalStateException("Tile statistics are not available. Open a map first.");
    }
    final var next = TileStatisticHolder.resetRangesWithIncrementedVersion(stats, chunkedFile.getResolutions().length);
    map.put("TileStatisticHolder", next);
    TileHandlersHolder.clearExpectedProfileCache(map);
    scheduler.bumpGeneration(RequestTaskScheduler.CancellationDomain.TILE);
    return next.versionCounter().get();
  }

  private RequestTaskScheduler scheduler(final @NotNull io.vertx.ext.web.RoutingContext ctx) {
    final @NotNull LocalMap<String, Object> map = vertx.sharedData().getLocalMap("hict_server");
    final var wrapper = (ShareableWrappers.RequestTaskSchedulerWrapper) map.get(RequestTaskScheduler.LOCAL_MAP_KEY);
    if (wrapper == null) {
      ctx.fail(new IllegalStateException("Request scheduler is not initialized"));
      return null;
    }
    return wrapper.getRequestTaskScheduler();
  }
}
