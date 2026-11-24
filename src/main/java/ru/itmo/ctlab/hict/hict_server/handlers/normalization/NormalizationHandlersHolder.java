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

import ru.itmo.ctlab.hict.hict_server.jobs.BackgroundJob;

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
import ru.itmo.ctlab.hict.hict_library.chunkedfile.SourceMetadata;
import ru.itmo.ctlab.hict.hict_server.HandlersHolder;
import ru.itmo.ctlab.hict.hict_server.concurrent.RequestTaskScheduler;
import ru.itmo.ctlab.hict.hict_server.dto.RequestJson;
import ru.itmo.ctlab.hict.hict_server.handlers.tiles.TileHandlersHolder;
import ru.itmo.ctlab.hict.hict_server.handlers.util.TileStatisticHolder;
import ru.itmo.ctlab.hict.hict_server.normalization.CoverageNormalizationService;
import ru.itmo.ctlab.hict.hict_server.util.shareable.ShareableWrappers;

import java.util.Locale;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

/**
 * Normalization vectors computed from the matrix itself (square-root coverage).
 *
 * <p>{@code /normalization/coverage/prepare|status|cancel} manage the background job, and
 * {@code /normalization/weights} switches which weights tiles use once the vectors exist.
 */
public class NormalizationHandlersHolder extends HandlersHolder {
  private enum Source {
    PRIMARY("chunkedFile", "Primary"),
    SECONDARY("chunkedFileSecondary", "Secondary");

    final String key;
    final String label;

    Source(String key, String label) {
      this.key = key;
      this.label = label;
    }

    static Source requested(JsonObject body) {
      final var value = RequestJson.optionalString(body, "source", "PRIMARY");
      try {
        return valueOf(value.trim().toUpperCase(Locale.ROOT));
      } catch (IllegalArgumentException invalid) {
        throw new HttpException(400, "source must be PRIMARY or SECONDARY");
      }
    }
  }

  private final Vertx vertx;
  // Prepare one file at a time to keep temporary coverage arrays within the configured budget.
  private final ExecutorService executor = Executors.newSingleThreadExecutor(runnable -> {
    final var thread = new Thread(runnable, "hict-coverage-normalization");
    thread.setDaemon(true);
    return thread;
  });
  private final CoverageNormalizationService primary = new CoverageNormalizationService(executor, "Primary");
  private final CoverageNormalizationService secondary = new CoverageNormalizationService(executor, "Secondary");

  public NormalizationHandlersHolder(final Vertx vertx) {
    this.vertx = vertx;
  }

  public List<BackgroundJob> backgroundJobs() {
    return Stream.concat(primary.backgroundJobs().stream(), secondary.backgroundJobs().stream()).toList();
  }

  public boolean cancelBackgroundJob(String id) {
    return primary.cancelBackgroundJob(id) || secondary.cancelBackgroundJob(id);
  }

  public void cancelForMapChange() {
    primary.invalidateForMapChange();
    cancelForSecondaryChange();
  }

  public void cancelForSecondaryChange() {
    secondary.invalidateForMapChange();
  }

  public void close() {
    cancelForMapChange();
    executor.shutdownNow();
  }

  @Override
  public void addHandlersToRouter(final @NotNull Router router) {
    router.post("/normalization/coverage/prepare").handler(ctx -> {
      final var source = Source.requested(requestBody(ctx));
      final LocalMap<String, Object> map = vertx.sharedData().getLocalMap("hict_server");
      synchronized (map) {
        final var file = requireFile(map, source);
        if (lacksContactData(file)) {
          throw new HttpException(409, source.label + " needs contact data for square-root coverage");
        }
        final var service = service(source);
        try {
          final var status = service.start(file, CoverageWeightsBuilder.DEFAULT_BUDGET_BYTES);
          ctx.json(describe(status, source, file));
        } catch (CoverageNormalizationService.AlreadyRunningException conflict) {
          throw new HttpException(409, conflict.getMessage());
        }
      }
    });
    router.post("/normalization/coverage/status").handler(ctx -> {
      final var source = Source.requested(requestBody(ctx));
      final LocalMap<String, Object> map = vertx.sharedData().getLocalMap("hict_server");
      synchronized (map) {
        ctx.json(describe(service(source).status(), source, openFile(map, source)));
      }
    });
    router.post("/normalization/coverage/cancel").handler(ctx -> {
      final var source = Source.requested(requestBody(ctx));
      final LocalMap<String, Object> map = vertx.sharedData().getLocalMap("hict_server");
      synchronized (map) {
        ctx.json(describe(service(source).cancel(), source, openFile(map, source)));
      }
    });
    router.post("/normalization/weights").handler(ctx -> {
      final var scheduler = scheduler(ctx);
      if (scheduler == null) return;
      final var body = requestBody(ctx);
      final var source = Source.requested(body);
      final NormalizationWeights.Mode mode;
      try {
        mode = NormalizationWeights.Mode.valueOf(RequestJson.requireString(body, "mode").trim().toUpperCase(Locale.ROOT));
      } catch (IllegalArgumentException invalid) {
        throw new HttpException(400, "mode must be FILE or SQRT_COVERAGE");
      }
      scheduler.submit(
        ctx,
        RequestTaskScheduler.RequestPriority.ASSEMBLY,
        null,
        () -> {
          final LocalMap<String, Object> map = vertx.sharedData().getLocalMap("hict_server");
          synchronized (map) {
            final var file = requireFile(map, source);
            if (mode == NormalizationWeights.Mode.SQRT_COVERAGE
              && file.getNormalizationWeights().prepared() == null) {
              throw new HttpException(409, "Prepare square-root coverage for " + source.label + " first.");
            }
            file.getNormalizationWeights().select(mode);
            return Map.of("source", source.name(), "mode", mode.name(),
              "version", bumpTileVersion(requireFile(map, Source.PRIMARY), scheduler));
          }
        },
        result -> ctx.response().putHeader("content-type", "application/json").end(Json.encode(result))
      );
    });
  }

  private static JsonObject requestBody(io.vertx.ext.web.RoutingContext ctx) {
    return ctx.body().length() == 0 ? new JsonObject() : RequestJson.requireObject(ctx);
  }

  private CoverageNormalizationService service(Source source) {
    return source == Source.PRIMARY ? primary : secondary;
  }

  private static JsonObject describe(CoverageNormalizationService.Status status, Source source, ChunkedFile file) {
    final var weights = file == null ? null : file.getNormalizationWeights();
    final boolean prepared = weights != null && weights.prepared() != null;
    final var mode = weights == null ? NormalizationWeights.Mode.FILE : weights.mode();
    final var effectiveState = prepared && status.state() != CoverageNormalizationService.State.RUNNING
      ? CoverageNormalizationService.State.READY : status.state();
    return new JsonObject()
      .put("source", source.name())
      .put("jobId", status.jobId())
      .put("state", effectiveState.name())
      .put("progress", prepared ? 1.0d : status.progress())
      .put("message", status.message())
      .put("algorithm", status.algorithmVersion())
      .put("error", status.error())
      .put("prepared", prepared)
      .put("mode", mode.name())
      .put("estimatedBytes", file == null || lacksContactData(file) ? 0L : CoverageWeightsBuilder.estimateBytes(file));
  }

  private static ChunkedFile openFile(LocalMap<String, Object> map, Source source) {
    final var wrapper = (ShareableWrappers.ChunkedFileWrapper) map.get(source.key);
    return wrapper == null ? null : wrapper.getChunkedFile();
  }

  private static ChunkedFile requireFile(LocalMap<String, Object> map, Source source) {
    final var file = openFile(map, source);
    if (file == null) {
      throw new HttpException(409, source == Source.PRIMARY ? "Open a map first" : "Attach a secondary source first");
    }
    return file;
  }

  private static boolean lacksContactData(ChunkedFile file) {
    return file.getSourceMetadata().contactDataState() == SourceMetadata.ContactDataState.ABSENT;
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
