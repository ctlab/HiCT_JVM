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

package ru.itmo.ctlab.hict.hict_server.handlers.tracks;

import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.shareddata.LocalMap;
import io.vertx.ext.web.Router;
import lombok.NonNull;
import org.jetbrains.annotations.NotNull;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.domain.QueryLengthUnit;
import ru.itmo.ctlab.hict.hict_server.HandlersHolder;
import ru.itmo.ctlab.hict.hict_server.concurrent.RequestTaskScheduler;
import ru.itmo.ctlab.hict.hict_server.tracks.Track1DManager;
import ru.itmo.ctlab.hict.hict_server.util.shareable.ShareableWrappers;

import java.util.List;
import java.util.Map;

public class TrackHandlersHolder extends HandlersHolder {
  private final Vertx vertx;

  public TrackHandlersHolder(final Vertx vertx) {
    this.vertx = vertx;
  }

  @Override
  public void addHandlersToRouter(final @NotNull Router router) {
    router.post("/tracks/list_files").handler(ctx -> {
      final var scheduler = getScheduler(ctx);
      if (scheduler == null) {
        return;
      }
      final var manager = getTrackManager(ctx);
      if (manager == null) {
        return;
      }
      scheduler.submit(
        ctx,
        RequestTaskScheduler.RequestPriority.UI_UX,
        null,
        manager::listTrackFiles,
        files -> ctx.response().putHeader("content-type", "application/json").end(Json.encode(files))
      );
    });

    router.post("/tracks/open").handler(ctx -> {
      final var scheduler = getScheduler(ctx);
      if (scheduler == null) {
        return;
      }
      final var request = ctx.body().asJsonObject();
      final var filename = request.getString("filename");
      if (filename == null || filename.isBlank()) {
        ctx.fail(new IllegalArgumentException("Track filename is required"));
        return;
      }
      final var manager = getTrackManager(ctx);
      if (manager == null) {
        return;
      }
      final @NotNull @NonNull LocalMap<String, Object> map = this.vertx.sharedData().getLocalMap("hict_server");
      final var chunkedFile = extractChunkedFile(map, ctx);
      if (chunkedFile == null) {
        return;
      }
      scheduler.submit(
        ctx,
        RequestTaskScheduler.RequestPriority.ASSEMBLY,
        null,
        () -> {
          final var summary = manager.openTrack(
            filename,
            request.getString("name"),
            request.getString("color")
          );
          manager.startPrecompute(chunkedFile, summary.getTrackId(), false);
          return summary;
        },
        summary -> ctx.response()
          .putHeader("content-type", "application/json")
          .end(Json.encode(summary))
      );
    });

    router.post("/tracks/list").handler(ctx -> {
      final var scheduler = getScheduler(ctx);
      if (scheduler == null) {
        return;
      }
      final var manager = getTrackManager(ctx);
      if (manager == null) {
        return;
      }
      scheduler.submit(
        ctx,
        RequestTaskScheduler.RequestPriority.UI_UX,
        null,
        manager::listTracks,
        tracks -> ctx.response().putHeader("content-type", "application/json").end(Json.encode(tracks))
      );
    });

    router.post("/tracks/update").handler(ctx -> {
      final var scheduler = getScheduler(ctx);
      if (scheduler == null) {
        return;
      }
      final var request = ctx.body().asJsonObject();
      final var trackId = request.getString("trackId");
      if (trackId == null || trackId.isBlank()) {
        ctx.fail(new IllegalArgumentException("trackId is required"));
        return;
      }
      final var manager = getTrackManager(ctx);
      if (manager == null) {
        return;
      }
      scheduler.submit(
        ctx,
        RequestTaskScheduler.RequestPriority.ASSEMBLY,
        null,
        () -> manager.updateTrack(
          trackId,
          request.containsKey("visible") ? request.getBoolean("visible") : null,
          request.getString("color"),
          request.getString("name"),
          request.getString("renderMode"),
          request.getString("aggregationMode")
        ),
        updated -> ctx.response()
          .putHeader("content-type", "application/json")
          .end(Json.encode(updated))
      );
    });

    router.post("/tracks/remove").handler(ctx -> {
      final var scheduler = getScheduler(ctx);
      if (scheduler == null) {
        return;
      }
      final var request = ctx.body().asJsonObject();
      final var trackId = request.getString("trackId");
      if (trackId == null || trackId.isBlank()) {
        ctx.fail(new IllegalArgumentException("trackId is required"));
        return;
      }
      final var manager = getTrackManager(ctx);
      if (manager == null) {
        return;
      }
      scheduler.submit(
        ctx,
        RequestTaskScheduler.RequestPriority.ASSEMBLY,
        null,
        () -> {
          manager.removeTrack(trackId);
          return Map.of("status", "removed", "trackId", trackId);
        },
        response -> ctx.response()
          .putHeader("content-type", "application/json")
          .end(Json.encode(response))
      );
    });

    router.post("/tracks/precompute/status").handler(ctx -> {
      final var scheduler = getScheduler(ctx);
      if (scheduler == null) {
        return;
      }
      final var manager = getTrackManager(ctx);
      if (manager == null) {
        return;
      }
      scheduler.submit(
        ctx,
        RequestTaskScheduler.RequestPriority.UI_UX,
        null,
        manager::getPrecomputeStatus,
        status -> ctx.response()
          .putHeader("content-type", "application/json")
          .end(Json.encode(status))
      );
    });

    router.post("/tracks/precompute/start").handler(ctx -> {
      final var scheduler = getScheduler(ctx);
      if (scheduler == null) {
        return;
      }
      final var request = ctx.body().asJsonObject();
      final var manager = getTrackManager(ctx);
      if (manager == null) {
        return;
      }
      final @NotNull @NonNull LocalMap<String, Object> map = this.vertx.sharedData().getLocalMap("hict_server");
      final var chunkedFile = extractChunkedFile(map, ctx);
      if (chunkedFile == null) {
        return;
      }
      scheduler.submit(
        ctx,
        RequestTaskScheduler.RequestPriority.TRACK,
        RequestTaskScheduler.CancellationDomain.TRACK,
        () -> manager.startPrecompute(
          chunkedFile,
          request.getString("trackId"),
          request.getBoolean("force", false)
        ),
        status -> ctx.response()
          .putHeader("content-type", "application/json")
          .end(Json.encode(status)),
        () -> ctx.response()
          .putHeader("content-type", "application/json")
          .end(Json.encode(Map.of("status", "cancelled")))
      );
    });

    router.post("/tracks/query_1d").handler(ctx -> {
      final var scheduler = getScheduler(ctx);
      if (scheduler == null) {
        return;
      }
      final var request = ctx.body().asJsonObject();
      final var widthPx = request.getInteger("widthPx", 512);
      final var bpResolution = request.getLong("bpResolution", 1L);

      final @NotNull @NonNull LocalMap<String, Object> map = this.vertx.sharedData().getLocalMap("hict_server");
      final var manager = getTrackManager(ctx);
      if (manager == null) {
        return;
      }
      final var chunkedFile = extractChunkedFile(map, ctx);
      if (chunkedFile == null) {
        return;
      }
      final var resolvedUnits = resolveUnits(request);
      final var start = resolveStart(request, resolvedUnits);
      final var end = resolveEnd(request, resolvedUnits, start + 1L);

      scheduler.submit(
        ctx,
        RequestTaskScheduler.RequestPriority.TRACK,
        RequestTaskScheduler.CancellationDomain.TRACK,
        () -> manager.queryVisibleTracks(chunkedFile, start, end, widthPx, bpResolution, resolvedUnits),
        result -> ctx.response()
          .putHeader("content-type", "application/json")
          .end(Json.encode(result)),
        () -> ctx.response()
          .putHeader("content-type", "application/json")
          .end(Json.encode(new Track1DManager.QueryResult(
            0L,
            0L,
            start,
            Math.max(start + 1L, end),
            widthPx,
            bpResolution,
            List.of()
          )))
      );
    });
  }

  private static @NotNull QueryLengthUnit resolveUnits(final @NotNull io.vertx.core.json.JsonObject request) {
    final var declared = request.getString("unit", request.getString("units"));
    if (declared != null && !declared.isBlank()) {
      return parseUnits(declared);
    }
    if (request.containsKey("startPx") || request.containsKey("endPx")) {
      return QueryLengthUnit.PIXELS;
    }
    if (request.containsKey("startBin") || request.containsKey("endBin")) {
      return QueryLengthUnit.BINS;
    }
    if (request.containsKey("startBP") || request.containsKey("endBP")) {
      return QueryLengthUnit.BASE_PAIRS;
    }
    return QueryLengthUnit.PIXELS;
  }

  private static long resolveStart(final @NotNull io.vertx.core.json.JsonObject request,
                                   final @NotNull QueryLengthUnit units) {
    return switch (units) {
      case PIXELS -> request.getLong("startPx", request.getLong("start", 0L));
      case BINS -> request.getLong("startBin", request.getLong("start", 0L));
      case BASE_PAIRS -> request.getLong("startBP", request.getLong("start", 0L));
    };
  }

  private static long resolveEnd(final @NotNull io.vertx.core.json.JsonObject request,
                                 final @NotNull QueryLengthUnit units,
                                 final long fallback) {
    return switch (units) {
      case PIXELS -> request.getLong("endPx", request.getLong("end", fallback));
      case BINS -> request.getLong("endBin", request.getLong("end", fallback));
      case BASE_PAIRS -> request.getLong("endBP", request.getLong("end", fallback));
    };
  }

  private static @NotNull QueryLengthUnit parseUnits(final @NotNull String rawValue) {
    final var normalized = rawValue.trim().toUpperCase();
    return switch (normalized) {
      case "PIXEL", "PIXELS", "PX" -> QueryLengthUnit.PIXELS;
      case "BIN", "BINS" -> QueryLengthUnit.BINS;
      case "BP", "BASE_PAIRS", "BASEPAIR", "BASEPAIRS" -> QueryLengthUnit.BASE_PAIRS;
      default -> throw new IllegalArgumentException(
        "Unsupported query unit '" + rawValue + "'. Use one of: PIXELS, BINS, BP."
      );
    };
  }

  private Track1DManager getTrackManager(final @NotNull io.vertx.ext.web.RoutingContext ctx) {
    final @NotNull @NonNull LocalMap<String, Object> map = this.vertx.sharedData().getLocalMap("hict_server");
    final var managerWrapper = (ShareableWrappers.Track1DManagerWrapper) map.get("Track1DManager");
    if (managerWrapper == null) {
      ctx.fail(new IllegalStateException("Track manager is not initialized. Open a HiCT file first."));
      return null;
    }
    return managerWrapper.getTrack1DManager();
  }

  private RequestTaskScheduler getScheduler(final @NotNull io.vertx.ext.web.RoutingContext ctx) {
    final @NotNull @NonNull LocalMap<String, Object> map = this.vertx.sharedData().getLocalMap("hict_server");
    final var wrapper = (ShareableWrappers.RequestTaskSchedulerWrapper) map.get(RequestTaskScheduler.LOCAL_MAP_KEY);
    if (wrapper == null) {
      ctx.fail(new IllegalStateException("Request scheduler is not initialized"));
      return null;
    }
    return wrapper.getRequestTaskScheduler();
  }

  private ChunkedFile extractChunkedFile(final @NotNull LocalMap<String, Object> map, final @NotNull io.vertx.ext.web.RoutingContext ctx) {
    final var chunkedFileWrapper = ((ShareableWrappers.ChunkedFileWrapper) (map.get("chunkedFile")));
    if (chunkedFileWrapper == null) {
      ctx.fail(new RuntimeException("Chunked file is not present in the local map, maybe the file is not yet opened?"));
      return null;
    }
    return chunkedFileWrapper.getChunkedFile();
  }
}
