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
import lombok.RequiredArgsConstructor;
import org.jetbrains.annotations.NotNull;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_server.HandlersHolder;
import ru.itmo.ctlab.hict.hict_server.tracks.Track1DManager;
import ru.itmo.ctlab.hict.hict_server.util.shareable.ShareableWrappers;

import java.util.Map;

@RequiredArgsConstructor
public class TrackHandlersHolder extends HandlersHolder {
  private final Vertx vertx;

  @Override
  public void addHandlersToRouter(final @NotNull Router router) {
    router.post("/tracks/list_files").blockingHandler(ctx -> {
      final var manager = getTrackManager(ctx);
      if (manager == null) {
        return;
      }
      ctx.response()
        .putHeader("content-type", "application/json")
        .end(Json.encode(manager.listTrackFiles()));
    });

    router.post("/tracks/open").blockingHandler(ctx -> {
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
      final var summary = manager.openTrack(
        filename,
        request.getString("name"),
        request.getString("color")
      );
      ctx.response()
        .putHeader("content-type", "application/json")
        .end(Json.encode(summary));
    });

    router.post("/tracks/list").blockingHandler(ctx -> {
      final var manager = getTrackManager(ctx);
      if (manager == null) {
        return;
      }
      ctx.response()
        .putHeader("content-type", "application/json")
        .end(Json.encode(manager.listTracks()));
    });

    router.post("/tracks/update").blockingHandler(ctx -> {
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
      final var updated = manager.updateTrack(
        trackId,
        request.containsKey("visible") ? request.getBoolean("visible") : null,
        request.getString("color"),
        request.getString("name"),
        request.getString("renderMode"),
        request.getString("aggregationMode")
      );
      ctx.response()
        .putHeader("content-type", "application/json")
        .end(Json.encode(updated));
    });

    router.post("/tracks/remove").blockingHandler(ctx -> {
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
      manager.removeTrack(trackId);
      ctx.response()
        .putHeader("content-type", "application/json")
        .end(Json.encode(Map.of("status", "removed", "trackId", trackId)));
    });

    router.post("/tracks/query_1d").blockingHandler(ctx -> {
      final var request = ctx.body().asJsonObject();
      final var startBp = request.getLong("startBp", 0L);
      final var endBp = request.getLong("endBp", startBp + 1L);
      final var widthPx = request.getInteger("widthPx", 512);

      final @NotNull @NonNull LocalMap<String, Object> map = this.vertx.sharedData().getLocalMap("hict_server");
      final var manager = getTrackManager(ctx);
      if (manager == null) {
        return;
      }
      final var chunkedFile = extractChunkedFile(map, ctx);
      if (chunkedFile == null) {
        return;
      }
      final var result = manager.queryVisibleTracks(chunkedFile, startBp, endBp, widthPx);
      ctx.response()
        .putHeader("content-type", "application/json")
        .end(Json.encode(result));
    });
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

  private ChunkedFile extractChunkedFile(final @NotNull LocalMap<String, Object> map, final @NotNull io.vertx.ext.web.RoutingContext ctx) {
    final var chunkedFileWrapper = ((ShareableWrappers.ChunkedFileWrapper) (map.get("chunkedFile")));
    if (chunkedFileWrapper == null) {
      ctx.fail(new RuntimeException("Chunked file is not present in the local map, maybe the file is not yet opened?"));
      return null;
    }
    return chunkedFileWrapper.getChunkedFile();
  }
}
