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

package ru.itmo.ctlab.hict.hict_server.handlers.fastaonly;

import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.shareddata.LocalMap;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.HttpException;
import org.jetbrains.annotations.NotNull;
import ru.itmo.ctlab.hict.hict_library.converters.FastaOnlyAssembly;
import ru.itmo.ctlab.hict.hict_server.HandlersHolder;
import ru.itmo.ctlab.hict.hict_server.fastaonly.FastaOnlyJobService;
import ru.itmo.ctlab.hict.hict_server.util.shareable.ShareableWrappers;

/**
 * {@code /fasta_only/start|status|cancel}: make an assembly from a FASTA with no contact data. The client
 * opens {@code outputFilename} with the FASTA linked, then applies {@code agpFilename} when present.
 */
public class FastaOnlyHandlersHolder extends HandlersHolder {
  private static final int MAX_MIN_N_RUN = 1_000_000;
  private final Vertx vertx;
  private final FastaOnlyJobService service = new FastaOnlyJobService();

  public FastaOnlyHandlersHolder(final Vertx vertx) {
    this.vertx = vertx;
  }

  @Override
  public void addHandlersToRouter(final @NotNull Router router) {
    router.post("/fasta_only/start").handler(ctx -> {
      final var body = ctx.body().asJsonObject();
      final var fasta = body == null ? null : body.getString("fastaFilename");
      if (fasta == null || fasta.isBlank()) {
        ctx.fail(new HttpException(400, "fastaFilename is required"));
        return;
      }
      final boolean split = body.getBoolean("splitAtN", false);
      final int minRun = body.getInteger("minNRun", FastaOnlyAssembly.DEFAULT_MIN_N_RUN);
      if (minRun < 1 || minRun > MAX_MIN_N_RUN) {
        ctx.fail(new HttpException(400, "minNRun must be between 1 and " + MAX_MIN_N_RUN));
        return;
      }
      final @NotNull LocalMap<String, Object> map = vertx.sharedData().getLocalMap("hict_server");
      final var dataDirectory = (ShareableWrappers.PathWrapper) map.get("dataDirectory");
      if (dataDirectory == null) {
        ctx.fail(new IllegalStateException("The data directory is not configured"));
        return;
      }
      try {
        final var status = service.start(dataDirectory.getPath(), new FastaOnlyJobService.Request(fasta, split, minRun));
        ctx.response().putHeader("content-type", "application/json").end(Json.encode(status));
      } catch (final IllegalStateException busy) {
        ctx.fail(new HttpException(409, busy.getMessage()));
      }
    });
    router.post("/fasta_only/status").handler(ctx ->
      ctx.response().putHeader("content-type", "application/json").end(Json.encode(service.status())));
    router.post("/fasta_only/cancel").handler(ctx ->
      ctx.response().putHeader("content-type", "application/json").end(Json.encode(service.cancel())));
  }
}
