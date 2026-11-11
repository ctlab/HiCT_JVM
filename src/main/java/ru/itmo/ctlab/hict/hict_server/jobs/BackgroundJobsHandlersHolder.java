package ru.itmo.ctlab.hict.hict_server.jobs;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.HttpException;
import ru.itmo.ctlab.hict.hict_server.HandlersHolder;

import java.util.Comparator;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

/** Adapts job services without moving their execution or persisted state. */
public final class BackgroundJobsHandlersHolder extends HandlersHolder implements AutoCloseable {
  public record Service(String name, Supplier<List<BackgroundJob>> snapshot,
                        Predicate<String> cancel, Runnable shutdown) { }

  private final List<Service> services;

  public BackgroundJobsHandlersHolder(List<Service> services) {
    this.services = List.copyOf(services);
  }

  public List<BackgroundJob> snapshot() {
    return services.stream().flatMap(service -> service.snapshot.get().stream()
      .map(job -> job.withId(service.name + "/" + job.id())))
      .sorted(Comparator.comparing(BackgroundJob::createdAtMs, Comparator.nullsLast(Comparator.reverseOrder())))
      .toList();
  }

  @Override
  public void addHandlersToRouter(Router router) {
    router.get("/jobs").handler(ctx -> {
      final var jobs = new JsonArray(snapshot().stream().map(job ->
        JsonObject.mapFrom(job).put("canCancel", job.canCancel())).toList());
      ctx.response().putHeader("content-type", "application/json").end(jobs.encode());
    });
    router.post("/jobs/cancel").handler(ctx -> {
      final CancelBackgroundJobRequestDTO request;
      try { request = CancelBackgroundJobRequestDTO.fromJson(ctx.body().asJsonObject()); }
      catch (IllegalArgumentException invalid) { ctx.fail(new HttpException(400, invalid.getMessage())); return; }
      final var service = services.stream().filter(item -> item.name.equals(request.service())).findFirst().orElse(null);
      if (service == null || !service.cancel.test(request.localId())) {
        ctx.fail(new HttpException(404, "The job is no longer available"));
        return;
      }
      ctx.response().putHeader("content-type", "application/json").end(new JsonObject().put("status", "cancelling").encode());
    });
  }

  @Override
  public void close() {
    RuntimeException failure = null;
    for (var service : services) {
      try {
        service.snapshot.get().stream().filter(BackgroundJob::canCancel).forEach(job -> service.cancel.test(job.id()));
      } catch (RuntimeException error) {
        if (failure == null) failure = error;
        else failure.addSuppressed(error);
      }
    }
    for (var service : services) {
      try { service.shutdown.run(); }
      catch (RuntimeException error) {
        if (failure == null) failure = error;
        else failure.addSuppressed(error);
      }
    }
    if (failure != null) throw failure;
  }
}
