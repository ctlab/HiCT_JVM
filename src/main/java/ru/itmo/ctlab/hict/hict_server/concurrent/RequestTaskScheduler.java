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

package ru.itmo.ctlab.hict.hict_server.concurrent;

import io.vertx.core.Vertx;
import io.vertx.ext.web.RoutingContext;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.FutureTask;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

@Slf4j
public final class RequestTaskScheduler implements AutoCloseable {
  public static final String LOCAL_MAP_KEY = "requestTaskScheduler";

  private final @NotNull Vertx vertx;
  private final @NotNull EnumMap<RequestPriority, ThreadPoolExecutor> pools;
  private final @NotNull EnumMap<RequestPriority, PoolSizing> poolSizing;
  private final @NotNull EnumMap<RequestPriority, AtomicInteger> activeWorkersByPriority;
  private final @NotNull EnumMap<CancellationDomain, AtomicLong> generations;
  private final @NotNull EnumMap<CancellationDomain, ConcurrentHashMap<Long, Set<FutureTask<?>>>> trackedTasks;
  private final @NotNull Semaphore globalElasticPermits;
  private final int totalMaxWorkers;
  private final int reservedMinWorkers;

  @Getter
  public static final class PoolSizing {
    private final int minWorkers;
    private final int maxWorkers;

    public PoolSizing(final int minWorkers, final int maxWorkers) {
      this.minWorkers = Math.max(2, minWorkers);
      this.maxWorkers = Math.max(this.minWorkers, maxWorkers);
    }
  }

  public record SchedulerConfig(
    int totalMaxWorkers,
    int queueCapacityPerPriority,
    int keepAliveSeconds,
    @NotNull Map<RequestPriority, PoolSizing> perPrioritySizing
  ) {
    public SchedulerConfig {
      if (queueCapacityPerPriority < 1) {
        queueCapacityPerPriority = 1;
      }
      if (keepAliveSeconds < 1) {
        keepAliveSeconds = 30;
      }
    }
  }

  public enum RequestPriority {
    UI_UX,
    ASSEMBLY,
    TILE,
    TRACK,
    EXPORT
  }

  public enum CancellationDomain {
    TILE,
    TRACK,
    EXPORT
  }

  public record PoolDiagnostics(
    int corePoolSize,
    int maxPoolSize,
    int currentPoolSize,
    int largestPoolSize,
    int activeCount,
    int queueSize,
    int queueCapacity,
    long completedTaskCount,
    long taskCount
  ) {
  }

  public record CancellationDomainDiagnostics(
    long currentGeneration,
    int trackedTaskCount,
    @NotNull Map<Long, Integer> trackedTasksByGeneration
  ) {
  }

  public record SchedulerDiagnostics(
    long timestampMs,
    int totalMaxWorkers,
    int reservedMinWorkers,
    int elasticWorkersInUse,
    int elasticWorkersAvailable,
    @NotNull Map<RequestPriority, PoolDiagnostics> pools,
    @NotNull Map<CancellationDomain, CancellationDomainDiagnostics> cancellationDomains
  ) {
  }

  @FunctionalInterface
  public interface ThrowingSupplier<T> {
    T get() throws Exception;
  }

  private static final class RequestTaskCancelledException extends RuntimeException {
    private RequestTaskCancelledException(final @NotNull String message) {
      super(message);
    }
  }

  public RequestTaskScheduler(final @NotNull Vertx vertx,
                              final @NotNull SchedulerConfig config) {
    this.vertx = vertx;
    this.pools = new EnumMap<>(RequestPriority.class);
    this.poolSizing = new EnumMap<>(RequestPriority.class);
    this.activeWorkersByPriority = new EnumMap<>(RequestPriority.class);
    this.generations = new EnumMap<>(CancellationDomain.class);
    this.trackedTasks = new EnumMap<>(CancellationDomain.class);

    final var validatedSizing = new EnumMap<RequestPriority, PoolSizing>(RequestPriority.class);
    RequestPriority[] priorities = RequestPriority.values();
    int minTotal = 0;
    for (final var priority : priorities) {
      final var sizing = config.perPrioritySizing().getOrDefault(priority, new PoolSizing(2, 2));
      final var validated = new PoolSizing(sizing.getMinWorkers(), sizing.getMaxWorkers());
      validatedSizing.put(priority, validated);
      this.poolSizing.put(priority, validated);
      this.activeWorkersByPriority.put(priority, new AtomicInteger(0));
      minTotal += validated.getMinWorkers();
    }
    final int totalMaxWorkers = Math.max(minTotal, config.totalMaxWorkers());
    final int elasticBudget = Math.max(0, totalMaxWorkers - minTotal);
    this.globalElasticPermits = new Semaphore(elasticBudget, true);
    this.totalMaxWorkers = totalMaxWorkers;
    this.reservedMinWorkers = minTotal;

    for (final var domain : CancellationDomain.values()) {
      this.generations.put(domain, new AtomicLong(0L));
      this.trackedTasks.put(domain, new ConcurrentHashMap<>());
    }

    for (final var priority : priorities) {
      final var sizing = validatedSizing.get(priority);
      final var pool = new ThreadPoolExecutor(
        sizing.getMinWorkers(),
        sizing.getMaxWorkers(),
        config.keepAliveSeconds(),
        TimeUnit.SECONDS,
        new java.util.concurrent.ArrayBlockingQueue<>(config.queueCapacityPerPriority()),
        new NamedThreadFactory("hict-" + priority.name().toLowerCase() + "-worker"),
        new ThreadPoolExecutor.AbortPolicy()
      );
      pool.prestartAllCoreThreads();
      this.pools.put(priority, pool);
    }

    log.info(
      "Initialized request scheduler: totalMaxWorkers={}, reservedMinWorkers={}, elasticBudget={}, queueCapacityPerPriority={}, keepAliveSeconds={}, pools={}",
      totalMaxWorkers,
      minTotal,
      elasticBudget,
      config.queueCapacityPerPriority(),
      config.keepAliveSeconds(),
      validatedSizing
    );
  }

  public <T> void submit(final @NotNull RoutingContext ctx,
                         final @NotNull RequestPriority priority,
                         final @Nullable CancellationDomain domain,
                         final @NotNull ThrowingSupplier<T> supplier,
                         final @NotNull Consumer<T> onSuccess) {
    submit(ctx, priority, domain, supplier, onSuccess, null);
  }

  public <T> void submit(final @NotNull RoutingContext ctx,
                         final @NotNull RequestPriority priority,
                         final @Nullable CancellationDomain domain,
                         final @NotNull ThrowingSupplier<T> supplier,
                         final @NotNull Consumer<T> onSuccess,
                         final @Nullable Runnable onCancelled) {
    final var pool = this.pools.get(priority);
    if (pool == null) {
      dispatchFailure(ctx, new IllegalStateException("No pool is configured for priority " + priority));
      return;
    }
    final long generationSnapshot = domain != null ? this.currentGeneration(domain) : -1L;
    final var selfRef = new java.util.concurrent.atomic.AtomicReference<FutureTask<?>>();
    final FutureTask<Void> futureTask = new FutureTask<>(() -> {
      final var activeWorkers = this.activeWorkersByPriority.get(priority);
      final int minWorkers = this.poolSizing.get(priority).getMinWorkers();
      boolean elasticPermitAcquired = false;
      boolean workerRegistered = false;
      try {
        this.ensureNotCancelled(domain, generationSnapshot);
        final int workersNow = activeWorkers.incrementAndGet();
        workerRegistered = true;
        if (workersNow > minWorkers) {
          this.globalElasticPermits.acquire();
          elasticPermitAcquired = true;
        }
        this.ensureNotCancelled(domain, generationSnapshot);
        final var value = supplier.get();
        this.ensureNotCancelled(domain, generationSnapshot);
        dispatchSuccess(ctx, value, onSuccess);
      } catch (final Throwable t) {
        if (isCancellation(t)) {
          dispatchCancelled(ctx, onCancelled);
        } else {
          dispatchFailure(ctx, t);
        }
      } finally {
        if (elasticPermitAcquired) {
          this.globalElasticPermits.release();
        }
        if (workerRegistered) {
          activeWorkers.decrementAndGet();
        }
        final var task = selfRef.get();
        if (domain != null && task != null) {
          unregisterTask(domain, generationSnapshot, task);
        }
      }
      return null;
    });
    selfRef.set(futureTask);

    if (domain != null) {
      registerTask(domain, generationSnapshot, futureTask);
    }

    try {
      pool.execute(futureTask);
    } catch (final java.util.concurrent.RejectedExecutionException rejection) {
      if (domain != null) {
        unregisterTask(domain, generationSnapshot, futureTask);
      }
      dispatchFailure(ctx, new IllegalStateException(
        "Request queue is saturated for priority " + priority + ". Please retry.", rejection
      ));
    }
  }

  public long bumpGeneration(final @NotNull CancellationDomain domain) {
    final long newGeneration = this.generations.get(domain).incrementAndGet();
    final var mapForDomain = this.trackedTasks.get(domain);
    mapForDomain.entrySet().removeIf(entry -> {
      if (entry.getKey() >= newGeneration) {
        return false;
      }
      entry.getValue().forEach(task -> task.cancel(true));
      return true;
    });
    return newGeneration;
  }

  public void bumpAssemblyGeneration() {
    bumpGeneration(CancellationDomain.TILE);
    bumpGeneration(CancellationDomain.TRACK);
    bumpGeneration(CancellationDomain.EXPORT);
  }

  public @NotNull SchedulerDiagnostics diagnosticsSnapshot() {
    final var poolSnapshots = new EnumMap<RequestPriority, PoolDiagnostics>(RequestPriority.class);
    for (final var entry : this.pools.entrySet()) {
      final var pool = entry.getValue();
      final int queueSize = pool.getQueue().size();
      final int queueCapacity = queueSize + pool.getQueue().remainingCapacity();
      poolSnapshots.put(
        entry.getKey(),
        new PoolDiagnostics(
          pool.getCorePoolSize(),
          pool.getMaximumPoolSize(),
          pool.getPoolSize(),
          pool.getLargestPoolSize(),
          pool.getActiveCount(),
          queueSize,
          queueCapacity,
          pool.getCompletedTaskCount(),
          pool.getTaskCount()
        )
      );
    }
    final var cancellationSnapshots = new EnumMap<CancellationDomain, CancellationDomainDiagnostics>(CancellationDomain.class);
    for (final var domain : CancellationDomain.values()) {
      final var trackedByGeneration = new LinkedHashMap<Long, Integer>();
      int trackedCount = 0;
      for (final var entry : this.trackedTasks.get(domain).entrySet()) {
        final int size = entry.getValue().size();
        trackedCount += size;
        trackedByGeneration.put(entry.getKey(), size);
      }
      cancellationSnapshots.put(
        domain,
        new CancellationDomainDiagnostics(
          this.currentGeneration(domain),
          trackedCount,
          trackedByGeneration
        )
      );
    }
    final int elasticAvailable = this.globalElasticPermits.availablePermits();
    return new SchedulerDiagnostics(
      System.currentTimeMillis(),
      this.totalMaxWorkers,
      this.reservedMinWorkers,
      Math.max(0, this.totalMaxWorkers - this.reservedMinWorkers - elasticAvailable),
      elasticAvailable,
      poolSnapshots,
      cancellationSnapshots
    );
  }

  public long currentGeneration(final @NotNull CancellationDomain domain) {
    return this.generations.get(domain).get();
  }

  private void registerTask(final @NotNull CancellationDomain domain,
                            final long generation,
                            final @NotNull FutureTask<?> task) {
    this.trackedTasks
      .get(domain)
      .computeIfAbsent(generation, g -> ConcurrentHashMap.newKeySet())
      .add(task);
  }

  private void unregisterTask(final @NotNull CancellationDomain domain,
                              final long generation,
                              final @NotNull FutureTask<?> task) {
    final var mapForDomain = this.trackedTasks.get(domain);
    final var tasksForGeneration = mapForDomain.get(generation);
    if (tasksForGeneration == null) {
      return;
    }
    tasksForGeneration.remove(task);
    if (tasksForGeneration.isEmpty()) {
      mapForDomain.remove(generation, tasksForGeneration);
    }
  }

  private void ensureNotCancelled(final @Nullable CancellationDomain domain,
                                  final long generationSnapshot) {
    if (domain == null) {
      return;
    }
    if (generationSnapshot != currentGeneration(domain)) {
      throw new RequestTaskCancelledException(
        "Request was cancelled for domain " + domain + " at generation " + generationSnapshot
      );
    }
    if (Thread.currentThread().isInterrupted()) {
      throw new CancellationException("Worker thread interrupted");
    }
  }

  private static boolean isCancellation(final @NotNull Throwable throwable) {
    Throwable cursor = throwable;
    while (cursor != null) {
      if (cursor instanceof RequestTaskCancelledException
        || cursor instanceof CancellationException
        || cursor instanceof InterruptedException) {
        return true;
      }
      cursor = cursor.getCause();
    }
    return false;
  }

  private <T> void dispatchSuccess(final @NotNull RoutingContext ctx,
                                   final @NotNull T value,
                                   final @NotNull Consumer<T> onSuccess) {
    this.vertx.runOnContext(v -> {
      if (ctx.response().ended()) {
        return;
      }
      try {
        onSuccess.accept(value);
      } catch (final Throwable t) {
        ctx.fail(t);
      }
    });
  }

  private void dispatchFailure(final @NotNull RoutingContext ctx,
                               final @NotNull Throwable throwable) {
    this.vertx.runOnContext(v -> {
      if (ctx.response().ended()) {
        return;
      }
      ctx.fail(throwable);
    });
  }

  private void dispatchCancelled(final @NotNull RoutingContext ctx,
                                 final @Nullable Runnable onCancelled) {
    this.vertx.runOnContext(v -> {
      if (ctx.response().ended()) {
        return;
      }
      if (onCancelled != null) {
        onCancelled.run();
      } else {
        ctx.response().setStatusCode(200).end();
      }
    });
  }

  @Override
  public void close() {
    this.pools.values().forEach(pool -> {
      pool.shutdownNow();
      try {
        pool.awaitTermination(3, TimeUnit.SECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });
  }

  private static final class NamedThreadFactory implements ThreadFactory {
    private final @NotNull String prefix;
    private final @NotNull AtomicInteger counter = new AtomicInteger(0);

    private NamedThreadFactory(final @NotNull String prefix) {
      this.prefix = prefix;
    }

    @Override
    public @NotNull Thread newThread(final @NotNull Runnable r) {
      final var thread = new Thread(r);
      thread.setName(prefix + "-" + counter.incrementAndGet());
      thread.setDaemon(true);
      return thread;
    }
  }
}
