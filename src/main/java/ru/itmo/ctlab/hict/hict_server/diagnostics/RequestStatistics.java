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

package ru.itmo.ctlab.hict.hict_server.diagnostics;

import org.jetbrains.annotations.NotNull;

import java.lang.management.ManagementFactory;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

public final class RequestStatistics {
  private static final int WINDOW_SECONDS = 120;
  private static final int ENDPOINT_LIMIT = 16;
  private final long startedNanos = System.nanoTime();
  private final long startedMs = System.currentTimeMillis();
  private final @NotNull LongAdder totalRequests = new LongAdder();
  private final @NotNull LongAdder inFlightRequests = new LongAdder();
  private final @NotNull ConcurrentHashMap<Long, LongAdder> buckets = new ConcurrentHashMap<>();
  private final @NotNull ConcurrentHashMap<String, EndpointCounters> endpoints = new ConcurrentHashMap<>();

  public void recordStarted(final @NotNull String path) {
    final long epochSecond = System.currentTimeMillis() / 1000L;
    this.totalRequests.increment();
    this.inFlightRequests.increment();
    bucket(this.buckets, epochSecond).increment();
    endpoint(path).record(epochSecond);
    trimBuckets(epochSecond);
  }

  public void recordFinished() {
    this.inFlightRequests.decrement();
  }

  public @NotNull Map<String, Object> snapshot() {
    final long nowMs = System.currentTimeMillis();
    final long nowSecond = nowMs / 1000L;
    final double uptimeSeconds = Math.max(1.0e-9d, (System.nanoTime() - this.startedNanos) / 1_000_000_000.0d);
    final long total = this.totalRequests.sum();
    final var heap = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
    final var nonHeap = ManagementFactory.getMemoryMXBean().getNonHeapMemoryUsage();
    final var threads = ManagementFactory.getThreadMXBean();
    final var runtime = Runtime.getRuntime();
    return Map.ofEntries(
      Map.entry("timestampMs", nowMs),
      Map.entry("startedMs", this.startedMs),
      Map.entry("uptimeSeconds", uptimeSeconds),
      Map.entry("totalRequests", total),
      Map.entry("inFlightRequests", this.inFlightRequests.sum()),
      Map.entry("meanRequestsPerSecond", total / uptimeSeconds),
      Map.entry("requestsPerSecondLast10s", requestsPerSecond(nowSecond, 10)),
      Map.entry("requestsPerSecondLast60s", requestsPerSecond(nowSecond, 60)),
      Map.entry("heapUsedBytes", heap.getUsed()),
      Map.entry("heapCommittedBytes", heap.getCommitted()),
      Map.entry("heapMaxBytes", heap.getMax()),
      Map.entry("nonHeapUsedBytes", nonHeap.getUsed()),
      Map.entry("availableProcessors", runtime.availableProcessors()),
      Map.entry("liveThreads", threads.getThreadCount()),
      Map.entry("daemonThreads", threads.getDaemonThreadCount()),
      Map.entry("peakThreads", threads.getPeakThreadCount()),
      Map.entry("endpoints", endpointSnapshots(nowSecond))
    );
  }

  private double requestsPerSecond(final long nowSecond,
                                   final int seconds) {
    long sum = 0L;
    for (long second = nowSecond - seconds + 1L; second <= nowSecond; second++) {
      final var bucket = this.buckets.get(second);
      if (bucket != null) {
        sum += bucket.sum();
      }
    }
    return sum / (double) seconds;
  }

  private @NotNull java.util.List<Map<String, Object>> endpointSnapshots(final long nowSecond) {
    return this.endpoints.entrySet()
      .stream()
      .map(entry -> entry.getValue().snapshot(entry.getKey(), nowSecond))
      .sorted(Comparator.<Map<String, Object>, Long>comparing(map -> (Long) map.get("totalRequests")).reversed())
      .limit(ENDPOINT_LIMIT)
      .toList();
  }

  private void trimBuckets(final long nowSecond) {
    final long oldest = nowSecond - WINDOW_SECONDS;
    this.buckets.keySet().removeIf(second -> second < oldest);
    this.endpoints.values().forEach(endpoint -> endpoint.trim(nowSecond));
  }

  private static @NotNull LongAdder bucket(final @NotNull ConcurrentHashMap<Long, LongAdder> buckets,
                                           final long epochSecond) {
    return buckets.computeIfAbsent(epochSecond, ignored -> new LongAdder());
  }

  private @NotNull EndpointCounters endpoint(final @NotNull String path) {
    return this.endpoints.computeIfAbsent(normalizePath(path), ignored -> new EndpointCounters());
  }

  private static @NotNull String normalizePath(final @NotNull String path) {
    final var trimmed = path.trim();
    return trimmed.isBlank() ? "/" : trimmed;
  }

  private static final class EndpointCounters {
    private final @NotNull LongAdder total = new LongAdder();
    private final @NotNull ConcurrentHashMap<Long, LongAdder> buckets = new ConcurrentHashMap<>();

    void record(final long epochSecond) {
      this.total.increment();
      bucket(this.buckets, epochSecond).increment();
    }

    @NotNull Map<String, Object> snapshot(final @NotNull String path,
                                          final long nowSecond) {
      return Map.of(
        "path", path,
        "totalRequests", this.total.sum(),
        "requestsPerSecondLast10s", requestsPerSecond(nowSecond, 10),
        "requestsPerSecondLast60s", requestsPerSecond(nowSecond, 60)
      );
    }

    void trim(final long nowSecond) {
      final long oldest = nowSecond - WINDOW_SECONDS;
      this.buckets.keySet().removeIf(second -> second < oldest);
    }

    private double requestsPerSecond(final long nowSecond,
                                     final int seconds) {
      long sum = 0L;
      for (long second = nowSecond - seconds + 1L; second <= nowSecond; second++) {
        final var bucket = this.buckets.get(second);
        if (bucket != null) {
          sum += bucket.sum();
        }
      }
      return sum / (double) seconds;
    }
  }
}
