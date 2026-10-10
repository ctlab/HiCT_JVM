package ru.itmo.ctlab.hict.hict_server.concurrent;

import io.vertx.core.Vertx;
import io.vertx.ext.web.Router;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;
import static ru.itmo.ctlab.hict.hict_server.concurrent.RequestTaskScheduler.*;

class RequestTaskSchedulerTest {
  private final Vertx vertx = Vertx.vertx();
  private final RequestTaskScheduler scheduler = new RequestTaskScheduler(vertx,
    new SchedulerConfig(10, 1, 30, Map.of()));
  private final CountDownLatch release = new CountDownLatch(1);
  private final CountDownLatch active = new CountDownLatch(2);
  private final AtomicInteger queuedCalls = new AtomicInteger();
  private final AtomicInteger cancelledResponses = new AtomicInteger();
  private final AtomicInteger successfulResponses = new AtomicInteger();
  private final HttpClient client = HttpClient.newHttpClient();
  private int port;

  @AfterEach
  void close() throws Exception {
    release.countDown();
    scheduler.close();
    vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
    client.close();
  }

  private void start() throws Exception {
    var router = Router.router(vertx);
    router.get("/:kind").handler(ctx -> scheduler.submit(ctx, RequestPriority.TILE,
      CancellationDomain.TILE, () -> {
        if (ctx.pathParam("kind").equals("block")) {
          active.countDown();
          // Deliberately model a native call that cannot be interrupted safely.
          while (release.getCount() != 0) {
            try { release.await(); } catch (InterruptedException ignored) { }
          }
        } else if (ctx.pathParam("kind").equals("fail")) {
          throw new IllegalStateException("expected failure");
        } else queuedCalls.incrementAndGet();
        return "ready";
      }, value -> {
        successfulResponses.incrementAndGet();
        ctx.response().end(value);
      }, () -> {
        cancelledResponses.incrementAndGet();
        ctx.response().setStatusCode(409).end("cancelled");
      }));
    var server = vertx.createHttpServer().requestHandler(router).listen(0)
      .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
    port = server.actualPort();
  }

  private CompletableFuture<HttpResponse<String>> request(String kind) {
    return client.sendAsync(HttpRequest.newBuilder(URI.create("http://127.0.0.1:"+port+"/"+kind))
      .timeout(Duration.ofSeconds(8)).build(), HttpResponse.BodyHandlers.ofString());
  }

  private void until(BooleanSupplier ready) throws Exception {
    long deadline = System.nanoTime()+TimeUnit.SECONDS.toNanos(3);
    while (!ready.getAsBoolean() && System.nanoTime()<deadline) Thread.sleep(2);
    assertTrue(ready.getAsBoolean(), "Timed out waiting for scheduler state");
  }

  @Test
  void queuedCancellationCompletesHttpWithoutRunningSupplier() throws Exception {
    start(); var first=request("block");var second=request("block");
    assertTrue(active.await(3,TimeUnit.SECONDS));
    var queued=request("ready");
    until(()->scheduler.diagnosticsSnapshot().pools().get(RequestPriority.TILE).queueSize()==1);
    scheduler.bumpGeneration(CancellationDomain.TILE);
    assertEquals(409,queued.get(3,TimeUnit.SECONDS).statusCode());
    assertEquals(0,queuedCalls.get());
    assertEquals(409,first.get(3,TimeUnit.SECONDS).statusCode());
    assertEquals(409,second.get(3,TimeUnit.SECONDS).statusCode());
    assertEquals(3,cancelledResponses.get());
    release.countDown();
    until(()->scheduler.diagnosticsSnapshot().pools().get(RequestPriority.TILE).activeCount()==0);
    assertEquals(0,successfulResponses.get());
    assertEquals(0,scheduler.diagnosticsSnapshot().cancellationDomains().get(CancellationDomain.TILE).trackedTaskCount());
    assertEquals(200,request("ready").get(3,TimeUnit.SECONDS).statusCode());
  }

  @Test
  void runningCancellationCompletesBeforeUninterruptibleSupplierReturns() throws Exception {
    start();var first=request("block");var second=request("block");
    assertTrue(active.await(3,TimeUnit.SECONDS));
    scheduler.bumpGeneration(CancellationDomain.TILE);
    assertEquals(409,first.get(3,TimeUnit.SECONDS).statusCode());
    assertEquals(409,second.get(3,TimeUnit.SECONDS).statusCode());
    assertEquals(1,release.getCount());
    release.countDown();
    until(()->scheduler.diagnosticsSnapshot().pools().get(RequestPriority.TILE).activeCount()==0);
    assertEquals(2,cancelledResponses.get());
    assertEquals(0,successfulResponses.get());
  }

  @Test
  void normalSuccessAndFailureCompleteAndUnregister() throws Exception {
    start();assertEquals(200,request("ready").get(3,TimeUnit.SECONDS).statusCode());
    assertEquals(500,request("fail").get(3,TimeUnit.SECONDS).statusCode());
    assertEquals(1,successfulResponses.get());assertEquals(0,cancelledResponses.get());
    assertEquals(0,scheduler.diagnosticsSnapshot().cancellationDomains().get(CancellationDomain.TILE).trackedTaskCount());
  }

  @Test
  void saturatedQueueRespondsAndDoesNotLeakTracking() throws Exception {
    start();var first=request("block");var second=request("block");
    assertTrue(active.await(3,TimeUnit.SECONDS));var queued=request("ready");
    until(()->scheduler.diagnosticsSnapshot().pools().get(RequestPriority.TILE).queueSize()==1);
    assertEquals(500,request("ready").get(3,TimeUnit.SECONDS).statusCode());
    assertEquals(3,scheduler.diagnosticsSnapshot().cancellationDomains().get(CancellationDomain.TILE).trackedTaskCount());
    release.countDown();
    for(var response:java.util.List.of(first,second,queued))assertEquals(200,response.get(3,TimeUnit.SECONDS).statusCode());
    assertEquals(3,successfulResponses.get());
    assertEquals(0,scheduler.diagnosticsSnapshot().cancellationDomains().get(CancellationDomain.TILE).trackedTaskCount());
  }
}
