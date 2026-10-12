package ru.itmo.ctlab.hict.hict_server;

import io.vertx.core.Context;
import io.vertx.core.Vertx;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class MainVerticleStartupTest {
  @Test void slowNativeLoadingDoesNotBlockEventLoopAndFailureFailsDeployment() throws Exception {
    var vertx = Vertx.vertx();
    var entered = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    var timerFired = new CountDownLatch(1);
    var application = new MainVerticle() {
      @Override protected void initializeNativeLibraries() {
        assertFalse(Context.isOnEventLoopThread());
        entered.countDown();
        try { if (!release.await(5, TimeUnit.SECONDS)) throw new IllegalStateException("Test did not release loader"); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IllegalStateException(e); }
        throw new IllegalStateException("Simulated native initialization failure");
      }
    };
    try {
      var deployment = vertx.deployVerticle(application).toCompletionStage().toCompletableFuture();
      assertTrue(entered.await(3, TimeUnit.SECONDS));
      application.getVertx().runOnContext(ignored -> timerFired.countDown());
      assertTrue(timerFired.await(2, TimeUnit.SECONDS), "Event loop must remain responsive while native loading is blocked");
      release.countDown();
      var failure = assertThrows(ExecutionException.class, () -> deployment.get(5, TimeUnit.SECONDS));
      assertEquals("Simulated native initialization failure", failure.getCause().getMessage());
    } finally {
      release.countDown();
      vertx.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
    }
  }
}
