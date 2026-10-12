package ru.itmo.ctlab.hict.hict_server.logging;

import ch.qos.logback.classic.AsyncAppender;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.ConsoleAppender;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;

public class AsyncLoggingTest {
  private static CountDownLatch entered;
  private static CountDownLatch release;
  private static AtomicInteger count;
  public static class SlowConsole extends ConsoleAppender<ILoggingEvent> {
    @Override protected void append(ILoggingEvent event) {
      entered.countDown();
      try { if (release.getCount() > 0 && !release.await(5, TimeUnit.SECONDS)) throw new AssertionError("Sink not released"); }
      catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new AssertionError(e); }
      count.incrementAndGet();
    }
  }
  @Test void slowConsoleDoesNotBlockNormalRequestsAndShutdownDrainsDiagnostics() throws Exception {
    var context = new LoggerContext();
    entered = new CountDownLatch(1);
    release = new CountDownLatch(1);
    count = new AtomicInteger();
    try {
      var configurator = new JoranConfigurator();
      configurator.setContext(context);
      final String xml;
      try (var input = getClass().getResourceAsStream("/logback.xml")) {
        xml = new String(input.readAllBytes(), StandardCharsets.UTF_8).replace(
          "ch.qos.logback.core.ConsoleAppender", SlowConsole.class.getName());
      }
      configurator.doConfigure(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));
      var root = context.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
      var async = assertInstanceOf(AsyncAppender.class, root.getAppender("ASYNC"));
      assertEquals(0, async.getDiscardingThreshold());
      assertFalse(async.isNeverBlock(), "Overload must not silently discard errors");
      root.warn("first message");
      assertTrue(entered.await(2, TimeUnit.SECONDS));
      for (int i=0;i<200;i++) root.error("diagnostic {}", i);
      assertEquals(0, count.get(), "Caller should enqueue while the output thread is blocked");
      release.countDown();context.stop();
      assertEquals(201, count.get(), "Queued errors must drain at shutdown");
    } finally { release.countDown();context.stop(); }
  }
}
