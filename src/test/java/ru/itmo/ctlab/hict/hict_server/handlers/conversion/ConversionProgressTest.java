package ru.itmo.ctlab.hict.hict_server.handlers.conversion;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class ConversionProgressTest {
  @Test
  void estimatesStageAndOverallTimeFromTheServerClock() {
    var clock = new TestClock();
    var job = job(clock);
    assertEquals(0, job.toDto().elapsedMillis());
    assertEquals(-1, job.toDto().etaMillis());
    assertNull(job.toDto().stageCount());
    job.start();
    ConversionHandlersHolder.parseProgress(job, "HICT_STAGE stage=metadata progress=0.0100 overall=0.0100 step=1/3 detail=Reading metadata");
    clock.advance(6_000);
    assertEquals(6_000, job.toDto().elapsedMillis());
    assertEquals(-1, job.toDto().etaMillis());
    assertEquals(-1, job.toDto().resolutionEtaMillis());
    ConversionHandlersHolder.parseProgress(job, "HICT_STAGE stage=metadata progress=0.5000 overall=0.2500 step=1/3 detail=Reading metadata");
    assertEquals(18_000, job.toDto().etaMillis());
    assertEquals(6_000, job.toDto().resolutionEtaMillis());
    assertEquals(1, job.toDto().stageIndex());
    assertEquals(3, job.toDto().stageCount());

    ConversionHandlersHolder.parseProgress(job, "HICT_STAGE stage=balance progress=0.5000 overall=0.5000 step=2/3 detail=Balancing resolution 5000");
    assertEquals(0, job.toDto().resolutionElapsedMillis());
    assertEquals(-1, job.toDto().resolutionEtaMillis());
    assertEquals(5_000, job.toDto().currentResolution());
    clock.advance(7_000);
    assertEquals(7_000, job.toDto().resolutionEtaMillis());
    job.finish("finished");
    clock.advance(20_000);
    assertEquals(13_000, job.toDto().elapsedMillis());
    assertEquals(-1, job.toDto().etaMillis());
  }

  @Test
  void usesReportedEtaWithoutTakingElapsedFromToolLines() {
    var clock = new TestClock();
    var job = job(clock);
    job.start();
    clock.advance(3_000);
    ConversionHandlersHolder.parseProgress(job, "Overall progress: 28% (7/25), elapsed=00:00, eta=00:22");
    ConversionHandlersHolder.parseProgress(job, "Resolution 1000 write: 50% (1/2), elapsed=00:00, eta=00:08");
    assertEquals(3_000, job.toDto().elapsedMillis());
    assertEquals(22_000, job.toDto().etaMillis());
    assertEquals(8_000, job.toDto().resolutionEtaMillis());
    assertNull(job.toDto().stageIndex());
    job.finish("failed");
    clock.advance(20_000);
    assertEquals(3_000, job.toDto().elapsedMillis());
    assertEquals(-1, job.toDto().etaMillis());
    assertEquals(-1, job.toDto().resolutionEtaMillis());
  }

  private static ConversionHandlersHolder.ConversionJob job(Clock clock) {
    return new ConversionHandlersHolder.ConversionJob(clock, "job",
      Path.of("input.cool"), Path.of("output.hict.hdf5"), null,
      ConversionDirection.MCOOL_TO_HICT, 1, false, false, null);
  }

  private static final class TestClock extends Clock {
    private Instant now = Instant.parse("2026-11-27T12:00:00Z");

    void advance(long millis) {
      now = now.plusMillis(millis);
    }

    @Override
    public ZoneId getZone() { return ZoneOffset.UTC; }

    @Override
    public Clock withZone(ZoneId zone) { return Clock.fixed(now, zone); }

    @Override
    public Instant instant() { return now; }
  }
}
