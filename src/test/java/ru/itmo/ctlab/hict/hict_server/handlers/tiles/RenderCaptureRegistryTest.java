package ru.itmo.ctlab.hict.hict_server.handlers.tiles;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.handler.HttpException;
import org.junit.jupiter.api.Test;
import ru.itmo.ctlab.hict.hict_library.visualization.DistanceExpectedNormalizer;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import static org.junit.jupiter.api.Assertions.*;

class RenderCaptureRegistryTest {
  private static RenderCaptureRegistry.Stamp stamp(Object primary, long generation) {
    return new RenderCaptureRegistry.Stamp(primary, null, null, null, generation, 0);
  }
  private static DistanceExpectedNormalizer.DiagonalProfile profile(double mean, long domainEnd) {
    return new DistanceExpectedNormalizer.DiagonalProfile(1, 0, 4, 0, 4, 0, new double[]{mean, 2},
      new DistanceExpectedNormalizer.SegmentProfile[]{new DistanceExpectedNormalizer.SegmentProfile(0, domainEnd, 0, new double[]{mean})});
  }
  @Test void sourceIdentityAndGenerationChangesPermanentlyInvalidateCaptures() {
    var registry = new RenderCaptureRegistry();
    var primary = new Object();
    var stamp = stamp(primary, 0);
    var capture = registry.register(stamp, null, new JsonObject());
    assertSame(capture, registry.require(capture.id(), stamp(primary, 0)));
    assertEquals(409, assertThrows(HttpException.class, () -> registry.require(capture.id(), stamp(primary, 1))).getStatusCode());
    assertThrows(HttpException.class, () -> registry.require(capture.id(), stamp));
    var second = registry.register(stamp, null, new JsonObject());
    assertThrows(HttpException.class, () -> registry.require(second.id(), stamp(new Object(), 0)));
    assertThrows(HttpException.class, () -> registry.require(second.id(), stamp));
  }
  @Test void settingsAndTrackChangesInvalidateCaptures() {
    for (var next : new RenderCaptureRegistry.Stamp[]{
      new RenderCaptureRegistry.Stamp(null, new Object(), null, null, 0, 0),
      new RenderCaptureRegistry.Stamp(null, null, new Object(), null, 0, 0),
      new RenderCaptureRegistry.Stamp(null, null, null, new Object(), 0, 0),
      new RenderCaptureRegistry.Stamp(null, null, null, null, 0, 1)}) {
      var registry = new RenderCaptureRegistry();
      var capture = registry.register(stamp(null, 0), null, new JsonObject());
      assertThrows(HttpException.class, () -> registry.require(capture.id(), next));
    }
  }
  @Test void expiryReleaseAndCountBudgetBoundAbandonedCaptures() {
    var clock = new AtomicLong();
    var registry = new RenderCaptureRegistry(1, 1024, Duration.ofNanos(10), clock::get);
    var stamp = stamp(null, 0);
    var capture = registry.register(stamp, null, new JsonObject());
    assertEquals(429, assertThrows(HttpException.class, () -> registry.register(stamp, null, new JsonObject())).getStatusCode());
    clock.set(10);
    assertEquals(0, registry.size());
    assertThrows(HttpException.class, () -> registry.validate(capture, stamp));
    var next = registry.register(stamp, null, new JsonObject());
    registry.release(next.id());
    registry.release(next.id());
    assertThrows(HttpException.class, () -> registry.validate(next, stamp));
    assertThrows(HttpException.class, () -> registry.require(next.id(), stamp));
    assertEquals(400, assertThrows(HttpException.class, () -> registry.require("../escape", stamp)).getStatusCode());
  }
  @Test void actualProfileArraysCountAgainstMemoryBudgetAndMetadataIsCopied() {
    var clock = new AtomicLong();
    var registry = new RenderCaptureRegistry(4, 100, Duration.ofSeconds(1), clock::get);
    var stamp = stamp(null, 0);
    assertThrows(HttpException.class, () -> registry.register(stamp, profile(1, 4), new JsonObject().put("description", "x".repeat(100))));
    var metadata = new JsonObject().put("nested", new JsonObject().put("value", 1));
    var capture = registry.register(stamp, null, metadata);
    metadata.getJsonObject("nested").put("value", 2);
    var copy = capture.metadata();
    copy.getJsonObject("nested").put("value", 3);
    assertEquals(1, capture.metadata().getJsonObject("nested").getInteger("value"));
  }
  @Test void scientificFingerprintIncludesMeansDomainsResolutionAndGridUnit() {
    var baseline = RenderCaptureRegistry.profileMetadata(profile(1, 4), 4, true);
    assertEquals(baseline, RenderCaptureRegistry.profileMetadata(profile(1, 4), 4, true));
    for (var changed : new JsonObject[]{
      RenderCaptureRegistry.profileMetadata(profile(2, 4), 4, true),
      RenderCaptureRegistry.profileMetadata(profile(1, 3), 4, true),
      RenderCaptureRegistry.profileMetadata(profile(1, 4), 10, true),
      RenderCaptureRegistry.profileMetadata(profile(1, 4), 4, false)})
      assertNotEquals(baseline.getString("sha256"), changed.getString("sha256"));
    assertEquals("PIXELS", baseline.getString("unit"));
  }
}
