package ru.itmo.ctlab.hict.hict_server.handlers.tiles;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.handler.HttpException;
import lombok.extern.slf4j.Slf4j;
import ru.itmo.ctlab.hict.hict_library.visualization.DistanceExpectedNormalizer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.Map;
import java.util.UUID;
import java.util.function.LongSupplier;

/** Short-lived immutable render inputs. Viewport profile changes do not alter a captured profile. */
@Slf4j
public final class RenderCaptureRegistry {
  public record Stamp(Object primary, Object secondary, Object visualization, Object pipeline,
                      long tileGeneration, long trackGeneration) {
    boolean matches(Stamp next) {
      return next != null && primary == next.primary && secondary == next.secondary
        && visualization == next.visualization && pipeline == next.pipeline
        && tileGeneration == next.tileGeneration && trackGeneration == next.trackGeneration;
    }

    void logChanges(String stage, Stamp next) {
      if (!log.isDebugEnabled()) return;
      if (next == null) {
        log.debug("Figure capture {} lost its render state", stage);
        return;
      }
      log.debug("Figure capture {} changed: primary={}, secondary={}, visualization={}, pipeline={}, tile generation {} -> {}, track generation {} -> {}",
        stage, primary != next.primary, secondary != next.secondary, visualization != next.visualization,
        pipeline != next.pipeline, tileGeneration, next.tileGeneration, trackGeneration, next.trackGeneration);
    }
  }

  public static final class Capture {
    private final String id;
    private final Stamp stamp;
    private final DistanceExpectedNormalizer.DiagonalProfile profile;
    private final JsonObject metadata;
    private final long expiresAt, bytes;
    private boolean invalid;

    private Capture(String id, Stamp stamp, DistanceExpectedNormalizer.DiagonalProfile profile,
                    JsonObject metadata, long expiresAt, long bytes) {
      this.id = id; this.stamp = stamp; this.profile = profile; this.metadata = metadata.copy();
      this.expiresAt = expiresAt; this.bytes = bytes;
    }
    public String id() { return id; }
    public DistanceExpectedNormalizer.DiagonalProfile profile() { return profile; }
    public JsonObject metadata() { return metadata.copy(); }
  }

  private final Map<String, Capture> captures = new HashMap<>();
  private final int maximumCount;
  private final long maximumBytes, lifetimeNanos;
  private final LongSupplier now;

  public RenderCaptureRegistry() { this(4, 64L * 1024 * 1024, Duration.ofMinutes(5), System::nanoTime); }
  RenderCaptureRegistry(int maximumCount, long maximumBytes, Duration lifetime, LongSupplier now) {
    if (maximumCount <= 0 || maximumBytes <= 0 || lifetime.isZero() || lifetime.isNegative())
      throw new IllegalArgumentException("Positive capture count, memory budget and lifetime are required");
    this.maximumCount = maximumCount; this.maximumBytes = maximumBytes; this.lifetimeNanos = lifetime.toNanos(); this.now = now;
  }

  public synchronized Capture register(Stamp stamp, DistanceExpectedNormalizer.DiagonalProfile profile, JsonObject metadata) {
    expire();
    long bytes = metadata.encode().getBytes(StandardCharsets.UTF_8).length + profileBytes(profile);
    long used = captures.values().stream().mapToLong(c -> c.bytes).sum();
    if (captures.size() >= maximumCount || bytes > maximumBytes - used)
      throw new HttpException(429, "Figure rendering contexts are busy or exceed the memory budget. Close another export or choose a coarser view.");
    var id = UUID.randomUUID().toString();
    var capture = new Capture(id, stamp, profile, metadata, now.getAsLong() + lifetimeNanos, bytes);
    captures.put(id, capture);
    return capture;
  }

  public synchronized Capture require(String id, Stamp current) {
    if (id == null || !id.matches("[a-f0-9-]{36}")) throw new HttpException(400, "A valid figure rendering context is required");
    expire();
    var capture = captures.get(id);
    if (capture == null) throw new HttpException(409, "The figure rendering context expired. Capture the figure again.");
    validate(capture, current);
    return capture;
  }

  public synchronized void validate(Capture capture, Stamp current) {
    final var active = captures.get(capture.id) == capture && !capture.invalid && now.getAsLong() < capture.expiresAt;
    if (!active || !capture.stamp.matches(current)) {
      if (active) capture.stamp.logChanges("validation", current);
      capture.invalid = true;
      throw new HttpException(409, "The assembly, normalization, tracks or rendering changed during figure capture. Capture the figure again.");
    }
  }

  public synchronized void release(String id) { captures.remove(id); }
  public synchronized void invalidate(Capture capture) { capture.invalid = true; }
  synchronized int size() { expire(); return captures.size(); }
  private void expire() {
    long time = now.getAsLong();
    captures.values().removeIf(c -> time >= c.expiresAt);
  }
  private static long profileBytes(DistanceExpectedNormalizer.DiagonalProfile profile) {
    if (profile == null) return 0;
    long length = profile.means().length;
    for (var segment : profile.segmentProfiles()) length += segment.means().length;
    return Math.multiplyExact(length, Double.BYTES) + 64L * profile.segmentProfiles().length;
  }

  /** Fingerprints the actual means and domains, not just the requested fit rectangle. */
  public static JsonObject profileMetadata(DistanceExpectedNormalizer.DiagonalProfile profile, long bpResolution, boolean visibleOnly) {
    if (profile == null) return null;
    try {
      var digest = MessageDigest.getInstance("SHA-256");
      var buffer = ByteBuffer.allocate(8192);
      java.util.function.LongConsumer put = value -> {
        if (buffer.remaining() < Long.BYTES) { digest.update(buffer.array(), 0, buffer.position()); buffer.clear(); }
        buffer.putLong(value);
      };
      put.accept(1); put.accept(bpResolution); put.accept(visibleOnly ? 1 : 0);
      put.accept(profile.resolutionOrder()); put.accept(profile.startRowPx()); put.accept(profile.endRowPx());
      put.accept(profile.startColPx()); put.accept(profile.endColPx()); put.accept(profile.minDiagonal());
      put.accept(profile.means().length);
      for (double mean : profile.means()) put.accept(Double.doubleToLongBits(mean));
      put.accept(profile.segmentProfiles().length);
      for (var segment : profile.segmentProfiles()) {
        put.accept(segment.startPx()); put.accept(segment.endPx()); put.accept(segment.minDiagonal()); put.accept(segment.means().length);
        for (double mean : segment.means()) put.accept(Double.doubleToLongBits(mean));
      }
      digest.update(buffer.array(), 0, buffer.position());
      return new JsonObject().put("schemaVersion", 1).put("algorithm", "HICT-EXPECTED-PROFILE-1")
        .put("sha256", HexFormat.of().formatHex(digest.digest())).put("bpResolution", bpResolution)
        .put("unit", visibleOnly ? "PIXELS" : "BINS").put("startRowPx", profile.startRowPx()).put("endRowPx", profile.endRowPx())
        .put("startColPx", profile.startColPx()).put("endColPx", profile.endColPx())
        .put("minDiagonal", profile.minDiagonal()).put("diagonalCount", profile.means().length)
        .put("domainCount", profile.segmentProfiles().length)
        .put("sampleSemantics", "UNIQUE_UNORDERED_SOURCE_PAIR_MASS").put("distanceSemantics", "DISPLAY_BIN_DISTANCE");
    } catch (java.security.NoSuchAlgorithmException impossible) { throw new IllegalStateException(impossible); }
  }
}
