package ru.itmo.ctlab.hict.hict_server.handlers.fileop;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;
import java.util.function.BooleanSupplier;
import java.util.function.LongConsumer;

/**
 * Content identity for session resources. Two algorithms are understood:
 * {@code SHA-256} hashes the whole file (explicit, can take minutes for large maps) and
 * {@code HICT-QUICK-1} hashes the size, the first and last 4 MiB and 16 evenly spaced 64 KiB blocks,
 * so saving a session stays fast on multi-gigabyte maps. Neither caches by size or modification time,
 * and the modification time is not part of the identity, so copying a data folder keeps sessions valid.
 */
public record SessionFileFingerprint(String algorithm, long sizeBytes, String sha256) {
  public static final String FULL = "SHA-256";
  public static final String QUICK = "HICT-QUICK-1";
  private static final long EDGE = 4L << 20;
  private static final int BLOCK = 64 << 10;
  private static final int BLOCKS = 16;

  public static boolean supports(String algorithm) { return FULL.equals(algorithm) || QUICK.equals(algorithm); }

  public static SessionFileFingerprint compute(Path path, String algorithm) throws IOException {
    return switch (algorithm) {
      case FULL -> compute(path);
      case QUICK -> computeQuick(path);
      default -> throw new IOException("Unsupported fingerprint algorithm: " + algorithm);
    };
  }

  /** Why {@code current} (computed with the same algorithm) differs from this saved identity, or null if it matches. */
  public String explainDifference(SessionFileFingerprint current) {
    if (equals(current)) return null;
    if (sizeBytes != current.sizeBytes)
      return "its size changed from " + sizeBytes + " to " + current.sizeBytes + " bytes";
    return "its content differs from the saved version" + (QUICK.equals(algorithm) ? " (checked at the start, middle and end)" : "");
  }

  /** Samples the file instead of reading all of it. Cost is bounded by about 9 MiB regardless of file size. */
  public static SessionFileFingerprint computeQuick(Path path) throws IOException {
    return computeQuick(path, () -> Thread.currentThread().isInterrupted());
  }

  public static SessionFileFingerprint computeQuick(Path path, BooleanSupplier cancelled) throws IOException {
    checkCancelled(cancelled);
    final var resolved = path.toRealPath();
    final var before = Files.readAttributes(resolved, BasicFileAttributes.class);
    if (!before.isRegularFile()) throw new IOException("Session resource is not a regular file: " + path);
    final long size = before.size();
    final MessageDigest digest;
    try { digest = MessageDigest.getInstance("SHA-256"); }
    catch (NoSuchAlgorithmException impossible) { throw new IllegalStateException(impossible); }
    digest.update(QUICK.getBytes(java.nio.charset.StandardCharsets.US_ASCII));
    digest.update(java.nio.ByteBuffer.allocate(Long.BYTES).putLong(size).array());
    try (var channel = java.nio.channels.FileChannel.open(resolved, java.nio.file.StandardOpenOption.READ)) {
      final long covered = 2 * EDGE + (long) BLOCKS * BLOCK;
      if (size <= covered) {
        hashRange(channel, digest, 0, size, cancelled);
      } else {
        hashRange(channel, digest, 0, EDGE, cancelled);
        final long span = size - 2 * EDGE - BLOCK;
        for (int i = 0; i < BLOCKS; i++) hashRange(channel, digest, EDGE + span / (BLOCKS - 1) * i, BLOCK, cancelled);
        hashRange(channel, digest, size - EDGE, EDGE, cancelled);
      }
    }
    checkCancelled(cancelled);
    final var after = Files.readAttributes(resolved, BasicFileAttributes.class);
    if (after.size() != size || !after.lastModifiedTime().equals(before.lastModifiedTime())
        || !Objects.equals(after.fileKey(), before.fileKey()) || !path.toRealPath().equals(resolved)) {
      throw new IOException("Session resource changed while fingerprinting: " + path);
    }
    return new SessionFileFingerprint(QUICK, size, HexFormat.of().formatHex(digest.digest()));
  }

  private static void hashRange(java.nio.channels.FileChannel channel, MessageDigest digest, long offset, long length,
                                BooleanSupplier cancelled) throws IOException {
    final var buffer = java.nio.ByteBuffer.allocate((int) Math.min(length, 1 << 20));
    long position = offset, remaining = length;
    while (remaining > 0) {
      checkCancelled(cancelled);
      buffer.clear().limit((int) Math.min(buffer.capacity(), remaining));
      int read = channel.read(buffer, position);
      if (read < 0) throw new IOException("Session resource shrank while fingerprinting");
      digest.update(buffer.array(), 0, read);
      position += read;
      remaining -= read;
    }
  }

  public static SessionFileFingerprint compute(Path path) throws IOException {
    return compute(path, ignored -> {}, () -> Thread.currentThread().isInterrupted());
  }

  public static SessionFileFingerprint compute(Path path, LongConsumer progress, BooleanSupplier cancelled) throws IOException {
    checkCancelled(cancelled);
    final var resolved = path.toRealPath();
    final var before = Files.readAttributes(resolved, BasicFileAttributes.class);
    if (!before.isRegularFile()) throw new IOException("Session resource is not a regular file: " + path);
    final MessageDigest digest;
    try { digest = MessageDigest.getInstance("SHA-256"); }
    catch (NoSuchAlgorithmException impossible) { throw new IllegalStateException(impossible); }
    long bytes = 0;
    final var buffer = new byte[1 << 20];
    try (var input = Files.newInputStream(resolved)) {
      while (true) {
        checkCancelled(cancelled);
        int read = input.read(buffer);
        if (read < 0) break;
        if (read == 0) continue;
        digest.update(buffer, 0, read);
        bytes = Math.addExact(bytes, read);
        progress.accept(bytes);
      }
    }
    checkCancelled(cancelled);
    final var after = Files.readAttributes(resolved, BasicFileAttributes.class);
    if (bytes != before.size() || after.size() != before.size()
        || !after.lastModifiedTime().equals(before.lastModifiedTime())
        || !Objects.equals(after.fileKey(), before.fileKey())
        || !path.toRealPath().equals(resolved)) {
      throw new IOException("Session resource changed while fingerprinting: " + path);
    }
    return new SessionFileFingerprint(FULL, bytes, HexFormat.of().formatHex(digest.digest()));
  }

  private static void checkCancelled(BooleanSupplier cancelled) throws InterruptedIOException {
    if (cancelled.getAsBoolean()) throw new InterruptedIOException("Session fingerprint cancelled");
  }
}
