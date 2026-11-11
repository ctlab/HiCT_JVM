package ru.itmo.ctlab.hict.hict_library.util;

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

/** Bounded content identity using the HICT-QUICK-1 session sampling format. */
public record QuickFileFingerprint(long sizeBytes, String sha256) {
  private static final String QUICK = "HICT-QUICK-1";
  private static final long EDGE = 4L << 20;
  private static final int BLOCK = 64 << 10;
  private static final int BLOCKS = 16;

  public static QuickFileFingerprint compute(Path path) throws IOException {
    return compute(path, () -> Thread.currentThread().isInterrupted());
  }

  public static QuickFileFingerprint compute(Path path, BooleanSupplier cancelled) throws IOException {
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
    return new QuickFileFingerprint(size, HexFormat.of().formatHex(digest.digest()));
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

  private static void checkCancelled(BooleanSupplier cancelled) throws InterruptedIOException {
    if (cancelled.getAsBoolean()) throw new InterruptedIOException("File fingerprint cancelled");
  }
}
