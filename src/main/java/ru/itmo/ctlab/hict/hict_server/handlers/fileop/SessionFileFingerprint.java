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

/** Content identity for session resources. Deliberately does not cache by size/mtime. */
public record SessionFileFingerprint(String algorithm, long sizeBytes, String sha256) {
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
    return new SessionFileFingerprint("SHA-256", bytes, HexFormat.of().formatHex(digest.digest()));
  }

  private static void checkCancelled(BooleanSupplier cancelled) throws InterruptedIOException {
    if (cancelled.getAsBoolean()) throw new InterruptedIOException("Session fingerprint cancelled");
  }
}
