package ru.itmo.ctlab.hict.hict_server.handlers.fileop;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import static org.junit.jupiter.api.Assertions.*;

class SessionFileFingerprintTest {
  @TempDir Path directory;

  @Test void hashesContentAndDetectsSameSizeSameTimestampReplacement() throws Exception {
    var path = directory.resolve("input");
    Files.writeString(path, "abc");
    var modified = Files.getLastModifiedTime(path);
    var fingerprint = SessionFileFingerprint.compute(path);
    assertEquals("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad", fingerprint.sha256());
    assertEquals(3, fingerprint.sizeBytes());
    assertEquals("SHA-256", fingerprint.algorithm());
    Files.writeString(path, "abd");
    Files.setLastModifiedTime(path, modified);
    assertNotEquals(fingerprint, SessionFileFingerprint.compute(path));
    Files.writeString(path, "abc");
    assertEquals(fingerprint, SessionFileFingerprint.compute(path));
  }

  @Test void reportsProgressAndHonorsCancellation() throws Exception {
    var path = directory.resolve("large");
    Files.write(path, new byte[(1 << 20) + 17]);
    var progress = new AtomicLong();
    var fingerprint = SessionFileFingerprint.compute(path, progress::set, () -> false);
    assertEquals(Files.size(path), progress.get());
    assertEquals(Files.size(path), fingerprint.sizeBytes());
    var cancel = new AtomicBoolean();
    assertThrows(InterruptedIOException.class, () -> SessionFileFingerprint.compute(path,
      ignored -> cancel.set(true), cancel::get));
    assertEquals(fingerprint, SessionFileFingerprint.compute(path));
  }

  @Test void refusesChangingResourcesAndDirectories() throws Exception {
    var path = directory.resolve("changing");
    Files.write(path, new byte[17]);
    assertThrows(IOException.class, () -> SessionFileFingerprint.compute(path, ignored -> {
      try { Files.writeString(path, "changed length"); }
      catch (IOException e) { throw new java.io.UncheckedIOException(e); }
    }, () -> false));
    assertThrows(IOException.class, () -> SessionFileFingerprint.compute(directory));
    assertThrows(IOException.class, () -> SessionFileFingerprint.compute(directory.resolve("missing")));
  }

  @Test void quickIdentityIsBoundedAndSensitiveToStartMiddleEndAndSize() throws Exception {
    var path = directory.resolve("big");
    long size = 3L << 30;
    try (var file = new java.io.RandomAccessFile(path.toFile(), "rw")) { file.setLength(size); }
    long started = System.nanoTime();
    var quick = SessionFileFingerprint.computeQuick(path);
    long millis = (System.nanoTime() - started) / 1_000_000;
    assertTrue(millis < 2000, "quick identity of a 3 GiB file took " + millis + " ms");
    assertEquals(SessionFileFingerprint.QUICK, quick.algorithm());
    assertEquals(size, quick.sizeBytes());
    assertEquals(quick, SessionFileFingerprint.compute(path, SessionFileFingerprint.QUICK));
    long middle = (1L << 22) + ((size - (1L << 23) - (64 << 10)) / 15) * 7 + 5;
    for (long offset : new long[]{0, 100, size - 1, size - (1L << 22), middle}) {
      try (var file = new java.io.RandomAccessFile(path.toFile(), "rw")) { file.seek(offset); file.write(1); }
      var changed = SessionFileFingerprint.computeQuick(path);
      assertNotEquals(quick, changed, "edit at " + offset + " must change the identity");
      assertEquals("its content differs from the saved version (checked at the start, middle and end)", quick.explainDifference(changed));
      try (var file = new java.io.RandomAccessFile(path.toFile(), "rw")) { file.seek(offset); file.write(0); }
      assertEquals(quick, SessionFileFingerprint.computeQuick(path));
    }
    try (var file = new java.io.RandomAccessFile(path.toFile(), "rw")) { file.setLength(size + 1); }
    assertEquals("its size changed from " + size + " to " + (size + 1) + " bytes",
      quick.explainDifference(SessionFileFingerprint.computeQuick(path)));
  }

  @Test void quickIdentityOfSmallFilesCoversEveryByteAndIgnoresTimestamps() throws Exception {
    var path = directory.resolve("small");
    Files.write(path, new byte[1000]);
    var modified = Files.getLastModifiedTime(path);
    var quick = SessionFileFingerprint.computeQuick(path);
    var digest = java.security.MessageDigest.getInstance("SHA-256");
    digest.update("HICT-QUICK-1".getBytes(java.nio.charset.StandardCharsets.US_ASCII));
    digest.update(java.nio.ByteBuffer.allocate(Long.BYTES).putLong(1000).array());
    digest.update(new byte[1000]);
    assertEquals(java.util.HexFormat.of().formatHex(digest.digest()), quick.sha256(),
      "Shared sampling must preserve fingerprints in existing session files");
    Files.setLastModifiedTime(path, java.nio.file.attribute.FileTime.fromMillis(modified.toMillis() + 60_000));
    assertEquals(quick, SessionFileFingerprint.computeQuick(path));
    var bytes = new byte[1000];
    bytes[517] = 1;
    Files.write(path, bytes);
    assertNotEquals(quick, SessionFileFingerprint.computeQuick(path));
    assertNotEquals(SessionFileFingerprint.compute(path).sha256(), SessionFileFingerprint.computeQuick(path).sha256());
    assertThrows(IOException.class, () -> SessionFileFingerprint.compute(path, "MD5"));
    assertTrue(SessionFileFingerprint.supports("SHA-256") && SessionFileFingerprint.supports("HICT-QUICK-1"));
  }
}
