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
}
