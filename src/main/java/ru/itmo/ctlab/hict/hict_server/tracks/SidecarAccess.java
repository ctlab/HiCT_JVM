package ru.itmo.ctlab.hict.hict_server.tracks;

import java.nio.file.Path;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/** Coordinates the lifetime of HDF5 sidecar handles, not matrix computation.
 * HDF5 cannot upgrade a file to read/write while another read-only handle is open.
 * A bounded set of locks is shared by managers in this JVM; normal in-memory
 * track queries never acquire these locks.
 */
final class SidecarAccess implements AutoCloseable {
  private static final ReentrantReadWriteLock[] LOCKS = new ReentrantReadWriteLock[128];
  static {
    for (int i = 0; i < LOCKS.length; i++) {
      LOCKS[i] = new ReentrantReadWriteLock();
    }
  }
  private final Lock lock;
  private SidecarAccess(Path path, boolean writing) {
    var fileLock = LOCKS[Math.floorMod(path.toAbsolutePath().normalize().hashCode(), LOCKS.length)];
    lock = writing ? fileLock.writeLock() : fileLock.readLock();
    lock.lock();
  }
  static SidecarAccess read(Path path) {
    return new SidecarAccess(path, false);
  }
  static SidecarAccess write(Path path) {
    return new SidecarAccess(path, true);
  }
  @Override
  public void close() {
    lock.unlock();
  }
}
