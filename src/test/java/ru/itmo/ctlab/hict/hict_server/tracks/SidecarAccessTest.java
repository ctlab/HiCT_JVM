package ru.itmo.ctlab.hict.hict_server.tracks;

import ch.systemsx.cisd.hdf5.HDF5Factory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.hdf5.HDF5LibraryInitializer;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

class SidecarAccessTest {
  @TempDir Path directory;
  @Test void writerWaitsForReaderCloseThenPersistsCompleteDataset() throws Exception {
    HDF5LibraryInitializer.initializeHDF5Library();
    var path = directory.resolve("weights.h5");
    try (var writer = HDF5Factory.open(path.toFile())) { writer.int64().writeArray("/values", new long[]{1,2,3}); }
    var started = new CountDownLatch(1);
    var acquired = new CountDownLatch(1);
    try (var executor = Executors.newSingleThreadExecutor()) {
      java.util.concurrent.Future<?> write;
      try (var access = SidecarAccess.read(path); var reader = HDF5Factory.openForReading(path.toFile())) {
        write = executor.submit(() -> {
          started.countDown();
          try (var lease = SidecarAccess.write(directory.resolve(".").resolve("weights.h5"));
               var writer = HDF5Factory.open(path.toFile())) {
            acquired.countDown();
            writer.int64().writeArray("/values", new long[]{4,5,6});
          }
        });
        assertTrue(started.await(2, TimeUnit.SECONDS));
        assertFalse(acquired.await(100, TimeUnit.MILLISECONDS), "Writer must not open while a read-only handle exists");
        assertArrayEquals(new long[]{1,2,3}, reader.int64().readArray("/values"));
      }
      write.get(5, TimeUnit.SECONDS);
      try (var access = SidecarAccess.read(path); var reader = HDF5Factory.openForReading(path.toFile())) {
        assertArrayEquals(new long[]{4,5,6}, reader.int64().readArray("/values"));
      }
    }
  }
}
