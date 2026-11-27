package ru.itmo.ctlab.hict.hict_server.normalization;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.itmo.ctlab.hict.fixtures.ReinforcementHdf5Fixture;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.CoverageWeightsBuilder;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.NormalizationWeights;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class CoverageCacheTest {
  @TempDir Path directory;

  @Test void roundTripsVectorsAndRejectsDamagedPayloads() throws Exception {
    var map = ReinforcementHdf5Fixture.create(directory, "roundtrip", List.of(4L, 10L));
    var processed = directory.resolve("processed");
    try (var file = open(map)) {
      var key = CoverageCache.key(file);
      var expectedBins = CoverageCache.expectedBins(file);
      var original = CoverageWeightsBuilder.build(file, CoverageWeightsBuilder.DEFAULT_BUDGET_BYTES, () -> false, null);
      assertNull(CoverageCache.read(processed, key, expectedBins));
      CoverageCache.write(processed, key, original);
      assertWeightsEqual(original, CoverageCache.read(processed, key, expectedBins));

      var target = processed.resolve("coverage").resolve(key + ".bin");
      var bytes = Files.readAllBytes(target);
      bytes[bytes.length - 5] ^= 1;
      Files.write(target, bytes);
      assertThrows(IOException.class, () -> CoverageCache.read(processed, key, expectedBins));
      Files.setLastModifiedTime(map, FileTime.fromMillis(Files.getLastModifiedTime(map).toMillis() + 2_000));
      assertNotEquals(key, CoverageCache.key(file));
    }
  }

  @Test void reopensWithoutCallingTheScannerAndRebuildsOnCorruption() throws Exception {
    var map = ReinforcementHdf5Fixture.create(directory, "reopen", List.of(4L, 10L));
    var processed = directory.resolve("processed");
    var scans = new AtomicInteger();
    var executor = Executors.newSingleThreadExecutor();
    var service = new CoverageNormalizationService(executor, "Primary", (file, budget, cancelled, progress) -> {
      scans.incrementAndGet();
      return CoverageWeightsBuilder.build(file, budget, cancelled, progress);
    });
    try {
      final String key;
      final NormalizationWeights.Prepared original;
      try (var file = open(map)) {
        service.start(file, CoverageWeightsBuilder.DEFAULT_BUDGET_BYTES, processed);
        executor.submit(() -> {}).get(5, TimeUnit.SECONDS);
        assertEquals("Saved coverage", service.status().message());
        original = file.getNormalizationWeights().prepared();
        key = CoverageCache.key(file);
        assertEquals(1, scans.get());
        file.getScaffoldingOperations().reverseSelectionRangeBp(0, 17);
        assertSame(original, file.getNormalizationWeights().prepared());
        assertEquals(key, CoverageCache.key(file));
        service.invalidateForMapChange();
      }
      try (var file = open(map)) {
        service.start(file, CoverageWeightsBuilder.DEFAULT_BUDGET_BYTES, processed);
        executor.submit(() -> {}).get(5, TimeUnit.SECONDS);
        assertEquals("Loaded saved coverage", service.status().message());
        assertEquals(1, scans.get(), "The cache hit must bypass the contact scanner");
        assertWeightsEqual(original, file.getNormalizationWeights().prepared());
        service.invalidateForMapChange();
      }
      Files.write(processed.resolve("coverage").resolve(key + ".bin"), new byte[] {1, 2, 3, 4});
      try (var file = open(map)) {
        service.start(file, CoverageWeightsBuilder.DEFAULT_BUDGET_BYTES, processed);
        executor.submit(() -> {}).get(5, TimeUnit.SECONDS);
        assertEquals("Saved coverage", service.status().message());
        assertEquals(2, scans.get());
        assertWeightsEqual(original, file.getNormalizationWeights().prepared());
      }
    } finally {
      executor.shutdownNow();
    }
  }

  private static ChunkedFile open(Path file) {
    return new ChunkedFile(new ChunkedFile.ChunkedFileOptions(file, 1, 2));
  }

  private static void assertWeightsEqual(NormalizationWeights.Prepared expected, NormalizationWeights.Prepared actual) {
    assertNotNull(actual);
    assertEquals(expected.algorithmVersion(), actual.algorithmVersion());
    assertArrayEquals(expected.validBinsByResolution(), actual.validBinsByResolution());
    assertEquals(expected.weightsByResolution().length, actual.weightsByResolution().length);
    for (int level = 0; level < expected.weightsByResolution().length; level++) {
      assertArrayEquals(expected.weightsByResolution()[level], actual.weightsByResolution()[level]);
    }
  }
}
