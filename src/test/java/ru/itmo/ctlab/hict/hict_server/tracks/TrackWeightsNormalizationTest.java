package ru.itmo.ctlab.hict.hict_server.tracks;

import ch.systemsx.cisd.hdf5.HDF5Factory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.itmo.ctlab.hict.fixtures.ReinforcementHdf5Fixture;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.CoverageWeightsBuilder;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.NormalizationWeights;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static ru.itmo.ctlab.hict.hict_library.chunkedfile.util.PathGenerators.getStripeBinWeightsDatasetPath;

class TrackWeightsNormalizationTest {
  @TempDir Path directory;

  private Path fixture(String name, double[] weights) throws Exception {
    final var path = ReinforcementHdf5Fixture.create(directory, name, List.of(4L, 10L));
    try (var writer = HDF5Factory.open(path.toFile())) {
      writer.float64().writeMatrix(getStripeBinWeightsDatasetPath(4), new double[][]{weights});
    }
    return path;
  }

  private static void prepare(Track1DManager tracks, ChunkedFile primary, ChunkedFile secondary) throws Exception {
    tracks.startPrecompute(primary, secondary, null, true);
    final long deadline = System.nanoTime() + 10_000_000_000L;
    while (tracks.getPrecomputeStatus().getRunningJobs() > 0) {
      assertTrue(System.nanoTime() < deadline, "Precompute did not finish");
      Thread.sleep(10);
    }
  }

  private static void values(Track1DManager tracks, ChunkedFile primary, ChunkedFile secondary,
                             double[] expectedPrimary, double[] expectedSecondary) throws Exception {
    final long deadline = System.nanoTime() + 10_000_000_000L;
    for (;;) {
      final var result = tracks.queryVisibleTracks(primary, secondary, 0, 5, 5, 4);
      if (result.getTracks().stream().allMatch(t -> t.getStatusMessage() == null || t.getStatusMessage().isBlank())) {
        assertEquals(2, result.getTracks().size());
        assertArrayEquals(expectedPrimary, result.getTracks().get(0).getBins().stream().mapToDouble(Track1DManager.TrackBin::getValue).toArray(), 1e-12);
        assertArrayEquals(expectedSecondary, result.getTracks().get(1).getBins().stream().mapToDouble(Track1DManager.TrackBin::getValue).toArray(), 1e-12);
        return;
      }
      assertTrue(System.nanoTime() < deadline, "Weights did not become available");
      Thread.sleep(10);
    }
  }

  @Test void weightTracksFollowNormalizationAfterPrecomputeInBothDirectionsAndSources() throws Exception {
    final double[] primaryWeights = {2, 3, 4, 5, 6}, secondaryWeights = {7, 8, 9, 10, 11};
    final var primaryPath = fixture("primary", primaryWeights);
    final var secondaryPath = fixture("secondary", secondaryWeights);
    final var primaryBytes = Files.readAllBytes(primaryPath);
    final var secondaryBytes = Files.readAllBytes(secondaryPath);
    try (var primary = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(primaryPath, 1, 2));
         var secondary = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(secondaryPath, 1, 2));
         var tracks = new Track1DManager(directory, directory.resolve("processed"))) {
      tracks.openCoolerWeightsTrack("Primary", null, "PRIMARY");
      tracks.openCoolerWeightsTrack("Secondary", null, "SECONDARY");
      prepare(tracks, primary, secondary);
      values(tracks, primary, secondary, primaryWeights, secondaryWeights);
      for (var file : List.of(primary, secondary)) {
        file.getNormalizationWeights().publish(CoverageWeightsBuilder.build(file, 1L << 20, () -> false, null));
        file.getNormalizationWeights().select(NormalizationWeights.Mode.SQRT_COVERAGE);
      }
      // Independent row sums [4,10,7,0,11], symmetric total 32 and total-preserving scale.
      final double factor = Math.sqrt((1.0 / 4 + 6.0 / Math.sqrt(40) + 14.0 / Math.sqrt(70) + 1) / 32);
      final double[] coverage = {1 / (factor * 2), 1 / (factor * Math.sqrt(10)),
        1 / (factor * Math.sqrt(7)), 1 / (factor * Math.sqrt(11))};
      values(tracks, primary, secondary, coverage, coverage);
      prepare(tracks, primary, secondary);
      values(tracks, primary, secondary, coverage, coverage);
      for (var file : List.of(primary, secondary)) file.getNormalizationWeights().select(NormalizationWeights.Mode.FILE);
      values(tracks, primary, secondary, primaryWeights, secondaryWeights);
    }
    assertArrayEquals(primaryBytes, Files.readAllBytes(primaryPath));
    assertArrayEquals(secondaryBytes, Files.readAllBytes(secondaryPath));
  }
}
