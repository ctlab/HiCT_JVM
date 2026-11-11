package ru.itmo.ctlab.hict.hict_library.chunkedfile;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.itmo.ctlab.hict.fixtures.ReinforcementHdf5Fixture;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.resolution.ResolutionDescriptor;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** Square-root coverage weights computed from a real converted map. */
class CoverageWeightsTest {
  @TempDir Path directory;

  private static double[][] fullMatrix(ChunkedFile file, long resolution, int bins) {
    var descriptor = ResolutionDescriptor.fromBpResolution(resolution, file);
    var raw = file.getMatrixQueries().getSubmatrix(descriptor, 0, 0, bins, bins, false);
    var result = new double[bins][bins];
    for (int r = 0; r < bins; r++) for (int c = 0; c < bins; c++) result[r][c] = raw.matrix().getAsDouble(r, c);
    return result;
  }

  @Test void coverageWeightsEqualTheReferenceRecipeOnTheFullMatrix() throws Exception {
    var path = ReinforcementHdf5Fixture.create(directory, "cov", List.of(4L, 10L));
    try (var file = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(path, 1, 2))) {
      var prepared = CoverageWeightsBuilder.build(file, CoverageWeightsBuilder.DEFAULT_BUDGET_BYTES, () -> false, null);
      for (int order = 1; order < file.getResolutions().length; order++) {
        long resolution = file.getResolutions()[order];
        int bins = (int) file.getMatrixSizeBins()[order];
        var m = fullMatrix(file, resolution, bins);
        // Reference recipe straight from the full symmetric matrix.
        double[] rowSum = new double[bins];
        double total = 0;
        for (int r = 0; r < bins; r++) for (int c = 0; c < bins; c++) { rowSum[r] += m[r][c]; total += m[r][c]; }
        double[] d = new double[bins];
        for (int i = 0; i < bins; i++) d[i] = Math.sqrt(rowSum[i]);
        double normalized = 0;
        for (int r = 0; r < bins; r++) for (int c = 0; c < bins; c++)
          if (d[r] > 0 && d[c] > 0) normalized += m[r][c] / (d[r] * d[c]);
        double matrixSum = 0;
        for (int r = 0; r < bins; r++) for (int c = 0; c < bins; c++) if (d[r] > 0 && d[c] > 0) matrixSum += m[r][c];
        double factor = Math.sqrt(normalized / matrixSum);
        var weights = prepared.weightsByResolution()[order];
        assertEquals(bins, weights.length, "resolution " + resolution);
        for (int i = 0; i < bins; i++) {
          if (d[i] > 0) assertEquals(1.0 / (factor * d[i]), weights[i], 1e-9, "resolution " + resolution + " bin " + i);
          else assertTrue(Double.isNaN(weights[i]), "bin without contacts has no weight");
        }
      }
    }
  }

  @Test void selectedWeightsFollowTheBinsThroughReversalAndFileWeightsAreUntouched() throws Exception {
    var path = ReinforcementHdf5Fixture.create(directory, "rev", List.of(4L));
    try (var file = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(path, 1, 2))) {
      var descriptor = ResolutionDescriptor.fromBpResolution(4L, file);
      int bins = (int) file.getMatrixSizeBins()[1];
      var fileWeights = file.getMatrixQueries().getSubmatrix(descriptor, 0, 0, bins, bins, false).rowWeights().clone();
      assertThrows(IllegalStateException.class, () -> file.getNormalizationWeights().select(NormalizationWeights.Mode.SQRT_COVERAGE));

      file.getNormalizationWeights().publish(CoverageWeightsBuilder.build(file, CoverageWeightsBuilder.DEFAULT_BUDGET_BYTES, () -> false, null));
      var before = file.getMatrixQueries().getSubmatrix(descriptor, 0, 0, bins, bins, false).rowWeights().clone();
      assertArrayEquals(fileWeights, before, 0.0, "publishing alone must not change what is drawn");

      file.getNormalizationWeights().select(NormalizationWeights.Mode.SQRT_COVERAGE);
      var coverage = file.getMatrixQueries().getSubmatrix(descriptor, 0, 0, bins, bins, false).rowWeights().clone();
      assertFalse(java.util.Arrays.equals(fileWeights, coverage), "coverage must replace the file weights");

      file.getScaffoldingOperations().reverseSelectionRangeBp(0, 17);
      var reversed = file.getMatrixQueries().getSubmatrix(descriptor, 0, 0, bins, bins, false).rowWeights();
      for (int i = 0; i < bins; i++) {
        double a = coverage[bins - 1 - i], b = reversed[i];
        assertTrue((Double.isNaN(a) && Double.isNaN(b)) || Math.abs(a - b) < 1e-12, "bin " + i);
      }
      file.getScaffoldingOperations().reverseSelectionRangeBp(0, 17);

      file.getNormalizationWeights().select(NormalizationWeights.Mode.FILE);
      var back = file.getMatrixQueries().getSubmatrix(descriptor, 0, 0, bins, bins, false).rowWeights();
      assertArrayEquals(fileWeights, back, 0.0);
    }
  }

  @Test void oversizedMapsAreRefusedBeforeAnyScan() throws Exception {
    var path = ReinforcementHdf5Fixture.create(directory, "big", List.of(4L));
    try (var file = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(path, 1, 2))) {
      var error = assertThrows(IllegalStateException.class, () -> CoverageWeightsBuilder.build(file, 8L, () -> false, null));
      assertTrue(error.getMessage().contains("HICT_COVERAGE_MAX_BYTES"), error.getMessage());
    }
  }

  @Test void cancellationLeavesNothingPublished() throws Exception {
    var path = ReinforcementHdf5Fixture.create(directory, "cancel", List.of(4L));
    try (var file = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(path, 1, 2))) {
      assertThrows(ru.itmo.ctlab.hict.hict_library.visualization.CoverageNormalizer.CancelledException.class,
        () -> CoverageWeightsBuilder.build(file, CoverageWeightsBuilder.DEFAULT_BUDGET_BYTES, () -> true, null));
      assertNull(file.getNormalizationWeights().prepared());
    }
  }
}
