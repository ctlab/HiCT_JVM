package ru.itmo.ctlab.hict.hict_library.chunkedfile;

import ch.systemsx.cisd.hdf5.HDF5Factory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.itmo.ctlab.hict.fixtures.ReinforcementHdf5Fixture;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.resolution.ResolutionDescriptor;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ReinforcementHdf5Test {
  @TempDir Path directory;

  @Test void convertedPixelsZeroRowsPartialBinsAndReversalMatchLiteralOracle() throws Exception {
    var path = ReinforcementHdf5Fixture.create(directory, "primary", List.of(4L, 10L));
    try (var reader = HDF5Factory.openForReading(directory.resolve("primary.mcool").toFile())) {
      assertArrayEquals(new long[]{4, 8, 12, 16, 17}, reader.int64().readArray("/resolutions/4/bins/end"));
    }
    try (var file = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(path, 1, 2))) {
      assertArrayEquals(new long[]{0, 4, 10}, file.getResolutions());
      assertArrayEquals(new long[]{17, 5, 2}, file.getMatrixSizeBins());
      double[][] expected = {{1,3,0,0,0},{3,0,7,0,0},{0,7,0,0,0},{0,0,0,0,0},{0,0,0,0,11}};
      check(file, 4, expected, false);
      // Upper-triangle input contacts that collapse onto a diagonal are counted
      // once, not twice. Full symmetric matrix sums need not survive rebinning.
      check(file, 10, new double[][]{{11,0},{0,11}}, false);
      file.getScaffoldingOperations().reverseSelectionRangeBp(0, 17);
      check(file, 4, expected, true);
      file.getScaffoldingOperations().reverseSelectionRangeBp(0, 17);
      check(file, 4, expected, false);
    }
  }

  @Test void disjointFineAndCoarsePyramidsUseTheirOwnStoredCoordinates() throws Exception {
    for (var levels : List.of(List.of(3L, 7L), List.of(1L), List.of(25L))) {
      var path = ReinforcementHdf5Fixture.create(directory, "source-" + levels.getFirst(), levels);
      try (var file = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(path, 1, 2))) {
        assertEquals(levels.size() + 1, file.getResolutions().length);
        for (long resolution : levels) {
          int bins = (int) ((17 + resolution - 1) / resolution);
          var matrix = file.getMatrixQueries().getSubmatrix(ResolutionDescriptor.fromBpResolution(resolution, file),
            0, 0, bins, bins, false).matrix();
          double[][] expected = new double[bins][bins];
          for (var contact : ReinforcementHdf5Fixture.CONTACTS) {
            int row = (int) (contact[0] / resolution), col = (int) (contact[1] / resolution);
            expected[row][col] += contact[2];
            if (row != col) expected[col][row] += contact[2];
          }
          for (int row = 0; row < bins; row++) for (int col = 0; col < bins; col++)
            assertEquals(expected[row][col], matrix.getAsDouble(row, col), "resolution=" + resolution + " at " + row + "," + col);
        }
      }
    }
  }

  private void check(ChunkedFile file, long resolution, double[][] expected, boolean reversed) {
    int bins = expected.length;
    var descriptor = ResolutionDescriptor.fromBpResolution(resolution, file);
    // Query every single pixel as well as the complete matrix to exercise offsets.
    var all = file.getMatrixQueries().getSubmatrix(descriptor, 0, 0, bins, bins, false).matrix();
    for (int row = 0; row < bins; row++) for (int col = 0; col < bins; col++) {
      double value = expected[reversed ? bins - 1 - row : row][reversed ? bins - 1 - col : col];
      assertEquals(value, all.getAsDouble(row, col));
      assertEquals(value, file.getMatrixQueries().getSubmatrix(descriptor, row, col, row + 1, col + 1, false)
        .matrix().getAsDouble(0, 0));
    }
  }
}
