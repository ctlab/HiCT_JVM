package ru.itmo.ctlab.hict.hict_library.chunkedfile;

import ch.systemsx.cisd.hdf5.HDF5Factory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.itmo.ctlab.hict.fixtures.ReinforcementHdf5Fixture;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.resolution.ResolutionDescriptor;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SequenceOverlayCoverageTest {
  @TempDir Path directory;

  @Test void computedZeroIsDistinctFromMissingAndFollowsAssemblyOrder() throws Exception {
    var map = ReinforcementHdf5Fixture.create(directory, "coverage", List.of(4L, 10L), 17,
      new long[][] {{0, 0, 10}});
    try (var writer = HDF5Factory.open(map.toFile())) {
      var region = "/sequence_analysis/updates/region/4/";
      writer.int64().writeMatrix(region + "row_bins", new long[][] {{0, 1}});
      writer.int64().writeMatrix(region + "column_bins", new long[][] {{0, 1}});
      writer.bool().setAttr("/sequence_analysis", "explicit_coverage", true);
    }
    try (var file = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(map, 1, 2))) {
      var resolution = ResolutionDescriptor.fromBpResolution(4, file);
      var matrix = file.matrixQueries().getSubmatrix(resolution, 0, 0, 5, 5, false, true);
      assertEquals(0, matrix.matrix().getAsDouble(1, 1));
      assertEquals(0, matrix.matrix().getAsDouble(2, 2));
      assertTrue(matrix.isComputed(1, 1));
      assertFalse(matrix.isComputed(2, 2));
      file.getScaffoldingOperations().reverseSelectionRangeBp(0, 17);
      var moved = file.matrixQueries().getSubmatrix(resolution, 0, 0, 5, 5, false, true);
      assertFalse(moved.isComputed(1, 1));
      assertTrue(moved.isComputed(3, 3));
      var other = file.matrixQueries().getSubmatrix(ResolutionDescriptor.fromBpResolution(10, file),
        0, 0, 2, 2, false, true);
      assertTrue(other.computedCells().isEmpty());
    }
  }
}
