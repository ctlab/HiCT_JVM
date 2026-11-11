package ru.itmo.ctlab.hict.hict_library.assembly;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.itmo.ctlab.hict.fixtures.ReinforcementHdf5Fixture;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.resolution.ResolutionDescriptor;
import ru.itmo.ctlab.hict.hict_library.domain.QueryLengthUnit;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class AssemblyBinGridTest {
  @TempDir Path directory;

  @Test
  void projectsPartialAndNonNestedBinsThroughReversalAndSplit() throws Exception {
    final var primaryPath = ReinforcementHdf5Fixture.create(directory, "primary", List.of(4L, 10L));
    final var secondaryPath = ReinforcementHdf5Fixture.create(directory, "secondary", List.of(3L, 7L));
    try (var primary = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(primaryPath, 1, 2));
         var secondary = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(secondaryPath, 1, 2))) {
      final var primaryLevel = ResolutionDescriptor.fromBpResolution(4, primary);
      final var secondaryLevel = ResolutionDescriptor.fromBpResolution(3, secondary);
      final var secondaryGrid = new AssemblyBinGrid(secondary, secondaryLevel);
      final var region = new AssemblyAnalysisPlan.Region(1, 16);
      final var estimate = AssemblyAnalysisPlan.estimate(secondary, List.of(3L, 7L), region, region);
      final var analysis = AssemblyAnalysisPlan.capture(secondary, List.of(3L, 7L), region, region);
      final long actualCells = analysis.levels().stream().mapToLong(l -> (long) l.rows().size() * l.columns().size()).sum();
      org.junit.jupiter.api.Assertions.assertTrue(estimate.cells() >= actualCells);
      org.junit.jupiter.api.Assertions.assertTrue(estimate.outputBytes() >= actualCells * 3 * Long.BYTES);
      final var initial = new AssemblyBinGrid(primary, primaryLevel);
      assertEquals(List.of(new AssemblyBinGrid.Overlap(0, 3, 3), new AssemblyBinGrid.Overlap(1, 1, 3)),
        overlaps(secondaryGrid, initial.binAt(0)));
      assertEquals(List.of(new AssemblyBinGrid.Overlap(5, 1, 2)), overlaps(secondaryGrid, initial.binAt(4)));
      assertNull(initial.binAt(-1));
      assertNull(initial.binAt(5));
      final var counts = ProjectedMatrix.query(initial, secondaryGrid, secondary, primaryLevel, secondaryLevel,
        0, 0, 5, 5, ProjectedMatrix.Reducer.COUNTS).matrix();
      double total = 0;
      for (int r = 0; r < counts.rows(); r++) {
        for (int c = 0; c < counts.cols(); c++) total += counts.getAsDouble(r, c);
      }
      assertEquals(32, total, 1e-9);
      assertEquals(2.75, counts.getAsDouble(4, 4), 1e-9);
      final var identity = ProjectedMatrix.query(initial, secondaryGrid, secondary, primaryLevel, secondaryLevel,
        4, 4, 5, 5, ProjectedMatrix.Reducer.IDENTITY).matrix();
      assertEquals(11, identity.getAsDouble(0, 0));
      final var presence = ProjectedMatrix.query(initial, secondaryGrid, secondary, primaryLevel, secondaryLevel,
        4, 4, 5, 5, ProjectedMatrix.Reducer.PRESENCE).matrix();
      assertEquals(1, presence.getAsDouble(0, 0));

      primary.getScaffoldingOperations().splitContigAtBin(8,
        ResolutionDescriptor.fromResolutionOrder(0), QueryLengthUnit.BASE_PAIRS);
      final var split = new AssemblyBinGrid(primary, primaryLevel);
      // Native Split removes the selected 4 bp bin from the assembly.
      assertEquals(overlaps(secondaryGrid, initial.binAt(3)), overlaps(secondaryGrid, split.binAt(2)));
      assertNull(split.binAt(4));

      primary.getScaffoldingOperations().reverseSelectionRangeBp(0, ReinforcementHdf5Fixture.LENGTH - 4);
      final var reversed = new AssemblyBinGrid(primary, primaryLevel);
      assertEquals(new AssemblyBinGrid.Bin("tiny", 16, 17, 0, true), reversed.binAt(0));
      assertEquals(overlaps(secondaryGrid, initial.binAt(4)), overlaps(secondaryGrid, reversed.binAt(0)));
      assertEquals(overlaps(secondaryGrid, initial.binAt(0)), overlaps(secondaryGrid, reversed.binAt(3)));
    }
  }

  private static List<AssemblyBinGrid.Overlap> overlaps(AssemblyBinGrid grid, AssemblyBinGrid.Bin bin) {
    final var result = new ArrayList<AssemblyBinGrid.Overlap>();
    grid.overlaps(bin, result::add);
    return result;
  }
}
