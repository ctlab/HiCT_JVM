package ru.itmo.ctlab.hict.hict_library.visualization;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.itmo.ctlab.hict.fixtures.ReinforcementHdf5Fixture;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.ChunkedFile;
import ru.itmo.ctlab.hict.hict_library.chunkedfile.resolution.ResolutionDescriptor;
import ru.itmo.ctlab.hict.hict_library.visualization.colormap.gradient.SimpleLinearGradient;
import java.nio.file.Path;
import java.awt.Color;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class DisplayedSignalTest {
  @TempDir Path directory;

  @Test void diagnosticSignalAndRenderedPixelsShareMissingSupportAndMeasuredZeros() throws Exception {
    var path = ReinforcementHdf5Fixture.create(directory, "displayed-signal", List.of(4L));
    try (var file = new ChunkedFile(new ChunkedFile.ChunkedFileOptions(path, 1, 2))) {
      var resolution = ResolutionDescriptor.fromBpResolution(4, file);
      var matrix = file.matrixQueries().getSubmatrix(resolution, 0, 0, 5, 5, false);
      var palette = new SimpleLinearGradient(32, Color.BLUE, Color.RED, 0.5, 2);
      var options = new SimpleVisualizationOptions(-1, -1, false, false, false, false, .995,
        SignalDisplayMode.OBSERVED_OVER_EXPECTED, palette);
      var accumulator = DistanceExpectedNormalizer.newSegmentedAccumulator(
        resolution.getResolutionOrderInArray(), 0, 5, 0, 5,
        new DistanceExpectedNormalizer.PixelDomain[]{new DistanceExpectedNormalizer.PixelDomain(0,2),
          new DistanceExpectedNormalizer.PixelDomain(2,5)});
      // Deliberately nonzero support for every within-domain distance, even where observed = 0.
      accumulator.addSignal(new double[][]{{4,4,4,4,4},{4,4,4,4,4},{4,4,4,4,4},{4,4,4,4,4},{4,4,4,4,4}},0,0);
      var profile = accumulator.toProfile();
      var numeric = file.tileVisualizationProcessor().processDisplaySignal(matrix, options, profile).values();
      var rgba = TileVisualizationProcessor.mapObservedOverExpectedRgba(numeric, palette);
      int zeros = 0, missing = 0;
      for (int r=0;r<5;r++) for(int c=0;c<5;c++) {
        if ((r < 2) != (c < 2)) {
          assertTrue(Double.isNaN(numeric[r][c]));
          assertEquals(0, rgba[(r*5+c)*4+3]);
          missing++;
        } else {
          assertEquals(matrix.matrix().getAsDouble(r,c)/4, numeric[r][c], 1e-12);
          if (numeric[r][c] == 0) {
            assertEquals(255, Byte.toUnsignedInt(rgba[(r*5+c)*4+3]));
            zeros++;
          }
        }
      }
      assertEquals(12, missing);
      assertTrue(zeros > 0, "fixture must distinguish supported zeros from missing ratios");
    }
  }
}
